from __future__ import annotations
import http.client
import json
from pathlib import Path
from theseus_api import ApiSuccess
from theseus_local.launcher import _write_launch_diagnostic
from theseus_local.project_run import ProjectRunEntry, ProjectRunRecord
from theseus_ui import CampaignUiApplication, LocalWorkspaceCampaignClient, asset_bytes, start_ui_server
from theseus_ui.local_control import LocalUiRegistry
class _FakeActions:
    def __init__(self, configuration):
        # Retain the exact configuration while simulating durable campaign actions.
        self.configuration = configuration
    def create_campaign(self, action_id, configuration):
        # Return one completed create receipt without executing the mutation engine.
        return ApiSuccess({"action_id": action_id, "campaign_id": configuration.campaign_id.value, "status": "completed", "campaign_revision": 0})
    def start_campaign(self, action_id, campaign_id, *, expected_revision):
        # Return one running start receipt without spawning a child process.
        return ApiSuccess({"action_id": action_id, "campaign_id": campaign_id, "status": "running", "campaign_revision": expected_revision})
def _project(tmp_path: Path, count: int = 9) -> Path:
    # Build one project large enough to use the bounded ProjectRun dispatcher.
    project = tmp_path / "project"
    project.mkdir()
    for index in range(count):
        (project / f"module_{index:03d}.py").write_text(f"VALUE = {index}\n", encoding="utf-8")
    return project
def test_a14_queued_run_is_not_reported_running_and_duplicate_launch_is_rejected(tmp_path: Path, monkeypatch) -> None:
    # Keep later one-click submissions from masquerading as simultaneously running ProjectRuns.
    client = LocalWorkspaceCampaignClient(tmp_path / "ui-state", actions_factory=lambda configuration: _FakeActions(configuration))
    project_id = client.register_project(str(_project(tmp_path)))["value"]["project_id"]
    monkeypatch.setattr(client, "_start_project_run_dispatcher", lambda _run_id: None)
    first = client.create_project_run({"project_id": project_id})
    second = client.create_project_run({"project_id": project_id})
    assert first["ok"] is True
    assert first["value"]["status"] == "queued"
    assert first["value"]["queued_count"] == 9
    assert first["value"]["active_count"] == 0
    assert first["value"]["queue_position"] == 1
    assert second["ok"] is False
    assert second["error"]["code"] == "project_run_already_active"
    assert second["error"]["details"]["run_id"] == first["value"]["run_id"]
def test_a14_failure_groups_and_stage_survive_registry_restart(tmp_path: Path) -> None:
    # Persist exact file failure stage and code so the ProjectRun screen survives UI restarts.
    state = tmp_path / "ui-state"
    registry = LocalUiRegistry(state)
    project = registry.register_project(_project(tmp_path, count=1))
    run = ProjectRunRecord(
        "project-run-diagnostics",
        project.project_id,
        (
            ProjectRunEntry("module_000.py", "campaign-failed", "failed", "campaign_baseline_failed", 0, 0, "baseline", "2026-08-14T10:00:00+00:00"),
            ProjectRunEntry("module_001.py", "campaign-ok", "completed", None, 2, 2),
        ),
    )
    registry.register_project_run(run)
    restarted = LocalWorkspaceCampaignClient(state, actions_factory=lambda configuration: _FakeActions(configuration))
    detail = restarted.get_project_run(run.run_id)["value"]
    assert detail["status"] == "partial"
    assert detail["failed_count"] == 1
    assert detail["completed_count"] == 1
    assert detail["failure_groups"] == [{"stage": "baseline", "code": "campaign_baseline_failed", "count": 1}]
    failed = detail["campaigns"][0]
    assert failed["source_path"] == "module_000.py"
    assert failed["error_stage"] == "baseline"
    assert failed["failed_at"] == "2026-08-14T10:00:00+00:00"
def test_a14_cancel_queued_run_is_durable_and_writes_operator_events(tmp_path: Path, monkeypatch) -> None:
    # Cancel an entirely queued duplicate without launching any child campaign and retain the reason in the ProjectRun journal.
    client = LocalWorkspaceCampaignClient(tmp_path / "ui-state", actions_factory=lambda configuration: _FakeActions(configuration))
    project_id = client.register_project(str(_project(tmp_path)))["value"]["project_id"]
    monkeypatch.setattr(client, "_start_project_run_dispatcher", lambda _run_id: None)
    created = client.create_project_run({"project_id": project_id})["value"]
    cancelled = client.cancel_project_run(created["run_id"])["value"]
    assert cancelled["status"] == "cancelled"
    assert cancelled["queued_count"] == 0
    assert cancelled["cancelled_count"] == 9
    assert cancelled["completed_at"] is not None
    events = cancelled["events"]
    assert [item["event_type"] for item in events] == ["run_created", "run_cancel_requested", "run_finished"]
def test_a14_launcher_diagnostic_records_private_stage_without_exception_message(tmp_path: Path) -> None:
    # Keep public diagnostics bounded while pointing operators to the private stderr log for the full traceback.
    database = tmp_path / "campaign" / "campaign.sqlite3"
    database.parent.mkdir(parents=True)
    _write_launch_diagnostic(database, "campaign-1", "action-1", stage="resume_campaign", exception_type="ValueError")
    payload = json.loads((database.parent / "campaign-launch.diagnostic.json").read_text(encoding="utf-8"))
    assert payload["stage"] == "resume_campaign"
    assert payload["exception_type"] == "ValueError"
    assert payload["stderr_log"] == "campaign-launch.stderr.log"
    assert "message" not in payload
def test_a14_browser_exposes_project_run_diagnostics_and_cancel_action() -> None:
    # Keep the browser wired to the ProjectRun detail route instead of forcing operators into registry.json.
    html = asset_bytes("index.html").decode("utf-8")
    script = asset_bytes("app.js").decode("utf-8")
    styles = asset_bytes("styles.css").decode("utf-8")
    assert 'id="project-run-detail-panel"' in html
    assert "Причины ошибок" in html
    assert "Журнал запуска" in html
    assert "openProjectRun" in script
    assert "failure_groups" in script
    assert "/actions/cancel" in script
    assert "project_run_already_active" in script
    assert ".project-run-summary-grid" in styles
def test_a14_http_cancel_route_targets_exact_project_run() -> None:
    # Route one explicit ProjectRun cancellation without requiring a campaign revision from the browser.
    class Client:
        def __init__(self):
            # Retain only the run identity observed by the route test.
            self.run_id = None
        def cancel_project_run(self, run_id):
            # Return one stable local success projection for the requested run.
            self.run_id = run_id
            return {"ok": True, "kind": "success", "value": {"run_id": run_id, "status": "cancelled"}}
    client = Client()
    response = CampaignUiApplication(client).handle("POST", "/api/project-runs/project-run-1/actions/cancel", {}, {})
    assert response.status == 200
    assert client.run_id == "project-run-1"
    assert response.body["value"]["status"] == "cancelled"
def test_a14_legacy_failure_code_backfills_stage_for_existing_project_runs() -> None:
    # Recover useful stage grouping from A3-era entries that persisted only a campaign_*_failed code.
    entry = ProjectRunEntry.from_dict({"source_path": "pkg/app.py", "campaign_id": "campaign-1", "launch_status": "rejected", "error_code": "campaign_baseline_failed"})
    assert entry.error_stage == "baseline"

def test_a14_http_security_gate_allows_project_run_cancel_route() -> None:
    # Keep the loopback state-changing allowlist aligned with the new ProjectRun cancellation route.
    class Client:
        def __init__(self):
            # Retain the cancellation identity observed through the real HTTP adapter.
            self.run_id = None
        def cancel_project_run(self, run_id):
            # Return one stable cancellation result through the minimal fake client.
            self.run_id = run_id
            return {"ok": True, "kind": "success", "value": {"run_id": run_id, "status": "cancelled"}}
    client = Client()
    running = start_ui_server(CampaignUiApplication(client), port=0, session_token="session-a14")
    connection = http.client.HTTPConnection("127.0.0.1", running.port, timeout=5)
    try:
        connection.request(
            "POST",
            "/api/project-runs/project-run-1/actions/cancel",
            body=b"{}",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Origin": f"http://127.0.0.1:{running.port}",
                "X-Theseus-Session": "session-a14",
            },
        )
        response = connection.getresponse()
        payload = json.loads(response.read())
    finally:
        connection.close()
        running.close()
    assert response.status == 200
    assert payload["value"]["status"] == "cancelled"
    assert client.run_id == "project-run-1"
