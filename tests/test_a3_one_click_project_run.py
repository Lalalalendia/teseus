from __future__ import annotations
import json
import sys
from dataclasses import replace
from pathlib import Path
from theseus_api import ApiSuccess
from theseus_local import ProjectProfile, discover_project, project_source_files
from theseus_ui import CampaignUiApplication, LocalWorkspaceCampaignClient
from theseus_ui.local_control import LocalUiRegistry
class _FakeActions:
    def __init__(self, configuration):
        # Retain the exact campaign configuration emitted by the project-run authority.
        self.configuration = configuration
    def create_campaign(self, action_id, configuration):
        # Simulate the existing durable create action without executing the mutation engine.
        return ApiSuccess({"action_id": action_id, "campaign_id": configuration.campaign_id.value, "status": "completed", "campaign_revision": 0})
    def start_campaign(self, action_id, campaign_id, *, expected_revision):
        # Simulate the detached coordinator launch accepted by the existing action boundary.
        return ApiSuccess({"action_id": action_id, "campaign_id": campaign_id, "status": "running", "campaign_revision": expected_revision})
def _project(tmp_path: Path) -> tuple[Path, Path]:
    # Build one src-layout project containing production, test, and excluded Python files.
    project = tmp_path / "project"
    (project / "src" / "sample").mkdir(parents=True)
    (project / "tests").mkdir()
    (project / ".venv" / ("Scripts" if sys.platform == "win32" else "bin")).mkdir(parents=True)
    (project / "src" / "sample" / "__init__.py").write_text("", encoding="utf-8")
    (project / "src" / "sample" / "app.py").write_text("def choose():\n    return True\n", encoding="utf-8")
    (project / "src" / "sample" / "helper.py").write_text("VALUE = 1\n", encoding="utf-8")
    (project / "src" / "sample" / "test_embedded.py").write_text("def test_noop():\n    assert True\n", encoding="utf-8")
    (project / "tests" / "test_app.py").write_text("def test_choose():\n    assert True\n", encoding="utf-8")
    (project / "pyproject.toml").write_text('[tool.pytest.ini_options]\ntestpaths = ["tests"]\n', encoding="utf-8")
    interpreter = project / ".venv" / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
    interpreter.write_text("", encoding="utf-8")
    if sys.platform != "win32":
        interpreter.chmod(interpreter.stat().st_mode | 0o111)
    return project, interpreter.resolve()
def test_a3_project_run_discovers_production_files_and_launches_existing_file_campaigns(tmp_path: Path) -> None:
    # Expand one click into deterministic file campaigns while reusing the saved project profile.
    project, interpreter = _project(tmp_path)
    captured = []
    def actions_factory(configuration):
        # Capture every existing campaign contract created by the project-level meta run.
        captured.append(configuration)
        return _FakeActions(configuration)
    state_dir = tmp_path / "ui-state"
    client = LocalWorkspaceCampaignClient(state_dir, actions_factory=actions_factory)
    project_id = client.register_project(str(project), "One click")["value"]["project_id"]
    launched = client.create_project_run({"project_id": project_id})
    assert launched["ok"] is True
    value = launched["value"]
    assert value["status"] == "running"
    assert value["source_count"] == 3
    assert value["campaign_count"] == 3
    assert value["launch_failures"] == 0
    assert [item.scope.source_path for item in captured] == ["src/sample/__init__.py", "src/sample/app.py", "src/sample/helper.py"]
    assert all(item.scope.scope_kind == "file" for item in captured)
    assert all(item.project.test_command.argv == (str(interpreter), "-m", "pytest", "-q") for item in captured)
    assert all(item.budget.max_mutants == 100 for item in captured)
    assert all(item.reports_dir == str((state_dir / "projects" / project_id / "reports").resolve()) for item in captured)
def test_a3_project_run_is_persisted_and_visible_after_ui_restart(tmp_path: Path) -> None:
    # Persist the meta-run membership so a restarted UI can still show the one-click launch.
    project, _interpreter = _project(tmp_path)
    state_dir = tmp_path / "ui-state"
    client = LocalWorkspaceCampaignClient(state_dir, actions_factory=lambda configuration: _FakeActions(configuration))
    project_id = client.register_project(str(project))["value"]["project_id"]
    created = client.create_project_run({"project_id": project_id})["value"]
    restarted = LocalWorkspaceCampaignClient(state_dir, actions_factory=lambda configuration: _FakeActions(configuration))
    runs = restarted.list_project_runs(limit=10, project_id=project_id)
    detail = restarted.get_project_run(created["run_id"])
    assert runs["ok"] is True
    assert runs["value"]["items"][0]["run_id"] == created["run_id"]
    assert runs["value"]["items"][0]["source_count"] == 3
    assert detail["ok"] is True
    assert len(detail["value"]["campaigns"]) == 3
    persisted = json.loads((state_dir / "registry.json").read_text(encoding="utf-8"))
    assert persisted["project_runs"][0]["run_id"] == created["run_id"]
    assert len(persisted["project_runs"][0]["entries"]) == 3
def test_a3_http_project_run_route_requires_only_registered_project_id(tmp_path: Path) -> None:
    # Accept one project-only launch request without requiring source_path or campaign internals.
    project, _interpreter = _project(tmp_path)
    client = LocalWorkspaceCampaignClient(tmp_path / "ui-state", actions_factory=lambda configuration: _FakeActions(configuration))
    project_id = client.register_project(str(project))["value"]["project_id"]
    app = CampaignUiApplication(client)
    response = app.handle("POST", "/api/project-runs", {}, {"project_id": project_id})
    listed = app.handle("GET", "/api/project-runs", {"limit": ["10"], "project_id": [project_id]})
    assert response.status == 200
    assert response.body["ok"] is True
    assert response.body["value"]["campaign_count"] == 3
    assert listed.status == 200
    assert listed.body["value"]["items"][0]["run_id"] == response.body["value"]["run_id"]
def test_a3_project_run_rejects_project_without_production_sources(tmp_path: Path) -> None:
    # Refuse a one-click run when discovery cannot identify any production Python file.
    project = tmp_path / "tests-only"
    (project / "tests").mkdir(parents=True)
    (project / "tests" / "test_only.py").write_text("def test_only():\n    assert True\n", encoding="utf-8")
    client = LocalWorkspaceCampaignClient(tmp_path / "ui-state", actions_factory=lambda configuration: _FakeActions(configuration))
    project_id = client.register_project(str(project))["value"]["project_id"]
    result = client.create_project_run({"project_id": project_id})
    assert result["ok"] is False
    assert result["error"]["code"] == "project_run_no_sources"
def test_a3_packaged_ui_exposes_one_click_project_run_without_source_path() -> None:
    # Keep the browser primary action project-scoped while preserving manual campaign controls separately.
    from theseus_ui import asset_bytes
    html = asset_bytes("index.html").decode("utf-8")
    script = asset_bytes("app.js").decode("utf-8")
    assert 'id="project-run-form"' in html
    assert 'id="project-run-button"' in html
    assert ">Запустить проект<" in html
    assert "Ручная кампания" in html
    assert 'requestJson("/api/project-runs"' in script
    project_run_submit = script.split("async function submitProjectRun", 1)[1].split("async function submitCreateCampaign", 1)[0]
    assert "source_path" not in project_run_submit
def test_a3_project_source_files_rejects_generated_pytest_roots_from_stale_profile(tmp_path: Path) -> None:
    # Keep one-click execution safe even when an older in-memory profile still names a generated pytest directory.
    project, _interpreter = _project(tmp_path)
    generated = project / ".pytest-pr73-suite"
    generated.mkdir()
    (generated / "leaked.py").write_text("VALUE = 1\n", encoding="utf-8")
    profile = ProjectProfile.from_discovery(discover_project(project))
    stale_profile = replace(profile, source_roots=(*profile.source_roots, ".pytest-pr73-suite"))
    sources = project_source_files(project, stale_profile)
    assert ".pytest-pr73-suite/leaked.py" not in sources
    assert sources == ("src/sample/__init__.py", "src/sample/app.py", "src/sample/helper.py")
def test_a3_large_project_run_is_persisted_before_bounded_dispatch(tmp_path: Path, monkeypatch) -> None:
    # Queue large project runs durably instead of launching every file campaign in one HTTP request.
    project = tmp_path / "large-project"
    project.mkdir()
    for index in range(9):
        (project / f"module_{index}.py").write_text(f"VALUE = {index}\n", encoding="utf-8")
    captured = []
    def actions_factory(configuration):
        # Record any accidental inline launch so the test proves the large-run boundary stays queued.
        captured.append(configuration)
        return _FakeActions(configuration)
    client = LocalWorkspaceCampaignClient(tmp_path / "ui-state", actions_factory=actions_factory)
    project_id = client.register_project(str(project))["value"]["project_id"]
    dispatched = []
    monkeypatch.setattr(client, "_start_project_run_dispatcher", lambda run_id: dispatched.append(run_id))
    result = client.create_project_run({"project_id": project_id})
    assert result["ok"] is True
    assert result["value"]["source_count"] == 9
    assert result["value"]["status"] == "queued"
    assert captured == []
    assert dispatched == [result["value"]["run_id"]]
    persisted = client.registry.project_run(result["value"]["run_id"])
    assert persisted is not None
    assert {entry.launch_status for entry in persisted.entries} == {"queued"}
def test_a3_project_run_reports_source_enumeration_failure_without_generic_mask(tmp_path: Path, monkeypatch) -> None:
    # Preserve the failing ProjectRun stage without inventing a browser-specific source-count rejection.
    from theseus_ui import local_control as local_control_module
    project, _interpreter = _project(tmp_path)
    client = LocalWorkspaceCampaignClient(tmp_path / "ui-state", actions_factory=lambda configuration: _FakeActions(configuration))
    project_id = client.register_project(str(project))["value"]["project_id"]
    monkeypatch.setattr(local_control_module, "project_source_files", lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("source inventory is invalid")))
    result = client.create_project_run({"project_id": project_id})
    assert result["ok"] is False
    assert result["error"]["code"] == "project_run_discovery_failed"
    assert result["error"]["message"] == "source inventory is invalid"
def test_a3_project_run_accepts_more_than_legacy_4096_sources(tmp_path: Path, monkeypatch) -> None:
    # Prove the one-click queue accepts a large real-project inventory beyond the former browser cap.
    from theseus_ui import local_control as local_control_module
    project, _interpreter = _project(tmp_path)
    client = LocalWorkspaceCampaignClient(tmp_path / "ui-state", actions_factory=lambda configuration: _FakeActions(configuration))
    project_id = client.register_project(str(project))["value"]["project_id"]
    sources = tuple(f"pkg/module_{index:05d}.py" for index in range(5000))
    monkeypatch.setattr(local_control_module, "project_source_files", lambda *_args, **_kwargs: sources)
    dispatched = []
    monkeypatch.setattr(client, "_start_project_run_dispatcher", lambda run_id: dispatched.append(run_id))
    result = client.create_project_run({"project_id": project_id})
    assert result["ok"] is True
    assert result["value"]["source_count"] == 5000
    assert dispatched == [result["value"]["run_id"]]
    restored = LocalUiRegistry(tmp_path / "ui-state").project_run(result["value"]["run_id"])
    assert restored is not None
    assert len(restored.entries) == 5000
def test_a3_campaign_registry_does_not_rewrite_unchanged_state(tmp_path: Path, monkeypatch) -> None:
    # Avoid full registry JSON rewrites during every project-run polling cycle when discovery found nothing new.
    registry = LocalUiRegistry(tmp_path / "ui-state")
    project, _interpreter = _project(tmp_path)
    registry.register_project(project)
    saves = []
    monkeypatch.setattr(registry, "_save", lambda: saves.append(True))
    first = registry.campaigns()
    second = registry.campaigns()
    assert first == second == ()
    assert saves == []
