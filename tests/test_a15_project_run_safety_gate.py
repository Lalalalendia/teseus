from __future__ import annotations
import sys
from pathlib import Path
from types import SimpleNamespace
import pytest
from test_intelligence_unified_v1.baseline_service import BaselineService
from test_intelligence_unified_v1.models import ProcessResult
from test_intelligence_unified_v1.runner import LevelSpec, MutationConfig, MutationRunner
from theseus_local.project_run import ProjectRunEntry, ProjectRunRecord
from theseus_local.workspace import cleanup_campaign_workspaces
from theseus_ui import LocalWorkspaceCampaignClient
from theseus_ui import local_control as local_control_module


def _component_project(root: Path) -> Path:
    # Build two explicit pytest components so baseline circuit breaking can remain component-scoped.
    for component in ("backend", "shadow_backend"):
        base = root / component
        (base / "tests").mkdir(parents=True)
        (base / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
        (base / "tests" / "test_app.py").write_text("def test_app():\n    assert True\n", encoding="utf-8")
        (base / "pytest.ini").write_text("[pytest]\ntestpaths = tests\npythonpath = .\n", encoding="utf-8")
    return root


def test_a15_project_baseline_key_is_shared_across_file_workspaces(tmp_path: Path) -> None:
    # Reuse one explicit project test command when only the file-scoped mutation target changes.
    reports = tmp_path / "reports"
    first_root = tmp_path / "workspace-a"
    second_root = tmp_path / "workspace-b"
    for root in (first_root, second_root):
        (root / "backend").mkdir(parents=True)
    command = (sys.executable, "-m", "pytest", "-q")
    first = MutationRunner(MutationConfig(project_root=first_root, source="backend/a.py", test_command_argv=command, test_command_cwd=first_root / "backend", reports_dir=reports))
    second = MutationRunner(MutationConfig(project_root=second_root, source="backend/b.py", test_command_argv=command, test_command_cwd=second_root / "backend", reports_dir=reports))
    level = LevelSpec("L1", "project command", command)
    selection = SimpleNamespace(index_version="index-shared", map_version="map-shared", snapshot_id="selection")
    snapshot_a = SimpleNamespace(original_sha256="source-a", text="VALUE = 1\n")
    snapshot_b = SimpleNamespace(original_sha256="source-b", text="VALUE = 2\n")
    assert first._baseline_key(level, snapshot_a, selection, None) == second._baseline_key(level, snapshot_b, selection, None)



def test_a15_successful_project_baseline_is_reused_across_file_campaigns(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Prove the shared key reaches the existing durable baseline cache instead of only matching in isolation.
    reports = tmp_path / "reports"
    command = (sys.executable, "-m", "pytest", "-q")
    calls: list[str] = []
    selection = SimpleNamespace(index_version="index-shared", map_version="map-shared", snapshot_id="selection")
    level = LevelSpec("L1", "project command", command)
    def build_runner(name: str, source: str) -> MutationRunner:
        # Build one file-campaign runner sharing only the project report cache and relative pytest cwd.
        root = tmp_path / name
        (root / "backend").mkdir(parents=True)
        runner = MutationRunner(MutationConfig(project_root=root, source=source, test_command_argv=command, test_command_cwd=root / "backend", reports_dir=reports))
        runner._load_cache()
        def fake_run_test_command(argv, **kwargs):
            # Materialize the baseline artifact exactly like the physical execution boundary would.
            calls.append(source)
            output = Path(kwargs["output_artifact"])
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text("passed\n", encoding="utf-8")
            return ProcessResult(tuple(argv), str(runner.test_cwd), 0, 0.01, False, "")
        monkeypatch.setattr(runner, "_run_test_command", fake_run_test_command)
        return runner
    first = build_runner("workspace-a", "backend/a.py")
    first_rows = BaselineService(first).run((level,), SimpleNamespace(original_sha256="source-a", text="A"), selection, None, "run-a")
    second = build_runner("workspace-b", "backend/b.py")
    second_rows = BaselineService(second).run((level,), SimpleNamespace(original_sha256="source-b", text="B"), selection, None, "run-b")
    assert calls == ["backend/a.py"]
    assert first_rows[0]["baseline_reused"] is False
    assert second_rows[0]["baseline_reused"] is True
    assert second.performance.baseline_cache_hits == 1

def test_a15_project_baseline_key_is_component_scoped(tmp_path: Path) -> None:
    # Keep otherwise identical project commands distinct when pytest runs from different component directories.
    root = tmp_path / "workspace"
    (root / "backend").mkdir(parents=True)
    (root / "shadow_backend").mkdir()
    command = (sys.executable, "-m", "pytest", "-q")
    backend = MutationRunner(MutationConfig(project_root=root, source="backend/a.py", test_command_argv=command, test_command_cwd=root / "backend", reports_dir=tmp_path / "reports"))
    shadow = MutationRunner(MutationConfig(project_root=root, source="shadow_backend/a.py", test_command_argv=command, test_command_cwd=root / "shadow_backend", reports_dir=tmp_path / "reports"))
    level = LevelSpec("L1", "project command", command)
    selection = SimpleNamespace(index_version="index-shared", map_version="map-shared", snapshot_id="selection")
    snapshot = SimpleNamespace(original_sha256="source", text="VALUE = 1\n")
    assert backend._baseline_key(level, snapshot, selection, None) != shadow._baseline_key(level, snapshot, selection, None)


def test_a15_baseline_failure_blocks_only_matching_component(tmp_path: Path) -> None:
    # Reject queued siblings after one baseline failure without suppressing an independent pytest component.
    project = _component_project(tmp_path / "project")
    client = LocalWorkspaceCampaignClient(tmp_path / "ui-state")
    project_id = client.register_project(str(project))["value"]["project_id"]
    failed = ProjectRunEntry("backend/app.py", "campaign-backend-1", "failed", "campaign_baseline_failed", 0, 0, "baseline", "2026-08-15T00:00:00+00:00")
    run = ProjectRunRecord(
        "project-run-a15-baseline",
        project_id,
        (
            failed,
            ProjectRunEntry("backend/other.py", "campaign-backend-2", "queued"),
            ProjectRunEntry("shadow_backend/app.py", "campaign-shadow-1", "queued"),
        ),
    )
    client.registry.register_project_run(run)
    updated = client._apply_project_run_failure_gate(run, failed)
    assert updated.entries[1].launch_status == "rejected"
    assert updated.entries[1].error_code == "project_baseline_blocked"
    assert updated.entries[1].error_stage == "baseline"
    assert updated.entries[2].launch_status == "queued"


def test_a15_cleanup_reclaims_project_copies_but_preserves_durable_evidence(tmp_path: Path) -> None:
    # Delete only heavy campaign and attempt workspaces while retaining reports, registry and durable spool evidence.
    reports = tmp_path / "private" / "reports"
    state = reports.parent / "state"
    campaign_id = "campaign-a15"
    campaign_workspace = state / "workspaces" / campaign_id
    campaign_workspace.mkdir(parents=True)
    (campaign_workspace / "project.bin").write_bytes(b"x" * 1024)
    (campaign_workspace.parent / f"{campaign_id}.ownership.json").write_text("{}\n", encoding="utf-8")
    worker = state / "workers" / campaign_id / "worker-000"
    attempt = worker / "attempt-0-instance"
    workspace = attempt / "workspace"
    workspace.mkdir(parents=True)
    (workspace / "project.bin").write_bytes(b"x" * 2048)
    (attempt / "workspace.ownership.json").write_text("{}\n", encoding="utf-8")
    spool = worker / "spool" / "delivery.json"
    spool.parent.mkdir(parents=True)
    spool.write_text("{}\n", encoding="utf-8")
    diagnostic = attempt / "reports" / "diagnostic.json"
    diagnostic.parent.mkdir(parents=True)
    diagnostic.write_text("{}\n", encoding="utf-8")
    cleanup = cleanup_campaign_workspaces(reports, campaign_id)
    assert cleanup["reclaimed_bytes"] >= 3072
    assert not campaign_workspace.exists()
    assert not (campaign_workspace.parent / f"{campaign_id}.ownership.json").exists()
    assert not workspace.exists()
    assert not (attempt / "workspace.ownership.json").exists()
    assert spool.is_file()
    assert diagnostic.is_file()


def test_a15_project_run_refuses_to_start_below_disk_reserve(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Fail before creating hundreds of child campaigns when the state volume cannot preserve the safety reserve.
    project = tmp_path / "project"
    project.mkdir()
    (project / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
    client = LocalWorkspaceCampaignClient(tmp_path / "ui-state")
    project_id = client.register_project(str(project))["value"]["project_id"]
    monkeypatch.setattr(local_control_module.shutil, "disk_usage", lambda _path: SimpleNamespace(total=10_000, used=9_900, free=100))
    result = client.create_project_run({"project_id": project_id})
    assert result["ok"] is False
    assert result["error"]["code"] == "project_run_disk_budget_exceeded"
    details = result["error"]["details"]
    assert details["free_bytes"] == 100
    assert details["required_free_bytes"] > details["free_bytes"]
    assert client.registry.project_runs(project_id) == ()


def test_a15_browser_names_storage_and_baseline_safety_failures() -> None:
    # Keep new safety gates operator-readable instead of exposing only internal error codes.
    from theseus_ui import asset_bytes
    script = asset_bytes("app.js").decode("utf-8")
    assert 'project_baseline_blocked: "Файл пропущен: общий baseline компонента не прошёл"' in script
    assert 'project_run_disk_budget_exceeded: "Недостаточно свободного места для безопасного запуска"' in script
    assert 'workspace_cleaned: "Временная рабочая копия очищена"' in script


def test_a15_dispatcher_stops_repeating_failed_component_baseline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Turn one observed baseline failure into a component-wide gate before another child campaign is launched.
    project = _component_project(tmp_path / "project")
    client = LocalWorkspaceCampaignClient(tmp_path / "ui-state")
    project_id = client.register_project(str(project))["value"]["project_id"]
    run = ProjectRunRecord(
        "project-run-a15-dispatch",
        project_id,
        (
            ProjectRunEntry("backend/app.py", "campaign-backend-1", "running"),
            ProjectRunEntry("backend/other.py", "campaign-backend-2", "queued"),
        ),
    )
    client.registry.register_project_run(run)
    monkeypatch.setattr(client, "_campaign_state", lambda _campaign_id: None)
    monkeypatch.setattr(client, "_campaign_attempt_failure", lambda _campaign_id: {"code": "campaign_baseline_failed", "stage": "baseline"})
    monkeypatch.setattr(client, "_campaign_attempt_failure_code", lambda _campaign_id: None)
    monkeypatch.setattr(client, "_cleanup_project_run_entry", lambda _run, _entry: True)
    launched: list[int] = []
    monkeypatch.setattr(client, "_launch_project_run_entry", lambda _run, index: (launched.append(index), False))
    monkeypatch.setattr(client, "_resume_project_run_dispatchers", lambda: None)
    client._dispatch_project_run(run.run_id)
    stored = client.registry.project_run(run.run_id)
    assert stored is not None
    assert stored.entries[0].error_code == "campaign_baseline_failed"
    assert stored.entries[1].error_code == "project_baseline_blocked"
    assert launched == []
