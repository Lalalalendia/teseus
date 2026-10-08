from __future__ import annotations

import json
from pathlib import Path

from theseus_local.project_run import ProjectRunEntry, ProjectRunRecord
from theseus_ui.local_control import LocalWorkspaceCampaignClient


def test_nonterminal_failed_attempt_has_stable_stage_code(tmp_path: Path) -> None:
    # Convert a resumable coordinator failure into a project-run stage code without exposing raw error text.
    project = tmp_path / "project"
    project.mkdir()
    (project / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
    client = LocalWorkspaceCampaignClient(tmp_path / "ui-state")
    project_id = client.register_project(str(project))["value"]["project_id"]
    campaign_id = "campaign-stage-failure"
    database = tmp_path / "campaign-state" / campaign_id / "campaign.sqlite3"
    database.parent.mkdir(parents=True)
    database.touch()
    client.registry.register_campaign(project_id, campaign_id, database)
    (database.parent / "coordinator.performance.json").write_text(
        json.dumps(
            {
                "status": "preparing",
                "error": "private checkout detail",
                "phases": [
                    {"phase": "campaign_preparation", "wall_seconds": 1.0},
                    {"phase": "test_collection", "wall_seconds": 2.0},
                    {"phase": "cleanup", "wall_seconds": 0.1},
                ],
            }
        ),
        encoding="utf-8",
    )
    assert client._campaign_attempt_failure_code(campaign_id) == "campaign_test_collection_failed"


def test_dispatcher_marks_failed_attempt_and_can_reach_next_queued_entry(tmp_path: Path, monkeypatch) -> None:
    # Stop one failed resumable child from blocking the complete project-run queue forever.
    project = tmp_path / "project"
    project.mkdir()
    (project / "a.py").write_text("A = 1\n", encoding="utf-8")
    (project / "b.py").write_text("B = 1\n", encoding="utf-8")
    client = LocalWorkspaceCampaignClient(tmp_path / "ui-state")
    project_id = client.register_project(str(project))["value"]["project_id"]
    run = ProjectRunRecord(
        "project-run-test",
        project_id,
        (
            ProjectRunEntry("a.py", "campaign-a", "running"),
            ProjectRunEntry("b.py", "campaign-b", "queued"),
        ),
    )
    client.registry.register_project_run(run)
    launched: list[int] = []
    monkeypatch.setattr(client, "_campaign_terminal", lambda campaign_id: False if campaign_id == "campaign-a" else True)
    monkeypatch.setattr(client, "_campaign_attempt_failure_code", lambda campaign_id: "campaign_test_collection_failed" if campaign_id == "campaign-a" else None)
    def launch(current: ProjectRunRecord, index: int):
        # Record the next queue position and consume it without starting a process.
        launched.append(index)
        entry = current.entries[index]
        updated = client._update_project_run_entry(current, index, ProjectRunEntry(entry.source_path, entry.campaign_id, "rejected", "synthetic"), force_persist=True)
        return updated, False
    monkeypatch.setattr(client, "_launch_project_run_entry", launch)
    client._dispatch_project_run(run.run_id)
    saved = client.registry.project_run(run.run_id)
    assert saved is not None
    assert saved.entries[0].error_code == "campaign_test_collection_failed"
    assert launched == [1]


def test_restart_reconciles_legacy_queued_sources_against_refreshed_profile(tmp_path: Path) -> None:
    # Remove stale root-tooling entries while preserving queued sources owned by current pytest components.
    project = tmp_path / "project"
    (project / "backend" / "app").mkdir(parents=True)
    (project / "backend" / "app" / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
    (project / "backend" / "tests").mkdir()
    (project / "backend" / "tests" / "test_module.py").write_text("def test_value():\n    assert True\n", encoding="utf-8")
    (project / "backend" / "pytest.ini").write_text("[pytest]\npythonpath = .\n", encoding="utf-8")
    (project / "dump.py").write_text("VALUE = 1\n", encoding="utf-8")
    client = LocalWorkspaceCampaignClient(tmp_path / "ui-state")
    project_id = client.register_project(str(project))["value"]["project_id"]
    run = ProjectRunRecord(
        "project-run-legacy-sources",
        project_id,
        (
            ProjectRunEntry("dump.py", "campaign-tool", "queued"),
            ProjectRunEntry("backend/app/module.py", "campaign-backend", "queued"),
        ),
    )
    client.registry.register_project_run(run)
    reconciled = client._reconcile_project_run_sources(run)
    assert reconciled.entries[0].error_code == "source_outside_current_profile"
    assert reconciled.entries[1].launch_status == "queued"
