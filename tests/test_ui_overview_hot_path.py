from __future__ import annotations

from pathlib import Path

import theseus_ui.local_control as local_control_module
from theseus_local.project_run import ProjectRunEntry, ProjectRunRecord
from theseus_ui import asset_bytes
from theseus_ui.local_control import LocalUiRegistry, LocalWorkspaceCampaignClient, RegisteredCampaign


def _project(tmp_path: Path) -> Path:
    # Build one minimal registered Python checkout without expensive runtime execution.
    project = tmp_path / "project"
    project.mkdir()
    (project / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
    return project


def test_project_overview_uses_registry_snapshot_without_discovery_or_campaign_scan(tmp_path: Path, monkeypatch) -> None:
    # Keep the project list O(number of registered projects) and independent from campaign/database discovery.
    client = LocalWorkspaceCampaignClient(tmp_path / "ui-state")
    project_id = client.register_project(str(_project(tmp_path)))["value"]["project_id"]
    monkeypatch.setattr(client.registry, "campaigns", lambda: (_ for _ in ()).throw(AssertionError("campaign scan used")))
    monkeypatch.setattr(client.registry, "project_profile", lambda _project_id: (_ for _ in ()).throw(AssertionError("profile rediscovery used")))

    result = client.list_projects(limit=10)

    assert result["ok"] is True
    assert result["value"]["items"][0]["project_id"] == project_id


def test_campaign_overview_pages_bindings_before_opening_sqlite(tmp_path: Path, monkeypatch) -> None:
    # Open at most one SQLite database per visible campaign instead of every historical campaign before pagination.
    client = LocalWorkspaceCampaignClient(tmp_path / "ui-state")
    project_id = client.register_project(str(_project(tmp_path)))["value"]["project_id"]
    with client.registry._lock:
        client.registry._campaigns = {
            f"campaign-{index:05d}": RegisteredCampaign(
                f"campaign-{index:05d}",
                project_id,
                tmp_path / "db" / f"campaign-{index:05d}.sqlite3",
            )
            for index in range(5000)
        }
    opened: list[str] = []

    class _FakeCampaignClient:
        def __init__(self, database_path: Path) -> None:
            # Retain the selected database identity so the test can count physical reads.
            self.database_path = Path(database_path)

        def list_campaigns(self, *, limit: int, project_id: str | None = None):
            # Return one compact row for exactly the database selected after registry pagination.
            campaign_id = self.database_path.stem
            opened.append(campaign_id)
            return {
                "ok": True,
                "kind": "success",
                "value": {
                    "items": [
                        {
                            "campaign_id": campaign_id,
                            "project_id": project_id,
                            "status": "complete",
                            "revision_number": 1,
                            "completed_mutants": 1,
                            "total_mutants": 1,
                            "mutation_score": 100.0,
                            "last_activity": None,
                        }
                    ]
                },
            }

    monkeypatch.setattr(local_control_module, "PublicApiCampaignClient", _FakeCampaignClient)
    monkeypatch.setattr(client.registry, "campaigns", lambda: (_ for _ in ()).throw(AssertionError("campaign rescan used")))

    first = client.list_campaigns(limit=5, project_id=project_id)

    assert first["ok"] is True
    assert len(first["value"]["items"]) == 5
    assert len(opened) == 5
    assert first["value"]["next_cursor"] is not None


def test_project_run_overview_refreshes_only_bounded_active_children(tmp_path: Path, monkeypatch) -> None:
    # Keep a 5000-file project run cheap by reading only active children and using persisted state for queued entries.
    client = LocalWorkspaceCampaignClient(tmp_path / "ui-state")
    project_id = client.register_project(str(_project(tmp_path)))["value"]["project_id"]
    entries = (ProjectRunEntry("app.py", "campaign-active", "running"),) + tuple(
        ProjectRunEntry(f"pkg/module_{index:05d}.py", f"campaign-{index:05d}", "queued")
        for index in range(4999)
    )
    run = ProjectRunRecord("project-run-large", project_id, entries)
    with client.registry._lock:
        client.registry._project_runs[run.run_id] = run
        client.registry._campaigns["campaign-active"] = RegisteredCampaign(
            "campaign-active", project_id, tmp_path / "campaign-active.sqlite3"
        )
    opened: list[str] = []

    class _FakeCampaignClient:
        def __init__(self, database_path: Path) -> None:
            # Retain the selected active database path for bounded-read assertions.
            self.database_path = Path(database_path)

        def list_campaigns(self, *, limit: int, project_id: str | None = None):
            # Return one active projection without touching any queued child database.
            opened.append(self.database_path.stem)
            return {
                "ok": True,
                "kind": "success",
                "value": {
                    "items": [
                        {
                            "campaign_id": "campaign-active",
                            "project_id": project_id,
                            "status": "running",
                            "revision_number": 1,
                            "completed_mutants": 2,
                            "total_mutants": 10,
                            "mutation_score": None,
                            "last_activity": None,
                        }
                    ]
                },
            }

    monkeypatch.setattr(local_control_module, "PublicApiCampaignClient", _FakeCampaignClient)

    result = client.list_project_runs(limit=1, project_id=project_id)

    assert result["ok"] is True
    assert result["value"]["items"][0]["source_count"] == 5000
    assert result["value"]["items"][0]["completed_mutants"] == 2
    assert opened == ["campaign-active"]


def test_dispatcher_does_not_reopen_terminal_children_on_every_poll(tmp_path: Path, monkeypatch) -> None:
    # Persist terminal child status so dispatcher cost stays constant instead of growing with completed file count.
    client = LocalWorkspaceCampaignClient(tmp_path / "ui-state")
    project_id = client.register_project(str(_project(tmp_path)))["value"]["project_id"]
    entries = tuple(
        ProjectRunEntry(f"pkg/done_{index:03d}.py", f"campaign-done-{index:03d}", "complete", None, 1, 1)
        for index in range(100)
    ) + (
        ProjectRunEntry("app.py", "campaign-active", "running"),
        ProjectRunEntry("next.py", "campaign-next", "queued"),
    )
    run = ProjectRunRecord("project-run-dispatch", project_id, entries)
    with client.registry._lock:
        client.registry._project_runs[run.run_id] = run
    states: list[str] = []

    def campaign_state(campaign_id: str):
        # Return terminal state for the sole active child and record how many campaign reads dispatcher performs.
        states.append(campaign_id)
        return {"campaign_id": campaign_id, "status": "complete", "completed_mutants": 3, "total_mutants": 3}

    def launch(current: ProjectRunRecord, index: int):
        # Consume the queued child without creating a subprocess so the dispatcher can terminate deterministically.
        entry = current.entries[index]
        rejected = ProjectRunEntry(entry.source_path, entry.campaign_id, "rejected", "synthetic")
        return client._update_project_run_entry(current, index, rejected, force_persist=False), False

    monkeypatch.setattr(client, "_campaign_state", campaign_state)
    monkeypatch.setattr(client, "_campaign_attempt_failure_code", lambda _campaign_id: None)
    monkeypatch.setattr(client, "_launch_project_run_entry", launch)
    monkeypatch.setattr(client, "_resume_project_run_dispatchers", lambda: None)

    client._dispatch_project_run(run.run_id)

    saved = client.registry.project_run(run.run_id)
    assert saved is not None
    assert saved.entries[100].launch_status == "complete"
    assert saved.entries[100].completed_mutants == 3
    assert states == ["campaign-active"]


def test_browser_overview_renders_projects_without_waiting_for_campaign_reads() -> None:
    # Keep slow campaign and project-run requests from blocking the primary registered-project selector.
    script = asset_bytes("app.js").decode("utf-8")
    block = script.split("async function loadOverview()", 1)[1].split("function renderProjects", 1)[0]
    assert "await loadOverviewProjects();" in block
    assert "void loadOverviewCampaigns();" in block
    assert "void loadOverviewProjectRuns();" in block
    assert "Promise.all([" not in block
