from __future__ import annotations

from pathlib import Path

from theseus_api import ApiSuccess
from theseus_ui import LocalWorkspaceCampaignClient


class _FakeActions:
    def __init__(self, configuration):
        self.configuration = configuration

    def create_campaign(self, action_id, configuration):
        return ApiSuccess({"action_id": action_id, "campaign_id": configuration.campaign_id.value, "status": "completed", "campaign_revision": 0})

    def start_campaign(self, action_id, campaign_id, *, expected_revision):
        return ApiSuccess({"action_id": action_id, "campaign_id": campaign_id, "status": "running", "campaign_revision": expected_revision})


def test_local_ui_registers_project_and_keeps_registry_across_restart(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    source = project / "app.py"
    source.write_text("def choose():\n    return True\n", encoding="utf-8")
    state = tmp_path / "ui-state"
    client = LocalWorkspaceCampaignClient(
        state,
        actions_factory=lambda configuration: _FakeActions(configuration),
    )
    registered = client.register_project(str(project), "Browser project")
    assert registered["ok"] is True
    projects = client.list_projects(limit=10)
    assert projects["value"]["items"][0]["display_name"] == "Browser project"
    project_id = projects["value"]["items"][0]["project_id"]
    created = client.create_campaign(
        {
            "project_id": project_id,
            "source_path": "app.py",
            "scope_kind": "file",
            "operators": [],
            "max_mutants": 1,
            "max_workers": 1,
            "max_seconds": 10,
            "max_test_seconds": 5,
            "reuse_mode": "hint",
        }
    )
    assert created["ok"] is True
    assert created["value"]["status"] == "running"
    restarted = LocalWorkspaceCampaignClient(state, actions_factory=lambda configuration: _FakeActions(configuration))
    assert restarted.list_projects(limit=10)["value"]["items"][0]["campaign_count"] == 1

