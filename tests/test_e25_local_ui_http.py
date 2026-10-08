from __future__ import annotations

import http.client
import json
from pathlib import Path

from theseus_api import ApiSuccess
from theseus_ui import CampaignUiApplication, LocalWorkspaceCampaignClient, start_ui_server


class _FakeActions:
    def __init__(self, configuration):
        self.configuration = configuration

    def create_campaign(self, action_id, configuration):
        return ApiSuccess({"action_id": action_id, "campaign_id": configuration.campaign_id.value, "status": "completed"})

    def start_campaign(self, action_id, campaign_id, *, expected_revision):
        return ApiSuccess({"action_id": action_id, "campaign_id": campaign_id, "status": "running", "campaign_revision": expected_revision})


def _request(port: int, method: str, path: str, *, body: object | None = None):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        payload = json.dumps(body).encode("utf-8") if body is not None else None
        connection.request(
            method,
            path,
            body=payload,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Origin": f"http://127.0.0.1:{port}",
                "X-Theseus-Session": "session-test",
            },
        )
        response = connection.getresponse()
        return response.status, json.loads(response.read())
    finally:
        connection.close()


def test_local_server_supports_project_registration_and_campaign_launch(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    (project / "app.py").write_text("def choose():\n    return True\n", encoding="utf-8")
    client = LocalWorkspaceCampaignClient(
        tmp_path / "ui-state",
        actions_factory=lambda configuration: _FakeActions(configuration),
    )
    running = start_ui_server(
        CampaignUiApplication(client),
        port=0,
        session_token="session-test",
    )
    try:
        project_status, project_result = _request(
            running.port,
            "POST",
            "/api/projects",
            body={"project_root": str(project)},
        )
        assert project_status == 200
        project_id = project_result["value"]["project_id"]
        campaign_status, campaign_result = _request(
            running.port,
            "POST",
            "/api/campaigns",
            body={
                "project_id": project_id,
                "source_path": "app.py",
                "scope_kind": "file",
                "operators": [],
                "max_mutants": 1,
                "max_workers": 1,
                "max_seconds": 10,
                "max_test_seconds": 5,
                "reuse_mode": "hint",
            },
        )
        assert campaign_status == 200
        assert campaign_result["value"]["status"] == "running"
        project_run_status, project_run_result = _request(
            running.port,
            "POST",
            "/api/project-runs",
            body={"project_id": project_id},
        )
        assert project_run_status == 200
        assert project_run_result["value"]["campaign_count"] == 1
    finally:
        running.close()

