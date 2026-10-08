from __future__ import annotations
import ast
import http.client
import inspect
import json
from pathlib import Path
from theseus_ui import CampaignUiApplication, UI_MAX_REQUEST_BODY, start_ui_server
from theseus_ui.client import PublicApiCampaignClient
from test_e21_campaign_ui import FakeCampaignClient
def _request(port: int, method: str, path: str, *, body: bytes | None = None, headers: dict[str, str] | None = None):
    # Send one isolated loopback HTTP request and decode its complete response.
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        connection.request(method, path, body=body, headers=headers or {})
        response = connection.getresponse()
        content = response.read()
        return response.status, dict(response.getheaders()), content
    finally:
        connection.close()
def test_server_binds_only_to_loopback_and_serves_packaged_assets() -> None:
    # Start on an OS-selected port and expose only the exact static allowlist.
    running = start_ui_server(CampaignUiApplication(FakeCampaignClient()), port=0, session_token="session-test")
    try:
        assert running.server.server_address[0] == "127.0.0.1"
        status, headers, content = _request(running.port, "GET", "/")
        assert status == 200
        assert headers["Content-Security-Policy"].startswith("default-src 'self'")
        assert "Theseus — кампании".encode("utf-8") in content
        session_status, _, session_content = _request(running.port, "GET", "/api/session")
        assert session_status == 200
        assert json.loads(session_content)["value"]["session_token"] == "session-test"
        foreign_origin, _, _ = _request(running.port, "GET", "/api/projects?limit=1", headers={"Origin": "http://example.test"})
        assert foreign_origin == 403
        missing, _, _ = _request(running.port, "GET", "/assets/")
        traversal, _, _ = _request(running.port, "GET", "/assets/../client.py")
        assert missing == 404
        assert traversal == 404
    finally:
        running.close()
def test_state_changes_require_same_origin_json_and_session_token() -> None:
    # Reject cross-origin and unauthenticated writes before invoking campaign or recovery actions.
    client = FakeCampaignClient()
    running = start_ui_server(CampaignUiApplication(client), port=0, session_token="session-test")
    payload = json.dumps({"action_id": "action-1", "expected_revision": 7}).encode("utf-8")
    try:
        no_origin, _, _ = _request(
            running.port,
            "POST",
            "/api/campaigns/campaign-1/actions/reconcile",
            body=payload,
            headers={"Content-Type": "application/json", "X-Theseus-Session": "session-test"},
        )
        wrong_session, _, _ = _request(
            running.port,
            "POST",
            "/api/campaigns/campaign-1/actions/reconcile",
            body=payload,
            headers={"Content-Type": "application/json", "Origin": f"http://127.0.0.1:{running.port}", "X-Theseus-Session": "wrong"},
        )
        valid, _, content = _request(
            running.port,
            "POST",
            "/api/campaigns/campaign-1/actions/reconcile",
            body=payload,
            headers={"Content-Type": "application/json", "Origin": f"http://127.0.0.1:{running.port}", "X-Theseus-Session": "session-test"},
        )
        assert no_origin == 403
        assert wrong_session == 403
        assert valid == 200
        assert json.loads(content)["value"]["status"] == "running"
        assert client.action_calls == 1
    finally:
        running.close()
def test_request_body_is_bounded_before_json_parsing() -> None:
    # Reject an advertised oversized body without reading or dispatching it.
    running = start_ui_server(CampaignUiApplication(FakeCampaignClient()), port=0, session_token="session-test")
    try:
        status, _, content = _request(
            running.port,
            "POST",
            "/api/campaigns/campaign-1/actions/recover",
            body=b"{}",
            headers={"Content-Type": "application/json", "Content-Length": str(UI_MAX_REQUEST_BODY + 1), "Origin": f"http://127.0.0.1:{running.port}", "X-Theseus-Session": "session-test"},
        )
        assert status == 413
        assert json.loads(content)["error"]["code"] == "request_too_large"
    finally:
        running.close()
def test_recovery_reads_are_json_only_and_path_filtered() -> None:
    # Serialize recovery diagnostics through the same no-store privacy filter as campaign reads.
    running = start_ui_server(CampaignUiApplication(FakeCampaignClient()), port=0, session_token="session-test")
    try:
        status, headers, content = _request(running.port, "GET", "/api/campaigns/campaign-1/recovery-diagnostics?limit=10")
        decoded = json.loads(content)
        assert status == 200
        assert headers["Cache-Control"] == "no-store"
        serialized = json.dumps(decoded, sort_keys=True).lower()
        assert "workspace_path" not in serialized
        assert "spool_path" not in serialized
        assert "database_path" not in serialized
        assert "birth_token" not in serialized
    finally:
        running.close()
def test_production_client_source_uses_only_public_theseus_api() -> None:
    # Prevent direct SQLite, coordinator, mutation-store, statistics, knowledge or filesystem-state coupling.
    source = Path(inspect.getsourcefile(PublicApiCampaignClient)).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in (node.names if isinstance(node, ast.Import) else [ast.alias(node.module or "")])
    }
    lowered = source.lower()
    assert "theseus_api" in imports
    assert "sqlite3" not in imports
    assert not any(name.startswith("theseus_local") for name in imports)
    assert not any(name.startswith("gallifrey_mutation") for name in imports)
    assert not any(name.startswith("theseus_statistics") for name in imports)
    assert not any(name.startswith("theseus_knowledge") for name in imports)
    assert " offset " not in lowered
