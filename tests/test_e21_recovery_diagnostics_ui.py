from __future__ import annotations
import inspect
from pathlib import Path
from theseus_ui import CampaignUiApplication, asset_bytes
from theseus_ui.client import CampaignUiClient, PublicApiCampaignClient
from theseus_ui.serialization import to_json_value
from test_e21_campaign_ui import FakeCampaignClient
def test_recovery_client_protocol_exposes_all_bounded_reads() -> None:
    # Keep every recovery collection explicitly bounded and cursor-aware at the UI boundary.
    methods = (
        "get_recovery_diagnostics",
        "inspect_quarantine",
        "get_artifact_registry",
        "get_test_statistics",
        "get_reuse_evidence",
        "get_recovery_action",
        "reconcile_campaign",
        "recover_campaign",
    )
    for name in methods:
        assert hasattr(CampaignUiClient, name)
    for name in ("inspect_quarantine", "get_artifact_registry", "get_test_statistics", "get_reuse_evidence"):
        signature = inspect.signature(getattr(CampaignUiClient, name))
        assert "limit" in signature.parameters
        assert "cursor" in signature.parameters
def test_recovery_diagnostics_route_preserves_authoritative_values() -> None:
    # Return API-provided hung, orphaned and recovery states without recomputing them in the application.
    client = FakeCampaignClient()
    response = CampaignUiApplication(client).handle(
        "GET",
        "/api/campaigns/campaign-1/recovery-diagnostics",
        {"limit": ["20"]},
    )
    assert response.status == 200
    value = response.body["value"]
    assert value["recovery"]["state"] == "blocked"
    assert value["leases"]["items"][0]["hung_state"] == "stalled"
    assert value["workers"]["items"][0]["orphaned"] is True
    assert ("recovery", ("campaign-1", 20)) in client.calls
def test_quarantine_cursor_is_forwarded_without_offset_translation() -> None:
    # Pass opaque continuation cursors unchanged and never translate them into page numbers or offsets.
    client = FakeCampaignClient()
    app = CampaignUiApplication(client)
    first = app.handle("GET", "/api/campaigns/campaign-1/quarantine", {"limit": ["1"]})
    second = app.handle("GET", "/api/campaigns/campaign-1/quarantine", {"limit": ["1"], "cursor": ["q-cursor"]})
    assert first.body["value"]["next_cursor"] == "q-cursor"
    assert second.body["value"]["items"][0]["quarantine_id"] == "q-2"
    assert ("quarantine", ("campaign-1", 1, None)) in client.calls
    assert ("quarantine", ("campaign-1", 1, "q-cursor")) in client.calls
    source = Path(inspect.getsourcefile(CampaignUiApplication)).read_text(encoding="utf-8").lower()
    assert " offset " not in source
def test_recovery_assets_never_render_private_process_or_filesystem_fields() -> None:
    # Keep browser rendering limited to sanitized identities and public evidence states.
    script = asset_bytes("app.js").decode("utf-8").lower()
    assert "innerhtml" not in script
    assert "process_id" not in script
    assert "child_process_id" not in script
    assert "birth_token" not in script
    assert "spool_path" not in script
    assert "workspace_path" not in script
    assert "database_path" not in script
    assert "textcontent" in script
def test_transport_filter_removes_process_and_filesystem_evidence() -> None:
    # Strip private runtime identifiers before browser responses are serialized.
    value = to_json_value({
        "worker_id": "worker-1",
        "process_id": 1234,
        "child_process_id": 5678,
        "process_birth_token": "secret-token",
        "workspace_path": "D:/private/workspace",
        "status": "stopped",
    })
    assert value == {"worker_id": "worker-1", "status": "stopped"}

def test_artifact_registry_fallback_uses_existing_public_read_only() -> None:
    # Allow path-free artifact metadata through LocalApiService without creating a filesystem fallback.
    source = Path(inspect.getsourcefile(PublicApiCampaignClient)).read_text(encoding="utf-8")
    method_source = source.split("def get_artifact_registry", 2)[-1].split("def get_test_statistics", 1)[0]
    assert "self._reads.list_artifacts" in method_source
    assert "open(" not in method_source
    assert "Path(" not in method_source
