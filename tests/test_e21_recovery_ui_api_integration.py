from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from theseus_api import ApiRejected, ApiSuccess, LocalOperatorActions
from theseus_ui.client import PublicApiCampaignClient

def _success(value: object) -> dict[str, object]:
    # Wrap one fake public API value in the canonical success discriminator.
    return {"ok": True, "kind": "success", "value": value}

class _Reads:
    def __init__(self) -> None:
        # Record exact PR26-A read calls for adapter integration assertions.
        self.calls: list[tuple[str, object]] = []
    def get_recovery_diagnostics(self, campaign_id: str, *, limit: int) -> dict[str, object]:
        # Return the actual flat PR26-A recovery DTO shape.
        self.calls.append(("recovery", (campaign_id, limit)))
        return _success({
            "campaign_id": campaign_id,
            "campaign_status": "running",
            "campaign_revision": 7,
            "recovery_state": "blocked",
            "last_recovery_at": "2026-08-06T00:00:00Z",
            "pending_finalization": True,
            "pending_outbox": 2,
            "leases": {"items": [{"shard_id": "shard-1", "worker_id": "worker-1", "lease_id": "lease-1", "attempt": 1, "status": "running", "heartbeat_at": "2026-08-06T00:00:00Z", "expires_at": "2026-08-06T00:00:01Z", "expired": True, "stalled": True, "revision_number": 4}], "limit": limit, "next_cursor": None},
            "workers": {"items": [{"worker_id": "worker-1", "instance_id": "instance-1", "status": "stopped", "current_shard_id": "shard-1", "last_heartbeat_at": "2026-08-06T00:00:00Z", "orphaned": True, "process_alive": False, "workspace_healthy": True, "revision_number": 3}], "limit": limit, "next_cursor": None},
            "spool": {"pending_count": 1, "acknowledged_count": 2, "quarantined_count": 0, "oldest_pending_at": "2026-08-06T00:00:00Z", "deliveries": {"items": [{"event_id": "delivery-1", "worker_id": "worker-1", "shard_id": "shard-1", "lease_id": "lease-1", "attempt": 1, "mutant_count": 1}], "limit": limit, "next_cursor": "spool-next"}},
            "blockers": [{"code": "lease_stalled", "message": "lease is stalled", "severity": "warning", "component": "lease"}],
        })
    def get_artifact_registry(self, campaign_id: str, *, limit: int, cursor: str | None = None) -> dict[str, object]:
        # Return the actual PR26-A registry wrapper shape.
        self.calls.append(("artifacts", (campaign_id, limit, cursor)))
        return _success({"finalization_status": "completed", "artifacts": {"items": [{"campaign_id": campaign_id, "logical_key": "report", "logical_role": "campaign_report", "content_sha256": "a" * 64, "size_bytes": 10}], "limit": limit, "next_cursor": cursor}})
    def get_test_statistics(self, *, limit: int, cursor: str | None = None) -> dict[str, object]:
        # Require the keyword-only global PR26-A statistics signature.
        self.calls.append(("statistics", (limit, cursor)))
        return _success({"items": [{"entity_type": "test", "entity_id": "tests/test_app.py::test_value", "passed_count": 2}], "limit": limit, "next_cursor": cursor})
    def get_reuse_evidence(self, campaign_id: str, *, limit: int, cursor: str | None = None) -> dict[str, object]:
        # Return the actual PR26-A reuse DTO field names.
        self.calls.append(("reuse", (campaign_id, limit, cursor)))
        return _success({"items": [{"mutant_id": "mutant-1", "kind": "exact", "authorized": True}], "limit": limit, "next_cursor": cursor})

class _Actions:
    def __init__(self) -> None:
        # Record exact PR26-A action and quarantine calls.
        self.calls: list[tuple[str, object]] = []
    def inspect_quarantine(self, campaign_id: str, *, limit: int, cursor: str | None = None) -> dict[str, object]:
        # Return the canonical conflict DTO shape from PR26-A.
        self.calls.append(("quarantine", (campaign_id, limit, cursor)))
        return _success({"items": [{"conflict_id": "conflict-1", "conflict_type": "payload_conflict", "identity_type": "effect", "identity_key": "effect-1", "scope_id": None, "reason": "payload differs", "created_at": "2026-08-06T00:00:00Z", "status": "quarantined"}], "limit": limit, "truncated": False, "next_cursor": cursor})
    def get_recovery_action(self, campaign_id: str, action_id: str) -> dict[str, object]:
        # Return one durable recovery receipt through the new polling boundary.
        self.calls.append(("receipt", (campaign_id, action_id)))
        return _success({"action_id": action_id, "action": "reconcile_campaign", "campaign_id": campaign_id, "status": "running", "campaign_revision": 7})
    def reconcile_campaign(self, action_id: str, campaign_id: str, *, expected_revision: int) -> dict[str, object]:
        # Return one successful reconcile receipt.
        self.calls.append(("reconcile", (action_id, campaign_id, expected_revision)))
        return _success({"action_id": action_id, "campaign_id": campaign_id, "status": "completed", "campaign_revision": expected_revision})
    def recover_campaign(self, action_id: str, campaign_id: str, *, expected_revision: int) -> dict[str, object]:
        # Return one successful recover receipt.
        self.calls.append(("recover", (action_id, campaign_id, expected_revision)))
        return _success({"action_id": action_id, "campaign_id": campaign_id, "status": "completed", "campaign_revision": expected_revision})

def _client() -> tuple[PublicApiCampaignClient, _Reads, _Actions]:
    # Build the production adapter around public fake boundaries without invoking private stores.
    reads = _Reads()
    actions = _Actions()
    client = object.__new__(PublicApiCampaignClient)
    client._reads = reads
    client._actions = actions
    client._stream = SimpleNamespace()
    client._launch_authority = None
    return client, reads, actions

def test_pr26_ui_adapter_matches_every_public_api_signature_and_shape() -> None:
    # Connect all PR26 reads and actions while preserving the browser protocol aliases.
    client, reads, actions = _client()
    recovery = client.get_recovery_diagnostics("campaign-1", limit=5)
    quarantine = client.inspect_quarantine("campaign-1", limit=5, cursor="q-next")
    artifacts = client.get_artifact_registry("campaign-1", limit=5, cursor="a-next")
    statistics = client.get_test_statistics("campaign-1", limit=5, cursor="s-next")
    reuse = client.get_reuse_evidence("campaign-1", limit=5, cursor="r-next")
    receipt = client.get_recovery_action("campaign-1", "action-1")
    reconcile = client.reconcile_campaign("action-2", "campaign-1", expected_revision=7)
    recover = client.recover_campaign("action-3", "campaign-1", expected_revision=7)
    assert recovery["value"]["recovery"]["state"] == "blocked"
    assert recovery["value"]["leases"]["items"][0]["hung_state"] == "stalled"
    assert recovery["value"]["workers"]["items"][0]["process_evidence_status"] == "terminated"
    assert recovery["value"]["spool"]["deliveries"]["items"][0]["delivery_id"] == "delivery-1"
    assert recovery["value"]["spool"]["truncated"] is True
    assert quarantine["value"]["items"][0]["quarantine_id"] == "conflict-1"
    assert artifacts["value"]["items"][0]["finalization_status"] == "completed"
    assert artifacts["value"]["items"][0]["artifact_id"] is None
    assert statistics["value"]["items"][0]["test_id"] == "tests/test_app.py::test_value"
    assert reuse["value"]["items"][0]["reuse_kind"] == "exact"
    assert receipt["value"]["status"] == "running"
    assert reconcile["value"]["status"] == "completed"
    assert recover["value"]["status"] == "completed"
    assert ("statistics", (5, "s-next")) in reads.calls
    assert ("receipt", ("campaign-1", "action-1")) in actions.calls

@dataclass(frozen=True)
class Success:
    value: object
    duplicate: bool = False

class _ReceiptStore:
    def close(self) -> None:
        # Match the production repository lifecycle without mutating test state.
        return None

class _ReceiptService:
    def __init__(self, action: object) -> None:
        # Retain one durable action and count read-only polling calls.
        self.action = action
        self.calls = 0
    def get_operator_action(self, action_id: str) -> Success:
        # Return only the requested durable action identity.
        self.calls += 1
        return Success(self.action if action_id == self.action.action_id else None)

def test_public_recovery_action_polling_is_read_only_and_campaign_fenced(tmp_path: Path) -> None:
    # Read one running receipt repeatedly without starting, completing, or changing it.
    action = SimpleNamespace(
        action_id="action-poll",
        action_type="recover_campaign",
        campaign_id=SimpleNamespace(value="campaign-1"),
        status=SimpleNamespace(value="running"),
        result={},
        error_code=None,
        retriable=False,
        expected_revision=7,
    )
    service = _ReceiptService(action)
    actions = LocalOperatorActions(
        tmp_path / "campaign.sqlite3",
        mutation_context_factory=lambda _path: (_ReceiptStore(), service),
    )
    first = actions.get_recovery_action("campaign-1", "action-poll")
    repeated = actions.get_recovery_action("campaign-1", "action-poll")
    foreign = actions.get_recovery_action("campaign-2", "action-poll")
    assert isinstance(first, ApiSuccess)
    assert isinstance(repeated, ApiSuccess)
    assert first.to_dict() == repeated.to_dict()
    assert first.value.status == "running"
    assert service.calls == 3
    assert isinstance(foreign, ApiRejected)
    assert foreign.error.code == "action_not_found"
