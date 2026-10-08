from __future__ import annotations
from collections.abc import Mapping
from pathlib import Path
from theseus_ui import CampaignUiApplication, PublicApiCampaignClient
class FakeCampaignClient:
    def __init__(self) -> None:
        # Retain deterministic calls, revisions and recovery action receipts for application tests.
        self.calls: list[tuple[str, object]] = []
        self.action_calls = 0
        self.recovery_action_status = "running"
    @staticmethod
    def _page(items: list[dict[str, object]], limit: int, next_cursor: str | None = None) -> dict[str, object]:
        # Build one stable public success page.
        return {
            "ok": True,
            "kind": "success",
            "value": {"items": items, "limit": limit, "next_cursor": next_cursor},
        }
    def list_projects(self, *, limit: int, cursor: str | None = None):
        # Return one project page and record the explicit bound.
        self.calls.append(("projects", (limit, cursor)))
        return self._page([{"project_id": "project-1", "display_name": "Project", "root_path": "D:/private/project"}], limit)
    def list_campaigns(self, *, limit: int, cursor: str | None = None, project_id: str | None = None):
        # Return one campaign page and record the optional project filter.
        self.calls.append(("campaigns", (limit, cursor, project_id)))
        return self._page(
            [{"campaign_id": "campaign-1", "status": "running", "revision_number": 7, "completed_mutants": 2, "total_mutants": 4}],
            limit,
        )
    def get_campaign(self, campaign_id: str, *, related_limit: int):
        # Return one bounded campaign detail fixture.
        self.calls.append(("campaign", (campaign_id, related_limit)))
        return {
            "ok": True,
            "kind": "success",
            "value": {
                "campaign": {"campaign_id": campaign_id, "revision_number": 7, "status": "running"},
                "plan": {"plan_id": "plan-1", "selected_mutants": 4},
                "workers": {"items": [], "limit": related_limit, "next_cursor": None},
                "shards": {"items": [], "limit": related_limit, "next_cursor": None},
                "executions": {"items": [], "limit": related_limit, "next_cursor": None},
                "artifacts": {"items": [], "limit": related_limit, "next_cursor": None},
            },
        }
    def list_plans(self, *, limit: int, cursor: str | None = None, campaign_id: str | None = None):
        # Return one bounded plan page.
        self.calls.append(("plans", (limit, cursor, campaign_id)))
        return self._page([], limit)
    def get_progress(self, campaign_id: str, *, shard_limit: int, shard_cursor: str | None = None):
        # Return one bounded progress snapshot.
        self.calls.append(("progress", (campaign_id, shard_limit, shard_cursor)))
        return {"ok": True, "kind": "success", "value": {"campaign_id": campaign_id, "campaign_revision": 7}}
    def list_workers(self, campaign_id: str, *, limit: int, cursor: str | None = None):
        # Return one bounded worker page.
        self.calls.append(("workers", (campaign_id, limit, cursor)))
        return self._page([], limit)
    def list_shards(self, campaign_id: str, *, limit: int, cursor: str | None = None):
        # Return one bounded shard page.
        self.calls.append(("shards", (campaign_id, limit, cursor)))
        return self._page([], limit)
    def list_executions(self, campaign_id: str, *, limit: int, cursor: str | None = None):
        # Return one bounded execution page.
        self.calls.append(("executions", (campaign_id, limit, cursor)))
        return self._page([], limit)
    def list_artifacts(self, campaign_id: str, *, limit: int, cursor: str | None = None):
        # Return one bounded artifact page.
        self.calls.append(("artifacts", (campaign_id, limit, cursor)))
        return self._page([], limit)
    def get_knowledge(self, campaign_id: str, *, limit: int, cursor: str | None = None):
        # Return one bounded knowledge page.
        self.calls.append(("knowledge", (campaign_id, limit, cursor)))
        return {"ok": True, "kind": "success", "value": {"records": {"items": [], "limit": limit, "next_cursor": None}}}
    def list_statistics(self, entity_type: str, *, limit: int, cursor: str | None = None):
        # Return one bounded statistics page.
        self.calls.append(("statistics", (entity_type, limit, cursor)))
        return self._page([], limit)
    def stream_events(self, *, limit: int, cursor: str | None = None, campaign_id: str | None = None, event_type: str | None = None):
        # Return one bounded event page and preserve the supplied cursor.
        self.calls.append(("events", (limit, cursor, campaign_id, event_type)))
        return {"ok": True, "kind": "success", "value": {"items": [], "limit": limit, "next_cursor": cursor or "cursor-1", "has_more": False}}
    def create_campaign(self, request: Mapping[str, object]):
        # Record one launch authority request without importing a coordinator.
        self.calls.append(("create", dict(request)))
        return {"ok": True, "kind": "success", "value": {"campaign_id": "campaign-new"}}
    def cancel_campaign(self, action_id: str, campaign_id: str, *, expected_revision: int):
        # Return one stable stale-revision rejection without any automatic retry.
        self.action_calls += 1
        self.calls.append(("cancel", (action_id, campaign_id, expected_revision)))
        return {"ok": False, "kind": "rejected", "error": {"code": "stale_revision", "message": "campaign changed", "retriable": False, "details": {}}}
    def retry_campaign(self, action_id: str, campaign_id: str, *, expected_revision: int):
        # Return one successful campaign-level retry receipt.
        self.action_calls += 1
        return {"ok": True, "kind": "success", "value": {"action_id": action_id, "campaign_id": campaign_id, "campaign_revision": expected_revision}}
    def resume_campaign(self, action_id: str, campaign_id: str, *, expected_revision: int):
        # Return one successful resume receipt.
        self.action_calls += 1
        return {"ok": True, "kind": "success", "value": {"action_id": action_id, "campaign_id": campaign_id, "campaign_revision": expected_revision}}
    def get_recovery_diagnostics(self, campaign_id: str, *, limit: int):
        # Return bounded authoritative recovery sections without path-bearing fields.
        self.calls.append(("recovery", (campaign_id, limit)))
        return {
            "ok": True,
            "kind": "success",
            "value": {
                "progress": {"campaign_status": "running", "campaign_revision": 7},
                "recovery": {"state": "blocked", "pending_deliveries": 2, "pending_finalization": True, "last_activity": "2026-08-06T00:00:00Z"},
                "blockers": [{"code": "lease_stalled", "message": "one lease is stalled"}],
                "leases": {"items": [{"shard_id": "shard-1", "worker_id": "worker-1", "attempt": 1, "lease_status": "running", "heartbeat_age_seconds": 30, "expiration_state": "expired", "hung_state": "stalled", "observed_revision": 4}]},
                "workers": {"items": [{"worker_id": "worker-1", "lifecycle_status": "stopped", "current_shard_id": "shard-1", "heartbeat_at": "2026-08-06T00:00:00Z", "orphaned": True, "process_evidence_status": "terminated"}]},
                "spool": {"pending_count": 2, "acknowledged_count": 3, "quarantined_count": 1, "oldest_pending_at": "2026-08-06T00:00:00Z", "deliveries": {"items": [{"delivery_id": "delivery-1", "campaign_id": campaign_id, "shard_id": "shard-1", "execution_id": "execution-1", "attempt": 1, "status": "pending", "recorded_at": "2026-08-06T00:00:00Z"}]}, "truncated": False},
            },
        }
    def inspect_quarantine(self, campaign_id: str, *, limit: int, cursor: str | None = None):
        # Return a two-page cursor fixture for quarantine continuation tests.
        self.calls.append(("quarantine", (campaign_id, limit, cursor)))
        if cursor is None:
            return self._page([{"quarantine_id": "q-1", "reason_code": "payload_conflict", "campaign_id": campaign_id}], limit, "q-cursor")
        return self._page([{"quarantine_id": "q-2", "reason_code": "stale_delivery", "campaign_id": campaign_id}], limit)
    def get_artifact_registry(self, campaign_id: str, *, limit: int, cursor: str | None = None):
        # Return one path-free artifact registry page.
        self.calls.append(("artifact_registry", (campaign_id, limit, cursor)))
        return self._page([{"logical_key": "report", "artifact_id": "artifact-1", "content_sha256": "a" * 64, "size_bytes": 10, "registration_status": "registered", "finalization_status": "completed"}], limit)
    def get_test_statistics(self, campaign_id: str, *, limit: int, cursor: str | None = None):
        # Return one campaign-scoped test statistics page.
        self.calls.append(("test_statistics", (campaign_id, limit, cursor)))
        return self._page([{"test_id": "tests/test_app.py::test_value", "duration_avg_ms": 10.0, "passed_count": 2, "failed_count": 0, "error_count": 0, "timeout_count": 0, "retry_count": 1, "recovery_count": 1, "flaky_transition_count": 0}], limit)
    def get_reuse_evidence(self, campaign_id: str, *, limit: int, cursor: str | None = None):
        # Return one sanitized reuse evidence page.
        self.calls.append(("reuse", (campaign_id, limit, cursor)))
        return self._page([{"mutant_id": "mutant-1", "reuse_kind": "exact", "authorized": True, "audit_required": False, "evidence_quality": "validated", "blockers": [], "source_execution_id": "execution-1", "source_event_id": "event-1"}], limit)
    def get_recovery_action(self, campaign_id: str, action_id: str):
        # Return the current durable recovery action state for polling.
        self.calls.append(("recovery_action", (campaign_id, action_id)))
        return {"ok": True, "kind": "success", "value": {"action_id": action_id, "action": "recover", "campaign_id": campaign_id, "status": self.recovery_action_status, "campaign_revision": 7}}
    def reconcile_campaign(self, action_id: str, campaign_id: str, *, expected_revision: int):
        # Return one running durable reconciliation receipt.
        self.action_calls += 1
        self.calls.append(("reconcile", (action_id, campaign_id, expected_revision)))
        return {"ok": True, "kind": "success", "value": {"action_id": action_id, "action": "reconcile", "campaign_id": campaign_id, "status": "running", "campaign_revision": expected_revision}}
    def recover_campaign(self, action_id: str, campaign_id: str, *, expected_revision: int):
        # Return one completed durable recovery receipt.
        self.action_calls += 1
        self.calls.append(("recover", (action_id, campaign_id, expected_revision)))
        return {"ok": True, "kind": "success", "value": {"action_id": action_id, "action": "recover", "campaign_id": campaign_id, "status": "completed", "campaign_revision": expected_revision + 1}}
def _create_payload() -> dict[str, object]:
    # Build one complete browser launch form payload.
    return {
        "project_root": "D:/project",
        "source_path": "app.py",
        "function": "calculate",
        "scope_kind": "function",
        "operators": ["condition_to_not"],
        "max_mutants": 10,
        "max_workers": 2,
        "max_seconds": 600.0,
        "max_test_seconds": 120.0,
        "no_escalation": True,
        "reuse_mode": "hint",
    }
def test_ui_routes_all_campaign_reads_through_the_client_protocol() -> None:
    # Keep every list bounded and pass opaque cursors unchanged to the fake client.
    client = FakeCampaignClient()
    app = CampaignUiApplication(client)
    projects = app.handle("GET", "/api/projects", {"limit": ["7"], "cursor": ["cursor-p"]})
    campaigns = app.handle("GET", "/api/campaigns", {"limit": ["8"], "project_id": ["project-1"]})
    detail = app.handle("GET", "/api/campaigns/campaign-1", {"limit": ["9"]})
    events = app.handle("GET", "/api/campaigns/campaign-1/events", {"limit": ["6"], "cursor": ["event-cursor"]})
    assert projects.status == 200
    assert campaigns.status == 200
    assert detail.status == 200
    assert events.status == 200
    assert ("projects", (7, "cursor-p")) in client.calls
    assert ("campaigns", (8, None, "project-1")) in client.calls
    assert ("campaign", ("campaign-1", 9)) in client.calls
    assert ("events", (6, "event-cursor", "campaign-1", None)) in client.calls
    assert "root_path" not in projects.body["value"]["items"][0]
def test_create_form_calls_only_the_launch_protocol() -> None:
    # Validate and forward one complete form without translating it into coordinator objects.
    client = FakeCampaignClient()
    app = CampaignUiApplication(client)
    response = app.handle("POST", "/api/campaigns", {}, _create_payload())
    assert response.status == 200
    assert client.calls[-1][0] == "create"
    request = client.calls[-1][1]
    assert request["source_path"] == "app.py"
    assert request["operators"] == ["condition_to_not"]
def test_stale_action_refresh_is_left_to_the_browser_and_is_not_retried() -> None:
    # Make exactly one protocol call for a stale action and return HTTP conflict.
    client = FakeCampaignClient()
    app = CampaignUiApplication(client)
    response = app.handle("POST", "/api/campaigns/campaign-1/actions/cancel", {}, {"action_id": "action-stale", "expected_revision": 7})
    assert response.status == 409
    assert response.body["error"]["code"] == "stale_revision"
    assert client.action_calls == 1
def test_recovery_routes_forward_bounded_cursors_and_action_receipts() -> None:
    # Route every recovery read and action through the fake client with no filesystem access.
    client = FakeCampaignClient()
    app = CampaignUiApplication(client)
    diagnostics = app.handle("GET", "/api/campaigns/campaign-1/recovery-diagnostics", {"limit": ["11"]})
    quarantine = app.handle("GET", "/api/campaigns/campaign-1/quarantine", {"limit": ["3"], "cursor": ["q-cursor"]})
    artifacts = app.handle("GET", "/api/campaigns/campaign-1/artifact-registry", {"limit": ["4"]})
    statistics = app.handle("GET", "/api/campaigns/campaign-1/test-statistics", {"limit": ["5"]})
    reuse = app.handle("GET", "/api/campaigns/campaign-1/reuse-evidence", {"limit": ["6"]})
    receipt = app.handle("GET", "/api/campaigns/campaign-1/recovery-actions/action-1", {"limit": ["1"]})
    reconcile = app.handle("POST", "/api/campaigns/campaign-1/actions/reconcile", {}, {"action_id": "action-1", "expected_revision": 7})
    assert all(item.status == 200 for item in (diagnostics, quarantine, artifacts, statistics, reuse, receipt, reconcile))
    assert ("recovery", ("campaign-1", 11)) in client.calls
    assert ("quarantine", ("campaign-1", 3, "q-cursor")) in client.calls
    assert ("recovery_action", ("campaign-1", "action-1")) in client.calls
    assert client.action_calls == 1
def test_invalid_limits_and_traversal_are_rejected_before_client_access() -> None:
    # Reject unbounded pages and encoded traversal without invoking the fake boundary.
    client = FakeCampaignClient()
    app = CampaignUiApplication(client)
    too_large = app.handle("GET", "/api/projects", {"limit": ["101"]})
    traversal = app.handle("GET", "/api/campaigns/..%2Fprivate", {"limit": ["10"]})
    action_traversal = app.handle("GET", "/api/campaigns/campaign-1/recovery-actions/..%2Faction", {"limit": ["1"]})
    assert too_large.status == 400
    assert traversal.status == 404
    assert action_traversal.status == 404
    assert client.calls == []
def test_production_adapter_reports_only_missing_launch_authority_explicitly(tmp_path: Path) -> None:
    # Keep campaign creation unavailable until an explicit launch authority is injected.
    client = PublicApiCampaignClient(tmp_path / "campaign.sqlite3")
    launch = client.create_campaign(_create_payload())
    assert launch["error"]["code"] == "launch_authority_unavailable"
