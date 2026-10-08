"""Public client protocol and production adapter for the campaign UI."""
from __future__ import annotations
import inspect
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Protocol, TypeAlias, runtime_checkable
from theseus_api import LocalApiService, LocalEventStream, LocalOperatorActions
from .serialization import JsonValue, to_json_value
ClientResponse: TypeAlias = Mapping[str, JsonValue]
CampaignCreateRequest: TypeAlias = Mapping[str, JsonValue]
ProjectRunCreateRequest: TypeAlias = Mapping[str, JsonValue]
LaunchAuthority: TypeAlias = Callable[[CampaignCreateRequest], object]
def _rejected(
    code: str,
    message: str,
    *,
    details: Mapping[str, JsonValue] | None = None,
) -> ClientResponse:
    # Build one stable client-side rejection compatible with public API outcomes.
    return {
        "ok": False,
        "kind": "rejected",
        "error": {
            "code": code,
            "message": message,
            "retriable": False,
            "details": dict(details or {}),
        },
    }
def _normalize(value: object) -> ClientResponse:
    # Convert one public API outcome into a privacy-filtered plain mapping.
    normalized = to_json_value(value)
    if not isinstance(normalized, dict):
        raise TypeError("public API outcome must serialize to an object")
    return normalized
def _success_mapping(response: ClientResponse) -> Mapping[str, JsonValue] | None:
    # Return one successful response value mapping without changing failed outcomes.
    value = response.get("value")
    return value if response.get("ok") is True and isinstance(value, Mapping) else None
def _with_success_value(response: ClientResponse, value: Mapping[str, JsonValue]) -> ClientResponse:
    # Replace only one successful response value while retaining its public discriminator.
    return {"ok": True, "kind": str(response.get("kind", "success")), "value": dict(value)}
def _adapt_recovery_diagnostics(response: ClientResponse, campaign_id: str) -> ClientResponse:
    # Map public recovery DTO names to the stable browser protocol without recomputing authority.
    value = _success_mapping(response)
    if value is None:
        return response
    raw_leases = value.get("leases")
    leases = dict(raw_leases) if isinstance(raw_leases, Mapping) else {"items": [], "limit": 0, "next_cursor": None}
    lease_items = leases.get("items")
    leases["items"] = [
        {
            **dict(item),
            "lease_status": item.get("status"),
            "heartbeat_age_seconds": None,
            "expiration_state": "expired" if item.get("expired") is True else "active",
            "hung_state": "stalled" if item.get("stalled") is True else "healthy",
            "observed_revision": item.get("revision_number"),
        }
        for item in lease_items
        if isinstance(item, Mapping)
    ] if isinstance(lease_items, list) else []
    raw_workers = value.get("workers")
    workers = dict(raw_workers) if isinstance(raw_workers, Mapping) else {"items": [], "limit": 0, "next_cursor": None}
    worker_items = workers.get("items")
    workers["items"] = [
        {
            **dict(item),
            "lifecycle_status": item.get("status"),
            "heartbeat_at": item.get("last_heartbeat_at"),
            "process_evidence_status": "alive" if item.get("process_alive") is True else "terminated",
        }
        for item in worker_items
        if isinstance(item, Mapping)
    ] if isinstance(worker_items, list) else []
    raw_spool = value.get("spool")
    spool = dict(raw_spool) if isinstance(raw_spool, Mapping) else {}
    raw_deliveries = spool.get("deliveries")
    deliveries = dict(raw_deliveries) if isinstance(raw_deliveries, Mapping) else {"items": [], "limit": 0, "next_cursor": None}
    delivery_items = deliveries.get("items")
    deliveries["items"] = [
        {
            **dict(item),
            "delivery_id": item.get("event_id"),
            "campaign_id": campaign_id,
            "execution_id": None,
            "status": "pending",
            "recorded_at": None,
        }
        for item in delivery_items
        if isinstance(item, Mapping)
    ] if isinstance(delivery_items, list) else []
    spool["deliveries"] = deliveries
    spool["truncated"] = deliveries.get("next_cursor") is not None
    return _with_success_value(
        response,
        {
            "progress": {
                "campaign_status": value.get("campaign_status"),
                "campaign_revision": value.get("campaign_revision"),
            },
            "recovery": {
                "state": value.get("recovery_state"),
                "pending_deliveries": spool.get("pending_count", 0),
                "pending_finalization": value.get("pending_finalization", False),
                "last_activity": value.get("last_recovery_at"),
            },
            "pending_outbox": value.get("pending_outbox", 0),
            "blockers": value.get("blockers", []),
            "leases": leases,
            "workers": workers,
            "spool": spool,
        },
    )
def _adapt_quarantine(response: ClientResponse, campaign_id: str) -> ClientResponse:
    # Add UI aliases to one public quarantine page while retaining the canonical conflict fields.
    value = _success_mapping(response)
    if value is None:
        return response
    raw_items = value.get("items")
    items = [
        {
            **dict(item),
            "quarantine_id": item.get("conflict_id"),
            "reason_code": item.get("conflict_type"),
            "campaign_id": campaign_id,
            "shard_id": None,
            "execution_id": None,
            "attempt": None,
            "recorded_at": item.get("created_at"),
            "evidence_summary": item.get("reason"),
        }
        for item in raw_items
        if isinstance(item, Mapping)
    ] if isinstance(raw_items, list) else []
    return _with_success_value(response, {**dict(value), "items": items})
def _adapt_artifact_registry(response: ClientResponse) -> ClientResponse:
    # Flatten one public registry wrapper into the browser page contract without inventing artifact identities.
    value = _success_mapping(response)
    if value is None:
        return response
    raw_page = value.get("artifacts")
    page = dict(raw_page) if isinstance(raw_page, Mapping) else dict(value)
    raw_items = page.get("items")
    finalization_status = value.get("finalization_status")
    page["items"] = [
        {
            **dict(item),
            "artifact_id": item.get("artifact_id"),
            "registration_status": "registered",
            "finalization_status": finalization_status,
        }
        for item in raw_items
        if isinstance(item, Mapping)
    ] if isinstance(raw_items, list) else []
    return _with_success_value(response, page)
def _adapt_test_statistics(response: ClientResponse) -> ClientResponse:
    # Add the UI test identity alias to the global canonical test-statistics page.
    value = _success_mapping(response)
    if value is None:
        return response
    raw_items = value.get("items")
    items = [
        {**dict(item), "test_id": item.get("entity_id")}
        for item in raw_items
        if isinstance(item, Mapping)
    ] if isinstance(raw_items, list) else []
    return _with_success_value(response, {**dict(value), "items": items})
def _adapt_reuse_evidence(response: ClientResponse) -> ClientResponse:
    # Add the UI reuse-kind alias while preserving the planner-owned decision fields.
    value = _success_mapping(response)
    if value is None:
        return response
    raw_items = value.get("items")
    items = [
        {**dict(item), "reuse_kind": item.get("kind")}
        for item in raw_items
        if isinstance(item, Mapping)
    ] if isinstance(raw_items, list) else []
    return _with_success_value(response, {**dict(value), "items": items})
@runtime_checkable
class CampaignUiClient(Protocol):
    """Transport-neutral operations consumed by the browser application."""
    def list_projects(self, *, limit: int, cursor: str | None = None) -> ClientResponse:
        # Return one bounded project page.
        ...
    def register_project(self, root_path: str, display_name: str | None = None) -> ClientResponse:
        # Register one local checkout for browser campaign creation.
        ...

    def list_campaigns(
        self,
        *,
        limit: int,
        cursor: str | None = None,
        project_id: str | None = None,
    ) -> ClientResponse:
        # Return one bounded campaign page.
        ...
    def list_project_runs(
        self,
        *,
        limit: int,
        cursor: str | None = None,
        project_id: str | None = None,
    ) -> ClientResponse:
        # Return one bounded project-run page.
        ...
    def get_project_run(self, run_id: str) -> ClientResponse:
        # Return one project-level run with child campaign progress.
        ...
    def create_project_run(self, request: ProjectRunCreateRequest) -> ClientResponse:
        # Create one one-click project run through the local project registry authority.
        ...
    def cancel_project_run(self, run_id: str) -> ClientResponse:
        # Cancel queued project-run work and request cancellation of its active child campaign.
        ...
    def get_campaign(self, campaign_id: str, *, related_limit: int) -> ClientResponse:
        # Return one bounded campaign detail snapshot.
        ...
    def list_plans(
        self,
        *,
        limit: int,
        cursor: str | None = None,
        campaign_id: str | None = None,
    ) -> ClientResponse:
        # Return one bounded plan page.
        ...
    def get_progress(
        self,
        campaign_id: str,
        *,
        shard_limit: int,
        shard_cursor: str | None = None,
    ) -> ClientResponse:
        # Return one bounded campaign progress snapshot.
        ...
    def list_workers(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ClientResponse:
        # Return one bounded worker page.
        ...
    def list_shards(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ClientResponse:
        # Return one bounded shard page.
        ...
    def list_executions(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ClientResponse:
        # Return one bounded execution page.
        ...
    def list_artifacts(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ClientResponse:
        # Return one bounded artifact page.
        ...
    def get_knowledge(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ClientResponse:
        # Return one bounded campaign knowledge page.
        ...
    def list_statistics(
        self,
        entity_type: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ClientResponse:
        # Return one bounded statistics page.
        ...
    def stream_events(
        self,
        *,
        limit: int,
        cursor: str | None = None,
        campaign_id: str | None = None,
        event_type: str | None = None,
    ) -> ClientResponse:
        # Return one bounded resumable event page.
        ...
    def create_campaign(self, request: CampaignCreateRequest) -> ClientResponse:
        # Create one campaign through the public launch authority.
        ...
    def start_campaign(
        self,
        action_id: str,
        campaign_id: str,
        *,
        expected_revision: int,
    ) -> ClientResponse:
        # Start one campaign through the durable detached launch authority.
        ...

    def cancel_campaign(
        self,
        action_id: str,
        campaign_id: str,
        *,
        expected_revision: int,
    ) -> ClientResponse:
        # Cancel one campaign through a durable operator action.
        ...
    def retry_campaign(
        self,
        action_id: str,
        campaign_id: str,
        *,
        expected_revision: int,
    ) -> ClientResponse:
        # Retry one campaign through a durable operator action.
        ...
    def resume_campaign(
        self,
        action_id: str,
        campaign_id: str,
        *,
        expected_revision: int,
    ) -> ClientResponse:
        # Resume one campaign through a durable operator action.
        ...
    def get_recovery_diagnostics(self, campaign_id: str, *, limit: int) -> ClientResponse:
        # Return one bounded authoritative recovery diagnostics snapshot.
        ...
    def inspect_quarantine(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ClientResponse:
        # Return one bounded keyset page of quarantined recovery evidence.
        ...
    def get_artifact_registry(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ClientResponse:
        # Return one bounded artifact registry page without physical paths.
        ...
    def get_test_statistics(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ClientResponse:
        # Return one bounded campaign-scoped test statistics page.
        ...
    def get_reuse_evidence(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ClientResponse:
        # Return one bounded reuse evidence page without raw knowledge payloads.
        ...
    def get_recovery_action(self, campaign_id: str, action_id: str) -> ClientResponse:
        # Return one durable recovery action receipt for bounded browser polling.
        ...
    def reconcile_campaign(
        self,
        action_id: str,
        campaign_id: str,
        *,
        expected_revision: int,
    ) -> ClientResponse:
        # Reconcile one campaign through the public durable recovery authority.
        ...
    def recover_campaign(
        self,
        action_id: str,
        campaign_id: str,
        *,
        expected_revision: int,
    ) -> ClientResponse:
        # Recover or resume one campaign through the public durable recovery authority.
        ...
class PublicApiCampaignClient:
    """Production UI adapter composed exclusively from public ``theseus_api`` objects."""
    def __init__(
        self,
        campaign_database: str | Path,
        *,
        knowledge_database: str | Path | None = None,
        statistics_database: str | Path | None = None,
        launch_authority: LaunchAuthority | None = None,
    ) -> None:
        # Construct public read, stream and action boundaries without coordinator or store imports.
        self._reads = LocalApiService(
            campaign_database,
            knowledge_database=knowledge_database,
            statistics_database=statistics_database,
        )
        self._stream = LocalEventStream(
            campaign_database,
            knowledge_database=knowledge_database,
            statistics_database=statistics_database,
        )
        self._actions = LocalOperatorActions(
            campaign_database,
            knowledge_database=knowledge_database,
            statistics_database=statistics_database,
        )
        self._launch_authority = launch_authority
    @staticmethod
    def _result(value: object) -> ClientResponse:
        # Normalize one public DTO outcome without retaining runtime objects.
        return _normalize(value)
    @staticmethod
    def _method(target: object, name: str) -> Callable[..., object] | None:
        # Resolve one optional public method without importing its implementation module.
        method = getattr(target, name, None)
        return method if callable(method) else None
    @staticmethod
    def _supports_keyword(method: Callable[..., object], name: str) -> bool:
        # Check one public callable signature before forwarding an optional cursor.
        try:
            signature = inspect.signature(method)
        except (TypeError, ValueError):
            return False
        return name in signature.parameters
    @staticmethod
    def _unavailable(method: str) -> ClientResponse:
        # Return one typed integration rejection until PR26-A exposes the public boundary.
        return _rejected(
            "recovery_api_unavailable",
            "public recovery diagnostics API is not connected",
            details={"method": method},
        )
    def list_projects(self, *, limit: int, cursor: str | None = None) -> ClientResponse:
        # Delegate the bounded project read to LocalApiService.
        return self._result(self._reads.list_projects(limit=limit, cursor=cursor))
    def register_project(self, root_path: str, display_name: str | None = None) -> ClientResponse:
        # Keep project registration explicit until the local multi-project authority is enabled.
        del root_path, display_name
        return _rejected("project_registry_unavailable", "local project registry is not connected")

    def list_campaigns(
        self,
        *,
        limit: int,
        cursor: str | None = None,
        project_id: str | None = None,
    ) -> ClientResponse:
        # Delegate the bounded campaign read to LocalApiService.
        return self._result(
            self._reads.list_campaigns(limit=limit, cursor=cursor, project_id=project_id)
        )
    def get_campaign(self, campaign_id: str, *, related_limit: int) -> ClientResponse:
        # Delegate the bounded campaign detail read to LocalApiService.
        return self._result(
            self._reads.get_campaign(campaign_id, related_limit=related_limit)
        )
    def list_plans(
        self,
        *,
        limit: int,
        cursor: str | None = None,
        campaign_id: str | None = None,
    ) -> ClientResponse:
        # Delegate the bounded plan read to LocalApiService.
        return self._result(
            self._reads.list_plans(
                limit=limit,
                cursor=cursor,
                campaign_id=campaign_id,
            )
        )
    def get_progress(
        self,
        campaign_id: str,
        *,
        shard_limit: int,
        shard_cursor: str | None = None,
    ) -> ClientResponse:
        # Delegate the bounded progress read to LocalEventStream.
        return self._result(
            self._stream.get_progress(
                campaign_id,
                shard_limit=shard_limit,
                shard_cursor=shard_cursor,
            )
        )
    def list_workers(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ClientResponse:
        # Delegate the bounded worker read to LocalApiService.
        return self._result(
            self._reads.list_workers(campaign_id, limit=limit, cursor=cursor)
        )
    def list_shards(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ClientResponse:
        # Delegate the bounded shard read to LocalApiService.
        return self._result(
            self._reads.list_shards(campaign_id, limit=limit, cursor=cursor)
        )
    def list_executions(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ClientResponse:
        # Delegate the bounded execution read to LocalApiService.
        return self._result(
            self._reads.list_executions(campaign_id, limit=limit, cursor=cursor)
        )
    def list_artifacts(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ClientResponse:
        # Delegate the bounded artifact read to LocalApiService.
        return self._result(
            self._reads.list_artifacts(campaign_id, limit=limit, cursor=cursor)
        )
    def get_knowledge(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ClientResponse:
        # Delegate the bounded knowledge read to LocalApiService.
        return self._result(
            self._reads.get_knowledge(campaign_id, limit=limit, cursor=cursor)
        )
    def list_statistics(
        self,
        entity_type: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ClientResponse:
        # Delegate the bounded statistics read to LocalApiService.
        return self._result(
            self._reads.list_statistics(entity_type, limit=limit, cursor=cursor)
        )
    def stream_events(
        self,
        *,
        limit: int,
        cursor: str | None = None,
        campaign_id: str | None = None,
        event_type: str | None = None,
    ) -> ClientResponse:
        # Delegate the one-shot event read to LocalEventStream.
        return self._result(
            self._stream.stream_events(
                limit=limit,
                cursor=cursor,
                campaign_id=campaign_id,
                event_type=event_type,
            )
        )
    def create_campaign(self, request: CampaignCreateRequest) -> ClientResponse:
        # Invoke the injected public launch authority or reject without coordinator access.
        if self._launch_authority is None:
            return _rejected(
                "launch_authority_unavailable",
                "campaign launch authority is not connected",
            )
        return self._result(self._launch_authority(dict(request)))
    def start_campaign(
        self,
        action_id: str,
        campaign_id: str,
        *,
        expected_revision: int,
    ) -> ClientResponse:
        # Delegate one durable detached campaign start through the public action boundary.
        return self._result(
            self._actions.start_campaign(
                action_id,
                campaign_id,
                expected_revision=expected_revision,
            )
        )

    def cancel_campaign(
        self,
        action_id: str,
        campaign_id: str,
        *,
        expected_revision: int,
    ) -> ClientResponse:
        # Delegate cancellation to the durable public operator boundary.
        return self._result(
            self._actions.cancel_campaign(
                action_id,
                campaign_id,
                expected_revision=expected_revision,
            )
        )
    def retry_campaign(
        self,
        action_id: str,
        campaign_id: str,
        *,
        expected_revision: int,
    ) -> ClientResponse:
        # Delegate campaign retry to the durable public operator boundary.
        return self._result(
            self._actions.retry_campaign(
                action_id,
                campaign_id,
                expected_revision=expected_revision,
            )
        )
    def resume_campaign(
        self,
        action_id: str,
        campaign_id: str,
        *,
        expected_revision: int,
    ) -> ClientResponse:
        # Delegate campaign resume to the durable public operator boundary.
        return self._result(
            self._actions.resume_campaign(
                action_id,
                campaign_id,
                expected_revision=expected_revision,
            )
        )
    def get_recovery_diagnostics(self, campaign_id: str, *, limit: int) -> ClientResponse:
        # Read PR26-A diagnostics and adapt only public field names for the browser protocol.
        method = self._method(self._reads, "get_recovery_diagnostics")
        if method is None:
            return self._unavailable("get_recovery_diagnostics")
        return _adapt_recovery_diagnostics(
            self._result(method(campaign_id, limit=limit)),
            campaign_id,
        )
    def inspect_quarantine(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ClientResponse:
        # Delegate only when the public quarantine boundary supports stable cursor continuation.
        method = self._method(self._actions, "inspect_quarantine")
        if method is None or not self._supports_keyword(method, "cursor"):
            return self._unavailable("inspect_quarantine")
        return _adapt_quarantine(
            self._result(method(campaign_id, limit=limit, cursor=cursor)),
            campaign_id,
        )
    def get_artifact_registry(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ClientResponse:
        # Prefer the PR26-A registry method and fall back to the existing public path-free artifact page.
        method = self._method(self._reads, "get_artifact_registry")
        if method is not None:
            return _adapt_artifact_registry(
                self._result(method(campaign_id, limit=limit, cursor=cursor))
            )
        return _adapt_artifact_registry(
            self._result(self._reads.list_artifacts(campaign_id, limit=limit, cursor=cursor))
        )
    def get_test_statistics(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ClientResponse:
        # Read global test statistics while retaining campaign-page protocol symmetry.
        _ = campaign_id
        method = self._method(self._reads, "get_test_statistics")
        if method is None:
            return self._unavailable("get_test_statistics")
        return _adapt_test_statistics(
            self._result(method(limit=limit, cursor=cursor))
        )
    def get_reuse_evidence(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ClientResponse:
        # Delegate only to the sanitized PR26-A reuse-evidence boundary.
        method = self._method(self._reads, "get_reuse_evidence")
        if method is None:
            return self._unavailable("get_reuse_evidence")
        return _adapt_reuse_evidence(
            self._result(method(campaign_id, limit=limit, cursor=cursor))
        )
    def get_recovery_action(self, campaign_id: str, action_id: str) -> ClientResponse:
        # Poll one public durable recovery receipt without private store fallbacks.
        method = self._method(self._actions, "get_recovery_action")
        if method is None:
            return self._unavailable("get_recovery_action")
        return self._result(method(campaign_id, action_id))
    def reconcile_campaign(
        self,
        action_id: str,
        campaign_id: str,
        *,
        expected_revision: int,
    ) -> ClientResponse:
        # Delegate reconciliation only to the durable PR26-A recovery action.
        method = self._method(self._actions, "reconcile_campaign")
        if method is None:
            return self._unavailable("reconcile_campaign")
        return self._result(method(action_id, campaign_id, expected_revision=expected_revision))
    def recover_campaign(
        self,
        action_id: str,
        campaign_id: str,
        *,
        expected_revision: int,
    ) -> ClientResponse:
        # Delegate recovery only to the durable PR26-A recovery action.
        method = self._method(self._actions, "recover_campaign")
        if method is None:
            return self._unavailable("recover_campaign")
        return self._result(method(action_id, campaign_id, expected_revision=expected_revision))
