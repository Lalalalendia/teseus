"""Route-level campaign UI application over the public client protocol."""
from __future__ import annotations
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TypeAlias
from urllib.parse import unquote
from .client import CampaignCreateRequest, CampaignUiClient, ClientResponse, ProjectRunCreateRequest
from .serialization import JsonValue, to_json_value
UI_MAX_LIMIT = 100
UI_DEFAULT_LIMIT = 25
UI_MAX_TEXT_LENGTH = 4096
ResponseBody: TypeAlias = Mapping[str, JsonValue]
@dataclass(frozen=True, slots=True)
class UiResponse:
    """One status-coded JSON response returned to the local HTTP adapter."""
    status: int
    body: ResponseBody
def _error(code: str, message: str, *, status: int = 400) -> UiResponse:
    # Build one stable UI-local rejection without exception text.
    return UiResponse(
        status,
        {
            "ok": False,
            "kind": "rejected",
            "error": {
                "code": code,
                "message": message,
                "retriable": False,
                "details": {},
            },
        },
    )
def _query_text(query: Mapping[str, list[str]], name: str) -> str | None:
    # Read one optional single-valued query parameter.
    values = query.get(name)
    if not values:
        return None
    value = values[-1].strip()
    if len(value) > UI_MAX_TEXT_LENGTH:
        raise ValueError(f"{name} is too long")
    return value or None
def _query_limit(query: Mapping[str, list[str]], name: str = "limit") -> int:
    # Parse one explicit bounded page limit.
    raw = _query_text(query, name)
    if raw is None:
        return UI_DEFAULT_LIMIT
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < 1 or value > UI_MAX_LIMIT:
        raise ValueError(f"{name} must be between 1 and {UI_MAX_LIMIT}")
    return value
def _required_text(payload: Mapping[str, JsonValue], name: str) -> str:
    # Normalize one required bounded JSON string.
    value = payload.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    current = value.strip()
    if len(current) > UI_MAX_TEXT_LENGTH:
        raise ValueError(f"{name} is too long")
    return current
def _optional_text(payload: Mapping[str, JsonValue], name: str) -> str | None:
    # Normalize one optional bounded JSON string.
    value = payload.get(name)
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    current = value.strip()
    if len(current) > UI_MAX_TEXT_LENGTH:
        raise ValueError(f"{name} is too long")
    return current or None
def _required_revision(payload: Mapping[str, JsonValue]) -> int:
    # Parse one non-negative observed campaign revision.
    value = payload.get("expected_revision")
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("expected_revision must be a non-negative integer")
    return value
def _status_for_outcome(value: ClientResponse) -> int:
    # Map stable API outcome kinds and codes to local HTTP statuses.
    if value.get("ok") is True:
        return 200
    error = value.get("error")
    code = str(error.get("code", "request_rejected")) if isinstance(error, Mapping) else "request_rejected"
    if code in {"not_found", "campaign_not_found", "project_run_not_found", "project_not_found", "action_not_found"}:
        return 404
    if code in {"stale_revision", "action_id_conflict", "action_in_progress", "project_run_already_active"}:
        return 409
    if code == "project_registry_unavailable":
        return 501
    if code in {"invalid_campaign_launch", "invalid_project_run", "project_run_no_sources"}:
        return 400
    if code in {"recovery_api_unavailable", "launch_authority_unavailable"}:
        return 501
    if value.get("kind") == "failed":
        return 503
    return 400
def _response(value: ClientResponse) -> UiResponse:
    # Sanitize one client response and attach its stable HTTP status.
    normalized = to_json_value(value)
    if not isinstance(normalized, dict):
        return _error("client_protocol_error", "client returned an invalid response", status=502)
    return UiResponse(_status_for_outcome(normalized), normalized)
def _safe_path_identifier(value: str) -> str | None:
    # Decode one bounded route identity and reject traversal or separator characters.
    current = unquote(value).strip()
    if (
        not current
        or len(current) > UI_MAX_TEXT_LENGTH
        or current in {".", ".."}
        or "/" in current
        or "\\" in current
    ):
        return None
    return current
def _campaign_path(path: str) -> tuple[str, tuple[str, ...]] | None:
    # Parse one exact campaign resource path without accepting traversal segments.
    raw_parts = tuple(item for item in path.split("/") if item)
    if len(raw_parts) < 3 or raw_parts[:2] != ("api", "campaigns"):
        return None
    decoded = tuple(_safe_path_identifier(item) for item in raw_parts[2:])
    if any(item is None for item in decoded):
        return None
    return str(decoded[0]), tuple(str(item) for item in decoded[1:])

def _project_run_path(path: str) -> str | None:
    # Parse one exact project-run detail path without accepting traversal segments.
    raw_parts = tuple(item for item in path.split("/") if item)
    if len(raw_parts) != 3 or raw_parts[:2] != ("api", "project-runs"):
        return None
    return _safe_path_identifier(raw_parts[2])
def _project_run_action_path(path: str) -> tuple[str, str] | None:
    # Parse one exact project-run action path while rejecting traversal and unknown nested shapes.
    raw_parts = tuple(item for item in path.split("/") if item)
    if len(raw_parts) != 5 or raw_parts[:2] != ("api", "project-runs") or raw_parts[3] != "actions":
        return None
    run_id = _safe_path_identifier(raw_parts[2])
    action = _safe_path_identifier(raw_parts[4])
    return (run_id, action) if run_id is not None and action is not None else None
class CampaignUiApplication:
    """Bounded JSON route dispatcher that depends only on ``CampaignUiClient``."""
    def __init__(self, client: CampaignUiClient) -> None:
        # Retain one protocol implementation for all browser requests.
        self._client = client
    def handle(
        self,
        method: str,
        path: str,
        query: Mapping[str, list[str]],
        payload: Mapping[str, JsonValue] | None = None,
    ) -> UiResponse:
        # Dispatch one request without filesystem, SQLite or coordinator access.
        try:
            if method == "GET":
                return self._handle_get(path, query)
            if method == "POST":
                return self._handle_post(path, payload or {})
            return _error("method_not_allowed", "method is not allowed", status=405)
        except ValueError as exc:
            return _error("invalid_request", str(exc))
        except Exception:
            return UiResponse(
                502,
                {
                    "ok": False,
                    "kind": "failed",
                    "error": {
                        "code": "client_request_failed",
                        "message": "campaign UI client request failed",
                        "retriable": True,
                        "details": {},
                    },
                },
            )
    def _handle_get(self, path: str, query: Mapping[str, list[str]]) -> UiResponse:
        # Dispatch one bounded read-only browser route.
        limit = _query_limit(query)
        cursor = _query_text(query, "cursor")
        if path == "/api/projects":
            return _response(self._client.list_projects(limit=limit, cursor=cursor))
        if path == "/api/campaigns":
            return _response(
                self._client.list_campaigns(
                    limit=limit,
                    cursor=cursor,
                    project_id=_query_text(query, "project_id"),
                )
            )
        if path == "/api/project-runs":
            return _response(
                self._client.list_project_runs(
                    limit=limit,
                    cursor=cursor,
                    project_id=_query_text(query, "project_id"),
                )
            )
        project_run_id = _project_run_path(path)
        if project_run_id is not None:
            return _response(self._client.get_project_run(project_run_id))
        parsed = _campaign_path(path)
        if parsed is None:
            return _error("not_found", "route does not exist", status=404)
        campaign_id, suffix = parsed
        if not suffix:
            return _response(self._client.get_campaign(campaign_id, related_limit=limit))
        if suffix[0] == "recovery-actions":
            if len(suffix) != 2:
                return _error("not_found", "route does not exist", status=404)
            return _response(self._client.get_recovery_action(campaign_id, suffix[1]))
        if len(suffix) != 1:
            return _error("not_found", "route does not exist", status=404)
        resource = suffix[0]
        if resource == "plans":
            return _response(
                self._client.list_plans(
                    limit=limit,
                    cursor=cursor,
                    campaign_id=campaign_id,
                )
            )
        if resource == "progress":
            return _response(
                self._client.get_progress(
                    campaign_id,
                    shard_limit=limit,
                    shard_cursor=cursor,
                )
            )
        if resource == "workers":
            return _response(self._client.list_workers(campaign_id, limit=limit, cursor=cursor))
        if resource == "shards":
            return _response(self._client.list_shards(campaign_id, limit=limit, cursor=cursor))
        if resource == "executions":
            return _response(self._client.list_executions(campaign_id, limit=limit, cursor=cursor))
        if resource == "artifacts":
            return _response(self._client.list_artifacts(campaign_id, limit=limit, cursor=cursor))
        if resource == "knowledge":
            return _response(self._client.get_knowledge(campaign_id, limit=limit, cursor=cursor))
        if resource == "statistics":
            return _response(self._client.list_statistics("campaign", limit=limit, cursor=cursor))
        if resource == "events":
            return _response(
                self._client.stream_events(
                    limit=limit,
                    cursor=cursor,
                    campaign_id=campaign_id,
                    event_type=_query_text(query, "event_type"),
                )
            )
        if resource == "recovery-diagnostics":
            return _response(self._client.get_recovery_diagnostics(campaign_id, limit=limit))
        if resource == "quarantine":
            return _response(self._client.inspect_quarantine(campaign_id, limit=limit, cursor=cursor))
        if resource == "artifact-registry":
            return _response(self._client.get_artifact_registry(campaign_id, limit=limit, cursor=cursor))
        if resource == "test-statistics":
            return _response(self._client.get_test_statistics(campaign_id, limit=limit, cursor=cursor))
        if resource == "reuse-evidence":
            return _response(self._client.get_reuse_evidence(campaign_id, limit=limit, cursor=cursor))
        return _error("not_found", "route does not exist", status=404)
    def _handle_post(self, path: str, payload: Mapping[str, JsonValue]) -> UiResponse:
        # Dispatch one state-changing browser request through the public client protocol.
        if path == "/api/projects":
            root_path = _required_text(payload, "project_root")
            display_name = _optional_text(payload, "display_name")
            return _response(self._client.register_project(root_path, display_name))

        if path == "/api/campaigns":
            request = self._campaign_request(payload)
            return _response(self._client.create_campaign(request))
        if path == "/api/project-runs":
            request = self._project_run_request(payload)
            return _response(self._client.create_project_run(request))
        project_run_action = _project_run_action_path(path)
        if project_run_action is not None:
            run_id, action = project_run_action
            if action == "cancel":
                return _response(self._client.cancel_project_run(run_id))
            return _error("not_found", "route does not exist", status=404)
        parsed = _campaign_path(path)
        if parsed is None:
            return _error("not_found", "route does not exist", status=404)
        campaign_id, suffix = parsed
        if len(suffix) != 2 or suffix[0] != "actions":
            return _error("not_found", "route does not exist", status=404)
        action = suffix[1]
        action_id = _required_text(payload, "action_id")
        revision = _required_revision(payload)
        if action == "start":
            return _response(
                self._client.start_campaign(
                    action_id,
                    campaign_id,
                    expected_revision=revision,
                )
            )

        if action == "cancel":
            return _response(
                self._client.cancel_campaign(
                    action_id,
                    campaign_id,
                    expected_revision=revision,
                )
            )
        if action == "retry":
            return _response(
                self._client.retry_campaign(
                    action_id,
                    campaign_id,
                    expected_revision=revision,
                )
            )
        if action == "resume":
            return _response(
                self._client.resume_campaign(
                    action_id,
                    campaign_id,
                    expected_revision=revision,
                )
            )
        if action == "reconcile":
            return _response(
                self._client.reconcile_campaign(
                    action_id,
                    campaign_id,
                    expected_revision=revision,
                )
            )
        if action == "recover":
            return _response(
                self._client.recover_campaign(
                    action_id,
                    campaign_id,
                    expected_revision=revision,
                )
            )
        return _error("not_found", "operator action does not exist", status=404)
    @staticmethod
    def _campaign_request(payload: Mapping[str, JsonValue]) -> CampaignCreateRequest:
        # Validate one browser launch form while keeping the local authority request explicit.
        project_id = _optional_text(payload, "project_id")
        project_root = _optional_text(payload, "project_root")
        if project_id is None and project_root is None:
            raise ValueError("project_id or project_root is required")
        operators = payload.get("operators", [])
        if not isinstance(operators, list) or any(not isinstance(item, str) or not item.strip() for item in operators):
            raise ValueError("operators must be an array of non-empty strings")
        test_command = payload.get("test_command")
        if test_command is not None and not isinstance(test_command, (str, list)):
            raise ValueError("test_command must be text or an argv array")
        if isinstance(test_command, list) and any(not isinstance(item, str) or not item.strip() for item in test_command):
            raise ValueError("test_command must contain non-empty strings")
        no_escalation = payload.get("no_escalation")
        if no_escalation is not None and not isinstance(no_escalation, bool):
            raise ValueError("no_escalation must be boolean or null")
        request: dict[str, JsonValue] = {
            "project_id": project_id,
            "project_root": project_root,
            "display_name": _optional_text(payload, "display_name"),
            "source_path": _required_text(payload, "source_path"),
            "function": _optional_text(payload, "function"),
            "scope_kind": _optional_text(payload, "scope_kind") or "file",
            "operators": [item.strip() for item in operators],
            "max_mutants": payload.get("max_mutants"),
            "max_workers": payload.get("max_workers"),
            "max_seconds": payload.get("max_seconds"),
            "max_test_seconds": payload.get("max_test_seconds"),
            "no_escalation": no_escalation,
            "reuse_mode": _optional_text(payload, "reuse_mode"),
            "test_command": test_command,
            "campaign_id": _optional_text(payload, "campaign_id"),
            "create_action_id": _optional_text(payload, "create_action_id"),
            "start_action_id": _optional_text(payload, "start_action_id"),
        }
        return request

    @staticmethod
    def _project_run_request(payload: Mapping[str, JsonValue]) -> ProjectRunCreateRequest:
        # Validate one one-click launch while leaving source partitioning to the project-run authority.
        project_id = _required_text(payload, "project_id")
        operators = payload.get("operators", [])
        if not isinstance(operators, list) or any(not isinstance(item, str) or not item.strip() for item in operators):
            raise ValueError("operators must be an array of non-empty strings")
        test_command = payload.get("test_command")
        if test_command is not None and not isinstance(test_command, (str, list)):
            raise ValueError("test_command must be text or an argv array")
        if isinstance(test_command, list) and any(not isinstance(item, str) or not item.strip() for item in test_command):
            raise ValueError("test_command must contain non-empty strings")
        no_escalation = payload.get("no_escalation")
        if no_escalation is not None and not isinstance(no_escalation, bool):
            raise ValueError("no_escalation must be boolean or null")
        return {
            "project_id": project_id,
            "operators": [item.strip() for item in operators],
            "max_mutants": payload.get("max_mutants"),
            "max_workers": payload.get("max_workers"),
            "max_seconds": payload.get("max_seconds"),
            "max_test_seconds": payload.get("max_test_seconds"),
            "no_escalation": no_escalation,
            "reuse_mode": _optional_text(payload, "reuse_mode"),
            "test_command": test_command,
        }
