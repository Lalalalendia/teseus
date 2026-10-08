"""Transport-neutral read service over durable Theseus local projections."""
from __future__ import annotations
import base64
import binascii
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, TypeVar
from .contracts import (
    ApiError,
    ApiFailed,
    ApiOutcome,
    ApiPage,
    ApiRejected,
    ApiSuccess,
    ArtifactDto,
    ArtifactRegistryDto,
    CampaignDetailDto,
    CampaignDto,
    DiagnosticBlockerDto,
    ExecutionDto,
    JsonScalar,
    JsonValue,
    KnowledgeDto,
    KnowledgeExecutionDto,
    LeaseDiagnosticDto,
    PlanDto,
    ProjectDto,
    RecoveryActionDto,
    RecoveryDiagnosticsDto,
    RecoveryDto,
    ReuseEvidenceDto,
    ShardDto,
    SpoolDeliveryDto,
    SpoolDiagnosticsDto,
    StatisticsDto,
    WorkerDiagnosticDto,
    WorkerDto,
)
LOCAL_API_SCHEMA_VERSION = 1
LOCAL_API_MAX_LIMIT = 200
_STATISTICS_ENTITY_TYPES = frozenset(("campaign", "test", "mutant", "worker", "execution"))
R = TypeVar("R")
class _RequestRejected(RuntimeError):
    """Internal control flow for stable expected API rejections."""
    def __init__(self, code: str, message: str, details: Mapping[str, JsonScalar] | None = None) -> None:
        # Retain only stable scalar diagnostics before converting the exception into an API outcome.
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = dict(details or {})
class _ReadFailure(RuntimeError):
    """Internal control flow for stable technical API failures."""
    def __init__(
        self,
        code: str,
        message: str,
        *,
        retriable: bool = False,
        details: Mapping[str, JsonScalar] | None = None,
    ) -> None:
        # Retain only stable diagnostics and never expose the original exception object.
        super().__init__(message)
        self.code = code
        self.message = message
        self.retriable = retriable
        self.details = dict(details or {})
def _non_empty(value: object, field_name: str) -> str:
    # Normalize one required public identifier and reject empty values before persistence access.
    if not isinstance(value, str) or not value.strip():
        raise _RequestRejected("invalid_request", f"{field_name} must be a non-empty string", {"field": field_name})
    return value.strip()
def _validate_limit(limit: int) -> int:
    # Reject booleans and requests outside the explicit local API page bound.
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise _RequestRejected("invalid_limit", "limit must be an integer", {"maximum": LOCAL_API_MAX_LIMIT})
    if limit < 1 or limit > LOCAL_API_MAX_LIMIT:
        raise _RequestRejected(
            "invalid_limit",
            f"limit must be between 1 and {LOCAL_API_MAX_LIMIT}",
            {"minimum": 1, "maximum": LOCAL_API_MAX_LIMIT},
        )
    return limit
def _encode_cursor(resource: str, *keys: str) -> str:
    # Encode one resource-scoped keyset position without exposing SQL columns.
    payload = json.dumps(
        {"schema_version": LOCAL_API_SCHEMA_VERSION, "resource": resource, "keys": list(keys)},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
def _decode_cursor(cursor: str | None, resource: str, key_count: int) -> tuple[str, ...] | None:
    # Decode and validate one resource-scoped cursor before constructing a keyset query.
    if cursor is None:
        return None
    if not isinstance(cursor, str) or not cursor.strip():
        raise _RequestRejected("invalid_cursor", "cursor must be a non-empty string")
    try:
        padding = "=" * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode((cursor + padding).encode("ascii"))
        value = json.loads(raw.decode("utf-8"))
    except (binascii.Error, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise _RequestRejected("invalid_cursor", "cursor is not a valid local API cursor") from exc
    if not isinstance(value, Mapping):
        raise _RequestRejected("invalid_cursor", "cursor payload must be an object")
    keys = value.get("keys")
    if (
        value.get("schema_version") != LOCAL_API_SCHEMA_VERSION
        or value.get("resource") != resource
        or not isinstance(keys, list)
        or len(keys) != key_count
        or any(not isinstance(item, str) for item in keys)
    ):
        raise _RequestRejected("invalid_cursor", "cursor does not match the requested resource")
    return tuple(keys)
def _mapping(value: object, field_name: str) -> Mapping[str, Any]:
    # Require one JSON object from a durable projection without accepting arbitrary runtime values.
    if not isinstance(value, Mapping):
        raise _ReadFailure("store_corrupted", f"{field_name} must be an object")
    return value
def _array(value: object, field_name: str) -> Sequence[Any]:
    # Require one JSON array from a durable projection without accepting strings as sequences.
    if not isinstance(value, (list, tuple)):
        raise _ReadFailure("store_corrupted", f"{field_name} must be an array")
    return value
def _load_payload(row: sqlite3.Row, column: str = "payload") -> Mapping[str, Any]:
    # Decode one persisted aggregate payload and convert malformed JSON into a stable failure.
    try:
        value = json.loads(str(row[column]))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise _ReadFailure("store_corrupted", "campaign database contains invalid JSON") from exc
    return _mapping(value, column)
def _optional_string(value: object) -> str | None:
    # Normalize one optional scalar identifier without converting nested values to strings.
    return value if isinstance(value, str) and value != "" else None
def _string(value: object, default: str = "") -> str:
    # Convert one persisted scalar label to a stable string while retaining an explicit default.
    if value is None:
        return default
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    raise _ReadFailure("store_corrupted", "persisted scalar field has an unsupported shape")
def _integer(value: object, default: int = 0) -> int:
    # Convert one persisted integer counter while rejecting booleans and malformed values.
    if value is None:
        return default
    if isinstance(value, bool):
        raise _ReadFailure("store_corrupted", "persisted integer field cannot be boolean")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise _ReadFailure("store_corrupted", "persisted integer field is invalid") from exc
def _float(value: object, default: float = 0.0) -> float:
    # Convert one persisted numeric aggregate while rejecting booleans and malformed values.
    if value is None:
        return default
    if isinstance(value, bool):
        raise _ReadFailure("store_corrupted", "persisted numeric field cannot be boolean")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise _ReadFailure("store_corrupted", "persisted numeric field is invalid") from exc
def _optional_float(value: object) -> float | None:
    # Convert one optional persisted numeric field without inventing a zero value.
    return None if value is None else _float(value)
_PRIVATE_METADATA_KEY_PARTS = ("path", "root", "workspace", "spool", "database", "directory")
def _json_value(value: object, *, hide_paths: bool = False) -> JsonValue:
    # Copy only JSON-compatible metadata and optionally remove filesystem-bearing keys recursively.
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        result: dict[str, JsonValue] = {}
        for key, item in value.items():
            name = str(key)
            if hide_paths and any(part in name.lower() for part in _PRIVATE_METADATA_KEY_PARTS):
                continue
            result[name] = _json_value(item, hide_paths=hide_paths)
        return result
    if isinstance(value, (list, tuple)):
        return [_json_value(item, hide_paths=hide_paths) for item in value]
    raise _ReadFailure("store_corrupted", "persisted metadata contains a non-JSON value")
def _project_dto(payload: Mapping[str, Any], *, campaign_count: int) -> ProjectDto:
    # Build one project read model from the campaign's public configuration projection.
    configuration = _mapping(payload.get("configuration", {}), "configuration")
    project = _mapping(configuration.get("project", {}), "configuration.project")
    revision = project.get("revision")
    revision_value = _mapping(revision, "configuration.project.revision") if revision is not None else {}
    project_id = _string(payload.get("project_id") or project.get("project_id"))
    if not project_id:
        raise _ReadFailure("store_corrupted", "campaign projection has no project_id")
    return ProjectDto(
        project_id=project_id,
        display_name=_string(project.get("display_name"), project_id),
        root_path=_optional_string(project.get("root_path")),
        main_root_path=_optional_string(project.get("main_root_path")),
        revision_id=_optional_string(revision_value.get("revision_id") or payload.get("revision_id")),
        campaign_count=max(0, int(campaign_count)),
    )
def _campaign_dto(payload: Mapping[str, Any]) -> CampaignDto:
    # Build one compact campaign DTO without returning the mutable aggregate object.
    scope = _mapping(payload.get("scope", {}), "scope")
    campaign_id = _string(payload.get("campaign_id"))
    project_id = _string(payload.get("project_id"))
    revision_id = _string(payload.get("revision_id"))
    if not campaign_id or not project_id or not revision_id:
        raise _ReadFailure("store_corrupted", "campaign identity fields are incomplete")
    return CampaignDto(
        campaign_id=campaign_id,
        project_id=project_id,
        revision_id=revision_id,
        status=_string(payload.get("status"), "unknown"),
        mode=_string(payload.get("mode"), "unknown"),
        plan_id=_optional_string(payload.get("plan_id")),
        prepared_snapshot_id=_optional_string(payload.get("prepared_snapshot_id")),
        source_path=_string(scope.get("source_path")),
        function=_optional_string(scope.get("function")),
        total_mutants=_integer(payload.get("total_mutants")),
        completed_mutants=_integer(payload.get("completed_mutants")),
        revision_number=_integer(payload.get("revision_number")),
    )
def _plan_dto(payload: Mapping[str, Any]) -> PlanDto | None:
    # Derive the committed plan read model from authoritative campaign binding fields only.
    plan_id = _optional_string(payload.get("plan_id"))
    if plan_id is None:
        return None
    return PlanDto(
        plan_id=plan_id,
        campaign_id=_string(payload.get("campaign_id")),
        prepared_snapshot_id=_optional_string(payload.get("prepared_snapshot_id")),
        selected_mutants=_integer(payload.get("total_mutants")),
        campaign_status=_string(payload.get("status"), "unknown"),
    )
def _worker_dto(payload: Mapping[str, Any]) -> WorkerDto:
    # Convert one worker aggregate projection without exposing workspace, spool or birth-token paths.
    identity = _mapping(payload.get("identity", {}), "worker.identity")
    capabilities = _mapping(payload.get("capabilities", {}), "worker.capabilities")
    python_versions = tuple(_string(item) for item in _array(capabilities.get("python_versions", []), "python_versions"))
    protocol_versions = tuple(
        _integer(item) for item in _array(capabilities.get("engine_protocol_versions", []), "engine_protocol_versions")
    )
    backends = tuple(_string(item) for item in _array(capabilities.get("workspace_backends", []), "workspace_backends"))
    return WorkerDto(
        campaign_id=_string(payload.get("campaign_id")),
        worker_id=_string(identity.get("worker_id")),
        instance_id=_string(identity.get("instance_id")),
        process_id=_integer(identity.get("process_id")),
        status=_string(payload.get("status"), "unknown"),
        heartbeat_sequence=_integer(payload.get("heartbeat_sequence")),
        last_heartbeat_at=_string(payload.get("last_heartbeat_at")),
        current_shard_id=_optional_string(payload.get("current_shard_id")),
        current_lease_id=_optional_string(payload.get("current_lease_id")),
        current_attempt=_integer(payload.get("current_attempt")) if payload.get("current_attempt") is not None else None,
        current_mutant_id=_optional_string(payload.get("current_mutant_id")),
        child_process_id=_integer(payload.get("child_process_id")) if payload.get("child_process_id") is not None else None,
        completed_mutants=_integer(payload.get("completed_mutants")),
        completed_assignments=_integer(payload.get("completed_assignments")),
        workspace_healthy=bool(payload.get("workspace_healthy", True)),
        revision_number=_integer(payload.get("revision_number")),
        platform=_string(capabilities.get("platform")),
        architecture=_string(capabilities.get("architecture")),
        python_versions=python_versions,
        engine_protocol_versions=protocol_versions,
        workspace_backends=backends,
        cpu_count=_integer(capabilities.get("cpu_count")),
        memory_limit_bytes=(
            _integer(capabilities.get("memory_limit_bytes"))
            if capabilities.get("memory_limit_bytes") is not None
            else None
        ),
    )
def _shard_dto(payload: Mapping[str, Any]) -> ShardDto:
    # Convert one shard aggregate into bounded ownership and progress fields.
    lease = payload.get("lease")
    lease_value = _mapping(lease, "shard.lease") if lease is not None else {}
    mutant_ids = _array(payload.get("mutant_ids", []), "shard.mutant_ids")
    return ShardDto(
        shard_id=_string(payload.get("shard_id")),
        campaign_id=_string(payload.get("campaign_id")),
        plan_id=_string(payload.get("plan_id")),
        ordinal=_integer(payload.get("ordinal")),
        status=_string(payload.get("status"), "unknown"),
        worker_id=_optional_string(payload.get("worker_id")),
        lease_id=_optional_string(lease_value.get("lease_id")),
        attempt=_integer(payload.get("attempt")),
        mutant_count=len(mutant_ids),
        completed_count=_integer(payload.get("completed_count")),
        estimated_cost=_float(payload.get("estimated_cost")),
        revision_number=_integer(payload.get("revision_number")),
    )
def _killer_test_id(observations: Sequence[Mapping[str, Any]]) -> str | None:
    # Expose only an explicit first-failure pytest event and never infer a killer from observation order.
    for observation in observations:
        if observation.get("first_failure") is not True:
            continue
        if _string(observation.get("evidence_kind")) != "pytest_test_event":
            continue
        if _string(observation.get("outcome")).lower() not in {"fail", "failed", "failure"}:
            continue
        test_id = _optional_string(observation.get("test_id"))
        if test_id is not None:
            return test_id
    return None
def _execution_dto(payload: Mapping[str, Any]) -> ExecutionDto:
    # Convert one immutable execution projection into bounded report evidence without returning nested arrays.
    selected_tests = _array(payload.get("selected_tests", []), "execution.selected_tests")
    artifacts = _array(payload.get("artifacts", []), "execution.artifacts")
    observations = tuple(
        _mapping(item, "execution.test_observations[]")
        for item in _array(payload.get("test_observations", []), "execution.test_observations")
    )
    semantic_result = _optional_string(payload.get("semantic_result"))
    return ExecutionDto(
        execution_id=_string(payload.get("execution_id")),
        campaign_id=_string(payload.get("campaign_id")),
        shard_id=_string(payload.get("shard_id")),
        mutant_id=_string(payload.get("mutant_id")),
        attempt=_integer(payload.get("attempt")),
        status=_string(payload.get("status"), "unknown"),
        semantic_result=semantic_result,
        killer_test_id=_killer_test_id(observations) if semantic_result == "killed" else None,
        duration_seconds=_optional_float(payload.get("duration_seconds")),
        restore_verified=bool(payload.get("restore_verified", False)),
        error=_optional_string(payload.get("error")),
        selected_test_count=len(selected_tests),
        artifact_count=len(artifacts),
        observation_count=len(observations),
        revision_number=_integer(payload.get("revision_number")),
        lease_id=_optional_string(payload.get("lease_id")),
    )
def _artifact_dto(payload: Mapping[str, Any]) -> ArtifactDto:
    # Convert one registry entry while deliberately omitting content_path and logical_path.
    metadata = _mapping(payload.get("metadata", {}), "artifact.metadata")
    return ArtifactDto(
        campaign_id=_string(payload.get("campaign_id")),
        logical_key=_string(payload.get("logical_key")),
        logical_role=_string(payload.get("logical_role")),
        content_sha256=_string(payload.get("content_sha256")),
        size_bytes=_integer(payload.get("size_bytes")),
        schema_version=_integer(payload.get("schema_version")),
        producer=_string(payload.get("producer")),
        created_at=_string(payload.get("created_at")),
        shard_id=_optional_string(payload.get("shard_id")),
        execution_id=_optional_string(payload.get("execution_id")),
        metadata={str(key): _json_value(value, hide_paths=True) for key, value in metadata.items() if not any(part in str(key).lower() for part in _PRIVATE_METADATA_KEY_PARTS)},
    )
def _knowledge_execution_dto(row: Mapping[str, Any]) -> KnowledgeExecutionDto:
    # Convert one Knowledge Plane query row without including its optional raw payload.
    return KnowledgeExecutionDto(
        event_id=_string(row.get("event_id")),
        effect_id=_string(row.get("effect_id")),
        campaign_id=_string(row.get("campaign_id")),
        execution_id=_string(row.get("execution_id")),
        mutant_id=_string(row.get("mutant_id")),
        attempt=_integer(row.get("attempt")),
        status=_string(row.get("status"), "unknown"),
        created_at=_string(row.get("created_at")),
        compacted=bool(row.get("compacted", False)),
        function_id=_optional_string(row.get("function_id")),
        source_kind=_optional_string(row.get("source_kind")),
        evidence_quality=_optional_string(row.get("evidence_quality")),
        evidence_schema_version=_integer(row.get("evidence_schema_version")),
        retention_class=_optional_string(row.get("retention_class")),
        project_id=_string(row.get("project_id")),
        revision_id=_string(row.get("revision_id")),
        environment_id=_string(row.get("environment_id")),
    )
def _field(value: object, name: str, default: object = None) -> object:
    # Read one field from either a public projection object or its mapping representation.
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)
def _statistics_dto(value: object) -> StatisticsDto:
    # Convert one Statistics projection object into an explicit transport-neutral DTO.
    return StatisticsDto(
        entity_type=_string(_field(value, "entity_type")),
        entity_id=_string(_field(value, "entity_id")),
        event_count=_integer(_field(value, "event_count")),
        started_count=_integer(_field(value, "started_count")),
        completed_count=_integer(_field(value, "completed_count")),
        passed_count=_integer(_field(value, "passed_count")),
        failed_count=_integer(_field(value, "failed_count")),
        error_count=_integer(_field(value, "error_count")),
        timeout_count=_integer(_field(value, "timeout_count")),
        retry_count=_integer(_field(value, "retry_count")),
        recovery_count=_integer(_field(value, "recovery_count")),
        escalation_count=_integer(_field(value, "escalation_count")),
        reuse_count=_integer(_field(value, "reuse_count")),
        infrastructure_failure_count=_integer(_field(value, "infrastructure_failure_count")),
        flaky_transition_count=_integer(_field(value, "flaky_transition_count")),
        duration_count=_integer(_field(value, "duration_count")),
        duration_total_ms=_float(_field(value, "duration_total_ms")),
        duration_min_ms=_optional_float(_field(value, "duration_min_ms")),
        duration_max_ms=_optional_float(_field(value, "duration_max_ms")),
        duration_avg_ms=_float(_field(value, "duration_avg_ms")),
        duration_median_ms=_float(_field(value, "duration_median_ms")),
        duration_p95_ms=_float(_field(value, "duration_p95_ms")),
        duration_sample_count=_integer(_field(value, "duration_sample_count")),
        busy_duration_ms=_float(_field(value, "busy_duration_ms")),
        active_duration_ms=_float(_field(value, "active_duration_ms")),
        utilization=_float(_field(value, "utilization")),
        active=bool(_field(value, "active", False)),
        last_outcome=_optional_string(_field(value, "last_outcome")),
        last_event_type=_string(_field(value, "last_event_type")),
        last_event_timestamp=_string(_field(value, "last_event_timestamp")),
    )
def _recovery_action_dto(sequence: int, value: Mapping[str, Any]) -> RecoveryActionDto:
    # Split common recovery action fields from bounded JSON-compatible diagnostics.
    known = {"campaign_id", "status", "action", "operation", "type"}
    action = value.get("action") or value.get("operation") or value.get("type")
    return RecoveryActionDto(
        sequence=sequence,
        campaign_id=_optional_string(value.get("campaign_id")),
        status=_string(value.get("status"), "unknown"),
        action=_optional_string(action),
        details={str(key): _json_value(item, hide_paths=True) for key, item in value.items() if key not in known and not any(part in str(key).lower() for part in _PRIVATE_METADATA_KEY_PARTS)},
    )
class LocalApiService:
    """Stable local read boundary over campaign, knowledge, statistics and recovery projections."""
    def __init__(
        self,
        campaign_database: str | Path,
        *,
        knowledge_database: str | Path | None = None,
        statistics_database: str | Path | None = None,
        knowledge_store_factory: Callable[[Path], object] | None = None,
        statistics_store_factory: Callable[[Path], object] | None = None,
        recovery_path_resolver: Callable[[Path], Path] | None = None,
        query_observer: Callable[[str], None] | None = None,
    ) -> None:
        # Resolve infrastructure paths once while keeping all public results path-free.
        self._campaign_database = Path(campaign_database).expanduser().resolve()
        self._knowledge_database = Path(knowledge_database).expanduser().resolve() if knowledge_database else None
        self._statistics_database = Path(statistics_database).expanduser().resolve() if statistics_database else None
        self._knowledge_store_factory = knowledge_store_factory
        self._statistics_store_factory = statistics_store_factory
        self._recovery_path_resolver = recovery_path_resolver
        self._query_observer = query_observer
    def _execute(self, operation: Callable[[], R]) -> ApiOutcome[R]:
        # Convert request, persistence and decoding failures into stable typed outcomes.
        try:
            return ApiSuccess(operation())
        except _RequestRejected as exc:
            return ApiRejected(ApiError(exc.code, exc.message, details=exc.details))
        except _ReadFailure as exc:
            return ApiFailed(
                ApiError(
                    exc.code,
                    exc.message,
                    retriable=exc.retriable,
                    details=exc.details,
                )
            )
        except sqlite3.DatabaseError:
            return ApiFailed(ApiError("store_read_failed", "campaign database read failed", retriable=True))
        except (OSError, UnicodeError):
            return ApiFailed(ApiError("state_read_failed", "durable local state could not be read", retriable=True))
        except Exception:
            return ApiFailed(ApiError("local_api_read_failed", "local API read failed"))
    def _connect_campaign(self) -> sqlite3.Connection:
        # Open the existing campaign database in query-only mode without creating missing state.
        if not self._campaign_database.is_file():
            raise _ReadFailure(
                "campaign_store_unavailable",
                "campaign database does not exist",
                details={"database": self._campaign_database.name},
            )
        connection = sqlite3.connect(self._campaign_database, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=5000")
        if self._query_observer is not None:
            connection.set_trace_callback(self._query_observer)
        return connection
    @staticmethod
    def _page(rows: Sequence[sqlite3.Row], *, limit: int, builder: Callable[[sqlite3.Row], R], cursor: str | None) -> ApiPage[R]:
        # Materialize at most one bounded page and retain a cursor only when another row exists.
        visible = rows[:limit]
        return ApiPage(tuple(builder(row) for row in visible), limit, cursor if len(rows) > limit else None)
    def _campaign_payload(self, connection: sqlite3.Connection, campaign_id: str) -> tuple[Mapping[str, Any], int]:
        # Load one campaign and its project campaign count in a fixed single query.
        row = connection.execute(
            """
            SELECT c.payload,
                   (SELECT COUNT(*) FROM mutation_campaigns AS related
                    WHERE json_extract(related.payload, '$.project_id') = json_extract(c.payload, '$.project_id'))
                   AS project_campaign_count
            FROM mutation_campaigns AS c
            WHERE c.campaign_id = ?
            """,
            (campaign_id,),
        ).fetchone()
        if row is None:
            raise _RequestRejected("not_found", "campaign does not exist", {"campaign_id": campaign_id})
        return _load_payload(row), _integer(row["project_campaign_count"])
    def _projects_page(self, connection: sqlite3.Connection, *, limit: int, cursor: str | None) -> ApiPage[ProjectDto]:
        # Query one keyset page of distinct projects through representative campaign projections.
        position = _decode_cursor(cursor, "projects", 1)
        clauses = ["representative.project_id IS NOT NULL", "representative.project_id != ''"]
        parameters: list[object] = []
        if position is not None:
            clauses.append("representative.project_id > ?")
            parameters.append(position[0])
        parameters.append(limit + 1)
        rows = connection.execute(
            f"""
            WITH representative AS (
                SELECT json_extract(payload, '$.project_id') AS project_id,
                       MIN(campaign_id) AS campaign_id,
                       COUNT(*) AS campaign_count
                FROM mutation_campaigns
                GROUP BY json_extract(payload, '$.project_id')
            )
            SELECT representative.project_id, representative.campaign_count, campaigns.payload
            FROM representative
            JOIN mutation_campaigns AS campaigns ON campaigns.campaign_id = representative.campaign_id
            WHERE {' AND '.join(clauses)}
            ORDER BY representative.project_id ASC
            LIMIT ?
            """,
            parameters,
        ).fetchall()
        next_cursor = _encode_cursor("projects", str(rows[limit - 1]["project_id"])) if len(rows) > limit else None
        return self._page(
            rows,
            limit=limit,
            builder=lambda row: _project_dto(_load_payload(row), campaign_count=_integer(row["campaign_count"])),
            cursor=next_cursor,
        )
    def _campaigns_page(
        self,
        connection: sqlite3.Connection,
        *,
        limit: int,
        cursor: str | None,
        project_id: str | None,
    ) -> ApiPage[CampaignDto]:
        # Query one campaign-ID keyset page with an optional authoritative project filter.
        position = _decode_cursor(cursor, "campaigns", 1)
        clauses: list[str] = []
        parameters: list[object] = []
        if position is not None:
            clauses.append("campaign_id > ?")
            parameters.append(position[0])
        if project_id is not None:
            clauses.append("json_extract(payload, '$.project_id') = ?")
            parameters.append(_non_empty(project_id, "project_id"))
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        parameters.append(limit + 1)
        rows = connection.execute(
            f"SELECT campaign_id, payload FROM mutation_campaigns {where} ORDER BY campaign_id ASC LIMIT ?",
            parameters,
        ).fetchall()
        next_cursor = _encode_cursor("campaigns", str(rows[limit - 1]["campaign_id"])) if len(rows) > limit else None
        return self._page(rows, limit=limit, builder=lambda row: _campaign_dto(_load_payload(row)), cursor=next_cursor)
    def _plans_page(
        self,
        connection: sqlite3.Connection,
        *,
        limit: int,
        cursor: str | None,
        campaign_id: str | None,
    ) -> ApiPage[PlanDto]:
        # Query committed plans by a stable plan-ID and campaign-ID composite keyset.
        position = _decode_cursor(cursor, "plans", 2)
        clauses = ["plan_id IS NOT NULL", "plan_id != ''"]
        parameters: list[object] = []
        if position is not None:
            clauses.append("(plan_id > ? OR (plan_id = ? AND campaign_id > ?))")
            parameters.extend((position[0], position[0], position[1]))
        if campaign_id is not None:
            clauses.append("campaign_id = ?")
            parameters.append(_non_empty(campaign_id, "campaign_id"))
        parameters.append(limit + 1)
        rows = connection.execute(
            f"""
            WITH plans AS (
                SELECT campaign_id, payload, json_extract(payload, '$.plan_id') AS plan_id
                FROM mutation_campaigns
            )
            SELECT campaign_id, plan_id, payload
            FROM plans
            WHERE {' AND '.join(clauses)}
            ORDER BY plan_id ASC, campaign_id ASC
            LIMIT ?
            """,
            parameters,
        ).fetchall()
        next_cursor = (
            _encode_cursor("plans", str(rows[limit - 1]["plan_id"]), str(rows[limit - 1]["campaign_id"]))
            if len(rows) > limit
            else None
        )
        def build(row: sqlite3.Row) -> PlanDto:
            # Require the plan binding selected by SQL to remain present in its aggregate payload.
            value = _plan_dto(_load_payload(row))
            if value is None:
                raise _ReadFailure("store_corrupted", "plan query returned an unbound campaign")
            return value
        return self._page(rows, limit=limit, builder=build, cursor=next_cursor)
    def _workers_page(
        self,
        connection: sqlite3.Connection,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None,
    ) -> ApiPage[WorkerDto]:
        # Query one campaign-scoped worker-key page without loading unrelated registrations.
        position = _decode_cursor(cursor, "workers", 1)
        clauses = ["json_extract(payload, '$.campaign_id') = ?"]
        parameters: list[object] = [campaign_id]
        if position is not None:
            clauses.append("worker_key > ?")
            parameters.append(position[0])
        parameters.append(limit + 1)
        rows = connection.execute(
            f"SELECT worker_key, payload FROM mutation_workers WHERE {' AND '.join(clauses)} ORDER BY worker_key ASC LIMIT ?",
            parameters,
        ).fetchall()
        next_cursor = _encode_cursor("workers", str(rows[limit - 1]["worker_key"])) if len(rows) > limit else None
        return self._page(rows, limit=limit, builder=lambda row: _worker_dto(_load_payload(row)), cursor=next_cursor)
    def _shards_page(
        self,
        connection: sqlite3.Connection,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None,
    ) -> ApiPage[ShardDto]:
        # Query one campaign-scoped shard-ID keyset page from the authoritative topology projection.
        position = _decode_cursor(cursor, "shards", 1)
        clauses = ["json_extract(payload, '$.campaign_id') = ?"]
        parameters: list[object] = [campaign_id]
        if position is not None:
            clauses.append("shard_id > ?")
            parameters.append(position[0])
        parameters.append(limit + 1)
        rows = connection.execute(
            f"SELECT shard_id, payload FROM mutation_shards WHERE {' AND '.join(clauses)} ORDER BY shard_id ASC LIMIT ?",
            parameters,
        ).fetchall()
        next_cursor = _encode_cursor("shards", str(rows[limit - 1]["shard_id"])) if len(rows) > limit else None
        return self._page(rows, limit=limit, builder=lambda row: _shard_dto(_load_payload(row)), cursor=next_cursor)
    def _executions_page(
        self,
        connection: sqlite3.Connection,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None,
    ) -> ApiPage[ExecutionDto]:
        # Query one campaign-scoped execution-ID keyset page without materializing nested evidence.
        position = _decode_cursor(cursor, "executions", 1)
        clauses = ["json_extract(payload, '$.campaign_id') = ?"]
        parameters: list[object] = [campaign_id]
        if position is not None:
            clauses.append("execution_id > ?")
            parameters.append(position[0])
        parameters.append(limit + 1)
        rows = connection.execute(
            f"SELECT execution_id, payload FROM mutation_executions WHERE {' AND '.join(clauses)} ORDER BY execution_id ASC LIMIT ?",
            parameters,
        ).fetchall()
        next_cursor = _encode_cursor("executions", str(rows[limit - 1]["execution_id"])) if len(rows) > limit else None
        return self._page(rows, limit=limit, builder=lambda row: _execution_dto(_load_payload(row)), cursor=next_cursor)
    def _artifacts_page(
        self,
        connection: sqlite3.Connection,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None,
    ) -> ApiPage[ArtifactDto]:
        # Query one logical-key page of artifact metadata without exposing registry paths.
        position = _decode_cursor(cursor, "artifacts", 1)
        clauses = ["campaign_id = ?"]
        parameters: list[object] = [campaign_id]
        if position is not None:
            clauses.append("logical_key > ?")
            parameters.append(position[0])
        parameters.append(limit + 1)
        rows = connection.execute(
            f"SELECT logical_key, payload FROM mutation_artifacts WHERE {' AND '.join(clauses)} ORDER BY logical_key ASC LIMIT ?",
            parameters,
        ).fetchall()
        next_cursor = _encode_cursor("artifacts", str(rows[limit - 1]["logical_key"])) if len(rows) > limit else None
        return self._page(rows, limit=limit, builder=lambda row: _artifact_dto(_load_payload(row)), cursor=next_cursor)
    def list_projects(self, *, limit: int, cursor: str | None = None) -> ApiOutcome[ApiPage[ProjectDto]]:
        # Return one bounded project page derived from durable campaign state.
        def operation() -> ApiPage[ProjectDto]:
            # Execute the project query in one short read transaction.
            page_limit = _validate_limit(limit)
            connection = self._connect_campaign()
            try:
                connection.execute("BEGIN")
                return self._projects_page(connection, limit=page_limit, cursor=cursor)
            finally:
                connection.close()
        return self._execute(operation)
    def list_campaigns(
        self,
        *,
        limit: int,
        cursor: str | None = None,
        project_id: str | None = None,
    ) -> ApiOutcome[ApiPage[CampaignDto]]:
        # Return one bounded campaign page with an optional project identity filter.
        def operation() -> ApiPage[CampaignDto]:
            # Execute the campaign query in one short read transaction.
            page_limit = _validate_limit(limit)
            connection = self._connect_campaign()
            try:
                connection.execute("BEGIN")
                return self._campaigns_page(
                    connection,
                    limit=page_limit,
                    cursor=cursor,
                    project_id=project_id,
                )
            finally:
                connection.close()
        return self._execute(operation)
    def list_plans(
        self,
        *,
        limit: int,
        cursor: str | None = None,
        campaign_id: str | None = None,
    ) -> ApiOutcome[ApiPage[PlanDto]]:
        # Return one bounded page of committed plan bindings without importing planner internals.
        def operation() -> ApiPage[PlanDto]:
            # Execute the plan query in one short read transaction.
            page_limit = _validate_limit(limit)
            connection = self._connect_campaign()
            try:
                connection.execute("BEGIN")
                return self._plans_page(
                    connection,
                    limit=page_limit,
                    cursor=cursor,
                    campaign_id=campaign_id,
                )
            finally:
                connection.close()
        return self._execute(operation)
    def list_workers(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ApiOutcome[ApiPage[WorkerDto]]:
        # Return one bounded campaign worker page from authoritative SQLite state.
        def operation() -> ApiPage[WorkerDto]:
            # Validate scope before opening one short read transaction.
            current_campaign = _non_empty(campaign_id, "campaign_id")
            page_limit = _validate_limit(limit)
            connection = self._connect_campaign()
            try:
                connection.execute("BEGIN")
                self._campaign_payload(connection, current_campaign)
                return self._workers_page(connection, current_campaign, limit=page_limit, cursor=cursor)
            finally:
                connection.close()
        return self._execute(operation)
    def list_shards(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ApiOutcome[ApiPage[ShardDto]]:
        # Return one bounded campaign shard page from the committed topology projection.
        def operation() -> ApiPage[ShardDto]:
            # Validate scope before opening one short read transaction.
            current_campaign = _non_empty(campaign_id, "campaign_id")
            page_limit = _validate_limit(limit)
            connection = self._connect_campaign()
            try:
                connection.execute("BEGIN")
                self._campaign_payload(connection, current_campaign)
                return self._shards_page(connection, current_campaign, limit=page_limit, cursor=cursor)
            finally:
                connection.close()
        return self._execute(operation)
    def list_executions(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ApiOutcome[ApiPage[ExecutionDto]]:
        # Return one bounded campaign execution page without nested evidence payloads.
        def operation() -> ApiPage[ExecutionDto]:
            # Validate scope before opening one short read transaction.
            current_campaign = _non_empty(campaign_id, "campaign_id")
            page_limit = _validate_limit(limit)
            connection = self._connect_campaign()
            try:
                connection.execute("BEGIN")
                self._campaign_payload(connection, current_campaign)
                return self._executions_page(connection, current_campaign, limit=page_limit, cursor=cursor)
            finally:
                connection.close()
        return self._execute(operation)
    def list_artifacts(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ApiOutcome[ApiPage[ArtifactDto]]:
        # Return one bounded artifact registry page without requiring client filesystem access.
        def operation() -> ApiPage[ArtifactDto]:
            # Validate scope before opening one short read transaction.
            current_campaign = _non_empty(campaign_id, "campaign_id")
            page_limit = _validate_limit(limit)
            connection = self._connect_campaign()
            try:
                connection.execute("BEGIN")
                self._campaign_payload(connection, current_campaign)
                return self._artifacts_page(connection, current_campaign, limit=page_limit, cursor=cursor)
            finally:
                connection.close()
        return self._execute(operation)
    def get_campaign(
        self,
        campaign_id: str,
        *,
        related_limit: int,
        shards_cursor: str | None = None,
        workers_cursor: str | None = None,
        executions_cursor: str | None = None,
        artifacts_cursor: str | None = None,
    ) -> ApiOutcome[CampaignDetailDto]:
        # Return campaign detail and all related pages from one consistent fixed-query SQLite snapshot.
        def operation() -> CampaignDetailDto:
            # Read the aggregate and each bounded relation once without per-row database calls.
            current_campaign = _non_empty(campaign_id, "campaign_id")
            page_limit = _validate_limit(related_limit)
            connection = self._connect_campaign()
            try:
                connection.execute("BEGIN")
                payload, project_campaign_count = self._campaign_payload(connection, current_campaign)
                shards = self._shards_page(connection, current_campaign, limit=page_limit, cursor=shards_cursor)
                workers = self._workers_page(connection, current_campaign, limit=page_limit, cursor=workers_cursor)
                executions = self._executions_page(
                    connection,
                    current_campaign,
                    limit=page_limit,
                    cursor=executions_cursor,
                )
                artifacts = self._artifacts_page(
                    connection,
                    current_campaign,
                    limit=page_limit,
                    cursor=artifacts_cursor,
                )
                finalization = connection.execute(
                    "SELECT status FROM mutation_finalizations WHERE campaign_id = ?",
                    (current_campaign,),
                ).fetchone()
                return CampaignDetailDto(
                    campaign=_campaign_dto(payload),
                    project=_project_dto(payload, campaign_count=project_campaign_count),
                    plan=_plan_dto(payload),
                    shards=shards,
                    workers=workers,
                    executions=executions,
                    artifacts=artifacts,
                    finalization_status=str(finalization["status"]) if finalization is not None else None,
                )
            finally:
                connection.close()
        return self._execute(operation)
    def _resolve_knowledge_database(self, project_id: str) -> Path:
        # Resolve explicit, current and legacy knowledge locations without exposing them to callers.
        if self._knowledge_database is not None:
            return self._knowledge_database
        state_root = self._campaign_database.parent.parent.parent
        current = state_root / "knowledge" / f"{project_id}.sqlite3"
        legacy = self._campaign_database.parent / "knowledge.sqlite3"
        return current if current.is_file() or not legacy.is_file() else legacy
    def _open_knowledge_store(self, path: Path) -> object:
        # Open the existing Knowledge Plane through its public store boundary or an injected test adapter.
        if self._knowledge_store_factory is not None:
            return self._knowledge_store_factory(path)
        from theseus_knowledge import KnowledgePlaneStore
        return KnowledgePlaneStore(path)
    def get_knowledge(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
        include_compacted: bool = False,
    ) -> ApiOutcome[KnowledgeDto]:
        # Return campaign knowledge counters and one bounded execution page through KnowledgePlaneStore.
        def operation() -> KnowledgeDto:
            # Resolve campaign ownership before opening the separate project-level knowledge store.
            current_campaign = _non_empty(campaign_id, "campaign_id")
            page_limit = _validate_limit(limit)
            connection = self._connect_campaign()
            try:
                payload, _ = self._campaign_payload(connection, current_campaign)
            finally:
                connection.close()
            project_id = _campaign_dto(payload).project_id
            path = self._resolve_knowledge_database(project_id)
            if not path.is_file() and self._knowledge_store_factory is None:
                raise _RequestRejected("not_found", "knowledge projection does not exist", {"campaign_id": current_campaign})
            store = self._open_knowledge_store(path)
            try:
                summary = store.summarize_campaign(current_campaign)
                try:
                    page = store.query_executions(
                        campaign_id=current_campaign,
                        cursor=cursor,
                        limit=page_limit,
                        include_payload=False,
                        include_compacted=bool(include_compacted),
                    )
                except ValueError as exc:
                    raise _RequestRejected("invalid_cursor", "knowledge cursor is invalid") from exc
                rows = tuple(_knowledge_execution_dto(_mapping(row, "knowledge record")) for row in page.rows)
                return KnowledgeDto(
                    campaign_id=current_campaign,
                    executions=_integer(_field(summary, "executions")),
                    mutants=_integer(_field(summary, "mutants")),
                    attempts=_integer(_field(summary, "attempts")),
                    counts={
                        str(key): _integer(item)
                        for key, item in _mapping(_field(summary, "counts", {}), "knowledge counts").items()
                    },
                    revision=_integer(_field(summary, "revision")),
                    snapshot_revision=_integer(_field(page, "snapshot_revision")),
                    schema_version=_integer(_field(page, "schema_version"), 1),
                    records=ApiPage(rows, page_limit, _optional_string(_field(page, "next_cursor"))),
                )
            finally:
                close = getattr(store, "close", None)
                if callable(close):
                    close()
        return self._execute(operation)
    def _resolve_statistics_database(self) -> Path:
        # Resolve explicit or campaign-local statistics projection storage without exposing the path.
        if self._statistics_database is not None:
            return self._statistics_database
        candidates = (
            self._campaign_database.parent / "statistics.sqlite",
            self._campaign_database.parent / "statistics.sqlite3",
            self._campaign_database.parent.parent / "statistics.sqlite",
        )
        return next((path for path in candidates if path.is_file()), candidates[0])
    def _open_statistics_store(self, path: Path) -> object:
        # Open the existing Statistics projection through its public store boundary or an injected adapter.
        if self._statistics_store_factory is not None:
            return self._statistics_store_factory(path)
        from theseus_statistics import StatisticsEventStore, StatisticsProjectionStore
        return StatisticsProjectionStore(StatisticsEventStore(path))
    def list_statistics(
        self,
        entity_type: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ApiOutcome[ApiPage[StatisticsDto]]:
        # Return one bounded keyset page through StatisticsProjectionStore without rebuilding projections.
        def operation() -> ApiPage[StatisticsDto]:
            # Validate the public statistics domain before opening the projection store.
            current_type = _non_empty(entity_type, "entity_type")
            if current_type not in _STATISTICS_ENTITY_TYPES:
                raise _RequestRejected("invalid_request", "unsupported statistics entity_type", {"entity_type": current_type})
            page_limit = _validate_limit(limit)
            path = self._resolve_statistics_database()
            if not path.is_file() and self._statistics_store_factory is None:
                raise _RequestRejected("not_found", "statistics projection does not exist")
            store = self._open_statistics_store(path)
            try:
                page = store.query(current_type, limit=page_limit, cursor=cursor)
            except ValueError as exc:
                raise _RequestRejected("invalid_cursor", "statistics cursor is invalid") from exc
            return ApiPage(
                tuple(_statistics_dto(item) for item in page.items),
                page_limit,
                _optional_string(page.next_cursor),
            )
        return self._execute(operation)
    def get_statistics(self, entity_type: str, entity_id: str) -> ApiOutcome[StatisticsDto]:
        # Return one exact Statistics projection without mutating or rebuilding derived state.
        def operation() -> StatisticsDto:
            # Validate the entity identity before opening the projection store.
            current_type = _non_empty(entity_type, "entity_type")
            current_id = _non_empty(entity_id, "entity_id")
            if current_type not in _STATISTICS_ENTITY_TYPES:
                raise _RequestRejected("invalid_request", "unsupported statistics entity_type", {"entity_type": current_type})
            path = self._resolve_statistics_database()
            if not path.is_file() and self._statistics_store_factory is None:
                raise _RequestRejected("not_found", "statistics projection does not exist")
            store = self._open_statistics_store(path)
            try:
                value = store.get(current_type, current_id)
            except ValueError as exc:
                raise _RequestRejected("invalid_request", "statistics identity is invalid") from exc
            if value is None:
                raise _RequestRejected(
                    "not_found",
                    "statistics projection does not exist",
                    {"entity_type": current_type, "entity_id": current_id},
                )
            return _statistics_dto(value)
        return self._execute(operation)
    def _resolve_recovery_path(self) -> Path:
        # Resolve the canonical recovery report through the existing helper or an injected adapter.
        if self._recovery_path_resolver is not None:
            return Path(self._recovery_path_resolver(self._campaign_database)).resolve()
        from theseus_local.startup_recovery import recovery_report_path
        return recovery_report_path(self._campaign_database)
    def get_recovery(self, *, limit: int, cursor: str | None = None) -> ApiOutcome[RecoveryDto]:
        # Return the bounded startup-recovery report without exposing its filesystem location.
        def operation() -> RecoveryDto:
            # Read one already bounded report and page actions by their stable cumulative sequence.
            page_limit = _validate_limit(limit)
            position = _decode_cursor(cursor, "recovery", 1)
            start_after = 0
            if position is not None:
                try:
                    start_after = int(position[0])
                except ValueError as exc:
                    raise _RequestRejected("invalid_cursor", "recovery cursor is invalid") from exc
                if start_after < 0:
                    raise _RequestRejected("invalid_cursor", "recovery cursor is invalid")
            path = self._resolve_recovery_path()
            if not path.is_file():
                return RecoveryDto(
                    available=False,
                    status="not_available",
                    started_at=None,
                    completed_at=None,
                    run_count=0,
                    action_count=0,
                    error=None,
                    actions=ApiPage((), page_limit, None),
                )
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise _ReadFailure("recovery_report_corrupted", "startup recovery report contains invalid JSON") from exc
            report = _mapping(raw, "recovery report")
            actions = _array(report.get("actions", []), "recovery actions")
            selected: list[RecoveryActionDto] = []
            for sequence, item in enumerate(actions, start=1):
                if sequence <= start_after:
                    continue
                selected.append(_recovery_action_dto(sequence, _mapping(item, "recovery action")))
                if len(selected) >= page_limit + 1:
                    break
            visible = tuple(selected[:page_limit])
            next_cursor = _encode_cursor("recovery", str(visible[-1].sequence)) if len(selected) > page_limit else None
            return RecoveryDto(
                available=True,
                status=_string(report.get("status"), "unknown"),
                started_at=_optional_string(report.get("started_at")),
                completed_at=_optional_string(report.get("completed_at")),
                run_count=_integer(report.get("run_count")),
                action_count=_integer(report.get("action_count"), len(actions)),
                error=_optional_string(report.get("error")),
                actions=ApiPage(visible, page_limit, next_cursor),
            )
        return self._execute(operation)
    def get_artifact_registry(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ApiOutcome[ArtifactRegistryDto]:
        # Return registry metadata and finalization state from one bounded campaign snapshot.
        def operation() -> ArtifactRegistryDto:
            # Read only immutable registry rows and omit every physical or logical path.
            current_campaign = _non_empty(campaign_id, "campaign_id")
            page_limit = _validate_limit(limit)
            connection = self._connect_campaign()
            try:
                connection.execute("BEGIN")
                self._campaign_payload(connection, current_campaign)
                artifacts = self._artifacts_page(connection, current_campaign, limit=page_limit, cursor=cursor)
                row = connection.execute(
                    "SELECT status FROM mutation_finalizations WHERE campaign_id = ?",
                    (current_campaign,),
                ).fetchone()
                return ArtifactRegistryDto(
                    finalization_status=str(row["status"]) if row is not None else None,
                    artifacts=artifacts,
                )
            finally:
                connection.close()
        return self._execute(operation)
    def get_test_statistics(
        self,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ApiOutcome[ApiPage[StatisticsDto]]:
        # Expose the canonical bounded test-statistics projection under the E21 API name.
        return self.list_statistics("test", limit=limit, cursor=cursor)
    def get_reuse_evidence(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ApiOutcome[ApiPage[ReuseEvidenceDto]]:
        # Return the frozen planner reuse decision ledger without raw knowledge payloads.
        def operation() -> ApiPage[ReuseEvidenceDto]:
            # Page canonical mutant identities from the newest protected campaign reuse plan.
            current_campaign = _non_empty(campaign_id, "campaign_id")
            page_limit = _validate_limit(limit)
            position = _decode_cursor(cursor, "reuse_evidence", 1)
            connection = self._connect_campaign()
            try:
                payload, _ = self._campaign_payload(connection, current_campaign)
            finally:
                connection.close()
            project_id = _campaign_dto(payload).project_id
            path = self._resolve_knowledge_database(project_id)
            if not path.is_file() and self._knowledge_store_factory is None:
                return ApiPage((), page_limit, None)
            store = self._open_knowledge_store(path)
            try:
                query = getattr(store, "query_reuse_decisions", None)
                if callable(query):
                    try:
                        page = query(
                            current_campaign,
                            limit=page_limit,
                            after_mutant_id=position[0] if position is not None else None,
                        )
                    except ValueError as exc:
                        raise _RequestRejected("invalid_cursor", "reuse evidence cursor is invalid") from exc
                    selected = tuple(_mapping(item, "reuse decision") for item in page.rows)
                    has_more = page.next_cursor is not None
                else:
                    raw_plan = store.get_reuse_plan_payload(current_campaign)
                    if raw_plan is None:
                        return ApiPage((), page_limit, None)
                    decisions = _array(raw_plan.get("decisions", []), "reuse plan decisions")
                    normalized: list[Mapping[str, Any]] = []
                    for raw in decisions:
                        value = _mapping(raw, "reuse decision")
                        mutant_id = _string(value.get("mutant_id"))
                        if not mutant_id:
                            raise _ReadFailure("store_corrupted", "reuse decision has no mutant identity")
                        normalized.append(value)
                    normalized.sort(key=lambda item: _string(item.get("mutant_id")))
                    if position is not None:
                        normalized = [item for item in normalized if _string(item.get("mutant_id")) > position[0]]
                    selected = tuple(normalized[:page_limit + 1])
                    has_more = len(selected) > page_limit
            finally:
                close = getattr(store, "close", None)
                if callable(close):
                    close()
            visible = selected[:page_limit]
            items = tuple(
                ReuseEvidenceDto(
                    mutant_id=_string(item.get("mutant_id")),
                    kind=_string(item.get("kind"), "none"),
                    eligible=bool(item.get("eligible", False)),
                    authorized=bool(item.get("authorized", False)),
                    audit_required=bool(item.get("audit_required", False)),
                    evidence_quality=_string(item.get("evidence_quality"), "unknown"),
                    result_status=_optional_string(item.get("result_status")),
                    source_event_id=_optional_string(item.get("source_event_id")),
                    source_execution_id=_optional_string(item.get("source_execution_id")),
                    blockers=tuple(sorted(str(value) for value in _array(item.get("blockers", []), "reuse blockers") if str(value))),
                )
                for item in visible
            )
            next_cursor = (
                _encode_cursor("reuse_evidence", items[-1].mutant_id)
                if has_more and items
                else None
            )
            return ApiPage(items, page_limit, next_cursor)
        return self._execute(operation)
    def get_recovery_diagnostics(
        self,
        campaign_id: str,
        *,
        limit: int,
        leases_cursor: str | None = None,
        workers_cursor: str | None = None,
        spool_cursor: str | None = None,
    ) -> ApiOutcome[RecoveryDiagnosticsDto]:
        # Assemble bounded lease, worker, spool, finalization, and recovery evidence without private paths.
        def operation() -> RecoveryDiagnosticsDto:
            # Read one campaign snapshot and derive liveness only from authoritative timestamps and process fences.
            current_campaign = _non_empty(campaign_id, "campaign_id")
            page_limit = _validate_limit(limit)
            lease_position = _decode_cursor(leases_cursor, "recovery_leases", 1)
            worker_position = _decode_cursor(workers_cursor, "recovery_workers", 1)
            spool_position = _decode_cursor(spool_cursor, "recovery_spool", 2)
            now = datetime.now(timezone.utc)
            blockers = []
            connection = self._connect_campaign()
            try:
                connection.execute("BEGIN")
                campaign_payload, _ = self._campaign_payload(connection, current_campaign)
                campaign = _campaign_dto(campaign_payload)
                lease_params: list[object] = [current_campaign]
                lease_where = "json_extract(payload, '$.campaign_id') = ?"
                if lease_position is not None:
                    lease_where += " AND lease_id > ?"
                    lease_params.append(lease_position[0])
                lease_params.append(page_limit + 1)
                lease_rows = connection.execute(
                    "SELECT lease_id, payload FROM mutation_leases WHERE " + lease_where + " ORDER BY lease_id LIMIT ?",
                    lease_params,
                ).fetchall()
                worker_params: list[object] = [current_campaign]
                worker_where = "json_extract(payload, '$.campaign_id') = ?"
                if worker_position is not None:
                    worker_where += " AND worker_key > ?"
                    worker_params.append(worker_position[0])
                worker_params.append(page_limit + 1)
                worker_rows = connection.execute(
                    "SELECT worker_key, payload FROM mutation_workers WHERE " + worker_where + " ORDER BY worker_key LIMIT ?",
                    worker_params,
                ).fetchall()
                all_worker_rows = connection.execute(
                    "SELECT worker_key, payload FROM mutation_workers "
                    "WHERE json_extract(payload, '$.campaign_id') = ? ORDER BY worker_key LIMIT 1001",
                    (current_campaign,),
                ).fetchall()
                pending_outbox = _integer(
                    connection.execute(
                        "SELECT COUNT(*) FROM mutation_outbox WHERE campaign_id = ? AND delivered_at IS NULL",
                        (current_campaign,),
                    ).fetchone()[0]
                )
                finalization = connection.execute(
                    "SELECT status FROM mutation_finalizations WHERE campaign_id = ?",
                    (current_campaign,),
                ).fetchone()
            finally:
                connection.close()
            lease_items = []
            for row in lease_rows[:page_limit]:
                value = _load_payload(row)
                heartbeat_at = _string(value.get("heartbeat_at"))
                lease_seconds = _float(value.get("lease_seconds"), 0.0)
                try:
                    normalized = heartbeat_at[:-1] + "+00:00" if heartbeat_at.endswith("Z") else heartbeat_at
                    heartbeat = datetime.fromisoformat(normalized)
                    if heartbeat.tzinfo is None:
                        raise ValueError("naive heartbeat")
                    expires = heartbeat.astimezone(timezone.utc) + timedelta(seconds=lease_seconds)
                    expires_at = expires.isoformat().replace("+00:00", "Z")
                    expired = now > expires
                except ValueError:
                    expires_at = ""
                    expired = True
                status = _string(value.get("status"), "unknown")
                stalled = expired and status in {"claimed", "running", "delivering"}
                if stalled:
                    blockers.append(DiagnosticBlockerDto("expired_lease", "lease heartbeat expired", "error", "lease"))
                lease_items.append(
                    LeaseDiagnosticDto(
                        shard_id=_string(value.get("shard_id")),
                        worker_id=_string(value.get("worker_id")),
                        lease_id=_string(value.get("lease_id")),
                        attempt=_integer(value.get("attempt")),
                        status=status,
                        heartbeat_at=heartbeat_at,
                        expires_at=expires_at,
                        expired=expired,
                        stalled=stalled,
                        revision_number=_integer(value.get("revision_number")),
                    )
                )
            lease_next = (
                _encode_cursor("recovery_leases", str(lease_rows[page_limit - 1]["lease_id"]))
                if len(lease_rows) > page_limit
                else None
            )
            from theseus_local.worker_runtime.recovery import recorded_process_is_alive
            from theseus_local.worker_runtime.spool import DurableExecutionSpool, SpoolError
            def inspect_spool(path: Path, *, limit: int, after_event_id: str | None = None) -> object:
                # Convert spool cursor and journal failures into stable public read outcomes.
                try:
                    return DurableExecutionSpool.inspect_existing(
                        path,
                        limit=limit,
                        after_event_id=after_event_id,
                    )
                except ValueError as exc:
                    raise _RequestRejected("invalid_cursor", "spool cursor is invalid") from exc
                except SpoolError as exc:
                    raise _ReadFailure("spool_corrupted", "worker spool cannot be inspected") from exc
            worker_items = []
            for row in worker_rows[:page_limit]:
                value = _load_payload(row)
                identity = _mapping(value.get("identity", {}), "worker identity")
                worker_id = _string(identity.get("worker_id"))
                if not worker_id:
                    raise _ReadFailure("store_corrupted", "worker projection has no identity")
                status = _string(value.get("status"), "unknown")
                process_alive = recorded_process_is_alive(
                    _integer(identity.get("process_id"), 0) or None,
                    _optional_string(identity.get("process_birth_token")),
                )
                orphaned = status == "orphaned" or (status not in {"stopped", "failed"} and not process_alive)
                if orphaned:
                    blockers.append(DiagnosticBlockerDto("orphaned_worker", "worker process is not authoritative", "error", "worker"))
                worker_items.append(
                    WorkerDiagnosticDto(
                        worker_id=worker_id,
                        instance_id=_string(identity.get("instance_id")),
                        status=status,
                        current_shard_id=_optional_string(value.get("current_shard_id")),
                        last_heartbeat_at=_string(value.get("last_heartbeat_at")),
                        orphaned=orphaned,
                        process_alive=process_alive,
                        workspace_healthy=bool(value.get("workspace_healthy", False)),
                        revision_number=_integer(value.get("revision_number")),
                    )
                )
            worker_next = (
                _encode_cursor("recovery_workers", str(worker_rows[page_limit - 1]["worker_key"]))
                if len(worker_rows) > page_limit
                else None
            )
            if len(all_worker_rows) > 1000:
                blockers.append(DiagnosticBlockerDto("worker_scan_truncated", "worker diagnostics exceeded the bounded scan", "warning", "worker"))
            pending_count = 0
            acknowledged_count = 0
            quarantined_count = 0
            oldest_pending_at = None
            deliveries = []
            has_more_spool = False
            cursor_worker = spool_position[0] if spool_position is not None else None
            cursor_event = spool_position[1] if spool_position is not None else None
            cursor_worker_seen = cursor_worker is None
            for row in all_worker_rows[:1000]:
                value = _load_payload(row)
                identity = _mapping(value.get("identity", {}), "worker identity")
                worker_id = _string(identity.get("worker_id"))
                spool_path = _optional_string(value.get("spool_path"))
                if not worker_id or spool_path is None:
                    raise _ReadFailure("store_corrupted", "worker projection has incomplete spool identity")
                before_cursor = cursor_worker is not None and worker_id < cursor_worker
                if worker_id == cursor_worker:
                    cursor_worker_seen = True
                remaining = max(1, page_limit + 1 - len(deliveries))
                inspection = inspect_spool(
                    Path(spool_path),
                    limit=1 if before_cursor else remaining,
                    after_event_id=cursor_event if worker_id == cursor_worker else None,
                )
                pending_count += inspection.pending_count
                acknowledged_count += inspection.acknowledged_count
                quarantined_count += inspection.quarantined_count
                inspection_oldest = getattr(inspection, "oldest_pending_at", None)
                if inspection_oldest is not None:
                    oldest_pending_at = min(
                        value for value in (oldest_pending_at, inspection_oldest) if value is not None
                    )
                if before_cursor:
                    continue
                for entry in inspection.entries:
                    assignment = entry.payload.get("assignment")
                    assignment_value = assignment if isinstance(assignment, Mapping) else {}
                    result = entry.payload.get("result")
                    result_value = result if isinstance(result, Mapping) else {}
                    raw_results = result_value.get("results", ())
                    mutant_count = len(raw_results) if isinstance(raw_results, (list, tuple)) else 0
                    deliveries.append(
                        SpoolDeliveryDto(
                            event_id=entry.event_id,
                            worker_id=worker_id or None,
                            shard_id=_optional_string(assignment_value.get("shard_id")),
                            lease_id=_optional_string(assignment_value.get("lease_id")),
                            attempt=_integer(assignment_value.get("attempt")),
                            mutant_count=mutant_count,
                        )
                    )
                if inspection.next_event_id is not None or len(deliveries) > page_limit:
                    has_more_spool = True
            if not cursor_worker_seen:
                raise _RequestRejected("invalid_cursor", "spool cursor does not match a worker")
            visible_deliveries = tuple(deliveries[:page_limit])
            spool_next = None
            if has_more_spool and visible_deliveries:
                last = visible_deliveries[-1]
                spool_next = _encode_cursor("recovery_spool", str(last.worker_id or ""), last.event_id)
            if pending_count:
                blockers.append(DiagnosticBlockerDto("pending_spool_delivery", "durable worker deliveries await authoritative commit", "warning", "spool"))
            finalization_status = str(finalization["status"]) if finalization is not None else None
            pending_finalization = finalization_status in {"created", "registered"}
            if pending_finalization:
                blockers.append(DiagnosticBlockerDto("pending_finalization", "campaign finalization is incomplete", "warning", "artifact"))
            recovery_path = self._resolve_recovery_path()
            recovery_state = "not_available"
            last_recovery_at = None
            if recovery_path.is_file():
                try:
                    report = json.loads(recovery_path.read_text(encoding="utf-8"))
                    report_value = _mapping(report, "recovery report")
                    recovery_state = _string(report_value.get("status"), "unknown")
                    last_recovery_at = _optional_string(
                        report_value.get("completed_at") or report_value.get("started_at")
                    )
                except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
                    recovery_state = "corrupted"
                    blockers.append(DiagnosticBlockerDto("recovery_report_corrupted", "recovery report cannot be decoded", "error", "recovery"))
            return RecoveryDiagnosticsDto(
                campaign_id=current_campaign,
                campaign_status=campaign.status,
                campaign_revision=campaign.revision_number,
                recovery_state=recovery_state,
                last_recovery_at=last_recovery_at,
                pending_finalization=pending_finalization,
                pending_outbox=pending_outbox,
                leases=ApiPage(tuple(lease_items), page_limit, lease_next),
                workers=ApiPage(tuple(worker_items), page_limit, worker_next),
                spool=SpoolDiagnosticsDto(
                    pending_count=pending_count,
                    acknowledged_count=acknowledged_count,
                    quarantined_count=quarantined_count,
                    oldest_pending_at=oldest_pending_at,
                    deliveries=ApiPage(visible_deliveries, page_limit, spool_next),
                ),
                blockers=tuple(blockers[:LOCAL_API_MAX_LIMIT]),
            )
        return self._execute(operation)
