"""Stable transport-neutral read contracts for the Theseus local API."""
from __future__ import annotations
from dataclasses import dataclass, field, fields, is_dataclass
from typing import Generic, Mapping, TypeAlias, TypeVar
JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
T = TypeVar("T")
def _json_value(value: object) -> JsonValue:
    # Convert only stable JSON values and API DTOs so private runtime objects cannot cross the boundary.
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, ApiModel):
        return value.to_dict()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    if is_dataclass(value):
        raise TypeError(f"non-API dataclass cannot cross local API boundary: {type(value).__name__}")
    raise TypeError(f"unsupported local API value: {type(value).__name__}")
class ApiModel:
    """JSON-compatible base for all public local API DTOs."""
    def to_dict(self) -> dict[str, JsonValue]:
        # Serialize declared DTO fields without exposing Path, Enum, sqlite.Row or domain objects.
        return {item.name: _json_value(getattr(self, item.name)) for item in fields(self)}
@dataclass(frozen=True, slots=True)
class ApiError(ApiModel):
    """Stable error payload shared by rejected and failed read outcomes."""
    code: str
    message: str
    retriable: bool = False
    details: Mapping[str, JsonScalar] = field(default_factory=dict)
    def __post_init__(self) -> None:
        # Reject empty error identities and copy details into an independent plain mapping.
        if not isinstance(self.code, str) or not self.code.strip():
            raise ValueError("API error code must be a non-empty string")
        if not isinstance(self.message, str) or not self.message.strip():
            raise ValueError("API error message must be a non-empty string")
        object.__setattr__(self, "details", dict(self.details))
@dataclass(frozen=True, slots=True)
class ApiSuccess(ApiModel, Generic[T]):
    """Successful read outcome containing one stable DTO or page."""
    value: T
    def to_dict(self) -> dict[str, JsonValue]:
        # Emit one discriminated success shape suitable for CLI, UI and future transports.
        return {"ok": True, "kind": "success", "value": _json_value(self.value)}
@dataclass(frozen=True, slots=True)
class ApiRejected(ApiModel):
    """Expected request rejection such as invalid pagination or a missing resource."""
    error: ApiError
    def to_dict(self) -> dict[str, JsonValue]:
        # Emit one stable non-retriable rejection shape.
        return {"ok": False, "kind": "rejected", "error": self.error.to_dict()}
@dataclass(frozen=True, slots=True)
class ApiFailed(ApiModel):
    """Technical read failure such as corrupt SQLite or unavailable durable state."""
    error: ApiError
    def to_dict(self) -> dict[str, JsonValue]:
        # Emit one stable technical failure shape without leaking exception objects.
        return {"ok": False, "kind": "failed", "error": self.error.to_dict()}
ApiOutcome: TypeAlias = ApiSuccess[T] | ApiRejected | ApiFailed
@dataclass(frozen=True, slots=True)
class ApiPage(ApiModel, Generic[T]):
    """Bounded keyset page returned by every list read."""
    items: tuple[T, ...]
    limit: int
    next_cursor: str | None = None
@dataclass(frozen=True, slots=True)
class ProjectDto(ApiModel):
    """Project identity observed through one or more durable campaign projections."""
    project_id: str
    display_name: str
    root_path: str | None
    main_root_path: str | None
    revision_id: str | None
    campaign_count: int
@dataclass(frozen=True, slots=True)
class CampaignDto(ApiModel):
    """Compact immutable campaign read model."""
    campaign_id: str
    project_id: str
    revision_id: str
    status: str
    mode: str
    plan_id: str | None
    prepared_snapshot_id: str | None
    source_path: str
    function: str | None
    total_mutants: int
    completed_mutants: int
    revision_number: int
@dataclass(frozen=True, slots=True)
class PlanDto(ApiModel):
    """Committed campaign plan identity without planner implementation details."""
    plan_id: str
    campaign_id: str
    prepared_snapshot_id: str | None
    selected_mutants: int
    campaign_status: str
@dataclass(frozen=True, slots=True)
class WorkerDto(ApiModel):
    """Authoritative worker liveness and assignment projection without workspace paths."""
    campaign_id: str
    worker_id: str
    instance_id: str
    process_id: int
    status: str
    heartbeat_sequence: int
    last_heartbeat_at: str
    current_shard_id: str | None
    current_lease_id: str | None
    current_attempt: int | None
    current_mutant_id: str | None
    child_process_id: int | None
    completed_mutants: int
    completed_assignments: int
    workspace_healthy: bool
    revision_number: int
    platform: str
    architecture: str
    python_versions: tuple[str, ...]
    engine_protocol_versions: tuple[int, ...]
    workspace_backends: tuple[str, ...]
    cpu_count: int
    memory_limit_bytes: int | None
@dataclass(frozen=True, slots=True)
class ShardDto(ApiModel):
    """Bounded shard ownership and progress projection."""
    shard_id: str
    campaign_id: str
    plan_id: str
    ordinal: int
    status: str
    worker_id: str | None
    lease_id: str | None
    attempt: int
    mutant_count: int
    completed_count: int
    estimated_cost: float
    revision_number: int
@dataclass(frozen=True, slots=True)
class ExecutionDto(ApiModel):
    """Compact immutable execution evidence projection."""
    execution_id: str
    campaign_id: str
    shard_id: str
    mutant_id: str
    attempt: int
    status: str
    semantic_result: str | None
    duration_seconds: float | None
    restore_verified: bool
    error: str | None
    selected_test_count: int
    artifact_count: int
    observation_count: int
    revision_number: int
    lease_id: str | None
    killer_test_id: str | None = None
@dataclass(frozen=True, slots=True)
class ArtifactDto(ApiModel):
    """Immutable artifact registry metadata without physical or logical filesystem paths."""
    campaign_id: str
    logical_key: str
    logical_role: str
    content_sha256: str
    size_bytes: int
    schema_version: int
    producer: str
    created_at: str
    shard_id: str | None
    execution_id: str | None
    metadata: Mapping[str, JsonValue] = field(default_factory=dict)
    def __post_init__(self) -> None:
        # Copy artifact metadata so callers cannot mutate a frozen API response indirectly.
        object.__setattr__(self, "metadata", dict(self.metadata))
@dataclass(frozen=True, slots=True)
class KnowledgeExecutionDto(ApiModel):
    """One bounded Knowledge Plane execution record without raw payload materialization."""
    event_id: str
    effect_id: str
    campaign_id: str
    execution_id: str
    mutant_id: str
    attempt: int
    status: str
    created_at: str
    compacted: bool
    function_id: str | None
    source_kind: str | None
    evidence_quality: str | None
    evidence_schema_version: int
    retention_class: str | None
    project_id: str
    revision_id: str
    environment_id: str
@dataclass(frozen=True, slots=True)
class KnowledgeDto(ApiModel):
    """Campaign knowledge counters plus one bounded execution page."""
    campaign_id: str
    executions: int
    mutants: int
    attempts: int
    counts: Mapping[str, int]
    revision: int
    snapshot_revision: int
    schema_version: int
    records: ApiPage[KnowledgeExecutionDto]
    def __post_init__(self) -> None:
        # Copy materialized counters so the read result remains immutable to callers.
        object.__setattr__(self, "counts", {str(key): int(value) for key, value in self.counts.items()})
@dataclass(frozen=True, slots=True)
class StatisticsDto(ApiModel):
    """Stable statistics projection for one campaign, test, mutant, worker or execution."""
    entity_type: str
    entity_id: str
    event_count: int
    started_count: int
    completed_count: int
    passed_count: int
    failed_count: int
    error_count: int
    timeout_count: int
    retry_count: int
    recovery_count: int
    escalation_count: int
    reuse_count: int
    infrastructure_failure_count: int
    flaky_transition_count: int
    duration_count: int
    duration_total_ms: float
    duration_min_ms: float | None
    duration_max_ms: float | None
    duration_avg_ms: float
    duration_median_ms: float
    duration_p95_ms: float
    duration_sample_count: int
    busy_duration_ms: float
    active_duration_ms: float
    utilization: float
    active: bool
    last_outcome: str | None
    last_event_type: str
    last_event_timestamp: str
@dataclass(frozen=True, slots=True)
class RecoveryActionDto(ApiModel):
    """One stable action from the bounded startup-recovery report."""
    sequence: int
    campaign_id: str | None
    status: str
    action: str | None
    details: Mapping[str, JsonValue] = field(default_factory=dict)
    def __post_init__(self) -> None:
        # Copy action details so report data cannot be mutated through a caller-held mapping.
        object.__setattr__(self, "details", dict(self.details))
@dataclass(frozen=True, slots=True)
class RecoveryDto(ApiModel):
    """Latest startup-recovery decision without exposing report or database paths."""
    available: bool
    status: str
    started_at: str | None
    completed_at: str | None
    run_count: int
    action_count: int
    error: str | None
    actions: ApiPage[RecoveryActionDto]
@dataclass(frozen=True, slots=True)
class CampaignDetailDto(ApiModel):
    """Campaign aggregate and all bounded related read models from one SQLite snapshot."""
    campaign: CampaignDto
    project: ProjectDto
    plan: PlanDto | None
    shards: ApiPage[ShardDto]
    workers: ApiPage[WorkerDto]
    executions: ApiPage[ExecutionDto]
    artifacts: ApiPage[ArtifactDto]
    finalization_status: str | None
@dataclass(frozen=True, slots=True)
class EventDto(ApiModel):
    """One sanitized event from an append-only local source."""
    event_id: str
    source: str
    category: str
    event_type: str
    timestamp: str
    campaign_id: str | None
    worker_id: str | None
    shard_id: str | None
    execution_id: str | None
    details: Mapping[str, JsonValue] = field(default_factory=dict)
    def __post_init__(self) -> None:
        # Copy sanitized event details so callers cannot mutate a frozen stream item.
        object.__setattr__(self, "details", dict(self.details))
@dataclass(frozen=True, slots=True)
class EventStreamPageDto(ApiModel):
    """One bounded merged event page with a resumable composite cursor."""
    items: tuple[EventDto, ...]
    limit: int
    next_cursor: str
    has_more: bool
@dataclass(frozen=True, slots=True)
class ShardProgressDto(ApiModel):
    """Compact shard progress without lease or topology internals."""
    shard_id: str
    status: str
    attempt: int
    completed_mutants: int
    total_mutants: int
@dataclass(frozen=True, slots=True)
class StatisticsCheckpointDto(ApiModel):
    """Latest durable statistics projection position."""
    sequence: int
    event_id: str
@dataclass(frozen=True, slots=True)
class ProgressDto(ApiModel):
    """Bounded operator progress snapshot for one campaign."""
    campaign_id: str
    campaign_status: str
    campaign_revision: int
    completed_mutants: int
    total_mutants: int
    active_workers: int
    shards: ApiPage[ShardProgressDto]
    pending_outbox: int
    quarantine_count: int
    quarantine_truncated: bool
    recovery_state: str
    statistics_checkpoint: StatisticsCheckpointDto
@dataclass(frozen=True, slots=True)
class ActionReceiptDto(ApiModel):
    """Stable result of one idempotent operator action."""
    action_id: str
    action: str
    campaign_id: str
    status: str
    campaign_revision: int
    shard_id: str | None = None
    shard_revision: int | None = None
    shard_ids: tuple[str, ...] = ()
@dataclass(frozen=True, slots=True)
class ArtifactChunkDto(ApiModel):
    """Bounded verified artifact bytes without a physical path."""
    campaign_id: str
    logical_key: str
    content_sha256: str
    size_bytes: int
    offset: int
    data_base64: str
    next_offset: int | None
    complete: bool
@dataclass(frozen=True, slots=True)
class QuarantineRecordDto(ApiModel):
    """One sanitized knowledge conflict quarantine record."""
    conflict_id: str
    conflict_type: str
    identity_type: str
    identity_key: str
    scope_id: str | None
    reason: str
    created_at: str
    status: str
@dataclass(frozen=True, slots=True)
class QuarantineInspectionDto(ApiModel):
    """Bounded newest-first quarantine inspection snapshot."""
    items: tuple[QuarantineRecordDto, ...]
    limit: int
    truncated: bool
    next_cursor: str | None = None
@dataclass(frozen=True, slots=True)
class DiagnosticBlockerDto(ApiModel):
    """Typed operator blocker without raw exception or environment data."""
    code: str
    message: str
    severity: str
    component: str
@dataclass(frozen=True, slots=True)
class HealthDto(ApiModel):
    """Bounded component health suitable for CLI and future UI transports."""
    status: str
    campaign_store: str
    knowledge_store: str
    statistics_store: str
    recovery: str
    blockers: tuple[DiagnosticBlockerDto, ...]
@dataclass(frozen=True, slots=True)
class DiagnosticsDto(ApiModel):
    """Bounded diagnostics assembled only from stable read boundaries."""
    health: HealthDto
    progress: ProgressDto | None
    quarantine: QuarantineInspectionDto
    blockers: tuple[DiagnosticBlockerDto, ...]
@dataclass(frozen=True, slots=True)
class LeaseDiagnosticDto(ApiModel):
    """Authoritative lease liveness without process or filesystem identities."""
    shard_id: str
    worker_id: str
    lease_id: str
    attempt: int
    status: str
    heartbeat_at: str
    expires_at: str
    expired: bool
    stalled: bool
    revision_number: int
@dataclass(frozen=True, slots=True)
class WorkerDiagnosticDto(ApiModel):
    """Sanitized worker recovery state without PID, birth token, or private paths."""
    worker_id: str
    instance_id: str
    status: str
    current_shard_id: str | None
    last_heartbeat_at: str
    orphaned: bool
    process_alive: bool
    workspace_healthy: bool
    revision_number: int
@dataclass(frozen=True, slots=True)
class SpoolDeliveryDto(ApiModel):
    """One bounded pending durable delivery identity without its raw payload."""
    event_id: str
    worker_id: str | None
    shard_id: str | None
    lease_id: str | None
    attempt: int
    mutant_count: int
@dataclass(frozen=True, slots=True)
class SpoolDiagnosticsDto(ApiModel):
    """Aggregate worker-spool state plus one bounded pending-delivery page."""
    pending_count: int
    acknowledged_count: int
    quarantined_count: int
    oldest_pending_at: str | None
    deliveries: ApiPage[SpoolDeliveryDto]
@dataclass(frozen=True, slots=True)
class ReuseEvidenceDto(ApiModel):
    """One planner-authority reuse decision and its source evidence identities."""
    mutant_id: str
    kind: str
    eligible: bool
    authorized: bool
    audit_required: bool
    evidence_quality: str
    result_status: str | None
    source_event_id: str | None
    source_execution_id: str | None
    blockers: tuple[str, ...]
@dataclass(frozen=True, slots=True)
class ArtifactRegistryDto(ApiModel):
    """Bounded artifact registry page with its authoritative finalization status."""
    finalization_status: str | None
    artifacts: ApiPage[ArtifactDto]
@dataclass(frozen=True, slots=True)
class RecoveryDiagnosticsDto(ApiModel):
    """Bounded campaign recovery snapshot assembled from authoritative projections."""
    campaign_id: str
    campaign_status: str
    campaign_revision: int
    recovery_state: str
    last_recovery_at: str | None
    pending_finalization: bool
    pending_outbox: int
    leases: ApiPage[LeaseDiagnosticDto]
    workers: ApiPage[WorkerDiagnosticDto]
    spool: SpoolDiagnosticsDto
    blockers: tuple[DiagnosticBlockerDto, ...]
