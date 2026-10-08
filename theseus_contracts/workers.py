"""Shard and worker lease DTOs."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Mapping
from .enums import WorkerStatus
from .ids import CampaignId, ShardId, WorkerId
from .mutation import MutantExecutionResult
from .serialization import (
    WireModel,
    optional_string,
    required_string,
    sequence_of_strings,
    validate_utc_timestamp,
)
@dataclass(frozen=True, slots=True)
class WorkerIdentity(WireModel):
    """Identity of one running worker instance, not merely a reusable worker slot."""
    worker_id: str
    instance_id: str
    process_id: int
    process_birth_token: str
    def __post_init__(self) -> None:
        # Reject incomplete process identities so a late result cannot be attributed to a reused PID.
        if not self.worker_id or not self.instance_id or not self.process_birth_token:
            raise ValueError("worker identity fields must be non-empty")
        if int(self.process_id) <= 0:
            raise ValueError("process_id must be positive")
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WorkerIdentity":
        # Restore a process identity without accepting implicit defaults for ownership tokens.
        return cls(
            worker_id=required_string(value, "worker_id"),
            instance_id=required_string(value, "instance_id"),
            process_id=int(value.get("process_id", 0)),
            process_birth_token=required_string(value, "process_birth_token"),
        )
@dataclass(frozen=True, slots=True)
class WorkerCapabilities(WireModel):
    """Capabilities used by a scheduler before assigning an immutable shard."""
    platform: str
    architecture: str
    python_versions: tuple[str, ...]
    engine_protocol_versions: tuple[int, ...]
    workspace_backends: tuple[str, ...]
    cpu_count: int
    memory_limit_bytes: int | None = None
    def __post_init__(self) -> None:
        # Keep capability claims bounded and deterministic for assignment fingerprints.
        if not self.platform or not self.architecture:
            raise ValueError("platform and architecture must be non-empty")
        if int(self.cpu_count) < 1:
            raise ValueError("cpu_count must be positive")
        if self.memory_limit_bytes is not None and int(self.memory_limit_bytes) < 0:
            raise ValueError("memory_limit_bytes must not be negative")
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WorkerCapabilities":
        # Normalize advertised versions and backend names at the wire boundary.
        raw_protocols = value.get("engine_protocol_versions", [])
        if not isinstance(raw_protocols, (list, tuple)):
            raise ValueError("engine_protocol_versions must be an array")
        return cls(
            platform=required_string(value, "platform"),
            architecture=required_string(value, "architecture"),
            python_versions=sequence_of_strings(value, "python_versions"),
            engine_protocol_versions=tuple(int(item) for item in raw_protocols),
            workspace_backends=sequence_of_strings(value, "workspace_backends"),
            cpu_count=int(value.get("cpu_count", 0)),
            memory_limit_bytes=(
                int(value["memory_limit_bytes"])
                if value.get("memory_limit_bytes") is not None
                else None
            ),
        )
@dataclass(frozen=True, slots=True)
class ShardAssignment(WireModel):
    """Immutable assignment handed to a worker runtime after a lease claim."""
    campaign_id: str
    shard_id: str
    lease_id: str
    attempt: int
    mutant_ids: tuple[str, ...]
    prepared_snapshot_id: str
    workspace_descriptor_id: str
    expires_at: str
    def __post_init__(self) -> None:
        # Validate assignment identity before an agent mutates its workspace.
        if not all(
            (self.campaign_id, self.shard_id, self.lease_id, self.prepared_snapshot_id, self.workspace_descriptor_id)
        ):
            raise ValueError("shard assignment identity fields must be non-empty")
        if int(self.attempt) < 0:
            raise ValueError("attempt must not be negative")
        validate_utc_timestamp(self.expires_at, field_name="expires_at")
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ShardAssignment":
        # Restore only the immutable assignment fields; mutable worker state stays in Gallifrey.
        return cls(
            campaign_id=required_string(value, "campaign_id"),
            shard_id=required_string(value, "shard_id"),
            lease_id=required_string(value, "lease_id"),
            attempt=max(0, int(value.get("attempt", 0))),
            mutant_ids=sequence_of_strings(value, "mutant_ids"),
            prepared_snapshot_id=required_string(value, "prepared_snapshot_id"),
            workspace_descriptor_id=required_string(value, "workspace_descriptor_id"),
            expires_at=validate_utc_timestamp(required_string(value, "expires_at"), field_name="expires_at"),
        )
@dataclass(frozen=True, slots=True)
class WorkerHeartbeat(WireModel):
    """Liveness and progress message that renews both worker registration and shard ownership."""
    worker: WorkerIdentity
    sequence: int
    sent_at: str
    current_campaign_id: str | None = None
    current_shard_id: str | None = None
    current_lease_id: str | None = None
    current_attempt: int | None = None
    current_mutant_id: str | None = None
    child_process_id: int | None = None
    completed_mutants: int = 0
    workspace_healthy: bool = True
    child_process_birth_token: str | None = None
    def __post_init__(self) -> None:
        # Enforce monotonic counters and UTC timestamps before the heartbeat reaches the control plane.
        if int(self.sequence) < 0 or int(self.completed_mutants) < 0:
            raise ValueError("heartbeat counters must not be negative")
        if self.current_attempt is not None and int(self.current_attempt) < 0:
            raise ValueError("current_attempt must not be negative")
        if self.child_process_id is not None and int(self.child_process_id) <= 0:
            raise ValueError("child_process_id must be positive when present")
        if self.child_process_birth_token is not None and self.child_process_id is None:
            raise ValueError("child_process_birth_token requires child_process_id")
        validate_utc_timestamp(self.sent_at, field_name="sent_at")
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WorkerHeartbeat":
        # Decode the nested worker identity and preserve explicit null assignment fields.
        raw_worker = value.get("worker")
        if not isinstance(raw_worker, Mapping):
            raise ValueError("worker must be an object")
        return cls(
            worker=WorkerIdentity.from_dict(raw_worker),
            sequence=max(0, int(value.get("sequence", 0))),
            sent_at=validate_utc_timestamp(required_string(value, "sent_at"), field_name="sent_at"),
            current_campaign_id=optional_string(value, "current_campaign_id"),
            current_shard_id=optional_string(value, "current_shard_id"),
            current_lease_id=optional_string(value, "current_lease_id"),
            current_attempt=int(value["current_attempt"]) if value.get("current_attempt") is not None else None,
            current_mutant_id=optional_string(value, "current_mutant_id"),
            child_process_id=int(value["child_process_id"]) if value.get("child_process_id") is not None else None,
            child_process_birth_token=optional_string(value, "child_process_birth_token"),
            completed_mutants=max(0, int(value.get("completed_mutants", 0))),
            workspace_healthy=bool(value.get("workspace_healthy", True)),
        )
@dataclass(frozen=True, slots=True)
class HeartbeatReceipt(WireModel):
    """Authoritative acknowledgement returned after worker and lease validation."""
    accepted: bool
    lease_valid: bool
    lease_revision: int | None = None
    lease_expires_at: str | None = None
    cancellation_requested: bool = False
    reason: str | None = None
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "HeartbeatReceipt":
        # Preserve a negative receipt as data so the worker can stop safely instead of retrying forever.
        expires_at = optional_string(value, "lease_expires_at")
        if expires_at is not None:
            expires_at = validate_utc_timestamp(expires_at, field_name="lease_expires_at")
        return cls(
            accepted=bool(value.get("accepted", False)),
            lease_valid=bool(value.get("lease_valid", False)),
            lease_revision=int(value["lease_revision"]) if value.get("lease_revision") is not None else None,
            lease_expires_at=expires_at,
            cancellation_requested=bool(value.get("cancellation_requested", False)),
            reason=optional_string(value, "reason"),
        )
@dataclass(frozen=True, slots=True)
class ExecutionEnvelope(WireModel):
    """Typed delivery envelope for one worker execution and its raw evidence references."""
    event_id: str
    execution_id: str
    campaign_id: str
    shard_id: str
    mutant_id: str
    worker_id: str
    worker_instance_id: str
    lease_id: str
    attempt: int
    semantic_result: str
    restore_verified: bool
    test_observations: tuple[Mapping[str, Any], ...]
    artifact_refs: tuple[Mapping[str, Any], ...]
    payload_sha256: str
    def __post_init__(self) -> None:
        # Require stable event identity before a result can enter the delivery spool.
        if not all(
            (
                self.event_id,
                self.execution_id,
                self.campaign_id,
                self.shard_id,
                self.mutant_id,
                self.worker_id,
                self.worker_instance_id,
                self.lease_id,
                self.semantic_result,
                self.payload_sha256,
            )
        ):
            raise ValueError("execution envelope identity fields must be non-empty")
        if int(self.attempt) < 0:
            raise ValueError("attempt must not be negative")
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ExecutionEnvelope":
        # Decode one per-mutant delivery envelope without retaining untyped nested evidence.
        raw_observations = value.get("test_observations", [])
        raw_artifacts = value.get("artifact_refs", [])
        if (
            not isinstance(raw_observations, (list, tuple))
            or any(not isinstance(item, Mapping) for item in raw_observations)
            or not isinstance(raw_artifacts, (list, tuple))
            or any(not isinstance(item, Mapping) for item in raw_artifacts)
        ):
            raise ValueError("test_observations and artifact_refs must be arrays of objects")
        return cls(
            event_id=required_string(value, "event_id"),
            execution_id=required_string(value, "execution_id"),
            campaign_id=required_string(value, "campaign_id"),
            shard_id=required_string(value, "shard_id"),
            mutant_id=required_string(value, "mutant_id"),
            worker_id=required_string(value, "worker_id"),
            worker_instance_id=required_string(value, "worker_instance_id"),
            lease_id=required_string(value, "lease_id"),
            attempt=max(0, int(value.get("attempt", 0))),
            semantic_result=required_string(value, "semantic_result"),
            restore_verified=bool(value.get("restore_verified", False)),
            test_observations=tuple(dict(item) for item in raw_observations),
            artifact_refs=tuple(dict(item) for item in raw_artifacts),
            payload_sha256=required_string(value, "payload_sha256"),
        )
@dataclass(frozen=True, slots=True)
class ShardDescriptor(WireModel):
    """Deterministic set of mutant IDs assigned to one planner-owned topology slot."""
    shard_id: ShardId
    mutant_ids: tuple[str, ...]
    estimated_cost: float = 0.0
    plan_id: str = ""
    def __post_init__(self) -> None:
        # Reject malformed membership while allowing isolated non-plan audit descriptors.
        if not isinstance(self.shard_id, ShardId):
            raise TypeError("shard_id must be ShardId")
        if not self.mutant_ids or any(not str(item).strip() for item in self.mutant_ids):
            raise ValueError("shard descriptor requires non-empty mutant IDs")
        if len(set(self.mutant_ids)) != len(self.mutant_ids):
            raise ValueError("shard descriptor mutant IDs must be unique")
        if float(self.estimated_cost) < 0.0:
            raise ValueError("shard descriptor estimated_cost must not be negative")
        object.__setattr__(self, "plan_id", str(self.plan_id).strip())
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ShardDescriptor":
        # Restore shard membership without loading mutable scheduler state.
        return cls(
            shard_id=ShardId(required_string(value, "shard_id")),
            mutant_ids=sequence_of_strings(value, "mutant_ids"),
            estimated_cost=float(value.get("estimated_cost", 0.0)),
            plan_id=str(value.get("plan_id", "")).strip(),
        )
@dataclass(frozen=True, slots=True)
class ShardLease(WireModel):
    """Renewable worker ownership record used during reconciliation."""
    worker_id: WorkerId
    lease_id: str
    status: WorkerStatus | str
    lease_seconds: float
    heartbeat_at: str
    heartbeat_seq: int = 0
    attempt: int = 0
    worker_instance_id: str | None = None
    process_birth_token: str | None = None
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ShardLease":
        # Restore a lease while preserving unknown future worker statuses as strings.
        status_value = required_string(value, "status")
        try:
            status: WorkerStatus | str = WorkerStatus(status_value)
        except ValueError:
            status = status_value
        return cls(
            worker_id=WorkerId(required_string(value, "worker_id")),
            lease_id=required_string(value, "lease_id"),
            status=status,
            lease_seconds=float(value.get("lease_seconds", 10.0)),
            heartbeat_at=required_string(value, "heartbeat_at"),
            heartbeat_seq=max(0, int(value.get("heartbeat_seq", 0))),
            attempt=max(0, int(value.get("attempt", 0))),
            worker_instance_id=optional_string(value, "worker_instance_id"),
            process_birth_token=optional_string(value, "process_birth_token"),
        )
@dataclass(frozen=True, slots=True)
class ShardExecutionRequest(WireModel):
    """Control-plane request handed to one isolated worker."""
    campaign_id: CampaignId
    shard: ShardDescriptor
    lease: ShardLease
    configuration: Mapping[str, Any]
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ShardExecutionRequest":
        # Restore the request using only contract DTOs and JSON configuration.
        shard = value.get("shard")
        lease = value.get("lease")
        configuration = value.get("configuration", {})
        if not isinstance(shard, Mapping) or not isinstance(lease, Mapping) or not isinstance(configuration, Mapping):
            raise ValueError("shard, lease and configuration must be objects")
        return cls(
            campaign_id=CampaignId(required_string(value, "campaign_id")),
            shard=ShardDescriptor.from_dict(shard),
            lease=ShardLease.from_dict(lease),
            configuration=dict(configuration),
        )
@dataclass(frozen=True, slots=True)
class ShardExecutionResult(WireModel):
    """Worker completion projection with result rows and recovery metadata."""
    shard_id: ShardId
    worker_id: WorkerId
    status: WorkerStatus | str
    completed_mutants: int
    results: tuple[MutantExecutionResult, ...] = ()
    report_path: str | None = None
    error: str | None = None
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ShardExecutionResult":
        # Restore worker output while keeping result rows typed at the public edge.
        raw_results = value.get("results", [])
        if not isinstance(raw_results, (list, tuple)) or any(not isinstance(item, Mapping) for item in raw_results):
            raise ValueError("results must be an array of objects")
        status_value = required_string(value, "status")
        try:
            status: WorkerStatus | str = WorkerStatus(status_value)
        except ValueError:
            status = status_value
        return cls(
            shard_id=ShardId(required_string(value, "shard_id")),
            worker_id=WorkerId(required_string(value, "worker_id")),
            status=status,
            completed_mutants=max(0, int(value.get("completed_mutants", 0))),
            results=tuple(MutantExecutionResult.from_dict(item) for item in raw_results),
            report_path=optional_string(value, "report_path"),
            error=optional_string(value, "error"),
        )
