"""Versioned typed protocol between the Theseus coordinator and persistent workers."""
from __future__ import annotations
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, ClassVar, Mapping, TypeAlias
from .errors import (
    CorrelationMismatchError,
    IdentityMismatchError,
    IncompatibleProtocolError,
    MissingFieldError,
    UnknownMessageTypeError,
    UnsupportedSchemaError,
)
from .serialization import (
    SerializationError,
    WireModel,
    dumps,
    loads_object,
    optional_string,
    require_mapping,
    required_string,
    sequence_of_strings,
    to_json_value,
    utc_now,
    validate_utc_timestamp,
)
from .workers import (
    ExecutionEnvelope,
    HeartbeatReceipt,
    ShardAssignment,
    ShardExecutionResult,
    WorkerCapabilities,
    WorkerHeartbeat,
    WorkerIdentity,
)
WORKER_PROTOCOL_VERSION = 1
WORKER_SCHEMA_VERSION = 1
class WorkerMessageType(str, Enum):
    """Stable message types crossing the persistent worker process boundary."""
    REGISTER_WORKER = "worker.register"
    REGISTER_WORKER_RECEIPT = "worker.register_receipt"
    ACQUIRE_ASSIGNMENT = "worker.acquire_assignment"
    NO_ASSIGNMENT = "worker.no_assignment"
    SHARD_ASSIGNMENT = "worker.shard_assignment"
    WORKER_HEARTBEAT = "worker.heartbeat"
    HEARTBEAT_RECEIPT = "worker.heartbeat_receipt"
    EXECUTION_DELIVERY = "worker.execution_delivery"
    EXECUTION_RECEIPT = "worker.execution_receipt"
    EXECUTION_ACKNOWLEDGED = "worker.execution_acknowledged"
    ENGINE_STARTED = "worker.engine_started"
    ENGINE_STOPPED = "worker.engine_stopped"
    CANCEL_ASSIGNMENT = "worker.cancel_assignment"
    DRAIN_WORKER = "worker.drain"
    SHUTDOWN_WORKER = "worker.shutdown"
    WORKER_TERMINATED = "worker.terminated"
    PROTOCOL_ERROR = "worker.protocol_error"
class WorkerExecutionMode(str, Enum):
    """Execution adapter selected by one typed shard assignment."""
    FIXTURE = "fixture"
    ENGINE = "engine"
@dataclass(frozen=True, slots=True)
class WorkerExecutionSpec(WireModel):
    """Typed execution inputs carried by a shard assignment frame."""
    mode: WorkerExecutionMode
    result: Mapping[str, Any] = field(default_factory=dict)
    delay_seconds: float = 0.0
    configuration: Mapping[str, Any] | None = None
    execute_request: Mapping[str, Any] | None = None
    workspace: str | None = None
    report_root: str | None = None
    publish_engine_root: str | None = None
    expected_source_sha256: str | None = None
    prepared_snapshot_id: str | None = None
    expected_mutant_ids: tuple[str, ...] = ()
    test_fingerprints: Mapping[str, Mapping[str, str]] = field(default_factory=dict)
    command_timeouts: Mapping[str, float] = field(default_factory=dict)
    command: tuple[str, ...] | None = None
    cancel_path: str | None = None
    def __post_init__(self) -> None:
        # Enforce mode-specific execution requirements before a worker touches its workspace.
        if float(self.delay_seconds) < 0:
            raise ValueError("delay_seconds must not be negative")
        if self.mode == WorkerExecutionMode.FIXTURE:
            return
        if self.mode != WorkerExecutionMode.ENGINE:
            raise ValueError(f"unsupported worker execution mode: {self.mode}")
        if self.configuration is None or not isinstance(self.configuration, Mapping):
            raise ValueError("engine configuration must be an object")
        if not self.workspace or not self.report_root or not self.expected_source_sha256:
            raise ValueError("engine workspace, report_root and expected_source_sha256 are required")
        if self.command is not None and not self.command:
            raise ValueError("engine command must not be empty")
        if any(float(value) <= 0 for value in self.command_timeouts.values()):
            raise ValueError("engine command timeouts must be positive")
        outer_snapshot_id = str(self.prepared_snapshot_id or "").strip()
        if self.prepared_snapshot_id is not None and not outer_snapshot_id:
            raise ValueError("engine prepared_snapshot_id must be non-empty when provided")
        if self.execute_request is not None:
            nested_snapshot_id = str(
                self.execute_request.get("prepared_snapshot_id") or ""
            ).strip()
            if outer_snapshot_id and nested_snapshot_id and nested_snapshot_id != outer_snapshot_id:
                raise ValueError(
                    "worker execution prepared snapshot identity conflict: "
                    f"expected={outer_snapshot_id!r}, received={nested_snapshot_id!r}, "
                    "boundary='WorkerExecutionSpec.execute_request'"
                )
    @classmethod
    def fixture(
        cls,
        result: Mapping[str, Any],
        *,
        delay_seconds: float = 0.0,
    ) -> "WorkerExecutionSpec":
        # Build the bounded lifecycle fixture used by process-level regression tests.
        return cls(
            mode=WorkerExecutionMode.FIXTURE,
            result=dict(result),
            delay_seconds=max(0.0, float(delay_seconds)),
        )
    @classmethod
    def engine(
        cls,
        *,
        configuration: Mapping[str, Any],
        execute_request: Mapping[str, Any] | None,
        workspace: str,
        report_root: str,
        publish_engine_root: str | None,
        expected_source_sha256: str,
        expected_mutant_ids: tuple[str, ...],
        test_fingerprints: Mapping[str, Mapping[str, str]],
        command_timeouts: Mapping[str, float],
        command: tuple[str, ...] | None,
        cancel_path: str | None,
        prepared_snapshot_id: str | None = None,
    ) -> "WorkerExecutionSpec":
        # Build one explicit worker-owned engine execution specification.
        authoritative_snapshot_id = (
            str(prepared_snapshot_id).strip() if prepared_snapshot_id is not None else ""
        )
        normalized_execute_request = dict(execute_request) if execute_request is not None else None
        if normalized_execute_request is not None and authoritative_snapshot_id:
            nested_snapshot_id = str(
                normalized_execute_request.get("prepared_snapshot_id") or ""
            ).strip()
            if nested_snapshot_id and nested_snapshot_id != authoritative_snapshot_id:
                raise ValueError(
                    "worker execution prepared snapshot identity conflict: "
                    f"expected={authoritative_snapshot_id!r}, received={nested_snapshot_id!r}, "
                    "boundary='WorkerExecutionSpec.engine'"
                )
            normalized_execute_request["prepared_snapshot_id"] = authoritative_snapshot_id
        return cls(
            mode=WorkerExecutionMode.ENGINE,
            configuration=dict(configuration),
            execute_request=normalized_execute_request,
            workspace=str(workspace),
            report_root=str(report_root),
            publish_engine_root=str(publish_engine_root) if publish_engine_root is not None else None,
            expected_source_sha256=str(expected_source_sha256),
            prepared_snapshot_id=(authoritative_snapshot_id or None),
            expected_mutant_ids=tuple(str(item) for item in expected_mutant_ids),
            test_fingerprints={
                str(mutant_id): {str(nodeid): str(fingerprint) for nodeid, fingerprint in rows.items()}
                for mutant_id, rows in test_fingerprints.items()
            },
            command_timeouts={str(key): float(value) for key, value in command_timeouts.items()},
            command=tuple(str(item) for item in command) if command is not None else None,
            cancel_path=str(cancel_path) if cancel_path is not None else None,
        )
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WorkerExecutionSpec":
        # Decode explicit fixture or engine fields without retaining a stringly typed command payload.
        raw_mode = required_string(value, "mode")
        try:
            mode = WorkerExecutionMode(raw_mode)
        except ValueError as exc:
            raise ValueError(f"unsupported worker execution mode: {raw_mode}") from exc
        raw_result = value.get("result", {})
        raw_configuration = value.get("configuration")
        raw_execute_request = value.get("execute_request")
        raw_fingerprints = value.get("test_fingerprints", {})
        raw_timeouts = value.get("command_timeouts", {})
        raw_command = value.get("command")
        if not isinstance(raw_result, Mapping):
            raise ValueError("result must be an object")
        if raw_configuration is not None and not isinstance(raw_configuration, Mapping):
            raise ValueError("configuration must be an object or null")
        if raw_execute_request is not None and not isinstance(raw_execute_request, Mapping):
            raise ValueError("execute_request must be an object or null")
        if not isinstance(raw_fingerprints, Mapping) or not isinstance(raw_timeouts, Mapping):
            raise ValueError("test_fingerprints and command_timeouts must be objects")
        fingerprints: dict[str, dict[str, str]] = {}
        for mutant_id, rows in raw_fingerprints.items():
            if not isinstance(rows, Mapping):
                raise ValueError("test_fingerprints rows must be objects")
            fingerprints[str(mutant_id)] = {
                str(nodeid): str(fingerprint)
                for nodeid, fingerprint in rows.items()
            }
        command = None
        if raw_command is not None:
            if not isinstance(raw_command, (list, tuple)):
                raise ValueError("command must be an array or null")
            command = tuple(str(item) for item in raw_command)
        raw_expected = value.get("expected_mutant_ids", [])
        if not isinstance(raw_expected, (list, tuple)):
            raise ValueError("expected_mutant_ids must be an array")
        return cls(
            mode=mode,
            result=dict(raw_result),
            delay_seconds=max(0.0, float(value.get("delay_seconds", 0.0))),
            configuration=dict(raw_configuration) if isinstance(raw_configuration, Mapping) else None,
            execute_request=dict(raw_execute_request) if isinstance(raw_execute_request, Mapping) else None,
            workspace=optional_string(value, "workspace"),
            report_root=optional_string(value, "report_root"),
            publish_engine_root=optional_string(value, "publish_engine_root"),
            expected_source_sha256=optional_string(value, "expected_source_sha256"),
            prepared_snapshot_id=optional_string(value, "prepared_snapshot_id"),
            expected_mutant_ids=tuple(str(item) for item in raw_expected),
            test_fingerprints=fingerprints,
            command_timeouts={str(key): float(item) for key, item in raw_timeouts.items()},
            command=command,
            cancel_path=optional_string(value, "cancel_path"),
        )
@dataclass(frozen=True, slots=True)
class RegisterWorker(WireModel):
    """Initial worker registration containing immutable process identity and capabilities."""
    identity: WorkerIdentity
    capabilities: WorkerCapabilities
    spool_root: str
    runtime_identity: Mapping[str, Any] | None = None
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "RegisterWorker":
        # Decode nested registration DTOs before the host accepts a process identity.
        raw_identity = require_mapping(value.get("identity"), field_name="identity")
        raw_capabilities = require_mapping(value.get("capabilities"), field_name="capabilities")
        return cls(
            identity=WorkerIdentity.from_dict(raw_identity),
            capabilities=WorkerCapabilities.from_dict(raw_capabilities),
            spool_root=required_string(value, "spool_root"),
            runtime_identity=(
                dict(value["runtime_identity"])
                if isinstance(value.get("runtime_identity"), Mapping)
                else None
            ),
        )
@dataclass(frozen=True, slots=True)
class RegisterWorkerReceipt(WireModel):
    """Host acknowledgement allowing or rejecting one worker registration."""
    accepted: bool
    reason: str | None = None
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "RegisterWorkerReceipt":
        # Preserve an explicit negative receipt so the worker exits fail-closed.
        return cls(bool(value.get("accepted", False)), optional_string(value, "reason"))
@dataclass(frozen=True, slots=True)
class AcquireAssignment(WireModel):
    """Worker pull request emitted whenever the persistent agent is idle."""
    completed_assignments: int = 0
    def __post_init__(self) -> None:
        # Keep the monotonic completed-assignment counter non-negative.
        if int(self.completed_assignments) < 0:
            raise ValueError("completed_assignments must not be negative")
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "AcquireAssignment":
        # Decode the worker progress counter without accepting negative values.
        return cls(max(0, int(value.get("completed_assignments", 0))))
@dataclass(frozen=True, slots=True)
class NoAssignment(WireModel):
    """Host response telling an idle worker when it may poll again."""
    wait_seconds: float = 0.0
    def __post_init__(self) -> None:
        # Reject negative polling delays at the protocol boundary.
        if float(self.wait_seconds) < 0:
            raise ValueError("wait_seconds must not be negative")
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "NoAssignment":
        # Normalize an optional non-negative idle delay.
        return cls(max(0.0, float(value.get("wait_seconds", 0.0))))
@dataclass(frozen=True, slots=True)
class AssignShard(WireModel):
    """Typed assignment command carrying immutable lease identity and explicit execution inputs."""
    assignment: ShardAssignment
    execution: WorkerExecutionSpec
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "AssignShard":
        # Decode both immutable assignment identity and the mode-specific execution specification.
        raw_assignment = require_mapping(value.get("assignment"), field_name="assignment")
        raw_execution = require_mapping(value.get("execution"), field_name="execution")
        return cls(
            ShardAssignment.from_dict(raw_assignment),
            WorkerExecutionSpec.from_dict(raw_execution),
        )
@dataclass(frozen=True, slots=True)
class WorkerHeartbeatFrame(WireModel):
    """Typed process heartbeat carrying worker and current child-process state."""
    heartbeat: WorkerHeartbeat
    completed_assignments: int = 0
    def __post_init__(self) -> None:
        # Keep the process-level completed counter non-negative.
        if int(self.completed_assignments) < 0:
            raise ValueError("completed_assignments must not be negative")
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WorkerHeartbeatFrame":
        # Decode the nested heartbeat before the host invokes lease renewal.
        raw_heartbeat = require_mapping(value.get("heartbeat"), field_name="heartbeat")
        return cls(
            WorkerHeartbeat.from_dict(raw_heartbeat),
            max(0, int(value.get("completed_assignments", 0))),
        )
@dataclass(frozen=True, slots=True)
class WorkerHeartbeatReceipt(WireModel):
    """Typed response to one process heartbeat."""
    receipt: HeartbeatReceipt
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WorkerHeartbeatReceipt":
        # Decode the authoritative heartbeat decision as a nested public DTO.
        raw_receipt = require_mapping(value.get("receipt"), field_name="receipt")
        return cls(HeartbeatReceipt.from_dict(raw_receipt))
@dataclass(frozen=True, slots=True)
class ExecutionDelivery(WireModel):
    """Typed durable shard delivery emitted before authoritative coordinator commit."""
    event_id: str
    payload_sha256: str
    assignment: ShardAssignment
    worker: WorkerIdentity
    capabilities: WorkerCapabilities
    shard_result: ShardExecutionResult | None
    engine_process_id: int | None
    published_engine_artifacts: tuple[str, ...]
    mutant_event_ids: tuple[str, ...]
    execution_envelopes: tuple[ExecutionEnvelope, ...]
    assignment_number: int
    def __post_init__(self) -> None:
        # Require delivery identity and validate child and per-mutant evidence cardinality.
        if not self.event_id or not self.payload_sha256:
            raise ValueError("execution delivery identity fields must be non-empty")
        if int(self.assignment_number) < 1:
            raise ValueError("assignment_number must be positive")
        if self.engine_process_id is not None and int(self.engine_process_id) <= 0:
            raise ValueError("engine_process_id must be positive when present")
        if len(self.mutant_event_ids) != len(self.execution_envelopes):
            raise ValueError("mutant event IDs and execution envelopes must have equal cardinality")
        if tuple(item.event_id for item in self.execution_envelopes) != self.mutant_event_ids:
            raise ValueError("execution envelope event IDs must match mutant_event_ids")
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ExecutionDelivery":
        # Decode shard-level and per-mutant evidence into concrete public contracts.
        raw_assignment = require_mapping(value.get("assignment"), field_name="assignment")
        raw_worker = require_mapping(value.get("worker"), field_name="worker")
        raw_capabilities = require_mapping(value.get("capabilities"), field_name="capabilities")
        raw_shard_result = value.get("shard_result")
        if raw_shard_result is not None and not isinstance(raw_shard_result, Mapping):
            raise ValueError("shard_result must be an object or null")
        raw_envelopes = value.get("execution_envelopes", [])
        if not isinstance(raw_envelopes, (list, tuple)) or any(
            not isinstance(item, Mapping) for item in raw_envelopes
        ):
            raise ValueError("execution_envelopes must be an array of objects")
        engine_process_id = value.get("engine_process_id")
        return cls(
            event_id=required_string(value, "event_id"),
            payload_sha256=required_string(value, "payload_sha256"),
            assignment=ShardAssignment.from_dict(raw_assignment),
            worker=WorkerIdentity.from_dict(raw_worker),
            capabilities=WorkerCapabilities.from_dict(raw_capabilities),
            shard_result=(
                ShardExecutionResult.from_dict(raw_shard_result)
                if isinstance(raw_shard_result, Mapping)
                else None
            ),
            engine_process_id=(
                int(engine_process_id) if engine_process_id is not None else None
            ),
            published_engine_artifacts=sequence_of_strings(
                value,
                "published_engine_artifacts",
            ),
            mutant_event_ids=sequence_of_strings(value, "mutant_event_ids"),
            execution_envelopes=tuple(
                ExecutionEnvelope.from_dict(item) for item in raw_envelopes
            ),
            assignment_number=max(1, int(value.get("assignment_number", 1))),
        )
@dataclass(frozen=True, slots=True)
class ExecutionReceipt(WireModel):
    """Authoritative coordinator receipt sent only after durable fan-in commit."""
    event_id: str
    accepted: bool
    committed_at: str | None = None
    reason: str | None = None
    def __post_init__(self) -> None:
        # Validate receipt identity and optional authoritative commit timestamp.
        if not self.event_id:
            raise ValueError("event_id must be non-empty")
        if self.committed_at is not None:
            validate_utc_timestamp(self.committed_at, field_name="committed_at")
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ExecutionReceipt":
        # Preserve both positive commit evidence and explicit negative rejection details.
        committed_at = optional_string(value, "committed_at")
        return cls(
            event_id=required_string(value, "event_id"),
            accepted=bool(value.get("accepted", False)),
            committed_at=(
                validate_utc_timestamp(committed_at, field_name="committed_at")
                if committed_at is not None
                else None
            ),
            reason=optional_string(value, "reason"),
        )
@dataclass(frozen=True, slots=True)
class ExecutionAcknowledged(WireModel):
    """Worker confirmation that a positive execution receipt cleared its spool entry."""
    event_id: str
    completed_assignments: int
    def __post_init__(self) -> None:
        # Require a valid event identity and monotonic completed counter.
        if not self.event_id:
            raise ValueError("event_id must be non-empty")
        if int(self.completed_assignments) < 1:
            raise ValueError("completed_assignments must be positive")
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ExecutionAcknowledged":
        # Decode the local spool acknowledgement projection.
        return cls(
            required_string(value, "event_id"),
            max(1, int(value.get("completed_assignments", 1))),
        )
@dataclass(frozen=True, slots=True)
class EngineLifecycle(WireModel):
    """Worker-owned engine child start or stop projection."""
    child_process_id: int
    campaign_id: str
    shard_id: str
    def __post_init__(self) -> None:
        # Reject invalid child identities before process-tree ownership is recorded.
        if int(self.child_process_id) <= 0:
            raise ValueError("child_process_id must be positive")
        if not self.campaign_id or not self.shard_id:
            raise ValueError("campaign_id and shard_id must be non-empty")
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "EngineLifecycle":
        # Decode one child lifecycle event with explicit campaign and shard ownership.
        return cls(
            int(value.get("child_process_id", 0)),
            required_string(value, "campaign_id"),
            required_string(value, "shard_id"),
        )
@dataclass(frozen=True, slots=True)
class CancelAssignment(WireModel):
    """Host command invalidating the currently owned assignment."""
    campaign_id: str
    shard_id: str
    lease_id: str
    attempt: int
    reason: str
    def __post_init__(self) -> None:
        # Require exact assignment identity so cancellation cannot target an unrelated shard.
        if not all((self.campaign_id, self.shard_id, self.lease_id, self.reason)):
            raise ValueError("cancel assignment fields must be non-empty")
        if int(self.attempt) < 0:
            raise ValueError("attempt must not be negative")
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "CancelAssignment":
        # Decode a cancellation command without implicit lease or attempt defaults.
        return cls(
            required_string(value, "campaign_id"),
            required_string(value, "shard_id"),
            required_string(value, "lease_id"),
            max(0, int(value.get("attempt", 0))),
            required_string(value, "reason"),
        )
@dataclass(frozen=True, slots=True)
class DrainWorker(WireModel):
    """Host command preventing new assignments while allowing current delivery to finish."""
    reason: str = "drain_requested"
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "DrainWorker":
        # Normalize an optional operational drain reason.
        return cls(str(value.get("reason", "drain_requested")))
@dataclass(frozen=True, slots=True)
class ShutdownWorker(WireModel):
    """Host command terminating the persistent worker after safe cleanup."""
    reason: str = "shutdown_requested"
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ShutdownWorker":
        # Normalize an optional shutdown reason.
        return cls(str(value.get("reason", "shutdown_requested")))
@dataclass(frozen=True, slots=True)
class WorkerTerminated(WireModel):
    """Terminal worker process projection."""
    completed_assignments: int
    exit_code: int
    def __post_init__(self) -> None:
        # Keep the terminal completed counter non-negative.
        if int(self.completed_assignments) < 0:
            raise ValueError("completed_assignments must not be negative")
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WorkerTerminated":
        # Decode the terminal worker result without masking a non-zero exit code.
        return cls(
            max(0, int(value.get("completed_assignments", 0))),
            int(value.get("exit_code", 0)),
        )
@dataclass(frozen=True, slots=True)
class WorkerProtocolError(WireModel):
    """Fail-closed protocol error emitted before worker termination."""
    error: str
    error_type: str
    def __post_init__(self) -> None:
        # Require diagnostic identity so the host never receives an empty rejection.
        if not self.error or not self.error_type:
            raise ValueError("protocol error fields must be non-empty")
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WorkerProtocolError":
        # Decode a terminal protocol rejection without executing its original frame.
        return cls(required_string(value, "error"), required_string(value, "error_type"))
WorkerPayload: TypeAlias = (
    RegisterWorker
    | RegisterWorkerReceipt
    | AcquireAssignment
    | NoAssignment
    | AssignShard
    | WorkerHeartbeatFrame
    | WorkerHeartbeatReceipt
    | ExecutionDelivery
    | ExecutionReceipt
    | ExecutionAcknowledged
    | EngineLifecycle
    | CancelAssignment
    | DrainWorker
    | ShutdownWorker
    | WorkerTerminated
    | WorkerProtocolError
)
_PAYLOAD_TYPES: dict[WorkerMessageType, type[WorkerPayload]] = {
    WorkerMessageType.REGISTER_WORKER: RegisterWorker,
    WorkerMessageType.REGISTER_WORKER_RECEIPT: RegisterWorkerReceipt,
    WorkerMessageType.ACQUIRE_ASSIGNMENT: AcquireAssignment,
    WorkerMessageType.NO_ASSIGNMENT: NoAssignment,
    WorkerMessageType.SHARD_ASSIGNMENT: AssignShard,
    WorkerMessageType.WORKER_HEARTBEAT: WorkerHeartbeatFrame,
    WorkerMessageType.HEARTBEAT_RECEIPT: WorkerHeartbeatReceipt,
    WorkerMessageType.EXECUTION_DELIVERY: ExecutionDelivery,
    WorkerMessageType.EXECUTION_RECEIPT: ExecutionReceipt,
    WorkerMessageType.EXECUTION_ACKNOWLEDGED: ExecutionAcknowledged,
    WorkerMessageType.ENGINE_STARTED: EngineLifecycle,
    WorkerMessageType.ENGINE_STOPPED: EngineLifecycle,
    WorkerMessageType.CANCEL_ASSIGNMENT: CancelAssignment,
    WorkerMessageType.DRAIN_WORKER: DrainWorker,
    WorkerMessageType.SHUTDOWN_WORKER: ShutdownWorker,
    WorkerMessageType.WORKER_TERMINATED: WorkerTerminated,
    WorkerMessageType.PROTOCOL_ERROR: WorkerProtocolError,
}
HOST_TO_WORKER_MESSAGE_TYPES = frozenset(
    {
        WorkerMessageType.REGISTER_WORKER_RECEIPT,
        WorkerMessageType.NO_ASSIGNMENT,
        WorkerMessageType.SHARD_ASSIGNMENT,
        WorkerMessageType.HEARTBEAT_RECEIPT,
        WorkerMessageType.EXECUTION_RECEIPT,
        WorkerMessageType.CANCEL_ASSIGNMENT,
        WorkerMessageType.DRAIN_WORKER,
        WorkerMessageType.SHUTDOWN_WORKER,
    }
)
WORKER_TO_HOST_MESSAGE_TYPES = frozenset(
    {
        WorkerMessageType.REGISTER_WORKER,
        WorkerMessageType.ACQUIRE_ASSIGNMENT,
        WorkerMessageType.WORKER_HEARTBEAT,
        WorkerMessageType.EXECUTION_DELIVERY,
        WorkerMessageType.EXECUTION_ACKNOWLEDGED,
        WorkerMessageType.ENGINE_STARTED,
        WorkerMessageType.ENGINE_STOPPED,
        WorkerMessageType.WORKER_TERMINATED,
        WorkerMessageType.PROTOCOL_ERROR,
    }
)
_COMPAT_EVENT_NAMES: dict[WorkerMessageType, str] = {
    WorkerMessageType.REGISTER_WORKER: "registered",
    WorkerMessageType.ACQUIRE_ASSIGNMENT: "acquire",
    WorkerMessageType.WORKER_HEARTBEAT: "heartbeat",
    WorkerMessageType.EXECUTION_DELIVERY: "delivery",
    WorkerMessageType.EXECUTION_ACKNOWLEDGED: "acknowledged",
    WorkerMessageType.ENGINE_STARTED: "engine_started",
    WorkerMessageType.ENGINE_STOPPED: "engine_stopped",
    WorkerMessageType.WORKER_TERMINATED: "terminated",
    WorkerMessageType.PROTOCOL_ERROR: "error",
}
@dataclass(frozen=True, slots=True)
class WorkerProtocolFrame:
    """Complete typed worker wire frame with identity, correlation and version metadata."""
    protocol_version: int
    schema_version: int
    message_type: WorkerMessageType
    message_id: str
    request_id: str
    created_at: str
    worker_id: str
    instance_id: str
    process_id: int
    sequence: int
    state: str
    payload: WorkerPayload
    correlation_id: str | None = None
    _payload_types: ClassVar[Mapping[WorkerMessageType, type[WorkerPayload]]] = _PAYLOAD_TYPES
    def __post_init__(self) -> None:
        # Reject incompatible versions and incomplete process identity before routing a frame.
        if isinstance(self.protocol_version, bool) or int(self.protocol_version) != WORKER_PROTOCOL_VERSION:
            raise IncompatibleProtocolError(
                f"unsupported worker protocol_version: {self.protocol_version}"
            )
        if isinstance(self.schema_version, bool) or not isinstance(self.schema_version, int):
            raise UnsupportedSchemaError("worker schema_version must be an integer")
        if self.schema_version < 0 or self.schema_version > WORKER_SCHEMA_VERSION:
            raise UnsupportedSchemaError(
                f"unsupported worker schema_version: {self.schema_version}"
            )
        if not isinstance(self.message_type, WorkerMessageType):
            raise UnknownMessageTypeError(f"unsupported worker message_type: {self.message_type}")
        if not all((self.message_id, self.request_id, self.worker_id, self.instance_id, self.state)):
            raise MissingFieldError("worker frame identity fields must be non-empty")
        if int(self.process_id) <= 0:
            raise MissingFieldError("worker process_id must be positive")
        if int(self.sequence) < 0:
            raise MissingFieldError("worker sequence must not be negative")
        validate_utc_timestamp(self.created_at)
        expected_payload = self._payload_types[self.message_type]
        if not isinstance(self.payload, expected_payload):
            raise MissingFieldError(
                f"{self.message_type.value} payload must be {expected_payload.__name__}"
            )
    @classmethod
    def create(
        cls,
        message_type: WorkerMessageType,
        payload: WorkerPayload,
        *,
        worker_id: str,
        instance_id: str,
        process_id: int,
        sequence: int,
        state: str,
        correlation_id: str | None = None,
        request_id: str | None = None,
        message_id: str | None = None,
        created_at: str | None = None,
    ) -> "WorkerProtocolFrame":
        # Create one canonical frame while keeping response request identity explicit.
        resolved_message_id = str(message_id or uuid.uuid4().hex)
        return cls(
            protocol_version=WORKER_PROTOCOL_VERSION,
            schema_version=WORKER_SCHEMA_VERSION,
            message_type=message_type,
            message_id=resolved_message_id,
            request_id=str(request_id or resolved_message_id),
            created_at=str(created_at or utc_now()),
            worker_id=str(worker_id),
            instance_id=str(instance_id),
            process_id=int(process_id),
            sequence=int(sequence),
            state=str(state),
            payload=payload,
            correlation_id=str(correlation_id) if correlation_id is not None else None,
        )
    def to_dict(self) -> dict[str, Any]:
        # Serialize a typed payload through the deterministic contract JSON boundary.
        return {
            "protocol_version": self.protocol_version,
            "schema_version": self.schema_version,
            "message_type": self.message_type.value,
            "message_id": self.message_id,
            "request_id": self.request_id,
            "created_at": self.created_at,
            "worker_id": self.worker_id,
            "instance_id": self.instance_id,
            "process_id": self.process_id,
            "sequence": self.sequence,
            "state": self.state,
            "correlation_id": self.correlation_id,
            "payload": to_json_value(self.payload),
        }
    def to_json(self) -> str:
        # Encode one complete frame as canonical single-line JSON.
        return dumps(self.to_dict())
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WorkerProtocolFrame":
        # Decode and type-dispatch one worker frame before any runtime side effect.
        if not isinstance(value, Mapping):
            raise MissingFieldError("worker frame must be an object")
        protocol_version = value.get("protocol_version")
        schema_version = value.get("schema_version")
        if isinstance(protocol_version, bool) or not isinstance(protocol_version, int):
            raise IncompatibleProtocolError("worker protocol_version must be an integer")
        if protocol_version != WORKER_PROTOCOL_VERSION:
            raise IncompatibleProtocolError(
                f"unsupported worker protocol_version: {protocol_version}"
            )
        if isinstance(schema_version, bool) or not isinstance(schema_version, int):
            raise UnsupportedSchemaError("worker schema_version must be an integer")
        if schema_version < 0 or schema_version > WORKER_SCHEMA_VERSION:
            raise UnsupportedSchemaError(
                f"unsupported worker schema_version: {schema_version}"
            )
        raw_message_type = required_string(value, "message_type")
        try:
            message_type = WorkerMessageType(raw_message_type)
        except ValueError as exc:
            raise UnknownMessageTypeError(
                f"unsupported worker message_type: {raw_message_type}"
            ) from exc
        raw_payload = require_mapping(value.get("payload"), field_name="payload")
        payload_type = cls._payload_types[message_type]
        try:
            payload = payload_type.from_dict(raw_payload)
        except (SerializationError, TypeError, ValueError) as exc:
            raise MissingFieldError(
                f"invalid {message_type.value} payload: {exc}"
            ) from exc
        return cls(
            protocol_version=protocol_version,
            schema_version=schema_version,
            message_type=message_type,
            message_id=required_string(value, "message_id"),
            request_id=required_string(value, "request_id"),
            created_at=validate_utc_timestamp(required_string(value, "created_at")),
            worker_id=required_string(value, "worker_id"),
            instance_id=required_string(value, "instance_id"),
            process_id=int(value.get("process_id", 0)),
            sequence=int(value.get("sequence", -1)),
            state=required_string(value, "state"),
            payload=payload,
            correlation_id=optional_string(value, "correlation_id"),
        )
    @classmethod
    def from_json(cls, raw: str | bytes) -> "WorkerProtocolFrame":
        # Decode one complete JSON object directly into a typed frame.
        return cls.from_dict(loads_object(raw))
    def require_identity(self, *, worker_id: str, instance_id: str, process_id: int) -> None:
        # Fail closed when a frame belongs to another worker instance or recycled PID.
        if (
            self.worker_id != str(worker_id)
            or self.instance_id != str(instance_id)
            or self.process_id != int(process_id)
        ):
            raise IdentityMismatchError(
                "worker frame identity does not match the active process instance"
            )
    def require_correlation(self, correlation_id: str) -> None:
        # Reject responses that cannot be tied to the initiating assignment or delivery.
        if self.correlation_id != str(correlation_id):
            raise CorrelationMismatchError(
                f"worker correlation mismatch: expected {correlation_id}, received {self.correlation_id}"
            )
    def to_compat_dict(self) -> dict[str, Any]:
        # Provide a read-only legacy projection without weakening the typed wire representation.
        value = self.to_dict()
        if self.message_type == WorkerMessageType.EXECUTION_DELIVERY:
            payload = self.payload
            if not isinstance(payload, ExecutionDelivery):
                raise MissingFieldError("execution delivery payload is not typed")
            value["payload"] = {
                "event_id": payload.event_id,
                "payload_sha256": payload.payload_sha256,
                "payload": {
                    "assignment": payload.assignment.to_dict(),
                    "worker": payload.worker.to_dict(),
                    "capabilities": payload.capabilities.to_dict(),
                    "result": {
                        "shard_result": (
                            payload.shard_result.to_dict()
                            if payload.shard_result is not None
                            else None
                        ),
                        "engine_process_id": payload.engine_process_id,
                        "published_engine_artifacts": list(
                            payload.published_engine_artifacts
                        ),
                    },
                },
                "mutant_event_ids": list(payload.mutant_event_ids),
                "execution_envelopes": [
                    item.to_dict() for item in payload.execution_envelopes
                ],
                "worker": payload.worker.to_dict(),
                "assignment_number": payload.assignment_number,
            }
        return {
            **value,
            "event": _COMPAT_EVENT_NAMES.get(self.message_type, self.message_type.value),
        }
def encode_worker_frame(frame: WorkerProtocolFrame) -> str:
    # Encode only the dedicated typed worker frame contract.
    if not isinstance(frame, WorkerProtocolFrame):
        raise SerializationError("worker protocol requires WorkerProtocolFrame")
    return frame.to_json()
def decode_worker_frame(
    raw: str | bytes,
    *,
    allowed_types: frozenset[WorkerMessageType] | None = None,
) -> WorkerProtocolFrame:
    # Decode a frame and enforce the allowed direction before returning it to runtime code.
    frame = WorkerProtocolFrame.from_json(raw)
    if allowed_types is not None and frame.message_type not in allowed_types:
        raise UnknownMessageTypeError(
            f"worker message_type {frame.message_type.value} is not allowed in this direction"
        )
    return frame
