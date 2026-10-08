"""Engine and canonical statistics event contracts with append-only journal adapters."""
from __future__ import annotations
from dataclasses import dataclass
import os
from pathlib import Path
from threading import Lock
from typing import Any, Mapping, Protocol
from .enums import EngineEventType, StatisticsEventType, enum_value
from .errors import ContractError, MissingFieldError
from .ids import (
    CampaignId,
    EventId,
    ExecutionId,
    MutantId,
    PlanId,
    ProcessId,
    ProducerId,
    ShardId,
    TestId,
    WorkerId,
    deterministic_id,
)
from .protocol import PROTOCOL_VERSION, SCHEMA_VERSION, MessageEnvelope
from .serialization import (
    SerializationError,
    dumps,
    loads_object,
    optional_string,
    required_integer,
    required_string,
    to_json_value,
    utc_now,
    validate_utc_timestamp,
)
STATISTICS_EVENT_SCHEMA_VERSION = 1
_STATISTICS_IDENTITY_FIELDS = (
    "campaign_id",
    "plan_id",
    "shard_id",
    "execution_id",
    "worker_id",
    "test_id",
    "mutant_id",
    "process_id",
)
_REQUIRED_STATISTICS_IDENTITIES = {
    StatisticsEventType.TEST_STARTED.value: ("campaign_id", "plan_id", "shard_id", "execution_id", "worker_id", "test_id"),
    StatisticsEventType.TEST_COMPLETED.value: ("campaign_id", "plan_id", "shard_id", "execution_id", "worker_id", "test_id"),
    StatisticsEventType.MUTANT_STARTED.value: ("campaign_id", "plan_id", "shard_id", "execution_id", "worker_id", "mutant_id"),
    StatisticsEventType.MUTANT_COMPLETED.value: ("campaign_id", "plan_id", "shard_id", "execution_id", "worker_id", "mutant_id"),
    StatisticsEventType.SELECTION_ESCALATED.value: ("campaign_id", "plan_id", "test_id"),
    StatisticsEventType.EXECUTION_TIMEOUT.value: ("campaign_id", "execution_id"),
    StatisticsEventType.INFRASTRUCTURE_FAILED.value: ("campaign_id",),
    StatisticsEventType.REUSE_DECIDED.value: ("campaign_id", "plan_id", "test_id"),
    StatisticsEventType.PLAN_DECIDED.value: ("campaign_id", "plan_id", "mutant_id"),
    StatisticsEventType.RECOVERY_PERFORMED.value: ("campaign_id",),
    StatisticsEventType.WORKER_STARTED.value: ("worker_id",),
    StatisticsEventType.WORKER_HEARTBEAT.value: ("worker_id",),
    StatisticsEventType.WORKER_STOPPED.value: ("worker_id",),
    StatisticsEventType.PROCESS_STARTED.value: ("process_id",),
    StatisticsEventType.PROCESS_STOPPED.value: ("process_id",),
}
_SUBJECT_REQUIRED_STATISTICS_EVENTS = frozenset(
    {
        StatisticsEventType.INFRASTRUCTURE_FAILED.value,
        StatisticsEventType.RECOVERY_PERFORMED.value,
    }
)
def _statistics_logical_identity(
    *,
    schema_version: int,
    event_type: str,
    producer_id: ProducerId,
    producer_sequence: int,
    campaign_id: CampaignId | None,
    plan_id: PlanId | None,
    shard_id: ShardId | None,
    execution_id: ExecutionId | None,
    worker_id: WorkerId | None,
    test_id: TestId | None,
    mutant_id: MutantId | None,
    process_id: ProcessId | None,
) -> dict[str, Any]:
    # Build the complete timestamp-free logical identity used by every statistics event ID.
    values = {
        "campaign_id": campaign_id,
        "plan_id": plan_id,
        "shard_id": shard_id,
        "execution_id": execution_id,
        "worker_id": worker_id,
        "test_id": test_id,
        "mutant_id": mutant_id,
        "process_id": process_id,
    }
    return {
        "schema_version": schema_version,
        "event_type": event_type,
        "producer_id": producer_id.value,
        "producer_sequence": producer_sequence,
        **{field_name: values[field_name].value if values[field_name] is not None else None for field_name in _STATISTICS_IDENTITY_FIELDS},
    }
@dataclass(frozen=True, slots=True)
class EngineEvent:
    """Versioned event emitted by the mutation engine and consumed by control-plane adapters."""
    event_type: str
    event_id: EventId
    campaign_id: CampaignId
    timestamp: str
    payload: Mapping[str, Any]
    sequence: int = 0
    worker_id: WorkerId | None = None
    shard_id: ShardId | None = None
    protocol_version: int = PROTOCOL_VERSION
    schema_version: int = SCHEMA_VERSION
    def __post_init__(self) -> None:
        # Validate event identity and preserve unknown event names as ordinary strings.
        if not isinstance(self.event_type, str) or not self.event_type.strip():
            raise MissingFieldError("event_type is required")
        if not isinstance(self.event_id, EventId) or not isinstance(self.campaign_id, CampaignId):
            raise MissingFieldError("event_id and campaign_id must use typed identifiers")
        if not isinstance(self.payload, Mapping):
            raise MissingFieldError("payload must be an object")
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int) or self.sequence < 0:
            raise ContractError("sequence must be a non-negative integer")
        if self.protocol_version != PROTOCOL_VERSION:
            raise ContractError(f"unsupported protocol_version: {self.protocol_version}")
        if self.schema_version < 0 or self.schema_version > SCHEMA_VERSION:
            raise ContractError(f"unsupported schema_version: {self.schema_version}")
        validate_utc_timestamp(self.timestamp, field_name="timestamp")
    @property
    def known_type(self) -> bool:
        # Let consumers branch on known event types without rejecting future events.
        return self.event_type in {item.value for item in EngineEventType}
    def to_dict(self) -> dict[str, Any]:
        # Emit both generic message aliases and event-specific names for protocol adapters.
        return {
            "protocol_version": self.protocol_version,
            "schema_version": self.schema_version,
            "message_type": "engine_event",
            "message_id": self.event_id.value,
            "created_at": self.timestamp,
            "event_type": self.event_type,
            "event_id": self.event_id.value,
            "campaign_id": self.campaign_id.value,
            "timestamp": self.timestamp,
            "sequence": self.sequence,
            "worker_id": self.worker_id.value if self.worker_id else None,
            "shard_id": self.shard_id.value if self.shard_id else None,
            "payload": to_json_value(self.payload),
        }
    def to_json(self) -> str:
        # Serialize one event deterministically for snapshots and JSONL journals.
        return dumps(self.to_dict())
    @classmethod
    def create(
        cls,
        event_type: str | EngineEventType,
        campaign_id: CampaignId | str,
        payload: Mapping[str, Any],
        *,
        sequence: int = 0,
        event_id: EventId | str | None = None,
        timestamp: str | None = None,
        worker_id: WorkerId | str | None = None,
        shard_id: ShardId | str | None = None,
    ) -> "EngineEvent":
        # Construct a fully typed event while keeping custom future event types legal.
        event_name = enum_value(event_type)
        current_campaign = campaign_id if isinstance(campaign_id, CampaignId) else CampaignId(str(campaign_id))
        current_timestamp = timestamp or utc_now()
        current_event_id = event_id
        if current_event_id is None:
            current_event_id = EventId(
                deterministic_id(
                    "evt",
                    current_campaign.value,
                    event_name,
                    sequence,
                    current_timestamp,
                )
            )
        elif not isinstance(current_event_id, EventId):
            current_event_id = EventId(str(current_event_id))
        current_worker = worker_id if isinstance(worker_id, WorkerId) or worker_id is None else WorkerId(str(worker_id))
        current_shard = shard_id if isinstance(shard_id, ShardId) or shard_id is None else ShardId(str(shard_id))
        return cls(
            event_type=event_name,
            event_id=current_event_id,
            campaign_id=current_campaign,
            timestamp=current_timestamp,
            payload=dict(payload),
            sequence=sequence,
            worker_id=current_worker,
            shard_id=current_shard,
        )
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "EngineEvent":
        # Parse the stable envelope and ignore unknown top-level fields from newer producers.
        envelope = MessageEnvelope.from_dict(value)
        if envelope.message_type != "engine_event":
            raise ContractError(f"unexpected message_type: {envelope.message_type}")
        event_type = required_string(value, "event_type")
        event_value = value.get("event_id", envelope.message_id)
        campaign_value = required_string(value, "campaign_id")
        timestamp = value.get("timestamp", envelope.created_at)
        if not isinstance(timestamp, str):
            raise MissingFieldError("timestamp must be a string")
        worker_value = optional_string(value, "worker_id")
        shard_value = optional_string(value, "shard_id")
        sequence = value.get("sequence", 0)
        if not isinstance(event_value, str) or not event_value.strip():
            raise MissingFieldError("event_id must be a non-empty string")
        return cls(
            event_type=event_type,
            event_id=EventId(event_value),
            campaign_id=CampaignId(campaign_value),
            timestamp=timestamp,
            payload=dict(envelope.payload),
            sequence=sequence,
            worker_id=WorkerId(worker_value) if worker_value else None,
            shard_id=ShardId(shard_value) if shard_value else None,
            protocol_version=envelope.protocol_version,
            schema_version=envelope.schema_version,
        )
    @classmethod
    def from_json(cls, raw: str | bytes) -> "EngineEvent":
        # Decode one event record without making event ordering a parsing concern.
        return cls.from_dict(loads_object(raw))
@dataclass(frozen=True, slots=True)
class StatisticsEvent:
    """Canonical append-only event whose identity never depends on wall-clock time or payload order."""
    event_type: str
    event_id: EventId
    timestamp: str
    producer_id: ProducerId
    producer_sequence: int
    payload: Mapping[str, Any]
    schema_version: int
    campaign_id: CampaignId | None = None
    plan_id: PlanId | None = None
    shard_id: ShardId | None = None
    execution_id: ExecutionId | None = None
    worker_id: WorkerId | None = None
    test_id: TestId | None = None
    mutant_id: MutantId | None = None
    process_id: ProcessId | None = None
    protocol_version: int = PROTOCOL_VERSION
    def __post_init__(self) -> None:
        # Validate the canonical event and prove that its supplied ID matches its logical identity.
        if not isinstance(self.event_type, str) or not self.event_type.strip():
            raise MissingFieldError("event_type is required")
        if not isinstance(self.event_id, EventId):
            raise MissingFieldError("event_id must use EventId")
        if not isinstance(self.producer_id, ProducerId):
            raise MissingFieldError("producer_id must use ProducerId")
        if isinstance(self.producer_sequence, bool) or not isinstance(self.producer_sequence, int) or self.producer_sequence < 0:
            raise ContractError("producer_sequence must be a non-negative integer")
        if not isinstance(self.payload, Mapping):
            raise MissingFieldError("payload must be an object")
        if self.protocol_version != PROTOCOL_VERSION:
            raise ContractError(f"unsupported protocol_version: {self.protocol_version}")
        if self.schema_version != STATISTICS_EVENT_SCHEMA_VERSION:
            raise ContractError(f"unsupported statistics schema_version: {self.schema_version}")
        validate_utc_timestamp(self.timestamp, field_name="timestamp")
        expected_types = {
            "campaign_id": CampaignId,
            "plan_id": PlanId,
            "shard_id": ShardId,
            "execution_id": ExecutionId,
            "worker_id": WorkerId,
            "test_id": TestId,
            "mutant_id": MutantId,
            "process_id": ProcessId,
        }
        for field_name, expected_type in expected_types.items():
            current = getattr(self, field_name)
            if current is not None and not isinstance(current, expected_type):
                raise MissingFieldError(f"{field_name} must use {expected_type.__name__}")
        for field_name in _REQUIRED_STATISTICS_IDENTITIES.get(self.event_type, ()):
            if getattr(self, field_name) is None:
                raise MissingFieldError(f"{field_name} is required for {self.event_type}")
        if self.event_type in _SUBJECT_REQUIRED_STATISTICS_EVENTS and not any(
            (self.execution_id, self.worker_id, self.process_id)
        ):
            raise MissingFieldError(f"execution_id, worker_id or process_id is required for {self.event_type}")
        if self.event_id != self.derived_event_id():
            raise ContractError("statistics event_id does not match logical identity")
    @property
    def known_type(self) -> bool:
        # Identify roadmap event types without rejecting forward-compatible custom names.
        return self.event_type in {item.value for item in StatisticsEventType}
    def logical_identity(self) -> dict[str, Any]:
        # Return every identity axis explicitly while excluding timestamp and payload.
        return _statistics_logical_identity(
            schema_version=self.schema_version,
            event_type=self.event_type,
            producer_id=self.producer_id,
            producer_sequence=self.producer_sequence,
            campaign_id=self.campaign_id,
            plan_id=self.plan_id,
            shard_id=self.shard_id,
            execution_id=self.execution_id,
            worker_id=self.worker_id,
            test_id=self.test_id,
            mutant_id=self.mutant_id,
            process_id=self.process_id,
        )
    def derived_event_id(self) -> EventId:
        # Derive the stable event ID from logical identity only.
        return EventId(deterministic_id("stat_evt", self.logical_identity()))
    def to_dict(self) -> dict[str, Any]:
        # Emit a canonical envelope with all identity axes explicit, including null identities.
        return {
            "protocol_version": self.protocol_version,
            "schema_version": self.schema_version,
            "message_type": "statistics_event",
            "message_id": self.event_id.value,
            "created_at": self.timestamp,
            "event_type": self.event_type,
            "event_id": self.event_id.value,
            "timestamp": self.timestamp,
            "producer_id": self.producer_id.value,
            "producer_sequence": self.producer_sequence,
            "campaign_id": self.campaign_id.value if self.campaign_id else None,
            "plan_id": self.plan_id.value if self.plan_id else None,
            "shard_id": self.shard_id.value if self.shard_id else None,
            "execution_id": self.execution_id.value if self.execution_id else None,
            "worker_id": self.worker_id.value if self.worker_id else None,
            "test_id": self.test_id.value if self.test_id else None,
            "mutant_id": self.mutant_id.value if self.mutant_id else None,
            "process_id": self.process_id.value if self.process_id else None,
            "payload": to_json_value(self.payload),
        }
    def to_json(self) -> str:
        # Serialize the complete event with deterministic key ordering.
        return dumps(self.to_dict())
    @classmethod
    def create(
        cls,
        event_type: str | StatisticsEventType,
        producer_id: ProducerId | str,
        producer_sequence: int,
        payload: Mapping[str, Any],
        *,
        timestamp: str | None = None,
        campaign_id: CampaignId | str | None = None,
        plan_id: PlanId | str | None = None,
        shard_id: ShardId | str | None = None,
        execution_id: ExecutionId | str | None = None,
        worker_id: WorkerId | str | None = None,
        test_id: TestId | str | None = None,
        mutant_id: MutantId | str | None = None,
        process_id: ProcessId | str | None = None,
        schema_version: int = STATISTICS_EVENT_SCHEMA_VERSION,
    ) -> "StatisticsEvent":
        # Construct typed identities first and derive the event ID without consulting the timestamp.
        event_name = enum_value(event_type)
        current_producer = producer_id if isinstance(producer_id, ProducerId) else ProducerId(str(producer_id))
        current_campaign = campaign_id if isinstance(campaign_id, CampaignId) or campaign_id is None else CampaignId(str(campaign_id))
        current_plan = plan_id if isinstance(plan_id, PlanId) or plan_id is None else PlanId(str(plan_id))
        current_shard = shard_id if isinstance(shard_id, ShardId) or shard_id is None else ShardId(str(shard_id))
        current_execution = execution_id if isinstance(execution_id, ExecutionId) or execution_id is None else ExecutionId(str(execution_id))
        current_worker = worker_id if isinstance(worker_id, WorkerId) or worker_id is None else WorkerId(str(worker_id))
        current_test = test_id if isinstance(test_id, TestId) or test_id is None else TestId(str(test_id))
        current_mutant = mutant_id if isinstance(mutant_id, MutantId) or mutant_id is None else MutantId(str(mutant_id))
        current_process = process_id if isinstance(process_id, ProcessId) or process_id is None else ProcessId(str(process_id))
        current_event_id = EventId(
            deterministic_id(
                "stat_evt",
                _statistics_logical_identity(
                    schema_version=schema_version,
                    event_type=event_name,
                    producer_id=current_producer,
                    producer_sequence=producer_sequence,
                    campaign_id=current_campaign,
                    plan_id=current_plan,
                    shard_id=current_shard,
                    execution_id=current_execution,
                    worker_id=current_worker,
                    test_id=current_test,
                    mutant_id=current_mutant,
                    process_id=current_process,
                ),
            )
        )
        return cls(
            event_type=event_name,
            event_id=current_event_id,
            timestamp=timestamp or utc_now(),
            producer_id=current_producer,
            producer_sequence=producer_sequence,
            payload=dict(payload),
            schema_version=schema_version,
            campaign_id=current_campaign,
            plan_id=current_plan,
            shard_id=current_shard,
            execution_id=current_execution,
            worker_id=current_worker,
            test_id=current_test,
            mutant_id=current_mutant,
            process_id=current_process,
        )
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "StatisticsEvent":
        # Parse every identity axis explicitly and reject absent schema or producer sequence fields.
        envelope = MessageEnvelope.from_dict(value)
        if envelope.message_type != "statistics_event":
            raise ContractError(f"unexpected message_type: {envelope.message_type}")
        event_type = required_string(value, "event_type")
        event_value = value.get("event_id", envelope.message_id)
        timestamp = value.get("timestamp", envelope.created_at)
        if not isinstance(event_value, str) or not event_value.strip():
            raise MissingFieldError("event_id must be a non-empty string")
        if event_value != envelope.message_id:
            raise ContractError("statistics event_id conflicts with message_id")
        if not isinstance(timestamp, str):
            raise MissingFieldError("timestamp must be a string")
        if timestamp != envelope.created_at:
            raise ContractError("statistics timestamp conflicts with created_at")
        campaign_value = optional_string(value, "campaign_id")
        plan_value = optional_string(value, "plan_id")
        shard_value = optional_string(value, "shard_id")
        execution_value = optional_string(value, "execution_id")
        worker_value = optional_string(value, "worker_id")
        test_value = optional_string(value, "test_id")
        mutant_value = optional_string(value, "mutant_id")
        process_value = optional_string(value, "process_id")
        return cls(
            event_type=event_type,
            event_id=EventId(event_value),
            timestamp=timestamp,
            producer_id=ProducerId(required_string(value, "producer_id")),
            producer_sequence=required_integer(value, "producer_sequence", minimum=0),
            payload=dict(envelope.payload),
            schema_version=envelope.schema_version,
            campaign_id=CampaignId(campaign_value) if campaign_value else None,
            plan_id=PlanId(plan_value) if plan_value else None,
            shard_id=ShardId(shard_value) if shard_value else None,
            execution_id=ExecutionId(execution_value) if execution_value else None,
            worker_id=WorkerId(worker_value) if worker_value else None,
            test_id=TestId(test_value) if test_value else None,
            mutant_id=MutantId(mutant_value) if mutant_value else None,
            process_id=ProcessId(process_value) if process_value else None,
            protocol_version=envelope.protocol_version,
        )
    @classmethod
    def from_json(cls, raw: str | bytes) -> "StatisticsEvent":
        # Decode one canonical statistics event from its complete JSON object.
        return cls.from_dict(loads_object(raw))
class EngineEventSink(Protocol):
    """Minimal dependency direction for event publishers."""
    def publish(self, event: EngineEvent) -> None:
        # Define the synchronous event boundary used by local adapters.
        ...
class MemoryEventSink:
    """In-memory sink useful for adapters and contract tests."""
    def __init__(self) -> None:
        # Keep event collection independent from any persistence technology.
        self.events: list[EngineEvent] = []
    def publish(self, event: EngineEvent) -> None:
        # Append exactly the typed event supplied by the producer.
        if not isinstance(event, EngineEvent):
            raise ContractError("event sink accepts EngineEvent values only")
        self.events.append(event)
class JsonlEventSink:
    """Thread-safe append-only JSONL sink for local event journals."""
    def __init__(self, path: Path) -> None:
        # Open lazily so constructing an adapter never mutates a project or report directory.
        self.path = Path(path)
        self._lock = Lock()
    def publish(self, event: EngineEvent) -> None:
        # Append one complete event and flush it without forcing every record to disk.
        if not isinstance(event, EngineEvent):
            raise ContractError("event sink accepts EngineEvent values only")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        line = event.to_json() + "\n"
        with self._lock, self.path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())
class EventJournal:
    """Deduplicating, order-tolerant event projection."""
    def __init__(self, events: list[EngineEvent] | None = None) -> None:
        # Index by event ID so retries and replay cannot duplicate projections.
        self._events: dict[str, EngineEvent] = {}
        for event in events or []:
            self.append(event)
    def append(self, event: EngineEvent) -> bool:
        # Accept idempotent retries but reject an event ID reused for another payload.
        if not isinstance(event, EngineEvent):
            raise ContractError("journal accepts EngineEvent values only")
        existing = self._events.get(event.event_id.value)
        if existing is not None:
            if existing.to_json() != event.to_json():
                raise ContractError("event_id_conflict: event ID is bound to another payload")
            return False
        self._events[event.event_id.value] = event
        return True
    def events(self) -> tuple[EngineEvent, ...]:
        # Return a deterministic replay order independent of JSONL arrival order.
        return tuple(
            sorted(
                self._events.values(),
                key=lambda item: (item.sequence, item.timestamp, item.event_id.value),
            )
        )
    def __len__(self) -> int:
        # Expose the projected event count without exposing its dictionary storage.
        return len(self._events)
def read_event_journal(path: Path) -> EventJournal:
    # Stream a JSONL journal while retaining unknown event types and rejecting corrupt records.
    journal = EventJournal()
    with Path(path).open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                journal.append(EngineEvent.from_json(line))
            except (ContractError, SerializationError, ValueError) as exc:
                raise ContractError(f"invalid event at line {line_number}: {exc}") from exc
    return journal
