"""Incremental read models derived from canonical append-only statistics events."""
from __future__ import annotations
import base64
import binascii
import hashlib
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Literal, Mapping
from theseus_contracts.events import StatisticsEvent
from theseus_contracts.serialization import SerializationError, dumps, loads_object
from .store import StatisticsEventStore
STATISTICS_PROJECTION_SCHEMA_VERSION = 3
STATISTICS_PROJECTION_MAX_BATCH = 5_000
STATISTICS_PROJECTION_MAX_LIMIT = 500
STATISTICS_DURATION_SAMPLE_SIZE = 256
ProjectionEntityType = Literal["campaign", "plan", "test", "mutant", "worker", "execution"]
_PROJECTION_ENTITY_TYPES = frozenset(("campaign", "plan", "test", "mutant", "worker", "execution"))
_COMPLETION_EVENTS = frozenset(("test.completed", "mutant.completed", "execution.timeout", "infrastructure.failed"))
_WORKER_BUSY_EVENTS = frozenset(("mutant.completed", "execution.timeout", "process.stopped"))
_PASS_OUTCOMES = frozenset(("pass", "passed", "ok", "success", "succeeded"))
_FAIL_OUTCOMES = frozenset(("fail", "failed", "failure"))
_ERROR_OUTCOMES = frozenset(("error", "errored", "internal_error", "infrastructure_failed"))
_TIMEOUT_OUTCOMES = frozenset(("timeout", "timed_out", "timedout"))
_SUMMARY_COLUMNS = (
    "event_count",
    "started_count",
    "completed_count",
    "passed_count",
    "failed_count",
    "error_count",
    "timeout_count",
    "retry_count",
    "recovery_count",
    "escalation_count",
    "reuse_count",
    "killed_count",
    "survived_count",
    "invalid_count",
    "plan_decision_count",
    "plan_execute_count",
    "plan_reuse_count",
    "plan_partial_reuse_count",
    "plan_audit_count",
    "plan_excluded_count",
    "infrastructure_failure_count",
    "flaky_transition_count",
    "duration_count",
    "duration_total_ms",
    "duration_min_ms",
    "duration_max_ms",
    "busy_duration_ms",
)
class StatisticsProjectionError(RuntimeError):
    """Base failure for derived statistics read models."""
@dataclass(frozen=True, slots=True)
class StatisticsProjectionCursor:
    """Opaque keyset position for one entity-type summary query."""
    entity_type: str
    entity_id: str
    def __post_init__(self) -> None:
        # Validate the cursor before it can affect a bounded summary query.
        if not isinstance(self.entity_type, str) or self.entity_type not in _PROJECTION_ENTITY_TYPES:
            raise ValueError(f"unsupported projection entity_type: {self.entity_type}")
        if not isinstance(self.entity_id, str) or not self.entity_id.strip():
            raise ValueError("projection cursor entity_id must be a non-empty string")
    def encode(self) -> str:
        # Encode a stable keyset cursor without exposing SQL details.
        payload = dumps({"entity_id": self.entity_id, "entity_type": self.entity_type}).encode("utf-8")
        return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    @classmethod
    def decode(cls, value: str) -> "StatisticsProjectionCursor":
        # Decode and validate one opaque projection cursor.
        if not isinstance(value, str) or not value.strip():
            raise ValueError("projection cursor must be a non-empty string")
        try:
            padding = "=" * (-len(value) % 4)
            decoded = loads_object(base64.urlsafe_b64decode((value + padding).encode("ascii")))
        except (binascii.Error, UnicodeError, ValueError, SerializationError) as exc:
            raise ValueError("invalid statistics projection cursor") from exc
        return cls(entity_type=decoded.get("entity_type"), entity_id=decoded.get("entity_id"))
@dataclass(frozen=True, slots=True)
class StatisticsProjectionCheckpoint:
    """Durable event position applied to all derived read models."""
    sequence: int
    event_id: str
    def __post_init__(self) -> None:
        # Require an event ID after sequence zero while allowing the initial empty checkpoint.
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int) or self.sequence < 0:
            raise ValueError("projection checkpoint sequence must be a non-negative integer")
        if not isinstance(self.event_id, str) or (self.sequence > 0 and not self.event_id.strip()):
            raise ValueError("projection checkpoint event_id is required after sequence zero")
@dataclass(frozen=True, slots=True)
class StatisticsProjectionResult:
    """Bounded result of one incremental projection batch."""
    events_projected: int
    checkpoint: StatisticsProjectionCheckpoint
    has_more: bool
@dataclass(frozen=True, slots=True)
class StatisticsSummary:
    """One campaign, plan, test, mutant, worker or execution read model."""
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
    killed_count: int
    survived_count: int
    invalid_count: int
    plan_decision_count: int
    plan_execute_count: int
    plan_reuse_count: int
    plan_partial_reuse_count: int
    plan_audit_count: int
    plan_excluded_count: int
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
    def to_dict(self) -> dict[str, Any]:
        # Expose a JSON-compatible bounded read model for exports and adapters.
        return {field_name: getattr(self, field_name) for field_name in self.__dataclass_fields__}
@dataclass(frozen=True, slots=True)
class StatisticsSummaryPage:
    """Bounded keyset page of derived entity summaries."""
    items: tuple[StatisticsSummary, ...]
    next_cursor: str | None
def _validate_limit(value: int, *, maximum: int) -> int:
    # Reject booleans and unbounded projection operations.
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("limit must be an integer")
    if value < 1 or value > maximum:
        raise ValueError(f"limit must be between 1 and {maximum}")
    return value
def _event_entities(event: StatisticsEvent) -> tuple[tuple[str, str], ...]:
    # Map explicit canonical identities to the bounded statistics summary domains.
    values = (
        ("campaign", event.campaign_id.value if event.campaign_id else None),
        ("plan", event.plan_id.value if event.plan_id else None),
        ("test", event.test_id.value if event.test_id else None),
        ("mutant", event.mutant_id.value if event.mutant_id else None),
        ("worker", event.worker_id.value if event.worker_id else None),
        ("execution", event.execution_id.value if event.execution_id else None),
    )
    return tuple((entity_type, entity_id) for entity_type, entity_id in values if entity_id is not None)
def _payload_string(payload: Mapping[str, Any], *names: str) -> str | None:
    # Read the first non-empty scalar outcome field without accepting nested structures.
    for name in names:
        value = payload.get(name)
        if isinstance(value, str) and value.strip():
            return value.strip().lower()
    return None
def _normalized_outcome(event: StatisticsEvent) -> str | None:
    # Normalize only terminal pass/fail/error/timeout outcomes used by PR20 counters.
    if event.event_type == "execution.timeout":
        return "timeout"
    if event.event_type == "infrastructure.failed":
        return "error"
    if event.event_type not in _COMPLETION_EVENTS:
        return None
    value = _payload_string(event.payload, "outcome", "status", "result")
    if value in _PASS_OUTCOMES:
        return "passed"
    if value in _FAIL_OUTCOMES:
        return "failed"
    if value in _ERROR_OUTCOMES:
        return "error"
    if value in _TIMEOUT_OUTCOMES:
        return "timeout"
    return None
def _mutant_semantic_result(event: StatisticsEvent) -> str | None:
    # Normalize only authoritative mutant completion results used by adaptive planning history.
    if event.event_type != "mutant.completed":
        return None
    value = _payload_string(event.payload, "semantic_result", "outcome", "status", "result")
    if value in {"killed", "kill"}:
        return "killed"
    if value in {"survived", "survive"}:
        return "survived"
    if value in {"invalid", "invalid_mutant"}:
        return "invalid"
    return None
def _duration_ms(event: StatisticsEvent) -> float | None:
    # Read one finite non-negative duration from explicit millisecond or second payload fields.
    raw: object | None = None
    multiplier = 1.0
    for name in ("duration_ms", "elapsed_ms", "runtime_ms"):
        if name in event.payload:
            raw = event.payload.get(name)
            break
    else:
        for name in ("duration_seconds", "elapsed_seconds", "runtime_seconds"):
            if name in event.payload:
                raw = event.payload.get(name)
                multiplier = 1000.0
                break
    if raw is None or isinstance(raw, bool):
        return None
    try:
        value = float(raw) * multiplier
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) and value >= 0.0 else None
def _is_retry(event: StatisticsEvent) -> bool:
    # Count explicit retry flags and positive attempt numbers without guessing from timestamps.
    retry = event.payload.get("retry")
    if isinstance(retry, bool):
        return retry
    attempt = event.payload.get("attempt")
    return isinstance(attempt, int) and not isinstance(attempt, bool) and attempt > 0
def _parse_timestamp(value: str) -> datetime:
    # Parse the already validated UTC event timestamp for deterministic interval arithmetic.
    return datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
def _elapsed_ms(start: str | None, end: str) -> float:
    # Return a non-negative elapsed interval and fail closed to zero on reversed event time.
    if not start:
        return 0.0
    return max(0.0, (_parse_timestamp(end) - _parse_timestamp(start)).total_seconds() * 1000.0)
def _percentile(values: list[float], percentile: float) -> float:
    # Interpolate one percentile from the bounded deterministic sample only.
    if not values:
        return 0.0
    values.sort()
    position = (len(values) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    fraction = position - lower
    return values[lower] + (values[upper] - values[lower]) * fraction
class StatisticsProjectionStore:
    """Incremental, crash-safe read models over one canonical statistics event store."""
    def __init__(self, event_store: StatisticsEventStore) -> None:
        # Bind projection state to the same SQLite database as its immutable source events.
        if not isinstance(event_store, StatisticsEventStore):
            raise TypeError("event_store must be StatisticsEventStore")
        self.event_store = event_store
        connection = self.event_store._connect()
        try:
            self._ensure_schema(connection)
        finally:
            connection.close()
    @classmethod
    def _ensure_schema(cls, connection: sqlite3.Connection) -> None:
        # Create mutable derived tables without altering the append-only event identity contract.
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS statistics_projection_metadata (
                projection_name TEXT PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                last_sequence INTEGER NOT NULL,
                last_event_id TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS statistics_summaries (
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                event_count INTEGER NOT NULL DEFAULT 0,
                started_count INTEGER NOT NULL DEFAULT 0,
                completed_count INTEGER NOT NULL DEFAULT 0,
                passed_count INTEGER NOT NULL DEFAULT 0,
                failed_count INTEGER NOT NULL DEFAULT 0,
                error_count INTEGER NOT NULL DEFAULT 0,
                timeout_count INTEGER NOT NULL DEFAULT 0,
                retry_count INTEGER NOT NULL DEFAULT 0,
                recovery_count INTEGER NOT NULL DEFAULT 0,
                escalation_count INTEGER NOT NULL DEFAULT 0,
                reuse_count INTEGER NOT NULL DEFAULT 0,
                killed_count INTEGER NOT NULL DEFAULT 0,
                survived_count INTEGER NOT NULL DEFAULT 0,
                invalid_count INTEGER NOT NULL DEFAULT 0,
                plan_decision_count INTEGER NOT NULL DEFAULT 0,
                plan_execute_count INTEGER NOT NULL DEFAULT 0,
                plan_reuse_count INTEGER NOT NULL DEFAULT 0,
                plan_partial_reuse_count INTEGER NOT NULL DEFAULT 0,
                plan_audit_count INTEGER NOT NULL DEFAULT 0,
                plan_excluded_count INTEGER NOT NULL DEFAULT 0,
                infrastructure_failure_count INTEGER NOT NULL DEFAULT 0,
                flaky_transition_count INTEGER NOT NULL DEFAULT 0,
                duration_count INTEGER NOT NULL DEFAULT 0,
                duration_total_ms REAL NOT NULL DEFAULT 0.0,
                duration_min_ms REAL,
                duration_max_ms REAL,
                busy_duration_ms REAL NOT NULL DEFAULT 0.0,
                active_duration_ms REAL NOT NULL DEFAULT 0.0,
                active_since TEXT,
                last_outcome TEXT,
                last_event_type TEXT NOT NULL DEFAULT '',
                last_event_timestamp TEXT NOT NULL DEFAULT '',
                PRIMARY KEY(entity_type, entity_id)
            );
            CREATE INDEX IF NOT EXISTS ix_statistics_summaries_type_id
            ON statistics_summaries(entity_type, entity_id);
            CREATE TABLE IF NOT EXISTS statistics_duration_samples (
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                sample_slot INTEGER NOT NULL,
                event_id TEXT NOT NULL,
                sample_rank TEXT NOT NULL,
                duration_ms REAL NOT NULL,
                PRIMARY KEY(entity_type, entity_id, sample_slot),
                UNIQUE(entity_type, entity_id, event_id)
            );
            CREATE INDEX IF NOT EXISTS ix_statistics_duration_samples_entity
            ON statistics_duration_samples(entity_type, entity_id, sample_slot);
            CREATE TABLE IF NOT EXISTS statistics_campaign_test_states (
                campaign_id TEXT NOT NULL,
                test_id TEXT NOT NULL,
                last_outcome TEXT NOT NULL,
                PRIMARY KEY(campaign_id, test_id)
            );
            """
        )
        row = connection.execute(
            "SELECT schema_version FROM statistics_projection_metadata WHERE projection_name = 'default'"
        ).fetchone()
        if row is None:
            connection.execute(
                "INSERT INTO statistics_projection_metadata(projection_name, schema_version, last_sequence, last_event_id) VALUES ('default', ?, 0, '')",
                (STATISTICS_PROJECTION_SCHEMA_VERSION,),
            )
            cls._ensure_summary_columns(connection)
        else:
            current_version = int(row["schema_version"])
            if current_version not in {1, 2, STATISTICS_PROJECTION_SCHEMA_VERSION}:
                raise StatisticsProjectionError(
                    f"unsupported statistics projection schema_version: {row['schema_version']}"
                )
            cls._ensure_summary_columns(connection)
            if current_version != STATISTICS_PROJECTION_SCHEMA_VERSION:
                connection.execute(
                    "UPDATE statistics_projection_metadata SET schema_version = ? WHERE projection_name = 'default'",
                    (STATISTICS_PROJECTION_SCHEMA_VERSION,),
                )
    @staticmethod
    def _ensure_summary_columns(connection: sqlite3.Connection) -> None:
        # Add planner and adaptive-history counters without rebuilding immutable event history.
        existing = {str(row[1]) for row in connection.execute("PRAGMA table_info(statistics_summaries)").fetchall()}
        for column in (
            "killed_count",
            "survived_count",
            "invalid_count",
            "plan_decision_count",
            "plan_execute_count",
            "plan_reuse_count",
            "plan_partial_reuse_count",
            "plan_audit_count",
            "plan_excluded_count",
        ):
            if column not in existing:
                connection.execute(
                    f"ALTER TABLE statistics_summaries ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0"
                )
    @staticmethod
    def _summary_row(connection: sqlite3.Connection, entity_type: str, entity_id: str) -> sqlite3.Row | None:
        # Load one bounded mutable row needed to calculate transitions and worker intervals.
        return connection.execute(
            "SELECT * FROM statistics_summaries WHERE entity_type = ? AND entity_id = ?",
            (entity_type, entity_id),
        ).fetchone()
    @staticmethod
    def _sample_duration(
        connection: sqlite3.Connection,
        *,
        entity_type: str,
        entity_id: str,
        event_id: str,
        duration_ms: float,
    ) -> None:
        # Keep one deterministic minimum-rank observation in each of 256 stable hash slots.
        digest = hashlib.sha256(f"{entity_type}\0{entity_id}\0{event_id}".encode("utf-8")).hexdigest()
        sample_slot = int(digest[:16], 16) % STATISTICS_DURATION_SAMPLE_SIZE
        connection.execute(
            """
            INSERT INTO statistics_duration_samples(
                entity_type, entity_id, sample_slot, event_id, sample_rank, duration_ms
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(entity_type, entity_id, event_id) DO NOTHING
            ON CONFLICT(entity_type, entity_id, sample_slot) DO UPDATE SET
                event_id = excluded.event_id,
                sample_rank = excluded.sample_rank,
                duration_ms = excluded.duration_ms
            WHERE excluded.sample_rank < statistics_duration_samples.sample_rank
            """,
            (entity_type, entity_id, sample_slot, event_id, digest, duration_ms),
        )
    def _apply_summary(
        self,
        connection: sqlite3.Connection,
        *,
        event: StatisticsEvent,
        entity_type: str,
        entity_id: str,
        outcome: str | None,
        duration_ms: float | None,
        flaky_increment: int,
    ) -> None:
        # Apply one event delta to one entity summary without reading unrelated history.
        existing = self._summary_row(connection, entity_type, entity_id)
        counters = {name: 0 for name in _SUMMARY_COLUMNS}
        counters["duration_min_ms"] = None
        counters["duration_max_ms"] = None
        counters["event_count"] = 1
        if event.event_type.endswith(".started"):
            counters["started_count"] = 1
        if event.event_type.endswith(".completed") or event.event_type.endswith(".stopped"):
            counters["completed_count"] = 1
        if outcome == "passed":
            counters["passed_count"] = 1
        elif outcome == "failed":
            counters["failed_count"] = 1
        elif outcome == "error":
            counters["error_count"] = 1
        elif outcome == "timeout":
            counters["timeout_count"] = 1
        if _is_retry(event):
            counters["retry_count"] = 1
        if event.event_type == "recovery.performed":
            counters["recovery_count"] = 1
        if event.event_type == "selection.escalated":
            counters["escalation_count"] = 1
        if event.event_type == "reuse.decided":
            counters["reuse_count"] = 1
        semantic_result = _mutant_semantic_result(event)
        if semantic_result == "killed":
            counters["killed_count"] = 1
        elif semantic_result == "survived":
            counters["survived_count"] = 1
        elif semantic_result == "invalid":
            counters["invalid_count"] = 1
        if event.event_type == "plan.decided":
            counters["plan_decision_count"] = 1
            action = _payload_string(event.payload, "action")
            if action == "execute":
                counters["plan_execute_count"] = 1
            elif action == "reuse":
                counters["plan_reuse_count"] = 1
            elif action == "partial_reuse":
                counters["plan_partial_reuse_count"] = 1
            elif action == "audit":
                counters["plan_audit_count"] = 1
            elif action in {"budget_excluded", "outside_scope", "deduplicate"}:
                counters["plan_excluded_count"] = 1
        if event.event_type == "infrastructure.failed":
            counters["infrastructure_failure_count"] = 1
        counters["flaky_transition_count"] = flaky_increment
        if duration_ms is not None:
            counters["duration_count"] = 1
            counters["duration_total_ms"] = duration_ms
            counters["duration_min_ms"] = duration_ms
            counters["duration_max_ms"] = duration_ms
        if entity_type == "worker" and duration_ms is not None and event.event_type in _WORKER_BUSY_EVENTS:
            counters["busy_duration_ms"] = duration_ms
        active_since = str(existing["active_since"]) if existing is not None and existing["active_since"] else None
        active_duration_increment = 0.0
        if entity_type == "worker" and event.event_type == "worker.started" and active_since is None:
            active_since = event.timestamp
        elif entity_type == "worker" and event.event_type == "worker.stopped" and active_since is not None:
            active_duration_increment = _elapsed_ms(active_since, event.timestamp)
            active_since = None
        last_outcome = outcome if outcome is not None else (str(existing["last_outcome"]) if existing is not None and existing["last_outcome"] else None)
        if existing is None:
            connection.execute(
                """
                INSERT INTO statistics_summaries(
                    entity_type, entity_id, event_count, started_count, completed_count,
                    passed_count, failed_count, error_count, timeout_count, retry_count,
                    recovery_count, escalation_count, reuse_count, killed_count,
                    survived_count, invalid_count, plan_decision_count, plan_execute_count,
                    plan_reuse_count, plan_partial_reuse_count,
                    plan_audit_count, plan_excluded_count, infrastructure_failure_count,
                    flaky_transition_count, duration_count, duration_total_ms, duration_min_ms,
                    duration_max_ms, busy_duration_ms, active_duration_ms, active_since,
                    last_outcome, last_event_type, last_event_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    entity_type,
                    entity_id,
                    counters["event_count"],
                    counters["started_count"],
                    counters["completed_count"],
                    counters["passed_count"],
                    counters["failed_count"],
                    counters["error_count"],
                    counters["timeout_count"],
                    counters["retry_count"],
                    counters["recovery_count"],
                    counters["escalation_count"],
                    counters["reuse_count"],
                    counters["killed_count"],
                    counters["survived_count"],
                    counters["invalid_count"],
                    counters["plan_decision_count"],
                    counters["plan_execute_count"],
                    counters["plan_reuse_count"],
                    counters["plan_partial_reuse_count"],
                    counters["plan_audit_count"],
                    counters["plan_excluded_count"],
                    counters["infrastructure_failure_count"],
                    counters["flaky_transition_count"],
                    counters["duration_count"],
                    counters["duration_total_ms"],
                    counters["duration_min_ms"],
                    counters["duration_max_ms"],
                    counters["busy_duration_ms"],
                    active_duration_increment,
                    active_since,
                    last_outcome,
                    event.event_type,
                    event.timestamp,
                ),
            )
        else:
            connection.execute(
                """
                UPDATE statistics_summaries SET
                    event_count = event_count + ?,
                    started_count = started_count + ?,
                    completed_count = completed_count + ?,
                    passed_count = passed_count + ?,
                    failed_count = failed_count + ?,
                    error_count = error_count + ?,
                    timeout_count = timeout_count + ?,
                    retry_count = retry_count + ?,
                    recovery_count = recovery_count + ?,
                    escalation_count = escalation_count + ?,
                    reuse_count = reuse_count + ?,
                    killed_count = killed_count + ?,
                    survived_count = survived_count + ?,
                    invalid_count = invalid_count + ?,
                    plan_decision_count = plan_decision_count + ?,
                    plan_execute_count = plan_execute_count + ?,
                    plan_reuse_count = plan_reuse_count + ?,
                    plan_partial_reuse_count = plan_partial_reuse_count + ?,
                    plan_audit_count = plan_audit_count + ?,
                    plan_excluded_count = plan_excluded_count + ?,
                    infrastructure_failure_count = infrastructure_failure_count + ?,
                    flaky_transition_count = flaky_transition_count + ?,
                    duration_count = duration_count + ?,
                    duration_total_ms = duration_total_ms + ?,
                    duration_min_ms = CASE WHEN ? IS NULL THEN duration_min_ms WHEN duration_min_ms IS NULL OR ? < duration_min_ms THEN ? ELSE duration_min_ms END,
                    duration_max_ms = CASE WHEN ? IS NULL THEN duration_max_ms WHEN duration_max_ms IS NULL OR ? > duration_max_ms THEN ? ELSE duration_max_ms END,
                    busy_duration_ms = busy_duration_ms + ?,
                    active_duration_ms = active_duration_ms + ?,
                    active_since = ?,
                    last_outcome = ?,
                    last_event_type = ?,
                    last_event_timestamp = ?
                WHERE entity_type = ? AND entity_id = ?
                """,
                (
                    counters["event_count"],
                    counters["started_count"],
                    counters["completed_count"],
                    counters["passed_count"],
                    counters["failed_count"],
                    counters["error_count"],
                    counters["timeout_count"],
                    counters["retry_count"],
                    counters["recovery_count"],
                    counters["escalation_count"],
                    counters["reuse_count"],
                    counters["killed_count"],
                    counters["survived_count"],
                    counters["invalid_count"],
                    counters["plan_decision_count"],
                    counters["plan_execute_count"],
                    counters["plan_reuse_count"],
                    counters["plan_partial_reuse_count"],
                    counters["plan_audit_count"],
                    counters["plan_excluded_count"],
                    counters["infrastructure_failure_count"],
                    counters["flaky_transition_count"],
                    counters["duration_count"],
                    counters["duration_total_ms"],
                    counters["duration_min_ms"],
                    counters["duration_min_ms"],
                    counters["duration_min_ms"],
                    counters["duration_max_ms"],
                    counters["duration_max_ms"],
                    counters["duration_max_ms"],
                    counters["busy_duration_ms"],
                    active_duration_increment,
                    active_since,
                    last_outcome,
                    event.event_type,
                    event.timestamp,
                    entity_type,
                    entity_id,
                ),
            )
        if duration_ms is not None:
            self._sample_duration(
                connection,
                entity_type=entity_type,
                entity_id=entity_id,
                event_id=event.event_id.value,
                duration_ms=duration_ms,
            )
    @staticmethod
    def _is_flaky_transition(previous_outcome: str | None, outcome: str) -> bool:
        # Count only transitions between passing and non-passing terminal test outcomes.
        return (
            previous_outcome is not None
            and previous_outcome != outcome
            and (previous_outcome == "passed" or outcome == "passed")
        )
    def _apply_event(self, connection: sqlite3.Connection, event: StatisticsEvent) -> None:
        # Derive all entity deltas from one canonical event exactly once.
        outcome = _normalized_outcome(event)
        duration_ms = _duration_ms(event)
        test_flaky_increment = 0
        campaign_flaky_increment = 0
        if (
            event.event_type == "test.completed"
            and event.test_id is not None
            and outcome in {"passed", "failed", "error", "timeout"}
        ):
            previous = self._summary_row(connection, "test", event.test_id.value)
            previous_outcome = str(previous["last_outcome"]) if previous is not None and previous["last_outcome"] else None
            test_flaky_increment = int(self._is_flaky_transition(previous_outcome, outcome))
            if event.campaign_id is not None:
                campaign_state = connection.execute(
                    "SELECT last_outcome FROM statistics_campaign_test_states WHERE campaign_id = ? AND test_id = ?",
                    (event.campaign_id.value, event.test_id.value),
                ).fetchone()
                campaign_previous = str(campaign_state["last_outcome"]) if campaign_state is not None else None
                campaign_flaky_increment = int(self._is_flaky_transition(campaign_previous, outcome))
                connection.execute(
                    """
                    INSERT INTO statistics_campaign_test_states(campaign_id, test_id, last_outcome)
                    VALUES (?, ?, ?)
                    ON CONFLICT(campaign_id, test_id) DO UPDATE SET last_outcome = excluded.last_outcome
                    """,
                    (event.campaign_id.value, event.test_id.value, outcome),
                )
        for entity_type, entity_id in _event_entities(event):
            current_outcome = outcome
            current_duration = duration_ms
            if entity_type == "test" and event.event_type not in {
                "test.completed",
                "execution.timeout",
                "infrastructure.failed",
            }:
                current_outcome = None
                current_duration = None
            elif entity_type == "mutant" and event.event_type not in {
                "mutant.completed",
                "execution.timeout",
                "infrastructure.failed",
            }:
                current_outcome = None
                current_duration = None
            self._apply_summary(
                connection,
                event=event,
                entity_type=entity_type,
                entity_id=entity_id,
                outcome=current_outcome,
                duration_ms=current_duration,
                flaky_increment=(
                    campaign_flaky_increment
                    if entity_type == "campaign"
                    else test_flaky_increment if entity_type == "test" else 0
                ),
            )
    def checkpoint(self) -> StatisticsProjectionCheckpoint:
        # Return the durable source position without inspecting event history.
        connection = self.event_store._connect()
        try:
            self._ensure_schema(connection)
            row = connection.execute(
                "SELECT last_sequence, last_event_id FROM statistics_projection_metadata WHERE projection_name = 'default'"
            ).fetchone()
            return StatisticsProjectionCheckpoint(sequence=int(row["last_sequence"]), event_id=str(row["last_event_id"]))
        finally:
            connection.close()
    def project(self, *, max_events: int = 500) -> StatisticsProjectionResult:
        # Apply one bounded event batch and advance summaries plus checkpoint atomically.
        batch_limit = _validate_limit(max_events, maximum=STATISTICS_PROJECTION_MAX_BATCH)
        connection = self.event_store._connect()
        try:
            self._ensure_schema(connection)
            connection.execute("BEGIN IMMEDIATE")
            checkpoint = connection.execute(
                "SELECT last_sequence, last_event_id FROM statistics_projection_metadata WHERE projection_name = 'default'"
            ).fetchone()
            last_sequence = int(checkpoint["last_sequence"])
            last_event_id = str(checkpoint["last_event_id"])
            rows = connection.execute(
                """
                SELECT sequence, event_id, event_json
                FROM statistics_events
                WHERE sequence > ? OR (sequence = ? AND event_id > ?)
                ORDER BY sequence ASC, event_id ASC
                LIMIT ?
                """,
                (last_sequence, last_sequence, last_event_id, batch_limit + 1),
            ).fetchall()
            selected = rows[:batch_limit]
            for row in selected:
                self._apply_event(connection, StatisticsEvent.from_json(str(row["event_json"])))
            if selected:
                last_sequence = int(selected[-1]["sequence"])
                last_event_id = str(selected[-1]["event_id"])
                connection.execute(
                    """
                    UPDATE statistics_projection_metadata
                    SET last_sequence = ?, last_event_id = ?
                    WHERE projection_name = 'default'
                    """,
                    (last_sequence, last_event_id),
                )
            connection.commit()
            return StatisticsProjectionResult(
                events_projected=len(selected),
                checkpoint=StatisticsProjectionCheckpoint(last_sequence, last_event_id),
                has_more=len(rows) > batch_limit,
            )
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
    def rebuild(self, *, batch_size: int = 500) -> StatisticsProjectionResult:
        # Reset only derived tables and deterministically replay immutable history in bounded batches.
        _validate_limit(batch_size, maximum=STATISTICS_PROJECTION_MAX_BATCH)
        connection = self.event_store._connect()
        try:
            self._ensure_schema(connection)
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("DELETE FROM statistics_duration_samples")
            connection.execute("DELETE FROM statistics_campaign_test_states")
            connection.execute("DELETE FROM statistics_summaries")
            connection.execute(
                "UPDATE statistics_projection_metadata SET last_sequence = 0, last_event_id = '' WHERE projection_name = 'default'"
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
        total = 0
        result = StatisticsProjectionResult(0, StatisticsProjectionCheckpoint(0, ""), False)
        while True:
            current = self.project(max_events=batch_size)
            total += current.events_projected
            result = StatisticsProjectionResult(total, current.checkpoint, current.has_more)
            if not current.has_more:
                return result
    @staticmethod
    def _summary_from_row(row: sqlite3.Row, samples: Iterable[float]) -> StatisticsSummary:
        # Materialize one summary from counters plus at most 256 duration samples.
        values = [float(value) for value in samples]
        duration_count = int(row["duration_count"])
        active_duration = float(row["active_duration_ms"] or 0.0)
        if row["active_since"] and row["last_event_timestamp"]:
            active_duration += _elapsed_ms(str(row["active_since"]), str(row["last_event_timestamp"]))
        busy_duration = float(row["busy_duration_ms"] or 0.0)
        utilization = min(1.0, busy_duration / active_duration) if active_duration > 0.0 else 0.0
        return StatisticsSummary(
            entity_type=str(row["entity_type"]),
            entity_id=str(row["entity_id"]),
            event_count=int(row["event_count"]),
            started_count=int(row["started_count"]),
            completed_count=int(row["completed_count"]),
            passed_count=int(row["passed_count"]),
            failed_count=int(row["failed_count"]),
            error_count=int(row["error_count"]),
            timeout_count=int(row["timeout_count"]),
            retry_count=int(row["retry_count"]),
            recovery_count=int(row["recovery_count"]),
            escalation_count=int(row["escalation_count"]),
            reuse_count=int(row["reuse_count"]),
            killed_count=int(row["killed_count"]),
            survived_count=int(row["survived_count"]),
            invalid_count=int(row["invalid_count"]),
            plan_decision_count=int(row["plan_decision_count"]),
            plan_execute_count=int(row["plan_execute_count"]),
            plan_reuse_count=int(row["plan_reuse_count"]),
            plan_partial_reuse_count=int(row["plan_partial_reuse_count"]),
            plan_audit_count=int(row["plan_audit_count"]),
            plan_excluded_count=int(row["plan_excluded_count"]),
            infrastructure_failure_count=int(row["infrastructure_failure_count"]),
            flaky_transition_count=int(row["flaky_transition_count"]),
            duration_count=duration_count,
            duration_total_ms=round(float(row["duration_total_ms"] or 0.0), 3),
            duration_min_ms=round(float(row["duration_min_ms"]), 3) if row["duration_min_ms"] is not None else None,
            duration_max_ms=round(float(row["duration_max_ms"]), 3) if row["duration_max_ms"] is not None else None,
            duration_avg_ms=round(float(row["duration_total_ms"] or 0.0) / duration_count, 3) if duration_count else 0.0,
            duration_median_ms=round(_percentile(values.copy(), 0.5), 3),
            duration_p95_ms=round(_percentile(values.copy(), 0.95), 3),
            duration_sample_count=len(values),
            busy_duration_ms=round(busy_duration, 3),
            active_duration_ms=round(active_duration, 3),
            utilization=round(utilization, 6),
            active=row["active_since"] is not None,
            last_outcome=str(row["last_outcome"]) if row["last_outcome"] else None,
            last_event_type=str(row["last_event_type"]),
            last_event_timestamp=str(row["last_event_timestamp"]),
        )
    @staticmethod
    def _load_samples(
        connection: sqlite3.Connection,
        *,
        entity_type: str,
        entity_ids: tuple[str, ...],
    ) -> dict[str, list[float]]:
        # Fetch bounded samples for one page in a single query instead of N+1 reads.
        if not entity_ids:
            return {}
        placeholders = ",".join("?" for _ in entity_ids)
        rows = connection.execute(
            f"SELECT entity_id, duration_ms FROM statistics_duration_samples WHERE entity_type = ? AND entity_id IN ({placeholders}) ORDER BY entity_id, sample_slot",
            (entity_type, *entity_ids),
        ).fetchall()
        samples: dict[str, list[float]] = {}
        for row in rows:
            samples.setdefault(str(row["entity_id"]), []).append(float(row["duration_ms"]))
        return samples
    def get_many(
        self,
        entity_type: ProjectionEntityType,
        entity_ids: Iterable[str],
    ) -> dict[str, StatisticsSummary]:
        # Load one bounded batch of summaries and duration samples without N+1 reads.
        if entity_type not in _PROJECTION_ENTITY_TYPES:
            raise ValueError(f"unsupported projection entity_type: {entity_type}")
        normalized = tuple(sorted({str(item).strip() for item in entity_ids if str(item).strip()}))
        if len(normalized) > STATISTICS_PROJECTION_MAX_BATCH:
            raise ValueError(f"entity_ids must contain at most {STATISTICS_PROJECTION_MAX_BATCH} values")
        if not normalized:
            return {}
        placeholders = ",".join("?" for _ in normalized)
        connection = self.event_store._connect()
        try:
            self._ensure_schema(connection)
            rows = connection.execute(
                f"SELECT * FROM statistics_summaries WHERE entity_type = ? AND entity_id IN ({placeholders}) ORDER BY entity_id",
                (entity_type, *normalized),
            ).fetchall()
            samples = self._load_samples(connection, entity_type=entity_type, entity_ids=normalized)
            return {
                str(row["entity_id"]): self._summary_from_row(
                    row,
                    samples.get(str(row["entity_id"]), ()),
                )
                for row in rows
            }
        finally:
            connection.close()
    def get(self, entity_type: ProjectionEntityType, entity_id: str) -> StatisticsSummary | None:
        # Load one entity summary and only its bounded duration sample.
        if entity_type not in _PROJECTION_ENTITY_TYPES:
            raise ValueError(f"unsupported projection entity_type: {entity_type}")
        if not isinstance(entity_id, str) or not entity_id.strip():
            raise ValueError("entity_id must be a non-empty string")
        connection = self.event_store._connect()
        try:
            self._ensure_schema(connection)
            row = connection.execute(
                "SELECT * FROM statistics_summaries WHERE entity_type = ? AND entity_id = ?",
                (entity_type, entity_id),
            ).fetchone()
            if row is None:
                return None
            samples = self._load_samples(connection, entity_type=entity_type, entity_ids=(entity_id,))
            return self._summary_from_row(row, samples.get(entity_id, ()))
        finally:
            connection.close()
    def query(
        self,
        entity_type: ProjectionEntityType,
        *,
        limit: int = 100,
        cursor: str | None = None,
    ) -> StatisticsSummaryPage:
        # Return one bounded entity page using entity-ID keyset pagination.
        if entity_type not in _PROJECTION_ENTITY_TYPES:
            raise ValueError(f"unsupported projection entity_type: {entity_type}")
        page_limit = _validate_limit(limit, maximum=STATISTICS_PROJECTION_MAX_LIMIT)
        position = StatisticsProjectionCursor.decode(cursor) if cursor is not None else None
        if position is not None and position.entity_type != entity_type:
            raise ValueError("projection cursor entity_type does not match query")
        connection = self.event_store._connect()
        try:
            self._ensure_schema(connection)
            if position is None:
                rows = connection.execute(
                    "SELECT * FROM statistics_summaries WHERE entity_type = ? ORDER BY entity_id ASC LIMIT ?",
                    (entity_type, page_limit + 1),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM statistics_summaries WHERE entity_type = ? AND entity_id > ? ORDER BY entity_id ASC LIMIT ?",
                    (entity_type, position.entity_id, page_limit + 1),
                ).fetchall()
            selected = rows[:page_limit]
            entity_ids = tuple(str(row["entity_id"]) for row in selected)
            samples = self._load_samples(connection, entity_type=entity_type, entity_ids=entity_ids)
            items = tuple(self._summary_from_row(row, samples.get(str(row["entity_id"]), ())) for row in selected)
            has_more = len(rows) > page_limit
            next_cursor = (
                StatisticsProjectionCursor(entity_type, items[-1].entity_id).encode()
                if has_more and items
                else None
            )
            return StatisticsSummaryPage(items=items, next_cursor=next_cursor)
        finally:
            connection.close()
    def export(
        self,
        entity_type: ProjectionEntityType,
        *,
        limit: int = 100,
        cursor: str | None = None,
    ) -> tuple[tuple[dict[str, Any], ...], str | None]:
        # Export exactly one bounded query page as JSON-compatible dictionaries.
        page = self.query(entity_type, limit=limit, cursor=cursor)
        return tuple(item.to_dict() for item in page.items), page.next_cursor
