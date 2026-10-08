"""SQLite append-only ingestion projection for canonical statistics events."""
from __future__ import annotations
import base64
import binascii
import hashlib
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from theseus_contracts.events import StatisticsEvent
from theseus_contracts.errors import ContractError
from theseus_contracts.serialization import SerializationError, dumps, loads_object
from theseus_performance.sqlite_metrics import SQLiteMetrics
STATISTICS_STORE_SCHEMA_VERSION = 1
STATISTICS_QUERY_MAX_LIMIT = 500
STATISTICS_INGEST_MAX_RECORDS = 10_000
STATISTICS_EVENT_MAX_BYTES = 1_048_576
_CHECKPOINT_TAIL_BYTES = 4096
class StatisticsStoreError(RuntimeError):
    """Base failure for the canonical statistics projection."""
class StatisticsEventConflictError(StatisticsStoreError):
    """Raised when one deterministic event ID is reused for different event content."""
class StatisticsIngestionError(StatisticsStoreError):
    """Raised when an append-only source cannot be consumed safely."""
@dataclass(frozen=True, slots=True)
class StatisticsEventCursor:
    """Opaque keyset position ordered by store sequence and deterministic event ID."""
    sequence: int
    event_id: str
    def __post_init__(self) -> None:
        # Reject malformed cursors before they can affect a bounded keyset query.
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int) or self.sequence < 0:
            raise ValueError("cursor sequence must be a non-negative integer")
        if not isinstance(self.event_id, str) or not self.event_id.strip():
            raise ValueError("cursor event_id must be a non-empty string")
    def encode(self) -> str:
        # Encode the cursor deterministically without exposing SQL implementation details.
        payload = dumps({"event_id": self.event_id, "sequence": self.sequence}).encode("utf-8")
        return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    @classmethod
    def decode(cls, value: str) -> "StatisticsEventCursor":
        # Decode and validate one opaque keyset cursor.
        if not isinstance(value, str) or not value.strip():
            raise ValueError("cursor must be a non-empty string")
        padding = "=" * (-len(value) % 4)
        try:
            payload = base64.urlsafe_b64decode((value + padding).encode("ascii"))
            decoded = loads_object(payload)
        except (binascii.Error, UnicodeError, ValueError, SerializationError) as exc:
            raise ValueError("invalid statistics cursor") from exc
        sequence = decoded.get("sequence")
        event_id = decoded.get("event_id")
        return cls(sequence=sequence, event_id=event_id)
@dataclass(frozen=True, slots=True)
class StoredStatisticsEvent:
    """One canonical event paired with its immutable projection sequence."""
    sequence: int
    event: StatisticsEvent
    @property
    def cursor(self) -> StatisticsEventCursor:
        # Return the stable keyset position immediately after this stored event.
        return StatisticsEventCursor(sequence=self.sequence, event_id=self.event.event_id.value)
@dataclass(frozen=True, slots=True)
class StatisticsEventPage:
    """Bounded keyset page returned by the statistics projection."""
    items: tuple[StoredStatisticsEvent, ...]
    next_cursor: str | None
@dataclass(frozen=True, slots=True)
class StatisticsIngestionResult:
    """Bounded result of one incremental append-only source ingestion call."""
    source_id: str
    records_read: int
    events_seen: int
    events_appended: int
    replays: int
    source_offset: int
    has_more: bool
class StatisticsEventStore:
    """Persistent append-only event projection with atomic replay and conflict checks."""
    def __init__(
        self,
        path: Path,
        *,
        metrics: SQLiteMetrics | None = None,
    ) -> None:
        # Initialize the schema eagerly so configuration errors fail before producers run.
        self.path = Path(path)
        self._metrics = metrics
        connection = self._connect()
        connection.close()
    def _connect(self) -> sqlite3.Connection:
        # Open one WAL connection and verify the exact store schema version.
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        if self._metrics is not None:
            connection.set_trace_callback(self._metrics.trace)
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS statistics_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS statistics_events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                schema_version INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                producer_id TEXT NOT NULL,
                producer_sequence INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                campaign_id TEXT,
                plan_id TEXT,
                shard_id TEXT,
                execution_id TEXT,
                worker_id TEXT,
                test_id TEXT,
                mutant_id TEXT,
                process_id TEXT,
                event_sha256 TEXT NOT NULL,
                event_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_statistics_events_type_sequence
            ON statistics_events(event_type, sequence, event_id);
            CREATE INDEX IF NOT EXISTS ix_statistics_events_campaign_sequence
            ON statistics_events(campaign_id, sequence, event_id);
            CREATE INDEX IF NOT EXISTS ix_statistics_events_test_sequence
            ON statistics_events(test_id, sequence, event_id);
            CREATE INDEX IF NOT EXISTS ix_statistics_events_execution_sequence
            ON statistics_events(execution_id, sequence, event_id);
            CREATE TRIGGER IF NOT EXISTS trg_statistics_events_no_update
            BEFORE UPDATE ON statistics_events
            BEGIN
                SELECT RAISE(ABORT, 'statistics_events is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_statistics_events_no_delete
            BEFORE DELETE ON statistics_events
            BEGIN
                SELECT RAISE(ABORT, 'statistics_events is append-only');
            END;
            CREATE TABLE IF NOT EXISTS statistics_ingestion_sources (
                source_id TEXT PRIMARY KEY,
                source_path TEXT NOT NULL,
                byte_offset INTEGER NOT NULL,
                tail_start INTEGER NOT NULL,
                tail_sha256 TEXT NOT NULL
            );
            """
        )
        row = connection.execute(
            "SELECT value FROM statistics_metadata WHERE key = 'schema_version'"
        ).fetchone()
        if row is None:
            connection.execute(
                "INSERT INTO statistics_metadata(key, value) VALUES ('schema_version', ?)",
                (str(STATISTICS_STORE_SCHEMA_VERSION),),
            )
        elif str(row["value"]) != str(STATISTICS_STORE_SCHEMA_VERSION):
            connection.close()
            raise StatisticsStoreError(f"unsupported statistics store schema_version: {row['value']}")
        return connection
    @staticmethod
    def _canonical_event(event: StatisticsEvent) -> tuple[str, str]:
        # Serialize and hash the complete event once for exact replay and conflict comparison.
        if not isinstance(event, StatisticsEvent):
            raise TypeError("statistics store accepts StatisticsEvent values only")
        canonical = event.to_json()
        return canonical, hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    def _append_in_transaction(self, connection: sqlite3.Connection, event: StatisticsEvent) -> bool:
        # Append one event or distinguish exact replay from an event-ID conflict atomically.
        canonical, event_sha256 = self._canonical_event(event)
        existing = connection.execute(
            "SELECT event_sha256, event_json FROM statistics_events WHERE event_id = ?",
            (event.event_id.value,),
        ).fetchone()
        if existing is not None:
            if str(existing["event_sha256"]) == event_sha256 and str(existing["event_json"]) == canonical:
                return False
            raise StatisticsEventConflictError(
                f"statistics event_id conflict: {event.event_id.value}"
            )
        connection.execute(
            """
            INSERT INTO statistics_events(
                event_id, schema_version, event_type, producer_id, producer_sequence,
                timestamp, campaign_id, plan_id, shard_id, execution_id, worker_id,
                test_id, mutant_id, process_id, event_sha256, event_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id.value,
                event.schema_version,
                event.event_type,
                event.producer_id.value,
                event.producer_sequence,
                event.timestamp,
                event.campaign_id.value if event.campaign_id else None,
                event.plan_id.value if event.plan_id else None,
                event.shard_id.value if event.shard_id else None,
                event.execution_id.value if event.execution_id else None,
                event.worker_id.value if event.worker_id else None,
                event.test_id.value if event.test_id else None,
                event.mutant_id.value if event.mutant_id else None,
                event.process_id.value if event.process_id else None,
                event_sha256,
                canonical,
            ),
        )
        return True
    def append(self, event: StatisticsEvent) -> bool:
        # Commit one immutable event and return false only for an exact replay.
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            appended = self._append_in_transaction(connection, event)
            connection.commit()
            return appended
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
    def append_many(self, events: Iterable[StatisticsEvent]) -> tuple[int, int]:
        # Commit one caller-supplied event batch atomically without materializing history.
        connection = self._connect()
        appended = 0
        replays = 0
        try:
            connection.execute("BEGIN IMMEDIATE")
            for event in events:
                if self._append_in_transaction(connection, event):
                    appended += 1
                else:
                    replays += 1
            connection.commit()
            return appended, replays
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
    @staticmethod
    def _validate_limit(limit: int, *, maximum: int) -> int:
        # Reject booleans, empty pages and requests above the explicit bounded maximum.
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise ValueError("limit must be an integer")
        if limit < 1 or limit > maximum:
            raise ValueError(f"limit must be between 1 and {maximum}")
        return limit
    @staticmethod
    def _tail_hash(handle, offset: int) -> tuple[int, str]:
        # Hash only a bounded window ending at the checkpoint so prior source bytes are not rescanned.
        tail_start = max(0, offset - _CHECKPOINT_TAIL_BYTES)
        handle.seek(tail_start)
        payload = handle.read(offset - tail_start)
        return tail_start, hashlib.sha256(payload).hexdigest()
    def ingest_jsonl(
        self,
        path: Path,
        *,
        source_id: str,
        max_records: int = 1000,
    ) -> StatisticsIngestionResult:
        # Resume one append-only JSONL source from its durable byte checkpoint in a bounded transaction.
        if not isinstance(source_id, str) or not source_id.strip():
            raise ValueError("source_id must be a non-empty string")
        record_limit = self._validate_limit(max_records, maximum=STATISTICS_INGEST_MAX_RECORDS)
        source_path = Path(path).resolve()
        try:
            source_size = source_path.stat().st_size
        except OSError as exc:
            raise StatisticsIngestionError(f"cannot stat statistics source: {source_path}") from exc
        connection = self._connect()
        records_read = 0
        events_seen = 0
        events_appended = 0
        replays = 0
        has_more = False
        try:
            connection.execute("BEGIN IMMEDIATE")
            checkpoint = connection.execute(
                "SELECT byte_offset, tail_start, tail_sha256 FROM statistics_ingestion_sources WHERE source_id = ?",
                (source_id,),
            ).fetchone()
            offset = int(checkpoint["byte_offset"]) if checkpoint is not None else 0
            if source_size < offset:
                raise StatisticsIngestionError(
                    f"append-only source was truncated: {source_id}; size={source_size}; offset={offset}"
                )
            with source_path.open("rb") as handle:
                if checkpoint is not None:
                    expected_start = int(checkpoint["tail_start"])
                    handle.seek(expected_start)
                    expected_size = offset - expected_start
                    actual_sha256 = hashlib.sha256(handle.read(expected_size)).hexdigest()
                    if actual_sha256 != str(checkpoint["tail_sha256"]):
                        raise StatisticsIngestionError(f"append-only source checkpoint conflict: {source_id}")
                handle.seek(offset)
                while records_read < record_limit:
                    record_start = handle.tell()
                    line = handle.readline(STATISTICS_EVENT_MAX_BYTES + 1)
                    if not line:
                        break
                    if len(line) > STATISTICS_EVENT_MAX_BYTES:
                        raise StatisticsIngestionError(
                            f"statistics event exceeds {STATISTICS_EVENT_MAX_BYTES} bytes: {source_id}@{record_start}"
                        )
                    if not line.endswith(b"\n"):
                        handle.seek(record_start)
                        break
                    records_read += 1
                    offset = handle.tell()
                    if not line.strip():
                        continue
                    events_seen += 1
                    try:
                        event = StatisticsEvent.from_json(line)
                    except (ContractError, ValueError, SerializationError) as exc:
                        raise StatisticsIngestionError(
                            f"invalid statistics event: {source_id}@{record_start}: {exc}"
                        ) from exc
                    if self._append_in_transaction(connection, event):
                        events_appended += 1
                    else:
                        replays += 1
                has_more = bool(handle.read(1))
                tail_start, tail_sha256 = self._tail_hash(handle, offset)
            connection.execute(
                """
                INSERT INTO statistics_ingestion_sources(
                    source_id, source_path, byte_offset, tail_start, tail_sha256
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(source_id) DO UPDATE SET
                    source_path = excluded.source_path,
                    byte_offset = excluded.byte_offset,
                    tail_start = excluded.tail_start,
                    tail_sha256 = excluded.tail_sha256
                """,
                (source_id, str(source_path), offset, tail_start, tail_sha256),
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
        return StatisticsIngestionResult(
            source_id=source_id,
            records_read=records_read,
            events_seen=events_seen,
            events_appended=events_appended,
            replays=replays,
            source_offset=offset,
            has_more=has_more,
        )
    def query(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
        event_type: str | None = None,
        producer_id: str | None = None,
        campaign_id: str | None = None,
        execution_id: str | None = None,
        worker_id: str | None = None,
        test_id: str | None = None,
    ) -> StatisticsEventPage:
        # Return one deterministic bounded page using a sequence-plus-event-ID keyset cursor.
        page_limit = self._validate_limit(limit, maximum=STATISTICS_QUERY_MAX_LIMIT)
        position = StatisticsEventCursor.decode(cursor) if cursor is not None else None
        filters: list[str] = []
        parameters: list[object] = []
        if position is not None:
            filters.append("(sequence > ? OR (sequence = ? AND event_id > ?))")
            parameters.extend((position.sequence, position.sequence, position.event_id))
        for column, value in (
            ("event_type", event_type),
            ("producer_id", producer_id),
            ("campaign_id", campaign_id),
            ("execution_id", execution_id),
            ("worker_id", worker_id),
            ("test_id", test_id),
        ):
            if value is not None:
                if not isinstance(value, str) or not value.strip():
                    raise ValueError(f"{column} filter must be a non-empty string")
                filters.append(f"{column} = ?")
                parameters.append(value)
        where = f"WHERE {' AND '.join(filters)}" if filters else ""
        parameters.append(page_limit + 1)
        connection = self._connect()
        try:
            rows = connection.execute(
                f"SELECT sequence, event_json FROM statistics_events {where} "
                "ORDER BY sequence ASC, event_id ASC LIMIT ?",
                parameters,
            ).fetchall()
        finally:
            connection.close()
        has_more = len(rows) > page_limit
        selected = rows[:page_limit]
        items = tuple(
            StoredStatisticsEvent(
                sequence=int(row["sequence"]),
                event=StatisticsEvent.from_json(str(row["event_json"])),
            )
            for row in selected
        )
        next_cursor = items[-1].cursor.encode() if has_more and items else None
        return StatisticsEventPage(items=items, next_cursor=next_cursor)
    def count(self) -> int:
        # Return the projected event count without reading or deserializing event history.
        connection = self._connect()
        try:
            row = connection.execute("SELECT COUNT(*) AS value FROM statistics_events").fetchone()
            return int(row["value"])
        finally:
            connection.close()
