"""Bounded event streaming and progress reads for the transport-neutral local API."""
from __future__ import annotations
import base64
import binascii
import json
import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from .contracts import (
    ApiError,
    ApiFailed,
    ApiOutcome,
    ApiPage,
    ApiRejected,
    ApiSuccess,
    EventDto,
    EventStreamPageDto,
    JsonValue,
    ProgressDto,
    ShardProgressDto,
    StatisticsCheckpointDto,
)
LOCAL_EVENT_STREAM_SCHEMA_VERSION = 1
LOCAL_EVENT_STREAM_MAX_LIMIT = 200
LOCAL_PROGRESS_MAX_SHARDS = 200
_RECOVERY_MAX_BYTES = 4 * 1024 * 1024
_QUARANTINE_COUNT_LIMIT = 1000
_PRIVATE_KEY_PARTS = ("path", "root", "workspace", "spool", "database", "directory", "secret", "token", "environment")
_EVENT_DETAIL_KEYS = frozenset(
    (
        "status",
        "outcome",
        "duration_ms",
        "attempt",
        "retry",
        "completed_mutants",
        "total_mutants",
        "completed_count",
        "heartbeat_sequence",
        "active",
        "utilization",
        "reason_code",
    )
)
class _StreamRejected(RuntimeError):
    """Internal expected stream rejection."""
    def __init__(self, code: str, message: str) -> None:
        # Retain only stable rejection data before conversion to an API outcome.
        super().__init__(message)
        self.code = code
        self.message = message
class _StreamFailed(RuntimeError):
    """Internal technical stream failure."""
    def __init__(self, code: str, message: str, *, retriable: bool = False) -> None:
        # Retain only stable failure data and omit the original exception text.
        super().__init__(message)
        self.code = code
        self.message = message
        self.retriable = retriable
@dataclass(frozen=True, slots=True)
class _StreamCursor:
    """Composite source positions bound to one filter set."""
    campaign_id: str | None
    event_type: str | None
    outbox_created_at: str | None = None
    outbox_effect_id: str | None = None
    statistics_cursor: str | None = None
    def encode(self) -> str:
        # Encode all source positions and filters into one opaque deterministic cursor.
        payload = json.dumps(
            {
                "schema_version": LOCAL_EVENT_STREAM_SCHEMA_VERSION,
                "campaign_id": self.campaign_id,
                "event_type": self.event_type,
                "outbox_created_at": self.outbox_created_at,
                "outbox_effect_id": self.outbox_effect_id,
                "statistics_cursor": self.statistics_cursor,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    @classmethod
    def decode(
        cls,
        value: str | None,
        *,
        campaign_id: str | None,
        event_type: str | None,
    ) -> "_StreamCursor":
        # Decode one cursor and reject attempts to reuse it with other filters.
        if value is None:
            return cls(campaign_id=campaign_id, event_type=event_type)
        if not isinstance(value, str) or not value.strip():
            raise _StreamRejected("invalid_cursor", "cursor must be a non-empty string")
        try:
            padding = "=" * (-len(value) % 4)
            raw = base64.urlsafe_b64decode((value + padding).encode("ascii"))
            payload = json.loads(raw.decode("utf-8"))
        except (binascii.Error, UnicodeError, ValueError, json.JSONDecodeError) as exc:
            raise _StreamRejected("invalid_cursor", "cursor is not a valid event stream cursor") from exc
        if not isinstance(payload, Mapping) or payload.get("schema_version") != LOCAL_EVENT_STREAM_SCHEMA_VERSION:
            raise _StreamRejected("invalid_cursor", "cursor schema is not supported")
        if payload.get("campaign_id") != campaign_id or payload.get("event_type") != event_type:
            raise _StreamRejected("invalid_cursor", "cursor does not match the requested event filters")
        fields = ("outbox_created_at", "outbox_effect_id", "statistics_cursor")
        if any(payload.get(field) is not None and not isinstance(payload.get(field), str) for field in fields):
            raise _StreamRejected("invalid_cursor", "cursor source position is invalid")
        if (payload.get("outbox_created_at") is None) != (payload.get("outbox_effect_id") is None):
            raise _StreamRejected("invalid_cursor", "cursor outbox position is incomplete")
        return cls(
            campaign_id=campaign_id,
            event_type=event_type,
            outbox_created_at=payload.get("outbox_created_at"),
            outbox_effect_id=payload.get("outbox_effect_id"),
            statistics_cursor=payload.get("statistics_cursor"),
        )
def _validate_limit(value: int, *, maximum: int) -> int:
    # Reject booleans and every request outside the explicit bounded range.
    if isinstance(value, bool) or not isinstance(value, int) or value < 1 or value > maximum:
        raise _StreamRejected("invalid_limit", f"limit must be between 1 and {maximum}")
    return value
def _optional_identifier(value: object, field_name: str) -> str | None:
    # Normalize one optional public filter without accepting empty identifiers.
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise _StreamRejected("invalid_request", f"{field_name} must be a non-empty string")
    return value.strip()
def _safe_details(value: object) -> dict[str, JsonValue]:
    # Retain only allowlisted scalar event details and remove path, secret and environment fields.
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, JsonValue] = {}
    for key, item in value.items():
        name = str(key)
        lowered = name.lower()
        if name not in _EVENT_DETAIL_KEYS or any(part in lowered for part in _PRIVATE_KEY_PARTS):
            continue
        if item is None or isinstance(item, (str, int, float, bool)):
            result[name] = item
    return result
def _nested_mapping(value: object) -> Mapping[str, Any]:
    # Return one mapping or an empty object without coercing arbitrary runtime values.
    return value if isinstance(value, Mapping) else {}
def _id_from_mapping(value: Mapping[str, Any], key: str) -> str | None:
    # Read one non-empty identifier from a sanitized persisted mapping.
    item = value.get(key)
    return str(item) if isinstance(item, (str, int)) and str(item) else None
def _outbox_category(event_type: str, effect_id: str) -> str:
    # Classify one durable mutation effect into a stable operator stream category.
    lowered = event_type.lower()
    effect_lowered = effect_id.lower()
    if effect_lowered.startswith("recovery.") or any(token in lowered for token in ("orphan", "recover", "finalization")):
        return "recovery"
    if any(token in lowered for token in ("execution", "execute")):
        return "execution"
    if ".worker." in lowered or lowered.startswith("worker."):
        return "worker"
    if any(token in lowered for token in ("shard", "lease")):
        return "shard"
    return "progress"
def _outbox_event(row: sqlite3.Row) -> EventDto:
    # Convert one mutation outbox row without exposing its complete effect receipt.
    try:
        receipt = json.loads(str(row["payload"]))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise _StreamFailed("store_corrupted", "mutation outbox contains invalid JSON") from exc
    receipt_value = _nested_mapping(receipt)
    payload = _nested_mapping(receipt_value.get("payload"))
    subject: Mapping[str, Any] = {}
    for name in ("campaign", "worker", "shard", "execution", "lease", "finalization_intent"):
        candidate = payload.get(name)
        if isinstance(candidate, Mapping):
            subject = candidate
            break
    details = _safe_details(subject)
    details.update({key: value for key, value in _safe_details(payload).items() if key not in details})
    event_type = str(row["event_type"])
    effect_id = str(row["effect_id"])
    return EventDto(
        event_id=effect_id,
        source="mutation_outbox",
        category=_outbox_category(event_type, effect_id),
        event_type=event_type,
        timestamp=str(row["created_at"]),
        campaign_id=str(row["campaign_id"]) if row["campaign_id"] is not None else None,
        worker_id=_id_from_mapping(subject, "worker_id") or _id_from_mapping(_nested_mapping(subject.get("identity")), "worker_id"),
        shard_id=_id_from_mapping(subject, "shard_id"),
        execution_id=_id_from_mapping(subject, "execution_id"),
        details=details,
    )
def _statistics_event(stored: object) -> EventDto:
    # Convert one canonical statistics record through its public typed event fields.
    event = getattr(stored, "event", None)
    if event is None:
        raise _StreamFailed("statistics_store_corrupted", "statistics event page contains an invalid record")
    def identifier(value: object) -> object:
        # Normalize typed identifiers without coupling the stream DTO to domain classes.
        return getattr(value, "value", None) if value is not None else None
    return EventDto(
        event_id=str(identifier(getattr(event, "event_id", None)) or ""),
        source="statistics",
        category="statistics",
        event_type=str(getattr(event, "event_type", "")),
        timestamp=str(getattr(event, "timestamp", "")),
        campaign_id=identifier(getattr(event, "campaign_id", None)),
        worker_id=identifier(getattr(event, "worker_id", None)),
        shard_id=identifier(getattr(event, "shard_id", None)),
        execution_id=identifier(getattr(event, "execution_id", None)),
        details=_safe_details(getattr(event, "payload", {})),
    )
class LocalEventStream:
    """One-shot bounded event and progress reader without polling or mutation."""
    def __init__(
        self,
        campaign_database: str | Path,
        *,
        statistics_database: str | Path | None = None,
        knowledge_database: str | Path | None = None,
        statistics_event_store_factory: Callable[[Path], object] | None = None,
        statistics_projection_store_factory: Callable[[object], object] | None = None,
        knowledge_store_factory: Callable[[Path], object] | None = None,
        recovery_path_resolver: Callable[[Path], Path] | None = None,
    ) -> None:
        # Resolve durable roots once while keeping every public DTO path-free.
        self._campaign_database = Path(campaign_database).expanduser().resolve()
        self._statistics_database = Path(statistics_database).expanduser().resolve() if statistics_database else None
        self._knowledge_database = Path(knowledge_database).expanduser().resolve() if knowledge_database else None
        self._statistics_event_store_factory = statistics_event_store_factory
        self._statistics_projection_store_factory = statistics_projection_store_factory
        self._knowledge_store_factory = knowledge_store_factory
        self._recovery_path_resolver = recovery_path_resolver
    def _execute(self, operation: Callable[[], Any]) -> ApiOutcome[Any]:
        # Convert expected and technical stream failures into stable API outcomes.
        try:
            return ApiSuccess(operation())
        except _StreamRejected as exc:
            return ApiRejected(ApiError(exc.code, exc.message))
        except _StreamFailed as exc:
            return ApiFailed(ApiError(exc.code, exc.message, retriable=exc.retriable))
        except sqlite3.DatabaseError:
            return ApiFailed(ApiError("store_read_failed", "campaign database read failed", retriable=True))
        except (OSError, UnicodeError):
            return ApiFailed(ApiError("state_read_failed", "durable local state could not be read", retriable=True))
        except Exception:
            return ApiFailed(ApiError("local_stream_failed", "local event stream read failed"))
    def _connect_campaign(self) -> sqlite3.Connection:
        # Open the existing mutation database in read-only query mode.
        if not self._campaign_database.is_file():
            raise _StreamFailed("campaign_store_unavailable", "campaign database does not exist")
        connection = sqlite3.connect(self._campaign_database, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=5000")
        return connection
    def _statistics_path(self) -> Path:
        # Resolve the per-campaign statistics database without returning it to callers.
        return self._statistics_database or self._campaign_database.parent / "statistics.sqlite3"
    def _open_statistics_event_store(self, path: Path) -> object:
        # Open canonical statistics through the existing public store boundary.
        if self._statistics_event_store_factory is not None:
            return self._statistics_event_store_factory(path)
        from theseus_statistics import StatisticsEventStore
        return StatisticsEventStore(path)
    def _open_statistics_projection_store(self, event_store: object) -> object:
        # Open the existing projection boundary or its injected test adapter.
        if self._statistics_projection_store_factory is not None:
            return self._statistics_projection_store_factory(event_store)
        from theseus_statistics import StatisticsProjectionStore
        return StatisticsProjectionStore(event_store)
    def _resolve_knowledge_path(self, project_id: str) -> Path:
        # Resolve current and legacy project knowledge locations internally.
        if self._knowledge_database is not None:
            return self._knowledge_database
        state_root = self._campaign_database.parent.parent.parent
        current = state_root / "knowledge" / f"{project_id}.sqlite3"
        legacy = self._campaign_database.parent / "knowledge.sqlite3"
        return current if current.is_file() or not legacy.is_file() else legacy
    def _open_knowledge_store(self, path: Path) -> object:
        # Open knowledge through its public store boundary or an injected adapter.
        if self._knowledge_store_factory is not None:
            return self._knowledge_store_factory(path)
        from theseus_knowledge import KnowledgePlaneStore
        return KnowledgePlaneStore(path)
    def _resolve_recovery_path(self) -> Path:
        # Resolve the latest recovery report internally without exposing its path.
        if self._recovery_path_resolver is not None:
            return self._recovery_path_resolver(self._campaign_database)
        from theseus_local.startup_recovery import recovery_report_path
        return recovery_report_path(self._campaign_database)
    def stream(
        self,
        *,
        limit: int,
        cursor: str | None = None,
        campaign_id: str | None = None,
        event_type: str | None = None,
    ) -> ApiOutcome[EventStreamPageDto]:
        # Read one merged outbox/statistics page and return immediately without polling.
        def operation() -> EventStreamPageDto:
            # Fetch a bounded page from each append-only source and advance only consumed positions.
            page_limit = _validate_limit(limit, maximum=LOCAL_EVENT_STREAM_MAX_LIMIT)
            campaign_filter = _optional_identifier(campaign_id, "campaign_id")
            type_filter = _optional_identifier(event_type, "event_type")
            position = _StreamCursor.decode(cursor, campaign_id=campaign_filter, event_type=type_filter)
            connection = self._connect_campaign()
            try:
                clauses: list[str] = []
                parameters: list[object] = []
                if position.outbox_created_at is not None:
                    clauses.append("(created_at > ? OR (created_at = ? AND effect_id > ?))")
                    parameters.extend((position.outbox_created_at, position.outbox_created_at, position.outbox_effect_id))
                if campaign_filter is not None:
                    clauses.append("campaign_id = ?")
                    parameters.append(campaign_filter)
                if type_filter is not None:
                    clauses.append("event_type = ?")
                    parameters.append(type_filter)
                where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
                parameters.append(page_limit + 1)
                outbox_rows = connection.execute(
                    f"SELECT effect_id, event_type, campaign_id, payload, created_at FROM mutation_outbox {where} "
                    "ORDER BY created_at ASC, effect_id ASC LIMIT ?",
                    parameters,
                ).fetchall()
            finally:
                connection.close()
            outbox_candidates = tuple((_outbox_event(row), row) for row in outbox_rows)
            statistics_candidates: tuple[tuple[EventDto, object], ...] = ()
            statistics_page = None
            statistics_path = self._statistics_path()
            if statistics_path.is_file() or self._statistics_event_store_factory is not None:
                store = self._open_statistics_event_store(statistics_path)
                try:
                    try:
                        statistics_page = store.query(
                            limit=page_limit + 1,
                            cursor=position.statistics_cursor,
                            event_type=type_filter,
                            campaign_id=campaign_filter,
                        )
                    except ValueError as exc:
                        raise _StreamRejected("invalid_cursor", "statistics cursor is invalid") from exc
                    statistics_candidates = tuple(
                        (_statistics_event(stored), stored)
                        for stored in getattr(statistics_page, "items", ())
                    )
                finally:
                    close = getattr(store, "close", None)
                    if callable(close):
                        close()
            sources = (outbox_candidates, statistics_candidates)
            indexes = [0, 0]
            selected: list[tuple[int, EventDto, object]] = []
            while len(selected) < page_limit:
                heads: list[tuple[str, str, str, int, EventDto, object]] = []
                for source_index, values in enumerate(sources):
                    current_index = indexes[source_index]
                    if current_index >= len(values):
                        continue
                    event, raw = values[current_index]
                    heads.append(
                        (
                            event.timestamp,
                            str(source_index),
                            event.event_id,
                            source_index,
                            event,
                            raw,
                        )
                    )
                if not heads:
                    break
                _, _, _, source_index, event, raw = min(heads, key=lambda item: item[:3])
                selected.append((source_index, event, raw))
                indexes[source_index] += 1
            outbox_created_at = position.outbox_created_at
            outbox_effect_id = position.outbox_effect_id
            statistics_cursor = position.statistics_cursor
            items: list[EventDto] = []
            for source_index, event, raw in selected:
                items.append(event)
                if source_index == 0:
                    outbox_created_at = str(raw["created_at"])
                    outbox_effect_id = str(raw["effect_id"])
                else:
                    stored_cursor = getattr(raw, "cursor", None)
                    encode = getattr(stored_cursor, "encode", None)
                    if not callable(encode):
                        raise _StreamFailed("statistics_store_corrupted", "statistics event cursor is unavailable")
                    statistics_cursor = str(encode())
            next_position = _StreamCursor(
                campaign_id=campaign_filter,
                event_type=type_filter,
                outbox_created_at=outbox_created_at,
                outbox_effect_id=outbox_effect_id,
                statistics_cursor=statistics_cursor,
            )
            has_more = any(index < len(values) for index, values in zip(indexes, sources))
            return EventStreamPageDto(
                items=tuple(items),
                limit=page_limit,
                next_cursor=next_position.encode(),
                has_more=has_more,
            )
        return self._execute(operation)
    def stream_events(
        self,
        *,
        limit: int,
        cursor: str | None = None,
        campaign_id: str | None = None,
        event_type: str | None = None,
    ) -> ApiOutcome[EventStreamPageDto]:
        # Expose the roadmap stream name while preserving the one-shot bounded implementation.
        return self.stream(
            limit=limit,
            cursor=cursor,
            campaign_id=campaign_id,
            event_type=event_type,
        )
    def get_progress(
        self,
        campaign_id: str,
        *,
        shard_limit: int,
        shard_cursor: str | None = None,
    ) -> ApiOutcome[ProgressDto]:
        # Read one bounded progress snapshot from campaign, knowledge, recovery and statistics boundaries.
        def operation() -> ProgressDto:
            # Use fixed aggregate queries and a keyset shard page without reading execution history.
            current_campaign = _optional_identifier(campaign_id, "campaign_id")
            if current_campaign is None:
                raise _StreamRejected("invalid_request", "campaign_id must be a non-empty string")
            page_limit = _validate_limit(shard_limit, maximum=LOCAL_PROGRESS_MAX_SHARDS)
            shard_position = _StreamCursor.decode(
                shard_cursor,
                campaign_id=current_campaign,
                event_type="progress.shards",
            ) if shard_cursor is not None else None
            shard_key = shard_position.outbox_effect_id if shard_position is not None else None
            connection = self._connect_campaign()
            try:
                campaign_row = connection.execute(
                    "SELECT payload, revision_number FROM mutation_campaigns WHERE campaign_id = ?",
                    (current_campaign,),
                ).fetchone()
                if campaign_row is None:
                    raise _StreamRejected("not_found", "campaign does not exist")
                try:
                    campaign_payload = json.loads(str(campaign_row["payload"]))
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise _StreamFailed("store_corrupted", "campaign projection contains invalid JSON") from exc
                if not isinstance(campaign_payload, Mapping):
                    raise _StreamFailed("store_corrupted", "campaign projection must be an object")
                clauses = ["json_extract(payload, '$.campaign_id') = ?"]
                parameters: list[object] = [current_campaign]
                if shard_key is not None:
                    clauses.append("shard_id > ?")
                    parameters.append(shard_key)
                parameters.append(page_limit + 1)
                shard_rows = connection.execute(
                    f"SELECT shard_id, payload FROM mutation_shards WHERE {' AND '.join(clauses)} "
                    "ORDER BY shard_id ASC LIMIT ?",
                    parameters,
                ).fetchall()
                active_workers = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM mutation_workers WHERE json_extract(payload, '$.campaign_id') = ? "
                        "AND json_extract(payload, '$.status') NOT IN ('stopped', 'failed', 'orphaned')",
                        (current_campaign,),
                    ).fetchone()[0]
                )
                pending_outbox = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM mutation_outbox WHERE campaign_id = ? AND delivered_at IS NULL",
                        (current_campaign,),
                    ).fetchone()[0]
                )
            finally:
                connection.close()
            shard_items: list[ShardProgressDto] = []
            for row in shard_rows[:page_limit]:
                try:
                    payload = json.loads(str(row["payload"]))
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise _StreamFailed("store_corrupted", "shard projection contains invalid JSON") from exc
                if not isinstance(payload, Mapping):
                    raise _StreamFailed("store_corrupted", "shard projection must be an object")
                mutant_ids = payload.get("mutant_ids", ())
                total = len(mutant_ids) if isinstance(mutant_ids, (list, tuple)) else 0
                shard_items.append(
                    ShardProgressDto(
                        shard_id=str(row["shard_id"]),
                        status=str(payload.get("status", "unknown")),
                        attempt=int(payload.get("attempt", 0) or 0),
                        completed_mutants=int(payload.get("completed_count", 0) or 0),
                        total_mutants=total,
                    )
                )
            next_shard_cursor = None
            if len(shard_rows) > page_limit and shard_items:
                next_shard_cursor = _StreamCursor(
                    campaign_id=current_campaign,
                    event_type="progress.shards",
                    outbox_created_at="progress",
                    outbox_effect_id=shard_items[-1].shard_id,
                ).encode()
            configuration = campaign_payload.get("configuration")
            project = configuration.get("project") if isinstance(configuration, Mapping) else None
            project_id = str(campaign_payload.get("project_id") or (project.get("project_id") if isinstance(project, Mapping) else ""))
            quarantine_count = 0
            quarantine_truncated = False
            knowledge_path = self._resolve_knowledge_path(project_id)
            if knowledge_path.is_file() or self._knowledge_store_factory is not None:
                knowledge = self._open_knowledge_store(knowledge_path)
                try:
                    conflicts = tuple(knowledge.list_conflicts(limit=_QUARANTINE_COUNT_LIMIT))
                    quarantine_count = len(conflicts)
                    quarantine_truncated = quarantine_count >= _QUARANTINE_COUNT_LIMIT
                finally:
                    close = getattr(knowledge, "close", None)
                    if callable(close):
                        close()
            recovery_state = "unavailable"
            recovery_path = self._resolve_recovery_path()
            if recovery_path.is_file():
                if recovery_path.stat().st_size > _RECOVERY_MAX_BYTES:
                    recovery_state = "report_too_large"
                else:
                    try:
                        report = json.loads(recovery_path.read_text(encoding="utf-8"))
                        recovery_state = str(report.get("status", "unknown")) if isinstance(report, Mapping) else "invalid"
                    except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
                        recovery_state = "invalid"
            checkpoint = StatisticsCheckpointDto(0, "")
            statistics_path = self._statistics_path()
            if statistics_path.is_file() or self._statistics_event_store_factory is not None:
                event_store = self._open_statistics_event_store(statistics_path)
                try:
                    projection = self._open_statistics_projection_store(event_store)
                    value = projection.checkpoint()
                    checkpoint = StatisticsCheckpointDto(
                        int(getattr(value, "sequence", 0)),
                        str(getattr(value, "event_id", "")),
                    )
                finally:
                    close = getattr(event_store, "close", None)
                    if callable(close):
                        close()
            return ProgressDto(
                campaign_id=current_campaign,
                campaign_status=str(campaign_payload.get("status", "unknown")),
                campaign_revision=int(campaign_row["revision_number"]),
                completed_mutants=int(campaign_payload.get("completed_mutants", 0) or 0),
                total_mutants=int(campaign_payload.get("total_mutants", 0) or 0),
                active_workers=active_workers,
                shards=ApiPage(tuple(shard_items), page_limit, next_shard_cursor),
                pending_outbox=pending_outbox,
                quarantine_count=quarantine_count,
                quarantine_truncated=quarantine_truncated,
                recovery_state=recovery_state,
                statistics_checkpoint=checkpoint,
            )
        return self._execute(operation)
    def progress(
        self,
        campaign_id: str,
        *,
        shard_limit: int,
        shard_cursor: str | None = None,
    ) -> ApiOutcome[ProgressDto]:
        # Expose the roadmap progress name while retaining bounded shard pagination.
        return self.get_progress(
            campaign_id,
            shard_limit=shard_limit,
            shard_cursor=shard_cursor,
        )
