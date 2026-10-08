from __future__ import annotations
import inspect
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from theseus_api import ApiRejected, ApiSuccess, LocalEventStream
@dataclass(frozen=True)
class _Id:
    value: str
@dataclass(frozen=True)
class _StatisticsEvent:
    event_id: _Id
    event_type: str
    timestamp: str
    campaign_id: _Id | None
    worker_id: _Id | None
    shard_id: _Id | None
    execution_id: _Id | None
    payload: dict[str, object]
@dataclass(frozen=True)
class _StatisticsCursor:
    position: int
    def encode(self) -> str:
        # Encode the next fake statistics position as a stable test cursor.
        return str(self.position)
@dataclass(frozen=True)
class _StoredEvent:
    sequence: int
    event: _StatisticsEvent
    @property
    def cursor(self) -> _StatisticsCursor:
        # Return the source position immediately after this fake event.
        return _StatisticsCursor(self.sequence)
class _StatisticsEventStore:
    def __init__(self, path: Path) -> None:
        # Keep one deterministic append-only statistics fixture independent from the path.
        self.path = path
        self.items = (
            _StoredEvent(
                1,
                _StatisticsEvent(
                    _Id("statistics-1"),
                    "test.completed",
                    "2026-08-05T12:00:02Z",
                    _Id("campaign-1"),
                    _Id("worker-1"),
                    _Id("shard-1"),
                    _Id("execution-1"),
                    {"outcome": "passed", "duration_ms": 10, "environment_secret": "hidden"},
                ),
            ),
            _StoredEvent(
                2,
                _StatisticsEvent(
                    _Id("statistics-2"),
                    "worker.stopped",
                    "2026-08-05T12:00:04Z",
                    _Id("campaign-1"),
                    _Id("worker-1"),
                    None,
                    None,
                    {"status": "stopped"},
                ),
            ),
        )
    def query(
        self,
        *,
        limit: int,
        cursor: str | None,
        event_type: str | None,
        campaign_id: str | None,
    ) -> SimpleNamespace:
        # Return one bounded fake keyset page with the production query signature.
        position = int(cursor or 0)
        values = tuple(
            item
            for item in self.items
            if item.sequence > position
            and (event_type is None or item.event.event_type == event_type)
            and (campaign_id is None or item.event.campaign_id and item.event.campaign_id.value == campaign_id)
        )
        selected = values[:limit]
        return SimpleNamespace(items=selected, next_cursor=(selected[-1].cursor.encode() if len(values) > len(selected) else None))
    def count(self) -> int:
        # Return the stable fake event count for health checks.
        return len(self.items)
class _StatisticsProjection:
    def __init__(self, event_store: object) -> None:
        # Retain the event store only to match the production constructor contract.
        self.event_store = event_store
    def checkpoint(self) -> SimpleNamespace:
        # Return one deterministic latest statistics projection position.
        return SimpleNamespace(sequence=2, event_id="statistics-2")
class _ConflictStore:
    def __init__(self, path: Path) -> None:
        # Keep the configured path only to mirror the production store factory.
        self.path = path
    def list_conflicts(self, *, limit: int):
        # Return a bounded conflict list for progress quarantine counts.
        return tuple(SimpleNamespace(conflict_id=f"conflict-{index}") for index in range(min(2, limit)))
    def close(self) -> None:
        # Match the production close lifecycle without side effects.
        return None
def _create_database(path: Path) -> None:
    # Create minimal authoritative campaign, shard, worker and outbox projections.
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE mutation_campaigns (campaign_id TEXT PRIMARY KEY, revision_number INTEGER NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE mutation_shards (shard_id TEXT PRIMARY KEY, revision_number INTEGER NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE mutation_workers (worker_key TEXT PRIMARY KEY, revision_number INTEGER NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE mutation_outbox (
                effect_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                delivered_at TEXT
            );
            """
        )
        campaign = {
            "campaign_id": "campaign-1",
            "project_id": "project-1",
            "status": "running",
            "total_mutants": 4,
            "completed_mutants": 2,
            "configuration": {"project": {"project_id": "project-1"}},
        }
        connection.execute(
            "INSERT INTO mutation_campaigns(campaign_id, revision_number, payload) VALUES (?, ?, ?)",
            ("campaign-1", 7, json.dumps(campaign)),
        )
        for index in range(1, 4):
            shard = {
                "campaign_id": "campaign-1",
                "status": "complete" if index == 1 else "running",
                "attempt": index - 1,
                "completed_count": 1 if index == 1 else 0,
                "mutant_ids": [f"mutant-{index}"],
            }
            connection.execute(
                "INSERT INTO mutation_shards(shard_id, revision_number, payload) VALUES (?, ?, ?)",
                (f"shard-{index}", index, json.dumps(shard)),
            )
        for index, status in ((1, "running"), (2, "stopped")):
            worker = {"campaign_id": "campaign-1", "status": status}
            connection.execute(
                "INSERT INTO mutation_workers(worker_key, revision_number, payload) VALUES (?, ?, ?)",
                (f"campaign-1\x1fworker-{index}", index, json.dumps(worker)),
            )
        rows = (
            (
                "effect-1",
                "mutation.start",
                "2026-08-05T12:00:01Z",
                {"campaign": {"campaign_id": "campaign-1", "status": "running", "completed_mutants": 0, "total_mutants": 4}},
                None,
            ),
            (
                "effect-2",
                "mutation.retry_shard",
                "2026-08-05T12:00:03Z",
                {"shard": {"campaign_id": "campaign-1", "shard_id": "shard-2", "status": "created", "attempt": 1}},
                None,
            ),
            (
                "effect-3",
                "mutation.worker.stop",
                "2026-08-05T12:00:05Z",
                {"worker": {"campaign_id": "campaign-1", "identity": {"worker_id": "worker-1"}, "status": "stopped"}},
                "2026-08-05T12:00:06Z",
            ),
        )
        for effect_id, event_type, created_at, payload, delivered_at in rows:
            receipt = {"effect_id": effect_id, "effect_type": event_type, "campaign_id": "campaign-1", "payload": payload}
            connection.execute(
                "INSERT INTO mutation_outbox(effect_id, event_type, campaign_id, payload, created_at, delivered_at) VALUES (?, ?, ?, ?, ?, ?)",
                (effect_id, event_type, "campaign-1", json.dumps(receipt), created_at, delivered_at),
            )
        connection.commit()
    finally:
        connection.close()
def _stream(tmp_path: Path) -> LocalEventStream:
    # Build one stream over real outbox SQLite and injected statistics/knowledge boundaries.
    database = tmp_path / "campaign.sqlite3"
    _create_database(database)
    statistics = tmp_path / "statistics.sqlite3"
    statistics.touch()
    knowledge = tmp_path / "knowledge.sqlite3"
    knowledge.touch()
    recovery = tmp_path / "startup-recovery.report.json"
    recovery.write_text(json.dumps({"status": "complete"}), encoding="utf-8")
    return LocalEventStream(
        database,
        statistics_database=statistics,
        knowledge_database=knowledge,
        statistics_event_store_factory=_StatisticsEventStore,
        statistics_projection_store_factory=_StatisticsProjection,
        knowledge_store_factory=_ConflictStore,
        recovery_path_resolver=lambda _: recovery,
    )
def test_event_stream_merges_sources_with_stable_composite_cursor(tmp_path: Path) -> None:
    # Page merged outbox and statistics events without gaps, duplicates or polling.
    stream = _stream(tmp_path)
    first = stream.stream(limit=2, campaign_id="campaign-1")
    assert isinstance(first, ApiSuccess)
    assert [item.event_id for item in first.value.items] == ["effect-1", "statistics-1"]
    assert first.value.has_more is True
    repeated_first = stream.stream(limit=2, campaign_id="campaign-1")
    assert isinstance(repeated_first, ApiSuccess)
    assert repeated_first.to_dict() == first.to_dict()
    second = stream.stream(limit=2, cursor=first.value.next_cursor, campaign_id="campaign-1")
    assert isinstance(second, ApiSuccess)
    assert [item.event_id for item in second.value.items] == ["effect-2", "statistics-2"]
    repeated_second = stream.stream(limit=2, cursor=first.value.next_cursor, campaign_id="campaign-1")
    assert isinstance(repeated_second, ApiSuccess)
    assert repeated_second.to_dict() == second.to_dict()
    third = stream.stream(limit=2, cursor=second.value.next_cursor, campaign_id="campaign-1")
    assert isinstance(third, ApiSuccess)
    assert [item.event_id for item in third.value.items] == ["effect-3"]
    assert "environment_secret" not in json.dumps(first.to_dict(), sort_keys=True)
def test_event_stream_filters_and_rejects_cross_filter_cursor(tmp_path: Path) -> None:
    # Bind cursors to campaign and event-type filters so callers cannot silently skip records.
    stream = _stream(tmp_path)
    filtered = stream.stream(limit=5, campaign_id="campaign-1", event_type="test.completed")
    assert isinstance(filtered, ApiSuccess)
    assert [item.event_id for item in filtered.value.items] == ["statistics-1"]
    wrong_filter = stream.stream(
        limit=5,
        cursor=filtered.value.next_cursor,
        campaign_id="campaign-1",
        event_type="worker.stopped",
    )
    assert isinstance(wrong_filter, ApiRejected)
    assert wrong_filter.error.code == "invalid_cursor"
    invalid = stream.stream(limit=5, cursor="not-a-cursor", campaign_id="campaign-1")
    assert isinstance(invalid, ApiRejected)
    assert invalid.error.code == "invalid_cursor"
def test_progress_is_bounded_and_uses_latest_authoritative_checkpoints(tmp_path: Path) -> None:
    # Return campaign, worker, shard, outbox, quarantine, recovery and statistics progress in one snapshot.
    stream = _stream(tmp_path)
    first = stream.get_progress("campaign-1", shard_limit=2)
    assert isinstance(first, ApiSuccess)
    progress = first.value
    assert progress.campaign_status == "running"
    assert progress.campaign_revision == 7
    assert progress.completed_mutants == 2
    assert progress.total_mutants == 4
    assert progress.active_workers == 1
    assert progress.pending_outbox == 2
    assert progress.quarantine_count == 2
    assert progress.recovery_state == "complete"
    assert progress.statistics_checkpoint.sequence == 2
    assert [item.shard_id for item in progress.shards.items] == ["shard-1", "shard-2"]
    assert progress.shards.next_cursor is not None
    second = stream.get_progress("campaign-1", shard_limit=2, shard_cursor=progress.shards.next_cursor)
    assert isinstance(second, ApiSuccess)
    assert [item.shard_id for item in second.value.shards.items] == ["shard-3"]
def test_stream_source_contains_no_offset_or_polling_loop() -> None:
    # Keep the transport-neutral stream as a one-shot bounded read implementation.
    source = Path(inspect.getsourcefile(LocalEventStream)).read_text(encoding="utf-8").lower()
    assert " offset " not in source
    assert "while true" not in source
    assert "sleep(" not in source
