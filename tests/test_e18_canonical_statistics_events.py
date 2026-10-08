from __future__ import annotations
import sqlite3
from pathlib import Path
import pytest
from theseus_contracts import (
    STATISTICS_EVENT_SCHEMA_VERSION,
    StatisticsEvent,
    StatisticsEventType,
    decode_message,
    encode_message,
)
from theseus_contracts.errors import ContractError, MissingFieldError
from theseus_statistics import (
    STATISTICS_QUERY_MAX_LIMIT,
    JsonlStatisticsEventSink,
    StatisticsEventConflictError,
    StatisticsEventStore,
)
_ALL_EVENT_TYPES = tuple(item.value for item in StatisticsEventType)
def _event(
    event_type: str = StatisticsEventType.TEST_COMPLETED.value,
    *,
    sequence: int = 1,
    timestamp: str = "2026-08-05T12:00:00Z",
    payload: dict[str, object] | None = None,
    test_id: str = "tests/test_sample.py::test_value",
) -> StatisticsEvent:
    # Build one fully identified canonical event for contract and projection tests.
    return StatisticsEvent.create(
        event_type,
        "producer-local-runtime",
        sequence,
        payload or {"outcome": "passed", "duration_ms": 12.5},
        timestamp=timestamp,
        campaign_id="campaign-001",
        plan_id="plan-001",
        shard_id="shard-001",
        execution_id="execution-001",
        worker_id="worker-001",
        test_id=test_id,
        mutant_id="mutant-001",
        process_id="process-001",
    )
def test_all_roadmap_statistics_event_types_are_canonical() -> None:
    # Accept every PR19 event name while keeping every identity axis explicit on the wire.
    for sequence, event_type in enumerate(_ALL_EVENT_TYPES, start=1):
        event = _event(event_type, sequence=sequence)
        assert event.known_type is True
        assert event.schema_version == STATISTICS_EVENT_SCHEMA_VERSION
        assert event.to_dict()["producer_id"] == "producer-local-runtime"
        assert set(
            (
                "campaign_id",
                "plan_id",
                "shard_id",
                "execution_id",
                "worker_id",
                "test_id",
                "mutant_id",
                "process_id",
            )
        ).issubset(event.to_dict())
def test_statistics_schema_version_is_required() -> None:
    # Reject a wire event whose schema boundary was omitted instead of assuming a legacy version.
    raw = _event().to_dict()
    raw.pop("schema_version")
    with pytest.raises(MissingFieldError):
        StatisticsEvent.from_dict(raw)
def test_required_event_identity_is_fail_closed() -> None:
    # Reject a test lifecycle event without its explicit canonical test identity.
    with pytest.raises(MissingFieldError, match="test_id"):
        StatisticsEvent.create(
            StatisticsEventType.TEST_COMPLETED,
            "producer-local-runtime",
            1,
            {"outcome": "passed"},
            timestamp="2026-08-05T12:00:00Z",
            campaign_id="campaign-001",
            plan_id="plan-001",
            shard_id="shard-001",
            execution_id="execution-001",
            worker_id="worker-001",
        )
def test_timestamp_is_not_part_of_statistics_event_identity() -> None:
    # Derive the same event ID for the same logical event observed at different UTC timestamps.
    first = _event(timestamp="2026-08-05T12:00:00Z")
    second = _event(timestamp="2026-08-05T12:00:01Z")
    assert first.event_id == second.event_id
    assert "timestamp" not in first.logical_identity()
def test_logical_identity_change_changes_event_id() -> None:
    # Change an explicit identity axis and observe a different deterministic event ID.
    first = _event(test_id="tests/test_sample.py::test_value")
    second = _event(test_id="tests/test_sample.py::test_other")
    assert first.event_id != second.event_id
def test_mapping_order_does_not_change_canonical_event_id_or_json() -> None:
    # Canonicalize payload mappings so insertion order cannot affect serialization or replay.
    first = _event(payload={"outcome": "passed", "details": {"a": 1, "b": 2}})
    second = _event(payload={"details": {"b": 2, "a": 1}, "outcome": "passed"})
    assert first.event_id == second.event_id
    assert first.to_json() == second.to_json()
def test_protocol_round_trip_preserves_statistics_event() -> None:
    # Route the canonical event through the shared message protocol without treating it as EngineEvent.
    event = _event()
    decoded = decode_message(encode_message(event))
    assert isinstance(decoded, StatisticsEvent)
    assert decoded == event
def test_statistics_event_alias_conflicts_are_rejected() -> None:
    # Reject inconsistent envelope aliases so one wire event has only one canonical representation.
    event = _event()
    raw = event.to_dict()
    raw["message_id"] = "stat_evt_other"
    with pytest.raises(ContractError, match="message_id"):
        StatisticsEvent.from_dict(raw)
    raw = event.to_dict()
    raw["created_at"] = "2026-08-05T12:00:01Z"
    with pytest.raises(ContractError, match="created_at"):
        StatisticsEvent.from_dict(raw)
def test_forged_statistics_event_id_is_rejected() -> None:
    # Reject a caller-supplied ID that does not match the event's timestamp-free logical identity.
    event = _event()
    raw = event.to_dict()
    raw["event_id"] = "stat_evt_forged"
    raw["message_id"] = "stat_evt_forged"
    with pytest.raises(ContractError, match="does not match"):
        StatisticsEvent.from_dict(raw)
def test_store_exact_replay_is_idempotent(tmp_path: Path) -> None:
    # Store one event once and treat its exact retry as a replay without another row.
    store = StatisticsEventStore(tmp_path / "statistics.sqlite")
    event = _event()
    assert store.append(event) is True
    assert store.append(event) is False
    assert store.count() == 1
def test_same_event_id_with_different_payload_conflicts_and_preserves_original(tmp_path: Path) -> None:
    # Fail closed on payload drift while leaving the previously committed immutable event untouched.
    store = StatisticsEventStore(tmp_path / "statistics.sqlite")
    original = _event(payload={"outcome": "passed"})
    conflicting = _event(payload={"outcome": "failed"})
    assert original.event_id == conflicting.event_id
    assert store.append(original) is True
    with pytest.raises(StatisticsEventConflictError, match=original.event_id.value):
        store.append(conflicting)
    page = store.query(limit=1)
    assert store.count() == 1
    assert page.items[0].event.payload == {"outcome": "passed"}
def test_store_is_append_only_even_through_direct_sql(tmp_path: Path) -> None:
    # Reject update and delete statements at the database boundary, not only through the Python API.
    path = tmp_path / "statistics.sqlite"
    store = StatisticsEventStore(path)
    store.append(_event())
    connection = sqlite3.connect(path)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("UPDATE statistics_events SET event_type = 'changed'")
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("DELETE FROM statistics_events")
    finally:
        connection.close()
def test_store_reopen_preserves_replay_and_conflict_semantics(tmp_path: Path) -> None:
    # Reopen the SQLite projection and retain deterministic replay and conflict behavior.
    path = tmp_path / "statistics.sqlite"
    event = _event(payload={"state": "complete"})
    assert StatisticsEventStore(path).append(event) is True
    reopened = StatisticsEventStore(path)
    assert reopened.append(event) is False
    with pytest.raises(StatisticsEventConflictError):
        reopened.append(_event(payload={"state": "failed"}))
def test_store_appends_multiple_events_and_queries_with_bounded_keyset_pages(tmp_path: Path) -> None:
    # Traverse stable sequence-plus-ID pages without duplicates, gaps or offset pagination.
    store = StatisticsEventStore(tmp_path / "statistics.sqlite")
    events = tuple(_event(sequence=sequence) for sequence in range(1, 7))
    assert store.append_many(events) == (6, 0)
    first = store.query(limit=2)
    second = store.query(limit=2, cursor=first.next_cursor)
    third = store.query(limit=2, cursor=second.next_cursor)
    observed = tuple(item.event.event_id.value for page in (first, second, third) for item in page.items)
    assert observed == tuple(event.event_id.value for event in events)
    assert len(set(observed)) == 6
    assert third.next_cursor is None
def test_same_timestamp_events_paginate_without_duplicates(tmp_path: Path) -> None:
    # Use the immutable store sequence as the primary keyset order when timestamps are identical.
    store = StatisticsEventStore(tmp_path / "statistics.sqlite")
    events = tuple(_event(sequence=sequence, timestamp="2026-08-05T12:00:00Z") for sequence in range(1, 5))
    store.append_many(events)
    first = store.query(limit=1)
    second = store.query(limit=1, cursor=first.next_cursor)
    assert first.items[0].event.timestamp == second.items[0].event.timestamp
    assert first.items[0].event.event_id != second.items[0].event.event_id
def test_query_rejects_unbounded_limits(tmp_path: Path) -> None:
    # Reject query requests above the explicit maximum instead of silently scanning more history.
    store = StatisticsEventStore(tmp_path / "statistics.sqlite")
    with pytest.raises(ValueError, match=str(STATISTICS_QUERY_MAX_LIMIT)):
        store.query(limit=STATISTICS_QUERY_MAX_LIMIT + 1)
def test_jsonl_ingestion_resumes_from_byte_checkpoint(tmp_path: Path) -> None:
    # Ingest only newly appended complete records after the first durable source checkpoint.
    journal = tmp_path / "events.jsonl"
    sink = JsonlStatisticsEventSink(journal)
    store = StatisticsEventStore(tmp_path / "statistics.sqlite")
    first = _event(sequence=1)
    second = _event(sequence=2)
    sink.publish(first)
    initial = store.ingest_jsonl(journal, source_id="worker-001", max_records=10)
    sink.publish(second)
    resumed = store.ingest_jsonl(journal, source_id="worker-001", max_records=10)
    replay = store.ingest_jsonl(journal, source_id="worker-001", max_records=10)
    assert initial.events_appended == 1
    assert resumed.events_seen == 1
    assert resumed.events_appended == 1
    assert resumed.source_offset > initial.source_offset
    assert replay.events_seen == 0
    assert store.count() == 2
def test_jsonl_ingestion_leaves_partial_tail_for_next_call(tmp_path: Path) -> None:
    # Advance the source checkpoint only after a complete newline-terminated event record.
    journal = tmp_path / "events.jsonl"
    event = _event()
    raw = event.to_json().encode("utf-8")
    journal.write_bytes(raw)
    store = StatisticsEventStore(tmp_path / "statistics.sqlite")
    partial = store.ingest_jsonl(journal, source_id="worker-partial", max_records=10)
    with journal.open("ab") as handle:
        handle.write(b"\n")
    complete = store.ingest_jsonl(journal, source_id="worker-partial", max_records=10)
    assert partial.events_seen == 0
    assert partial.source_offset == 0
    assert complete.events_appended == 1
def test_pr19_store_has_no_pr20_aggregation_surface(tmp_path: Path) -> None:
    # Keep percentile, flaky-rate and aggregate materialization outside the PR19 projection API.
    store = StatisticsEventStore(tmp_path / "statistics.sqlite")
    for name in ("percentile", "percentiles", "flaky_rate", "aggregate", "summarize"):
        assert not hasattr(store, name)
