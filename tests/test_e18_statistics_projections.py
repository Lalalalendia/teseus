from __future__ import annotations
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import pytest
from theseus_contracts import StatisticsEvent, StatisticsEventType
from theseus_statistics import (
    STATISTICS_DURATION_SAMPLE_SIZE,
    STATISTICS_PROJECTION_MAX_LIMIT,
    StatisticsEventConflictError,
    StatisticsEventStore,
    StatisticsProjectionStore,
)

def _timestamp(seconds: int) -> str:
    # Build deterministic UTC timestamps for projection interval assertions.
    return (datetime(2026, 8, 5, 12, 0, tzinfo=timezone.utc) + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")

def _event(
    event_type: str,
    sequence: int,
    *,
    seconds: int | None = None,
    payload: dict[str, object] | None = None,
    test_id: str = "tests/test_sample.py::test_value",
    mutant_id: str = "mutant-001",
    execution_id: str = "execution-001",
    worker_id: str = "worker-001",
    campaign_id: str = "campaign-001",
) -> StatisticsEvent:
    # Build one fully identified canonical event accepted by every PR20 summary domain.
    return StatisticsEvent.create(
        event_type,
        "producer-projection-tests",
        sequence,
        payload or {},
        timestamp=_timestamp(sequence if seconds is None else seconds),
        campaign_id=campaign_id,
        plan_id="plan-001",
        shard_id="shard-001",
        execution_id=execution_id,
        worker_id=worker_id,
        test_id=test_id,
        mutant_id=mutant_id,
        process_id="process-001",
    )

def _stores(tmp_path: Path) -> tuple[StatisticsEventStore, StatisticsProjectionStore]:
    # Create one immutable event store and its independent mutable projection layer.
    event_store = StatisticsEventStore(tmp_path / "statistics.sqlite")
    return event_store, StatisticsProjectionStore(event_store)

def test_projection_is_incremental_and_checkpointed(tmp_path: Path) -> None:
    # Apply only events after the durable projection checkpoint on every bounded call.
    event_store, projections = _stores(tmp_path)
    event_store.append_many(
        (
            _event(StatisticsEventType.TEST_COMPLETED.value, 1, payload={"outcome": "passed", "duration_ms": 10}),
            _event(StatisticsEventType.TEST_COMPLETED.value, 2, payload={"outcome": "failed", "duration_ms": 20}),
            _event(StatisticsEventType.TEST_COMPLETED.value, 3, payload={"outcome": "passed", "duration_ms": 30}),
        )
    )
    first = projections.project(max_events=2)
    second = projections.project(max_events=2)
    repeated = projections.project(max_events=2)
    assert first.events_projected == 2
    assert first.has_more is True
    assert second.events_projected == 1
    assert second.checkpoint.sequence > first.checkpoint.sequence
    assert repeated.events_projected == 0
    assert repeated.checkpoint == second.checkpoint
    assert projections.get("test", "tests/test_sample.py::test_value").event_count == 3

def test_projection_builds_campaign_test_mutant_worker_and_execution_summaries(tmp_path: Path) -> None:
    # Derive all requested counters, duration aggregates, flaky transitions and worker utilization.
    event_store, projections = _stores(tmp_path)
    event_store.append_many(
        (
            _event(StatisticsEventType.WORKER_STARTED.value, 1, seconds=0),
            _event(StatisticsEventType.MUTANT_STARTED.value, 2, seconds=1, payload={"attempt": 1}),
            _event(StatisticsEventType.TEST_COMPLETED.value, 3, seconds=2, payload={"outcome": "passed", "duration_ms": 10}),
            _event(StatisticsEventType.TEST_COMPLETED.value, 4, seconds=3, payload={"outcome": "failed", "duration_ms": 30}),
            _event(StatisticsEventType.MUTANT_COMPLETED.value, 5, seconds=4, payload={"outcome": "passed", "duration_ms": 50}),
            _event(StatisticsEventType.RECOVERY_PERFORMED.value, 6, seconds=5),
            _event(StatisticsEventType.EXECUTION_TIMEOUT.value, 7, seconds=6, payload={"duration_ms": 100}),
            _event(StatisticsEventType.WORKER_STOPPED.value, 8, seconds=10),
        )
    )
    assert projections.project(max_events=20).events_projected == 8
    campaign = projections.get("campaign", "campaign-001")
    test = projections.get("test", "tests/test_sample.py::test_value")
    mutant = projections.get("mutant", "mutant-001")
    worker = projections.get("worker", "worker-001")
    execution = projections.get("execution", "execution-001")
    assert all(item is not None for item in (campaign, test, mutant, worker, execution))
    assert campaign.passed_count == 2
    assert campaign.failed_count == 1
    assert campaign.timeout_count == 1
    assert campaign.retry_count == 1
    assert campaign.recovery_count == 1
    assert campaign.flaky_transition_count == 1
    assert test.duration_count == 3
    assert test.duration_total_ms == 140.0
    assert test.duration_min_ms == 10.0
    assert test.duration_max_ms == 100.0
    assert test.duration_median_ms == 30.0
    assert test.duration_p95_ms == 93.0
    assert test.flaky_transition_count == 1
    assert mutant.retry_count == 1
    assert mutant.recovery_count == 1
    assert execution.timeout_count == 1
    assert worker.active is False
    assert worker.active_duration_ms == 10_000.0
    assert worker.busy_duration_ms == 150.0
    assert worker.utilization == 0.015


def test_campaign_flaky_transitions_do_not_leak_between_campaigns(tmp_path: Path) -> None:
    # Keep campaign flaky state scoped by both campaign and test identity.
    event_store, projections = _stores(tmp_path)
    event_store.append_many(
        (
            _event(StatisticsEventType.TEST_COMPLETED.value, 1, campaign_id="campaign-a", payload={"outcome": "passed"}),
            _event(StatisticsEventType.TEST_COMPLETED.value, 2, campaign_id="campaign-b", payload={"outcome": "failed"}),
            _event(StatisticsEventType.TEST_COMPLETED.value, 3, campaign_id="campaign-b", payload={"outcome": "passed"}),
        )
    )
    projections.project(max_events=10)
    global_test = projections.get("test", "tests/test_sample.py::test_value")
    campaign_a = projections.get("campaign", "campaign-a")
    campaign_b = projections.get("campaign", "campaign-b")
    assert global_test.flaky_transition_count == 2
    assert campaign_a.flaky_transition_count == 0
    assert campaign_b.flaky_transition_count == 1

def test_projection_counts_escalation_reuse_infrastructure_and_recovery(tmp_path: Path) -> None:
    # Keep operational decision and recovery counters in campaign summaries.
    event_store, projections = _stores(tmp_path)
    event_store.append_many(
        (
            _event(StatisticsEventType.SELECTION_ESCALATED.value, 1),
            _event(StatisticsEventType.REUSE_DECIDED.value, 2),
            _event(StatisticsEventType.INFRASTRUCTURE_FAILED.value, 3),
            _event(StatisticsEventType.RECOVERY_PERFORMED.value, 4),
        )
    )
    projections.project(max_events=10)
    campaign = projections.get("campaign", "campaign-001")
    assert campaign.escalation_count == 1
    assert campaign.reuse_count == 1
    assert campaign.infrastructure_failure_count == 1
    assert campaign.error_count == 1
    assert campaign.recovery_count == 1

def test_duration_percentiles_use_bounded_deterministic_samples(tmp_path: Path) -> None:
    # Bound percentile memory independently from the number of completed duration events.
    event_store, projections = _stores(tmp_path)
    events = tuple(
        _event(
            StatisticsEventType.TEST_COMPLETED.value,
            sequence,
            payload={"outcome": "passed", "duration_ms": sequence},
        )
        for sequence in range(1, 401)
    )
    event_store.append_many(events)
    projections.project(max_events=500)
    summary = projections.get("test", "tests/test_sample.py::test_value")
    assert summary.duration_count == 400
    assert summary.duration_total_ms == 80_200.0
    assert 1 <= summary.duration_sample_count <= STATISTICS_DURATION_SAMPLE_SIZE
    connection = sqlite3.connect(event_store.path)
    try:
        stored_samples = connection.execute(
            "SELECT COUNT(*) FROM statistics_duration_samples WHERE entity_type = 'test' AND entity_id = ?",
            ("tests/test_sample.py::test_value",),
        ).fetchone()[0]
    finally:
        connection.close()
    assert stored_samples == summary.duration_sample_count
    assert stored_samples <= STATISTICS_DURATION_SAMPLE_SIZE

def test_projection_rebuild_is_idempotent(tmp_path: Path) -> None:
    # Replaying immutable history from zero produces the same summaries and checkpoint every time.
    event_store, projections = _stores(tmp_path)
    event_store.append_many(
        tuple(
            _event(
                StatisticsEventType.TEST_COMPLETED.value,
                sequence,
                payload={"outcome": "passed" if sequence % 2 else "failed", "duration_ms": sequence * 5},
            )
            for sequence in range(1, 7)
        )
    )
    first_result = projections.rebuild(batch_size=2)
    first_summary = projections.get("test", "tests/test_sample.py::test_value")
    second_result = projections.rebuild(batch_size=3)
    second_summary = projections.get("test", "tests/test_sample.py::test_value")
    assert first_result.events_projected == 6
    assert second_result.events_projected == 6
    assert first_result.checkpoint == second_result.checkpoint
    assert first_summary == second_summary

def test_projection_query_and_export_are_bounded_and_keyset_based(tmp_path: Path) -> None:
    # Page summaries by stable entity ID without OFFSET scans or duplicate exports.
    event_store, projections = _stores(tmp_path)
    event_store.append_many(
        tuple(
            _event(
                StatisticsEventType.TEST_COMPLETED.value,
                sequence,
                test_id=f"tests/test_sample.py::test_{sequence}",
                execution_id=f"execution-{sequence}",
                payload={"outcome": "passed", "duration_ms": sequence},
            )
            for sequence in range(1, 6)
        )
    )
    projections.project(max_events=10)
    first = projections.query("test", limit=2)
    second = projections.query("test", limit=2, cursor=first.next_cursor)
    third_rows, third_cursor = projections.export("test", limit=2, cursor=second.next_cursor)
    observed = [item.entity_id for item in first.items] + [item.entity_id for item in second.items] + [str(item["entity_id"]) for item in third_rows]
    assert observed == sorted(observed)
    assert len(observed) == 5
    assert len(set(observed)) == 5
    assert third_cursor is None
    with pytest.raises(ValueError, match=str(STATISTICS_PROJECTION_MAX_LIMIT)):
        projections.query("test", limit=STATISTICS_PROJECTION_MAX_LIMIT + 1)
    with pytest.raises(ValueError, match="does not match"):
        projections.query("worker", limit=2, cursor=first.next_cursor)


def test_conflicting_ingestion_cannot_mutate_projected_summaries(tmp_path: Path) -> None:
    # Preserve both immutable history and derived counters after a fail-closed payload conflict.
    event_store, projections = _stores(tmp_path)
    original = _event(StatisticsEventType.TEST_COMPLETED.value, 1, payload={"outcome": "passed", "duration_ms": 10})
    conflicting = _event(StatisticsEventType.TEST_COMPLETED.value, 1, payload={"outcome": "failed", "duration_ms": 20})
    event_store.append(original)
    projections.project(max_events=10)
    before = projections.get("test", "tests/test_sample.py::test_value")
    with pytest.raises(StatisticsEventConflictError):
        event_store.append(conflicting)
    assert projections.project(max_events=10).events_projected == 0
    assert projections.get("test", "tests/test_sample.py::test_value") == before

def test_projection_never_changes_canonical_event_identity(tmp_path: Path) -> None:
    # Leave the source event JSON and deterministic identity unchanged after projection and rebuild.
    event_store, projections = _stores(tmp_path)
    event = _event(StatisticsEventType.TEST_COMPLETED.value, 1, payload={"outcome": "passed", "duration_ms": 10})
    canonical = event.to_json()
    event_store.append(event)
    projections.project(max_events=10)
    projections.rebuild(batch_size=1)
    stored = event_store.query(limit=1).items[0].event
    assert stored.event_id == event.event_id
    assert stored.to_json() == canonical
