from __future__ import annotations

from pathlib import Path

from theseus_local.worker_runtime import (
    DurableExecutionSpool,
    matching_pending_delivery,
    quarantine_non_current_deliveries,
)


def _payload(*, lease_id: str, attempt: int, status: str = "complete") -> dict:
    # Build the smallest durable delivery envelope used by crash reassignment tests.
    return {
        "assignment": {
            "campaign_id": "campaign-crash",
            "shard_id": "shard-000",
            "lease_id": lease_id,
            "attempt": attempt,
        },
        "result": {"shard_result": {"status": status, "completed_mutants": 1, "results": []}},
    }


def test_same_assignment_replays_and_stale_attempt_is_quarantined(tmp_path: Path) -> None:
    # Preserve same-attempt replay while making an old crashed attempt invisible to reassignment.
    spool = DurableExecutionSpool(tmp_path / "spool")
    stale = spool.publish("event-stale", _payload(lease_id="lease-0", attempt=0))
    current = spool.publish("event-current", _payload(lease_id="lease-1", attempt=1))

    assert matching_pending_delivery(
        spool,
        campaign_id="campaign-crash",
        shard_id="shard-000",
        lease_id="lease-1",
        attempt=1,
    ) == current
    assert quarantine_non_current_deliveries(
        spool,
        campaign_id="campaign-crash",
        shard_id="shard-000",
        current_lease_id="lease-1",
        current_attempt=1,
    ) == (stale.event_id,)
    assert spool.pending() == (current,)
    assert spool.quarantined(stale.event_id)


def test_replay_identity_does_not_cross_shards_or_attempts(tmp_path: Path) -> None:
    # Refuse to replay evidence whose assignment identity differs by even one ownership field.
    spool = DurableExecutionSpool(tmp_path / "spool")
    spool.publish("event-1", _payload(lease_id="lease-1", attempt=1))
    assert matching_pending_delivery(
        spool,
        campaign_id="campaign-crash",
        shard_id="shard-other",
        lease_id="lease-1",
        attempt=1,
    ) is None
    assert matching_pending_delivery(
        spool,
        campaign_id="campaign-crash",
        shard_id="shard-000",
        lease_id="lease-1",
        attempt=0,
    ) is None
