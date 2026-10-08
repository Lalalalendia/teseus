from __future__ import annotations

from dataclasses import replace

from theseus_contracts import CampaignId, ShardId, WorkerId, WorkerStatus
from gallifrey_mutation import MutationLease, MutationLeaseState


def _lease() -> MutationLease:
    # Build one valid authoritative lease that can be projected through every lifecycle state.
    return MutationLease(
        campaign_id=CampaignId("campaign-lease-status-projection"),
        shard_id=ShardId("shard-lease-status-projection"),
        lease_id="lease-status-projection",
        worker_id=WorkerId("worker-lease-status-projection"),
        attempt=0,
        lease_seconds=30.0,
        heartbeat_at="2026-08-04T15:00:00Z",
    )


def test_every_authoritative_lease_state_has_a_public_worker_status_projection() -> None:
    # Evaluate the complete mapping so an invalid enum member cannot break even the claimed path.
    expected = {
        MutationLeaseState.CLAIMED: WorkerStatus.LEASED,
        MutationLeaseState.RUNNING: WorkerStatus.RUNNING,
        MutationLeaseState.DELIVERING: WorkerStatus.RUNNING,
        MutationLeaseState.RELEASED: WorkerStatus.COMPLETE,
        MutationLeaseState.FAILED: WorkerStatus.ERROR,
        MutationLeaseState.CANCELLED: WorkerStatus.CANCELLED,
        MutationLeaseState.ORPHANED: WorkerStatus.ORPHANED,
    }
    base = _lease()
    assert {
        state: replace(base, status=state).to_shard_lease().status
        for state in MutationLeaseState
    } == expected
