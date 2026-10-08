from __future__ import annotations
from theseus_contracts import CampaignId, ShardDescriptor, ShardId, ShardLease, WorkerId, WorkerStatus
from gallifrey_mutation import InMemoryMutationStore, MutationCampaignService, MutationShard, Rejected, Success

def test_reassignment_advances_attempt_once_and_rejects_a_second_generation() -> None:
    # Keep one effect replay idempotent while preventing another effect from advancing the same orphan twice.
    store = InMemoryMutationStore()
    service = MutationCampaignService(store)
    campaign_id = CampaignId("campaign-e14-reassignment-idempotency")
    shard = MutationShard.from_descriptor(
        campaign_id,
        ShardDescriptor(ShardId("shard-e14-reassignment-idempotency"), ("mutant-1",)),
        ordinal=0,
    )
    assert isinstance(store.save_shard(shard, expected_revision=None), Success)
    old_contract = ShardLease(
        WorkerId("worker-1"),
        "lease-old",
        WorkerStatus.LEASED,
        30.0,
        "2026-08-05T00:00:00Z",
        attempt=0,
    )
    claimed = service.claim_shard_lease(
        "effect.claim.old",
        shard.shard_id,
        old_contract,
        expected_shard_revision=shard.revision_number,
    )
    assert isinstance(claimed, Success)
    orphaned = service.orphan_lease_assignment(
        "effect.orphan.old",
        old_contract.lease_id,
        expected_lease_revision=claimed.value.lease.revision_number,
        expected_shard_revision=claimed.value.shard.revision_number,
        expected_worker_revision=None,
        require_expired=False,
    )
    assert isinstance(orphaned, Success)
    replacement = ShardLease(
        WorkerId("worker-1"),
        "lease-new",
        WorkerStatus.LEASED,
        30.0,
        "2026-08-05T00:01:00Z",
        attempt=1,
        worker_instance_id="instance-new",
    )
    first = service.reassign_orphaned_shard(
        "effect.reassign.once",
        shard.shard_id,
        replacement,
        expected_shard_revision=orphaned.value.shard.revision_number,
    )
    assert isinstance(first, Success)
    duplicate = service.reassign_orphaned_shard(
        "effect.reassign.once",
        shard.shard_id,
        replacement,
        expected_shard_revision=orphaned.value.shard.revision_number,
    )
    assert isinstance(duplicate, Success)
    assert duplicate.duplicate
    assert duplicate.value.shard.attempt == 1
    second_generation = service.reassign_orphaned_shard(
        "effect.reassign.twice",
        shard.shard_id,
        ShardLease(
            WorkerId("worker-1"),
            "lease-unexpected-second",
            WorkerStatus.LEASED,
            30.0,
            "2026-08-05T00:02:00Z",
            attempt=2,
            worker_instance_id="instance-third",
        ),
        expected_shard_revision=first.value.shard.revision_number,
    )
    assert isinstance(second_generation, Rejected)
    assert second_generation.code == "shard_not_orphaned"
    current = store.get_shard(shard.shard_id)
    assert isinstance(current, Success)
    assert current.value is not None and current.value.attempt == 1
