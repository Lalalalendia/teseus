from __future__ import annotations
from theseus_contracts import (
    CampaignId,
    ShardDescriptor,
    ShardId,
    ShardLease,
    WorkerCapabilities,
    WorkerHeartbeat,
    WorkerId,
    WorkerIdentity,
    WorkerStatus,
)
from gallifrey_mutation import InMemoryMutationStore, MutationCampaignService, MutationShard, Rejected, Success
def _capabilities() -> WorkerCapabilities:
    # Advertise one deterministic local worker capability set.
    return WorkerCapabilities("win32", "AMD64", ("3.14.2",), (1,), ("copy",), 2)
def test_old_instance_cannot_renew_or_deliver_after_reassignment() -> None:
    # Fence the old instance, lease and attempt after a replacement process owns the same immutable shard.
    store = InMemoryMutationStore()
    service = MutationCampaignService(store)
    campaign_id = CampaignId("campaign-e14-reassignment-fencing")
    shard = MutationShard.from_descriptor(
        campaign_id,
        ShardDescriptor(ShardId("shard-e14-reassignment-fencing"), ("mutant-1",)),
        ordinal=0,
    )
    assert isinstance(store.save_shard(shard, expected_revision=None), Success)
    old_identity = WorkerIdentity("worker-1", "instance-old", 4101, "birth-old")
    registered = service.register_worker(
        "effect.register.old",
        campaign_id,
        old_identity,
        _capabilities(),
        workspace="D:/workers/old",
        spool_path="D:/workers/spool",
    )
    assert isinstance(registered, Success)
    old_contract = ShardLease(
        WorkerId("worker-1"),
        "lease-old",
        WorkerStatus.LEASED,
        30.0,
        "2026-08-05T00:00:00Z",
        attempt=0,
        worker_instance_id="instance-old",
    )
    claimed = service.claim_shard_lease(
        "effect.claim.old",
        shard.shard_id,
        old_contract,
        expected_shard_revision=shard.revision_number,
    )
    assert isinstance(claimed, Success)
    bound = service.bind_worker_lease(
        "effect.bind.old",
        campaign_id,
        old_contract.lease_id,
        old_identity,
        expected_lease_revision=claimed.value.lease.revision_number,
        expected_shard_revision=claimed.value.shard.revision_number,
        expected_worker_revision=registered.value.revision_number,
    )
    assert isinstance(bound, Success)
    orphaned = service.orphan_lease_assignment(
        "effect.orphan.old",
        old_contract.lease_id,
        expected_lease_revision=bound.value.lease.revision_number,
        expected_shard_revision=bound.value.shard.revision_number,
        expected_worker_revision=bound.value.worker.revision_number if bound.value.worker else None,
        require_expired=False,
    )
    assert isinstance(orphaned, Success)
    replacement_contract = ShardLease(
        WorkerId("worker-1"),
        "lease-new",
        WorkerStatus.LEASED,
        30.0,
        "2026-08-05T00:01:00Z",
        attempt=1,
        worker_instance_id="instance-new",
    )
    reassigned = service.reassign_orphaned_shard(
        "effect.reassign.new",
        shard.shard_id,
        replacement_contract,
        expected_shard_revision=orphaned.value.shard.revision_number,
    )
    assert isinstance(reassigned, Success)
    new_identity = WorkerIdentity("worker-1", "instance-new", 4102, "birth-new")
    new_registered = service.register_worker(
        "effect.register.new",
        campaign_id,
        new_identity,
        _capabilities(),
        workspace="D:/workers/new",
        spool_path="D:/workers/spool",
    )
    assert isinstance(new_registered, Success)
    new_bound = service.bind_worker_lease(
        "effect.bind.new",
        campaign_id,
        replacement_contract.lease_id,
        new_identity,
        expected_lease_revision=reassigned.value.lease.revision_number,
        expected_shard_revision=reassigned.value.shard.revision_number,
        expected_worker_revision=new_registered.value.revision_number,
    )
    assert isinstance(new_bound, Success)
    stale_heartbeat = service.renew_worker_lease(
        "effect.heartbeat.old.late",
        campaign_id,
        WorkerHeartbeat(
            old_identity,
            1,
            "2026-08-05T00:01:01Z",
            current_campaign_id=campaign_id.value,
            current_shard_id=shard.shard_id.value,
            current_lease_id=old_contract.lease_id,
            current_attempt=0,
        ),
        expected_lease_revision=orphaned.value.lease.revision_number,
        expected_shard_revision=new_bound.value.shard.revision_number,
        expected_worker_revision=new_bound.value.worker.revision_number if new_bound.value.worker else 0,
    )
    assert isinstance(stale_heartbeat, Rejected)
    assert stale_heartbeat.code in {"lease_not_active", "stale_worker_instance"}
    stale_delivery = service.begin_lease_delivery(
        "effect.delivery.old.late",
        campaign_id,
        old_contract.lease_id,
        expected_lease_revision=orphaned.value.lease.revision_number,
        expected_worker_revision=new_bound.value.worker.revision_number if new_bound.value.worker else 0,
    )
    assert isinstance(stale_delivery, Rejected), (
        "E14 stale delivery fencing accepted an obsolete lease generation.\n"
        f"old_lease_id={old_contract.lease_id!r}\n"
        f"new_lease_id={replacement_contract.lease_id!r}\n"
        f"old_instance_id={old_identity.instance_id!r}\n"
        f"new_instance_id={new_identity.instance_id!r}\n"
        f"outcome={stale_delivery!r}"
    )
    assert stale_delivery.code in {"lease_not_running", "stale_worker_instance"}, (
        "E14 stale delivery rejection reported expiration before ownership fencing.\n"
        f"expected_codes={('lease_not_running', 'stale_worker_instance')!r}\n"
        f"actual_code={stale_delivery.code!r}\n"
        f"details={dict(stale_delivery.details)!r}"
    )
    assert new_bound.value.shard.mutant_ids == shard.mutant_ids
    assert new_bound.value.shard.attempt == 1