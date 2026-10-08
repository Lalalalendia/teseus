from __future__ import annotations
from pathlib import Path
from theseus_contracts import CampaignId, ShardDescriptor, ShardId, ShardLease, WorkerId, WorkerStatus
from gallifrey_mutation import MutationCampaignService, MutationShard, SQLiteMutationStore, Success

def test_restart_replays_the_exact_reassignment_effect(tmp_path: Path) -> None:
    # Persist attempt+1 once and return the same receipt after SQLite restart without incrementing again.
    path = tmp_path / "reassignment.sqlite3"
    first = SQLiteMutationStore(path)
    campaign_id = CampaignId("campaign-e14-reassignment-restart")
    shard = MutationShard.from_descriptor(
        campaign_id,
        ShardDescriptor(ShardId("shard-e14-reassignment-restart"), ("mutant-1",)),
        ordinal=0,
    )
    assert isinstance(first.save_shard(shard, expected_revision=None), Success)
    service = MutationCampaignService(first)
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
    applied = service.reassign_orphaned_shard(
        "effect.reassign.restart",
        shard.shard_id,
        replacement,
        expected_shard_revision=orphaned.value.shard.revision_number,
    )
    assert isinstance(applied, Success)
    first.close()
    second = SQLiteMutationStore(path)
    replayed = MutationCampaignService(second).reassign_orphaned_shard(
        "effect.reassign.restart",
        shard.shard_id,
        replacement,
        expected_shard_revision=orphaned.value.shard.revision_number,
    )
    assert isinstance(replayed, Success)
    assert replayed.duplicate
    assert replayed.value.lease == applied.value.lease
    assert replayed.value.shard == applied.value.shard
    restored = second.get_shard(shard.shard_id)
    assert isinstance(restored, Success)
    assert restored.value is not None and restored.value.attempt == 1
    second.close()
