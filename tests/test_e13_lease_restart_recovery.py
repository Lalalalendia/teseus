from __future__ import annotations
from pathlib import Path
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    MutationScope,
    ProjectDescriptor,
    ProjectId,
    ShardDescriptor,
    ShardId,
    ShardLease,
    WorkerCapabilities,
    WorkerId,
    WorkerIdentity,
    WorkerStatus,
)
from gallifrey_mutation import (
    MutationCampaignService,
    MutationShard,
    SQLiteMutationStore,
    Success,
)

def _identity() -> WorkerIdentity:
    # Build one process-fenced worker identity shared by unified lease tests.
    return WorkerIdentity("worker-lease", "instance-lease", 4101, "birth-lease")

def _capabilities() -> WorkerCapabilities:
    # Advertise the local engine and copy workspace required by assignment.
    return WorkerCapabilities("win32", "AMD64", ("3.14.2",), (1,), ("copy",), 2)

def _configuration(campaign_id: CampaignId) -> CampaignConfiguration:
    # Build one complete public campaign contract for fan-in validation tests.
    return CampaignConfiguration(
        campaign_id=campaign_id,
        project=ProjectDescriptor(
            project_id=ProjectId("project-unified-lease"),
            display_name="Unified lease fixture",
            root_path=".",
        ),
        scope=MutationScope(source_path="app.py"),
        budget=CampaignBudget(max_mutants=1, max_workers=1),
    )

def _setup(store):
    # Create one shard, registered worker and authoritative bound lease generation.
    service = MutationCampaignService(store)
    campaign_id = CampaignId("campaign-unified-lease")
    shard = MutationShard.from_descriptor(
        campaign_id,
        ShardDescriptor(ShardId("shard-unified-lease"), ("mutant-1",)),
        ordinal=0,
    )
    assert isinstance(store.save_shard(shard, expected_revision=None), Success)
    worker = service.register_worker(
        "effect.register.unified",
        campaign_id,
        _identity(),
        _capabilities(),
        workspace="D:/state/worker/workspace",
        spool_path="D:/state/worker/spool",
    )
    assert isinstance(worker, Success)
    public_lease = ShardLease(
        WorkerId("worker-lease"),
        "lease-unified-1",
        WorkerStatus.LEASED,
        30.0,
        "2026-08-04T15:00:00Z",
        heartbeat_seq=0,
        attempt=0,
        worker_instance_id="instance-lease",
    )
    claimed = service.claim_shard_lease(
        "effect.lease.claim",
        shard.shard_id,
        public_lease,
        expected_shard_revision=0,
    )
    assert isinstance(claimed, Success)
    pre = service.renew_claimed_lease(
        "effect.lease.pre-heartbeat",
        public_lease.lease_id,
        heartbeat_at="2026-08-04T15:00:01Z",
        heartbeat_sequence=1,
        expected_lease_revision=claimed.value.lease.revision_number,
        expected_shard_revision=claimed.value.shard.revision_number,
        now="2026-08-04T15:00:01Z",
    )
    assert isinstance(pre, Success)
    bound = service.bind_worker_lease(
        "effect.lease.bind",
        campaign_id,
        public_lease.lease_id,
        _identity(),
        expected_lease_revision=pre.value.lease.revision_number,
        expected_shard_revision=pre.value.shard.revision_number,
        expected_worker_revision=worker.value.revision_number,
    )
    assert isinstance(bound, Success)
    return service, campaign_id, bound.value


def test_sqlite_restart_restores_exact_lease_generation(tmp_path: Path) -> None:
    # Restore authoritative lease identity and revisions without consulting worker JSON files.
    path = tmp_path / "mutation.sqlite3"
    first = SQLiteMutationStore(path)
    service, campaign_id, state = _setup(first)
    first.close()
    second = SQLiteMutationStore(path)
    restored = second.get_lease(state.lease.lease_id)
    worker = second.get_worker(campaign_id, "worker-lease")
    shard = second.get_shard(state.shard.shard_id)
    assert isinstance(restored, Success) and restored.value == state.lease
    assert isinstance(worker, Success) and worker.value == state.worker
    assert isinstance(shard, Success) and shard.value == state.shard
    second.close()
