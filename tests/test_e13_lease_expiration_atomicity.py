from __future__ import annotations
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
    InMemoryMutationStore,
    MutationCampaignService,
    MutationLeaseState,
    MutationShard,
    MutationWorkerState,
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


def test_expiration_orphans_lease_shard_and_worker_together() -> None:
    # End all ownership projections in one effect after the authoritative expiration boundary.
    service, campaign_id, state = _setup(InMemoryMutationStore())
    assert state.worker is not None
    orphaned = service.orphan_lease_assignment(
        "effect.lease.orphan",
        state.lease.lease_id,
        expected_lease_revision=state.lease.revision_number,
        expected_shard_revision=state.shard.revision_number,
        expected_worker_revision=state.worker.revision_number,
        now="2026-08-04T16:00:00Z",
    )
    assert isinstance(orphaned, Success)
    assert orphaned.value.lease.status == MutationLeaseState.ORPHANED
    assert orphaned.value.shard.status.value == "orphaned"
    assert orphaned.value.worker is not None
    assert orphaned.value.worker.status == MutationWorkerState.ORPHANED

def test_failure_closes_pre_registration_claim_without_worker_row() -> None:
    # Close lease and failed shard atomically even when the worker process never registered.
    store = InMemoryMutationStore()
    service = MutationCampaignService(store)
    campaign_id = CampaignId("campaign-pre-registration-failure")
    shard = MutationShard.from_descriptor(
        campaign_id,
        ShardDescriptor(ShardId("shard-pre-registration-failure"), ("mutant-1",)),
        ordinal=0,
    )
    assert isinstance(store.save_shard(shard, expected_revision=None), Success)
    public_lease = ShardLease(
        WorkerId("worker-never-registered"),
        "lease-pre-registration-failure",
        WorkerStatus.LEASED,
        30.0,
        "2026-08-04T15:00:00Z",
        attempt=0,
    )
    claimed = service.claim_shard_lease(
        "effect.claim.pre-registration-failure",
        shard.shard_id,
        public_lease,
        expected_shard_revision=shard.revision_number,
    )
    assert isinstance(claimed, Success)
    failed_shard = claimed.value.shard.record_completion(
        0,
        expected_revision=claimed.value.shard.revision_number,
        failed=True,
    )
    assert isinstance(failed_shard, Success)
    saved = store.save_shard(
        failed_shard.value,
        expected_revision=claimed.value.shard.revision_number,
    )
    assert isinstance(saved, Success)
    failed = service.fail_lease_assignment(
        "effect.fail.pre-registration-failure",
        campaign_id,
        claimed.value.lease.lease_id,
        expected_lease_revision=claimed.value.lease.revision_number,
        expected_worker_revision=None,
    )
    assert isinstance(failed, Success)
    assert failed.value.lease.status == MutationLeaseState.FAILED
    assert failed.value.worker is None
    assert failed.value.shard.lease is not None
    assert failed.value.shard.lease.status == WorkerStatus.ERROR
