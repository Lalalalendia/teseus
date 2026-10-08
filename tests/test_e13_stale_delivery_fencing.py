from __future__ import annotations
from dataclasses import replace
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    ExecutionId,
    MutantExecutionResult,
    MutantId,
    MutationScope,
    ProjectDescriptor,
    ProjectId,
    ShardDescriptor,
    ShardExecutionResult,
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
    Rejected,
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
def _running_campaign(service: MutationCampaignService, campaign_id: CampaignId):
    # Advance one campaign to running with one planned mutant before authoritative fan-in.
    created = service.create_campaign(_configuration(campaign_id))
    assert isinstance(created, Success)
    current = created
    for name, handler in (
        ("prepare", service.prepare),
        ("collect", service.collect),
        ("index", service.index),
        ("baseline", service.baseline),
        ("discover", service.discover),
        ("plan", service.plan),
        ("start", service.start),
    ):
        current = handler(
            f"effect.campaign.{name}",
            campaign_id,
            expected_revision=current.value.revision_number,
        )
        assert isinstance(current, Success), current
    progress = current.value.record_discovery(1, expected_revision=current.value.revision_number)
    assert isinstance(progress, Success)
    saved = service.store.save_campaign(
        progress.value,
        expected_revision=current.value.revision_number,
    )
    assert isinstance(saved, Success)
    return saved.value
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
def test_released_authoritative_lease_rejects_late_result() -> None:
    # Fence late durable evidence after terminal fan-in releases the authoritative lease generation.
    store = InMemoryMutationStore()
    service, campaign_id, state = _setup(store)
    campaign = _running_campaign(service, campaign_id)
    assert state.worker is not None
    delivering = service.begin_lease_delivery(
        "effect.lease.delivering",
        campaign_id,
        state.lease.lease_id,
        expected_lease_revision=state.lease.revision_number,
        expected_worker_revision=state.worker.revision_number,
        now="2026-08-04T15:00:02Z",
    )
    assert isinstance(delivering, Success), (
        "E14 delivery transition failed before the deterministic fixture fan-in time.\n"
        f"lease_id={state.lease.lease_id!r}\n"
        f"lease_status={getattr(state.lease.status, 'value', state.lease.status)!r}\n"
        f"lease_expires_at={state.lease.expires_at!r}\n"
        f"delivery_started_at={'2026-08-04T15:00:02Z'!r}\n"
        f"outcome={delivering!r}"
    )
    accepted_result = ShardExecutionResult(
        shard_id=state.shard.shard_id,
        worker_id=state.lease.worker_id,
        status=WorkerStatus.COMPLETE,
        completed_mutants=1,
        results=(
            MutantExecutionResult(
                execution_id=ExecutionId("execution-current"),
                mutant_id=MutantId("mutant-1"),
                status="killed",
                classification_reason="authoritative delivery",
                restore_verified=True,
                lease_id=state.lease.lease_id,
                attempt=state.lease.attempt,
            ),
        ),
    )
    recorded = service.record_shard_result(
        "effect.result.current",
        campaign_id,
        state.shard.shard_id,
        accepted_result,
        expected_campaign_revision=campaign.revision_number,
        now="2026-08-04T15:00:03Z",
    )
    assert isinstance(recorded, Success)
    released = service.release_lease_assignment(
        "effect.lease.release",
        campaign_id,
        state.lease.lease_id,
        expected_lease_revision=delivering.value.lease.revision_number,
        expected_worker_revision=delivering.value.worker.revision_number,
    )
    assert isinstance(released, Success)
    assert released.value.lease.status == MutationLeaseState.RELEASED
    assert released.value.worker is not None
    assert released.value.worker.status == MutationWorkerState.IDLE
    assert released.value.shard.lease is not None
    assert released.value.shard.lease.status == WorkerStatus.COMPLETE
    semantic_rewrite = replace(
        released.value.shard,
        ordinal=released.value.shard.ordinal + 1,
        revision_number=released.value.shard.revision_number + 1,
    )
    immutable = store.save_shard(
        semantic_rewrite,
        expected_revision=released.value.shard.revision_number,
    )
    assert isinstance(immutable, Rejected)
    assert immutable.code == "aggregate_immutable"
    late_result = ShardExecutionResult(
        shard_id=state.shard.shard_id,
        worker_id=state.lease.worker_id,
        status=WorkerStatus.COMPLETE,
        completed_mutants=1,
        results=(
            MutantExecutionResult(
                execution_id=ExecutionId("execution-late"),
                mutant_id=MutantId("mutant-1"),
                status="killed",
                classification_reason="late delivery",
                restore_verified=True,
                lease_id=state.lease.lease_id,
                attempt=state.lease.attempt,
            ),
        ),
    )
    rejected = service.record_shard_result(
        "effect.result.late",
        campaign_id,
        state.shard.shard_id,
        late_result,
        expected_campaign_revision=recorded.value.campaign.revision_number,
        now="2026-08-04T15:00:04Z",
    )
    assert isinstance(rejected, Rejected)
    assert rejected.code == "lease_not_active"