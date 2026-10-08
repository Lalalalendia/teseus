from __future__ import annotations

from pathlib import Path

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
    WorkerId,
)
from theseus_contracts.enums import WorkerStatus

from gallifrey_mutation import (
    InMemoryMutationStore,
    MutationCampaignService,
    MutationShard,
    MutationShardState,
    Success,
)


def _configuration(tmp_path: Path) -> CampaignConfiguration:
    # Build one minimal campaign contract for lease validation tests.
    return CampaignConfiguration(
        campaign_id=CampaignId("cmp_leases"),
        project=ProjectDescriptor(
            project_id=ProjectId("project_leases"),
            display_name="lease fixture",
            root_path=str(tmp_path),
        ),
        scope=MutationScope(source_path="app.py"),
        budget=CampaignBudget(max_mutants=1, max_workers=1),
    )


def _running_campaign(service: MutationCampaignService, campaign_id: CampaignId):
    # Advance the domain campaign through its ordered lifecycle before shard fan-in.
    current = service._campaign(campaign_id)
    assert isinstance(current, Success)
    for name, handler in (
        ("prepare", service.prepare),
        ("collect", service.collect),
        ("index", service.index),
        ("baseline", service.baseline),
        ("discover", service.discover),
        ("plan", service.plan),
        ("start", service.start),
    ):
        current = handler(f"effect_{name}", campaign_id, expected_revision=current.value.revision_number)
        assert isinstance(current, Success), current
    progress = current.value.record_discovery(1, expected_revision=current.value.revision_number)
    assert isinstance(progress, Success)
    saved = service.store.save_campaign(progress.value, expected_revision=current.value.revision_number)
    assert isinstance(saved, Success)
    return saved.value


def _leased_shard(service: MutationCampaignService, campaign_id: CampaignId):
    # Persist a claimed and running shard with one explicit lease token.
    shard = MutationShard.from_descriptor(
        campaign_id,
        ShardDescriptor(ShardId("shard_leases"), ("mutant_1",)),
        ordinal=0,
    )
    created = service.create_shard(shard)
    assert isinstance(created, Success)
    lease = ShardLease(
        WorkerId("worker_1"),
        "lease_current",
        WorkerStatus.RUNNING,
        30.0,
        "2026-01-01T00:00:00Z",
        attempt=0,
    )
    claimed = service.claim_shard(
        "effect_claim",
        shard.shard_id,
        lease,
        expected_revision=shard.revision_number,
    )
    assert isinstance(claimed, Success)
    started = service.start_shard(
        "effect_start_shard",
        shard.shard_id,
        expected_revision=claimed.value.revision_number,
    )
    assert isinstance(started, Success)
    return started.value, lease


def _result(*, lease_id: str, worker_id: str = "worker_1") -> ShardExecutionResult:
    # Build one result row carrying the worker's lease proof.
    return ShardExecutionResult(
        shard_id=ShardId("shard_leases"),
        worker_id=WorkerId(worker_id),
        status=WorkerStatus.COMPLETE,
        completed_mutants=1,
        results=(
            MutantExecutionResult(
                execution_id=ExecutionId("execution_1"),
                mutant_id=MutantId("mutant_1"),
                status="killed",
                classification_reason="assertion",
                restore_verified=True,
                lease_id=lease_id,
                attempt=0,
            ),
        ),
    )


def test_e13_stale_lease_result_is_rejected(tmp_path: Path) -> None:
    # Never allow a late result from an expired worker lease into fan-in storage.
    store = InMemoryMutationStore()
    service = MutationCampaignService(store)
    created = service.create_campaign(_configuration(tmp_path))
    assert isinstance(created, Success)
    running = _running_campaign(service, created.value.campaign_id)
    shard, _ = _leased_shard(service, created.value.campaign_id)

    rejected = service.record_shard_result(
        "effect_stale_result",
        created.value.campaign_id,
        shard.shard_id,
        _result(lease_id="lease_expired"),
        expected_campaign_revision=running.revision_number,
    )

    assert rejected.code == "stale_lease"
    assert store.get_execution(ExecutionId("execution_1")).value is None


def test_e13_worker_mismatch_and_duplicate_lease_are_rejected(tmp_path: Path) -> None:
    # Bind one shard to one worker and prevent both duplicate claims and foreign results.
    store = InMemoryMutationStore()
    service = MutationCampaignService(store)
    created = service.create_campaign(_configuration(tmp_path))
    assert isinstance(created, Success)
    running = _running_campaign(service, created.value.campaign_id)
    shard, lease = _leased_shard(service, created.value.campaign_id)

    duplicate = shard.claim(
        ShardLease(
            lease.worker_id,
            "lease_second",
            WorkerStatus.RUNNING,
            30.0,
            lease.heartbeat_at,
            attempt=lease.attempt,
        ),
        expected_revision=shard.revision_number,
    )
    assert duplicate.code == "duplicate_lease"

    foreign = service.record_shard_result(
        "effect_foreign_result",
        created.value.campaign_id,
        shard.shard_id,
        _result(lease_id=lease.lease_id, worker_id="worker_2"),
        expected_campaign_revision=running.revision_number,
    )
    assert foreign.code == "worker_mismatch"


def test_e13_lease_renewal_expiry_orphan_and_retry_are_durable(tmp_path: Path) -> None:
    # Exercise the complete Gallifrey lease lifecycle instead of checking only token equality.
    store = InMemoryMutationStore()
    service = MutationCampaignService(store)
    created = service.create_campaign(_configuration(tmp_path))
    assert isinstance(created, Success)
    _running_campaign(service, created.value.campaign_id)
    shard, lease = _leased_shard(service, created.value.campaign_id)

    renewed = service.renew_shard_lease(
        "effect_renew",
        shard.shard_id,
        ShardLease(
            worker_id=lease.worker_id,
            lease_id=lease.lease_id,
            status=WorkerStatus.RUNNING,
            lease_seconds=lease.lease_seconds,
            heartbeat_at="2026-01-01T00:00:10Z",
            heartbeat_seq=1,
            attempt=lease.attempt,
        ),
        expected_revision=shard.revision_number,
    )
    assert isinstance(renewed, Success)
    assert renewed.value.lease_expired("2026-01-01T00:00:41Z") is True

    orphaned = service.orphan_expired_shard(
        "effect_orphan",
        shard.shard_id,
        expected_revision=renewed.value.revision_number,
        now="2026-01-01T00:00:41Z",
    )
    assert isinstance(orphaned, Success)
    assert orphaned.value.status == MutationShardState.ORPHANED
    retried = service.retry_shard(
        "effect_retry",
        shard.shard_id,
        expected_revision=orphaned.value.revision_number,
    )
    assert isinstance(retried, Success)
    assert retried.value.status == MutationShardState.CREATED
    assert retried.value.attempt == 1


def test_e13_lease_wire_roundtrip_preserves_attempt() -> None:
    # Keep lease tokens and retry attempts stable across the public JSON boundary.
    lease = ShardLease(
        WorkerId("worker_1"),
        "lease_1",
        WorkerStatus.RETRYING,
        10.0,
        "2026-01-01T00:00:00Z",
        heartbeat_seq=4,
        attempt=2,
    )
    restored = ShardLease.from_dict(lease.to_dict())

    assert restored.lease_id == "lease_1"
    assert restored.heartbeat_seq == 4
    assert restored.attempt == 2


def test_e13_lease_renewal_rejects_stale_worker_instance(tmp_path: Path) -> None:
    # A restarted process with the same worker slot must not renew the previous instance lease.
    store = InMemoryMutationStore()
    service = MutationCampaignService(store)
    created = service.create_campaign(_configuration(tmp_path))
    assert isinstance(created, Success)
    _running_campaign(service, created.value.campaign_id)
    shard = MutationShard.from_descriptor(
        created.value.campaign_id,
        ShardDescriptor(ShardId("shard_instance"), ("mutant_1",)),
        ordinal=0,
    )
    assert isinstance(service.create_shard(shard), Success)
    lease = ShardLease(
        WorkerId("worker_1"),
        "lease_instance",
        WorkerStatus.RUNNING,
        30.0,
        "2026-01-01T00:00:00Z",
        attempt=0,
        worker_instance_id="instance-a",
    )
    claimed = service.claim_shard("effect_claim_instance", shard.shard_id, lease, expected_revision=0)
    assert isinstance(claimed, Success)
    started = service.start_shard(
        "effect_start_instance",
        shard.shard_id,
        expected_revision=claimed.value.revision_number,
    )
    assert isinstance(started, Success)
    rejected = service.renew_shard_lease(
        "effect_renew_stale_instance",
        shard.shard_id,
        ShardLease(
            WorkerId("worker_1"),
            "lease_instance",
            WorkerStatus.RUNNING,
            30.0,
            "2026-01-01T00:00:01Z",
            heartbeat_seq=1,
            attempt=0,
            worker_instance_id="instance-b",
        ),
        expected_revision=started.value.revision_number,
    )
    assert rejected.code == "stale_worker_instance"
