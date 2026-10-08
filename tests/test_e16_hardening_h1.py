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
    Success,
)


_LEASE_TIME = "2026-01-01T00:00:00Z"
_LIVE_NOW = "2026-01-01T00:00:01Z"


def _configuration(tmp_path: Path, campaign_id: str = "cmp_h1") -> CampaignConfiguration:
    # Build a two-mutant campaign whose immutable membership is easy to inspect in fan-in tests.
    return CampaignConfiguration(
        campaign_id=CampaignId(campaign_id),
        project=ProjectDescriptor(
            project_id=ProjectId(f"project_{campaign_id}"),
            display_name="H1 fan-in fixture",
            root_path=str(tmp_path),
        ),
        scope=MutationScope(source_path="app.py"),
        budget=CampaignBudget(max_mutants=2, max_workers=1),
    )


def _running_campaign(service: MutationCampaignService, campaign_id: CampaignId):
    # Advance the aggregate through its legal lifecycle and publish the immutable mutant count.
    created = service._campaign(campaign_id)
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
        current = handler(f"effect_{name}_{campaign_id.value}", campaign_id, expected_revision=current.value.revision_number)
        assert isinstance(current, Success), current
    discovered = current.value.record_discovery(2, expected_revision=current.value.revision_number)
    assert isinstance(discovered, Success)
    saved = service.store.save_campaign(discovered.value, expected_revision=current.value.revision_number)
    assert isinstance(saved, Success)
    return saved.value


def _leased_shard(service: MutationCampaignService, campaign_id: CampaignId):
    # Persist one running shard with a lease that is live only at the explicit test timestamp.
    shard = MutationShard.from_descriptor(
        campaign_id,
        ShardDescriptor(ShardId("shard_h1"), ("mutant_1", "mutant_2")),
        ordinal=0,
    )
    created = service.create_shard(shard)
    assert isinstance(created, Success)
    claimed = service.claim_shard(
        "effect_claim_h1",
        shard.shard_id,
        ShardLease(
            WorkerId("worker_h1"),
            "lease_h1",
            WorkerStatus.RUNNING,
            30.0,
            _LEASE_TIME,
            attempt=0,
        ),
        expected_revision=shard.revision_number,
    )
    assert isinstance(claimed, Success)
    started = service.start_shard(
        "effect_start_h1",
        shard.shard_id,
        expected_revision=claimed.value.revision_number,
    )
    assert isinstance(started, Success)
    return started.value


def _result(*results: MutantExecutionResult, status: WorkerStatus = WorkerStatus.COMPLETE) -> ShardExecutionResult:
    # Build a result bundle whose reported progress can be compared with its validated membership.
    return ShardExecutionResult(
        shard_id=ShardId("shard_h1"),
        worker_id=WorkerId("worker_h1"),
        status=status,
        completed_mutants=len(results),
        results=results,
    )


def _mutant(execution_id: str, mutant_id: str) -> MutantExecutionResult:
    # Build terminal evidence bound to the current lease and attempt.
    return MutantExecutionResult(
        execution_id=ExecutionId(execution_id),
        mutant_id=MutantId(mutant_id),
        status="killed",
        classification_reason="assertion",
        restore_verified=True,
        level_results=({"nodeids": ["tests/test_app.py::test_one"]},),
        lease_id="lease_h1",
        attempt=0,
    )


def test_h1_rejects_foreign_mutant_before_persistence(tmp_path: Path) -> None:
    # Prevent a worker from smuggling an execution outside its immutable shard membership.
    store = InMemoryMutationStore()
    service = MutationCampaignService(store)
    created = service.create_campaign(_configuration(tmp_path))
    assert isinstance(created, Success)
    running = _running_campaign(service, created.value.campaign_id)
    shard = _leased_shard(service, created.value.campaign_id)

    rejected = service.record_shard_result(
        "effect_foreign_h1",
        created.value.campaign_id,
        shard.shard_id,
        _result(_mutant("execution_foreign", "mutant_foreign")),
        expected_campaign_revision=running.revision_number,
        now=_LIVE_NOW,
    )

    assert rejected.code == "foreign_mutant"
    assert store.get_execution(ExecutionId("execution_foreign")).value is None


def test_h1_rejects_expired_current_lease(tmp_path: Path) -> None:
    # Prevent a result carrying the current token from entering fan-in after lease expiration.
    store = InMemoryMutationStore()
    service = MutationCampaignService(store)
    created = service.create_campaign(_configuration(tmp_path, "cmp_h1_expired"))
    assert isinstance(created, Success)
    running = _running_campaign(service, created.value.campaign_id)
    shard = _leased_shard(service, created.value.campaign_id)

    rejected = service.record_shard_result(
        "effect_expired_h1",
        created.value.campaign_id,
        shard.shard_id,
        _result(_mutant("execution_expired", "mutant_1")),
        expected_campaign_revision=running.revision_number,
        now="2026-01-01T00:01:00Z",
    )

    assert rejected.code == "lease_expired"
    assert store.get_execution(ExecutionId("execution_expired")).value is None


def test_h1_rejects_empty_terminal_bundle(tmp_path: Path) -> None:
    # A worker cannot complete a non-empty shard by reporting progress without execution evidence.
    store = InMemoryMutationStore()
    service = MutationCampaignService(store)
    created = service.create_campaign(_configuration(tmp_path, "cmp_h1_empty"))
    assert isinstance(created, Success)
    running = _running_campaign(service, created.value.campaign_id)
    shard = _leased_shard(service, created.value.campaign_id)
    empty = _result(status=WorkerStatus.COMPLETE)
    empty = ShardExecutionResult(
        shard_id=empty.shard_id,
        worker_id=empty.worker_id,
        status=empty.status,
        completed_mutants=1,
        results=(),
    )

    rejected = service.record_shard_result(
        "effect_empty_h1",
        created.value.campaign_id,
        shard.shard_id,
        empty,
        expected_campaign_revision=running.revision_number,
        now=_LIVE_NOW,
    )

    assert rejected.code == "empty_terminal_bundle"


def test_h1_aggregate_requires_complete_immutable_fan_in(tmp_path: Path) -> None:
    # Do not enter aggregation while a planned shard still lacks one of its mutant executions.
    store = InMemoryMutationStore()
    service = MutationCampaignService(store)
    created = service.create_campaign(_configuration(tmp_path, "cmp_h1_aggregate"))
    assert isinstance(created, Success)
    running = _running_campaign(service, created.value.campaign_id)
    shard = _leased_shard(service, created.value.campaign_id)

    partial = service.record_shard_result(
        "effect_partial_h1",
        created.value.campaign_id,
        shard.shard_id,
        _result(_mutant("execution_partial", "mutant_1"), status=WorkerStatus.ERROR),
        expected_campaign_revision=running.revision_number,
        now=_LIVE_NOW,
    )

    assert isinstance(partial, Success)
    rejected = service.aggregate(
        "effect_aggregate_h1",
        created.value.campaign_id,
        expected_revision=partial.value.campaign.revision_number,
    )

    assert rejected.code == "incomplete_shard"
