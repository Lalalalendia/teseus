from __future__ import annotations
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from gallifrey_mutation import (
    CampaignState,
    MutationCampaign,
    MutationCampaignService,
    MutationExecution,
    MutationExecutionState,
    MutationResult,
    MutationShard,
    MutationShardState,
    OperatorActionStatus,
    Rejected,
    SQLiteMutationStore,
    Success,
)
from theseus_api import ApiSuccess, LocalOperatorActions
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    ExecutionId,
    MutantId,
    MutationScope,
    ProjectDescriptor,
    ProjectId,
    ShardDescriptor,
    ShardId,
)
from theseus_local import LocalCampaignCoordinator

def _configuration(root: Path, campaign_id: str = "campaign-operator-authority") -> CampaignConfiguration:
    # Build one durable campaign configuration for operator action authority tests.
    return CampaignConfiguration(
        campaign_id=CampaignId(campaign_id),
        project=ProjectDescriptor(
            ProjectId("project-operator-authority"),
            "Operator authority",
            str(root),
        ),
        scope=MutationScope("app.py", scope_kind="project"),
        budget=CampaignBudget(max_workers=2),
        reports_dir=str(root / "reports"),
    )

def _store_with_retryable_shards(tmp_path: Path) -> tuple[SQLiteMutationStore, MutationCampaignService, MutationCampaign]:
    # Persist one running campaign with failed, partial and complete immutable shards.
    store = SQLiteMutationStore(tmp_path / "campaign.sqlite3")
    service = MutationCampaignService(store)
    campaign = replace(
        MutationCampaign.create(_configuration(tmp_path)),
        status=CampaignState.RUNNING,
        plan_id="plan-operator-authority",
        prepared_snapshot_id="prepared-operator-authority",
        total_mutants=3,
    )
    assert isinstance(store.save_campaign(campaign, expected_revision=None), Success)
    statuses = (
        MutationShardState.FAILED,
        MutationShardState.PARTIAL,
        MutationShardState.COMPLETE,
    )
    for ordinal, status in enumerate(statuses):
        descriptor = ShardDescriptor(
            ShardId(f"shard-{ordinal}"),
            (f"mutant-{ordinal}",),
            plan_id="plan-operator-authority",
        )
        shard = replace(
            MutationShard.from_descriptor(campaign.campaign_id, descriptor, ordinal=ordinal),
            status=status,
            completed_count=1 if status == MutationShardState.COMPLETE else 0,
        )
        assert isinstance(store.save_shard(shard, expected_revision=None), Success)
    return store, service, campaign

def test_campaign_retry_is_atomic_replay_safe_and_preserves_complete_shards(tmp_path: Path) -> None:
    # Requeue only retryable shards under one durable action without changing topology or complete work.
    store, service, campaign = _store_with_retryable_shards(tmp_path)
    try:
        first = service.retry_campaign(
            "action-retry-campaign",
            campaign.campaign_id,
            expected_revision=campaign.revision_number,
        )
        assert isinstance(first, Success)
        assert first.value.status == OperatorActionStatus.COMPLETED
        assert tuple(first.value.result["shard_ids"]) == ("shard-0", "shard-1")
        shards = store.list_shards(campaign.campaign_id)
        assert isinstance(shards, Success)
        by_id = {item.shard_id.value: item for item in shards.value}
        assert by_id["shard-0"].status == MutationShardState.CREATED
        assert by_id["shard-1"].status == MutationShardState.CREATED
        assert by_id["shard-0"].attempt == 1
        assert by_id["shard-1"].attempt == 1
        assert by_id["shard-2"].status == MutationShardState.COMPLETE
        assert by_id["shard-2"].attempt == 0
        repeated = service.retry_campaign(
            "action-retry-campaign",
            campaign.campaign_id,
            expected_revision=campaign.revision_number,
        )
        assert isinstance(repeated, Success) and repeated.duplicate is True
        after = store.list_shards(campaign.campaign_id)
        assert isinstance(after, Success)
        assert {item.shard_id.value: item.attempt for item in after.value} == {
            "shard-0": 1,
            "shard-1": 1,
            "shard-2": 0,
        }
        conflict = service.retry_campaign(
            "action-retry-campaign",
            campaign.campaign_id,
            expected_revision=campaign.revision_number + 1,
        )
        assert isinstance(conflict, Rejected)
        assert conflict.code == "action_id_conflict"
    finally:
        store.close()

def test_stale_operator_rejection_is_durable_and_replayed(tmp_path: Path) -> None:
    # Persist a stale optimistic-concurrency rejection so later state changes cannot alter its outcome.
    store, service, campaign = _store_with_retryable_shards(tmp_path)
    try:
        first = service.request_operator_action(
            "action-stale",
            "resume",
            campaign.campaign_id,
            expected_revision=campaign.revision_number + 1,
        )
        assert isinstance(first, Success)
        assert first.value.status == OperatorActionStatus.REJECTED
        assert first.value.error_code == "stale_revision"
        repeated = service.request_operator_action(
            "action-stale",
            "resume",
            campaign.campaign_id,
            expected_revision=campaign.revision_number + 1,
        )
        assert isinstance(repeated, Success) and repeated.duplicate is True
        assert repeated.value == first.value
    finally:
        store.close()

def test_resume_action_replays_terminal_receipt_without_second_coordinator_run(tmp_path: Path) -> None:
    # Persist requested, running and completed resume states around one authoritative coordinator invocation.
    store = SQLiteMutationStore(tmp_path / "campaign.sqlite3")
    campaign = MutationCampaign.create(_configuration(tmp_path, "campaign-resume-authority"))
    assert isinstance(store.save_campaign(campaign, expected_revision=None), Success)
    store.close()
    calls: list[str] = []
    def resume_executor(database_path: Path, campaign_id: str) -> object:
        # Simulate one successful coordinator resume without coupling this test to the engine process.
        calls.append(campaign_id)
        return SimpleNamespace(
            campaign=replace(
                campaign,
                status=CampaignState.RUNNING,
                revision_number=campaign.revision_number + 1,
            ),
            database_path=database_path,
        )
    actions = LocalOperatorActions(
        tmp_path / "campaign.sqlite3",
        resume_executor=resume_executor,
    )
    first = actions.resume(
        "action-resume-campaign",
        campaign.campaign_id.value,
        expected_revision=campaign.revision_number,
    )
    repeated = actions.resume_campaign(
        "action-resume-campaign",
        campaign.campaign_id.value,
        expected_revision=campaign.revision_number,
    )
    assert isinstance(first, ApiSuccess)
    assert isinstance(repeated, ApiSuccess)
    assert repeated.to_dict() == first.to_dict()
    assert calls == [campaign.campaign_id.value]
    reopened = SQLiteMutationStore(tmp_path / "campaign.sqlite3")
    try:
        action = reopened.get_operator_action("action-resume-campaign")
        assert isinstance(action, Success)
        assert action.value is not None
        assert action.value.status == OperatorActionStatus.COMPLETED
        assert action.value.result["campaign_revision"] == 1
    finally:
        reopened.close()

def test_running_resume_action_is_recoverable_after_process_crash(tmp_path: Path) -> None:
    # Continue a durable running receipt after a crash instead of creating a second action identity.
    store = SQLiteMutationStore(tmp_path / "campaign.sqlite3")
    service = MutationCampaignService(store)
    campaign = MutationCampaign.create(_configuration(tmp_path, "campaign-resume-recovery"))
    assert isinstance(store.save_campaign(campaign, expected_revision=None), Success)
    requested = service.request_operator_action(
        "action-resume-recovery",
        "resume",
        campaign.campaign_id,
        expected_revision=campaign.revision_number,
    )
    assert isinstance(requested, Success)
    started = service.start_operator_action("action-resume-recovery")
    assert isinstance(started, Success)
    assert started.value.status == OperatorActionStatus.RUNNING
    store.close()
    calls: list[str] = []
    actions = LocalOperatorActions(
        tmp_path / "campaign.sqlite3",
        resume_executor=lambda database_path, campaign_id: (
            calls.append(campaign_id)
            or SimpleNamespace(
                campaign=replace(campaign, status=CampaignState.RUNNING, revision_number=1),
                database_path=database_path,
            )
        ),
    )
    outcome = actions.resume(
        "action-resume-recovery",
        campaign.campaign_id.value,
        expected_revision=campaign.revision_number,
    )
    assert isinstance(outcome, ApiSuccess)
    assert calls == [campaign.campaign_id.value]
    assert outcome.value.status == "completed"

def test_confirmed_execution_is_carried_into_retry_attempt_without_rerun() -> None:
    # Convert verified prior evidence into a new attempt-bound result while preserving semantic truth.
    execution = MutationExecution(
        execution_id=ExecutionId("execution-confirmed"),
        campaign_id=CampaignId("campaign-confirmed"),
        shard_id=ShardId("shard-confirmed"),
        mutant_id=MutantId("mutant-confirmed"),
        attempt=0,
        status=MutationExecutionState.COMPLETE,
        semantic_result=MutationResult.KILLED,
        selected_tests=("tests/test_app.py::test_value",),
        restore_verified=True,
        revision_number=1,
        lease_id="lease-old",
        test_observations=({"test_id": "tests/test_app.py::test_value"},),
    )
    carried = LocalCampaignCoordinator._confirmed_execution_result(
        execution,
        campaign_id="campaign-confirmed",
        lease_id="lease-new",
        attempt=1,
    )
    assert carried is not None
    assert carried.status == "killed"
    assert carried.attempt == 1
    assert carried.lease_id == "lease-new"
    assert carried.execution_id != execution.execution_id
    assert carried.level_results[0]["level"] == "confirmed-retry"
