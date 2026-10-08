from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path

import pytest

from gallifrey_mutation import (
    CampaignState,
    MutationCampaign,
    MutationExecution,
    MutationExecutionState,
    MutationResult,
    MutationShard,
    MutationShardState,
)
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    ExecutionId,
    MutationScope,
    MutantId,
    ProjectDescriptor,
    ProjectId,
    ShardId,
    TestCommandDescriptor,
)
from theseus_local.canonical_report import (
    CanonicalReportError,
    build_canonical_report,
)


def _campaign(tmp_path: Path, *, total: int = 1) -> MutationCampaign:
    # Build one durable campaign aggregate suitable for isolated report contract cases.
    configuration = CampaignConfiguration(
        campaign_id=CampaignId("campaign-canonical-contract"),
        project=ProjectDescriptor(
            project_id=ProjectId("project-canonical-contract"),
            display_name="Canonical contract fixture",
            root_path=str(tmp_path),
            test_command=TestCommandDescriptor((sys.executable, "-m", "pytest", "-q")),
        ),
        scope=MutationScope(
            source_path="app.py",
            function="choose",
            operators=("condition_to_not",),
        ),
        budget=CampaignBudget(max_mutants=total, max_workers=2, max_test_seconds=7.0),
        no_escalation=True,
    )
    return replace(
        MutationCampaign.create(configuration),
        status=CampaignState.COMPLETED,
        total_mutants=total,
        completed_mutants=total,
        baseline_snapshot_id="baseline-contract",
        plan_id="plan-contract",
    )


def _shard(campaign: MutationCampaign, mutant_ids: tuple[str, ...], *, attempt: int = 1) -> MutationShard:
    # Create one current durable shard attempt for the supplied mutant identities.
    return MutationShard(
        shard_id=ShardId("shard-contract"),
        campaign_id=campaign.campaign_id,
        ordinal=0,
        mutant_ids=tuple(MutantId(item) for item in mutant_ids),
        attempt=attempt,
        status=MutationShardState.COMPLETE,
        completed_count=len(mutant_ids),
    )


def _execution(
    campaign: MutationCampaign,
    mutant_id: str,
    number: int,
    *,
    status: MutationExecutionState = MutationExecutionState.COMPLETE,
    semantic: MutationResult | None = MutationResult.KILLED,
    attempt: int = 1,
) -> MutationExecution:
    # Create one immutable execution row while varying only its terminal semantic state.
    return MutationExecution(
        execution_id=ExecutionId(f"execution-contract-{number}"),
        campaign_id=campaign.campaign_id,
        shard_id=ShardId("shard-contract"),
        mutant_id=MutantId(mutant_id),
        attempt=attempt,
        status=status,
        semantic_result=semantic,
        selected_tests=("tests/test_app.py::test_choose",),
        duration_seconds=float(number),
        restore_verified=status == MutationExecutionState.COMPLETE,
    )


def test_report_preserves_all_public_terminal_statuses(tmp_path: Path) -> None:
    # Project durable execution outcomes into exactly the six canonical semantic labels.
    campaign = _campaign(tmp_path, total=6)
    mutant_ids = tuple(f"mutant-{index}" for index in range(6))
    executions = (
        _execution(campaign, mutant_ids[0], 0, semantic=MutationResult.KILLED),
        _execution(campaign, mutant_ids[1], 1, semantic=MutationResult.SURVIVED),
        _execution(campaign, mutant_ids[2], 2, semantic=MutationResult.INVALID),
        _execution(campaign, mutant_ids[3], 3, semantic=MutationResult.TIMEOUT),
        _execution(campaign, mutant_ids[4], 4, semantic=MutationResult.INFRASTRUCTURE_ERROR),
        _execution(
            campaign,
            mutant_ids[5],
            5,
            status=MutationExecutionState.CANCELLED,
            semantic=None,
        ),
    )
    report = build_canonical_report(campaign, (_shard(campaign, mutant_ids),), executions)
    assert set(report["counts"]) == {
        "killed",
        "survived",
        "invalid",
        "timeout",
        "infrastructure_error",
        "cancelled",
    }
    assert all(report["counts"][key] == 1 for key in report["counts"])


def test_report_is_deterministic_and_has_required_sections(tmp_path: Path) -> None:
    # Keep semantic report bytes stable and expose the roadmap-required section contract.
    campaign = _campaign(tmp_path)
    shard = _shard(campaign, ("mutant-1",))
    execution = _execution(campaign, "mutant-1", 1)
    first = build_canonical_report(campaign, (shard,), (execution,))
    second = build_canonical_report(campaign, (shard,), (execution,))
    assert first == second
    assert {
        "campaign",
        "scope",
        "configuration",
        "baseline",
        "summary",
        "results",
        "performance",
        "artifacts",
        "integrity",
    }.issubset(first)


def test_terminal_projection_uses_completed_durable_state(tmp_path: Path) -> None:
    # Finalization publishes a terminal projection before the completion transition commits.
    campaign = replace(_campaign(tmp_path), status=CampaignState.MATERIALIZING)
    shard = _shard(campaign, ("mutant-1",))
    execution = _execution(campaign, "mutant-1", 1)
    report = build_canonical_report(
        campaign,
        (shard,),
        (execution,),
        status_override="complete",
    )
    assert report["status"] == "complete"
    assert report["campaign"]["status"] == "complete"
    assert report["campaign"]["durable_state"] == "completed"


def test_report_reconstruction_is_stable_after_execution_and_observation_reordering(tmp_path: Path) -> None:
    campaign = _campaign(tmp_path, total=2)
    shard = _shard(campaign, ("mutant-b", "mutant-a"))
    first = replace(
        _execution(campaign, "mutant-b", 1),
        selected_tests=("tests/z.py::test_z", "tests/a.py::test_a"),
        test_observations=(
            {"nodeid": "z", "status": "passed"},
            {"nodeid": "a", "status": "failed"},
        ),
        artifacts=(),
    )
    second = replace(
        _execution(campaign, "mutant-a", 2, semantic=MutationResult.SURVIVED),
        selected_tests=("tests/a.py::test_a", "tests/z.py::test_z"),
        test_observations=(
            {"status": "failed", "nodeid": "a"},
            {"status": "passed", "nodeid": "z"},
        ),
    )
    report_a = build_canonical_report(campaign, (shard,), (first, second))
    report_b = build_canonical_report(campaign, (shard,), (second, first))
    assert report_a == report_b


def test_report_excludes_stale_attempts_without_using_latest_engine_state(tmp_path: Path) -> None:
    # Select the current durable shard attempt and ignore an older terminal execution.
    campaign = _campaign(tmp_path, total=1)
    shard = _shard(campaign, ("mutant-1",), attempt=1)
    stale = _execution(campaign, "mutant-1", 1, semantic=MutationResult.SURVIVED, attempt=0)
    current = _execution(campaign, "mutant-1", 2, semantic=MutationResult.KILLED, attempt=1)
    report = build_canonical_report(campaign, (shard,), (stale, current))
    assert report["counts"]["killed"] == 1
    assert report["counts"]["survived"] == 0
    assert report["integrity"]["stale_attempts_excluded"] == 1


def test_report_rejects_duplicate_current_terminal_evidence(tmp_path: Path) -> None:
    # Fail closed when two different durable execution identities claim one current mutant.
    campaign = _campaign(tmp_path, total=1)
    shard = _shard(campaign, ("mutant-1",))
    first = _execution(campaign, "mutant-1", 1)
    second = _execution(campaign, "mutant-1", 2, semantic=MutationResult.SURVIVED)
    with pytest.raises(CanonicalReportError, match="ambiguous"):
        build_canonical_report(campaign, (shard,), (first, second))


def test_report_keeps_mutation_evidence_and_attempt_identities_distinct(tmp_path: Path) -> None:
    # Make the three identity layers explicit so a retry cannot overwrite mutation identity.
    campaign = _campaign(tmp_path)
    report = build_canonical_report(
        campaign,
        (_shard(campaign, ("mutant-1",)),),
        (_execution(campaign, "mutant-1", 1),),
    )
    row = report["results"][0]
    assert row["mutation_identity"]["mutant_id"] == "mutant-1"
    assert row["evidence_identity"]["execution_id"] == row["execution_id"]
    assert row["execution_attempt_identity"]["attempt"] == row["attempt"]
    assert row["mutation_identity"] != row["evidence_identity"]
    assert row["evidence_identity"] != row["execution_attempt_identity"]


def test_report_maps_failed_execution_to_infrastructure_error(tmp_path: Path) -> None:
    # Preserve worker/process failure as an infrastructure result rather than a survived mutant.
    campaign = _campaign(tmp_path)
    failed = _execution(
        campaign,
        "mutant-1",
        1,
        status=MutationExecutionState.FAILED,
        semantic=None,
    )
    report = build_canonical_report(campaign, (_shard(campaign, ("mutant-1",)),), (failed,))
    assert report["results"][0]["status"] == "infrastructure_error"
    assert report["counts"]["infrastructure_error"] == 1
