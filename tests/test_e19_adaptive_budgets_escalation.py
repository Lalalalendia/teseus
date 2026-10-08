from __future__ import annotations
import json
import pytest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    MutantDescriptor,
    MutantId,
    MutationScope,
    PreparedCampaign,
    ProjectDescriptor,
    ProjectId,
    SelectionSnapshot,
    StatisticsEvent,
    StatisticsEventType,
    TestLevelPlan as _TestLevelPlan,
)
from theseus_knowledge import KnowledgePlaneStore
from theseus_local import LocalCampaignCoordinator
from theseus_planner import CampaignPlanner
from theseus_statistics import StatisticsEventStore, StatisticsProjectionStore


def _mutant(mutant_id: str, line_no: int) -> MutantDescriptor:
    # Build one deterministic candidate for adaptive planner tests.
    return MutantDescriptor(
        MutantId(mutant_id),
        "condition_to_not",
        "app.py",
        line_no,
        1,
        "old",
        "new",
        function_id="choose",
    )


def _selection() -> SelectionSnapshot:
    # Freeze the selected, domain and full execution ladder used by PR22.
    return SelectionSnapshot(
        snapshot_id="selection-e19",
        source_path="app.py",
        source_sha256="source-e19",
        algorithm_version="selection-v3",
        levels=(
            _TestLevelPlan("L1", "selected", ("tests/test_app.py::test_selected",)),
            _TestLevelPlan("L2", "domain", ("tests/test_app.py::test_domain",)),
            _TestLevelPlan("L3", "full", ("tests/test_app.py::test_full",)),
        ),
        selected_tests=("tests/test_app.py::test_selected",),
    )


def _prepared(mutants: tuple[MutantDescriptor, ...]) -> PreparedCampaign:
    # Bind candidates and the escalation ladder to one immutable preparation snapshot.
    return PreparedCampaign(
        CampaignId("campaign-e19"),
        "app.py",
        "source-e19",
        "index-e19",
        mutants,
        selection=_selection(),
        snapshot_id="prepared-e19",
        function_id="choose",
    )


def _configuration(root: Path, **budget_values: object) -> CampaignConfiguration:
    # Build one adaptive campaign request with explicit public budget controls.
    return CampaignConfiguration(
        campaign_id=CampaignId("campaign-e19"),
        project=ProjectDescriptor(ProjectId("project-e19"), "E19", str(root)),
        scope=MutationScope("app.py", scope_kind="project"),
        budget=CampaignBudget(max_workers=4, **budget_values),
    )


def test_dynamic_shards_and_escalation_policy_are_frozen(tmp_path: Path) -> None:
    # Derive shard width and the complete correctness ladder from immutable adaptive inputs.
    mutants = tuple(_mutant(f"m{index}", index) for index in range(1, 5))
    configuration = _configuration(
        tmp_path,
        target_shard_seconds=3.0,
        max_test_seconds=10.0,
        adaptive_timeout_multiplier=2.5,
    )
    history = {
        item.mutant_id.value: {"duration_seconds": 2.0, "sample_count": 3, "kill_probability": 0.5}
        for item in mutants
    }
    plan = CampaignPlanner().build(configuration, _prepared(mutants), adaptive_observations=history)
    assert len(plan.shards) == 3
    assert plan.adaptive_policy["shard_count"] == 3
    assert plan.adaptive_policy["escalation_order"] == ["selected", "domain", "full"]
    assert plan.adaptive_policy["stop_on_kill"] is True
    assert [item["timeout_seconds"] for item in plan.levels] == [10.0, 20.0, 40.0]
    assert all(item["escalate_on"] == "survived" for item in plan.levels)
    assert plan.from_dict(plan.to_dict()) == plan


def test_coordinator_materializes_planner_owned_escalation_levels(tmp_path: Path) -> None:
    # Prevent worker preparation from recovering escalation levels outside the immutable campaign plan.
    mutant = _mutant("m1", 1)
    prepared = _prepared((mutant,))
    plan = CampaignPlanner().build(
        replace(_configuration(tmp_path), no_escalation=True),
        prepared,
    )
    runtime_prepared = LocalCampaignCoordinator._prepared_for_campaign_plan(prepared, plan)
    assert runtime_prepared.selection is not None
    assert tuple(item.name for item in runtime_prepared.selection.levels) == ("L1",)
    assert tuple(item.mutant_id.value for item in runtime_prepared.mutants) == ("m1",)


def test_survivor_priority_and_exploration_are_deterministic(tmp_path: Path) -> None:
    # Rank deterministic exploration first and then prioritize a confirmed survivor over an easy kill.
    mutants = (_mutant("m-killed", 1), _mutant("m-survived", 2), _mutant("m-unknown", 3))
    configuration = _configuration(tmp_path, exploration_rate=1.0, adaptive_seed="seed-e19")
    history = {
        "m-killed": {"duration_seconds": 1.0, "killed_count": 5, "sample_count": 5},
        "m-survived": {"duration_seconds": 1.0, "survived_count": 5, "sample_count": 5},
    }
    first = CampaignPlanner().build(configuration, _prepared(mutants), adaptive_observations=history)
    second = CampaignPlanner().build(
        configuration,
        _prepared(tuple(reversed(mutants))),
        mutants=tuple(reversed(mutants)),
        adaptive_observations=history,
    )
    assert [item.mutant.mutant_id.value for item in first.selected] == [
        "m-unknown",
        "m-survived",
        "m-killed",
    ]
    assert first.plan_id == second.plan_id
    reasons = {item.mutant.mutant_id.value: item.reasons for item in first.selected}
    assert "adaptive:exploration" in reasons["m-unknown"]
    assert "adaptive:historical-survivor" in reasons["m-survived"]


def test_random_audit_is_planner_owned_without_legacy_audit_flag(tmp_path: Path) -> None:
    # Sample authorized reuse through the immutable plan even when legacy policy did not require an audit.
    mutant = _mutant("m-reuse", 1)
    configuration = CampaignConfiguration(
        campaign_id=CampaignId("campaign-e19"),
        project=ProjectDescriptor(ProjectId("project-e19"), "E19", str(tmp_path)),
        scope=MutationScope("app.py", scope_kind="project"),
        budget=CampaignBudget(max_workers=1, random_audit_rate=1.0),
        reuse_mode="exact",
    )
    reuse = {
        "m-reuse": {
            "kind": "exact",
            "eligible": True,
            "authorized": True,
            "reuse_mode": "exact",
            "audit_required": False,
            "source_event_id": "event-reuse",
            "matched_test_ids": ["tests/test_app.py::test_selected"],
        }
    }
    plan = CampaignPlanner().build(configuration, _prepared((mutant,)), reuse_decisions=reuse)
    assert plan.audit_sample == ("m-reuse",)
    assert plan.decisions[0].action == "audit"
    assert plan.decisions[0].audit_selected is True


def test_statistics_history_is_loaded_in_one_bounded_batch(tmp_path: Path) -> None:
    # Materialize mutant outcomes and expose them as one adaptive observation snapshot.
    event_store = StatisticsEventStore(tmp_path / "statistics.sqlite3")
    projections = StatisticsProjectionStore(event_store)
    event_store.append_many(
        (
            StatisticsEvent.create(
                StatisticsEventType.MUTANT_COMPLETED,
                "producer-e19",
                1,
                {"semantic_result": "killed", "duration_ms": 100.0},
                timestamp="2026-08-06T00:00:01Z",
                campaign_id="campaign-history",
                plan_id="plan-history",
                shard_id="shard-history",
                execution_id="execution-killed",
                worker_id="worker-history",
                mutant_id="m-killed",
            ),
            StatisticsEvent.create(
                StatisticsEventType.MUTANT_COMPLETED,
                "producer-e19",
                2,
                {"semantic_result": "survived", "duration_ms": 200.0},
                timestamp="2026-08-06T00:00:02Z",
                campaign_id="campaign-history",
                plan_id="plan-history",
                shard_id="shard-history",
                execution_id="execution-survived",
                worker_id="worker-history",
                mutant_id="m-survived",
            ),
        )
    )
    projections.project(max_events=10)
    catalog = (_mutant("m-killed", 1), _mutant("m-survived", 2), _mutant("m-missing", 3))
    observations = LocalCampaignCoordinator._adaptive_planner_observations(projections, catalog)
    assert observations["m-killed"]["kill_probability"] == 1.0
    assert observations["m-killed"]["duration_seconds"] == 0.1
    assert observations["m-killed"]["duration_p95_seconds"] == 0.1
    assert observations["m-killed"]["duration_sample_count"] == 1.0
    assert observations["m-survived"]["kill_probability"] == 0.0
    assert observations["m-survived"]["duration_seconds"] == 0.2
    assert "m-missing" not in observations
    assert set(projections.get_many("mutant", ("m-killed", "m-survived"))) == {
        "m-killed",
        "m-survived",
    }


def test_knowledge_decision_supplies_history_before_campaign_statistics_exist(tmp_path: Path) -> None:
    # Use the frozen Knowledge Plane result identity when the new campaign statistics store is empty.
    event_store = StatisticsEventStore(tmp_path / "empty-statistics.sqlite3")
    projections = StatisticsProjectionStore(event_store)
    decisions = {
        "m-survived": SimpleNamespace(result_status="survived"),
        "m-killed": SimpleNamespace(result_status="killed"),
    }
    catalog = (_mutant("m-survived", 1), _mutant("m-killed", 2))
    observations = LocalCampaignCoordinator._adaptive_planner_observations(
        projections,
        catalog,
        decisions,
    )
    assert observations["m-survived"]["survived_count"] == 1.0
    assert observations["m-survived"]["kill_probability"] == 0.0
    assert observations["m-killed"]["killed_count"] == 1.0
    assert observations["m-killed"]["kill_probability"] == 1.0


def test_execute_timeout_uses_estimate_without_dropping_retry_floor(tmp_path: Path) -> None:
    # Shorten a large shard timeout while preserving one mutant's full retry and cleanup allowance.
    configuration = _configuration(tmp_path, max_test_seconds=10.0)
    legacy = LocalCampaignCoordinator._engine_command_timeouts(configuration, 10)["execute-shard"]
    adaptive = LocalCampaignCoordinator._engine_command_timeouts(
        configuration,
        10,
        estimated_shard_seconds=2.0,
        timeout_multiplier=2.0,
    )["execute-shard"]
    assert legacy == 330.0
    assert adaptive == 60.0


def test_resume_uses_frozen_adaptive_snapshot_after_history_changes(tmp_path: Path) -> None:
    # Validate resume from persisted adaptive evidence without consulting a changed history snapshot.
    mutant = _mutant("m1", 1)
    configuration = _configuration(tmp_path, exploration_rate=0.25, adaptive_seed="resume-seed")
    prepared = _prepared((mutant,))
    reports = tmp_path / "reports"
    reports.mkdir()
    first = CampaignPlanner().build(
        configuration,
        prepared,
        adaptive_observations={"m1": {"survived_count": 3, "sample_count": 3}},
    )
    (reports / "campaign.plan.json").write_text(
        json.dumps(first.to_dict(), ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    resumed = LocalCampaignCoordinator._load_or_build_campaign_plan(
        configuration,
        prepared,
        reports,
        mutants=(mutant,),
        reuse_decisions={},
        audit_policy=None,
        adaptive_observations={"m1": {"killed_count": 100, "sample_count": 100}},
    )
    assert resumed == first
    assert resumed.adaptive_snapshot["m1"]["survived_count"] == 3.0


def test_corrupt_cost_history_remains_a_non_authoritative_hint(tmp_path: Path) -> None:
    mutant = _mutant("m-corrupt", 1)
    configuration = _configuration(tmp_path)
    plan = CampaignPlanner().build(
        configuration,
        _prepared((mutant,)),
        cost_observations={"m-corrupt": "not-a-number"},
        adaptive_observations={
            "m-corrupt": {
                "killed_count": "broken",
                "survived_count": 2,
                "sample_count": "broken",
                "duration_seconds": "broken",
                "duration_p95_seconds": "nan",
                "duration_sample_count": "broken",
            }
        },
    )
    assert plan.selected_count == 1
    assert plan.estimated_wall_seconds > 0.0
    assert plan.adaptive_snapshot["m-corrupt"]["survived_count"] == 2.0
    assert plan.adaptive_snapshot["m-corrupt"]["duration_seconds"] == 0.0
    assert plan.adaptive_snapshot["m-corrupt"]["duration_p95_seconds"] == 0.0


def test_cost_history_is_bulk_runtime_filtered_and_non_authoritative(tmp_path: Path) -> None:
    # Keep physical cost history useful for planning while preserving semantic evidence authority.
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    store.ingest_effect(
        effect_id="effect-cost-1",
        campaign_id="campaign-cost",
        effect_type="mutation.execute_shard",
        project_id="project-e19",
        revision_id="revision-e19",
        environment_id="environment-e19",
        payload={
            "executions": [
                {
                    "execution_id": "execution-cost-1",
                    "mutant_id": "m-cost",
                    "attempt": 0,
                    "status": "complete",
                    "semantic_result": "killed",
                    "restore_verified": True,
                    "evidence_schema_version": 2,
                    "source_kind": "observed",
                    "duration_seconds": 2.0,
                    "selected_tests": ["test_one", "test_two"],
                    "performance_hint": {
                        "runtime_class": "runtime-a",
                        "authoritative": False,
                    },
                    "test_observations": [
                        {
                            "test_id": "test_one",
                            "test_fingerprint": "fingerprint-one",
                            "duration_ms": 100.0,
                            "outcome": "failed",
                            "evidence_kind": "pytest_test_event",
                        },
                        {
                            "test_id": "test_two",
                            "test_fingerprint": "fingerprint-two",
                            "duration_ms": 200.0,
                            "outcome": "passed",
                            "evidence_kind": "pytest_test_event",
                        },
                    ],
                }
            ]
        },
    )
    history = store.query_cost_observations(
        ("m-cost", "m-missing"),
        project_id="project-e19",
        runtime_class="runtime-a",
    )
    assert set(history) == {"m-cost"}
    assert history["m-cost"]["duration_seconds"] == 2.0
    assert history["m-cost"]["duration_p95_seconds"] == 2.0
    assert history["m-cost"]["test_subset_cost_seconds"] == pytest.approx(0.3)
    assert history["m-cost"]["test_subset_count"] == 2.0
    assert history["m-cost"]["runtime_class"] == "runtime-a"
    projections = StatisticsProjectionStore(StatisticsEventStore(tmp_path / "statistics.sqlite3"))
    observations = LocalCampaignCoordinator._adaptive_planner_observations(
        projections,
        (_mutant("m-cost", 1),),
        knowledge=store,
        project_id="project-e19",
        runtime_class="runtime-a",
    )
    assert observations["m-cost"]["duration_seconds"] == 2.0
    assert observations["m-cost"]["test_subset_cost_seconds"] == pytest.approx(0.3)
    assert observations["m-cost"]["killed_count"] == 0.0
    plan = CampaignPlanner().build(
        _configuration(tmp_path),
        _prepared((_mutant("m-cost", 1),)),
        adaptive_observations=observations,
    )
    assert plan.selected_count == 1
    store.close()
