from __future__ import annotations
import inspect
import json
import sys
from dataclasses import replace
from pathlib import Path
import pytest
from gallifrey_mutation import MutationShard, SQLiteMutationStore
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    CampaignPlan,
    MutantDescriptor,
    MutantId,
    MutationScope,
    PreparedCampaign,
    ProjectDescriptor,
    ProjectId,
    ShardDescriptor,
    ShardId,
    TestCommandDescriptor,
)
from theseus_local import LocalCampaignCoordinator
from theseus_local.startup_recovery import _load_campaign_plan_authority
from theseus_planner import CampaignPlanner
def _configuration(root: Path, campaign_id: str = "cmp_e17_authority") -> CampaignConfiguration:
    # Build one deterministic campaign whose configured width exceeds its selected topology.
    return CampaignConfiguration(
        campaign_id=CampaignId(campaign_id),
        project=ProjectDescriptor(
            project_id=ProjectId("project_e17_authority"),
            display_name="E17 planner authority fixture",
            root_path=str(root),
            test_command=TestCommandDescriptor((sys.executable, "-m", "pytest", "-q", "test_app.py")),
        ),
        scope=MutationScope(source_path="app.py", function="choose", operators=("condition_to_not",)),
        budget=CampaignBudget(max_mutants=3, max_workers=8),
        no_escalation=True,
        reports_dir=str(root / "reports"),
    )
def _mutant(mutant_id: str, line_no: int) -> MutantDescriptor:
    # Build one planner candidate with stable source ordering.
    return MutantDescriptor(
        mutant_id=MutantId(mutant_id),
        mutation="condition_to_not",
        source_path="app.py",
        line_no=line_no,
        column_no=4,
        original="if value > 0:",
        replacement="if not (value > 0):",
        function_id="choose",
    )
def _prepared(root: Path, mutants: tuple[MutantDescriptor, ...]) -> PreparedCampaign:
    # Bind the catalog to the immutable preparation identity required by PR17.
    return PreparedCampaign(
        campaign_id=CampaignId("cmp_e17_authority"),
        source_path="app.py",
        source_sha256="source-authority",
        index_version="index-v1",
        mutants=mutants,
        snapshot_id="prepared-authority-v1",
    )
def _plan(root: Path) -> tuple[CampaignPlan, PreparedCampaign, tuple[MutantDescriptor, ...]]:
    # Produce one complete plan used by the pure contract tests.
    catalog = (_mutant("m3", 30), _mutant("m1", 10), _mutant("m2", 20))
    prepared = _prepared(root, catalog)
    plan = CampaignPlanner().build(_configuration(root), prepared, mutants=catalog)
    return plan, prepared, catalog
def test_planner_owns_worker_width_ordering_and_shard_identity(tmp_path: Path) -> None:
    # Prove configured capacity is reduced to immutable planner topology and bound into every shard.
    plan, _, _ = _plan(tmp_path)
    assert plan.worker_count == 3
    assert plan.worker_count == len(plan.shards)
    assert all(item.plan_id == plan.plan_id for item in plan.shards)
    ranks = {item.mutant.mutant_id.value: item.rank for item in plan.selected}
    assert tuple(item.rank for item in plan.selected) == (0, 1, 2)
    assert all(
        descriptor.mutant_ids == tuple(sorted(descriptor.mutant_ids, key=ranks.__getitem__))
        for descriptor in plan.shards
    )
    assert CampaignPlan.from_dict(plan.to_dict()) == plan
def test_campaign_plan_rejects_foreign_shard_authority_and_unknown_membership(tmp_path: Path) -> None:
    # Reject both a shard rebound to another plan and a topology containing an unknown mutant.
    plan, _, _ = _plan(tmp_path)
    foreign = replace(plan.shards[0], plan_id="plan-foreign")
    with pytest.raises(ValueError, match="owning plan_id"):
        replace(plan, shards=(foreign, *plan.shards[1:]), artifact_sha256="")
    unknown = replace(plan.shards[0], mutant_ids=("unknown-mutant",))
    with pytest.raises(ValueError, match="cover selected mutants exactly once"):
        replace(plan, shards=(unknown, *plan.shards[1:]), artifact_sha256="")
def test_mutation_shard_roundtrip_preserves_immutable_plan_topology(tmp_path: Path) -> None:
    # Persist and restore the domain shard without losing its plan, ordinal, membership or cost identity.
    plan, _, _ = _plan(tmp_path)
    descriptor = plan.shards[0]
    shard = MutationShard.from_descriptor(plan.campaign_id, descriptor, ordinal=0)
    restored = MutationShard.from_dict(shard.to_dict())
    assert restored == shard
    assert restored.matches_plan_descriptor(plan.plan_id, descriptor, ordinal=0)
    assert not restored.matches_plan_descriptor(plan.plan_id, replace(descriptor, mutant_ids=("m2",)), ordinal=0)


def test_domain_shard_allows_unbound_fixture_without_granting_plan_authority() -> None:
    # Preserve isolated lease tests while ensuring an unbound shard cannot match a runtime plan.
    descriptor = ShardDescriptor(ShardId("shard-isolated"), ("mutant-isolated",))
    shard = MutationShard.from_descriptor(CampaignId("campaign-isolated"), descriptor, ordinal=0)
    assert shard.plan_id == ""
    assert not shard.matches_plan_descriptor("plan-runtime", descriptor, ordinal=0)
def test_coordinator_authority_rejects_snapshot_and_catalog_drift(tmp_path: Path) -> None:
    # Fail closed when orchestration receives another prepared snapshot or a catalog missing a selected mutant.
    plan, prepared, catalog = _plan(tmp_path)
    assert LocalCampaignCoordinator._validate_campaign_plan_authority(plan, prepared, catalog) is plan
    with pytest.raises(RuntimeError, match="prepared_snapshot_id"):
        LocalCampaignCoordinator._validate_campaign_plan_authority(
            plan,
            replace(prepared, snapshot_id="prepared-authority-v2"),
            catalog,
        )
    with pytest.raises(RuntimeError, match="unknown mutant_id"):
        LocalCampaignCoordinator._validate_campaign_plan_authority(plan, prepared, catalog[1:])
def test_coordinator_consumes_plan_topology_without_budget_resharding() -> None:
    # Lock the architecture so runtime execution cannot regain an independent topology algorithm.
    source = inspect.getsource(LocalCampaignCoordinator._execute_parallel_shards)
    assert "shard_descriptors = campaign_plan.shards" in source
    assert "campaign_plan.worker_count" in source
    assert "budget.max_workers" not in source
    assert "CampaignPlanner().build" not in source
def test_local_coordinator_materializes_and_reuses_exact_plan_topology(tmp_path: Path) -> None:
    # Run and resume one campaign while proving SQLite shard topology remains byte-for-byte planner-owned.
    (tmp_path / "app.py").write_text(
        "def choose(value):\n"
        "    if value > 0:\n"
        "        return 1\n"
        "    if value == 0:\n"
        "        return 2\n"
        "    return 0\n",
        encoding="utf-8",
    )
    (tmp_path / "test_app.py").write_text(
        "from app import choose\n\n"
        "def test_positive():\n"
        "    assert choose(1) == 1\n\n"
        "def test_zero():\n"
        "    assert choose(0) == 2\n",
        encoding="utf-8",
    )
    configuration = _configuration(tmp_path, "cmp_e17_authority_runtime")
    first = LocalCampaignCoordinator().run(configuration)
    assert first.succeeded, first.error
    plan_path = first.database_path.parent / "campaign.plan.json"
    plan = CampaignPlan.from_dict(json.loads(plan_path.read_text(encoding="utf-8")))
    store = SQLiteMutationStore(first.database_path)
    try:
        campaign_outcome = store.get_campaign(configuration.campaign_id)
        shard_outcome = store.list_shards(configuration.campaign_id)
        campaign = campaign_outcome.value
        shards = shard_outcome.value
        assert campaign is not None
        authority_plan, descriptors = _load_campaign_plan_authority(first.database_path, campaign)
        topology_before = tuple(
            (item.plan_id, item.shard_id.value, item.ordinal, tuple(mutant.value for mutant in item.mutant_ids), item.estimated_cost)
            for item in shards
        )
    finally:
        store.close()
    assert authority_plan.plan_id == plan.plan_id
    assert first.engine_result is not None
    assert first.engine_result.summary.workers == plan.worker_count
    assert set(descriptors) == {item.shard_id.value for item in plan.shards}
    second = LocalCampaignCoordinator().run(configuration)
    assert second.succeeded, second.error
    store = SQLiteMutationStore(second.database_path)
    try:
        shards_after = store.list_shards(configuration.campaign_id).value
        topology_after = tuple(
            (item.plan_id, item.shard_id.value, item.ordinal, tuple(mutant.value for mutant in item.mutant_ids), item.estimated_cost)
            for item in shards_after
        )
    finally:
        store.close()
    assert topology_after == topology_before
