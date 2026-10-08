import json
import sys
from pathlib import Path
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
    TestCommandDescriptor as CommandDescriptor,
)
from theseus_local import LocalCampaignCoordinator
from theseus_planner import CampaignPlanner
def _configuration(root: Path, *, max_mutants: int | None = None, max_seconds: float | None = None) -> CampaignConfiguration:
    # Build a small public campaign request without importing runner implementation details.
    return CampaignConfiguration(
        campaign_id=CampaignId("cmp_e17_planner"),
        project=ProjectDescriptor(
            project_id=ProjectId("project_e17_planner"),
            display_name="E17 planner fixture",
            root_path=str(root),
            test_command=CommandDescriptor((sys.executable, "-c", "from app import choose; assert choose(1) == 1")),
        ),
        scope=MutationScope(source_path="app.py", function="choose", operators=("condition_to_not",)),
        budget=CampaignBudget(max_mutants=max_mutants, max_seconds=max_seconds, max_workers=2),
        no_escalation=True,
        reports_dir=str(root / "reports"),
    )
def _prepared(root: Path, mutants: tuple[MutantDescriptor, ...]) -> PreparedCampaign:
    # Provide the immutable preparation boundary consumed by CampaignPlanner.
    return PreparedCampaign(
        campaign_id=CampaignId("cmp_e17_planner"),
        source_path="app.py",
        source_sha256="source-sha",
        index_version="index-v1",
        mutants=mutants,
        snapshot_id="prepared-v1",
    )
def _mutant(mutant_id: str, line_no: int, *, function_id: str = "choose", mutation: str = "condition_to_not") -> MutantDescriptor:
    # Create one deterministic descriptor with only planner-relevant fields varied.
    return MutantDescriptor(
        mutant_id=MutantId(mutant_id),
        mutation=mutation,
        source_path="app.py",
        line_no=line_no,
        column_no=4,
        original="if value > 0:",
        replacement="if not (value > 0):",
        function_id=function_id,
    )
def test_planner_is_deterministic_and_emits_balanced_immutable_shards(tmp_path: Path) -> None:
    # Prove input iteration order cannot change the selected mutants or shard membership.
    configuration = _configuration(tmp_path, max_mutants=3)
    catalog = (_mutant("m3", 30), _mutant("m1", 10), _mutant("m2", 20), _mutant("outside", 40, function_id="other"))
    planner = CampaignPlanner()
    first = planner.build(configuration, _prepared(tmp_path, catalog), mutants=catalog)
    second = planner.build(configuration, _prepared(tmp_path, tuple(reversed(catalog))), mutants=tuple(reversed(catalog)))
    assert first.plan_id == second.plan_id
    assert [item.mutant.mutant_id.value for item in first.selected] == ["m1", "m2", "m3"]
    assert [list(shard.mutant_ids) for shard in first.shards] == [["m1", "m3"], ["m2"]]
    assert first.selected_count == 3
    assert first.eligible_count == 3
    assert any(item["mutant_id"] == "outside" for item in first.excluded)
    assert first.from_dict(first.to_dict()) == first
def test_planner_applies_seconds_budget_and_explains_skips(tmp_path: Path) -> None:
    # Make the finite wall-clock budget a deterministic prefix with explicit exclusion reasons.
    configuration = _configuration(tmp_path, max_seconds=2.0)
    catalog = (_mutant("m1", 10), _mutant("m2", 20), _mutant("m3", 30))
    plan = CampaignPlanner().build(configuration, _prepared(tmp_path, catalog))
    assert plan.selected_count == 2
    assert plan.estimated_cpu_seconds == 2.5
    assert plan.estimated_wall_seconds == 1.25
    assert plan.estimated_seconds == plan.estimated_wall_seconds
    assert plan.cost_model["cpu_seconds"] == plan.estimated_cpu_seconds
    assert plan.cost_model["wall_seconds"] == plan.estimated_wall_seconds
    assert [item["mutant_id"] for item in plan.excluded] == ["m3"]
    assert plan.excluded[0]["reason"] == "budget:max-seconds"
def test_local_coordinator_persists_and_reuses_the_campaign_plan(tmp_path: Path) -> None:
    # Verify E17 plan identity crosses preparation, Gallifrey state, canonical report and resume.
    (tmp_path / "app.py").write_text(
        "def choose(value):\n"
        "    if value > 0:\n"
        "        return 1\n"
        "    if value == 0:\n"
        "        return 2\n"
        "    return 0\n",
        encoding="utf-8",
    )
    configuration = _configuration(tmp_path, max_mutants=1)
    first = LocalCampaignCoordinator().run(configuration)
    assert first.succeeded
    plan_path = first.database_path.parent / "campaign.plan.json"
    plan_bytes = plan_path.read_bytes()
    plan = json.loads(plan_bytes)
    assert plan["selected_count"] == 1
    assert first.campaign.plan_id == plan["plan_id"]
    assert first.engine_result is not None
    assert first.engine_result.report["plan_id"] == plan["plan_id"]
    second = LocalCampaignCoordinator().run(configuration)
    assert second.succeeded
    assert plan_path.read_bytes() == plan_bytes
    assert second.campaign.plan_id == first.campaign.plan_id
