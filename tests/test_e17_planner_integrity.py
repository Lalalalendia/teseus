from pathlib import Path
import pytest
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
)
from theseus_knowledge import ReuseAuditPolicy
from theseus_planner import CampaignPlanner, PlanError
def _prepared(root: Path, mutants: tuple[MutantDescriptor, ...]) -> PreparedCampaign:
    # Build the smallest immutable preparation boundary for planner-only tests.
    return PreparedCampaign(CampaignId("cmp_e17_integrity"), "app.py", "source", "idx", mutants, snapshot_id="snap")
def _configuration(root: Path, scope: MutationScope, *, workers: int = 2) -> CampaignConfiguration:
    # Keep physical checkout paths out of semantic planner assertions.
    return CampaignConfiguration(
        campaign_id=CampaignId("cmp_e17_integrity"),
        project=ProjectDescriptor(ProjectId("project_e17"), "E17", str(root)),
        scope=scope,
        budget=CampaignBudget(max_workers=workers),
        reuse_mode="experimental",
    )
def _mutant(mutant_id: str, line: int, operator: str = "condition_to_not") -> MutantDescriptor:
    # Create a deterministic mutation descriptor with a controllable cost class.
    return MutantDescriptor(MutantId(mutant_id), operator, "app.py", line, 1, "old", "new")
def test_plan_round_trip_rejects_tampering_and_is_deterministic(tmp_path: Path) -> None:
    # Prove the persisted artifact cannot change action while retaining its old identity.
    mutants = (_mutant("m1", 1), _mutant("m2", 2))
    plan = CampaignPlanner().build(_configuration(tmp_path, MutationScope("app.py", scope_kind="project")), _prepared(tmp_path, mutants), mutants=mutants)
    raw = plan.to_dict()
    raw["decisions"][0]["action"] = "reuse"
    with pytest.raises(ValueError, match="integrity"):
        CampaignPlan.from_dict(raw)
    moved = _configuration(tmp_path / "moved", MutationScope("app.py", scope_kind="project"))
    assert plan.plan_id == CampaignPlanner().build(moved, _prepared(tmp_path / "moved", mutants), mutants=mutants).plan_id
def test_scope_and_empty_plan_fail_closed(tmp_path: Path) -> None:
    # Reject ambiguous scope requests and never materialize a lifecycle shard for no work.
    mutants = (_mutant("m1", 1),)
    with pytest.raises(PlanError, match="unknown mutant IDs"):
        CampaignPlanner().build(_configuration(tmp_path, MutationScope("app.py", scope_kind="explicit", mutant_ids=("missing",))), _prepared(tmp_path, mutants))
    empty = CampaignPlanner().build(_configuration(tmp_path, MutationScope("missing.py", scope_kind="file")), _prepared(tmp_path, mutants))
    assert empty.selected_count == 0
    assert empty.shards == ()
def test_audit_sample_is_planner_owned_and_shards_are_weighted(tmp_path: Path) -> None:
    # Ensure audit selection is frozen in the plan and sharding follows estimated cost, not row count.
    mutants = (_mutant("m1", 1), _mutant("m2", 2), _mutant("m3", 3))
    config = _configuration(tmp_path, MutationScope("app.py", scope_kind="project"), workers=2)
    reuse = {
        item.mutant_id.value: {
            "kind": "exact",
            "eligible": True,
            "audit_required": True,
            "source_event_id": f"event-{item.mutant_id.value}",
        }
        for item in mutants
    }
    policy = ReuseAuditPolicy(exact_sample_rate=1.0)
    plan = CampaignPlanner().build(config, _prepared(tmp_path, mutants), reuse_decisions=reuse, audit_policy=policy, cost_observations={"m1": 10, "m2": 1, "m3": 1})
    assert set(plan.audit_sample) == {"m1", "m2", "m3"}
    assert sorted(shard.estimated_cost for shard in plan.shards) == [2.0, 10.0]
