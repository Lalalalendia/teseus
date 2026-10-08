from __future__ import annotations
import hashlib
import json
from dataclasses import replace
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
from theseus_planner import CampaignPlanner
def _mutant(mutant_id: str, line: int) -> MutantDescriptor:
    # Create one weighted condition mutant for the explicit plan-cost contract.
    return MutantDescriptor(MutantId(mutant_id), "condition_to_not", "app.py", line, 1, "old", "new")
def _configuration(root: Path) -> CampaignConfiguration:
    # Build a two-worker planner request without execution-layer dependencies.
    return CampaignConfiguration(
        campaign_id=CampaignId("cmp_e17_cost_contract"),
        project=ProjectDescriptor(ProjectId("project_e17_cost_contract"), "E17 cost", str(root)),
        scope=MutationScope("app.py", scope_kind="project"),
        budget=CampaignBudget(max_workers=2),
    )
def _prepared(mutants: tuple[MutantDescriptor, ...]) -> PreparedCampaign:
    # Freeze the smallest prepared catalog required by CampaignPlanner.
    return PreparedCampaign(
        CampaignId("cmp_e17_cost_contract"),
        "app.py",
        "source",
        "idx",
        mutants,
        snapshot_id="snap",
    )
def test_campaign_plan_exposes_explicit_wall_and_cpu_estimates(tmp_path: Path) -> None:
    # Keep elapsed duration, aggregate work and the deprecated alias semantically distinct.
    mutants = (_mutant("m1", 1), _mutant("m2", 2))
    plan = CampaignPlanner().build(_configuration(tmp_path), _prepared(mutants), mutants=mutants)
    assert plan.estimated_wall_seconds == 1.25
    assert plan.estimated_cpu_seconds == 2.5
    assert plan.estimated_seconds == plan.estimated_wall_seconds
    assert plan.cost_model["wall_seconds"] == plan.estimated_wall_seconds
    assert plan.cost_model["cpu_seconds"] == plan.estimated_cpu_seconds
    assert plan.to_dict()["estimated_seconds"] == plan.estimated_wall_seconds
def test_legacy_estimated_seconds_is_verified_and_migrated(tmp_path: Path) -> None:
    # Verify the old serialized shape before migrating its ambiguous field into explicit dimensions.
    mutants = (_mutant("m1", 1), _mutant("m2", 2))
    plan = CampaignPlanner().build(_configuration(tmp_path), _prepared(mutants), mutants=mutants)
    legacy = plan.to_dict()
    legacy.pop("estimated_wall_seconds")
    legacy.pop("estimated_cpu_seconds")
    legacy["estimated_seconds"] = plan.estimated_cpu_seconds
    legacy.pop("artifact_sha256")
    encoded = json.dumps(
        CampaignPlan._canonical_json(legacy),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    legacy["artifact_sha256"] = hashlib.sha256(encoded).hexdigest()
    migrated = CampaignPlan.from_dict(legacy)
    assert migrated.estimated_wall_seconds == legacy["cost_model"]["wall_seconds"]
    assert migrated.estimated_cpu_seconds == legacy["cost_model"]["cpu_seconds"]
    assert migrated.estimated_seconds == migrated.estimated_wall_seconds
    migrated.verify_integrity()
def test_campaign_plan_rejects_divergent_explicit_cost_dimensions(tmp_path: Path) -> None:
    # Fail closed when a caller tries to detach explicit metrics from the immutable cost model.
    mutants = (_mutant("m1", 1),)
    plan = CampaignPlanner().build(_configuration(tmp_path), _prepared(mutants), mutants=mutants)
    with pytest.raises(ValueError, match="estimated_cpu_seconds"):
        replace(plan, estimated_cpu_seconds=plan.estimated_cpu_seconds + 1.0, artifact_sha256="")
