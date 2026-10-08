from __future__ import annotations
import json
import sys
from pathlib import Path
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    MutationScope,
    ProjectDescriptor,
    ProjectId,
    TestCommandDescriptor as CommandDescriptor,
)
from theseus_local import LocalCampaignCoordinator
from theseus_local.workspace import WorkspaceProvider
def _configuration(root: Path, campaign_id: str) -> CampaignConfiguration:
    # Build two campaigns that share one project-level knowledge history and exact test command.
    return CampaignConfiguration(
        campaign_id=CampaignId(campaign_id),
        project=ProjectDescriptor(
            project_id=ProjectId("project_e16_reuse"),
            display_name="E16 reuse fixture",
            root_path=str(root),
            test_command=CommandDescriptor((sys.executable, "-m", "pytest", "-q")),
            pytest_plugin_autoload=False,
        ),
        scope=MutationScope(source_path="app.py", function="choose", operators=("condition_to_not",)),
        budget=CampaignBudget(max_mutants=1, max_workers=1),
        no_escalation=True,
        reports_dir=str(root / "reports"),
        reuse_mode="experimental",
    )
def test_local_coordinator_reuses_only_after_project_knowledge_proof(tmp_path: Path) -> None:
    # Verify the second campaign records exact reuse and completes through Gallifrey without rerunning the shard.
    source = tmp_path / "app.py"
    source.write_text("def choose(value):\n    if value > 0:\n        return 1\n    return 0\n", encoding="utf-8")
    (tmp_path / "test_app.py").write_text(
        "from app import choose\n\n\ndef test_choose():\n    assert choose(1) == 1\n",
        encoding="utf-8",
    )
    coordinator = LocalCampaignCoordinator()
    first = coordinator.run(_configuration(tmp_path, "cmp_e16_reuse_first"))
    second = coordinator.run(_configuration(tmp_path, "cmp_e16_reuse_second"))
    assert first.succeeded
    assert second.succeeded
    plan_path = WorkspaceProvider(_configuration(tmp_path, "cmp_e16_reuse_second")).reports_root / "cmp_e16_reuse_second" / "reuse.plan.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    assert [item["kind"] for item in plan["decisions"]] == ["exact"], plan["decisions"]
    assert plan["decisions"][0]["eligible"] is True
    assert second.campaign.completed_mutants == 1
    reports_root = WorkspaceProvider(_configuration(tmp_path, "cmp_e16_reuse_second")).reports_root
    assert not list((reports_root / "engine" / "cmp_e16_reuse_second").glob("engine-shard-*.json"))
    first_report = json.loads(
        (reports_root / "cmp_e16_reuse_first" / "canonical.report.json").read_text(encoding="utf-8")
    )
    second_report = json.loads(
        (reports_root / "cmp_e16_reuse_second" / "canonical.report.json").read_text(encoding="utf-8")
    )
    assert [row["status"] for row in second_report["results"]] == [
        row["status"] for row in first_report["results"]
    ]