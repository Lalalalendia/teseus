from __future__ import annotations
import asyncio
import hashlib
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
    TestCommandDescriptor,
)
from theseus_local import LocalCampaignCoordinator
from theseus_local.workspace import WorkspaceProvider
from gallifrey_mutation import CampaignState
def _tree_digest(root: Path) -> dict[str, str]:
    # Capture the checkout before a campaign so mutation execution can be proven non-destructive.
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(item for item in root.rglob("*") if item.is_file())
    }
def _write_project(root: Path, *, conditions: int = 2) -> None:
    # Create a tiny deterministic project with the requested number of mutation sites.
    branches = [
        "    if value > 0:\n        result += 1\n",
        "    if value == 0:\n        result += 2\n",
        "    if value < 0:\n        result += 3\n",
    ][:conditions]
    body = "def choose(value):\n    result = 0\n" + "".join(branches) + "    return result\n"
    (root / "app.py").write_text(body, encoding="utf-8")
    tests = ["from app import choose\n\n", "def test_positive():\n    assert choose(1) == 1\n\n"]
    if conditions >= 2:
        tests.append("def test_zero():\n    assert choose(0) == 2\n\n")
    if conditions >= 3:
        tests.append("def test_negative():\n    assert choose(-1) == 3\n")
    (root / "test_app.py").write_text("".join(tests), encoding="utf-8")
def _configuration(
    root: Path,
    campaign_id: str,
    *,
    max_mutants: int = 2,
    max_workers: int = 1,
    reuse_mode: str = "hint",
) -> CampaignConfiguration:
    # Build one local coordinator request with an explicit safe test command and scope.
    return CampaignConfiguration(
        campaign_id=CampaignId(campaign_id),
        project=ProjectDescriptor(
            project_id=ProjectId("project-expected-behavior"),
            display_name="Expected behavior fixture",
            root_path=str(root),
            test_command=TestCommandDescriptor(
                (sys.executable, "-m", "pytest", "-q", "test_app.py")
            ),
        ),
        scope=MutationScope(source_path="app.py", function="choose", operators=("condition_to_not",)),
        budget=CampaignBudget(max_mutants=max_mutants, max_workers=max_workers),
        no_escalation=True,
        reports_dir=str(root / "reports"),
        reuse_mode=reuse_mode,
    )
def test_local_campaign_completes_and_materializes_authoritative_artifacts(tmp_path: Path) -> None:
    # Verify the complete local vertical slice, including restoration and canonical report materialization.
    _write_project(tmp_path, conditions=2)
    configuration = _configuration(tmp_path, "campaign-expected-complete", max_mutants=2, max_workers=2)
    before = _tree_digest(tmp_path)
    result = LocalCampaignCoordinator().run(configuration)
    report_root = WorkspaceProvider(configuration).reports_root / configuration.campaign_id.value
    assert result.succeeded, result.error
    assert result.campaign.status is CampaignState.COMPLETED
    assert result.campaign.completed_mutants == result.campaign.total_mutants
    assert result.engine_result is not None
    assert _tree_digest(tmp_path) == before
    assert (report_root / "canonical.report.json").is_file()
    assert (report_root / "reuse.plan.json").is_file()
    assert (report_root / "reuse.audit.json").is_file()
    assert result.database_path.is_file()
    canonical = json.loads((report_root / "canonical.report.json").read_text(encoding="utf-8"))
    assert canonical["source"] == "gallifrey_authoritative"
    assert canonical["completed_mutants"] == result.campaign.completed_mutants
def test_async_coordinator_has_the_same_terminal_success_contract(tmp_path: Path) -> None:
    # Keep the async application boundary semantically equivalent to the synchronous coordinator.
    _write_project(tmp_path, conditions=1)
    configuration = _configuration(tmp_path, "campaign-expected-async", max_mutants=1)
    result = asyncio.run(LocalCampaignCoordinator().run_async(configuration))
    assert result.succeeded, result.error
    assert result.campaign.status is CampaignState.COMPLETED
    assert result.engine_result is not None
def test_mutant_budget_limits_discovery_without_breaking_completion(tmp_path: Path) -> None:
    # Apply max_mutants as a hard selection boundary while preserving a complete campaign lifecycle.
    _write_project(tmp_path, conditions=3)
    configuration = _configuration(tmp_path, "campaign-expected-budget", max_mutants=1)
    result = LocalCampaignCoordinator().run(configuration)
    assert result.succeeded, result.error
    assert result.campaign.total_mutants == 1
    assert result.campaign.completed_mutants == 1
def test_default_reuse_mode_reports_evidence_but_runs_a_fresh_engine_shard(tmp_path: Path) -> None:
    # Preserve the safe hint-only default: historical evidence informs the plan but cannot skip execution.
    _write_project(tmp_path, conditions=1)
    first_configuration = _configuration(tmp_path, "campaign-hint-first", max_mutants=1)
    second_configuration = _configuration(tmp_path, "campaign-hint-second", max_mutants=1)
    first = LocalCampaignCoordinator().run(first_configuration)
    second = LocalCampaignCoordinator().run(second_configuration)
    report_root = WorkspaceProvider(second_configuration).reports_root / second_configuration.campaign_id.value
    assert first.succeeded, first.error
    assert second.succeeded, second.error
    plan = json.loads((report_root / "reuse.plan.json").read_text(encoding="utf-8"))
    assert plan["decisions"][0]["kind"] == "exact"
    assert plan["decisions"][0]["eligible"] is True
    assert plan["decisions"][0]["authorized"] is False
    engine_root = (
        WorkspaceProvider(second_configuration).reports_root
        / "engine"
        / second_configuration.campaign_id.value
    )
    assert list(engine_root.glob("engine-shard-*.json"))
def test_cancel_marker_is_external_and_is_removed_only_by_explicit_cleanup(tmp_path: Path) -> None:
    # Keep cancellation state outside the checkout and make its lifecycle explicitly disposable.
    _write_project(tmp_path, conditions=1)
    configuration = _configuration(tmp_path, "campaign-expected-cancel", max_mutants=1)
    marker = LocalCampaignCoordinator.request_cancel(configuration)
    assert marker.is_file()
    assert not (tmp_path / ".theseus").exists()
    LocalCampaignCoordinator.clear_cancel(configuration)
    assert not marker.exists()
