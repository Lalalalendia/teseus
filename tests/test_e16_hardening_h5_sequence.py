from __future__ import annotations
import json
import sys
import time
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
def _configuration(root: Path, campaign_id: str, reuse_mode: str) -> CampaignConfiguration:
    # Build a deterministic four-campaign sequence over one durable project history.
    return CampaignConfiguration(
        campaign_id=CampaignId(campaign_id),
        project=ProjectDescriptor(
            project_id=ProjectId("project_e16_sequence"),
            display_name="E16 reuse sequence fixture",
            root_path=str(root),
            test_command=TestCommandDescriptor((sys.executable, "-m", "pytest", "-q")),
            pytest_plugin_autoload=False,
        ),
        scope=MutationScope(source_path="app.py", function="choose", operators=("condition_to_not",)),
        budget=CampaignBudget(max_mutants=1, max_workers=1),
        no_escalation=True,
        reports_dir=str(root / "reports"),
        reuse_mode=reuse_mode,
    )
def test_reuse_sequence_finishes_after_hint_exact_partial_and_fresh_campaigns(tmp_path: Path) -> None:
    # Exercise the order-dependent path with bounded process cleanup after every campaign boundary.
    (tmp_path / "app.py").write_text(
        "def choose(value):\n    if value > 0:\n        return 1\n    return 0\n",
        encoding="utf-8",
    )
    test_file = tmp_path / "test_app.py"
    test_file.write_text(
        "from app import choose\n\n"
        "def test_positive():\n    assert choose(1) == 1\n\n"
        "def test_negative():\n    assert choose(-1) == 0\n",
        encoding="utf-8",
    )
    coordinator = LocalCampaignCoordinator()
    started = time.monotonic()
    first = coordinator.run(_configuration(tmp_path, "cmp_e16_sequence_hint", "hint"))
    second = coordinator.run(_configuration(tmp_path, "cmp_e16_sequence_exact", "experimental"))
    test_file.write_text(
        "from app import choose\n\n"
        "def test_positive():\n    assert choose(1) == 1\n\n"
        "def test_negative():\n    assert choose(-1) == int(0)\n",
        encoding="utf-8",
    )
    third_configuration = _configuration(tmp_path, "cmp_e16_sequence_partial", "experimental")
    third = coordinator.run(third_configuration)
    (tmp_path / "app.py").write_text(
        "def choose(value):\n    if value >= 0:\n        return 1\n    return 0\n",
        encoding="utf-8",
    )
    fourth = coordinator.run(_configuration(tmp_path, "cmp_e16_sequence_fresh", "hint"))
    assert first.succeeded, first.error
    assert second.succeeded, second.error
    assert third.succeeded, third.error
    assert fourth.succeeded, fourth.error
    assert time.monotonic() - started < 90.0
    reports_root = WorkspaceProvider(third_configuration).reports_root
    plan = json.loads(
        (reports_root / "cmp_e16_sequence_partial" / "reuse.plan.json").read_text(encoding="utf-8")
    )
    assert [item["kind"] for item in plan["decisions"]] == ["partial"], plan["decisions"]