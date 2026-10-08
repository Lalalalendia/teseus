from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pytest

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
from test_intelligence_unified_v1.engine import _config_from_contract


def _tree_digest(root: Path) -> dict[str, str]:
    # Capture the main checkout image so a mutation run can be proven non-destructive.
    rows: dict[str, str] = {}
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix()
        rows[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return rows


def _configuration(root: Path) -> CampaignConfiguration:
    # Build a minimal coordinator request whose legacy report path is intentionally inside the checkout.
    return CampaignConfiguration(
        campaign_id=CampaignId("cmp_h2_workspace"),
        project=ProjectDescriptor(
            project_id=ProjectId("project_h2_workspace"),
            display_name="H2 workspace fixture",
            root_path=str(root),
            test_command=TestCommandDescriptor(
                (sys.executable, "-c", "from app import choose; assert choose(1) == 1")
            ),
        ),
        scope=MutationScope(source_path="app.py", function="choose", operators=("condition_to_not",)),
        budget=CampaignBudget(max_mutants=1, max_workers=1),
        no_escalation=True,
        reports_dir=str(root / "reports"),
    )


def test_h2_workspace_and_state_are_outside_main_checkout(tmp_path: Path) -> None:
    # Keep the complete local vertical slice from creating control files in the registered checkout.
    source = tmp_path / "app.py"
    source_text = "def choose(value):\n    if value > 0:\n        return 1\n    return 0\n"
    source.write_text(source_text, encoding="utf-8")
    configuration = _configuration(tmp_path)
    before = _tree_digest(tmp_path)
    provider = WorkspaceProvider(configuration)
    handle = provider.prepare(configuration.campaign_id.value)
    runtime = handle.runtime_configuration(configuration)

    assert handle.workspace_root != handle.main_root
    assert handle.main_root not in handle.workspace_root.parents
    assert handle.reports_root != tmp_path / "reports"
    assert handle.reports_root != tmp_path
    assert Path(runtime.project.root_path) == handle.workspace_root
    assert Path(runtime.project.main_root_path or "") == handle.main_root

    result = LocalCampaignCoordinator().run(configuration)

    assert result.succeeded
    assert _tree_digest(tmp_path) == before
    assert handle.main_root not in result.database_path.parents
    assert handle.reports_root in result.database_path.parents
    assert not (tmp_path / ".theseus").exists()
    assert not (tmp_path / "reports").exists()


def test_h2_engine_rejects_main_checkout_as_registered_workspace(tmp_path: Path) -> None:
    # Fail closed when a caller explicitly labels the same directory as both workspace and main checkout.
    configuration = _configuration(tmp_path)
    invalid = ProjectDescriptor(
        project_id=configuration.project.project_id,
        display_name=configuration.project.display_name,
        root_path=str(tmp_path),
        main_root_path=str(tmp_path),
    )
    invalid_configuration = CampaignConfiguration(
        campaign_id=configuration.campaign_id,
        project=invalid,
        scope=configuration.scope,
        budget=configuration.budget,
    )

    with pytest.raises(ValueError, match="outside the registered main checkout"):
        _config_from_contract(invalid_configuration)
