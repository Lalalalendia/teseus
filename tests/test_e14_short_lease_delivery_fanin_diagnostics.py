"""End-to-end diagnostic regression for E14 delivery-to-fan-in lease ordering."""
from __future__ import annotations
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
from theseus_local.coordinator import LocalCampaignCoordinator


def test_short_lease_remains_valid_through_durable_delivery_and_atomic_fanin(
    tmp_path: Path,
) -> None:
    # Prove DELIVERING fences ownership so a durably received result cannot expire during fan-in.
    (tmp_path / "app.py").write_text(
        "def choose(value):\n"
        "    if value > 0:\n        return 1\n"
        "    if value == 0:\n        return 2\n"
        "    return 0\n",
        encoding="utf-8",
    )
    (tmp_path / "test_app.py").write_text(
        "import time\n\n"
        "from app import choose\n\n"
        "def test_positive():\n    time.sleep(0.8)\n    assert choose(1) == 1\n\n"
        "def test_zero():\n    time.sleep(0.8)\n    assert choose(0) == 2\n",
        encoding="utf-8",
    )
    configuration = CampaignConfiguration(
        campaign_id=CampaignId("cmp-e14-short-lease-delivery-fanin"),
        project=ProjectDescriptor(
            project_id=ProjectId("project-e14-short-lease-delivery-fanin"),
            display_name="E14 short lease delivery fan-in diagnostic",
            root_path=str(tmp_path),
            test_command=TestCommandDescriptor((sys.executable, "-m", "pytest", "-q")),
        ),
        scope=MutationScope(
            source_path="app.py",
            function="choose",
            operators=("condition_to_not",),
        ),
        budget=CampaignBudget(
            max_mutants=2,
            max_workers=2,
            max_test_seconds=5.0,
            lease_seconds=0.6,
        ),
        no_escalation=True,
        reports_dir=str(tmp_path / "reports"),
    )

    result = LocalCampaignCoordinator().run(configuration)

    assert result.succeeded, (
        "E14 invariant violated: a result that reached durable worker delivery was rejected "
        "before atomic fan-in completed.\n"
        f"campaign_id={configuration.campaign_id.value!r}\n"
        f"project_id={configuration.project.project_id.value!r}\n"
        f"lease_seconds={configuration.budget.lease_seconds!r}\n"
        f"max_workers={configuration.budget.max_workers!r}\n"
        f"campaign_status={getattr(result.campaign.status, 'value', result.campaign.status)!r}\n"
        f"coordinator_error={result.error!r}\n"
        f"database_path={str(result.database_path)!r}\n"
        f"events_path={str(result.events_path)!r}\n"
        f"protocol_path={str(result.protocol_path)!r}\n"
        f"stderr_path={str(result.stderr_path)!r}"
    )
