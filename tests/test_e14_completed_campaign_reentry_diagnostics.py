from __future__ import annotations

import sqlite3
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


def _configuration(root: Path) -> CampaignConfiguration:
    # Build one stable campaign identity used to prove terminal re-entry is read-only.
    return CampaignConfiguration(
        campaign_id=CampaignId("campaign-e14-completed-reentry"),
        project=ProjectDescriptor(
            project_id=ProjectId("project-e14-completed-reentry"),
            display_name="E14 completed campaign re-entry diagnostics",
            root_path=str(root),
            test_command=TestCommandDescriptor(
                (sys.executable, "-c", "from app import choose; assert choose(1) == 1")
            ),
            pytest_plugin_autoload=False,
        ),
        scope=MutationScope(
            source_path="app.py",
            function="choose",
            operators=("condition_to_not",),
        ),
        budget=CampaignBudget(max_mutants=1, max_workers=1),
        no_escalation=True,
        reports_dir=str(root / "reports"),
    )


def _registered_counts(database_path: Path) -> tuple[int, int, int, int]:
    # Read immutable completion cardinalities so repeated terminal entry cannot hide duplicate writes.
    with sqlite3.connect(database_path) as connection:
        return (
            int(connection.execute("SELECT COUNT(*) FROM mutation_artifacts").fetchone()[0]),
            int(connection.execute("SELECT COUNT(*) FROM mutation_finalizations").fetchone()[0]),
            int(connection.execute("SELECT COUNT(*) FROM mutation_executions").fetchone()[0]),
            int(connection.execute("SELECT COUNT(*) FROM mutation_effects").fetchone()[0]),
        )


def test_completed_campaign_reentry_is_read_only_and_registry_stable(tmp_path: Path) -> None:
    # Return a completed campaign without launching an engine or rewriting immutable registered reports.
    (tmp_path / "app.py").write_text(
        "def choose(value):\n"
        "    if value > 0:\n"
        "        return 1\n"
        "    return 0\n",
        encoding="utf-8",
    )
    configuration = _configuration(tmp_path)
    coordinator = LocalCampaignCoordinator()
    first = coordinator.run(configuration)
    assert first.succeeded, (
        "initial campaign did not establish the terminal registry fixture; "
        f"campaign={configuration.campaign_id.value}; status={first.campaign.status}; "
        f"error={first.error!r}; database={first.database_path}; stderr={first.stderr_path}"
    )
    report_root = first.database_path.parent
    immutable_paths = (
        report_root / "campaign.plan.json",
        report_root / "reuse.plan.json",
        report_root / "reuse.audit.json",
        report_root / "canonical.report.json",
        report_root / "canonical.report.md",
    )
    before_bytes = {path.name: path.read_bytes() for path in immutable_paths}
    before_protocol = first.protocol_path.read_bytes()
    before_counts = _registered_counts(first.database_path)
    second = coordinator.run(configuration)
    third = coordinator.run(configuration)
    assert second.succeeded and third.succeeded, (
        "completed campaign re-entry failed immutable finalization validation; "
        f"campaign={configuration.campaign_id.value}; "
        f"second_error={second.error!r}; third_error={third.error!r}; "
        f"database={first.database_path}; registry_counts={before_counts}"
    )
    after_bytes = {path.name: path.read_bytes() for path in immutable_paths}
    changed = sorted(
        name for name in before_bytes if before_bytes[name] != after_bytes[name]
    )
    assert not changed, (
        "terminal campaign re-entry rewrote immutable registered artifacts; "
        f"campaign={configuration.campaign_id.value}; changed={changed}; "
        f"report_root={report_root}; database={first.database_path}"
    )
    assert first.protocol_path.read_bytes() == before_protocol, (
        "terminal campaign re-entry launched or contacted a new engine process; "
        f"campaign={configuration.campaign_id.value}; protocol={first.protocol_path}; "
        f"before_bytes={len(before_protocol)}; after_bytes={first.protocol_path.stat().st_size}"
    )
    after_counts = _registered_counts(first.database_path)
    assert after_counts == before_counts, (
        "terminal campaign re-entry duplicated authoritative completion rows; "
        f"campaign={configuration.campaign_id.value}; before={before_counts}; "
        f"after={after_counts}; database={first.database_path}"
    )
