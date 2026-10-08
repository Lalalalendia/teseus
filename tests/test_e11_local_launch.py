import json
import sys
from pathlib import Path

from theseus_contracts import CampaignBudget, CampaignConfiguration, CampaignId, MutationScope, ProjectDescriptor, ProjectId, TestCommandDescriptor as CommandDescriptor

from theseus_local import LocalCampaignCoordinator


def test_local_campaign_runs_through_engine_process_and_sqlite(tmp_path: Path) -> None:
    # Verify the first real vertical slice crosses process protocol, facade, domain and persistence.
    source = tmp_path / "app.py"
    source_text = "def choose(value):\n    if value > 0:\n        return 1\n    return 0\n"
    source.write_text(source_text, encoding="utf-8")
    configuration = CampaignConfiguration(
        campaign_id=CampaignId("cmp_local_e11"),
        project=ProjectDescriptor(
            project_id=ProjectId("project_local_e11"),
            display_name="local e11 fixture",
            root_path=str(tmp_path),
            test_command=CommandDescriptor((sys.executable, "-c", "from app import choose; assert choose(1) == 1")),
        ),
        scope=MutationScope(source_path="app.py", function="choose", operators=("condition_to_not",)),
        budget=CampaignBudget(max_mutants=1, max_workers=1),
        no_escalation=True,
        reports_dir=str(tmp_path / "reports"),
    )
    result = LocalCampaignCoordinator().run(configuration)
    assert result.succeeded
    assert result.campaign.status.value == "completed"
    assert result.campaign.completed_mutants == 1
    assert result.engine_result is not None
    assert result.engine_result.summary.status == "complete"
    assert result.events_path.exists()
    assert result.protocol_path.exists()
    assert result.stderr_path.exists()
    assert source.read_text(encoding="utf-8") == source_text
    event_lines = [line for line in result.events_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert any(json.loads(line)["event_type"] == "report_materialized" for line in event_lines)


def test_engine_process_exit_without_terminal_event_marks_campaign_failed(tmp_path: Path) -> None:
    # Verify a child exit before a terminal event never appears as a completed campaign.
    source = tmp_path / "app.py"
    source.write_text("def choose(value):\n    return value\n", encoding="utf-8")
    configuration = CampaignConfiguration(
        campaign_id=CampaignId("cmp_local_e11_failed"),
        project=ProjectDescriptor(
            project_id=ProjectId("project_local_e11_failed"),
            display_name="local failed fixture",
            root_path=str(tmp_path),
        ),
        scope=MutationScope(source_path="app.py", function="choose"),
        budget=CampaignBudget(max_mutants=1, max_workers=1),
        reports_dir=str(tmp_path / "reports"),
    )
    result = LocalCampaignCoordinator(process_command=(sys.executable, "-c", "raise SystemExit(1)")).run(configuration)
    assert not result.succeeded
    assert result.campaign.status.value == "failed"
    assert result.error
    assert result.stderr_path.exists()
