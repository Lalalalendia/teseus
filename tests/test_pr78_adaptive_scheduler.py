from __future__ import annotations

import inspect
import json
import sys
from pathlib import Path
from types import SimpleNamespace

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
    TestCommandDescriptor,
)
from theseus_local import LocalCampaignCoordinator
from theseus_performance.project_benchmark import _diagnostic_metric_unit
from theseus_planner import CampaignPlanner


def _mutant(mutant_id: str, line_no: int) -> MutantDescriptor:
    # Build one stable costed candidate for the fine-grained planner contract.
    return MutantDescriptor(
        mutant_id=MutantId(mutant_id),
        mutation="condition_to_not",
        source_path="app.py",
        line_no=line_no,
        column_no=4,
        original="if value > 0:",
        replacement="if not (value > 0):",
        function_id="choose",
    )


def _prepared(root: Path, mutants: tuple[MutantDescriptor, ...]) -> PreparedCampaign:
    # Keep the preparation boundary immutable while varying only catalog order.
    return PreparedCampaign(
        campaign_id=CampaignId("cmp_pr78_scheduler"),
        source_path="app.py",
        source_sha256="source-pr78",
        index_version="index-pr78",
        mutants=mutants,
        snapshot_id="prepared-pr78",
    )


def _configuration(root: Path, *, target_shard_seconds: float = 1.0) -> CampaignConfiguration:
    # Configure two runtime slots so the test can distinguish topology from capacity.
    return CampaignConfiguration(
        campaign_id=CampaignId("cmp_pr78_scheduler"),
        project=ProjectDescriptor(
            project_id=ProjectId("project_pr78_scheduler"),
            display_name="PR78 scheduler fixture",
            root_path=str(root),
            test_command=TestCommandDescriptor(
                (sys.executable, "-m", "pytest", "-q", "test_app.py"),
            ),
        ),
        scope=MutationScope(
            source_path="app.py",
            function="choose",
            operators=("condition_to_not",),
        ),
        budget=CampaignBudget(
            max_mutants=6,
            max_workers=2,
            max_test_seconds=15.0,
            target_shard_seconds=target_shard_seconds,
        ),
        no_escalation=True,
        reports_dir=str(root / "reports"),
    )


def test_planner_emits_deterministic_fine_units_beyond_runtime_width(tmp_path: Path) -> None:
    # Small target units improve queue flexibility without changing selected identities or plan authority.
    catalog = tuple(_mutant(f"m{index}", index * 10) for index in range(1, 7))
    configuration = _configuration(tmp_path, target_shard_seconds=1.0)
    first = CampaignPlanner().build(configuration, _prepared(tmp_path, catalog), mutants=catalog)
    second = CampaignPlanner().build(
        configuration,
        _prepared(tmp_path, tuple(reversed(catalog))),
        mutants=tuple(reversed(catalog)),
    )

    assert first.plan_id == second.plan_id
    assert first.selected_count == 6
    assert first.worker_count == len(first.shards) == 6
    assert first.scheduler_worker_count == 2
    assert len(first.shards) > first.scheduler_worker_count
    assert first.adaptive_policy["scheduler_worker_count"] == 2
    assert first.from_dict(first.to_dict()) == first
    assert tuple(item.mutant_ids for item in first.shards) == tuple(
        item.mutant_ids for item in second.shards
    )


def test_scheduler_diagnostics_reconcile_queue_idle_and_cost_imbalance() -> None:
    # Verify the reported utilization is based on lifecycle work and bounded capacity, not mutant count.
    def spec(cost: float, setup: float, execution: float, queue_wait: float) -> dict[str, object]:
        return {
            "descriptor": SimpleNamespace(estimated_cost=cost),
            "worker_setup_seconds": setup,
            "worker_execution_seconds": execution,
            "scheduler_queue_wait_seconds": queue_wait,
        }

    results = [
        (spec(8.0, 1.0, 9.0, 0.0), None, None),
        (spec(1.0, 1.0, 2.0, 3.0), None, None),
    ]
    metrics = LocalCampaignCoordinator._scheduler_diagnostics(results, worker_slots=2)

    assert metrics["scheduler_worker_slots"] == 2.0
    assert metrics["scheduler_dispatched_units"] == 2.0
    assert metrics["scheduler_queue_wait_seconds"] == 3.0
    assert metrics["scheduler_queue_wait_max_seconds"] == 3.0
    assert metrics["scheduler_critical_path_seconds"] == 10.0
    assert metrics["worker_idle_seconds"] == 7.0
    assert metrics["worker_utilization"] == 0.65
    assert metrics["scheduler_cost_imbalance_ratio"] == 8.0 / 4.5


def test_scheduler_dispatch_is_bounded_and_cost_ordered() -> None:
    # Lock the runtime boundary while leaving fan-in and lease ownership in the existing coordinator path.
    source = inspect.getsource(LocalCampaignCoordinator._execute_parallel_shards)
    assert "campaign_plan.worker_count" in source
    assert "campaign_plan.scheduler_worker_count" in source
    assert "ThreadPoolExecutor(max_workers=scheduler_worker_count" in source
    assert "scheduler_queue_wait_seconds" in source
    assert "estimated_cost" in source

    assert _diagnostic_metric_unit("worker_utilization") == "ratio"
    assert _diagnostic_metric_unit("scheduler_cost_imbalance_ratio") == "ratio"
    assert _diagnostic_metric_unit("scheduler_worker_slots") == "count"
    assert _diagnostic_metric_unit("scheduler_dispatched_units") == "count"
    assert _diagnostic_metric_unit("worker_execution_max_seconds") == "seconds"


def test_local_coordinator_executes_fine_units_with_bounded_slots(tmp_path: Path) -> None:
    # Exercise real fresh-process execution with four planner units and only two runtime slots.
    (tmp_path / "app.py").write_text(
        "def choose(value):\n"
        "    if value > 0:\n"
        "        return 1\n"
        "    if value == 0:\n"
        "        return 2\n"
        "    if value < -1:\n"
        "        return 3\n"
        "    if value == 99:\n"
        "        return 4\n"
        "    return 0\n",
        encoding="utf-8",
    )
    (tmp_path / "test_app.py").write_text(
        "from app import choose\n\n"
        "def test_choose():\n"
        "    assert choose(1) == 1\n",
        encoding="utf-8",
    )

    configuration = _configuration(tmp_path, target_shard_seconds=0.1)
    result = LocalCampaignCoordinator().run(configuration)

    assert result.succeeded, result.error
    plan = json.loads((result.database_path.parent / "campaign.plan.json").read_text(encoding="utf-8"))
    assert plan["cost_model"]["scheduler_worker_count"] == 2.0
    assert len(plan["shards"]) == 4
    timeline = json.loads(
        (result.database_path.parent / "coordinator.performance.json").read_text(encoding="utf-8")
    )
    diagnostics = timeline["diagnostics"]
    assert diagnostics["scheduler_worker_slots"] == 2.0
    assert diagnostics["scheduler_dispatched_units"] == 4.0
    assert 0.0 <= diagnostics["worker_utilization"] <= 1.0

