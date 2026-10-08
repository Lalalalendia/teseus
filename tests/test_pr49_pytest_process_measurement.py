from __future__ import annotations
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from test_intelligence_unified_v1.pytest_plugin import _TestStatsPlugin
from test_intelligence_unified_v1.runner import MutationConfig, MutationRunner
from test_intelligence_unified_v1.test_stats import load_pytest_attempt_performance, stats_attempt_id
from theseus_local.coordinator import LocalCampaignCoordinator, _CoordinatorTimeline

def _write_performance_artifact(
    event_dir: Path,
    *,
    run_id: str,
    phase: str,
    level: str,
    mutant_id: str | None,
    retry: bool,
    pid: int,
) -> Path:
    # Write one reconciled synthetic pytest-process artifact for loader and timeline tests.
    attempt = stats_attempt_id(run_id, phase, level, mutant_id, retry)
    path = event_dir / f"{run_id}.{attempt}.main.{pid}.performance.json"
    payload = {
        "schema_version": 1,
        "run_id": run_id,
        "phase": phase,
        "level": level,
        "mutant_id": mutant_id,
        "retry": retry,
        "attempt": attempt,
        "worker_id": "main",
        "pid": pid,
        "exclusive": True,
        "metrics": {
            "config_initialization_seconds": 1.0,
            "collection_import_seconds": 2.0,
            "test_execution_seconds": 4.0,
            "session_finalize_seconds": 0.25,
            "framework_residual_seconds": 0.75,
            "plugin_lifecycle_seconds": 8.0,
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path

def test_pytest_plugin_publishes_one_process_performance_artifact(tmp_path: Path) -> None:
    # Keep measurement in a sidecar artifact so normal per-test journals and pytest outcomes stay unchanged.
    with patch.dict(
        os.environ,
        {
            "TI_TEST_STATS_OUT_DIR": str(tmp_path),
            "TI_TEST_STATS_RUN_ID": "run-pr49",
            "TI_TEST_STATS_PHASE": "mutant",
            "TI_TEST_STATS_LEVEL": "L1",
            "TI_TEST_STATS_MUTANT_ID": "m1",
            "TI_TEST_STATS_ATTEMPT": "attempt-pr49",
        },
        clear=False,
    ):
        plugin = _TestStatsPlugin()
        plugin.pytest_sessionstart(SimpleNamespace())
        plugin.pytest_sessionfinish(SimpleNamespace(exitstatus=0), 0)
    artifacts = list(tmp_path.glob("run-pr49.attempt-pr49.*.performance.json"))
    assert len(artifacts) == 1
    payload = json.loads(artifacts[0].read_text(encoding="utf-8"))
    metrics = payload["metrics"]
    component_total = sum(
        metrics[name]
        for name in (
            "config_initialization_seconds",
            "collection_import_seconds",
            "test_execution_seconds",
            "session_finalize_seconds",
            "framework_residual_seconds",
        )
    )
    assert payload["pid"] == os.getpid()
    assert payload["exclusive"] is True
    assert abs(component_total - metrics["plugin_lifecycle_seconds"]) < 1e-6

def test_pytest_performance_loader_rejects_parallel_or_ambiguous_processes(tmp_path: Path) -> None:
    # Fail closed when one attempt has multiple plugin PIDs because their wall times can overlap.
    event_dir = tmp_path / "events"
    event_dir.mkdir()
    _write_performance_artifact(
        event_dir, run_id="run-pr49", phase="mutant", level="L1", mutant_id="m1", retry=False, pid=101
    )
    observed = load_pytest_attempt_performance(
        event_dir, run_id="run-pr49", phase="mutant", level="L1", mutant_id="m1", retry=False
    )
    assert observed["status"] == "observed"
    assert observed["pid"] == 101
    _write_performance_artifact(
        event_dir, run_id="run-pr49", phase="mutant", level="L1", mutant_id="m1", retry=False, pid=102
    )
    ambiguous = load_pytest_attempt_performance(
        event_dir, run_id="run-pr49", phase="mutant", level="L1", mutant_id="m1", retry=False
    )
    assert ambiguous == {
        "status": "unavailable",
        "reason": "parallel_or_ambiguous_pytest_processes",
        "artifact_count": 2,
    }

def test_runner_keeps_pytest_process_and_attaches_reconciled_exclusive_breakdown(tmp_path: Path) -> None:
    # Preserve the PR47 aggregate phase while attaching non-accounting single-process child evidence.
    runner = MutationRunner(MutationConfig(project_root=tmp_path, source="app.py"))
    runner.performance.pytest_seconds = 10.0
    runner.performance.campaign_wall_seconds = 10.0
    runner._pytest_process_attempts = 1
    runner._pytest_process_observed_attempts = 1
    runner._pytest_process_observed_elapsed_seconds = 10.0
    runner._pytest_process_phase_seconds = {
        "pytest_bootstrap_shutdown_residual": 1.0,
        "pytest_config_initialization": 2.0,
        "pytest_collection_import": 2.0,
        "pytest_test_execution": 4.0,
        "pytest_session_finalize": 0.25,
        "pytest_framework_residual": 0.75,
    }
    payload = runner._performance_payload()
    timeline = payload["worker_execution_timeline"]
    phases = {item["phase"]: item["wall_seconds"] for item in timeline["phases"]}
    breakdown = timeline["pytest_process_breakdown"]
    assert phases["pytest_process"] == 10.0
    assert breakdown == payload["pytest_process_breakdown"]
    assert breakdown["status"] == "observed"
    assert breakdown["phase_seconds"] == {
        "bootstrap_shutdown_residual": 1.0,
        "config_initialization": 2.0,
        "collection_import": 2.0,
        "test_execution": 4.0,
        "session_finalize": 0.25,
        "framework_residual": 0.75,
        "unattributed": 0.0,
    }

def test_coordinator_preserves_aggregate_and_publishes_non_accounting_pytest_diagnostics(tmp_path: Path) -> None:
    # Carry nested pytest evidence beside the aggregate phase without double-counting worker wall time.
    runner_breakdown = {
        "status": "observed",
        "exclusive": True,
        "attempts_total": 1,
        "attempts_observed": 1,
        "attempts_unavailable": 0,
        "unavailable_reasons": {},
        "total_pytest_process_seconds": 3.0,
        "observed_process_seconds": 3.0,
        "phase_seconds": {
            "bootstrap_shutdown_residual": 0.5,
            "config_initialization": 0.5,
            "collection_import": 0.5,
            "test_execution": 1.0,
            "session_finalize": 0.25,
            "framework_residual": 0.25,
            "unattributed": 0.0,
        },
    }
    payload = LocalCampaignCoordinator._worker_execution_breakdown(
        worker_id="worker-pr49",
        total_seconds=9.0,
        assignment_dispatch_seconds=0.25,
        process_timeline={
            "exclusive": True,
            "total_wall_seconds": 8.5,
            "phases": [
                {"phase": "engine_session_startup", "wall_seconds": 1.0},
                {"phase": "engine_request_prepare", "wall_seconds": 1.0},
                {"phase": "engine_request_execute_shard", "wall_seconds": 6.0},
                {"phase": "engine_process_unattributed_residual", "wall_seconds": 0.5},
            ],
        },
        runner_timeline={
            "exclusive": True,
            "total_wall_seconds": 5.5,
            "phases": [
                {"phase": "mutation_preparation", "wall_seconds": 0.5},
                {"phase": "pytest_process", "wall_seconds": 3.0},
                {"phase": "test_stats_ingestion", "wall_seconds": 0.5},
                {"phase": "source_restoration", "wall_seconds": 0.5},
                {"phase": "runner_unattributed_residual", "wall_seconds": 1.0},
            ],
            "pytest_process_breakdown": runner_breakdown,
        },
    )
    phases = {item["phase"]: item["wall_seconds"] for item in payload["phases"]}
    assert payload["valid_nested_evidence"] is True
    assert payload["accounting_error_seconds"] == 0.0
    assert phases["pytest_process"] == 3.0
    assert payload["pytest_process_breakdown"] == runner_breakdown
    timeline = _CoordinatorTimeline("campaign-pr49", clock=lambda: 0.0)
    LocalCampaignCoordinator._publish_worker_execution_report(
        tmp_path / "worker-execution.performance.json",
        (({"worker_execution_breakdown": payload}, None, None),),
        timeline,
    )
    diagnostics = timeline.finish(tmp_path / "coordinator.performance.json", status="completed")["diagnostics"]
    assert diagnostics["worker_phase_pytest_process_sum_seconds"] == 3.0
    assert diagnostics["worker_pytest_process_collection_import_sum_seconds"] == 0.5
    assert diagnostics["worker_pytest_process_test_execution_max_seconds"] == 1.0

def test_runner_keeps_aggregate_pytest_phase_when_breakdown_is_inconsistent(tmp_path: Path) -> None:
    # Keep the original aggregate phase when child evidence cannot reconcile to subprocess authority.
    runner = MutationRunner(MutationConfig(project_root=tmp_path, source="app.py"))
    runner.performance.pytest_seconds = 5.0
    runner.performance.campaign_wall_seconds = 5.0
    runner._pytest_process_attempts = 1
    runner._pytest_process_observed_attempts = 1
    runner._pytest_process_observed_elapsed_seconds = 10.0
    runner._pytest_process_phase_seconds = {"pytest_test_execution": 10.0}
    payload = runner._performance_payload()
    phases = {item["phase"]: item["wall_seconds"] for item in payload["worker_execution_timeline"]["phases"]}
    assert phases["pytest_process"] == 5.0
    assert payload["pytest_process_breakdown"]["status"] == "inconsistent_total"
