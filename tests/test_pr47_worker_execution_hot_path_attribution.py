from __future__ import annotations
import ast
import inspect
import json
from pathlib import Path
from test_intelligence_unified_v1.models import PerformanceMetrics
from theseus_local.coordinator import LocalCampaignCoordinator, _CoordinatorTimeline
from theseus_local.process import EngineProcessSession


class FakeClock:
    """Deterministic monotonic clock for worker attribution tests."""

    def __init__(self) -> None:
        # Start one deterministic clock at zero seconds.
        self.value = 0.0

    def __call__(self) -> float:
        # Return the current deterministic sample.
        return self.value


def test_runner_worker_timeline_reconciles_existing_exclusive_metrics() -> None:
    # Convert the runner's non-overlapping counters into one complete worker timeline.
    metrics = PerformanceMetrics(
        index_seconds=0.5,
        selection_seconds=0.5,
        snapshot_seconds=0.5,
        mutant_generation_seconds=0.5,
        mutation_preparation_seconds=1.0,
        mutant_apply_seconds=1.0,
        pytest_seconds=3.0,
        pytest_wrapper_seconds=0.25,
        test_stats_ingestion_seconds=0.5,
        restore_seconds=1.0,
        report_write_seconds=0.25,
        campaign_wall_seconds=10.0,
    )
    timeline = metrics.to_dict()["worker_execution_timeline"]
    phases = {item["phase"]: item["wall_seconds"] for item in timeline["phases"]}
    assert timeline["exclusive"] is True
    assert timeline["total_wall_seconds"] == 10.0
    assert timeline["observed_phase_seconds"] == 9.0
    assert timeline["residual_seconds"] == 1.0
    assert timeline["accounted_seconds"] == 10.0
    assert timeline["accounting_error_seconds"] == 0.0
    assert phases["pytest_process"] == 3.0
    assert phases["runner_unattributed_residual"] == 1.0


def test_engine_process_payload_reconciles_startup_requests_and_idle_residual(tmp_path: Path) -> None:
    # Reconcile one engine session without starting a real child process.
    session = EngineProcessSession(
        events_path=tmp_path / "events.jsonl",
        protocol_path=tmp_path / "protocol.jsonl",
        stdout_path=tmp_path / "stdout.log",
        stderr_path=tmp_path / "stderr.log",
    )
    session._performance_started = 0.0
    session._record_performance("engine_session_startup", 1.0)
    session._record_performance("engine_request_execute_shard", 2.5)
    payload = session._performance_payload(status="running", now=10.0)
    phases = {item["phase"]: item["wall_seconds"] for item in payload["phases"]}
    assert payload["exclusive"] is True
    assert payload["total_wall_seconds"] == 10.0
    assert payload["observed_phase_seconds"] == 3.5
    assert payload["residual_seconds"] == 6.5
    assert payload["accounted_seconds"] == 10.0
    assert phases["engine_request_execute_shard"] == 2.5
    assert phases["engine_process_unattributed_residual"] == 6.5


def test_worker_breakdown_replaces_execute_request_with_runner_subphases() -> None:
    # Preserve nested runner detail while reconciling against the outer coordinator wait.
    process_timeline = {
        "exclusive": True,
        "total_wall_seconds": 8.5,
        "phases": [
            {"phase": "engine_session_startup", "wall_seconds": 1.0},
            {"phase": "engine_request_prepare", "wall_seconds": 1.0},
            {"phase": "engine_request_execute_shard", "wall_seconds": 6.0},
            {"phase": "engine_process_unattributed_residual", "wall_seconds": 0.5},
        ],
    }
    runner_timeline = {
        "exclusive": True,
        "total_wall_seconds": 5.5,
        "phases": [
            {"phase": "mutation_preparation", "wall_seconds": 0.5},
            {"phase": "mutation_application", "wall_seconds": 0.5},
            {"phase": "pytest_process", "wall_seconds": 3.0},
            {"phase": "test_stats_ingestion", "wall_seconds": 0.5},
            {"phase": "source_restoration", "wall_seconds": 0.5},
            {"phase": "runner_unattributed_residual", "wall_seconds": 0.5},
        ],
    }
    payload = LocalCampaignCoordinator._worker_execution_breakdown(
        worker_id="worker-001",
        total_seconds=9.0,
        assignment_dispatch_seconds=0.25,
        process_timeline=process_timeline,
        runner_timeline=runner_timeline,
    )
    phases = {item["phase"]: item["wall_seconds"] for item in payload["phases"]}
    assert payload["valid_nested_evidence"] is True
    assert payload["accounted_seconds"] == 9.0
    assert payload["accounting_error_seconds"] == 0.0
    assert phases["engine_execute_shard_protocol"] == 0.5
    assert phases["pytest_process"] == 3.0
    assert phases["durable_delivery_spool_and_runtime"] == 0.75
    assert "engine_request_execute_shard" not in phases


def test_worker_report_publishes_aggregate_diagnostics(tmp_path: Path) -> None:
    # Publish one worker row and expose sum/max diagnostics without changing coordinator phases.
    breakdown = LocalCampaignCoordinator._worker_execution_breakdown(
        worker_id="worker-001",
        total_seconds=4.0,
        assignment_dispatch_seconds=0.25,
        process_timeline={
            "exclusive": True,
            "total_wall_seconds": 3.0,
            "phases": [
                {"phase": "engine_session_startup", "wall_seconds": 0.5},
                {"phase": "engine_request_execute_shard", "wall_seconds": 2.5},
            ],
        },
        runner_timeline=None,
    )
    clock = FakeClock()
    timeline = _CoordinatorTimeline("campaign-pr47", clock=clock)
    path = tmp_path / "worker-execution.performance.json"
    payload = LocalCampaignCoordinator._publish_worker_execution_report(
        path,
        (({"worker_execution_breakdown": breakdown}, None, None),),
        timeline,
    )
    persisted = json.loads(path.read_text(encoding="utf-8"))
    diagnostics = timeline.finish(tmp_path / "coordinator.performance.json", status="completed")["diagnostics"]
    assert payload == persisted
    assert len(payload["workers"]) == 1
    assert diagnostics["worker_phase_engine_request_execute_shard_max_seconds"] == 2.5
    assert diagnostics["worker_phase_accounting_ratio_min"] == 1.0


def test_pr47_source_contract_keeps_instrumentation_measurement_only() -> None:
    # Keep worker attribution at existing boundaries without changing execution or lease ordering.
    process_source = inspect.getsource(EngineProcessSession)
    coordinator_source = inspect.getsource(LocalCampaignCoordinator)
    assert '"engine-process.performance.json"' in process_source
    assert '"worker-execution.performance.json"' in coordinator_source
    assert 'spec["assignment_dispatch_seconds"]' in coordinator_source
    assert "observed_shard_result = delivery_payload.shard_result" in coordinator_source
    assert "observed_shard_result.report_path" in coordinator_source
    assert "if observed_shard_result is not None" in coordinator_source
    assert "delivery_payload.shard_result.report_path" not in coordinator_source
    assert "worker_phase_accounting_ratio_min" in coordinator_source
    root = Path(__file__).resolve().parents[1]
    for relative in ("models.py", "runner.py", "theseus_local/process.py", "theseus_local/coordinator.py"):
        source = (root / relative).read_text(encoding="utf-8")
        lines = source.splitlines()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if node.name not in {
                "_record_performance",
                "_performance_payload",
                "_publish_performance",
                "_performance_phase_map",
                "_load_worker_runner_timeline",
                "_load_engine_process_timeline",
                "_worker_execution_breakdown",
                "_publish_worker_execution_report",
            }:
                continue
            first = node.body[0]
            assert first.lineno >= 2 and lines[first.lineno - 2].strip().startswith("#"), node.name
