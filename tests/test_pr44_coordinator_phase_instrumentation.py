from __future__ import annotations

import inspect
import json
from pathlib import Path
from types import SimpleNamespace

from theseus_performance.authority import WorkloadIdentity
from theseus_performance import project_benchmark
from theseus_local.coordinator import LocalCampaignCoordinator, _CoordinatorTimeline


class FakeClock:
    """Deterministic monotonic clock for exclusive timeline tests."""

    def __init__(self) -> None:
        # Start the deterministic clock at zero seconds.
        self.value = 0.0

    def __call__(self) -> float:
        # Return the current deterministic wall-clock sample.
        return self.value

    def advance(self, seconds: float) -> None:
        # Move the deterministic clock forward by one non-negative interval.
        self.value += float(seconds)


def _identity() -> WorkloadIdentity:
    # Build one stable benchmark comparison identity for adapter tests.
    return WorkloadIdentity(
        workload_name="offline-mutation-campaign",
        workload_version="real-project-baseline-v2",
        project_key="project-fixture",
        input_fingerprint="input-fixture",
        environment_fingerprint="environment-fixture",
        runtime_fingerprint="runtime-fixture",
    )


def test_coordinator_timeline_reconciles_exclusive_intervals_and_residual(tmp_path: Path) -> None:
    # Persist one exact exclusive timeline whose observed phases reconcile with total wall time.
    clock = FakeClock()
    timeline = _CoordinatorTimeline("campaign-fixture", clock=clock)
    timeline.switch("workspace_setup")
    clock.advance(2.0)
    timeline.switch("test_collection")
    clock.advance(3.0)
    timeline.observe_diagnostic("worker_setup_max_seconds", 1.25)
    path = tmp_path / "coordinator.performance.json"
    payload = timeline.finish(path, status="completed")
    persisted = json.loads(path.read_text(encoding="utf-8"))
    assert payload == persisted
    assert payload["exclusive"] is True
    assert payload["total_wall_seconds"] == 5.0
    assert payload["observed_phase_seconds"] == 5.0
    assert payload["residual_seconds"] == 0.0
    assert payload["accounted_seconds"] == 5.0
    assert payload["accounting_error_seconds"] == 0.0
    assert payload["diagnostics"]["worker_setup_max_seconds"] == 1.25
    assert [(item["phase"], item["wall_seconds"]) for item in payload["phases"]] == [
        ("workspace_setup", 2.0),
        ("test_collection", 3.0),
        ("unattributed_residual", 0.0),
    ]


def test_coordinator_timeline_accumulates_reentered_phase_without_overlap(tmp_path: Path) -> None:
    # Sum non-contiguous fan-in intervals while retaining one unique phase identity.
    clock = FakeClock()
    timeline = _CoordinatorTimeline("campaign-reentry", clock=clock)
    timeline.switch("fan_in")
    clock.advance(1.0)
    timeline.switch("worker_cleanup")
    clock.advance(2.0)
    timeline.switch("fan_in")
    clock.advance(3.0)
    payload = timeline.finish(tmp_path / "timeline.json", status="completed")
    phases = {item["phase"]: item["wall_seconds"] for item in payload["phases"]}
    assert phases["fan_in"] == 4.0
    assert phases["worker_cleanup"] == 2.0
    assert len(phases) == 3


def test_project_benchmark_prefers_coordinator_timeline_over_overlapping_runner_phases(tmp_path: Path) -> None:
    # Use the reconciled coordinator artifact as the primary phase source and retain runner metrics as diagnostics.
    report_root = tmp_path / "reports" / "campaign-fixture"
    report_root.mkdir(parents=True)
    database_path = report_root / "campaign.sqlite3"
    database_path.write_bytes(b"")
    timeline_path = report_root / "coordinator.performance.json"
    timeline_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "timeline_version": "coordinator-exclusive-v1",
                "campaign_id": "campaign-fixture",
                "status": "completed",
                "exclusive": True,
                "total_wall_seconds": 9.0,
                "observed_phase_seconds": 8.0,
                "residual_seconds": 1.0,
                "accounted_seconds": 9.0,
                "accounting_error_seconds": 0.0,
                "phases": [
                    {"phase": "workspace_setup", "wall_seconds": 3.0, "source": "coordinator.perf_counter"},
                    {"phase": "worker_execution_wait", "wall_seconds": 5.0, "source": "coordinator.perf_counter"},
                    {"phase": "unattributed_residual", "wall_seconds": 1.0, "source": "coordinator.reconciliation"},
                ],
                "diagnostics": {
                    "worker_setup_max_seconds": 2.0,
                    "worker_execution_max_seconds": 4.0,
                },
                "error": None,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    result = SimpleNamespace(
        succeeded=True,
        campaign=SimpleNamespace(
            campaign_id=SimpleNamespace(value="campaign-fixture"),
            status=SimpleNamespace(value="completed"),
        ),
        database_path=database_path,
        engine_result=None,
        error=None,
    )
    run = project_benchmark._scenario_run(
        result,
        identity=_identity(),
        run_id="run-fixture",
        started_at="2026-08-06T00:00:00+00:00",
        completed_at="2026-08-06T00:00:10+00:00",
        wall_seconds=10.0,
        coordinator_cpu_seconds=2.0,
        mode="cold",
        workers=1,
    )
    metrics = run.metric_map()
    assert [item.phase for item in run.phases] == [
        "workspace_setup",
        "worker_execution_wait",
        "unattributed_residual",
    ]
    assert metrics["coordinator_timeline_wall_seconds"].value == 9.0
    assert metrics["phase_accounted_seconds"].value == 9.0
    assert metrics["phase_residual_seconds"].value == 1.0
    assert metrics["phase_accounting_ratio"].value == 1.0
    assert metrics["benchmark_wrapper_overhead_seconds"].value == 1.0
    assert metrics["worker_setup_max_seconds"].value == 2.0
    assert run.metadata["coordinator_timeline_path"] == str(timeline_path)
    summary = project_benchmark._phase_summary(run)
    assert summary["dominant_observed_phase"] == "worker_execution_wait"
    assert summary["unmeasured_phases"] == []
    assert summary["phase_coverage_ratio"] == 1.0
    assert summary["unattributed_residual_seconds"] == 1.0
    assert summary["phase_accounting_ratio"] == 1.0
    assert summary["external_wall_coverage_ratio"] == 0.9


def test_coordinator_source_exposes_complete_exclusive_phase_boundaries() -> None:
    # Keep PR44 instrumentation at coordinator boundaries instead of inferring phases from one worker report.
    run_source = inspect.getsource(LocalCampaignCoordinator.run)
    shard_source = inspect.getsource(LocalCampaignCoordinator._execute_parallel_shards)
    for phase in (
        "workspace_setup",
        "authority_setup",
        "startup_recovery",
        "engine_startup",
        "campaign_preparation",
        "test_collection",
        "project_index",
        "baseline",
        "mutation_discovery",
        "planning",
        "reuse_audit",
        "aggregation",
        "finalization",
        "projection",
        "cleanup",
    ):
        assert f'timeline.switch("{phase}")' in run_source
    for phase in ("worker_dispatch", "worker_execution_wait", "fan_in", "worker_cleanup"):
        assert f'timeline.switch("{phase}")' in shard_source
    assert 'timeline=timeline' in run_source
    assert 'coordinator.performance.json' in run_source
