from __future__ import annotations
import inspect
import json
from pathlib import Path
from types import SimpleNamespace
from theseus_local.coordinator import LocalCampaignCoordinator, _CoordinatorTimeline
from theseus_performance import project_benchmark
from theseus_performance.authority import WorkloadIdentity


class FakeClock:
    """Deterministic monotonic clock for planning attribution tests."""

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
    # Build one stable benchmark identity for planning subphase adapter tests.
    return WorkloadIdentity(
        workload_name="offline-mutation-campaign",
        workload_version="real-project-baseline-v2",
        project_key="project-pr45",
        input_fingerprint="input-pr45",
        environment_fingerprint="environment-pr45",
        runtime_fingerprint="runtime-pr45",
    )


def test_planning_hot_path_exposes_ordered_exclusive_subphases() -> None:
    # Keep every expensive planning operation behind one ordered exclusive timeline boundary.
    source = inspect.getsource(LocalCampaignCoordinator.run)
    phases = (
        "planning_reuse_fingerprints",
        "planning_reuse_decisions",
        "planning_adaptive_history",
        "planning_plan_build",
        "planning_plan_binding",
        "planning_topology_persistence",
        "planning_outbox_projection",
        "planning_audit_checkpoint",
    )
    positions = [source.index(f'timeline.switch("{phase}")') for phase in phases]
    assert positions == sorted(positions)
    boundaries = (
        ("planning_reuse_fingerprints", "reuse_requests = self._reuse_requests("),
        ("planning_reuse_decisions", "if current.plan_id or campaign_plan_path.is_file():"),
        ("planning_adaptive_history", "adaptive_observations = ("),
        ("planning_plan_build", "campaign_plan = self._load_or_build_campaign_plan("),
        ("planning_plan_binding", "if protect_frozen_reuse:"),
        ("planning_topology_persistence", "stored_shards = self._require(store.list_shards("),
        ("planning_outbox_projection", "self._dispatch_outbox("),
        ("planning_audit_checkpoint", "audit_path = reports_dir / \"reuse.audit.json\""),
    )
    for phase, operation in boundaries:
        phase_position = source.index(f'timeline.switch("{phase}")')
        assert phase_position < source.index(operation, phase_position)


def test_planning_subphases_reconcile_without_parent_overlap(tmp_path: Path) -> None:
    # Prove planning attribution remains exclusive instead of double-counting one broad parent phase.
    clock = FakeClock()
    timeline = _CoordinatorTimeline("campaign-pr45", clock=clock)
    expected = (
        ("planning_reuse_fingerprints", 2.0),
        ("planning_reuse_decisions", 3.0),
        ("planning_adaptive_history", 1.0),
        ("planning_plan_build", 5.0),
        ("planning_plan_binding", 1.5),
        ("planning_topology_persistence", 2.5),
        ("planning_outbox_projection", 1.0),
        ("planning_audit_checkpoint", 1.0),
    )
    for phase, seconds in expected:
        timeline.switch(phase)
        clock.advance(seconds)
    payload = timeline.finish(tmp_path / "coordinator.performance.json", status="completed")
    phases = {item["phase"]: item["wall_seconds"] for item in payload["phases"]}
    assert payload["total_wall_seconds"] == 17.0
    assert payload["accounted_seconds"] == 17.0
    assert payload["accounting_error_seconds"] == 0.0
    assert phases["planning_plan_build"] == 5.0
    assert phases["unattributed_residual"] == 0.0
    assert "planning" not in phases


def test_benchmark_ranks_planning_subphases_from_coordinator_artifact(tmp_path: Path) -> None:
    # Preserve arbitrary exclusive planning phase names through the existing benchmark adapter.
    report_root = tmp_path / "reports" / "campaign-pr45"
    report_root.mkdir(parents=True)
    database_path = report_root / "campaign.sqlite3"
    database_path.write_bytes(b"")
    timeline_path = report_root / "coordinator.performance.json"
    timeline_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "timeline_version": "coordinator-exclusive-v1",
                "campaign_id": "campaign-pr45",
                "status": "completed",
                "exclusive": True,
                "total_wall_seconds": 10.0,
                "observed_phase_seconds": 10.0,
                "residual_seconds": 0.0,
                "accounted_seconds": 10.0,
                "accounting_error_seconds": 0.0,
                "phases": [
                    {"phase": "planning_reuse_fingerprints", "wall_seconds": 2.0, "source": "coordinator.perf_counter"},
                    {"phase": "planning_reuse_decisions", "wall_seconds": 3.0, "source": "coordinator.perf_counter"},
                    {"phase": "planning_plan_build", "wall_seconds": 5.0, "source": "coordinator.perf_counter"},
                    {"phase": "unattributed_residual", "wall_seconds": 0.0, "source": "coordinator.reconciliation"},
                ],
                "diagnostics": {},
                "error": None,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    result = SimpleNamespace(
        succeeded=True,
        campaign=SimpleNamespace(
            campaign_id=SimpleNamespace(value="campaign-pr45"),
            status=SimpleNamespace(value="completed"),
        ),
        database_path=database_path,
        engine_result=None,
        error=None,
    )
    run = project_benchmark._scenario_run(
        result,
        identity=_identity(),
        run_id="run-pr45",
        started_at="2026-08-06T00:00:00+00:00",
        completed_at="2026-08-06T00:00:10+00:00",
        wall_seconds=10.0,
        coordinator_cpu_seconds=1.0,
        mode="cold",
        workers=1,
    )
    summary = project_benchmark._phase_summary(run)
    assert summary["dominant_observed_phase"] == "planning_plan_build"
    assert summary["phase_accounting_ratio"] == 1.0
    assert summary["external_wall_coverage_ratio"] == 1.0
    assert summary["unmeasured_phases"] == []
