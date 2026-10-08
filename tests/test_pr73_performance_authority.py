from __future__ import annotations

import json
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from theseus_performance import (
    PerformanceCollector,
    PerformanceConflictError,
    PerformanceMetric,
    PerformanceRun,
    PhaseMeasurement,
    WorkloadIdentity,
    evaluate_phase_accounting,
    summarize_performance_runs,
)
from theseus_performance.project_benchmark import ProjectBenchmarkRequest, run_project_benchmark


def _identity(*, environment: str = "env-a") -> WorkloadIdentity:
    # Build one exact comparison boundary for repetition authority tests.
    return WorkloadIdentity(
        workload_name="offline-mutation-campaign",
        workload_version="workload-v1",
        project_key="project-fixture",
        input_fingerprint="input-fixture",
        environment_fingerprint=environment,
        runtime_fingerprint="python-3.14-theseus-fixture",
    )


def _run(run_id: str, wall_seconds: float, *, environment: str = "env-a") -> PerformanceRun:
    # Create one physical run whose total wall-clock evidence is observed.
    collector = PerformanceCollector(_identity(environment=environment), run_id=run_id)
    collector.observe("wall_seconds", wall_seconds, unit="seconds", source="fixture.wall")
    return collector.finish()


def _authoritative_run(run_id: str, residual_seconds: float) -> PerformanceRun:
    # Create exclusive phase evidence that either passes or fails the residual gate.
    base = _run(run_id, 10.0)
    metrics = {item.name: item for item in base.metrics}
    for name, value, unit in (
        ("coordinator_timeline_wall_seconds", 10.0, "seconds"),
        ("phase_accounted_seconds", 10.0, "seconds"),
        ("phase_residual_seconds", residual_seconds, "seconds"),
        ("benchmark_wrapper_overhead_seconds", 0.0, "seconds"),
    ):
        metrics[name] = PerformanceMetric.observed(name, value, unit, "fixture.timeline")
    known = 10.0 - residual_seconds
    phases = (
        PhaseMeasurement(
            phase="planning",
            wall_seconds=PerformanceMetric.observed("wall_seconds", known, "seconds", "fixture.timeline"),
            cpu_seconds=PerformanceMetric.unavailable("cpu_seconds", "seconds", "fixture"),
            peak_memory_bytes=PerformanceMetric.unavailable("peak_memory_bytes", "bytes", "fixture"),
        ),
        PhaseMeasurement(
            phase="unattributed_residual",
            wall_seconds=PerformanceMetric.observed(
                "wall_seconds", residual_seconds, "seconds", "fixture.timeline"
            ),
            cpu_seconds=PerformanceMetric.unavailable("cpu_seconds", "seconds", "fixture"),
            peak_memory_bytes=PerformanceMetric.unavailable("peak_memory_bytes", "bytes", "fixture"),
        ),
    )
    return replace(base, phases=phases, metrics=tuple(metrics[name] for name in sorted(metrics)))


def test_repetition_summary_uses_median_min_max_and_explicit_sufficiency() -> None:
    # Three exact-identity runs produce a sufficient, auditable distribution rather than one noisy sample.
    summary = summarize_performance_runs((_run("run-1", 1.0), _run("run-2", 3.0), _run("run-3", 2.0)))
    wall = next(item for item in summary.metrics if item.name == "wall_seconds")
    assert summary.sample_count == 3
    assert summary.repetition_status == "sufficient"
    assert wall.status == "observed"
    assert wall.minimum == 1.0
    assert wall.median == 2.0
    assert wall.maximum == 3.0
    assert wall.spread == 2.0
    assert wall.relative_spread == 1.0
    assert summary.representative_run_id == "run-3"
    assert summary.to_dict()["run_ids"] == ["run-1", "run-2", "run-3"]


def test_repetition_summary_rejects_cross_identity_aggregation() -> None:
    # A changed environment must never be hidden inside one performance distribution.
    with pytest.raises(PerformanceConflictError):
        summarize_performance_runs((_run("run-a", 1.0), _run("run-b", 1.0, environment="env-b")))


def test_phase_accounting_gate_rejects_large_unattributed_residual() -> None:
    # The authority accepts a reconciled timeline and rejects an unexplained residual bucket above 10 percent.
    passed = evaluate_phase_accounting(_authoritative_run("accounted", 0.0))
    failed = evaluate_phase_accounting(_authoritative_run("residual", 2.0))
    assert passed["status"] == "passed"
    assert passed["residual_ratio"] == 0.0
    assert failed["status"] == "failed"
    assert failed["residual_ratio"] == 0.2


class RepetitionFakeCoordinator:
    """Deterministic executor that exposes three physical observations per cold/warm lane."""

    configurations: list[object] = []

    def run(self, configuration):
        # Publish one runner report per physical repetition without starting child processes.
        self.configurations.append(configuration)
        campaign_id = configuration.campaign_id.value
        mode = "warm" if "-warm" in campaign_id else "cold"
        repetition = int(campaign_id.rsplit("-r", 1)[1]) if "-r" in campaign_id else 1
        base = {1: 1.0, 2: 3.0, 3: 2.0}[repetition]
        wall = base + (10.0 if mode == "warm" else 0.0)
        report_root = Path(configuration.reports_dir) / campaign_id
        report_root.mkdir(parents=True, exist_ok=True)
        raw_path = report_root / "raw-engine.report.json"
        raw_path.write_text(
            json.dumps(
                {
                    "status": "complete",
                    "metrics": {
                        "performance": {
                            "campaign_wall_seconds": wall,
                            "index_seconds": 0.1,
                            "selection_seconds": 0.1,
                            "mutant_generation_seconds": 0.1,
                            "pytest_seconds": wall,
                            "test_stats_ingestion_seconds": 0.1,
                            "report_materialization_seconds": 0.1,
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        canonical_path = report_root / "canonical.report.json"
        canonical_path.write_text("{}\n", encoding="utf-8")
        return SimpleNamespace(
            campaign=SimpleNamespace(
                campaign_id=configuration.campaign_id,
                status=SimpleNamespace(value="completed"),
            ),
            engine_result=SimpleNamespace(
                report={"raw_engine_report_path": str(raw_path)},
                summary=SimpleNamespace(report_path=str(canonical_path)),
            ),
            database_path=report_root / "campaign.sqlite3",
            succeeded=True,
            error=None,
        )


def test_project_benchmark_persists_physical_repetitions_and_authority_medians(tmp_path: Path) -> None:
    # A three-repeat benchmark keeps six physical scenarios and publishes two sufficient lane summaries.
    project = tmp_path / "project"
    project.mkdir()
    (project / "app.py").write_text("def choose(value):\n    return value\n", encoding="utf-8")
    (project / "test_app.py").write_text("def test_choose():\n    assert True\n", encoding="utf-8")
    request = ProjectBenchmarkRequest(
        project_root=project,
        source_path="app.py",
        test_command=(sys.executable, "-m", "pytest", "-q"),
        output_root=tmp_path / "benchmarks",
        max_mutants=1,
        worker_counts=(1,),
        repetitions=3,
    )
    RepetitionFakeCoordinator.configurations = []
    report = run_project_benchmark(request, coordinator_factory=RepetitionFakeCoordinator)
    assert report["completed"] is True
    assert len(report["scenarios"]) == 6
    assert [item["repetition"] for item in report["scenarios"]] == [1, 2, 3, 1, 2, 3]
    authority = report["performance_authority"]
    assert authority["requested_repetitions"] == 3
    assert authority["sufficient_repetitions"] is True
    assert authority["lane_count"] == 2
    assert authority["sufficient_lane_count"] == 2
    cold = next(item for item in authority["lanes"] if item["mode"] == "cold")
    wall = next(item for item in cold["summary"]["metrics"] if item["name"] == "wall_seconds")
    assert cold["physical_run_count"] == 3
    assert cold["summary"]["repetition_status"] == "sufficient"
    assert wall["median"] > 0.0
    assert len({item["run_id"] for item in report["scenarios"]}) == 6
    assert len(RepetitionFakeCoordinator.configurations) == 6
