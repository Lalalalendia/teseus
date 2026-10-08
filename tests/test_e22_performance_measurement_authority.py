from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import tracemalloc

import pytest

from theseus_performance import (
    STANDARD_PHASES,
    PerformanceCollector,
    PerformanceConflictError,
    PerformanceStore,
    RegressionBudget,
    WorkloadIdentity,
    performance_run_from_benchmark_report,
    performance_run_from_runner_report,
)


def _identity(*, environment: str = "env-a") -> WorkloadIdentity:
    # Build one exact comparison boundary shared by authority tests.
    return WorkloadIdentity(
        workload_name="offline-mutation-campaign",
        workload_version="workload-v1",
        project_key="project-fixture",
        input_fingerprint="input-fixture",
        environment_fingerprint=environment,
        runtime_fingerprint="python-3.14-theseus-1.29.6",
    )


def _run(run_id: str, wall_seconds: float, *, environment: str = "env-a"):
    # Build one completed run with an observed total wall metric.
    collector = PerformanceCollector(_identity(environment=environment), run_id=run_id)
    collector.observe("wall_seconds", wall_seconds, unit="seconds", source="fixture")
    return collector.finish()


def test_collector_keeps_observed_and_unavailable_evidence_distinct() -> None:
    # Measure one phase while keeping every disconnected source explicitly unavailable.
    collector = PerformanceCollector(_identity(), run_id="run-collector")
    with collector.phase("planning"):
        sum(range(100))
    collector.observe("process_spawn_count", 2, unit="count", source="fixture")
    result = collector.finish(metadata={"fixture": "collector"})
    phases = {item.phase: item for item in result.phases}
    metrics = result.metric_map()
    assert tuple(item.phase for item in result.phases[: len(STANDARD_PHASES)]) == STANDARD_PHASES
    assert phases["planning"].status == "observed"
    assert phases["test_collection"].status == "unavailable"
    assert metrics["process_spawn_count"].value == 2.0
    assert metrics["sqlite_query_count"].status == "unavailable"
    assert metrics["peak_memory_bytes"].status == "observed"


def test_comparison_identity_fences_environment_and_ignores_measurement_values() -> None:
    # Compare only exact workload inputs while allowing repeated measurements to differ.
    first = _run("run-a", 1.0)
    second = _run("run-b", 2.0)
    other_environment = _run("run-c", 1.0, environment="env-b")
    assert first.identity.comparison_key == second.identity.comparison_key
    assert first.content_sha256 != second.content_sha256
    assert other_environment.identity.comparison_key != first.identity.comparison_key


def test_store_is_append_only_conflict_safe_and_keyset_bounded(tmp_path: Path) -> None:
    # Persist exact replay once and reject one reused run identity with different content.
    store = PerformanceStore(tmp_path / "performance.sqlite3")
    first = _run("run-one", 1.0)
    second = _run("run-two", 1.1)
    third = _run("run-three", 1.2)
    assert store.append(first) is True
    assert store.append(first) is False
    with pytest.raises(PerformanceConflictError):
        store.append(replace(first, metrics=second.metrics))
    store.append(second)
    store.append(third)
    page_one, cursor = store.list_runs(first.identity.comparison_key, limit=2)
    assert [item.run_id for item in page_one] == ["run-one", "run-two"]
    assert cursor is not None
    page_two, next_cursor = store.list_runs(first.identity.comparison_key, limit=2, after_sequence=cursor)
    assert [item.run_id for item in page_two] == ["run-three"]
    assert next_cursor is None


def test_regression_gate_requires_pinned_baseline_and_explicit_budget(tmp_path: Path) -> None:
    # Remain observational until a measured baseline and threshold are deliberately configured.
    store = PerformanceStore(tmp_path / "performance.sqlite3")
    baseline = _run("baseline", 1.0)
    current = _run("current", 1.2)
    store.append(baseline)
    store.append(current)
    assert store.evaluate(current).status == "no_baseline"
    store.pin_baseline(baseline.run_id)
    assert store.evaluate(current).status == "observe_only"
    store.set_budget(
        current.identity.comparison_key,
        RegressionBudget("wall_seconds", max_relative_regression=0.10),
    )
    report = store.evaluate(current)
    assert report.status == "regressed"
    assert report.blocking is True
    assert report.findings[0].baseline_value == 1.0
    assert report.findings[0].current_value == 1.2


def test_runner_adapter_maps_existing_metrics_without_fake_cpu_or_memory() -> None:
    # Reuse current runner evidence while preserving unavailable fields as unavailable.
    report = {
        "status": "complete",
        "metrics": {
            "performance": {
                "campaign_wall_seconds": 4.0,
                "index_seconds": 0.5,
                "selection_seconds": 0.25,
                "mutant_generation_seconds": 0.1,
                "pytest_seconds": 2.0,
                "test_stats_ingestion_seconds": 0.2,
                "report_materialization_seconds": 0.15,
                "processes_started": 3,
            }
        },
    }
    run = performance_run_from_runner_report(report, identity=_identity(), run_id="runner-adapter")
    phases = {item.phase: item for item in run.phases}
    metrics = run.metric_map()
    assert phases["project_scan"].wall_seconds.value == 0.5
    assert phases["project_scan"].cpu_seconds.status == "unavailable"
    assert phases["test_collection"].status == "unavailable"
    assert metrics["process_spawn_count"].value == 3.0
    assert metrics["wall_seconds"].value == 4.0
    assert metrics["cpu_seconds"].status == "unavailable"
    assert metrics["peak_memory_bytes"].status == "unavailable"


def test_benchmark_adapter_preserves_workload_semantics_without_phase_guessing() -> None:
    # Import existing microbenchmarks as named evidence without claiming end-to-end phase timing.
    report = {
        "benchmark_version": "v1.26",
        "workload_order": ["index_cold_build", "small_e2e_campaign"],
        "workloads": {
            "index_cold_build": {"elapsed_seconds": 0.2, "details": {"files": 3}},
            "small_e2e_campaign": {"elapsed_seconds": 1.0, "details": {"status": "complete"}},
        },
    }
    run = performance_run_from_benchmark_report(report, identity=_identity(), run_id="benchmark-adapter")
    metrics = run.metric_map()
    assert metrics["workload.index_cold_build.wall_seconds"].value == 0.2
    assert metrics["workload.small_e2e_campaign.wall_seconds"].value == 1.0
    assert metrics["phase.project_scan.wall_seconds"].status == "unavailable"
    assert metrics["wall_seconds"].status == "unavailable"
    assert run.metadata["benchmark_version"] == "v1.26"


def test_higher_is_better_budget_detects_a_drop(tmp_path: Path) -> None:
    # Apply the configured metric direction instead of treating every increase as a regression.
    store = PerformanceStore(tmp_path / "performance.sqlite3")
    baseline_collector = PerformanceCollector(_identity(), run_id="utilization-baseline")
    baseline_collector.observe("worker_utilization", 0.90, unit="ratio", source="fixture")
    baseline = baseline_collector.finish()
    current_collector = PerformanceCollector(_identity(), run_id="utilization-current")
    current_collector.observe("worker_utilization", 0.70, unit="ratio", source="fixture")
    current = current_collector.finish()
    store.append(baseline)
    store.append(current)
    store.pin_baseline(baseline.run_id)
    store.set_budget(
        current.identity.comparison_key,
        RegressionBudget(
            "worker_utilization",
            direction="higher_is_better",
            max_absolute_regression=0.10,
        ),
    )
    assert store.evaluate(current).status == "regressed"


def test_collector_does_not_reset_or_stop_external_tracemalloc() -> None:
    # Respect external memory tracing and report isolated memory evidence as unavailable.
    tracemalloc.start()
    try:
        collector = PerformanceCollector(_identity(), run_id="external-trace")
        with collector.phase("planning"):
            bytearray(128)
        run = collector.finish()
        phase = next(item for item in run.phases if item.phase == "planning")
        metrics = run.metric_map()
        assert tracemalloc.is_tracing() is True
        assert phase.peak_memory_bytes.status == "unavailable"
        assert metrics["peak_memory_bytes"].status == "unavailable"
    finally:
        tracemalloc.stop()
