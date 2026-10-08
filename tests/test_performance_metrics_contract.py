import json
from pathlib import Path

from test_intelligence_unified_v1.io_utils import atomic_write_json
from test_intelligence_unified_v1.models import PerformanceMetrics
from test_intelligence_unified_v1.test_stats import ingest_test_stats


def test_performance_metrics_keep_legacy_totals_and_explicit_write_categories(tmp_path: Path) -> None:
    # Keep compatibility counters while making durability categories directly measurable.
    metrics = PerformanceMetrics()
    metrics.record_write(10, durability="critical", category="recovery")
    metrics.record_write(7, durability="normal", category="result_journal")
    metrics.record_write(3, durability="normal", category="unknown-category")

    payload = metrics.to_dict()

    assert payload["bytes_written"] == 20
    assert payload["report_bytes_written"] == 20
    assert payload["critical_writes"] == 1
    assert payload["normal_writes"] == 2
    assert payload["recovery_writes"] == 1
    assert payload["recovery_bytes"] == 10
    assert payload["result_journal_writes"] == 1
    assert payload["result_journal_bytes"] == 7
    assert payload["other_writes"] == 1
    assert payload["other_bytes"] == 3


def test_atomic_writer_forwards_the_selected_category(tmp_path: Path) -> None:
    # Attribute report writes at the helper boundary instead of inferring them later.
    metrics = PerformanceMetrics()
    atomic_write_json(
        tmp_path / "report.json",
        {"status": "complete"},
        durability="normal",
        category="report",
        metrics=metrics,
    )

    assert metrics.report_writes == 1
    assert metrics.report_bytes == (tmp_path / "report.json").stat().st_size
    assert metrics.report_bytes_written == metrics.report_bytes


def test_phase_totals_are_present_and_derived_without_changing_source_fields() -> None:
    # Expose stable phase fields for guardrails even when only legacy phase counters are set.
    metrics = PerformanceMetrics(index_seconds=1.5, mutant_apply_seconds=0.25, pytest_seconds=0.5)

    payload = metrics.to_dict()

    assert payload["preparation_seconds"] == 1.5
    assert payload["mutant_execution_seconds"] == 0.75
    assert payload["report_materialization_seconds"] == 0.0


def test_stats_ingestion_records_event_and_database_categories(tmp_path: Path) -> None:
    # Keep stats I/O visible while ingestion remains streaming and batch-oriented.
    project_root = tmp_path / "project"
    project_root.mkdir()
    event_dir = tmp_path / "events"
    event_dir.mkdir()
    (event_dir / "run.jsonl").write_text(
        json.dumps(
            {
                "event_id": "event-1",
                "run_id": "run-1",
                "phase": "baseline",
                "level": "L1",
                "nodeid": "tests/test_app.py::test_one",
                "outcome": "passed",
                "duration_ms": 1.0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    metrics = PerformanceMetrics()

    result = ingest_test_stats(
        event_dir,
        tmp_path / "stats.sqlite",
        project_root=project_root,
        run_id="run-1",
        source_path="app.py",
        metrics=metrics,
    )

    assert result["events_ingested"] == 1
    assert metrics.stats_event_writes == 1
    assert metrics.stats_database_writes == 1
    assert metrics.stats_event_bytes > 0
    assert metrics.stats_database_bytes > 0
