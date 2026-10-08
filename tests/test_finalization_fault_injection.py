import json
from pathlib import Path

import pytest

from test_intelligence_unified_v1 import runner as runner_module
from test_intelligence_unified_v1 import test_stats as stats_module
from test_intelligence_unified_v1.mutations import create_snapshot, write_manifest
from test_intelligence_unified_v1.runner import MutationConfig, MutationRunner
from test_intelligence_unified_v1.test_stats import (
    ingest_test_stats,
    open_test_stats_connection,
)


def test_stats_ingestion_rolls_back_reused_connection(tmp_path: Path, monkeypatch) -> None:
    # Remove already inserted rows when a later batch fails on a campaign connection.
    events = tmp_path / "events"
    events.mkdir()
    rows = [
        {
            "event_id": f"event-{index}",
            "run_id": "run-fault",
            "phase": "baseline",
            "level": "L1",
            "nodeid": f"tests/test_app.py::test_{index}",
            "outcome": "passed",
            "duration_ms": 1.0,
        }
        for index in range(2)
    ]
    (events / "run.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )
    connection = open_test_stats_connection(tmp_path / "stats.sqlite")
    monkeypatch.setattr(stats_module, "STATS_INSERT_BATCH_SIZE", 1)
    original_insert = stats_module._insert_stats_batch
    calls = {"count": 0}

    def fail_second_batch(db_connection, batch):
        # Inject a failure after the first batch has reached SQLite.
        calls["count"] += 1
        if calls["count"] == 2:
            raise RuntimeError("injected stats ingestion failure")
        return original_insert(db_connection, batch)

    monkeypatch.setattr(stats_module, "_insert_stats_batch", fail_second_batch)
    try:
        with pytest.raises(RuntimeError, match="injected stats ingestion failure"):
            ingest_test_stats(
                events,
                tmp_path / "stats.sqlite",
                project_root=tmp_path,
                run_id="run-fault",
                source_path="app.py",
                connection=connection,
            )
        count = connection.execute("SELECT COUNT(*) FROM test_attempts").fetchone()[0]
    finally:
        connection.close()

    assert count == 0


def test_manifest_finalization_failure_is_visible_in_report(tmp_path: Path, monkeypatch) -> None:
    # Never publish a successful campaign when the critical manifest finalization fails.
    target = tmp_path / "app.py"
    target.write_text("value = 1\n", encoding="utf-8")
    reports = tmp_path / "reports"
    snapshot = create_snapshot(target, reports / "recovery")
    manifest_path = reports / "run.manifest.json"
    write_manifest(manifest_path, snapshot)
    report_path = reports / "run.json"
    runner = MutationRunner(
        MutationConfig(project_root=tmp_path, source="app.py", reports_dir=reports)
    )
    runner._results_path = reports / "run.results.jsonl"
    runner._state_path = reports / "run.state.json"
    original_atomic_write_json = runner_module.atomic_write_json

    def fail_manifest_write(path, value, **kwargs):
        # Fail only the recovery boundary while allowing report checkpoints through.
        if Path(path).resolve() == manifest_path.resolve():
            raise OSError("injected finalization failure")
        return original_atomic_write_json(path, value, **kwargs)

    monkeypatch.setattr(runner_module, "atomic_write_json", fail_manifest_write)
    report = {
        "run_id": "run",
        "target": {"source_path": "app.py", "function_id": None},
        "results": [],
    }

    runner._finish_report(report_path, report, manifest_path, status="complete")

    saved_report = json.loads(report_path.read_text(encoding="utf-8"))
    saved_state = json.loads((reports / "run.state.json").read_text(encoding="utf-8"))
    saved_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert report["status"] == "finalization_error"
    assert saved_report["status"] == "finalization_error"
    assert saved_state["status"] == "finalization_error"
    assert "injected finalization failure" in report["error"]
    assert saved_manifest["status"] == "active"


def test_restore_error_status_survives_manifest_finalization_failure(tmp_path: Path, monkeypatch) -> None:
    # Keep the more severe restore failure visible if final manifest persistence also fails.
    target = tmp_path / "app.py"
    target.write_text("value = 1\n", encoding="utf-8")
    reports = tmp_path / "reports"
    snapshot = create_snapshot(target, reports / "recovery")
    manifest_path = reports / "run.manifest.json"
    write_manifest(manifest_path, snapshot)
    runner = MutationRunner(MutationConfig(project_root=tmp_path, source="app.py", reports_dir=reports))
    runner._results_path = reports / "run.results.jsonl"
    runner._state_path = reports / "run.state.json"
    original_atomic_write_json = runner_module.atomic_write_json

    def fail_manifest_write(path, value, **kwargs):
        # Leave all non-manifest checkpoints writable while the manifest is unavailable.
        if Path(path).resolve() == manifest_path.resolve():
            raise OSError("injected restore finalization failure")
        return original_atomic_write_json(path, value, **kwargs)

    monkeypatch.setattr(runner_module, "atomic_write_json", fail_manifest_write)
    report = {
        "run_id": "run",
        "target": {"source_path": "app.py", "function_id": None},
        "results": [],
        "error": "target restore failed",
    }

    runner._finish_report(report_path=reports / "run.json", report=report, manifest_path=manifest_path, status="restore_error")

    assert report["status"] == "restore_error"
    assert report["finalization_error"] == "injected restore finalization failure"
    assert "target restore failed" in report["error"]
