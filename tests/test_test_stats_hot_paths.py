import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from test_intelligence_unified_v1.pytest_plugin import _TestStatsPlugin
from test_intelligence_unified_v1 import test_stats as stats_module
from test_intelligence_unified_v1.test_stats import (
    STATS_DURATION_SAMPLE_SIZE,
    ingest_test_stats,
    merge_test_stats_databases,
    stats_db_path,
    summarize_test_stats,
)
def test_pytest_stats_plugin_reuses_one_line_buffered_handle(tmp_path: Path) -> None:
    # Keep one line-buffered journal descriptor for the entire pytest session.
    with patch.dict(
        os.environ,
        {
            "TI_TEST_STATS_OUT_DIR": str(tmp_path / "events"),
            "TI_TEST_STATS_RUN_ID": "run-hot",
            "TI_TEST_STATS_PHASE": "baseline",
            "TI_TEST_STATS_LEVEL": "L1",
            "TI_TEST_STATS_ATTEMPT": "attempt-hot",
        },
        clear=False,
    ):
        plugin = _TestStatsPlugin()
        handle = plugin._handle
        assert handle.line_buffering is True
        for index in range(3):
            nodeid = f"tests/test_app.py::test_{index}"
            for when, outcome in (("setup", "passed"), ("call", "passed"), ("teardown", "passed")):
                plugin.pytest_runtest_logreport(
                    SimpleNamespace(
                        nodeid=nodeid,
                        when=when,
                        outcome=outcome,
                        duration=0.001,
                        wasxfail=False,
                    )
                )
        assert plugin._handle is handle
        plugin.pytest_sessionfinish(SimpleNamespace(), 0)
    assert handle.closed is True
    journals = list((tmp_path / "events").glob("*.jsonl"))
    assert len(journals) == 1
    assert len(journals[0].read_text(encoding="utf-8").splitlines()) == 3
def test_stats_ingestion_streams_jsonl_and_uses_bounded_batches(tmp_path: Path, monkeypatch) -> None:
    # Stream journal lines into executemany-sized batches without loading the file into memory.
    root = tmp_path / "project"
    root.mkdir()
    events = tmp_path / "events"
    events.mkdir()
    payload = [
        {
            "event_id": f"event-{index}",
            "run_id": "run-hot",
            "phase": "baseline",
            "level": "L1",
            "nodeid": f"tests/test_app.py::test_{index}",
            "outcome": "passed",
            "duration_ms": index + 1,
        }
        for index in range(5)
    ]
    (events / "run.jsonl").write_text("".join(json.dumps(item) + "\n" for item in payload), encoding="utf-8")
    monkeypatch.setattr(stats_module, "STATS_INSERT_BATCH_SIZE", 2)
    batch_sizes: list[int] = []
    original_insert_batch = stats_module._insert_stats_batch
    def record_batch(connection, rows):
        # Record the size before ingestion clears the reusable batch list.
        batch_sizes.append(len(rows))
        return original_insert_batch(connection, rows)
    monkeypatch.setattr(stats_module, "_insert_stats_batch", record_batch)
    result = ingest_test_stats(events, stats_db_path(tmp_path / "reports"), project_root=root, run_id="run-hot", source_path="app.py")
    assert result["events_ingested"] == 5
    assert batch_sizes == [2, 2, 1]
    repeated = ingest_test_stats(events, stats_db_path(tmp_path / "reports"), project_root=root, run_id="run-hot", source_path="app.py")
    assert repeated["events_ingested"] == 0
    connection = stats_module.open_test_stats_connection(stats_db_path(tmp_path / "reports"))
    try:
        with patch.object(stats_module, "_ensure_schema", side_effect=AssertionError("schema must be initialized once")):
            persistent = ingest_test_stats(
                events,
                stats_db_path(tmp_path / "reports"),
                project_root=root,
                run_id="run-hot",
                source_path="app.py",
                connection=connection,
            )
    finally:
        connection.close()
    assert persistent["events_ingested"] == 0
def test_stats_database_merge_fetches_source_rows_in_batches(tmp_path: Path, monkeypatch) -> None:
    # Merge worker databases through the same bounded executemany path without fetchall.
    root = tmp_path / "project"
    root.mkdir()
    events = tmp_path / "events"
    events.mkdir()
    (events / "worker.jsonl").write_text(
        "".join(
            json.dumps(
                {
                    "event_id": f"worker-{index}",
                    "run_id": "worker-run",
                    "phase": "mutant",
                    "level": "L1",
                    "mutant_id": f"m-{index}",
                    "nodeid": f"tests/test_app.py::test_{index}",
                    "outcome": "failed",
                    "duration_ms": 1,
                    "first_failure": True,
                }
            )
            + "\n"
            for index in range(3)
        ),
        encoding="utf-8",
    )
    source_db = stats_db_path(tmp_path / "worker-reports")
    ingest_test_stats(events, source_db, project_root=root, run_id="worker-run", source_path="app.py")
    monkeypatch.setattr(stats_module, "STATS_INSERT_BATCH_SIZE", 2)
    batch_sizes: list[int] = []
    original_insert_batch = stats_module._insert_stats_batch
    def record_batch(connection, rows):
        # Record merge batch size before the source row list is discarded.
        batch_sizes.append(len(rows))
        return original_insert_batch(connection, rows)
    monkeypatch.setattr(stats_module, "_insert_stats_batch", record_batch)
    result = merge_test_stats_databases([source_db], stats_db_path(tmp_path / "merged-reports"), project_root=root)
    assert result["source_databases"] == 1
    assert result["events_ingested"] == 3
    assert batch_sizes == [2, 1]

def test_stats_percentiles_use_bounded_duration_samples(tmp_path: Path) -> None:
    # Keep percentile memory bounded even when one test accumulates a long execution history.
    root = tmp_path / "project"
    root.mkdir()
    events = tmp_path / "events"
    events.mkdir()
    nodeid = "tests/test_app.py::test_hot"
    (events / "history.jsonl").write_text(
        "".join(
            json.dumps(
                {
                    "event_id": f"duration-{index}",
                    "run_id": "run-duration",
                    "phase": "baseline",
                    "level": "L1",
                    "nodeid": nodeid,
                    "outcome": "passed",
                    "duration_ms": index + 1,
                }
            )
            + "\n"
            for index in range(STATS_DURATION_SAMPLE_SIZE + 100)
        ),
        encoding="utf-8",
    )
    db = stats_db_path(tmp_path / "reports")
    ingest_test_stats(events, db, project_root=root, run_id="run-duration", source_path="app.py")
    summary = summarize_test_stats(db, project_root=root, nodeid=nodeid, limit=1)[0]
    connection = stats_module.open_test_stats_connection(db)
    try:
        samples = connection.execute(
            "SELECT COUNT(*) FROM test_duration_samples WHERE project_root = ? AND nodeid = ?",
            (str(root.resolve()), nodeid),
        ).fetchone()[0]
    finally:
        connection.close()
    assert summary["executions"] == STATS_DURATION_SAMPLE_SIZE + 100
    assert 0 < samples <= STATS_DURATION_SAMPLE_SIZE
    assert summary["median_ms"] > 0.0
    assert summary["p95_ms"] >= summary["median_ms"]
