import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from test_intelligence_unified_v1.pytest_plugin import _TestStatsPlugin
from test_intelligence_unified_v1.index import build_index, plan_selection
from test_intelligence_unified_v1.test_stats import (
    ingest_test_stats,
    instrument_pytest_command,
    load_selection_stats,
    stats_db_path,
    summarize_test_stats,
)


def test_pytest_plugin_writes_one_event_per_test(tmp_path: Path) -> None:
    # Emit a failed test after setup/call/teardown aggregation without stdout payloads.
    with patch.dict(
        os.environ,
        {
            "TI_TEST_STATS_OUT_DIR": str(tmp_path / "events"),
            "TI_TEST_STATS_RUN_ID": "run-1",
            "TI_TEST_STATS_PHASE": "mutant",
            "TI_TEST_STATS_LEVEL": "L1",
            "TI_TEST_STATS_MUTANT_ID": "m4:await_to_expression:test",
            "TI_TEST_STATS_SOURCE_PATH": "app.py",
            "TI_TEST_STATS_TARGET_SHA256": "abc",
        },
        clear=False,
    ):
        plugin = _TestStatsPlugin()
        for when, outcome in (("setup", "passed"), ("call", "failed"), ("teardown", "passed")):
            plugin.pytest_runtest_logreport(
                SimpleNamespace(
                    nodeid="tests/test_app.py::test_load",
                    when=when,
                    outcome=outcome,
                    duration=0.01,
                    wasxfail=False,
                )
            )
        plugin.pytest_sessionfinish(SimpleNamespace(), 1)
    journals = list((tmp_path / "events").glob("*.jsonl"))
    assert len(journals) == 1
    event = json.loads(journals[0].read_text(encoding="utf-8"))
    assert event["nodeid"] == "tests/test_app.py::test_load"
    assert event["outcome"] == "failed"
    assert event["first_failure"] is True
    assert event["mutant_id"].startswith("m4:")


def test_stats_ingestion_builds_historical_test_rows(tmp_path: Path) -> None:
    # Ingest repeated test events and expose kill-rate, failure and percentile metrics.
    root = tmp_path / "project"
    root.mkdir()
    events = tmp_path / "events"
    events.mkdir()
    payloads = [
        {
            "event_id": "one",
            "run_id": "run-1",
            "phase": "baseline",
            "level": "L1",
            "nodeid": "tests/test_app.py::test_load",
            "outcome": "passed",
            "duration_ms": 10,
            "first_failure": False,
        },
        {
            "event_id": "two",
            "run_id": "run-2",
            "phase": "mutant",
            "level": "L1",
            "mutant_id": "m1",
            "nodeid": "tests/test_app.py::test_load",
            "outcome": "failed",
            "duration_ms": 20,
            "first_failure": True,
        },
        {
            "event_id": "three",
            "run_id": "run-2",
            "phase": "mutant",
            "level": "L1",
            "mutant_id": "m2",
            "nodeid": "tests/test_app.py::test_load",
            "outcome": "passed",
            "duration_ms": 30,
            "first_failure": False,
        },
    ]
    (events / "run.jsonl").write_text(
        "".join(json.dumps(item) + "\n" for item in payloads),
        encoding="utf-8",
    )
    db = stats_db_path(tmp_path / "reports")
    result = ingest_test_stats(events, db, project_root=root, run_id="run", source_path="app.py")
    assert result["events_ingested"] == 3
    rows = summarize_test_stats(db, project_root=root)
    assert rows[0]["executions"] == 3
    assert rows[0]["failed"] == 1
    assert rows[0]["mutant_attempts"] == 2
    assert rows[0]["mutant_kills"] == 1
    assert rows[0]["kill_rate"] == 0.5
    assert rows[0]["median_ms"] == 20.0
    assert load_selection_stats(db, root)["tests/test_app.py::test_load"]["test_kills"] == 1


def test_pytest_command_gets_plugin_before_delimiter() -> None:
    # Keep the safe argv contract while injecting stats before pytest arguments end.
    command = instrument_pytest_command(("python", "-m", "pytest", "tests", "--", "-k", "load"))
    plugin_index = command.index("-p")
    assert command[plugin_index + 1] == "test_intelligence_unified_v1.pytest_plugin"
    assert plugin_index == 3
    assert command[5] == "tests"
    assert command[-3:] == ("--", "-k", "load")


def test_selection_uses_historical_test_stats(tmp_path: Path) -> None:
    # Prefer a historically effective test while preserving the explicit candidate set.
    root = tmp_path / "project"
    root.mkdir()
    (root / "app.py").write_text("def choose(value):\n    return value\n", encoding="utf-8")
    tests_dir = root / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_app.py").write_text(
        "def test_fast():\n    pass\n\ndef test_slow():\n    pass\n",
        encoding="utf-8",
    )
    index = build_index(root, root / "index.sqlite")
    selected = root / "selected.txt"
    selected.write_text("tests/test_app.py::test_slow\ntests/test_app.py::test_fast\n", encoding="utf-8")
    events = tmp_path / "events"
    events.mkdir()
    (events / "history.jsonl").write_text(
        json.dumps(
            {
                "event_id": "kill",
                "phase": "mutant",
                "level": "L1",
                "mutant_id": "m1",
                "nodeid": "tests/test_app.py::test_fast",
                "outcome": "failed",
                "duration_ms": 5,
                "first_failure": True,
            }
        )
        + "\n"
        + json.dumps(
            {
                "event_id": "pass",
                "phase": "mutant",
                "level": "L1",
                "mutant_id": "m2",
                "nodeid": "tests/test_app.py::test_slow",
                "outcome": "passed",
                "duration_ms": 50,
                "first_failure": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    db = stats_db_path(root / "reports")
    ingest_test_stats(events, db, project_root=root, run_id="history", source_path="app.py")
    snapshot = plan_selection(
        root,
        index,
        "app.py",
        "choose",
        selected_tests_file=selected,
        test_stats_db=db,
    )
    assert snapshot.selected_tests == (
        "tests/test_app.py::test_fast",
        "tests/test_app.py::test_slow",
    )
