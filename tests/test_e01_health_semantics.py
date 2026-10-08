from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from test_intelligence_unified_v1.models import TestHealthStatus as HealthStatus, TestOutcome as Outcome
from test_intelligence_unified_v1.pytest_plugin import _TestStatsPlugin
from test_intelligence_unified_v1.test_stats import (
    classify_test_health,
    ingest_test_stats,
    normalize_test_outcome,
    stats_db_path,
    summarize_test_health,
    summarize_test_stats,
)


def _write_health_events(root: Path, payload: dict[str, list[str]]) -> Path:
    # Create deterministic baseline events for every health category without mutant phases.
    project_root = root / "project"
    project_root.mkdir()
    event_dir = root / "events"
    event_dir.mkdir()
    rows: list[dict[str, object]] = []
    for nodeid, outcomes in sorted(payload.items()):
        for attempt, outcome in enumerate(outcomes):
            rows.append(
                {
                    "event_id": f"{nodeid}:{attempt}",
                    "run_id": f"run-{attempt}",
                    "phase": "baseline",
                    "level": "L1",
                    "nodeid": nodeid,
                    "outcome": outcome,
                    "duration_ms": attempt + 1,
                    "recorded_at": f"2026-08-03T00:00:{attempt:02d}+00:00",
                }
            )
    (event_dir / "health.jsonl").write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
    return project_root


def _emit_plugin_event(root: Path, name: str, outcome: str, *, was_xfail: bool) -> dict[str, object]:
    # Exercise pytest's xfail/xpass metadata through the real session plugin boundary.
    output_dir = root / name
    with patch.dict(
        os.environ,
        {
            "TI_TEST_STATS_OUT_DIR": str(output_dir),
            "TI_TEST_STATS_RUN_ID": name,
            "TI_TEST_STATS_PHASE": "baseline",
            "TI_TEST_STATS_LEVEL": "L1",
            "TI_TEST_STATS_SOURCE_PATH": "app.py",
        },
        clear=False,
    ):
        plugin = _TestStatsPlugin()
        for when, report_outcome, report_was_xfail in (
            ("setup", "passed", False),
            ("call", outcome, was_xfail),
            ("teardown", "passed", False),
        ):
            plugin.pytest_runtest_logreport(
                SimpleNamespace(
                    nodeid=f"tests/test_app.py::{name}",
                    when=when,
                    outcome=report_outcome,
                    duration=0.001,
                    wasxfail=report_was_xfail,
                )
            )
        plugin.pytest_sessionfinish(SimpleNamespace(), 0)
    journal = next(output_dir.glob("*.jsonl"))
    return json.loads(journal.read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    ("raw", "was_xfail", "expected"),
    (
        ("passed", False, Outcome.PASSED.value),
        ("pass", False, Outcome.PASSED.value),
        ("failed", False, Outcome.FAILED.value),
        ("error", False, Outcome.ERROR.value),
        ("skipped", False, Outcome.SKIPPED.value),
        ("skipped", True, Outcome.XFAILED.value),
        ("passed", True, Outcome.XPASSED.value),
        ("cancel", False, Outcome.CANCELLED.value),
        ("timed_out", False, Outcome.TIMEOUT.value),
        ("something-new", False, Outcome.UNKNOWN.value),
        (None, False, Outcome.UNKNOWN.value),
    ),
)
def test_e01_normalizes_all_supported_outcomes(raw: object, was_xfail: bool, expected: str) -> None:
    # Keep the journal vocabulary closed and stable for old and new event producers.
    assert normalize_test_outcome(raw, was_xfail=was_xfail) == expected


def test_e01_plugin_emits_xfailed_and_xpassed_outcomes(tmp_path: Path) -> None:
    # Preserve expected-failure semantics instead of collapsing them into skipped/passed.
    xfailed = _emit_plugin_event(tmp_path, "xfailed", "skipped", was_xfail=True)
    xpassed = _emit_plugin_event(tmp_path, "xpassed", "passed", was_xfail=True)
    errored = _emit_plugin_event(tmp_path, "errored", "error", was_xfail=False)
    assert xfailed["outcome"] == Outcome.XFAILED.value
    assert xpassed["outcome"] == Outcome.XPASSED.value
    assert errored["outcome"] == Outcome.ERROR.value


def test_e01_health_classification_does_not_call_errors_or_skips_healthy(tmp_path: Path) -> None:
    # Prove every semantic health state and every explicit execution counter through SQLite.
    payload = {
        "tests/test_app.py::test_healthy": ["passed", "passed"],
        "tests/test_app.py::test_flaky": ["passed", "failed"],
        "tests/test_app.py::test_failing": ["failed", "failed"],
        "tests/test_app.py::test_erroring": ["error", "error"],
        "tests/test_app.py::test_skipped": ["skipped", "skipped"],
        "tests/test_app.py::test_xfailed": ["xfailed", "xfailed"],
        "tests/test_app.py::test_xpassed": ["xpassed", "xpassed"],
        "tests/test_app.py::test_timeout": ["timeout", "timeout"],
        "tests/test_app.py::test_unknown": ["unknown", "unknown"],
        "tests/test_app.py::test_never_passed": ["failed", "skipped"],
        "tests/test_app.py::test_insufficient": ["passed"],
        "tests/test_app.py::test_error_flaky": ["passed", "error"],
    }
    project_root = _write_health_events(tmp_path, payload)
    db = stats_db_path(tmp_path / "reports")
    ingest_test_stats(tmp_path / "events", db, project_root=project_root, run_id="health", source_path="app.py")

    rows = {row["nodeid"]: row for row in summarize_test_stats(db, project_root=project_root, order_by="nodeid", limit=100)}
    expected_statuses = {
        "test_healthy": HealthStatus.HEALTHY.value,
        "test_flaky": HealthStatus.FLAKY.value,
        "test_failing": HealthStatus.FAILING.value,
        "test_erroring": HealthStatus.ERRORING.value,
        "test_skipped": HealthStatus.MOSTLY_SKIPPED.value,
        "test_xfailed": HealthStatus.MOSTLY_SKIPPED.value,
        "test_xpassed": HealthStatus.HEALTHY.value,
        "test_timeout": HealthStatus.ERRORING.value,
        "test_unknown": HealthStatus.UNKNOWN.value,
        "test_never_passed": HealthStatus.NEVER_PASSED.value,
        "test_insufficient": HealthStatus.INSUFFICIENT_DATA.value,
        "test_error_flaky": HealthStatus.FLAKY.value,
    }
    for suffix, expected in expected_statuses.items():
        row = rows[f"tests/test_app.py::{suffix}"]
        assert row["health_status"] == expected
        assert row["health_status"] != HealthStatus.HEALTHY.value or suffix in {"test_healthy", "test_xpassed"}

    assert rows["tests/test_app.py::test_erroring"]["errors"] == 2
    assert rows["tests/test_app.py::test_erroring"]["health_error_executions"] == 2
    assert rows["tests/test_app.py::test_skipped"]["skipped"] == 2
    assert rows["tests/test_app.py::test_skipped"]["health_skipped_executions"] == 2
    assert rows["tests/test_app.py::test_timeout"]["timeout"] == 2
    assert rows["tests/test_app.py::test_timeout"]["health_timeout_executions"] == 2
    assert rows["tests/test_app.py::test_unknown"]["unknown"] == 2
    assert rows["tests/test_app.py::test_unknown"]["health_unknown_executions"] == 2

    summary = summarize_test_health(db, project_root=project_root)
    assert summary["healthy"] == 2
    assert summary["flaky"] == 2
    assert summary["erroring"] == 2
    assert summary["mostly_skipped"] == 2
    assert summary["passed_executions"] == 7
    assert summary["error_executions"] == 3
    assert summary["skipped_executions"] == 5
    assert summary["timeout_executions"] == 2
    assert summary["total_considered_executions"] == sum(len(values) for values in payload.values())


def test_e01_old_sqlite_rows_keep_correct_health_semantics(tmp_path: Path) -> None:
    # Read a schema-v2 database without rewriting its historical outcome rows.
    project_root = (tmp_path / "project").resolve()
    project_root.mkdir()
    db = tmp_path / "legacy.sqlite"
    with sqlite3.connect(db) as connection:
        connection.executescript(
            """
            CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO metadata(key, value) VALUES ('schema_version', '2');
            CREATE TABLE test_attempts (
                event_key TEXT PRIMARY KEY,
                project_root TEXT NOT NULL,
                run_id TEXT NOT NULL,
                source_path TEXT NOT NULL,
                target_sha256 TEXT,
                phase TEXT NOT NULL,
                level TEXT NOT NULL,
                mutant_id TEXT,
                nodeid TEXT NOT NULL,
                outcome TEXT NOT NULL,
                duration_ms REAL NOT NULL,
                first_failure INTEGER NOT NULL,
                worker_id TEXT NOT NULL,
                retry INTEGER NOT NULL,
                recorded_at TEXT NOT NULL
            );
            """
        )
        connection.executemany(
            "INSERT INTO test_attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("skip", str(project_root), "run", "app.py", None, "baseline", "L1", None, "test_skip", "skipped", 1, 0, "main", 0, "2026-08-03T00:00:00+00:00"),
                ("error", str(project_root), "run", "app.py", None, "baseline", "L1", None, "test_error", "error", 1, 0, "main", 0, "2026-08-03T00:00:01+00:00"),
            ],
        )
        connection.commit()

    rows = {row["nodeid"]: row for row in summarize_test_stats(db, project_root=project_root, limit=10, order_by="nodeid")}
    assert rows["test_skip"]["health_status"] == HealthStatus.MOSTLY_SKIPPED.value
    assert rows["test_error"]["health_status"] == HealthStatus.ERRORING.value


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    (
        ({"total_executions": 0, "passed_executions": 0, "failed_executions": 0, "error_executions": 0, "skipped_executions": 0, "timeout_executions": 0}, HealthStatus.INSUFFICIENT_DATA.value),
        ({"total_executions": 4, "passed_executions": 0, "failed_executions": 0, "error_executions": 4, "skipped_executions": 0, "timeout_executions": 0}, HealthStatus.ERRORING.value),
        ({"total_executions": 7, "passed_executions": 0, "failed_executions": 0, "error_executions": 0, "skipped_executions": 7, "timeout_executions": 0}, HealthStatus.MOSTLY_SKIPPED.value),
    ),
)
def test_e01_classifier_examples_are_explicit(kwargs: dict[str, int], expected: str) -> None:
    # Lock the roadmap examples independently from SQLite and report formatting.
    assert classify_test_health(**kwargs) == expected
