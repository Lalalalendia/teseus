from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from test_intelligence_unified_v1 import cli
from test_intelligence_unified_v1.test_stats import compare_test_runs, ingest_test_stats, merge_test_stats_databases, stats_db_path


def _write_scope_events(events: Path) -> None:
    # Create equal production runs plus a mutant-only failure for phase isolation checks.
    events.mkdir()
    payload = [
        {
            "event_id": "scope-before-baseline",
            "run_id": "before",
            "phase": "baseline",
            "nodeid": "tests/test_app.py::test_scope",
            "outcome": "passed",
            "duration_ms": 10,
        },
        {
            "event_id": "scope-after-baseline",
            "run_id": "after",
            "phase": "baseline",
            "nodeid": "tests/test_app.py::test_scope",
            "outcome": "passed",
            "duration_ms": 11,
        },
        {
            "event_id": "scope-after-mutant",
            "run_id": "after",
            "phase": "mutant",
            "mutant_id": "m1",
            "nodeid": "tests/test_app.py::test_scope",
            "outcome": "failed",
            "first_failure": True,
            "duration_ms": 12,
        },
    ]
    (events / "scope.jsonl").write_text("".join(json.dumps(item) + "\n" for item in payload), encoding="utf-8")


def test_e02_compare_excludes_mutants_by_default_and_supports_explicit_scope(tmp_path: Path) -> None:
    # Keep mutation kills out of the production regression gate unless the caller opts in.
    root = tmp_path / "project"
    root.mkdir()
    events = tmp_path / "events"
    _write_scope_events(events)
    db = stats_db_path(tmp_path / "reports")
    ingest_test_stats(events, db, project_root=root, run_id="ingest", source_path="app.py")

    default_rows = compare_test_runs(db, "before", "after", project_root=root, limit=20)
    assert default_rows[0]["status"] == "stable"
    assert default_rows[0]["before_executions"] == 1
    assert default_rows[0]["after_executions"] == 1
    assert default_rows[0]["after_failed"] == 0

    mutant_rows = compare_test_runs(db, "before", "after", project_root=root, phases=("mutant",), limit=20)
    assert mutant_rows[0]["status"] == "new"
    assert mutant_rows[0]["before_executions"] == 0
    assert mutant_rows[0]["after_failed"] == 1

    all_rows = compare_test_runs(db, "before", "after", project_root=root, phases=("baseline", "mutant"), limit=20)
    assert all_rows[0]["status"] == "regressed"
    assert all_rows[0]["delta_failures"] == 1


def test_e02_cli_reports_compare_phase_scope(tmp_path: Path, monkeypatch, capsys) -> None:
    # Publish the exact phase scope in machine-readable output for safe downstream gates.
    captured: dict[str, object] = {}

    def fake_compare(*args, **kwargs):
        # Avoid SQLite setup while asserting the CLI-to-query phase contract.
        del args
        captured["phases"] = kwargs["phases"]
        return []

    monkeypatch.setattr(cli, "compare_test_runs", fake_compare)
    code = cli.main(
        [
            "stats",
            str(tmp_path),
            "--compare",
            "before",
            "after",
            "--compare-phase",
            "mutant",
            "--format",
            "json",
        ]
    )
    value = json.loads(capsys.readouterr().out)

    assert code == 0
    assert captured["phases"] == ("mutant",)
    assert value["summary"]["phases"] == ["mutant"]


def test_e02_regression_gate_rejects_mutant_scope(tmp_path: Path) -> None:
    # Prevent mutant-kill comparisons from being accidentally used as a production gate.
    with pytest.raises(SystemExit):
        cli.main(
            [
                "stats",
                str(tmp_path),
                "--compare",
                "before",
                "after",
                "--compare-phase",
                "mutant",
                "--fail-on-regression",
                "--json",
            ]
        )


def test_e02_merge_identity_survives_database_relocation(tmp_path: Path) -> None:
    # Make repeated worker merge idempotent even when the source database changes path.
    root = tmp_path / "project"
    root.mkdir()
    events = tmp_path / "events"
    events.mkdir()
    (events / "worker.jsonl").write_text(
        json.dumps(
            {
                "event_id": "relocation-event",
                "run_id": "worker-run",
                "phase": "baseline",
                "nodeid": "tests/test_app.py::test_relocation",
                "outcome": "passed",
                "duration_ms": 1,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    source_db = stats_db_path(tmp_path / "worker-reports")
    ingest_test_stats(events, source_db, project_root=root, run_id="worker-run", source_path="app.py")
    relocated_db = tmp_path / "relocated.sqlite"
    shutil.copy2(source_db, relocated_db)
    target_db = stats_db_path(tmp_path / "merged-reports")

    first = merge_test_stats_databases([source_db], target_db, project_root=root)
    second = merge_test_stats_databases([relocated_db], target_db, project_root=root)

    assert first["events_ingested"] == 1
    assert second["events_ingested"] == 0
    with sqlite3.connect(target_db) as connection:
        assert connection.execute("SELECT COUNT(*) FROM test_attempts").fetchone()[0] == 1


def test_e02_fallback_event_identity_ignores_journal_path(tmp_path: Path) -> None:
    # Keep legacy journals idempotent when the same file is ingested from a new directory.
    root = tmp_path / "project"
    root.mkdir()
    original_dir = tmp_path / "original-events"
    original_dir.mkdir()
    event = {
        "run_id": "run",
        "phase": "baseline",
        "nodeid": "tests/test_app.py::test_fallback",
        "outcome": "passed",
        "duration_ms": 1,
    }
    (original_dir / "worker.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")
    copied_dir = tmp_path / "copied-events"
    copied_dir.mkdir()
    shutil.copy2(original_dir / "worker.jsonl", copied_dir / "renamed.jsonl")
    db = stats_db_path(tmp_path / "reports")

    first = ingest_test_stats(original_dir, db, project_root=root, run_id="run", source_path="app.py")
    second = ingest_test_stats(copied_dir, db, project_root=root, run_id="run", source_path="app.py")

    assert first["events_ingested"] == 1
    assert second["events_ingested"] == 0
