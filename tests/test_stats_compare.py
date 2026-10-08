import csv
import io
import json
from pathlib import Path

from test_intelligence_unified_v1 import cli
from test_intelligence_unified_v1.test_stats import compare_test_runs, ingest_test_stats, stats_db_path


def _write_compare_events(events: Path) -> None:
    # Create stable, recovered, regressed and newly failing tests across two runs.
    events.mkdir()
    payloads = [
        {"event_id": "before-regress", "run_id": "before", "phase": "standalone", "nodeid": "tests/test_app.py::test_regress", "outcome": "passed", "duration_ms": 10, "recorded_at": "2026-08-02T10:00:00+00:00"},
        {"event_id": "before-recover", "run_id": "before", "phase": "standalone", "nodeid": "tests/test_app.py::test_recover", "outcome": "failed", "duration_ms": 11, "recorded_at": "2026-08-02T10:00:01+00:00"},
        {"event_id": "before-stable", "run_id": "before", "phase": "standalone", "nodeid": "tests/test_app.py::test_stable", "outcome": "passed", "duration_ms": 12, "recorded_at": "2026-08-02T10:00:02+00:00"},
        {"event_id": "after-regress", "run_id": "after", "phase": "standalone", "nodeid": "tests/test_app.py::test_regress", "outcome": "failed", "duration_ms": 13, "recorded_at": "2026-08-02T11:00:00+00:00"},
        {"event_id": "after-recover", "run_id": "after", "phase": "standalone", "nodeid": "tests/test_app.py::test_recover", "outcome": "passed", "duration_ms": 14, "recorded_at": "2026-08-02T11:00:01+00:00"},
        {"event_id": "after-stable", "run_id": "after", "phase": "standalone", "nodeid": "tests/test_app.py::test_stable", "outcome": "passed", "duration_ms": 15, "recorded_at": "2026-08-02T11:00:02+00:00"},
        {"event_id": "after-new", "run_id": "after", "phase": "standalone", "nodeid": "tests/test_app.py::test_new", "outcome": "failed", "duration_ms": 16, "recorded_at": "2026-08-02T11:00:03+00:00"},
    ]
    (events / "events.jsonl").write_text("".join(json.dumps(item) + "\n" for item in payloads), encoding="utf-8")


def test_compare_test_runs_classifies_regressions_and_recovery(tmp_path: Path) -> None:
    # Compare two real journal snapshots and keep all nodeids in deterministic status order.
    root = tmp_path / "project"
    root.mkdir()
    events = tmp_path / "events"
    _write_compare_events(events)
    db = stats_db_path(tmp_path / "reports")
    ingest_test_stats(events, db, project_root=root, run_id="ingest", source_path="app.py")

    rows = compare_test_runs(db, "before", "after", project_root=root, limit=20)
    by_nodeid = {row["nodeid"]: row for row in rows}

    assert by_nodeid["tests/test_app.py::test_regress"]["status"] == "regressed"
    assert by_nodeid["tests/test_app.py::test_recover"]["status"] == "recovered"
    assert by_nodeid["tests/test_app.py::test_stable"]["status"] == "stable"
    assert by_nodeid["tests/test_app.py::test_new"]["status"] == "new"
    assert by_nodeid["tests/test_app.py::test_regress"]["delta_failures"] == 1
    assert by_nodeid["tests/test_app.py::test_recover"]["delta_failures"] == -1


def test_stats_compare_supports_markdown_csv_and_json(tmp_path: Path, monkeypatch, capsys) -> None:
    # Verify the CLI exposes the same three export contracts for before/after diffs.
    sample = {
        "before_run_id": "before",
        "after_run_id": "after",
        "nodeid": "tests/test_app.py::test_regress",
        "status": "regressed",
        "before_executions": 1,
        "after_executions": 1,
        "delta_executions": 0,
        "before_passed": 1,
        "after_passed": 0,
        "before_failed": 0,
        "after_failed": 1,
        "before_skipped": 0,
        "after_skipped": 0,
        "before_errors": 0,
        "after_errors": 0,
        "delta_failures": 1,
        "before_failure_rate": 0.0,
        "after_failure_rate": 1.0,
        "delta_failure_rate": 1.0,
        "before_avg_duration_ms": 10.0,
        "after_avg_duration_ms": 12.0,
        "delta_avg_duration_ms": 2.0,
    }

    def fake_compare(*args, **kwargs):
        # Return one deterministic diff without opening SQLite in the CLI contract test.
        del args, kwargs
        return [sample]

    monkeypatch.setattr(cli, "compare_test_runs", fake_compare)
    markdown_code = cli.main(["stats", str(tmp_path), "--compare", "before", "after", "--format", "markdown"])
    markdown = capsys.readouterr().out
    csv_path = tmp_path / "reports" / "compare.csv"
    csv_code = cli.main(
        [
            "stats",
            str(tmp_path),
            "--compare",
            "before",
            "after",
            "--format",
            "csv",
            "--out",
            str(csv_path),
        ]
    )
    csv_summary = json.loads(capsys.readouterr().out)
    csv_rows = list(csv.DictReader(io.StringIO(csv_path.read_text(encoding="utf-8"))))
    json_code = cli.main(["stats", str(tmp_path), "--compare", "before", "after", "--json"])
    json_value = json.loads(capsys.readouterr().out)

    assert markdown_code == 0
    assert "| nodeid | status | before exec | after exec |" in markdown
    assert "| regressed |" in markdown
    assert csv_code == 0
    assert csv_summary["comparisons"] == 1
    assert csv_rows[0]["before_run_id"] == "before"
    assert csv_rows[0]["status"] == "regressed"
    assert json_code == 0
    assert json_value["comparisons"][0]["delta_failures"] == 1
