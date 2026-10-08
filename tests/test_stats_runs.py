import csv
import io
import json
from pathlib import Path

from test_intelligence_unified_v1 import cli
from test_intelligence_unified_v1.test_stats import ingest_test_stats, stats_db_path, summarize_test_runs


def _write_run_events(events: Path) -> None:
    # Create two historical runs with distinct phases and outcomes.
    events.mkdir()
    payloads = [
        {
            "event_id": "run-one-pass",
            "run_id": "run-1",
            "phase": "baseline",
            "nodeid": "tests/test_app.py::test_load",
            "outcome": "passed",
            "duration_ms": 10,
            "recorded_at": "2026-08-02T10:00:00+00:00",
        },
        {
            "event_id": "run-one-kill",
            "run_id": "run-1",
            "phase": "mutant",
            "mutant_id": "m1",
            "nodeid": "tests/test_app.py::test_load",
            "outcome": "failed",
            "duration_ms": 20,
            "recorded_at": "2026-08-02T10:00:01+00:00",
        },
        {
            "event_id": "run-two-skip",
            "run_id": "run-2",
            "phase": "standalone",
            "nodeid": "tests/test_app.py::test_other",
            "outcome": "skipped",
            "duration_ms": 30,
            "recorded_at": "2026-08-02T11:00:00+00:00",
        },
        {
            "event_id": "run-two-error",
            "run_id": "run-2",
            "phase": "standalone",
            "nodeid": "tests/test_app.py::test_load",
            "outcome": "error",
            "duration_ms": 40,
            "recorded_at": "2026-08-02T11:00:01+00:00",
        },
    ]
    (events / "events.jsonl").write_text(
        "".join(json.dumps(item) + "\n" for item in payloads),
        encoding="utf-8",
    )


def test_summarize_test_runs_aggregates_one_row_per_run(tmp_path: Path) -> None:
    # Verify run history uses one grouped SQL query and keeps phase/outcome counters.
    root = tmp_path / "project"
    root.mkdir()
    events = tmp_path / "events"
    _write_run_events(events)
    db = stats_db_path(tmp_path / "reports")
    ingest_test_stats(events, db, project_root=root, run_id="ingest", source_path="app.py")

    rows = summarize_test_runs(db, project_root=root)
    by_run = {row["run_id"]: row for row in rows}

    assert by_run["run-1"]["executions"] == 2
    assert by_run["run-1"]["tests"] == 1
    assert by_run["run-1"]["failed"] == 1
    assert by_run["run-1"]["mutant_attempts"] == 1
    assert set(by_run["run-1"]["phases"]) == {"baseline", "mutant"}
    assert by_run["run-2"]["skipped"] == 1
    assert by_run["run-2"]["errors"] == 1
    assert by_run["run-2"]["failure_rate"] == 0.5


def test_stats_runs_supports_markdown_csv_and_json(tmp_path: Path, monkeypatch, capsys) -> None:
    # Keep run-level exports aligned with the existing stats presentation contract.
    sample = {
        "run_id": "run-1",
        "executions": 2,
        "tests": 1,
        "passed": 1,
        "failed": 1,
        "skipped": 0,
        "errors": 0,
        "failure_rate": 0.5,
        "total_duration_ms": 30.0,
        "avg_duration_ms": 15.0,
        "mutant_attempts": 1,
        "phases": ["baseline", "mutant"],
        "first_seen": "2026-08-02T10:00:00+00:00",
        "last_seen": "2026-08-02T10:00:01+00:00",
    }

    def fake_summary(*args, **kwargs):
        # Return one deterministic run without reopening SQLite in this CLI test.
        del args, kwargs
        return [sample]

    monkeypatch.setattr(cli, "summarize_test_runs", fake_summary)
    markdown_code = cli.main(["stats", str(tmp_path), "--runs", "--format", "markdown"])
    markdown = capsys.readouterr().out
    csv_path = tmp_path / "reports" / "runs.csv"
    csv_code = cli.main(
        [
            "stats",
            str(tmp_path),
            "--runs",
            "--format",
            "csv",
            "--out",
            str(csv_path),
        ]
    )
    csv_summary = json.loads(capsys.readouterr().out)
    csv_rows = list(csv.DictReader(io.StringIO(csv_path.read_text(encoding="utf-8"))))
    json_code = cli.main(["stats", str(tmp_path), "--runs", "--json"])
    json_value = json.loads(capsys.readouterr().out)

    assert markdown_code == 0
    assert "| run id | exec | tests | passed | failed |" in markdown
    assert "run-1" in markdown
    assert "baseline,mutant" in markdown
    assert csv_code == 0
    assert csv_summary["runs"] == 1
    assert csv_rows[0]["run_id"] == "run-1"
    assert csv_rows[0]["phases"] == "baseline,mutant"
    assert json_code == 0
    assert json_value["runs"][0]["failure_rate"] == 0.5
