import csv
import io
import json
from pathlib import Path

from test_intelligence_unified_v1 import cli


def _sample_stats_row() -> dict[str, object]:
    # Provide one complete historical row for every supported export format.
    return {
        "nodeid": "tests/test_app.py::test_load",
        "executions": 12,
        "passed": 9,
        "failed": 2,
        "skipped": 1,
        "errors": 0,
        "baseline_failures": 1,
        "regression_failures": 1,
        "standalone_failures": 1,
        "health_executions": 4,
        "health_passes": 3,
        "health_failures": 1,
        "mutant_attempts": 5,
        "mutant_kills": 3,
        "kill_rate": 0.6,
        "health_failure_rate": 0.25,
        "flaky_rate": 0.25,
        "health_status": "flaky",
        "total_duration_ms": 120.0,
        "avg_duration_ms": 10.0,
        "median_ms": 9.5,
        "p95_ms": 18.2,
        "last_seen": "2026-08-02T12:00:00+00:00",
    }


def test_stats_markdown_renders_an_operator_table(tmp_path: Path, monkeypatch, capsys) -> None:
    # Make the human-facing export expose execution, failure, health and timing columns.
    def fake_summary(*args, **kwargs):
        # Return a deterministic row without opening a database in this CLI contract test.
        del args, kwargs
        return [_sample_stats_row()]

    monkeypatch.setattr(cli, "summarize_test_stats", fake_summary)
    code = cli.main(
        [
            "stats",
            str(tmp_path),
            "--reports-dir",
            str(tmp_path / "reports"),
            "--format",
            "markdown",
        ]
    )
    output = capsys.readouterr().out

    assert code == 0
    assert "| nodeid | exec | passed | failed |" in output
    assert "tests/test_app.py::test_load" in output
    assert "| flaky |" in output
    assert "| 9.5 | 18.2 |" in output


def test_stats_csv_can_be_saved_to_an_atomic_report(tmp_path: Path, monkeypatch, capsys) -> None:
    # Keep CSV suitable for spreadsheets while the CLI stdout remains a small result summary.
    def fake_summary(*args, **kwargs):
        # Return a deterministic row for the file writer path.
        del args, kwargs
        return [_sample_stats_row()]

    monkeypatch.setattr(cli, "summarize_test_stats", fake_summary)
    output_path = tmp_path / "reports" / "stats.csv"
    code = cli.main(
        [
            "stats",
            str(tmp_path),
            "--reports-dir",
            str(tmp_path / "reports"),
            "--format",
            "csv",
            "--out",
            str(output_path),
        ]
    )
    summary = json.loads(capsys.readouterr().out)
    rows = list(csv.DictReader(io.StringIO(output_path.read_text(encoding="utf-8"))))

    assert code == 0
    assert summary["result"] == str(output_path.resolve())
    assert summary["format"] == "csv"
    assert rows[0]["executions"] == "12"
    assert rows[0]["failed"] == "2"
    assert rows[0]["mutant_kills"] == "3"
    assert rows[0]["health_status"] == "flaky"


def test_stats_json_flag_keeps_machine_contract(tmp_path: Path, monkeypatch, capsys) -> None:
    # Preserve the existing JSON shape while adding the new presentation formats.
    def fake_summary(*args, **kwargs):
        # Return one row through the legacy --json route.
        del args, kwargs
        return [_sample_stats_row()]

    monkeypatch.setattr(cli, "summarize_test_stats", fake_summary)
    code = cli.main(
        [
            "stats",
            str(tmp_path),
            "--reports-dir",
            str(tmp_path / "reports"),
            "--json",
        ]
    )
    value = json.loads(capsys.readouterr().out)

    assert code == 0
    assert value["tests"][0]["executions"] == 12
    assert value["tests"][0]["health_status"] == "flaky"
