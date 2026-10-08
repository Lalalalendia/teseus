import json
from pathlib import Path

from test_intelligence_unified_v1 import cli


def test_stats_compare_gate_fails_for_regressed_and_new_tests(tmp_path: Path, monkeypatch, capsys) -> None:
    # Block an opt-in CI gate only for regressions and newly appearing failures.
    def fake_compare(*args, **kwargs):
        # Return statuses sufficient to exercise the machine-readable gate summary.
        del args, kwargs
        return [
            {"nodeid": "tests/test_regressed.py::test_one", "status": "regressed"},
            {"nodeid": "tests/test_new.py::test_one", "status": "new"},
            {"nodeid": "tests/test_stable.py::test_one", "status": "stable"},
        ]

    monkeypatch.setattr(cli, "compare_test_runs", fake_compare)
    code = cli.main(
        [
            "stats",
            str(tmp_path),
            "--compare",
            "before",
            "after",
            "--fail-on-regression",
            "--json",
        ]
    )
    value = json.loads(capsys.readouterr().out)

    assert code == 1
    assert value["summary"]["regressed"] == 1
    assert value["summary"]["new"] == 1
    assert value["summary"]["blocking_changes"] == 2
    assert value["summary"]["gate_passed"] is False


def test_stats_compare_gate_passes_without_blocking_statuses(tmp_path: Path, monkeypatch, capsys) -> None:
    # Keep recovered and stable changes non-blocking while preserving the diff output.
    def fake_compare(*args, **kwargs):
        # Return a clean comparison for the successful gate path.
        del args, kwargs
        return [
            {"nodeid": "tests/test_recovered.py::test_one", "status": "recovered"},
            {"nodeid": "tests/test_stable.py::test_one", "status": "stable"},
        ]

    monkeypatch.setattr(cli, "compare_test_runs", fake_compare)
    code = cli.main(
        [
            "stats",
            str(tmp_path),
            "--compare",
            "before",
            "after",
            "--fail-on-regression",
            "--json",
        ]
    )
    value = json.loads(capsys.readouterr().out)

    assert code == 0
    assert value["summary"]["recovered"] == 1
    assert value["summary"]["blocking_changes"] == 0
    assert value["summary"]["gate_passed"] is True
