import json
from pathlib import Path

from test_intelligence_unified_v1 import cli
from test_intelligence_unified_v1.maintenance import inspect_benchmark_history, record_benchmark_history


def _benchmark_result(project_root: Path, elapsed_seconds: float) -> dict[str, object]:
    # Build the smallest stable benchmark payload accepted by the history gate.
    return {
        "schema_version": 2,
        "benchmark_version": "v1.17",
        "project_root": str(project_root.resolve()),
        "workload_order": ["fixture"],
        "fixture": {"source": "app.py"},
        "workloads": {"fixture": {"elapsed_seconds": elapsed_seconds}},
        "subprocess_passed": True,
    }


def test_history_inspection_compares_latest_matching_project(tmp_path: Path) -> None:
    # Ignore an interleaved project record when calculating the latest regression.
    history_path = tmp_path / "benchmark_history.jsonl"
    project_a = tmp_path / "project-a"
    project_b = tmp_path / "project-b"
    record_benchmark_history(history_path, _benchmark_result(project_a, 1.0))
    record_benchmark_history(history_path, _benchmark_result(project_b, 2.0))
    record_benchmark_history(history_path, _benchmark_result(project_a, 1.2))

    inspected = inspect_benchmark_history(history_path, limit=10)

    assert inspected["records_loaded"] == 3
    assert inspected["latest"]["project_root"] == str(project_a.resolve())
    assert inspected["comparison"]["summary"]["potential_regressions"] == 1


def test_cli_fail_on_regression_is_opt_in_and_history_is_append_only(tmp_path: Path, monkeypatch, capsys) -> None:
    # Make the CLI gate observable without running machine-dependent benchmark workloads.
    project_root = tmp_path / "project"
    project_root.mkdir()
    history_path = tmp_path / "benchmark_history.jsonl"
    elapsed = {"value": 1.0}

    def fake_benchmark(root: Path, reports_dir: Path) -> dict[str, object]:
        # Return a deterministic fixture while preserving the production CLI contract.
        value = _benchmark_result(root, elapsed["value"])
        value["workloads"] = {
            "fixture": {"elapsed_seconds": elapsed["value"], "details": {"status": "complete"}},
            "small_e2e_campaign": {"elapsed_seconds": 0.01, "details": {"status": "complete"}},
        }
        value["workload_order"] = ["fixture", "small_e2e_campaign"]
        return value

    monkeypatch.setattr(cli, "benchmark", fake_benchmark)
    first_code = cli.main(
        ["benchmark", str(project_root), "--history-file", str(history_path), "--fail-on-regression"]
    )
    capsys.readouterr()
    elapsed["value"] = 1.2
    second_code = cli.main(
        ["benchmark", str(project_root), "--history-file", str(history_path), "--fail-on-regression"]
    )
    capsys.readouterr()

    assert first_code == 0
    assert second_code == 1
    assert len(history_path.read_text(encoding="utf-8").splitlines()) == 2


def test_cli_benchmark_history_prints_bounded_inspection(tmp_path: Path, capsys) -> None:
    # Expose history diagnostics through the dedicated read-only command.
    history_path = tmp_path / "benchmark_history.jsonl"
    record_benchmark_history(history_path, _benchmark_result(tmp_path / "project", 1.0))

    code = cli.main(["benchmark-history", "--history-file", str(history_path), "--limit", "1"])
    value = json.loads(capsys.readouterr().out)

    assert code == 0
    assert value["records_loaded"] == 1
    assert value["latest"]["benchmark_version"] == "v1.17"
