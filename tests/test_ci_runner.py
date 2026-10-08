import json
import sys
from pathlib import Path

from test_intelligence_unified_v1 import cli, maintenance
from test_intelligence_unified_v1.models import ProcessResult


def _process_result(argv: list[str], cwd: Path, artifact: Path, exit_code: int) -> ProcessResult:
    # Build a bounded subprocess result for CI orchestration tests.
    return ProcessResult(
        argv=tuple(argv),
        cwd=str(cwd),
        exit_code=exit_code,
        elapsed_seconds=0.01,
        timed_out=False,
        output="",
        output_artifact=str(artifact),
    )


def test_ci_runner_executes_safe_argv_and_records_artifact_paths(tmp_path: Path, monkeypatch) -> None:
    # Replace process execution while retaining command translation and result materialization.
    calls: list[tuple[tuple[str, ...], Path]] = []

    def fake_run(argv, *, cwd, timeout_seconds, output_artifact, **kwargs):
        # Capture only argv and artifact identity; no shell can be involved in this test double.
        del timeout_seconds, kwargs
        calls.append((tuple(argv), output_artifact))
        return _process_result(list(argv), cwd, output_artifact, 0)

    monkeypatch.setattr(maintenance, "run_argv", fake_run)
    value = maintenance.run_ci_lanes(tmp_path, ("fast",), tmp_path / "reports")

    assert value["passed"] is True
    assert value["lanes"][0]["status"] == "passed"
    assert calls[0][0][0] == sys.executable
    pytest_index = calls[0][0].index("pytest")
    assert calls[0][0][pytest_index + 1 : pytest_index + 4] == ("-q", "-m", "not performance and not slow")
    assert "--basetemp" in calls[0][0]
    assert calls[0][0][calls[0][0].index("--basetemp") + 1].endswith("fast-pytest-tmp")
    assert calls[0][1].name == "fast-00.txt"


def test_ci_runner_skips_optional_coverage_or_fails_when_required(tmp_path: Path, monkeypatch) -> None:
    # Make missing coverage explicit without turning an optional lane into a silent success.
    monkeypatch.setattr(maintenance.shutil, "which", lambda name: None)

    skipped = maintenance.run_ci_lanes(tmp_path, ("coverage",), tmp_path / "reports")
    required = maintenance.run_ci_lanes(
        tmp_path,
        ("coverage",),
        tmp_path / "reports",
        require_optional=True,
    )

    assert skipped["passed"] is True
    assert skipped["skipped_lanes"] == ["coverage"]
    assert required["passed"] is False
    assert required["failed_lanes"] == ["coverage"]


def test_ci_runner_fail_fast_marks_remaining_lanes_not_run(tmp_path: Path, monkeypatch) -> None:
    # Preserve an explicit not-run record after the first required lane failure.
    def fake_run(argv, *, cwd, timeout_seconds, output_artifact, **kwargs):
        # Fail only the full lane and keep the command boundary deterministic.
        del cwd, timeout_seconds, output_artifact, kwargs
        pytest_index = argv.index("pytest")
        # The full lane has no pytest marker; basetemp is an orthogonal runner option.
        exit_code = 1 if "-m" not in argv[pytest_index + 1 :] else 0
        return _process_result(list(argv), tmp_path, tmp_path / "output.txt", exit_code)

    monkeypatch.setattr(maintenance, "run_argv", fake_run)
    value = maintenance.run_ci_lanes(
        tmp_path,
        ("fast", "full", "performance"),
        tmp_path / "reports",
        fail_fast=True,
    )

    assert value["passed"] is False
    assert [item["status"] for item in value["lanes"]] == ["passed", "failed", "not_run"]
    assert value["not_run_lanes"] == ["performance"]


def test_ci_cli_returns_runner_status_as_exit_code(tmp_path: Path, monkeypatch, capsys) -> None:
    # Keep the CLI contract thin: orchestration owns status, CLI owns JSON and exit code.
    expected = {
        "schema_version": 1,
        "project_root": str(tmp_path.resolve()),
        "reports_dir": str((tmp_path / "reports").resolve()),
        "selected_lanes": ["fast"],
        "lanes": [{"name": "fast", "status": "failed", "passed": False}],
        "passed": False,
        "failed_lanes": ["fast"],
        "skipped_lanes": [],
        "not_run_lanes": [],
    }

    def fake_run(*args, **kwargs):
        # Return the precomputed result without starting a project subprocess.
        del args, kwargs
        return expected

    monkeypatch.setattr(cli, "run_ci_lanes", fake_run)
    code = cli.main(["ci", str(tmp_path), "--lane", "fast"])
    value = json.loads(capsys.readouterr().out)

    assert code == 1
    assert value == expected
