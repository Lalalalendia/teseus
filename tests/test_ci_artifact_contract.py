import json
from pathlib import Path

from test_intelligence_unified_v1 import cli, maintenance
from test_intelligence_unified_v1.models import ProcessResult


def _passed_result(argv: list[str], cwd: Path, artifact: Path) -> ProcessResult:
    # Build a successful bounded result without writing subprocess output.
    return ProcessResult(
        argv=tuple(argv),
        cwd=str(cwd),
        exit_code=0,
        elapsed_seconds=0.01,
        timed_out=False,
        output="",
        output_artifact=str(artifact),
    )


def test_ci_runner_isolates_coverage_data_and_persists_result(tmp_path: Path, monkeypatch) -> None:
    # Keep coverage state outside the checkout and make the aggregate result durable.
    calls: list[dict[str, object]] = []

    monkeypatch.setattr(maintenance.shutil, "which", lambda name: "/opt/coverage" if name == "coverage" else None)

    def fake_run(argv, *, cwd, timeout_seconds, env, output_artifact, **kwargs):
        # Capture the exact environment and argv passed to both coverage commands.
        del timeout_seconds, kwargs
        calls.append({"argv": tuple(argv), "cwd": cwd, "env": env, "artifact": output_artifact})
        return _passed_result(list(argv), cwd, output_artifact)

    monkeypatch.setattr(maintenance, "run_argv", fake_run)
    reports_dir = tmp_path / "reports"
    result_file = reports_dir / "ci-result.json"

    value = maintenance.run_ci_lanes(
        tmp_path,
        ("coverage",),
        reports_dir,
        result_file=result_file,
    )

    coverage_file = str((reports_dir / "ci" / "coverage.data").resolve())
    assert value["passed"] is True
    assert value["result_file"] == str(result_file.resolve())
    assert json.loads(result_file.read_text(encoding="utf-8")) == value
    assert [call["argv"][0] for call in calls] == ["/opt/coverage", "/opt/coverage"]
    assert all(call["env"]["COVERAGE_FILE"] == coverage_file for call in calls)
    assert not (tmp_path / ".coverage").exists()


def test_ci_cli_defaults_to_an_atomic_result_file(tmp_path: Path, monkeypatch, capsys) -> None:
    # Keep the CLI default predictable while allowing callers to override the result path.
    expected = {
        "schema_version": 1,
        "project_root": str(tmp_path.resolve()),
        "reports_dir": str((tmp_path / "reports").resolve()),
        "selected_lanes": ["fast"],
        "lanes": [{"name": "fast", "status": "passed", "passed": True}],
        "passed": True,
        "failed_lanes": [],
        "skipped_lanes": [],
        "not_run_lanes": [],
    }
    captured: dict[str, object] = {}

    def fake_run(*args, **kwargs):
        # Capture the resolved result path without starting a project subprocess.
        del args
        captured.update(kwargs)
        return expected

    monkeypatch.setattr(cli, "run_ci_lanes", fake_run)
    code = cli.main(["ci", str(tmp_path), "--lane", "fast", "--reports-dir", str(tmp_path / "reports")])
    json.loads(capsys.readouterr().out)

    assert code == 0
    assert captured["result_file"] == (tmp_path / "reports" / "ci" / "result.json").resolve()
