from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import test_intelligence_unified_v1.runner as runner_module
from test_intelligence_unified_v1.models import MutantSelection, ProcessResult
from test_intelligence_unified_v1.runner import LevelSpec, MutationConfig, MutationRunner


def test_prepared_invocation_is_carved_from_runner_residual(tmp_path: Path) -> None:
    # Account measured pre-process preparation as an exclusive runner phase without changing total wall authority.
    runner = MutationRunner(MutationConfig(project_root=tmp_path, source="app.py"))
    runner.performance.pytest_seconds = 3.0
    runner.performance.campaign_wall_seconds = 5.0
    runner._prepared_invocation_seconds = 0.25
    runner._prepared_invocation_command_builds = 2
    runner._prepared_invocation_process_preparations = 2

    payload = runner._performance_payload()
    timeline = payload["worker_execution_timeline"]
    phases = {item["phase"]: item["wall_seconds"] for item in timeline["phases"]}

    assert phases["pytest_process"] == 3.0
    assert phases["prepared_invocation"] == 0.25
    assert phases["runner_unattributed_residual"] == 1.75
    assert timeline["total_wall_seconds"] == 5.0
    assert timeline["accounted_seconds"] == 5.0
    assert timeline["accounting_error_seconds"] == 0.0
    assert payload["prepared_invocation"] == timeline["prepared_invocation"]
    assert payload["prepared_invocation"] == {
        "status": "observed",
        "total_seconds": 0.25,
        "command_builds": 2,
        "process_preparations": 2,
        "seconds_per_process_preparation": 0.125,
    }


def test_run_test_command_measures_pre_process_work_without_changing_launch_contract(tmp_path: Path) -> None:
    # Measure command preparation before run_argv while preserving argv, cwd, timeout, retry and output arguments exactly.
    runner = MutationRunner(MutationConfig(project_root=tmp_path, source="app.py"))
    artifact = tmp_path / "attempt.log"
    expected = ProcessResult(
        argv=("python", "script.py"),
        cwd=str(tmp_path.resolve()),
        exit_code=0,
        elapsed_seconds=0.4,
        timed_out=False,
        output="",
        retry=True,
    )

    with (
        patch.object(runner_module, "instrument_pytest_command", side_effect=lambda argv: tuple(argv)),
        patch.object(runner_module, "is_pytest_command", return_value=False),
        patch.object(runner_module, "run_argv", return_value=expected) as run_argv,
        patch.object(runner_module.time, "perf_counter", side_effect=(1.0, 1.25, 2.0, 2.5)),
    ):
        observed = runner._run_test_command(
            ("python", "script.py"),
            phase="mutant",
            level="L1",
            mutant_id="m1",
            target_sha256="sha",
            report_id="run-pr49-2",
            output_artifact=artifact,
            timeout_seconds=7.5,
            retry=True,
        )

    assert observed is expected
    assert runner._prepared_invocation_seconds == 0.25
    assert runner._prepared_invocation_command_builds == 0
    assert runner._prepared_invocation_process_preparations == 1
    assert abs(runner.performance.pytest_wrapper_seconds - 0.1) < 1e-9
    run_argv.assert_called_once_with(
        ("python", "script.py"),
        cwd=runner.root,
        timeout_seconds=7.5,
        env=None,
        retry=True,
        output_artifact=artifact,
        metrics=runner.performance,
    )


def test_dynamic_l1_command_build_is_measured_without_changing_pytest_argv(tmp_path: Path) -> None:
    # Measure only dynamic L1 command construction after selection while keeping the established pytest flags and nodeid.
    runner = MutationRunner(MutationConfig(project_root=tmp_path, source="app.py"))
    level = LevelSpec("L1", "impact", ("placeholder",), (), ())
    selection = MutantSelection(
        mutant_id="m1",
        line_no=10,
        nodeids=("tests/test_sample.py::test_value",),
        source="impact",
        confidence=1.0,
    )

    with (
        patch.object(runner, "_build_mutant_selection", return_value=selection),
        patch.object(runner_module, "resolve_python", return_value="python"),
        patch.object(runner_module.time, "perf_counter", side_effect=(10.0, 10.05)),
    ):
        prepared, detail = runner._level_for_mutant(SimpleNamespace(), level)

    assert prepared is not None
    assert prepared.command_argv == (
        "python",
        "-m",
        "pytest",
        "tests/test_sample.py::test_value",
        "-q",
        "--maxfail=1",
        "--tb=line",
    )
    assert detail["nodeids"] == ["tests/test_sample.py::test_value"]
    assert abs(runner._prepared_invocation_seconds - 0.05) < 1e-9
    assert runner._prepared_invocation_command_builds == 1
    assert runner._prepared_invocation_process_preparations == 0
