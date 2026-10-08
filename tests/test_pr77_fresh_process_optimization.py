import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from test_intelligence_unified_v1 import runner as runner_module
from test_intelligence_unified_v1.models import MutantSelection, ProcessResult
from test_intelligence_unified_v1.runner import LevelSpec, MutationConfig, MutationRunner
from test_intelligence_unified_v1.test_stats import STATS_PLUGIN


class _RecordingBackend:
    def __init__(self) -> None:
        self.requests = []

    def execute(self, request, *, metrics=None):
        del metrics
        self.requests.append(request)
        return ProcessResult(
            argv=tuple(request.argv),
            cwd=str(request.cwd),
            exit_code=0,
            elapsed_seconds=0.01,
            timed_out=False,
            output="",
            retry=bool(request.retry),
        )


def _runner(tmp_path: Path, *, backend: _RecordingBackend | None = None) -> MutationRunner:
    return MutationRunner(
        MutationConfig(
            project_root=tmp_path,
            source="app.py",
            reports_dir=tmp_path / "reports",
        ),
        execution_backend=backend,
    )


def test_pr77_prepared_launch_reuses_static_facts_but_refreshes_attempt_identity(tmp_path: Path) -> None:
    # The descriptor is shared while every fresh child receives a unique attribution token.
    runner = _runner(tmp_path)
    raw_command = (sys.executable, "-m", "pytest", "tests", "-q")

    first = runner._get_prepared_test_launch(raw_command, report_id="run-pr77")
    second = runner._get_prepared_test_launch(raw_command, report_id="run-pr77")

    assert first is second
    assert first.pytest_command is True
    assert first.command_argv == runner_module.instrument_pytest_command(raw_command)
    plugin_index = first.command_argv.index("-p")
    assert first.command_argv[plugin_index + 1] == STATS_PLUGIN
    assert runner._prepared_launch_descriptor_builds == 1
    assert runner._prepared_launch_descriptor_hits == 1

    first_env = first.environment_for_attempt(
        run_id="run-pr77",
        phase="mutant",
        level="L1",
        mutant_id="m1",
        target_sha256="sha-1",
        retry=False,
    )
    second_env = first.environment_for_attempt(
        run_id="run-pr77",
        phase="mutant",
        level="L1",
        mutant_id="m2",
        target_sha256="sha-1",
        retry=True,
    )
    assert first_env is not None and second_env is not None
    dynamic_keys = {
        "TI_TEST_STATS_MUTANT_ID",
        "TI_TEST_STATS_TARGET_SHA256",
        "TI_TEST_STATS_RETRY",
        "TI_TEST_STATS_ATTEMPT",
    }
    assert {key: first_env[key] for key in first_env if key not in dynamic_keys} == {
        key: second_env[key] for key in second_env if key not in dynamic_keys
    }
    assert first_env["TI_TEST_STATS_MUTANT_ID"] == "m1"
    assert second_env["TI_TEST_STATS_MUTANT_ID"] == "m2"
    assert first_env["TI_TEST_STATS_RETRY"] == "0"
    assert second_env["TI_TEST_STATS_RETRY"] == "1"
    assert first_env["TI_TEST_STATS_ATTEMPT"] != second_env["TI_TEST_STATS_ATTEMPT"]
    assert "__prepared__" not in first_env["TI_TEST_STATS_ATTEMPT"]


def test_pr77_keeps_one_backend_request_per_fresh_process_boundary(tmp_path: Path) -> None:
    # Reusing launch facts must never collapse two mutant attempts into one physical execution.
    backend = _RecordingBackend()
    runner = _runner(tmp_path, backend=backend)
    command = (sys.executable, "-c", "print('ok')")

    with (
        patch.object(runner_module, "instrument_pytest_command", side_effect=lambda argv: tuple(argv)),
        patch.object(runner_module, "is_pytest_command", return_value=False),
    ):
        for mutant_id in ("m1", "m2"):
            runner._run_test_command(
                command,
                phase="mutant",
                level="L1",
                mutant_id=mutant_id,
                target_sha256="sha",
                report_id="run-pr77",
                output_artifact=tmp_path / f"{mutant_id}.out",
                timeout_seconds=5.0,
            )

    assert len(backend.requests) == 2
    assert [request.argv for request in backend.requests] == [command, command]
    assert backend.requests[0].execution_id != backend.requests[1].execution_id
    assert backend.requests[0].environment is None
    assert backend.requests[1].environment is None
    assert runner._prepared_launch_descriptor_builds == 1
    assert runner._prepared_launch_descriptor_hits == 1


def test_pr77_selection_command_cache_preserves_reference_argv_and_measurement(tmp_path: Path) -> None:
    # Repeated identical selections reuse only the prepared argv and retain exact pytest semantics.
    runner = _runner(tmp_path)
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
    ):
        first, _ = runner._level_for_mutant(SimpleNamespace(), level)
        second, _ = runner._level_for_mutant(SimpleNamespace(), level)

    assert first is not None and second is not None
    assert first.command_argv == second.command_argv
    assert first.command_argv == (
        "python",
        "-m",
        "pytest",
        "tests/test_sample.py::test_value",
        "-q",
        "--maxfail=1",
        "--tb=line",
    )
    assert runner._prepared_invocation_command_builds == 1


def test_pr77_optimized_launch_matches_reference_exit_and_collection(tmp_path: Path) -> None:
    # Compare a real fresh pytest launch with the instrumented optimized launch on the same project.
    (tmp_path / "test_sample.py").write_text(
        "def test_value():\n"
        "    assert 2 + 2 == 4\n",
        encoding="utf-8",
    )
    runner = _runner(tmp_path)
    runner._campaign_source_rel = "test_sample.py"
    command = (sys.executable, "-m", "pytest", "test_sample.py", "-q")
    reference = runner_module.run_argv(
        command,
        cwd=tmp_path,
        timeout_seconds=30.0,
        env=dict(os.environ),
        output_artifact=tmp_path / "reference.out",
    )
    optimized = runner._run_test_command(
        command,
        phase="mutant",
        level="L1",
        mutant_id="m1",
        target_sha256="sha",
        report_id="run-pr77-equivalence",
        output_artifact=tmp_path / "optimized.out",
        timeout_seconds=30.0,
    )

    assert reference.exit_code == optimized.exit_code == 0
    assert reference.timed_out is optimized.timed_out is False
    assert "1 passed" in reference.output
    assert "1 passed" in optimized.output
    assert optimized.argv == runner_module.instrument_pytest_command(command)
    assert runner._prepared_launch_descriptor_builds == 1
