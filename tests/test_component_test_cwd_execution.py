from __future__ import annotations

import sys
from pathlib import Path

from test_intelligence_unified_v1.models import ProcessResult
from test_intelligence_unified_v1.runner import MutationConfig, MutationRunner


class _CaptureBackend:
    def __init__(self) -> None:
        # Retain the physical requests so the test can assert the execution cwd contract.
        self.requests = []

    def execute(self, request, *, metrics=None):
        # Return one successful direct-command result without spawning a subprocess.
        self.requests.append(request)
        return ProcessResult(tuple(request.argv), str(request.cwd), 0, 0.01, False, "")


def test_runner_executes_test_command_from_component_cwd(tmp_path: Path) -> None:
    # Preserve the source checkout root while executing the test oracle from its component-local cwd.
    root = tmp_path / "project"
    component = root / "backend"
    component.mkdir(parents=True)
    (component / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
    backend = _CaptureBackend()
    runner = MutationRunner(
        MutationConfig(
            project_root=root,
            source="backend/app.py",
            test_command_argv=(sys.executable, "-c", "raise SystemExit(0)"),
            test_command_cwd=component,
            reports_dir=tmp_path / "reports",
        ),
        execution_backend=backend,
    )

    result = runner._run_test_command(
        runner.config.test_command_argv or (),
        phase="baseline",
        level="L1",
        mutant_id=None,
        target_sha256=None,
        report_id="component-cwd",
        output_artifact=tmp_path / "baseline.log",
        timeout_seconds=5.0,
    )

    assert result.passed is True
    assert len(backend.requests) == 1
    assert Path(backend.requests[0].cwd) == component.resolve()
    assert runner.root == root.resolve()
