import sys
from pathlib import Path

from test_intelligence_unified_v1.commands import run_argv


def test_timeout_terminates_process_tree_and_reports_bounded_cleanup(tmp_path: Path) -> None:
    # Exercise the real timeout boundary with a child process in the same process group.
    child_code = "import time; time.sleep(60)"
    parent_code = (
        "import subprocess,sys,time;"
        f"subprocess.Popen([sys.executable, '-c', {child_code!r}]);"
        "time.sleep(60)"
    )
    result = run_argv(
        (sys.executable, "-c", parent_code),
        cwd=tmp_path,
        timeout_seconds=0.25,
        output_artifact=tmp_path / "timeout-output.txt",
    )

    assert result.timed_out is True
    assert result.termination is not None
    assert result.termination["requested"] is True
    assert result.termination["parent_kill_succeeded"] is True
    assert result.process_tree_leak is False
    assert result.output_artifact == str(tmp_path / "timeout-output.txt")
