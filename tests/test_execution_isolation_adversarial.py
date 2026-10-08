from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path

from theseus_local.acceptance import assert_resource_quiescence
from theseus_local.isolation import ExecutionPolicy

from post_pr63_helpers import assert_source_intact, remote_fixture, snapshot_files


def test_unallowlisted_environment_marker_does_not_cross_worker_boundary(tmp_path: Path, monkeypatch) -> None:
    marker = tmp_path / "environment-marker.txt"
    monkeypatch.setenv("THESEUS_SECRET_TEST_MARKER", "secret-value")
    _, request, worker = remote_fixture(tmp_path)
    command = (
        sys.executable,
        "-c",
        f"from pathlib import Path; import os; Path({str(marker)!r}).write_text(os.getenv('THESEUS_SECRET_TEST_MARKER','<missing>'))",
    )
    result = worker.execute(replace(request, argv=command))
    assert result.exit_code == 0
    assert marker.read_text(encoding="utf-8") == "<missing>"


def test_import_path_injection_is_not_forwarded_to_worker_process(tmp_path: Path) -> None:
    marker = tmp_path / "pythonpath-marker.txt"
    _, request, worker = remote_fixture(tmp_path)
    command = (
        sys.executable,
        "-c",
        f"from pathlib import Path; import os; Path({str(marker)!r}).write_text(os.getenv('PYTHONPATH','<missing>'))",
    )
    result = worker.execute(replace(request, argv=command, environment={"PYTHONPATH": str(tmp_path / "poison")}))
    assert result.exit_code == 0
    assert marker.read_text(encoding="utf-8") == "<missing>"


def test_cwd_is_private_worker_workspace(tmp_path: Path) -> None:
    marker = tmp_path / "cwd-marker.txt"
    _, request, worker = remote_fixture(tmp_path)
    command = (
        sys.executable,
        "-c",
        f"from pathlib import Path; Path({str(marker)!r}).write_text(str(Path.cwd()))",
    )
    result = worker.execute(replace(request, argv=command))
    assert result.exit_code == 0
    observed = Path(marker.read_text(encoding="utf-8"))
    assert observed.is_relative_to(worker.root / "workspaces")
    assert observed != tmp_path.resolve()
    assert not observed.is_relative_to(tmp_path / "coordinator-cache")


def test_workspace_mutation_cannot_poison_the_next_attempt(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    expected_source = snapshot_files(tmp_path / "project")
    hostile = replace(
        request,
        execution_attempt_id="dirty",
        evidence_identity="dirty-evidence",
        argv=(sys.executable, "-c", "from pathlib import Path; Path('module.py').write_text('poisoned')"),
    )
    failed = worker.execute(hostile)
    assert failed.workspace_integrity == "failed"
    healthy = replace(request, execution_attempt_id="clean", evidence_identity="clean-evidence")
    assert worker.execute(healthy).exit_code == 0
    assert worker.state.value == "ready"
    assert_source_intact(tmp_path / "project", expected_source)


def test_nested_relative_writes_are_contained_by_the_disposable_attempt_root(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    hostile = replace(
        request,
        execution_attempt_id="nested-write",
        evidence_identity="nested-write-evidence",
        argv=(
            sys.executable,
            "-c",
            "from pathlib import Path; Path('../escape.txt').write_text('must be cleaned')",
        ),
    )
    result = worker.execute(hostile)
    assert result.started is True
    assert result.workspace_integrity == "verified"
    assert not tuple((worker.root / "workspaces").rglob("escape.txt"))
    assert not tuple((worker.root / "workspaces").rglob(".attempt-*"))


def test_output_policy_reports_bounded_diagnostic_without_affecting_next_attempt(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    worker.policy = ExecutionPolicy(timeout_seconds=5.0, max_output_bytes=1024)
    flood = replace(
        request,
        execution_attempt_id="flood",
        evidence_identity="flood-evidence",
        argv=(sys.executable, "-c", "import sys; sys.stdout.write('x' * 1_000_000)"),
    )
    result = worker.execute(flood)
    assert result.workspace_integrity == "failed"
    assert "output" in (result.diagnostic_error or "").lower()
    assert worker.execute(replace(request, execution_attempt_id="after-flood", evidence_identity="after-flood")).exit_code == 0


def test_broken_output_pipe_is_a_physical_result_not_worker_failure(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    broken = replace(
        request,
        execution_attempt_id="broken-pipe",
        evidence_identity="broken-pipe-evidence",
        argv=(sys.executable, "-c", "import os; os.close(1); os.close(2)"),
    )
    result = worker.execute(broken)
    assert result.started is True
    assert worker.state.value == "ready"


def test_repeated_cleanup_is_idempotent_and_leaves_no_live_resources(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    worker.policy = ExecutionPolicy(timeout_seconds=0.1)
    timeout = replace(request, execution_attempt_id="timeout", evidence_identity="timeout-evidence", timeout_seconds=0.1, argv=(sys.executable, "-c", "import time; time.sleep(10)"))
    assert worker.execute(timeout).timed_out is True
    assert_resource_quiescence()
    assert_resource_quiescence()
    assert not tuple((worker.root / "workspaces").rglob(".attempt-*"))
