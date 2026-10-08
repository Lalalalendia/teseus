from __future__ import annotations

import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pytest

from theseus_local.remote_worker import RemoteWorkerError, RemoteWorkerRuntime, RemoteWorkerState
from theseus_local.runtime_identity import current_runtime_identity

from post_pr63_helpers import remote_fixture


def test_worker_lifecycle_valid_transitions_and_re_registration(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    assert worker.state is RemoteWorkerState.READY
    worker.drain()
    assert worker.state is RemoteWorkerState.DRAINING
    with pytest.raises(RemoteWorkerError, match="draining"):
        worker.execute(request)
    worker.stop()
    assert worker.state is RemoteWorkerState.STOPPED
    worker.stop()  # stopping an already stopped runtime is idempotent
    with pytest.raises(RemoteWorkerError, match="cannot drain"):
        worker.drain()
    with pytest.raises(RemoteWorkerError, match="not registerable"):
        worker.accept_registration(current_runtime_identity())
    restarted = worker.restart()
    assert restarted.state is RemoteWorkerState.READY
    assert restarted.execute(request).started is True


def test_incompatible_registration_transitions_worker_to_failed(tmp_path: Path) -> None:
    _, _, worker = remote_fixture(tmp_path)
    mismatch = current_runtime_identity(theseus_version="99.0.0")
    with pytest.raises(RemoteWorkerError, match="incompatible"):
        worker.accept_registration(mismatch)
    assert worker.state is RemoteWorkerState.FAILED
    with pytest.raises(RemoteWorkerError, match="failed"):
        worker.execute(remote_fixture(tmp_path / "second")[1])
    with pytest.raises(RemoteWorkerError, match="cannot drain"):
        worker.drain()
    with pytest.raises(RemoteWorkerError, match="failed worker"):
        worker.stop()
    assert worker.restart().state is RemoteWorkerState.READY


def test_capacity_never_allows_more_active_assignments_than_slots(tmp_path: Path) -> None:
    store, request, worker = remote_fixture(tmp_path)
    marker_a = tmp_path / "started-a"
    marker_b = tmp_path / "started-b"
    def command(marker: Path) -> tuple[str, ...]:
        return (
            sys.executable,
            "-c",
            f"from pathlib import Path; import time; Path({str(marker)!r}).write_text('1'); time.sleep(0.5)",
        )
    worker = RemoteWorkerRuntime(worker_id="slots", root=tmp_path / "worker-slots", artifact_store=store, slots=2)
    first = replace(request, execution_attempt_id="attempt-a", evidence_identity="evidence-a", argv=command(marker_a))
    second = replace(request, execution_attempt_id="attempt-b", evidence_identity="evidence-b", argv=command(marker_b))
    third = replace(request, execution_attempt_id="attempt-c", evidence_identity="evidence-c", argv=command(tmp_path / "started-c"))
    with ThreadPoolExecutor(max_workers=2) as pool:
        first_future = pool.submit(worker.execute, first)
        second_future = pool.submit(worker.execute, second)
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and worker.registration().available_slots != 0:
            time.sleep(0.01)
        assert worker.registration().available_slots == 0
        with pytest.raises(RemoteWorkerError, match="capacity"):
            worker.execute(third)
        first_result = first_future.result(timeout=5)
        second_result = second_future.result(timeout=5)
    assert first_result.exit_code == 0
    assert second_result.exit_code == 0
    assert marker_a.is_file() and marker_b.is_file()
    assert worker.registration().available_slots == 2
    assert worker.state is RemoteWorkerState.READY


def test_hostile_timeout_does_not_poison_next_assignment(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    worker.policy = replace(worker.policy, timeout_seconds=1.0)
    hostile = replace(
        request,
        execution_attempt_id="hostile",
        evidence_identity="hostile-evidence",
        timeout_seconds=0.15,
        argv=(sys.executable, "-c", "import subprocess,sys,time; subprocess.Popen([sys.executable,'-c','import time; time.sleep(10)']); time.sleep(10)"),
    )
    result = worker.execute(hostile)
    assert result.timed_out is True
    healthy = replace(request, execution_attempt_id="healthy", evidence_identity="healthy-evidence")
    assert worker.execute(healthy).exit_code == 0
    assert worker.state is RemoteWorkerState.READY


def test_consecutive_assignments_publish_distinct_process_markers(tmp_path: Path) -> None:
    store, request, worker = remote_fixture(tmp_path)
    marker_a = tmp_path / "pid-a.txt"
    marker_b = tmp_path / "pid-b.txt"
    first = replace(
        request,
        execution_attempt_id="pid-a",
        evidence_identity="pid-a-evidence",
        argv=(sys.executable, "-c", f"from pathlib import Path; import os; Path({str(marker_a)!r}).write_text(str(os.getpid()))"),
    )
    second = replace(
        request,
        execution_attempt_id="pid-b",
        evidence_identity="pid-b-evidence",
        argv=(sys.executable, "-c", f"from pathlib import Path; import os; Path({str(marker_b)!r}).write_text(str(os.getpid()))"),
    )
    worker.execute(first)
    worker.execute(second)
    assert marker_a.read_text(encoding="utf-8") != marker_b.read_text(encoding="utf-8")
