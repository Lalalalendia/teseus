from __future__ import annotations
import errno
import os
from pathlib import Path
import pytest
from theseus_local.worker_runtime import PersistentWorkerProcess
class _DestroyedPipe:
    """Model a Windows pipe whose OS handle was destroyed with the process tree."""
    def __init__(self, error_number: int) -> None:
        # Retain the exact Windows-compatible errno returned by repeated close().
        self.error_number = int(error_number)
        self.closed = False
    def close(self) -> None:
        # Reproduce the invalid descriptor raised after taskkill destroys the pipe handle.
        raise OSError(self.error_number, os.strerror(self.error_number))
@pytest.mark.parametrize("error_number", (errno.EINVAL, errno.EBADF))
def test_repeated_cleanup_accepts_destroyed_pipe_after_terminate_tree(
    tmp_path: Path,
    error_number: int,
) -> None:
    # Terminate the real worker tree, then prove repeated invalid pipe closure is idempotent.
    worker = PersistentWorkerProcess(
        worker_id=f"worker-idempotent-pipe-{error_number}",
        instance_id=f"instance-idempotent-pipe-{error_number}",
        spool_root=tmp_path / "spool",
        heartbeat_interval_seconds=0.01,
        cwd=Path(__file__).parents[1],
    )
    worker.start()
    try:
        worker.wait_for("registered")
        worker.wait_for("acquire")
        process = worker._process
        assert process is not None
        worker.terminate_tree()
        worker.wait(timeout=10.0)
    except BaseException:
        worker.terminate_tree()
        worker.close()
        raise
    process.stdin = _DestroyedPipe(error_number)  # type: ignore[assignment]
    worker.close()
    worker.close()
def test_invalid_pipe_close_remains_an_error_while_process_is_alive(tmp_path: Path) -> None:
    # Keep invalid close fail-closed while the worker PID and transport threads are still live.
    worker = PersistentWorkerProcess(
        worker_id="worker-live-pipe-fail-closed",
        instance_id="instance-live-pipe-fail-closed",
        spool_root=tmp_path / "spool",
        heartbeat_interval_seconds=0.01,
        cwd=Path(__file__).parents[1],
    )
    try:
        worker.start()
        worker.wait_for("registered")
        worker.wait_for("acquire")
        process = worker._process
        assert process is not None and process.poll() is None
        original_stdin = process.stdin
        process.stdin = _DestroyedPipe(errno.EINVAL)  # type: ignore[assignment]
        try:
            errors = PersistentWorkerProcess._close_streams(process)
        finally:
            process.stdin = original_stdin
        assert errors and "stdin" in errors[0], (
            "invalid pipe closure was hidden while the worker process was still alive; "
            f"pid={process.pid}; errors={errors}; "
            f"threads={PersistentWorkerProcess.active_lifecycle_thread_names()}"
        )
    finally:
        worker.close()
