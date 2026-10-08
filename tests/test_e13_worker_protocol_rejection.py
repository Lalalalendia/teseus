from __future__ import annotations

from pathlib import Path

import pytest

from theseus_contracts import NoAssignment, WorkerMessageType, WorkerProtocolFrame, encode_worker_frame
from theseus_local.worker_runtime import PersistentWorkerProcess, WorkerProcessError


def test_real_worker_rejects_foreign_instance_before_command_execution(tmp_path: Path) -> None:
    # Send a syntactically valid frame for another instance and require fail-closed worker termination.
    worker = PersistentWorkerProcess(
        worker_id="worker-rejection",
        instance_id="instance-rejection",
        spool_root=tmp_path / "spool",
        heartbeat_interval_seconds=0.02,
        cwd=Path(__file__).parents[1],
    )
    try:
        worker.start()
        worker.wait_for("registered")
        acquire = worker.wait_for("acquire")
        process = worker._process
        assert process is not None and process.stdin is not None
        foreign = WorkerProtocolFrame.create(
            WorkerMessageType.NO_ASSIGNMENT,
            NoAssignment(0.0),
            worker_id="worker-rejection",
            instance_id="foreign-instance",
            process_id=worker.pid,
            sequence=999,
            state="host",
            request_id=acquire["message_id"],
        )
        process.stdin.write(encode_worker_frame(foreign) + "\n")
        process.stdin.flush()
        with pytest.raises(WorkerProcessError, match="identity"):
            worker.wait_for("terminated", timeout=5.0)
        assert worker.wait(timeout=5.0) == 1
    finally:
        worker.close()
