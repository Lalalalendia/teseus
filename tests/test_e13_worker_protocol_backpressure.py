from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from theseus_contracts import ShardAssignment
from theseus_local.worker_runtime import PersistentWorkerProcess


def _assignment() -> ShardAssignment:
    # Build one lease-valid assignment that remains active long enough to fill a synchronous Windows pipe.
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat().replace("+00:00", "Z")
    return ShardAssignment(
        campaign_id="campaign-worker-backpressure",
        shard_id="shard-worker-backpressure",
        lease_id="lease-worker-backpressure",
        attempt=0,
        mutant_ids=("m1",),
        prepared_snapshot_id="snapshot-worker-backpressure",
        workspace_descriptor_id="workspace-worker-backpressure",
        expires_at=expires_at,
    )


def test_heartbeat_receipts_cannot_block_delivery_stdout_reader(tmp_path: Path) -> None:
    # Keep stdout draining while heartbeat receipts accumulate behind a worker that is not yet reading stdin.
    worker = PersistentWorkerProcess(
        worker_id="worker-backpressure",
        spool_root=tmp_path / "spool",
        heartbeat_interval_seconds=0.002,
        cwd=Path(__file__).parents[1],
    )
    with worker:
        writer_thread = worker._writer_thread
        reader_thread = worker._reader_thread
        stderr_thread = worker._stderr_thread
        worker.wait_for("registered")
        worker.wait_for("acquire")
        correlation_id = worker.send_assignment(
            _assignment(),
            {
                "status": "complete",
                "shard_result": {
                    "status": "complete",
                    "completed_mutants": 0,
                    "results": [],
                },
            },
            delay_seconds=1.2,
        )
        delivery = worker.wait_for("delivery", correlation_id=correlation_id, timeout=10.0)
        worker.acknowledge(delivery["payload"]["event_id"])
        worker.wait_for("acknowledged", correlation_id=correlation_id, timeout=10.0)

    assert writer_thread is not None
    assert reader_thread is not None
    assert stderr_thread is not None
    assert writer_thread.is_alive() is False
    assert reader_thread.is_alive() is False
    assert stderr_thread.is_alive() is False
