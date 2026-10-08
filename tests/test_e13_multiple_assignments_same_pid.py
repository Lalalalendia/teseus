from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from theseus_contracts import ShardAssignment
from theseus_local.worker_runtime import PersistentWorkerProcess


def _assignment(ordinal: int) -> ShardAssignment:
    # Build one immutable assignment with a unique shard and lease identity.
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat().replace("+00:00", "Z")
    return ShardAssignment(
        campaign_id="campaign-persistent-worker",
        shard_id=f"shard-{ordinal:03d}",
        lease_id=f"lease-{ordinal:03d}",
        attempt=0,
        mutant_ids=(f"m{ordinal}",),
        prepared_snapshot_id="snapshot-persistent-worker",
        workspace_descriptor_id="workspace-persistent-worker",
        expires_at=expires_at,
    )


def test_two_assignments_use_the_same_real_worker_pid(tmp_path: Path) -> None:
    # Require ACK followed by another acquire without replacing the standalone worker process.
    with PersistentWorkerProcess(
        worker_id="persistent-worker-assignments",
        spool_root=tmp_path / "spool",
        heartbeat_interval_seconds=0.01,
        cwd=Path(__file__).parents[1],
    ) as worker:
        registered = worker.wait_for("registered")
        persistent_pid = registered["process_id"]
        persistent_instance = registered["instance_id"]
        delivery_events = []
        for ordinal in (1, 2):
            acquire = worker.wait_for("acquire")
            assert acquire["process_id"] == persistent_pid
            assert acquire["instance_id"] == persistent_instance
            correlation_id = worker.send_assignment(
                _assignment(ordinal),
                {
                    "status": "complete",
                    "ordinal": ordinal,
                    "shard_result": {
                        "status": "complete",
                        "completed_mutants": 1,
                        "results": [],
                    },
                },
                delay_seconds=0.03,
            )
            delivery = worker.wait_for("delivery", correlation_id=correlation_id)
            delivery_events.append(delivery)
            assert delivery["process_id"] == persistent_pid
            assert delivery["payload"]["worker"]["process_id"] == persistent_pid
            assert worker.returncode is None
            worker.acknowledge(delivery["payload"]["event_id"])
            acknowledged = worker.wait_for("acknowledged", correlation_id=correlation_id)
            assert acknowledged["payload"]["completed_assignments"] == ordinal
        assert {item["process_id"] for item in delivery_events} == {persistent_pid}
        worker.wait_for("acquire")
        worker.shutdown()
        terminated = worker.wait_for("terminated")
        assert terminated["payload"]["completed_assignments"] == 2
        assert worker.wait() == 0
    assert list((tmp_path / "spool").glob("*.jsonl"))
