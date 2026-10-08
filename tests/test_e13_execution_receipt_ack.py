from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from theseus_contracts import ShardAssignment
from theseus_local.worker_runtime import DurableExecutionSpool, PersistentWorkerProcess


def _assignment() -> ShardAssignment:
    # Build one immutable assignment for the commit-receipt boundary.
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat().replace("+00:00", "Z")
    return ShardAssignment(
        campaign_id="campaign-receipt",
        shard_id="shard-receipt",
        lease_id="lease-receipt",
        attempt=0,
        mutant_ids=("m1",),
        prepared_snapshot_id="snapshot-receipt",
        workspace_descriptor_id="workspace-receipt",
        expires_at=expires_at,
    )


def test_execution_receipt_clears_spool_only_after_authoritative_ack(tmp_path: Path) -> None:
    # Prove delivery remains durable until the host sends the typed post-commit ExecutionReceipt.
    spool_root = tmp_path / "spool"
    with PersistentWorkerProcess(
        worker_id="worker-receipt",
        instance_id="instance-receipt",
        spool_root=spool_root,
        heartbeat_interval_seconds=0.01,
        cwd=Path(__file__).parents[1],
    ) as worker:
        worker.wait_for("registered")
        worker.wait_for("acquire")
        correlation_id = worker.send_assignment(
            _assignment(),
            {"status": "complete", "shard_result": {"status": "complete", "results": []}},
        )
        delivery = worker.wait_for("delivery", correlation_id=correlation_id)
        event_id = delivery["payload"]["event_id"]
        assert delivery["message_type"] == "worker.execution_delivery"
        assert DurableExecutionSpool(spool_root).pending()[0].event_id == event_id
        worker.acknowledge(event_id)
        acknowledged = worker.wait_for("acknowledged", correlation_id=correlation_id)
        assert acknowledged["message_type"] == "worker.execution_acknowledged"
        assert acknowledged["payload"]["event_id"] == event_id
        assert DurableExecutionSpool(spool_root).pending() == ()
        assert DurableExecutionSpool(spool_root).acknowledged(event_id)


def test_coordinator_sends_execution_receipt_only_after_authoritative_commit() -> None:
    # Freeze the commit-before-receipt order in the production persistent-worker path.
    source = (Path(__file__).parents[1] / "theseus_local" / "coordinator.py").read_text(encoding="utf-8")
    commit_position = source.index("service.record_shard_result(")
    receipt_position = source.index("worker.acknowledge(delivery_event_id)")
    assert commit_position < receipt_position
