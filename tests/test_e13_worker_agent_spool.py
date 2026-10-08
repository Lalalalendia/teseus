from __future__ import annotations
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pytest
from theseus_contracts import (
    HeartbeatReceipt,
    ShardAssignment,
    WorkerIdentity,
)
from theseus_local.worker_runtime import (
    DurableExecutionSpool,
    WorkerAgent,
    WorkerLeaseLost,
)
def _assignment() -> ShardAssignment:
    # Build one short-lived immutable assignment for the agent contract tests.
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat().replace("+00:00", "Z")
    return ShardAssignment(
        campaign_id="campaign-agent",
        shard_id="shard-000",
        lease_id="lease-agent",
        attempt=0,
        mutant_ids=("m1",),
        prepared_snapshot_id="snapshot-agent",
        workspace_descriptor_id="workspace-agent",
        expires_at=expires_at,
    )
def test_spool_is_idempotent_and_recoverable(tmp_path: Path) -> None:
    # Require fsync-backed event delivery to survive reopening before acknowledgement.
    first = DurableExecutionSpool(tmp_path / "spool")
    entry = first.publish("event-1", {"result": {"status": "killed"}})
    assert first.publish("event-1", {"result": {"status": "killed"}}) == entry
    assert first.pending() == (entry,)
    reopened = DurableExecutionSpool(tmp_path / "spool")
    assert reopened.pending() == (entry,)
    reopened.acknowledge("event-1")
    assert reopened.pending() == ()
    assert DurableExecutionSpool(tmp_path / "spool").acknowledged("event-1")
def test_worker_agent_heartbeats_and_acknowledges_after_spooling(tmp_path: Path) -> None:
    # Keep the agent heartbeat lifecycle alive through execution and stop it only after durable delivery.
    agent = WorkerAgent(
        identity=WorkerIdentity("worker-agent", "instance-agent", os.getpid(), "birth-agent"),
        capabilities=WorkerAgent.local_capabilities(),
        spool=DurableExecutionSpool(tmp_path / "spool"),
        heartbeat_interval_seconds=0.01,
    )
    heartbeat_count = 0
    def heartbeat(message):
        # Accept typed heartbeats while recording that the live loop actually ran.
        nonlocal heartbeat_count
        heartbeat_count += 1
        assert message.worker.worker_id == "worker-agent"
        return HeartbeatReceipt(accepted=True, lease_valid=True, lease_revision=1)
    run = agent.run_assignment(_assignment(), lambda: {"status": "complete"}, heartbeat=heartbeat)
    assert heartbeat_count >= 1
    assert agent.state.value == "delivering"
    assert agent.spool.pending()[0].event_id == run.event_id
    agent.acknowledge(run.event_id)
    assert agent.state.value == "idle"
    assert agent.spool.pending() == ()
def test_worker_agent_stops_on_rejected_heartbeat(tmp_path: Path) -> None:
    # Turn a negative authoritative receipt into a fail-closed worker error without leaving a heartbeat thread.
    agent = WorkerAgent(
        identity=WorkerIdentity("worker-agent", "instance-agent", os.getpid(), "birth-agent"),
        capabilities=WorkerAgent.local_capabilities(),
        spool=DurableExecutionSpool(tmp_path / "spool"),
        heartbeat_interval_seconds=0.01,
    )
    def heartbeat(message):
        # Reject the first ownership check so no user execution can begin under a stale lease.
        del message
        return HeartbeatReceipt(accepted=False, lease_valid=False, reason="lease_expired")
    with pytest.raises(WorkerLeaseLost, match="lease_expired"):
        agent.run_assignment(_assignment(), lambda: {"status": "must-not-run"}, heartbeat=heartbeat)
    assert agent.state.value == "failed"
    assert agent.spool.pending() == ()
