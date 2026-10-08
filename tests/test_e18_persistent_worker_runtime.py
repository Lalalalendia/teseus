from __future__ import annotations
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pytest
from theseus_contracts import HeartbeatReceipt, ShardAssignment, WorkerIdentity
from theseus_local.worker_pool import PersistentWorkerSupervisor
from theseus_local.worker_runtime import DurableExecutionSpool, WorkerAgent
from test_intelligence_unified_v1.runner import MutationConfig, MutationRunner

def _assignment(shard_id: str, mutant_id: str) -> ShardAssignment:
    # Build a live immutable assignment for repeated acquisition and heartbeat tests.
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat().replace("+00:00", "Z")
    return ShardAssignment(
        campaign_id="campaign-persistent",
        shard_id=shard_id,
        lease_id=f"lease-{shard_id}",
        attempt=0,
        mutant_ids=(mutant_id,),
        prepared_snapshot_id="snapshot-persistent",
        workspace_descriptor_id=f"workspace-{shard_id}",
        expires_at=expires_at,
    )

def _result(mutant_id: str) -> dict[str, object]:
    # Build one shard payload containing a durable per-mutant execution row.
    return {
        "shard_result": {
            "status": "complete",
            "completed_mutants": 1,
            "results": [
                {
                    "execution_id": f"execution-{mutant_id}",
                    "mutant_id": mutant_id,
                    "status": "killed",
                }
            ],
        }
    }

def _agent(root: Path) -> WorkerAgent:
    # Build one reusable local agent with a short heartbeat interval.
    return WorkerAgent(
        identity=WorkerIdentity("worker-persistent", "instance-persistent", os.getpid(), "birth-persistent"),
        capabilities=WorkerAgent.local_capabilities(),
        spool=DurableExecutionSpool(root / "spool"),
        heartbeat_interval_seconds=0.01,
    )

def test_heartbeat_stays_active_until_after_delivery_ack(tmp_path: Path) -> None:
    # Keep renewing after execution returns and stop only after the coordinator ACK boundary.
    agent = _agent(tmp_path)
    heartbeats = 0
    def heartbeat(message):
        # Accept the current typed lease while counting renewals through fan-in.
        nonlocal heartbeats
        heartbeats += 1
        assert message.current_shard_id == "shard-001"
        return HeartbeatReceipt(accepted=True, lease_valid=True, lease_revision=heartbeats)
    delivery = agent.run_assignment(
        _assignment("shard-001", "m1"),
        lambda: _result("m1"),
        heartbeat=heartbeat,
    )
    count_after_execution = heartbeats
    time.sleep(0.03)
    assert agent.heartbeat_active is True
    assert heartbeats > count_after_execution
    assert delivery.mutant_event_ids
    assert len(agent.spool.mutant_events(parent_event_id=delivery.event_id)) == 1
    agent.acknowledge(delivery.event_id)
    assert agent.heartbeat_active is False
    assert agent.state.value == "idle"

def test_supervisor_replays_pending_then_acquires_follow_on_shards(tmp_path: Path) -> None:
    # Replay startup delivery before repeatedly acquiring the next shard on one agent instance.
    agent = _agent(tmp_path)
    supervisor = PersistentWorkerSupervisor(agent)
    pending = agent.spool.publish(
        "startup-event",
        {
            "assignment": _assignment("shard-old", "m0").to_dict(),
            "result": _result("m0"),
        },
    )
    replayed: list[str] = []
    assert supervisor.replay_pending(lambda entry: replayed.append(entry.event_id)) == (pending.event_id,)
    assert replayed == [pending.event_id]
    assignments = iter((_assignment("shard-001", "m1"), _assignment("shard-002", "m2")))
    committed: list[str] = []
    runs = supervisor.acquire_loop(
        lambda: next(assignments, None),
        lambda assignment: _result(assignment.mutant_ids[0]),
        lambda assignment, delivery: committed.append(assignment.shard_id),
    )
    assert len(runs) == 2
    assert committed == ["shard-001", "shard-002"]
    assert agent.spool.pending() == ()

def test_serial_runner_rejects_legacy_parallel_routing(tmp_path: Path) -> None:
    # Fail closed instead of reaching workers.py through the production runner entry point.
    runner = MutationRunner(MutationConfig(project_root=tmp_path, source="app.py", workers=2))
    with pytest.raises(ValueError, match="LocalCampaignCoordinator"):
        runner.run()
