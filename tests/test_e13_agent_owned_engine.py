from __future__ import annotations

import inspect
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from theseus_contracts import ShardAssignment
from theseus_local.coordinator import LocalCampaignCoordinator
from theseus_local.worker_runtime import PersistentWorkerProcess


def _assignment(shard_id: str, mutant_id: str) -> ShardAssignment:
    # Build one immutable assignment accepted by the standalone worker process.
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat().replace("+00:00", "Z")
    return ShardAssignment(
        campaign_id="campaign-agent-engine",
        shard_id=shard_id,
        lease_id=f"lease-{shard_id}",
        attempt=0,
        mutant_ids=(mutant_id,),
        prepared_snapshot_id="snapshot-agent-engine",
        workspace_descriptor_id="workspace-agent-engine",
        expires_at=expires_at,
    )


def _fake_engine(tmp_path: Path) -> tuple[str, ...]:
    # Create a staged engine fixture that exposes its child PID through the real session transport.
    script = tmp_path / "fake_engine.py"
    script.write_text(
        "import json, sys\n"
        "mutants = [\n"
        "  {'mutant_id': 'm1', 'mutation': 'condition_to_not', 'source_path': 'app.py', 'line_no': 1, 'column_no': 0, 'original': 'x', 'replacement': 'y'},\n"
        "  {'mutant_id': 'm2', 'mutation': 'condition_to_not', 'source_path': 'app.py', 'line_no': 2, 'column_no': 0, 'original': 'x', 'replacement': 'y'},\n"
        "]\n"
        "for line in sys.stdin:\n"
        "    frame = json.loads(line)\n"
        "    command = frame['command']\n"
        "    request = frame.get('request', {})\n"
        "    if command == 'prepare':\n"
        "        result = {'campaign_id': 'campaign-agent-engine', 'source_path': 'app.py', 'source_sha256': 'source-sha', 'index_version': 'index-v1', 'mutants': mutants}\n"
        "    elif command == 'execute-shard':\n"
        "        result = {'shard_id': request['shard']['shard_id'], 'worker_id': request['worker_id'], 'status': 'complete', 'completed_mutants': 0, 'results': [], 'report_path': 'fake://report'}\n"
        "    elif command == 'shutdown':\n"
        "        result = {'status': 'stopped'}\n"
        "    else:\n"
        "        result = {}\n"
        "    print(json.dumps({'request_id': frame['request_id'], 'ok': True, 'result': result}), flush=True)\n"
        "    if command == 'shutdown': break\n",
        encoding="utf-8",
    )
    return (sys.executable, str(script))


def _execute_request(assignment: ShardAssignment) -> dict[str, object]:
    # Build the private engine command payload without importing coordinator implementation details.
    return {
        "campaign_id": assignment.campaign_id,
        "shard": {
            "shard_id": assignment.shard_id,
            "mutant_ids": list(assignment.mutant_ids),
            "estimated_cost": 1.0,
        },
        "attempt": assignment.attempt,
        "worker_id": "persistent-worker-engine",
        "lease_id": assignment.lease_id,
        "test_overrides": {},
    }


def test_standalone_agent_owns_the_engine_child(tmp_path: Path) -> None:
    # Prove EngineProcessSession is constructed below the persistent worker PID rather than by the coordinator.
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    with PersistentWorkerProcess(
        worker_id="persistent-worker-engine",
        instance_id="instance-agent-engine",
        spool_root=tmp_path / "spool",
        heartbeat_interval_seconds=0.01,
        cwd=Path(__file__).parents[1],
    ) as worker:
        registered = worker.wait_for("registered")
        worker_pid = registered["process_id"]
        worker.wait_for("acquire")
        assignment = _assignment("shard-001", "m1")
        correlation_id = worker.send_engine_assignment(
            assignment,
            configuration={"campaign_id": assignment.campaign_id},
            execute_request=_execute_request(assignment),
            workspace=workspace,
            report_root=tmp_path / "reports" / "assignment-1",
            expected_source_sha256="source-sha",
            expected_mutant_ids=("m1", "m2"),
            test_fingerprints={"m1": {}},
            command_timeouts={"prepare": 5.0, "execute-shard": 5.0, "shutdown": 2.0},
            engine_command=_fake_engine(tmp_path),
        )
        started = worker.wait_for("engine_started", correlation_id=correlation_id)
        child_pid = started["payload"]["child_process_id"]
        assert started["process_id"] == worker_pid
        assert child_pid != worker_pid
        delivery = worker.wait_for("delivery", correlation_id=correlation_id)
        assert delivery["payload"]["payload"]["result"]["engine_process_id"] == child_pid
        worker.acknowledge(delivery["payload"]["event_id"])
        worker.wait_for("acknowledged", correlation_id=correlation_id)
        worker.wait_for("acquire")
        worker.shutdown()
        worker.wait_for("terminated")
        assert worker.wait() == 0


def test_parallel_coordinator_does_not_construct_engine_sessions() -> None:
    # Keep execution-process ownership below the standalone worker boundary in the production parallel path.
    source = inspect.getsource(LocalCampaignCoordinator._execute_parallel_shards)
    assert "PersistentWorkerProcess(" in source
    assert "EngineProcessSession(" not in source
    assert "_execute_dynamic_shards" not in inspect.getsource(LocalCampaignCoordinator)
