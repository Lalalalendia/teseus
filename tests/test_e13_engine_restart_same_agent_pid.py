from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from theseus_contracts import ShardAssignment
from theseus_local.worker_runtime import PersistentWorkerProcess


def _assignment(ordinal: int) -> ShardAssignment:
    # Build one sequential assignment for the same persistent agent identity.
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat().replace("+00:00", "Z")
    return ShardAssignment(
        campaign_id="campaign-engine-restart",
        shard_id=f"shard-{ordinal:03d}",
        lease_id=f"lease-{ordinal:03d}",
        attempt=0,
        mutant_ids=(f"m{ordinal}",),
        prepared_snapshot_id="snapshot-engine-restart",
        workspace_descriptor_id="workspace-engine-restart",
        expires_at=expires_at,
    )


def _fake_engine(tmp_path: Path) -> tuple[str, ...]:
    # Create one reusable engine program whose OS process is restarted for each assignment.
    script = tmp_path / "restart_engine.py"
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
        "    if command == 'prepare': result = {'campaign_id': 'campaign-engine-restart', 'source_path': 'app.py', 'source_sha256': 'source-sha', 'index_version': 'index-v1', 'mutants': mutants}\n"
        "    elif command == 'execute-shard': result = {'shard_id': request['shard']['shard_id'], 'worker_id': request['worker_id'], 'status': 'complete', 'completed_mutants': 0, 'results': []}\n"
        "    else: result = {'status': 'stopped'}\n"
        "    print(json.dumps({'request_id': frame['request_id'], 'ok': True, 'result': result}), flush=True)\n"
        "    if command == 'shutdown': break\n",
        encoding="utf-8",
    )
    return (sys.executable, str(script))


def test_engine_restarts_without_replacing_the_agent_pid(tmp_path: Path) -> None:
    # Require a fresh engine child for each shard while one persistent agent PID receives both assignments.
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    engine_command = _fake_engine(tmp_path)
    with PersistentWorkerProcess(
        worker_id="persistent-worker-restart",
        instance_id="instance-engine-restart",
        spool_root=tmp_path / "spool",
        heartbeat_interval_seconds=0.01,
        cwd=Path(__file__).parents[1],
    ) as worker:
        registered = worker.wait_for("registered")
        agent_pid = registered["process_id"]
        child_pids: list[int] = []
        for ordinal in (1, 2):
            worker.wait_for("acquire")
            assignment = _assignment(ordinal)
            correlation_id = worker.send_engine_assignment(
                assignment,
                configuration={"campaign_id": assignment.campaign_id},
                execute_request={
                    "campaign_id": assignment.campaign_id,
                    "shard": {"shard_id": assignment.shard_id, "mutant_ids": list(assignment.mutant_ids)},
                    "attempt": 0,
                    "worker_id": "persistent-worker-restart",
                    "lease_id": assignment.lease_id,
                    "test_overrides": {},
                },
                workspace=workspace,
                report_root=tmp_path / "reports" / str(ordinal),
                expected_source_sha256="source-sha",
                expected_mutant_ids=("m1", "m2"),
                test_fingerprints={f"m{ordinal}": {}},
                command_timeouts={"prepare": 5.0, "execute-shard": 5.0, "shutdown": 2.0},
                engine_command=engine_command,
            )
            started = worker.wait_for("engine_started", correlation_id=correlation_id)
            child_pids.append(int(started["payload"]["child_process_id"]))
            delivery = worker.wait_for("delivery", correlation_id=correlation_id)
            assert delivery["process_id"] == agent_pid
            worker.acknowledge(delivery["payload"]["event_id"])
            worker.wait_for("acknowledged", correlation_id=correlation_id)
        assert worker.pid == agent_pid
        assert len(set(child_pids)) == 2
        assert agent_pid not in child_pids
        worker.wait_for("acquire")
        worker.shutdown()
        worker.wait_for("terminated")
        assert worker.wait() == 0
