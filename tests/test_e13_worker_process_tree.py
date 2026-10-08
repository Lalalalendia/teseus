from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from theseus_contracts import ShardAssignment
from theseus_local.worker_runtime import PersistentWorkerProcess


def _pid_exists(pid: int) -> bool:
    # Check one process identity without adding a psutil runtime dependency.
    if os.name == "nt":
        result = subprocess.run(
            ("tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"),
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return str(pid).encode("ascii") in (result.stdout or b"")
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _wait_dead(pid: int, *, timeout: float = 5.0) -> None:
    # Wait for process-tree termination to become observable on both Windows and POSIX.
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _pid_exists(pid):
            return
        time.sleep(0.05)
    raise AssertionError(f"process remained alive: {pid}")


def test_terminating_agent_kills_engine_and_its_descendant(tmp_path: Path) -> None:
    # Prove the parent-side worker host terminates the complete agent-owned process tree.
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    grandchild_path = tmp_path / "grandchild.pid"
    engine_script = tmp_path / "hanging_engine.py"
    engine_script.write_text(
        "import json, subprocess, sys, time\n"
        f"grandchild_path = {str(grandchild_path)!r}\n"
        "for line in sys.stdin:\n"
        "    frame = json.loads(line)\n"
        "    if frame['command'] == 'prepare':\n"
        "        result = {'campaign_id': 'campaign-tree', 'source_path': 'app.py', 'source_sha256': 'source-sha', 'index_version': 'index-v1', 'mutants': [{'mutant_id': 'm1', 'mutation': 'condition_to_not', 'source_path': 'app.py', 'line_no': 1, 'column_no': 0, 'original': 'x', 'replacement': 'y'}]}\n"
        "        print(json.dumps({'request_id': frame['request_id'], 'ok': True, 'result': result}), flush=True)\n"
        "    elif frame['command'] == 'execute-shard':\n"
        "        child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
        "        open(grandchild_path, 'w', encoding='utf-8').write(str(child.pid))\n"
        "        time.sleep(60)\n",
        encoding="utf-8",
    )
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat().replace("+00:00", "Z")
    assignment = ShardAssignment(
        campaign_id="campaign-tree",
        shard_id="shard-tree",
        lease_id="lease-tree",
        attempt=0,
        mutant_ids=("m1",),
        prepared_snapshot_id="snapshot-tree",
        workspace_descriptor_id="workspace-tree",
        expires_at=expires_at,
    )
    worker = PersistentWorkerProcess(
        worker_id="persistent-worker-tree",
        instance_id="instance-worker-tree",
        spool_root=tmp_path / "spool",
        heartbeat_interval_seconds=0.02,
        cwd=Path(__file__).parents[1],
    )
    try:
        worker.start()
        registered = worker.wait_for("registered")
        agent_pid = int(registered["process_id"])
        worker.wait_for("acquire")
        correlation_id = worker.send_engine_assignment(
            assignment,
            configuration={"campaign_id": assignment.campaign_id},
            execute_request={
                "campaign_id": assignment.campaign_id,
                "shard": {"shard_id": assignment.shard_id, "mutant_ids": ["m1"]},
                "attempt": 0,
                "worker_id": "persistent-worker-tree",
                "lease_id": assignment.lease_id,
                "test_overrides": {},
            },
            workspace=workspace,
            report_root=tmp_path / "reports",
            expected_source_sha256="source-sha",
            expected_mutant_ids=("m1",),
            test_fingerprints={"m1": {}},
            command_timeouts={"prepare": 5.0, "execute-shard": 60.0, "shutdown": 1.0},
            engine_command=(sys.executable, str(engine_script)),
        )
        started = worker.wait_for("engine_started", correlation_id=correlation_id)
        engine_pid = int(started["payload"]["child_process_id"])
        deadline = time.monotonic() + 5.0
        while not grandchild_path.is_file() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert grandchild_path.is_file()
        grandchild_pid = int(grandchild_path.read_text(encoding="utf-8"))
        worker.terminate_tree()
        worker.wait(timeout=10.0)
        _wait_dead(agent_pid)
        _wait_dead(engine_pid)
        _wait_dead(grandchild_pid)
    finally:
        worker.close()
