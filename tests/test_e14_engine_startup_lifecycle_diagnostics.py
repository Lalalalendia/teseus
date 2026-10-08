from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from theseus_contracts import ShardAssignment
from theseus_local.process import EngineProcessSession
from theseus_local.worker_runtime import PersistentWorkerProcess


def _assignment() -> ShardAssignment:
    # Build one immutable assignment used to prove bounded pre-prepare engine restart.
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat().replace(
        "+00:00",
        "Z",
    )
    return ShardAssignment(
        campaign_id="campaign-engine-startup-recovery",
        shard_id="shard-000",
        lease_id="lease-engine-startup-recovery",
        attempt=0,
        mutant_ids=("m1",),
        prepared_snapshot_id="snapshot-engine-startup-recovery",
        workspace_descriptor_id="workspace-engine-startup-recovery",
        expires_at=expires_at,
    )


def _restartable_engine(tmp_path: Path) -> tuple[str, ...]:
    # Create an engine that exits before its first prepare response and succeeds on the next process.
    marker = tmp_path / "first-prepare-exited.marker"
    script = tmp_path / "restartable_engine.py"
    script.write_text(
        "import json, pathlib, sys\n"
        "marker = pathlib.Path(sys.argv[1])\n"
        "mutants = [{'mutant_id': 'm1', 'mutation': 'condition_to_not', "
        "'source_path': 'app.py', 'line_no': 1, 'column_no': 0, "
        "'original': 'x', 'replacement': 'not x'}]\n"
        "for line in sys.stdin:\n"
        "    frame = json.loads(line)\n"
        "    command = frame['command']\n"
        "    if command == 'prepare' and not marker.exists():\n"
        "        marker.write_text('exited-before-prepare-response', encoding='utf-8')\n"
        "        raise SystemExit(91)\n"
        "    if command == 'prepare':\n"
        "        result = {'campaign_id': 'campaign-engine-startup-recovery', "
        "'source_path': 'app.py', 'source_sha256': 'source-sha', "
        "'index_version': 'index-v1', 'snapshot_id': "
        "'snapshot-engine-startup-recovery', 'mutants': mutants}\n"
        "    else:\n"
        "        result = {'status': 'stopped'}\n"
        "    print(json.dumps({'request_id': frame['request_id'], 'ok': True, "
        "'result': result}), flush=True)\n"
        "    if command == 'shutdown':\n"
        "        break\n",
        encoding="utf-8",
    )
    return (sys.executable, str(script), str(marker))


def test_worker_restarts_engine_once_after_pre_prepare_transport_exit(tmp_path: Path) -> None:
    # Require one bounded restart before mutation execution while retaining the same persistent worker PID.
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    command = _restartable_engine(tmp_path)
    assignment = _assignment()
    with PersistentWorkerProcess(
        worker_id="worker-engine-startup-recovery",
        instance_id="instance-engine-startup-recovery",
        spool_root=tmp_path / "spool",
        heartbeat_interval_seconds=0.01,
        cwd=Path(__file__).parents[1],
    ) as worker:
        registered = worker.wait_for("registered")
        worker_pid = int(registered["process_id"])
        worker.wait_for("acquire")
        correlation_id = worker.send_engine_assignment(
            assignment,
            configuration={"campaign_id": assignment.campaign_id},
            execute_request=None,
            workspace=workspace,
            report_root=tmp_path / "reports",
            expected_source_sha256="source-sha",
            expected_mutant_ids=("m1",),
            test_fingerprints={},
            command_timeouts={"prepare": 5.0, "shutdown": 2.0},
            engine_command=command,
        )
        first_started = worker.wait_for("engine_started", correlation_id=correlation_id)
        first_stopped = worker.wait_for("engine_stopped", correlation_id=correlation_id)
        second_started = worker.wait_for("engine_started", correlation_id=correlation_id)
        second_stopped = worker.wait_for("engine_stopped", correlation_id=correlation_id)
        delivery = worker.wait_for("delivery", correlation_id=correlation_id)
        first_pid = int(first_started["payload"]["child_process_id"])
        second_pid = int(second_started["payload"]["child_process_id"])
        assert first_stopped["payload"]["child_process_id"] == first_pid, (
            "first failed engine attempt did not publish a matching stop identity; "
            f"first_started={first_started}; first_stopped={first_stopped}; "
            f"worker_pid={worker_pid}; correlation_id={correlation_id}"
        )
        assert second_stopped["payload"]["child_process_id"] == second_pid, (
            "successful replacement engine did not publish a matching stop identity; "
            f"second_started={second_started}; second_stopped={second_stopped}; "
            f"worker_pid={worker_pid}; correlation_id={correlation_id}"
        )
        assert first_pid != second_pid and worker.pid == worker_pid, (
            "pre-prepare recovery replaced the persistent worker or reused the failed child identity; "
            f"worker_pid={worker_pid}; current_worker_pid={worker.pid}; "
            f"first_engine_pid={first_pid}; second_engine_pid={second_pid}; "
            f"correlation_id={correlation_id}"
        )
        assert delivery["payload"]["payload"]["result"]["engine_process_id"] == second_pid, (
            "durable delivery was attributed to the failed engine attempt instead of the replacement; "
            f"delivery={delivery}; first_engine_pid={first_pid}; second_engine_pid={second_pid}; "
            f"correlation_id={correlation_id}"
        )
        marker = Path(command[-1])
        assert marker.read_text(encoding="utf-8") == "exited-before-prepare-response", (
            "fixture did not enter the intended pre-prepare transport failure window; "
            f"marker={marker}; delivery={delivery}; correlation_id={correlation_id}"
        )
        worker.acknowledge(delivery["payload"]["event_id"])
        worker.wait_for("acknowledged", correlation_id=correlation_id)


def test_engine_session_close_releases_all_parent_pipe_handles(tmp_path: Path) -> None:
    # Verify successful shutdown closes stdin, stdout, and stderr instead of relying on delayed Popen garbage collection.
    code = (
        "import json, sys\n"
        "for line in sys.stdin:\n"
        "    frame = json.loads(line)\n"
        "    print(json.dumps({'request_id': frame['request_id'], 'ok': True, "
        "'result': {}}), flush=True)\n"
        "    if frame['command'] == 'shutdown': break\n"
    )
    session = EngineProcessSession(
        events_path=tmp_path / "events.jsonl",
        protocol_path=tmp_path / "responses.jsonl",
        stdout_path=tmp_path / "stdout.log",
        stderr_path=tmp_path / "stderr.log",
        cwd=tmp_path,
        command=(sys.executable, "-c", code),
        request_timeout_seconds=5.0,
    )
    session.start()
    process = session._process
    assert process is not None
    streams = (process.stdin, process.stdout, process.stderr)
    assert session.request("ping") == {}
    returncode = session.close()
    assert returncode == 0, (
        "engine session did not exit cleanly after explicit shutdown; "
        f"returncode={returncode}; diagnostics={json.dumps(session.diagnostics(), sort_keys=True)}"
    )
    assert all(stream is not None and stream.closed for stream in streams), (
        "engine session leaked one or more parent-side pipe handles; "
        f"stream_states={[(type(stream).__name__, getattr(stream, 'closed', None)) for stream in streams]}; "
        f"diagnostics={json.dumps(session.diagnostics(), sort_keys=True)}"
    )
