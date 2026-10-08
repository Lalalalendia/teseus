from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

from theseus_local.process import EngineProcessError, EngineProcessSession


def _session(tmp_path: Path, code: str, **kwargs: object) -> EngineProcessSession:
    # Build a short-lived protocol session with all stream artifacts isolated per test.
    return EngineProcessSession(
        events_path=tmp_path / "events.jsonl",
        protocol_path=tmp_path / "responses.jsonl",
        stdout_path=tmp_path / "stdout.log",
        stderr_path=tmp_path / "stderr.log",
        cwd=tmp_path,
        command=(sys.executable, "-c", code),
        **kwargs,
    )


def test_engine_protocol_correlates_request_ids(tmp_path: Path) -> None:
    # Verify every response carries the request token used by the permanent reader.
    code = (
        "import json, sys\n"
        "for line in sys.stdin:\n"
        "    frame = json.loads(line)\n"
        "    response = {'request_id': frame['request_id'], 'ok': True, 'result': {'command': frame['command']}}\n"
        "    print(json.dumps(response), flush=True)\n"
        "    if frame['command'] == 'shutdown': break\n"
    )
    session = _session(tmp_path, code)
    with session:
        assert session.request("first")["command"] == "first"
        assert session.request("second")["command"] == "second"
    responses = (tmp_path / "responses.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(responses) == 3
    request_ids = [json.loads(item)["request_id"] for item in responses]
    assert len(set(request_ids)) == 3


def test_engine_command_watchdog_overrides_long_session_default(tmp_path: Path) -> None:
    # Ensure a command-specific watchdog terminates a silent child without waiting for 900 seconds.
    code = "import time; time.sleep(30)"
    session = _session(
        tmp_path,
        code,
        request_timeout_seconds=900.0,
        command_timeout_seconds={"execute-shard": 0.35, "shutdown": 0.2},
    )
    started = time.monotonic()
    with pytest.raises(EngineProcessError, match="execute-shard.*timed out"):
        with session:
            session.request("execute-shard")
    assert time.monotonic() - started < 5.0


def test_repeated_sessions_leave_no_reader_threads(tmp_path: Path) -> None:
    # Exercise repeated startup/close cycles so a stale stdout or stderr reader cannot survive a campaign.
    code = (
        "import json, sys\n"
        "for line in sys.stdin:\n"
        "    frame = json.loads(line)\n"
        "    print(json.dumps({'request_id': frame['request_id'], 'ok': True, 'result': {}}), flush=True)\n"
        "    if frame['command'] == 'shutdown': break\n"
    )
    for index in range(12):
        session_root = tmp_path / str(index)
        session_root.mkdir()
        session = _session(session_root, code, request_timeout_seconds=5.0)
        with session:
            session.request("ping")
            reader_threads = tuple(session._reader_threads)
        assert reader_threads
        assert not any(thread.is_alive() for thread in reader_threads)


def test_post_summary_child_is_killed_by_command_watchdog(tmp_path: Path) -> None:
    # Model pytest printing its summary after request receipt before a non-terminating cleanup hook.
    code = (
        "import sys, threading, time\n"
        "sys.stdin.readline()\n"
        "sys.stderr.write('1 passed\\n'); sys.stderr.flush()\n"
        "threading.Thread(target=lambda: time.sleep(30), daemon=False).start()\n"
        "time.sleep(30)\n"
    )
    session = _session(
        tmp_path,
        code,
        request_timeout_seconds=900.0,
        command_timeout_seconds={"execute-shard": 1.5, "shutdown": 0.5},
    )
    with pytest.raises(EngineProcessError, match="execute-shard.*timed out"):
        with session:
            session.request("execute-shard")
    assert "1 passed" in (tmp_path / "stderr.log").read_text(encoding="utf-8")
