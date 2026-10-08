from __future__ import annotations
import sys
import time
from pathlib import Path
import pytest
from theseus_local.process import EngineProcessError
from theseus_local.process import EngineProcessSession
def test_engine_session_drains_more_than_ten_mebibytes_of_stderr(tmp_path: Path) -> None:
    # Keep the protocol responsive while a child emits diagnostic stderr beyond the pipe capacity.
    code = (
        "import json, sys\n"
        "sys.stderr.write('x' * (11 * 1024 * 1024))\n"
        "sys.stderr.flush()\n"
        "for line in sys.stdin:\n"
        "    frame = json.loads(line)\n"
        "    print(json.dumps({'request_id': frame['request_id'], 'ok': True, 'result': {'command': frame['command']}}), flush=True)\n"
        "    if frame['command'] == 'shutdown':\n"
        "        break\n"
    )
    session = EngineProcessSession(
        events_path=tmp_path / "events.jsonl",
        protocol_path=tmp_path / "responses.jsonl",
        stdout_path=tmp_path / "stdout.log",
        stderr_path=tmp_path / "stderr.log",
        cwd=tmp_path,
        command=(sys.executable, "-c", code),
        request_timeout_seconds=20.0,
    )
    with session:
        assert session.request("ping")["command"] == "ping"
    assert (tmp_path / "stderr.log").stat().st_size >= 10 * 1024 * 1024
    assert (tmp_path / "stderr.log").stat().st_size <= session.max_artifact_bytes
def test_engine_session_close_kills_child_that_ignores_shutdown(tmp_path: Path) -> None:
    # Bound close even when a malformed worker ignores both protocol shutdown and SIGTERM.
    code = (
        "import signal, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "time.sleep(30)\n"
    )
    session = EngineProcessSession(
        events_path=tmp_path / "events.jsonl",
        protocol_path=tmp_path / "responses.jsonl",
        stdout_path=tmp_path / "stdout.log",
        stderr_path=tmp_path / "stderr.log",
        cwd=tmp_path,
        command=(sys.executable, "-c", code),
        request_timeout_seconds=0.3,
        heartbeat_interval_seconds=0.05,
    )
    started = time.monotonic()
    with pytest.raises(EngineProcessError):
        with session:
            session.request("hang")
    assert time.monotonic() - started < 8.0
    assert session._process is None