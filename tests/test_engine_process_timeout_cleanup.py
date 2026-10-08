from __future__ import annotations

import sys
from pathlib import Path

import pytest

from theseus_local.process import EngineProcessError, EngineProcessSession


def _session(tmp_path: Path, code: str, **kwargs: object) -> EngineProcessSession:
    # Build one isolated fake-engine session for watchdog and cleanup tests.
    return EngineProcessSession(
        events_path=tmp_path / "events.jsonl",
        protocol_path=tmp_path / "responses.jsonl",
        stdout_path=tmp_path / "stdout.log",
        stderr_path=tmp_path / "stderr.log",
        cwd=tmp_path,
        command=(sys.executable, "-c", code),
        **kwargs,
    )


def test_timeout_poisons_session_and_kills_post_summary_child(tmp_path: Path) -> None:
    # Model pytest finishing output while cleanup never returns and require a terminal process-tree kill.
    code = (
        "import sys, threading, time\n"
        "sys.stdin.readline()\n"
        "sys.stderr.write('1 passed in 0.08s\\n'); sys.stderr.flush()\n"
        "threading.Thread(target=lambda: time.sleep(30), daemon=False).start()\n"
        "time.sleep(30)\n"
    )
    session = _session(
        tmp_path,
        code,
        request_timeout_seconds=900.0,
        command_timeout_seconds={"execute-shard": 1.5, "shutdown": 0.5},
    )
    with session:
        pid = session.pid
        reader_threads = tuple(session._reader_threads)
        with pytest.raises(EngineProcessError, match="execute-shard.*timed out"):
            session.request("execute-shard")
        with pytest.raises(EngineProcessError, match="session is unusable"):
            session.request("ping")
    assert pid is not None
    assert pid not in EngineProcessSession.active_process_ids()
    assert not any(thread.is_alive() for thread in reader_threads)
    assert "1 passed in 0.08s" in (tmp_path / "stderr.log").read_text(encoding="utf-8")


def test_missing_request_id_poison_session(tmp_path: Path) -> None:
    # Reject uncorrelated stdout so a stale or diagnostic line can never satisfy the active request.
    code = "import json, sys; sys.stdin.readline(); print(json.dumps({'ok': True, 'result': {}}), flush=True)"
    session = _session(tmp_path, code, request_timeout_seconds=2.0)
    with session:
        with pytest.raises(EngineProcessError, match="protocol desynchronized"):
            session.request("ping")
        with pytest.raises(EngineProcessError, match="session is unusable"):
            session.request("second")
