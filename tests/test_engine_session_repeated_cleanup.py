from __future__ import annotations

import sys
from pathlib import Path

from theseus_local.process import EngineProcessSession


def _session(tmp_path: Path, code: str) -> EngineProcessSession:
    # Build one isolated fake-engine session for repeated lifecycle tests.
    return EngineProcessSession(
        events_path=tmp_path / "events.jsonl",
        protocol_path=tmp_path / "responses.jsonl",
        stdout_path=tmp_path / "stdout.log",
        stderr_path=tmp_path / "stderr.log",
        cwd=tmp_path,
        command=(sys.executable, "-c", code),
        request_timeout_seconds=5.0,
    )


def test_many_engine_sessions_release_processes_pipes_and_readers(tmp_path: Path) -> None:
    # Exercise repeated startup and shutdown so order-dependent transport residue becomes deterministic.
    code = (
        "import json, sys\n"
        "for line in sys.stdin:\n"
        "    frame = json.loads(line)\n"
        "    print(json.dumps({'request_id': frame['request_id'], 'ok': True, 'result': {}}), flush=True)\n"
        "    if frame['command'] == 'shutdown': break\n"
    )
    for index in range(25):
        root = tmp_path / str(index)
        root.mkdir()
        session = _session(root, code)
        with session:
            pid = session.pid
            reader_threads = tuple(session._reader_threads)
            assert session.request("ping") == {}
        assert pid is not None
        assert pid not in EngineProcessSession.active_process_ids()
        assert not any(thread.is_alive() for thread in reader_threads)
