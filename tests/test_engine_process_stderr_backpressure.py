from __future__ import annotations

import sys
from pathlib import Path

from theseus_local.process import EngineProcessSession


def _session(tmp_path: Path, code: str, **kwargs: object) -> EngineProcessSession:
    # Build one isolated fake-engine session for stderr backpressure tests.
    return EngineProcessSession(
        events_path=tmp_path / "events.jsonl",
        protocol_path=tmp_path / "responses.jsonl",
        stdout_path=tmp_path / "stdout.log",
        stderr_path=tmp_path / "stderr.log",
        cwd=tmp_path,
        command=(sys.executable, "-c", code),
        **kwargs,
    )


def test_large_stderr_is_drained_before_protocol_response(tmp_path: Path) -> None:
    # Verify a child can emit far beyond the Windows pipe capacity and still return its response.
    code = (
        "import json, sys\n"
        "for line in sys.stdin:\n"
        "    frame = json.loads(line)\n"
        "    if frame['command'] == 'pressure':\n"
        "        sys.stderr.buffer.write(b'x' * (8 * 1024 * 1024))\n"
        "        sys.stderr.buffer.flush()\n"
        "    print(json.dumps({'request_id': frame['request_id'], 'ok': True, 'result': {'done': True}}), flush=True)\n"
        "    if frame['command'] == 'shutdown': break\n"
    )
    session = _session(
        tmp_path,
        code,
        request_timeout_seconds=10.0,
        max_artifact_bytes=2 * 1024 * 1024,
    )
    with session:
        assert session.request("pressure") == {"done": True}
    diagnostics = session.diagnostics()
    assert (tmp_path / "stderr.log").stat().st_size == 2 * 1024 * 1024
    assert diagnostics["truncated_bytes"]["stderr.log"] >= 6 * 1024 * 1024
