import atexit
import hashlib
import io
import os
import sys
import threading
from pathlib import Path

from test_intelligence_unified_v1.commands import _read_output_summary
from test_intelligence_unified_v1.trace import TRACE_SITE


class CountingStream:
    def __init__(self, data: bytes) -> None:
        # Track seek/read calls while exposing the binary stream contract used by the summary reader.
        self._stream = io.BytesIO(data)
        self.read_calls = 0
        self.seek_calls = 0

    def flush(self) -> None:
        # Match the file API without adding a second read pass.
        self._stream.flush()

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        # Count repositioning so the test detects accidental rescans.
        self.seek_calls += 1
        return self._stream.seek(offset, whence)

    def read(self, size: int = -1) -> bytes:
        # Count each sequential chunk consumed by the one-pass scanner.
        self.read_calls += 1
        return self._stream.read(size)


def test_output_summary_scans_large_artifact_once() -> None:
    # Hash a bounded preview in one sequential pass over the output artifact.
    payload = bytes(range(256)) * 8
    stream = CountingStream(payload)

    size, preview, head, tail, digest = _read_output_summary(stream, output_limit=32)

    assert size == len(payload)
    assert digest == hashlib.sha256(payload).hexdigest()
    assert head == payload[:16].decode("utf-8", errors="replace")
    assert tail == payload[-16:].decode("utf-8", errors="replace")
    assert "truncated 2016 bytes" in preview
    assert stream.seek_calls == 1
    assert stream.read_calls == 2


def test_trace_site_caches_paths_by_code_object(tmp_path: Path) -> None:
    # Resolve one source code object's path once while collecting repeated line events.
    output = tmp_path / "trace.json"
    previous_trace = sys.gettrace()
    previous_thread_trace = threading.gettrace()
    namespace: dict[str, object] = {"__name__": "sitecustomize"}
    try:
        environment = {
            "TI_TRACE_ROOT": str(tmp_path),
            "TI_TRACE_OUT": str(output),
        }
        original_environment = dict(os.environ)
        os.environ.update(environment)
        try:
            exec(TRACE_SITE, namespace)
        finally:
            os.environ.clear()
            os.environ.update(original_environment)
        module: dict[str, object] = {}
        exec(
            compile(
                "def traced(value):\n    # Exercise repeated line events for one code object.\n    result = value + 1\n    return result\n",
                str(tmp_path / "app.py"),
                "exec",
            ),
            module,
        )
        traced = module["traced"]
        traced(1)
        cache = namespace["_code_locations"]
        assert traced.__code__ in cache
        cache_size = len(cache)
        traced(2)
        assert len(cache) == cache_size
        assert cache[traced.__code__][1] == "app.py"
    finally:
        sys.settrace(previous_trace)
        threading.settrace(previous_thread_trace)
        flush = namespace.get("_flush")
        if flush is not None:
            atexit.unregister(flush)
