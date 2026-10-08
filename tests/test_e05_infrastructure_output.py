import io
import sys
from pathlib import Path

from test_intelligence_unified_v1.commands import (
    _scan_output_summary,
    looks_like_infrastructure_failure,
    run_argv,
)


class _ChunkedStream:
    def __init__(self, chunks: list[bytes]) -> None:
        # Expose deterministic chunk boundaries for split-marker coverage.
        self._chunks = iter(chunks)

    def flush(self) -> None:
        # Match the binary artifact interface used by the scanner.
        return None

    def seek(self, offset: int) -> int:
        # Keep the scanner contract without replaying already supplied chunks.
        if offset != 0:
            raise ValueError("only rewind to zero is supported")
        return 0

    def read(self, size: int = -1) -> bytes:
        # Return one synthetic chunk per scanner read.
        del size
        return next(self._chunks, b"")


def test_full_artifact_flags_middle_marker_outside_preview(tmp_path: Path) -> None:
    # Detect an infrastructure marker in the middle of output omitted from head/tail preview.
    code = (
        "import sys;"
        "sys.stdout.write('A' * 100000);"
        "sys.stdout.write('INTERNALERROR synthetic\\n');"
        "sys.stdout.write('B' * 100000);"
        "sys.exit(1)"
    )
    result = run_argv(
        (sys.executable, "-c", code),
        cwd=tmp_path,
        timeout_seconds=5,
        output_limit=64,
        output_artifact=tmp_path / "output.log",
    )
    assert result.exit_code == 1
    assert "INTERNALERROR" not in result.output
    assert "pytest_internal_error" in result.infrastructure_flags
    assert result.diagnostic_excerpts
    assert looks_like_infrastructure_failure(result.output) is False
    assert looks_like_infrastructure_failure(
        result.output,
        infrastructure_flags=result.infrastructure_flags,
    )


def test_split_marker_is_detected_across_binary_chunks() -> None:
    # Preserve marker detection when a diagnostic token is split between reads.
    stream = _ChunkedStream([b"prefix INTERNAL", b"ERROR suffix"])
    result = _scan_output_summary(stream, output_limit=32)
    assert "pytest_internal_error" in result[5]


def test_multiple_flags_and_binary_output_are_bounded() -> None:
    # Classify several infrastructure families without decoding or retaining the full artifact.
    payload = b"\x00ERROR collecting tests\x00workerprocesscrashed\x00"
    size, preview, _, _, _, flags, excerpts = _scan_output_summary(io.BytesIO(payload), output_limit=8)
    assert size == len(payload)
    assert preview
    assert {"collection_error", "worker_crash"}.issubset(flags)
    assert len(excerpts) <= 8


def test_assertion_failure_does_not_become_infrastructure_failure(tmp_path: Path) -> None:
    # Keep ordinary test failures on the test-failure path when no infrastructure marker exists.
    result = run_argv(
        (sys.executable, "-c", "raise AssertionError('ordinary failure')"),
        cwd=tmp_path,
        timeout_seconds=5,
    )
    assert result.exit_code == 1
    assert result.infrastructure_flags == ()
    assert looks_like_infrastructure_failure(
        result.output,
        infrastructure_flags=result.infrastructure_flags,
    ) is False
