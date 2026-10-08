"""Safe line-oriented subprocess transport for the local Theseus engine."""
from __future__ import annotations
import json
import os
import queue
import subprocess
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock, RLock, Thread
from typing import Any, BinaryIO, Callable, ClassVar, Mapping
from test_intelligence_unified_v1.commands import terminate_process_tree
from test_intelligence_unified_v1.io_utils import atomic_write_json
class EngineProcessError(RuntimeError):
    """Raised when the engine process cannot produce a valid protocol response."""
def _default_command_timeouts() -> dict[str, float]:
    # Bound every engine stage so a post-pytest cleanup deadlock cannot stall the suite for fifteen minutes.
    return {
        "prepare": 30.0,
        "collect": 60.0,
        "index": 60.0,
        "baseline": 900.0,
        "discover": 60.0,
        "execute-shard": 120.0,
        "finalize": 30.0,
        "shutdown": 5.0,
    }
@dataclass
class EngineProcessSession:
    """Long-lived engine process carrying staged JSONL commands and responses."""
    events_path: Path
    protocol_path: Path
    stdout_path: Path
    stderr_path: Path
    cwd: Path | None = None
    command: tuple[str, ...] | None = None
    request_timeout_seconds: float = 900.0
    command_timeout_seconds: Mapping[str, float] = field(default_factory=_default_command_timeouts)
    heartbeat_callback: Callable[[], None] | None = None
    heartbeat_interval_seconds: float = 1.0
    cancel_callback: Callable[[], bool] | None = None
    max_artifact_bytes: int = 16 * 1024 * 1024
    _process: subprocess.Popen[bytes] | None = field(default=None, init=False, repr=False)
    _responses: queue.Queue[tuple[str | None, bytes] | None] = field(
        default_factory=queue.Queue,
        init=False,
        repr=False,
    )
    _reader_threads: list[Thread] = field(default_factory=list, init=False, repr=False)
    _artifact_lock: Lock = field(default_factory=Lock, init=False, repr=False)
    _request_lock: RLock = field(default_factory=RLock, init=False, repr=False)
    _request_sequence: int = field(default=0, init=False, repr=False)
    _reader_error: str | None = field(default=None, init=False, repr=False)
    _broken_error: str | None = field(default=None, init=False, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)
    _truncated_bytes: dict[str, int] = field(default_factory=dict, init=False, repr=False)
    _stderr_tail: deque[bytes] = field(default_factory=lambda: deque(maxlen=64), init=False, repr=False)
    _performance_started: float | None = field(default=None, init=False, repr=False)
    _performance_seconds: dict[str, float] = field(default_factory=dict, init=False, repr=False)
    _performance_counts: dict[str, int] = field(default_factory=dict, init=False, repr=False)
    _active_pids: ClassVar[set[int]] = set()
    _active_pids_lock: ClassVar[Lock] = Lock()
    @classmethod
    def active_process_ids(cls) -> frozenset[int]:
        # Return engine children currently owned by sessions for leak and recovery checks.
        with cls._active_pids_lock:
            return frozenset(cls._active_pids)
    @property
    def performance_path(self) -> Path:
        # Resolve the durable engine-process timeline beside the line-oriented protocol artifact.
        return self.protocol_path.with_name("engine-process.performance.json")
    def _record_performance(self, phase: str, seconds: float) -> None:
        # Accumulate one non-overlapping engine-process interval under a stable phase name.
        normalized = str(phase).strip()
        if not normalized:
            raise ValueError("engine process performance phase must be non-empty")
        self._performance_seconds[normalized] = (
            self._performance_seconds.get(normalized, 0.0) + max(0.0, float(seconds))
        )
        self._performance_counts[normalized] = self._performance_counts.get(normalized, 0) + 1
    def _performance_payload(self, *, status: str, now: float | None = None) -> dict[str, Any]:
        # Reconcile recorded engine-process phases with the complete session wall time.
        sampled_now = time.monotonic() if now is None else float(now)
        started = self._performance_started if self._performance_started is not None else sampled_now
        total = max(0.0, sampled_now - started)
        observed = sum(self._performance_seconds.values())
        residual = max(0.0, total - observed)
        phases = [
            {"phase": phase, "wall_seconds": seconds, "source": "engine-process.monotonic"}
            for phase, seconds in self._performance_seconds.items()
        ]
        phases.append(
            {
                "phase": "engine_process_unattributed_residual",
                "wall_seconds": residual,
                "source": "engine-process.reconciliation",
            }
        )
        return {
            "schema_version": 1,
            "timeline_version": "engine-process-exclusive-v1",
            "status": str(status),
            "exclusive": True,
            "total_wall_seconds": total,
            "observed_phase_seconds": observed,
            "residual_seconds": residual,
            "accounted_seconds": observed + residual,
            "accounting_error_seconds": abs(total - observed - residual),
            "phases": phases,
            "counts": dict(sorted(self._performance_counts.items())),
            "pid": self.pid,
        }
    def _publish_performance(self, *, status: str) -> None:
        # Publish one bounded measurement artifact without changing engine command semantics.
        try:
            atomic_write_json(
                self.performance_path,
                self._performance_payload(status=status),
                durability="normal",
                category="report",
            )
        except (OSError, TypeError, ValueError):
            return
    def __enter__(self) -> "EngineProcessSession":
        # Start the child lazily at the coordinator boundary, not at object construction.
        self.start()
        return self
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        # Always close the child and persist bounded stream diagnostics on exit.
        del exc_type, exc_value, traceback
        self.close()
    def start(self) -> None:
        # Launch one shell-free engine process and drain both pipes for its entire lifetime.
        startup_started = time.monotonic()
        if self._performance_started is None:
            self._performance_started = startup_started
        if self._closed:
            raise EngineProcessError("engine process session is already closed")
        if self._process is not None:
            return
        for path in (self.events_path, self.protocol_path, self.stdout_path, self.stderr_path):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch(exist_ok=True)
        command = self.command or (sys.executable, "-m", "theseus_local.engine_worker")
        full_command = (*command, "--events", str(self.events_path))
        module_root = Path(__file__).resolve().parent.parent
        inherited_pythonpath = os.environ.get("PYTHONPATH", "")
        pythonpath_parts = [str(module_root)]
        if inherited_pythonpath:
            for item in inherited_pythonpath.split(os.pathsep):
                if not item:
                    continue
                candidate = Path(item)
                pythonpath_parts.append(
                    str((Path.cwd() / candidate).resolve() if not candidate.is_absolute() else candidate)
                )
        child_environment = dict(os.environ)
        child_environment["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)
        self._process = subprocess.Popen(
            full_command,
            cwd=str(self.cwd) if self.cwd is not None else None,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            start_new_session=os.name != "nt",
            env=child_environment,
        )
        with self._active_pids_lock:
            self._active_pids.add(int(self._process.pid))
        self._responses = queue.Queue()
        self._reader_error = None
        self._broken_error = None
        self._truncated_bytes.clear()
        self._stderr_tail.clear()
        self._reader_threads = [
            Thread(target=self._drain_stdout, name="theseus-engine-stdout", daemon=False),
            Thread(target=self._drain_stderr, name="theseus-engine-stderr", daemon=False),
        ]
        for thread in self._reader_threads:
            thread.start()
        self._record_performance("engine_session_startup", time.monotonic() - startup_started)
        self._publish_performance(status="running")
    def _append_artifact(self, path: Path, chunk: bytes) -> None:
        # Spool stream bytes directly to bounded artifacts instead of retaining pipe output in memory.
        if not chunk:
            return
        with self._artifact_lock:
            try:
                current_size = path.stat().st_size if path.exists() else 0
            except OSError:
                current_size = 0
            remaining = max(0, int(self.max_artifact_bytes) - current_size)
            written = min(len(chunk), remaining)
            if written:
                with path.open("ab") as handle:
                    handle.write(chunk[:written])
                    handle.flush()
            dropped = len(chunk) - written
            if dropped:
                self._truncated_bytes[path.name] = self._truncated_bytes.get(path.name, 0) + dropped
            if path == self.stderr_path:
                self._stderr_tail.append(bytes(chunk[-4096:]))
    @staticmethod
    def _response_request_id(raw: bytes) -> str | None:
        # Extract the correlation token without making the stdout reader own full response validation.
        try:
            value = json.loads(raw.decode("utf-8", errors="strict"))
        except (TypeError, ValueError, UnicodeError):
            return None
        if not isinstance(value, dict):
            return None
        token = value.get("request_id")
        return str(token) if token is not None else None
    @staticmethod
    def _read_pipe_chunk(stream: BinaryIO, size: int) -> bytes:
        # Read immediately available pipe bytes when the buffered stream supports read1().
        read1 = getattr(stream, "read1", None)
        if callable(read1):
            return bytes(read1(size))
        return bytes(stream.read(size))
    def _next_request_id(self) -> str:
        # Generate a process-local monotonic token so protocol frames cannot be confused.
        self._request_sequence += 1
        return f"{os.getpid()}-{id(self):x}-{self._request_sequence}"
    def _drain_stdout(self) -> None:
        # Read protocol lines continuously so the child can never fill the stdout pipe.
        process = self._process
        stream = process.stdout if process is not None else None
        if stream is None:
            return
        try:
            while True:
                raw = stream.readline()
                if not raw:
                    break
                self._append_artifact(self.stdout_path, raw)
                self._responses.put((self._response_request_id(raw), raw))
        except (OSError, ValueError) as exc:
            self._reader_error = f"engine stdout drain failed: {exc}"
        finally:
            self._responses.put(None)
    def _drain_stderr(self) -> None:
        # Drain diagnostic stderr in prompt chunks for the complete child lifetime.
        process = self._process
        stream = process.stderr if process is not None else None
        if stream is None:
            return
        try:
            while True:
                chunk = self._read_pipe_chunk(stream, 64 * 1024)
                if not chunk:
                    break
                self._append_artifact(self.stderr_path, chunk)
        except (OSError, ValueError) as exc:
            self._reader_error = f"engine stderr drain failed: {exc}"
    @property
    def pid(self) -> int | None:
        # Expose the actual OS process identity for registry and recovery diagnostics.
        return self._process.pid if self._process is not None else None
    def diagnostics(self) -> dict[str, Any]:
        # Return bounded transport diagnostics suitable for timeout and cancellation artifacts.
        process = self._process
        return {
            "pid": self.pid,
            "returncode": process.poll() if process is not None else None,
            "reader_error": self._reader_error,
            "broken_error": self._broken_error,
            "stderr_tail": b"".join(self._stderr_tail).decode("utf-8", errors="replace")[-65536:],
            "truncated_bytes": dict(self._truncated_bytes),
            "active_reader_threads": [thread.name for thread in self._reader_threads if thread.is_alive()],
            "stdout_path": str(self.stdout_path),
            "stderr_path": str(self.stderr_path),
            "protocol_path": str(self.protocol_path),
        }
    def _exit_diagnostics(self, command: str, process: subprocess.Popen[bytes]) -> str:
        # Format one bounded process-exit diagnostic without losing artifact paths or stderr evidence.
        diagnostics = self.diagnostics()
        return (
            f"engine exited before {command} response: "
            f"pid={process.pid}; returncode={process.poll()}; "
            f"reader_error={diagnostics['reader_error']!r}; "
            f"stderr_tail={diagnostics['stderr_tail']!r}; "
            f"stdout={self.stdout_path}; stderr={self.stderr_path}; "
            f"protocol={self.protocol_path}"
        )
    @staticmethod
    def _close_streams(process: subprocess.Popen[bytes]) -> tuple[str, ...]:
        # Close every parent-side pipe explicitly so long suites cannot exhaust Windows handles.
        errors: list[str] = []
        for name, stream in (
            ("stdin", process.stdin),
            ("stdout", process.stdout),
            ("stderr", process.stderr),
        ):
            if stream is None or stream.closed:
                continue
            try:
                stream.close()
            except OSError as exc:
                errors.append(f"{name}: {exc}")
        return tuple(errors)
    def _mark_broken(self, message: str, process: subprocess.Popen[bytes] | None) -> None:
        # Poison the session and terminate its process tree because command state is no longer trustworthy.
        if self._broken_error is None:
            self._broken_error = message
        if process is None or process.poll() is not None:
            return
        try:
            terminate_process_tree(process)
        except (OSError, subprocess.SubprocessError) as exc:
            self._reader_error = f"engine process-tree termination failed: {exc}"
    def _request_timeout(self, command: str, timeout_seconds: float | None) -> float:
        # Resolve the explicit, command-specific, or session-wide watchdog in that order.
        if timeout_seconds is not None:
            return max(0.1, float(timeout_seconds))
        if command in self.command_timeout_seconds:
            return max(0.1, float(self.command_timeout_seconds[command]))
        default_timeout = _default_command_timeouts().get(command)
        if default_timeout is not None:
            return max(0.1, float(default_timeout))
        return max(0.1, float(self.request_timeout_seconds))
    def request(
        self,
        command: str,
        payload: dict[str, Any] | None = None,
        *,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        # Send one correlated command while one permanent reader owns stdout for the session.
        with self._request_lock:
            if self._closed:
                raise EngineProcessError("engine process session is already closed")
            if self._broken_error is not None:
                raise EngineProcessError(f"engine process session is unusable: {self._broken_error}")
            if self._process is None:
                self.start()
            process = self._process
            if process is None or process.stdin is None:
                raise EngineProcessError("engine process is not available")
            if process.poll() is not None:
                message = f"engine exited with code {process.returncode} before {command} request"
                self._mark_broken(message, process)
                raise EngineProcessError(message)
            request_started = time.monotonic()
            request_id = self._next_request_id()
            frame = {"request_id": request_id, "command": command, "request": payload or {}}
            try:
                process.stdin.write(
                    (json.dumps(frame, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
                )
                process.stdin.flush()
            except (BrokenPipeError, OSError) as exc:
                message = f"engine transport failed during {command}: {exc}"
                self._mark_broken(message, process)
                raise EngineProcessError(message) from exc
            started = time.monotonic()
            deadline = started + self._request_timeout(command, timeout_seconds)
            last_heartbeat = started
            raw_response: bytes | None = None
            while raw_response is None:
                now = time.monotonic()
                if self.cancel_callback is not None and self.cancel_callback():
                    message = f"engine request {command} cancelled"
                    self._mark_broken(message, process)
                    raise EngineProcessError(message)
                if self.heartbeat_callback is not None and now - last_heartbeat >= max(
                    0.05,
                    self.heartbeat_interval_seconds,
                ):
                    try:
                        self.heartbeat_callback()
                    except Exception as exc:
                        message = f"engine heartbeat failed during {command}: {exc}"
                        self._mark_broken(message, process)
                        raise EngineProcessError(message) from exc
                    last_heartbeat = now
                if now >= deadline:
                    timeout = deadline - started
                    message = f"engine request {command} timed out after {timeout:.2f}s"
                    self._mark_broken(message, process)
                    raise EngineProcessError(message)
                if process.poll() is not None and self._responses.empty():
                    # Give the permanent reader one final scheduling window before declaring a lost response.
                    try:
                        queued_after_exit = self._responses.get(timeout=0.1)
                    except queue.Empty:
                        queued_after_exit = None
                    if queued_after_exit is None:
                        message = self._exit_diagnostics(command, process)
                        self._mark_broken(message, process)
                        raise EngineProcessError(message)
                    response_id, candidate = queued_after_exit
                    if response_id != request_id:
                        received = response_id if response_id is not None else "missing"
                        message = (
                            f"engine protocol desynchronized during {command}: "
                            f"expected request_id {request_id}, received {received}"
                        )
                        self._mark_broken(message, process)
                        raise EngineProcessError(message)
                    raw_response = candidate
                    continue
                wait_seconds = min(0.25, max(0.01, deadline - now))
                try:
                    queued = self._responses.get(timeout=wait_seconds)
                except queue.Empty:
                    continue
                if queued is None:
                    message = self._exit_diagnostics(command, process)
                    self._mark_broken(message, process)
                    raise EngineProcessError(message)
                response_id, candidate = queued
                if response_id != request_id:
                    received = response_id if response_id is not None else "missing"
                    message = (
                        f"engine protocol desynchronized during {command}: "
                        f"expected request_id {request_id}, received {received}"
                    )
                    self._mark_broken(message, process)
                    raise EngineProcessError(message)
                raw_response = candidate
            self._append_artifact(self.protocol_path, raw_response)
            try:
                response = json.loads(raw_response.decode("utf-8", errors="strict"))
            except (json.JSONDecodeError, UnicodeError) as exc:
                message = f"invalid {command} protocol response: {exc}"
                self._mark_broken(message, process)
                raise EngineProcessError(message) from exc
            if not isinstance(response, dict):
                message = f"{command} protocol response must be an object"
                self._mark_broken(message, process)
                raise EngineProcessError(message)
            if str(response.get("request_id")) != request_id:
                message = f"{command} response request_id does not match request"
                self._mark_broken(message, process)
                raise EngineProcessError(message)
            if not bool(response.get("ok", False)):
                raise EngineProcessError(str(response.get("error", f"engine {command} failed")))
            result = response.get("result", {})
            if not isinstance(result, dict):
                message = f"{command} response result must be an object"
                self._mark_broken(message, process)
                raise EngineProcessError(message)
            phase = "engine_request_" + str(command).strip().lower().replace("-", "_")
            self._record_performance(phase, time.monotonic() - request_started)
            self._publish_performance(status="running")
            return result
    def _wait_or_terminate(self, process: subprocess.Popen[bytes]) -> None:
        # Wait for graceful exit, then escalate to process-tree termination and direct kill.
        try:
            process.wait(timeout=5)
            return
        except subprocess.TimeoutExpired:
            pass
        self._mark_broken("engine process did not exit after shutdown", process)
        try:
            process.wait(timeout=3)
            return
        except subprocess.TimeoutExpired:
            pass
        try:
            process.kill()
            process.wait(timeout=3)
        except (OSError, subprocess.TimeoutExpired) as exc:
            self._reader_error = f"engine process remained alive after close: {exc}"
    def close(self) -> int | None:
        # Request graceful shutdown, terminate when needed, close pipes, and join both permanent readers.
        with self._request_lock:
            if self._closed:
                return self._process.returncode if self._process is not None else None
            process = self._process
            if process is None:
                self._closed = True
                return None
            if process.poll() is None and self._broken_error is None and process.stdin is not None:
                try:
                    self.request("shutdown", timeout_seconds=5.0)
                except EngineProcessError:
                    pass
            if process.poll() is None:
                self._wait_or_terminate(process)
            teardown_started = time.monotonic()
            if process.stdin is not None and not process.stdin.closed:
                try:
                    process.stdin.close()
                except OSError as exc:
                    self._reader_error = f"engine stdin close failed: {exc}"
            for thread in self._reader_threads:
                thread.join(timeout=3)
            alive_threads = [thread.name for thread in self._reader_threads if thread.is_alive()]
            if alive_threads:
                # Closing read streams wakes any platform-specific buffered reader still blocked after child exit.
                stream_errors = self._close_streams(process)
                if stream_errors:
                    self._reader_error = f"engine stream close failed: {'; '.join(stream_errors)}"
                for thread in self._reader_threads:
                    thread.join(timeout=2)
                alive_threads = [thread.name for thread in self._reader_threads if thread.is_alive()]
            final_stream_errors = self._close_streams(process)
            if final_stream_errors and self._reader_error is None:
                self._reader_error = f"engine stream close failed: {'; '.join(final_stream_errors)}"
            if alive_threads:
                self._reader_error = f"engine reader threads did not exit: {', '.join(alive_threads)}"
            process_exited = process.poll() is not None
            if process_exited:
                with self._active_pids_lock:
                    self._active_pids.discard(int(process.pid))
            self._closed = True
            self._record_performance("engine_session_teardown", time.monotonic() - teardown_started)
            self._publish_performance(status="completed" if process_exited and not alive_threads else "error")
            self._process = None
            self._reader_threads = []
            if not process_exited:
                raise EngineProcessError(self._reader_error or "engine process remained alive after close")
            if alive_threads:
                raise EngineProcessError(self._reader_error)
            return process.returncode
