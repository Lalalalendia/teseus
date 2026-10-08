"""Safe subprocess execution and command fingerprinting.

The default path is always ``shell=False``. PowerShell/cmd wrappers and shell
metacharacters are rejected unless a caller deliberately implements a legacy
adapter outside this module.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict
from fnmatch import fnmatchcase
from pathlib import Path
from threading import Lock
from typing import BinaryIO, Callable, Mapping, Sequence

from .io_utils import stable_hash
from .models import PerformanceMetrics, ProcessResult, TerminationResult


class CommandError(ValueError):
    """Raised when a command cannot be represented as a safe argv list."""


_VOLATILE_ENV_KEYS = frozenset({"_", "OLDPWD", "PWD", "SHLVL", "LS_COLORS", "PROMPT_COMMAND"})

_INFRASTRUCTURE_MARKERS: dict[str, tuple[bytes, ...]] = {
    "pytest_internal_error": (b"internal error", b"internalerror", b"pytest_internal_error"),
    "conftest_import_error": (b"importerror while loading conftest", b"error loading conftest"),
    "collection_error": (b"error while collecting", b"error collecting", b"collection error"),
    "worker_crash": (b"workerprocesscrashed", b"worker crashed", b"worker process crashed", b"node down"),
    "python_fatal_error": (b"fatal python error", b"python fatal error"),
    "out_of_memory": (b"out of memory", b"cannot allocate memory", b"memoryerror", b"oom killer"),
    "access_violation": (b"access violation", b"segmentation fault", b"segmentationfault", b"sigsegv"),
    "process_terminated": (b"process terminated", b"terminated by signal", b"killed by signal"),
    "import_provenance_error": (b"import_provenance_error",),
}

_ACTIVE_CHILD_PIDS: set[int] = set()
_ACTIVE_CHILD_PIDS_LOCK = Lock()


def _create_windows_kill_job(process: subprocess.Popen[object]) -> int | None:
    """Assign a child to a kill-on-close job so detached descendants cannot outlive it."""

    if os.name != "nt":
        return None
    try:
        import ctypes

        class BasicLimitInformation(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", ctypes.c_uint32),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", ctypes.c_uint32),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", ctypes.c_uint32),
                ("SchedulingClass", ctypes.c_uint32),
            ]

        class IoCounters(ctypes.Structure):
            _fields_ = [("values", ctypes.c_ulonglong * 6)]

        class ExtendedLimitInformation(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", BasicLimitInformation),
                ("IoInfo", IoCounters),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.restype = ctypes.c_void_p
        kernel32.SetInformationJobObject.argtypes = [
            ctypes.c_void_p,
            ctypes.c_int,
            ctypes.c_void_p,
            ctypes.c_uint32,
        ]
        kernel32.AssignProcessToJobObject.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        job = kernel32.CreateJobObjectW(None, None)
        if not job:
            return None
        info = ExtendedLimitInformation()
        info.BasicLimitInformation.LimitFlags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if not kernel32.SetInformationJobObject(
            job,
            9,  # JobObjectExtendedLimitInformation
            ctypes.byref(info),
            ctypes.sizeof(info),
        ):
            kernel32.CloseHandle(job)
            return None
        if not kernel32.AssignProcessToJobObject(job, ctypes.c_void_p(int(process._handle))):
            kernel32.CloseHandle(job)
            return None
        return int(job)
    except (AttributeError, OSError, TypeError, ValueError):
        return None


def _close_windows_kill_job(job: int | None) -> None:
    if job is None or os.name != "nt":
        return
    try:
        import ctypes

        ctypes.WinDLL("kernel32", use_last_error=True).CloseHandle(ctypes.c_void_p(int(job)))
    except (AttributeError, OSError, TypeError, ValueError):
        return


def active_child_process_ids() -> frozenset[int]:
    # Expose only locally tracked command children for deterministic leak assertions and diagnostics.
    with _ACTIVE_CHILD_PIDS_LOCK:
        return frozenset(_ACTIVE_CHILD_PIDS)


def _register_child_process(pid: int) -> None:
    # Register a command child before waiting so every cleanup boundary can be audited.
    with _ACTIVE_CHILD_PIDS_LOCK:
        _ACTIVE_CHILD_PIDS.add(int(pid))


def _unregister_child_process(pid: int) -> None:
    # Remove a command child even when termination or artifact scanning raises.
    with _ACTIVE_CHILD_PIDS_LOCK:
        _ACTIVE_CHILD_PIDS.discard(int(pid))


def normalize_argv(argv: Sequence[str]) -> tuple[str, ...]:
    values = tuple(str(item) for item in argv if str(item))
    if values and values[0] == "&":
        values = values[1:]
    if not values:
        raise CommandError("command argv cannot be empty")
    lowered = {Path(values[0]).name.lower(), values[0].lower()}
    if lowered & {"powershell", "powershell.exe", "pwsh", "pwsh.exe", "cmd", "cmd.exe"}:
        raise CommandError("PowerShell/cmd wrappers require an explicit legacy runner")
    if any(value.lower() in {"-command", "/c", "/k"} for value in values[1:]):
        raise CommandError("shell wrapper arguments are not allowed in safe argv mode")
    return values


def parse_argv_json(value: str) -> tuple[str, ...]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        # Windows PowerShell may strip embedded JSON quotes while marshalling
        # an argument to a native process, turning ["python", "-m"] into
        # [python, -m]. Keep the JSON contract, but accept that unambiguous
        # comma-list fallback; the `--` delimiter remains the robust option
        # for arguments containing commas.
        stripped = value.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            body = stripped[1:-1].strip()
            parsed = [item.strip().strip("\"'") for item in body.split(",")] if body else []
        else:
            raise CommandError("--*-command-argv must be a JSON array, e.g. '[\"python\", \"-m\", \"pytest\"]'") from exc
    if not isinstance(parsed, list) or not all(isinstance(item, (str, int, float)) for item in parsed):
        raise CommandError("command argv JSON must be an array of scalar values")
    return normalize_argv([str(item) for item in parsed])


def parse_command_text(value: str) -> tuple[str, ...]:
    if any(operator in value for operator in ("&&", "||", "|", ";", ">", "<", "`", "$(")):
        raise CommandError("shell syntax is not allowed; pass a JSON argv array instead")
    try:
        tokens = shlex.split(value, posix=False)
    except ValueError as exc:
        raise CommandError(f"cannot parse command: {exc}") from exc
    cleaned = [token[1:-1] if len(token) >= 2 and token[0] == token[-1] and token[0] in "\"'" else token for token in tokens]
    return normalize_argv(cleaned)


def resolve_python(project_root: Path, explicit: str | None = None) -> str:
    if explicit:
        return explicit
    candidates = (
        project_root / ".venv" / "Scripts" / "python.exe",
        project_root / ".venv" / "bin" / "python",
        project_root / "venv" / "Scripts" / "python.exe",
        project_root / "venv" / "bin" / "python",
    )
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return sys.executable


def env_fingerprint(
    cwd: Path,
    env: Mapping[str, str] | None = None,
    *,
    include_cwd: bool = True,
    declared_keys: Sequence[str] = (),
    declared_patterns: Sequence[str] = (),
    tracked_variables: Sequence[str] = (),
    tracked_prefixes: Sequence[str] = (),
    ignored_variables: Sequence[str] = (),
    secret_variables: Sequence[str] = (),
    inherit_policy: str = "allowlisted",
    secret_key: str = "theseus-environment-v1",
) -> str:
    # Fingerprint only the environment values admitted by the explicit inheritance contract.
    values = dict(os.environ if env is None else env)
    tracked_keys = {
        str(item)
        for item in (*declared_keys, *tracked_variables)
        if str(item)
    }
    patterns = tuple(
        str(item)
        for item in (*declared_patterns, *tracked_prefixes)
        if str(item)
    )
    ignored = {
        str(item)
        for item in ignored_variables
        if str(item)
    }
    secrets = {
        str(item)
        for item in secret_variables
        if str(item)
    }
    mode = str(inherit_policy or "allowlisted").strip().lower()

    if mode not in {"allowlisted", "strict", "track_all_except"}:
        raise ValueError(f"unsupported environment inherit policy: {inherit_policy}")

    if mode == "track_all_except":
        relevant_keys = {
            key
            for key in values
            if key not in _VOLATILE_ENV_KEYS and key not in ignored
        }
    else:
        explicit_keys = tracked_keys | secrets
        relevant_keys = {
            key
            for key in values
            if key not in ignored
            and (
                key in explicit_keys
                or any(fnmatchcase(key, pattern) for pattern in patterns)
            )
        }

    relevant = {
        key: values[key]
        for key in relevant_keys
    }

    encoded: list[tuple[str, str, str]] = []
    hmac_key = hashlib.sha256(str(secret_key).encode("utf-8")).digest()

    for key, value in sorted(relevant.items()):
        if key in secrets:
            import hmac

            encoded.append(
                (
                    key,
                    "secret",
                    hmac.new(
                        hmac_key,
                        value.encode("utf-8"),
                        hashlib.sha256,
                    ).hexdigest(),
                )
            )
        else:
            encoded.append((key, "value", value))

    payload: dict[str, object] = {
        "env": encoded,
        "declared_keys": sorted(tracked_keys),
        "declared_patterns": sorted(patterns),
        "ignored_variables": sorted(ignored),
        "secret_variables": sorted(secrets),
        "inherit_policy": mode,
    }

    if include_cwd:
        payload["cwd"] = str(cwd.resolve())

    return stable_hash(payload)


def command_fingerprint(argv: Sequence[str], cwd: Path, env: Mapping[str, str] | None = None) -> str:
    return stable_hash({"argv": list(normalize_argv(argv)), "cwd": str(cwd.resolve()), "env": env_fingerprint(cwd, env)})


def terminate_process_tree(process: subprocess.Popen[object]) -> TerminationResult:
    """Kill a timed-out process tree and return bounded cleanup diagnostics."""

    tree_kill_succeeded = False
    parent_kill_succeeded = process.poll() is not None
    return_code: int | None = None
    errors: list[str] = []
    if not parent_kill_succeeded and os.name == "nt":
        try:
            result = subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                shell=False,
                timeout=5,
            )
            return_code = result.returncode
            tree_kill_succeeded = result.returncode == 0
            if not tree_kill_succeeded:
                errors.append(f"taskkill returned {result.returncode}")
        except (OSError, subprocess.SubprocessError) as exc:
            errors.append(str(exc))
    elif not parent_kill_succeeded and os.name != "nt":
        try:
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            tree_kill_succeeded = True
        except (OSError, ProcessLookupError) as exc:
            errors.append(str(exc))

    if process.poll() is None:
        try:
            process.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            pass
    if process.poll() is None:
        try:
            process.kill()
            parent_kill_succeeded = True
        except OSError as exc:
            errors.append(str(exc))
    try:
        process.wait(timeout=1.5)
    except subprocess.TimeoutExpired:
        errors.append("process did not exit after kill")
    parent_kill_succeeded = process.poll() is not None
    return TerminationResult(
        requested=True,
        tree_kill_succeeded=tree_kill_succeeded,
        parent_kill_succeeded=parent_kill_succeeded,
        return_code=return_code,
        error="; ".join(errors) if errors else None,
    )


def _scan_output_summary(
    stream: BinaryIO,
    output_limit: int,
) -> tuple[int, str, str, str, str, tuple[str, ...], tuple[str, ...]]:
    # Scan the full artifact once for previews, SHA, infrastructure flags and bounded excerpts.
    limit = max(1, output_limit)
    stream.flush()
    head_limit = max(1, limit // 2)
    tail_limit = max(1, limit - head_limit)
    stream.seek(0)
    head_buffer = bytearray()
    tail_buffer = bytearray()
    preview_buffer = bytearray()
    overlap_size = max(len(marker) for markers in _INFRASTRUCTURE_MARKERS.values() for marker in markers) - 1
    overlap = b""
    flags: set[str] = set()
    excerpts: list[str] = []
    excerpt_seen: set[str] = set()
    size = 0
    digest = hashlib.sha256()
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        digest.update(chunk)
        size += len(chunk)
        if len(preview_buffer) < limit:
            preview_buffer.extend(chunk[: limit - len(preview_buffer)])
        if len(head_buffer) < head_limit:
            head_buffer.extend(chunk[: head_limit - len(head_buffer)])
        tail_buffer.extend(chunk)
        if len(tail_buffer) > tail_limit:
            del tail_buffer[: len(tail_buffer) - tail_limit]
        searchable = overlap + chunk
        lowered_searchable = searchable.lower()
        for flag, markers in _INFRASTRUCTURE_MARKERS.items():
            for marker in markers:
                position = lowered_searchable.find(marker)
                if position < 0:
                    continue
                flags.add(flag)
                excerpt_start = max(0, position - 160)
                excerpt_end = min(len(searchable), position + len(marker) + 320)
                excerpt = searchable[excerpt_start:excerpt_end].decode("utf-8", errors="replace").strip()
                if excerpt and excerpt not in excerpt_seen and len(excerpts) < 8:
                    excerpt_seen.add(excerpt)
                    excerpts.append(excerpt[:512])
                break
        overlap = searchable[-overlap_size:] if overlap_size else b""
    if size <= limit:
        head = bytes(preview_buffer[:head_limit])
        tail = bytes(preview_buffer[head_limit:])
    else:
        head = bytes(head_buffer)
        tail = bytes(tail_buffer)
    head_text = head.decode("utf-8", errors="replace")
    tail_text = tail.decode("utf-8", errors="replace")
    if size <= limit:
        preview = head_text + tail_text
    else:
        preview = f"{head_text}\n...[truncated {size - len(head) - len(tail)} bytes]...\n{tail_text}"
    return size, preview, head_text, tail_text, digest.hexdigest(), tuple(sorted(flags)), tuple(excerpts)


def _read_output_summary(stream: BinaryIO, output_limit: int) -> tuple[int, str, str, str, str | None]:
    # Preserve the compact preview-reader contract while using the full scanner internally.
    size, preview, head, tail, digest, _, _ = _scan_output_summary(stream, output_limit)
    return size, preview, head, tail, digest


def run_argv(
    argv: Sequence[str],
    *,
    cwd: Path,
    timeout_seconds: float,
    env: Mapping[str, str] | None = None,
    output_limit: int = 32_000,
    retry: bool = False,
    output_artifact: Path | None = None,
    metrics: PerformanceMetrics | None = None,
    cancellation: Callable[[], bool] | None = None,
    deadline_monotonic: float | None = None,
) -> ProcessResult:
    # Execute one normalized shell-free process while exposing timeout and cancellation facts.
    normalized = normalize_argv(argv)
    started = time.perf_counter()
    creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) if os.name == "nt" else 0
    if cancellation is not None and cancellation():
        # Return a physical cancellation fact without spawning a process after the request boundary.
        return ProcessResult(
            argv=normalized,
            cwd=str(cwd.resolve()),
            exit_code=None,
            elapsed_seconds=0.0,
            timed_out=False,
            output="",
            retry=retry,
            output_artifact=str(output_artifact) if output_artifact is not None else None,
            termination={"requested": False, "reason": "cancelled_before_start"},
        )
    if output_artifact is not None:
        output_artifact.parent.mkdir(parents=True, exist_ok=True)
        stream: BinaryIO = output_artifact.open("w+b")
        artifact_name = str(output_artifact)
        close_stream = True
    else:
        stream = tempfile.TemporaryFile(mode="w+b")
        artifact_name = None
        close_stream = True
    process: subprocess.Popen[object] | None = None
    job_handle: int | None = None
    timed_out = False
    cancelled = False
    process_tree_leak = False
    termination: TerminationResult | None = None
    effective_deadline_monotonic = (
        float(deadline_monotonic)
        if deadline_monotonic is not None
        else time.monotonic() + float(timeout_seconds)
        if cancellation is not None
        else None
    )
    try:
        process = subprocess.Popen(
            list(normalized),
            cwd=str(cwd),
            env=dict(env) if env is not None else None,
            stdin=subprocess.DEVNULL,
            stdout=stream,
            stderr=subprocess.STDOUT,
            shell=False,
            creationflags=creationflags,
            start_new_session=os.name != "nt",
        )
        job_handle = _create_windows_kill_job(process)
        if metrics is not None:
            metrics.processes_started += 1
        _register_child_process(process.pid)
        if cancellation is None and deadline_monotonic is None:
            try:
                process.wait(timeout=timeout_seconds)
            except subprocess.TimeoutExpired:
                timed_out = True
        else:
            while process.poll() is None:
                if cancellation is not None and cancellation():
                    cancelled = True
                    termination = terminate_process_tree(process)
                    break
                remaining = float(timeout_seconds)
                if effective_deadline_monotonic is not None:
                    remaining = min(remaining, effective_deadline_monotonic - time.monotonic())
                if remaining <= 0.0:
                    timed_out = True
                    break
                try:
                    process.wait(timeout=min(remaining, 0.05))
                except subprocess.TimeoutExpired:
                    continue
        if timed_out:
            if metrics is not None:
                metrics.process_timeouts += 1
            termination = terminate_process_tree(process)
        if (timed_out or cancelled) and process.poll() is None:
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process_tree_leak = True
        elapsed = time.perf_counter() - started
        if metrics is not None:
            metrics.subprocess_elapsed_seconds += elapsed
        (
            size,
            output,
            head,
            tail,
            output_sha256,
            infrastructure_flags,
            diagnostic_excerpts,
        ) = _scan_output_summary(stream, output_limit)
        if metrics is not None:
            metrics.subprocess_output_bytes += size
            if process_tree_leak:
                metrics.process_tree_leaks += 1
        return ProcessResult(
            argv=normalized,
            cwd=str(cwd.resolve()),
            exit_code=None if timed_out or cancelled else process.returncode,
            elapsed_seconds=elapsed,
            timed_out=timed_out,
            output=output,
            retry=retry,
            output_artifact=artifact_name,
            output_bytes=size,
            output_sha256=output_sha256,
            output_head=head,
            output_tail=tail,
            process_tree_leak=process_tree_leak,
            termination=(
                {
                    **asdict(termination),
                    **({"reason": "cancelled"} if cancelled else {}),
                }
                if termination is not None
                else None
            ),
            infrastructure_flags=infrastructure_flags,
            diagnostic_excerpts=diagnostic_excerpts,
        )
    finally:
        _close_windows_kill_job(job_handle)
        if process is not None:
            _unregister_child_process(process.pid)
        if close_stream:
            stream.close()


def prepare_pytest_command(argv: Sequence[str], *, mode: str) -> tuple[str, ...]:
    """Add safe fail-fast flags only to commands that actually invoke pytest."""

    values = list(normalize_argv(argv))
    if mode not in {"baseline", "mutant"}:
        return tuple(values)
    is_pytest = any(
        value.lower() in {"pytest", "pytest.exe"}
        or Path(value).name.lower() in {"pytest", "pytest.exe"}
        or (value == "-m" and index + 1 < len(values) and values[index + 1].lower() == "pytest")
        for index, value in enumerate(values)
    )
    if not is_pytest:
        return tuple(values)
    lowered = [value.lower() for value in values]
    has_maxfail = any(value == "-x" or value == "--maxfail" or value.startswith("--maxfail=") for value in lowered)
    has_tb = any(value == "--tb" or value.startswith("--tb=") for value in lowered)
    if not has_maxfail:
        values.append("--maxfail=1")
    if not has_tb:
        values.append("--tb=line")
    return tuple(values)


def classify_pytest_exit(exit_code: int | None) -> str:
    return {
        0: "passed",
        1: "tests_failed",
        2: "interrupted",
        3: "internal_error",
        4: "usage_error",
        5: "no_tests_collected",
    }.get(exit_code, "spawn_error" if exit_code is None else "unknown")


def without_parallelism(argv: Sequence[str]) -> tuple[str, ...]:
    """Return a deterministic timeout-retry command without xdist workers."""

    values = list(normalize_argv(argv))
    result: list[str] = []
    index = 0
    while index < len(values):
        value = values[index]
        lower = value.lower()
        if lower in {"-n", "--numprocesses", "--dist", "--max-worker-restart"}:
            index += 2
            continue
        if lower.startswith("-n=") or lower.startswith("--numprocesses=") or lower.startswith("--dist="):
            index += 1
            continue
        result.append(value)
        index += 1
    return tuple(result)


def looks_like_infrastructure_failure(
    output: str,
    *,
    infrastructure_flags: Sequence[str] = (),
) -> bool:
    # Prefer flags found during the full artifact scan over bounded preview text.
    if infrastructure_flags:
        return True
    lowered = output.lower()
    markers = (
        "internal error",
        "internalerror",
        "workerprocesscrashed",
        "could not spawn",
        "no such file or directory",
        "keyboardinterrupt",
        "importerror while loading conftest",
        "import_provenance_error",
        "error while collecting",
    )
    return any(marker in lowered for marker in markers) or re.search(r"fixture\s+.+\s+not found", lowered) is not None
