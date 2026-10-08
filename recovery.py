"""Replay and reconciliation primitives for durable Theseus campaign artifacts."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .commands import env_fingerprint
from .io_utils import read_json, stable_hash
from .models import CampaignAccumulator


class JournalReplayError(ValueError):
    """Raised when a durable journal contains a corrupt non-tail record."""


@dataclass(frozen=True)
class JournalReplay:
    """Bounded diagnostics and deduplicated rows recovered from one JSONL journal."""

    rows: tuple[dict[str, Any], ...]
    duplicate_events: int = 0
    ignored_tail: bool = False
    invalid_rows: int = 0
    last_valid_offset: int = 0
    total_records: int = 0

    def to_dict(self) -> dict[str, Any]:
        # Serialize replay diagnostics without exposing tuple internals.
        return {
            "rows": len(self.rows),
            "duplicate_events": self.duplicate_events,
            "ignored_tail": self.ignored_tail,
            "invalid_rows": self.invalid_rows,
            "last_valid_offset": self.last_valid_offset,
            "total_records": self.total_records,
        }


@dataclass(frozen=True)
class CampaignReplay:
    """Checkpoint verification plus the accumulator reconstructed from a journal."""

    rows: tuple[dict[str, Any], ...]
    accumulator: CampaignAccumulator
    checkpoint: dict[str, Any]
    journal: JournalReplay


def _default_row_key(row: dict[str, Any]) -> str:
    # Prefer an explicit event identity and fall back to a canonical row digest.
    event_id = row.get("event_id")
    if event_id:
        return f"event:{event_id}"
    return f"row:{stable_hash(row)}"


def _result_row_key(row: dict[str, Any]) -> str:
    # Deduplicate one execution attempt while allowing later attempts for one mutant.
    execution_id = row.get("execution_id")
    if execution_id:
        return f"execution:{execution_id}"
    event_id = row.get("event_id")
    if event_id:
        return f"event:{event_id}"
    mutant = row.get("mutant")
    if isinstance(mutant, dict) and mutant.get("mutant_id"):
        return f"mutant:{mutant['mutant_id']}"
    return _default_row_key(row)


def _row_identity_keys(row: dict[str, Any], primary_key: str) -> tuple[str, ...]:
    # Track event and execution identities independently so a reused ID cannot hide corruption.
    keys = [primary_key]
    event_id = row.get("event_id")
    execution_id = row.get("execution_id")
    if event_id:
        keys.append(f"event:{event_id}")
    if execution_id:
        keys.append(f"execution:{execution_id}")
    return tuple(dict.fromkeys(keys))


def result_identity_digests(rows: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> dict[str, str]:
    # Build the checkpoint identity map used to authenticate suffix duplicates.
    identities: dict[str, str] = {}
    for position, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise JournalReplayError(f"result row {position} is not an object")
        primary = _result_row_key(row)
        digest = stable_hash(row)
        for identity in _row_identity_keys(row, primary):
            previous = identities.get(identity)
            if previous is not None and previous != digest:
                raise JournalReplayError(f"conflicting result identity: {identity}")
            identities[identity] = digest
    return identities


def repository_tree_fingerprint(root: Path) -> str:
    # Hash behavior-affecting project files while excluding generated campaign data.
    root = root.resolve()
    ignored_directories = {
        ".git",
        ".hg",
        ".svn",
        ".theseus",
        ".venv",
        "venv",
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        "build",
        "dist",
        "reports",
        "test_stats_workers",
        "node_modules",
    }
    included_suffixes = {
        ".cfg",
        ".ini",
        ".json",
        ".lock",
        ".py",
        ".pyi",
        ".toml",
        ".yaml",
        ".yml",
    }
    included_names = {
        "requirements.txt",
        "requirements-dev.txt",
        "Pipfile",
    }
    rows: list[tuple[str, str]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or any(part in ignored_directories for part in path.relative_to(root).parts):
            continue
        if path.suffix.lower() not in included_suffixes and path.name not in included_names:
            continue
        relative = path.relative_to(root).as_posix()
        try:
            content_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        except (OSError, UnicodeError):
            content_hash = "unreadable"
        rows.append((relative, content_hash))
    return stable_hash(rows)


def dependency_manifest_fingerprint(root: Path) -> str:
    # Keep dependency declarations in the resume boundary even when their suffix is not source-like.
    names = (
        "requirements.txt",
        "requirements.lock",
        "requirements-dev.txt",
        "requirements-dev.lock",
        "poetry.lock",
        "Pipfile",
        "Pipfile.lock",
        "uv.lock",
    )
    rows: list[tuple[str, str]] = []
    for name in names:
        path = root / name
        if not path.is_file():
            continue
        try:
            rows.append((name, hashlib.sha256(path.read_bytes()).hexdigest()))
        except (OSError, UnicodeError):
            rows.append((name, "unreadable"))
    return stable_hash(rows)


def runtime_fingerprint() -> str:
    # Capture Python, pytest and plugin identities that affect test execution.
    try:
        pytest_version = importlib.metadata.version("pytest")
    except importlib.metadata.PackageNotFoundError:
        pytest_version = None
    try:
        plugins = sorted(
            (
                str(item.name),
                str(getattr(getattr(item, "dist", None), "name", "")),
                str(getattr(getattr(item, "dist", None), "version", "")),
            )
            for item in importlib.metadata.entry_points(group="pytest11")
        )
    except (AttributeError, TypeError, importlib.metadata.PackageNotFoundError):
        plugins = []
    return stable_hash(
        {
            "python": platform.python_version(),
            "implementation": platform.python_implementation(),
            "platform": sys.platform,
            "pytest": pytest_version,
            "plugins": plugins,
        }
    )


def campaign_input_fingerprint(
    root: Path,
    source_path: str,
    source_sha256: str,
    selection: Mapping[str, Any] | None,
    configuration: Mapping[str, Any] | None,
) -> str:
    # Bind resume to source, repository tree, selection, configuration and runtime inputs.
    return stable_hash(
        {
            "source_path": str(source_path).replace("\\", "/").lstrip("./"),
            "source_sha256": str(source_sha256),
            "repository_tree": repository_tree_fingerprint(root),
            "dependency_manifests": dependency_manifest_fingerprint(root),
            "selection": dict(selection or {}),
            "configuration": dict(configuration or {}),
            "environment": env_fingerprint(root, include_cwd=False),
            "runtime": runtime_fingerprint(),
        }
    )


def _validate_result_record(value: dict[str, Any], path: Path, line_number: int) -> None:
    # Validate the versioned result envelope before it can affect a campaign accumulator.
    event_type = value.get("event_type")
    if event_type is None:
        return
    if event_type != "mutant_completed":
        raise JournalReplayError(f"unsupported result event type at {path}:{line_number}")
    schema = value.get("event_schema_version")
    if isinstance(schema, bool) or not isinstance(schema, int) or schema not in {1, 2}:
        raise JournalReplayError(f"unsupported result event schema at {path}:{line_number}")
    event_id = value.get("event_id")
    if not isinstance(event_id, str) or not event_id.strip():
        raise JournalReplayError(f"result event_id is required at {path}:{line_number}")
    if schema >= 2:
        execution_id = value.get("execution_id")
        if not isinstance(execution_id, str) or not execution_id.strip():
            raise JournalReplayError(f"result execution_id is required at {path}:{line_number}")
        attempt = value.get("attempt")
        if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 0:
            raise JournalReplayError(f"result attempt is invalid at {path}:{line_number}")
        mutant = value.get("mutant")
        if not isinstance(mutant, dict) or not str(mutant.get("mutant_id", "")).strip():
            raise JournalReplayError(f"result mutant identity is required at {path}:{line_number}")
        if "status" not in value or not str(value.get("status", "")).strip():
            raise JournalReplayError(f"result status is required at {path}:{line_number}")
        if "lease_id" not in value or "worker_id" not in value:
            raise JournalReplayError(f"result lease/worker identity is required at {path}:{line_number}")


def replay_jsonl(
    path: Path,
    *,
    key: Callable[[dict[str, Any]], str] | None = None,
    max_bytes: int | None = None,
    start_offset: int = 0,
    initial_identities: Mapping[str, str] | None = None,
) -> JournalReplay:
    # Stream one append-only journal from a checkpoint boundary, accepting only a malformed final tail.
    if not path.exists():
        return JournalReplay(rows=())
    if start_offset < 0:
        raise JournalReplayError("journal replay start boundary cannot be negative")
    if max_bytes is not None and max_bytes < start_offset:
        raise JournalReplayError("journal replay boundary cannot be negative")
    rows: list[dict[str, Any]] = []
    seen: dict[str, str] = {}
    if initial_identities is not None:
        if not isinstance(initial_identities, Mapping):
            raise JournalReplayError("initial journal identities must be an object")
        for identity, digest in initial_identities.items():
            if not isinstance(identity, str) or not identity.strip() or not isinstance(digest, str) or not digest.strip():
                raise JournalReplayError("initial journal identities must contain non-empty strings")
            seen[identity] = digest
    duplicate_events = 0
    ignored_tail = False
    invalid_rows = 0
    last_valid_offset = start_offset
    total_records = 0
    record_key = key or _default_row_key
    with path.open("rb") as handle:
        if start_offset:
            if start_offset > path.stat().st_size:
                raise JournalReplayError("journal replay start is beyond the journal")
            handle.seek(start_offset)
            if start_offset and start_offset != path.stat().st_size:
                handle.seek(start_offset - 1)
                if handle.read(1) != b"\n":
                    raise JournalReplayError("journal replay start does not align to a record boundary")
                handle.seek(start_offset)
        if max_bytes == 0:
            return JournalReplay(rows=())
        for line_number, raw_line in enumerate(handle, start=1):
            offset = handle.tell()
            if max_bytes is not None and offset > max_bytes:
                raise JournalReplayError(
                    f"checkpoint boundary {max_bytes} splits journal record at line {line_number}"
                )
            complete_line = raw_line.endswith(b"\n")
            payload = raw_line.rstrip(b"\r\n")
            if not payload:
                if complete_line:
                    last_valid_offset = offset
                    continue
                ignored_tail = True
                invalid_rows += 1
                break
            try:
                value = json.loads(payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
                invalid_rows += 1
                if not complete_line:
                    ignored_tail = True
                    break
                raise JournalReplayError(f"invalid JSONL record at {path}:{line_number}: {exc}") from exc
            if not isinstance(value, dict):
                invalid_rows += 1
                if not complete_line:
                    ignored_tail = True
                    break
                raise JournalReplayError(f"JSONL record at {path}:{line_number} is not an object")
            _validate_result_record(value, path, line_number)
            total_records += 1
            current_key = str(record_key(value))
            digest = stable_hash(value)
            identity_keys = _row_identity_keys(value, current_key)
            prior = next((seen[item] for item in identity_keys if item in seen), None)
            if prior is not None and prior != digest:
                raise JournalReplayError(
                    f"conflicting journal record identity at {path}:{line_number}"
                )
            if prior is not None:
                duplicate_events += 1
            else:
                for identity_key in identity_keys:
                    seen[identity_key] = digest
                rows.append(value)
            last_valid_offset = offset
            if max_bytes is not None and offset == max_bytes:
                break
    return JournalReplay(
        rows=tuple(rows),
        duplicate_events=duplicate_events,
        ignored_tail=ignored_tail,
        invalid_rows=invalid_rows,
        last_valid_offset=last_valid_offset,
        total_records=total_records,
    )


def replay_result_journal(
    path: Path,
    *,
    max_bytes: int | None = None,
    start_offset: int = 0,
    initial_identities: Mapping[str, str] | None = None,
) -> JournalReplay:
    # Replay mutation completion events with one durable row per execution attempt.
    return replay_jsonl(
        path,
        key=_result_row_key,
        max_bytes=max_bytes,
        start_offset=start_offset,
        initial_identities=initial_identities,
    )


def merge_authoritative_results(rows: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> list[dict[str, Any]]:
    # Validate execution identities and select one deterministic authoritative attempt per mutant.
    identities: dict[str, str] = {}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for position, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise JournalReplayError(f"result row {position} is not an object")
        _validate_result_record(row, Path("<merged-results>"), position)
        primary = _result_row_key(row)
        digest = stable_hash(row)
        keys = _row_identity_keys(row, primary)
        for identity in keys:
            previous = identities.get(identity)
            if previous is not None and previous != digest:
                raise JournalReplayError(f"conflicting result identity: {identity}")
        for identity in keys:
            identities[identity] = digest
        mutant = row.get("mutant")
        mutant_id = str(mutant.get("mutant_id", "")) if isinstance(mutant, dict) else ""
        group_key = mutant_id or f"__row__:{primary}"
        grouped.setdefault(group_key, []).append(row)

    def authority_key(row: dict[str, Any]) -> tuple[int, int, int, str]:
        # Prefer the newest retry, then a domain result over an infrastructure failure.
        try:
            attempt = int(row.get("attempt", 0))
        except (TypeError, ValueError):
            attempt = 0
        status = str(row.get("status", "error"))
        quality = 0 if status in {"error", "infrastructure_error", "timeout", "cancelled"} else 1
        sequence = row.get("sequence", 0)
        try:
            sequence_value = int(sequence)
        except (TypeError, ValueError):
            sequence_value = 0
        return attempt, quality, sequence_value, str(row.get("event_id", ""))

    return [max(group, key=authority_key) for key, group in sorted(grouped.items())]


def sha256_prefix(path: Path, size: int | None = None) -> str:
    # Hash exactly the checkpointed journal prefix without loading it into memory.
    digest = hashlib.sha256()
    remaining = None if size is None else int(size)
    with path.open("rb") as handle:
        while remaining is None or remaining > 0:
            chunk_size = 1024 * 1024 if remaining is None else min(1024 * 1024, remaining)
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
            if remaining is not None:
                remaining -= len(chunk)
    if remaining not in (None, 0):
        raise JournalReplayError(f"journal is shorter than requested prefix: {path}")
    return digest.hexdigest()


def journal_metadata(path: Path | None) -> dict[str, Any]:
    # Capture a checkpoint digest and byte boundary only when state is materialized.
    if path is None or not path.exists():
        return {"journal_size": 0, "journal_sha256": hashlib.sha256(b"").hexdigest()}
    size = path.stat().st_size
    return {"journal_size": size, "journal_sha256": sha256_prefix(path, size)}


def verify_checkpoint(state_path: Path) -> dict[str, Any]:
    # Verify the immutable journal prefix referenced by one sparse checkpoint.
    try:
        state = read_json(state_path)
    except (OSError, ValueError) as exc:
        raise JournalReplayError(f"cannot read checkpoint {state_path}: {exc}") from exc
    if not isinstance(state, dict):
        raise JournalReplayError(f"checkpoint is not an object: {state_path}")
    try:
        checkpoint_version = int(state.get("checkpoint_schema_version", 1))
    except (TypeError, ValueError) as exc:
        raise JournalReplayError("checkpoint_schema_version is invalid") from exc
    if checkpoint_version not in {1, 2}:
        raise JournalReplayError(f"unsupported checkpoint schema: {checkpoint_version}")
    raw_identities = state.get("completed_identity_digests")
    if raw_identities is not None:
        if not isinstance(raw_identities, Mapping):
            raise JournalReplayError("checkpoint completed_identity_digests must be an object")
        for identity, digest in raw_identities.items():
            if (
                not isinstance(identity, str)
                or not identity.strip()
                or not isinstance(digest, str)
                or not digest.strip()
            ):
                raise JournalReplayError("checkpoint identity digests must contain non-empty strings")
    journal_value = state.get("results_journal")
    recorded_size = state.get("journal_size")
    recorded_sha = state.get("journal_sha256")
    if journal_value is None or recorded_size is None or recorded_sha is None:
        return {
            "state": state,
            "journal_path": None,
            "verified": False,
            "legacy": True,
        }
    try:
        size = int(recorded_size)
    except (TypeError, ValueError) as exc:
        raise JournalReplayError(f"checkpoint journal_size is invalid: {recorded_size!r}") from exc
    if size < 0:
        raise JournalReplayError("checkpoint journal_size cannot be negative")
    if checkpoint_version >= 2:
        try:
            journal_offset = int(state.get("journal_offset", size))
        except (TypeError, ValueError) as exc:
            raise JournalReplayError("checkpoint journal_offset is invalid") from exc
        if journal_offset != size or journal_offset < 0:
            raise JournalReplayError("checkpoint journal_offset must equal journal_size")
    journal_path = Path(str(journal_value))
    if not journal_path.is_absolute():
        journal_path = state_path.parent / journal_path
    journal_path = journal_path.resolve()
    if not journal_path.exists():
        if size == 0:
            digest = hashlib.sha256(b"").hexdigest()
        else:
            raise JournalReplayError(f"checkpoint journal is missing: {journal_path}")
    else:
        actual_size = journal_path.stat().st_size
        if actual_size < size:
            raise JournalReplayError(
                f"journal shrank below checkpoint boundary: {actual_size} < {size} bytes"
            )
        digest = sha256_prefix(journal_path, size)
    if digest != str(recorded_sha):
        raise JournalReplayError(
            f"checkpoint journal SHA mismatch: expected {recorded_sha}, got {digest}"
        )
    return {
        "state": state,
        "journal_path": journal_path,
        "journal_size": size,
        "journal_sha256": str(recorded_sha),
        "verified": True,
        "legacy": False,
    }


def accumulator_from_results(rows: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> CampaignAccumulator:
    # Rebuild O(1) campaign counters from the deduplicated durable result rows.
    accumulator = CampaignAccumulator()
    for row in rows:
        accumulator.add_result(row)
    return accumulator


def replay_campaign(state_path: Path, journal_path: Path | None = None) -> CampaignReplay:
    # Verify the checkpoint and rebuild counters from its accumulator plus the durable suffix.
    checkpoint = verify_checkpoint(state_path)
    resolved_journal = journal_path.resolve() if journal_path else checkpoint.get("journal_path")
    if resolved_journal is None:
        value = checkpoint.get("state", {}).get("results_journal")
        if value:
            candidate = Path(str(value))
            resolved_journal = (candidate if candidate.is_absolute() else state_path.parent / candidate).resolve()
    state = checkpoint.get("state", {})
    accumulator_value = state.get("accumulator") if isinstance(state, dict) else None
    if isinstance(accumulator_value, dict) and resolved_journal is not None:
        offset = int(state.get("journal_offset", checkpoint.get("journal_size", 0)) or 0)
        # The checkpoint already authenticates the prefix, so replay only the appended suffix.
        raw_identities = state.get("completed_identity_digests", {})
        identities = raw_identities if isinstance(raw_identities, Mapping) else None
        journal = replay_result_journal(
            resolved_journal,
            start_offset=offset,
            initial_identities=identities,
        )
        accumulator = CampaignAccumulator.from_dict(accumulator_value)
        for row in journal.rows:
            accumulator.add_result(row)
    else:
        journal = replay_result_journal(resolved_journal) if resolved_journal else JournalReplay(rows=())
        accumulator = accumulator_from_results(journal.rows)
    return CampaignReplay(
        rows=journal.rows,
        accumulator=accumulator,
        checkpoint=checkpoint,
        journal=journal,
    )


def result_event_id(run_id: str, result: dict[str, Any]) -> str:
    # Derive a stable event identity from execution ownership, never mutable result content.
    mutant = result.get("mutant") if isinstance(result.get("mutant"), dict) else {}
    identity = {
        "run_id": run_id,
        "execution_id": result.get("execution_id"),
        "attempt": result.get("attempt", 0),
        "lease_id": result.get("lease_id"),
        "worker_id": result.get("worker_id"),
        "mutant_id": mutant.get("mutant_id"),
    }
    return stable_hash(identity)[:32]


def _windows_process_identity(pid: int) -> tuple[bool, str | None]:
    # Query Windows process liveness and creation time without sending a console control signal.
    import ctypes
    from ctypes import wintypes

    if int(pid) <= 0:
        return False, None

    process_query_limited_information = 0x1000
    synchronize = 0x00100000
    wait_object_0 = 0x00000000
    wait_timeout = 0x00000102
    error_access_denied = 5
    error_invalid_parameter = 87

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = (
        wintypes.DWORD,
        wintypes.BOOL,
        wintypes.DWORD,
    )
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.WaitForSingleObject.argtypes = (
        wintypes.HANDLE,
        wintypes.DWORD,
    )
    kernel32.WaitForSingleObject.restype = wintypes.DWORD
    kernel32.GetProcessTimes.argtypes = (
        wintypes.HANDLE,
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
    )
    kernel32.GetProcessTimes.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL

    handle = kernel32.OpenProcess(
        process_query_limited_information | synchronize,
        False,
        int(pid),
    )
    if not handle:
        error = ctypes.get_last_error()
        if error == error_invalid_parameter:
            return False, None
        if error == error_access_denied:
            return True, None
        return False, None

    try:
        wait_result = kernel32.WaitForSingleObject(handle, 0)
        if wait_result == wait_object_0:
            return False, None
        if wait_result != wait_timeout:
            return True, None

        creation_time = wintypes.FILETIME()
        exit_time = wintypes.FILETIME()
        kernel_time = wintypes.FILETIME()
        user_time = wintypes.FILETIME()
        if not kernel32.GetProcessTimes(
            handle,
            ctypes.byref(creation_time),
            ctypes.byref(exit_time),
            ctypes.byref(kernel_time),
            ctypes.byref(user_time),
        ):
            return True, None

        creation_ticks = (
            int(creation_time.dwHighDateTime) << 32
        ) | int(creation_time.dwLowDateTime)
        return True, f"win:{creation_ticks:016x}"
    finally:
        kernel32.CloseHandle(handle)


def _process_birth_token(pid: int) -> str | None:
    # Return a stable operating-system process creation token when the platform exposes one.
    if int(pid) <= 0:
        return None
    if os.name == "nt":
        alive, token = _windows_process_identity(int(pid))
        return token if alive else None
    stat_path = Path(f"/proc/{pid}/stat")
    try:
        raw = stat_path.read_text(encoding="utf-8")
        suffix = raw.rsplit(")", 1)[-1].split()
        return suffix[19] if len(suffix) > 19 else None
    except (OSError, ValueError):
        return None


def current_process_birth_token(pid: int | None = None) -> str | None:
    # Expose the current owner start token without making platform process APIs part of the domain model.
    return _process_birth_token(os.getpid() if pid is None else int(pid))


def owner_is_alive(manifest: dict[str, Any]) -> bool:
    # Prove that the recorded campaign owner is still the same live process before takeover.
    raw_pid = manifest.get("coordinator_pid") or manifest.get("pid")
    try:
        pid = int(raw_pid)
    except (TypeError, ValueError):
        return False
    if pid <= 0:
        return False

    recorded_token = manifest.get("process_birth_token")
    if os.name == "nt":
        alive, current_token = _windows_process_identity(pid)
        if not alive:
            return False
        if recorded_token is None:
            return True
        return current_token is not None and str(current_token) == str(recorded_token)

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    if recorded_token is None:
        return True
    current_token = _process_birth_token(pid)
    return current_token is not None and str(current_token) == str(recorded_token)


def inspect_campaign_recovery(reports_dir: Path) -> dict[str, Any]:
    # Inspect active manifests without mutating or deleting any recovery artifact.
    root = reports_dir.resolve()
    active: list[dict[str, Any]] = []
    orphaned: list[dict[str, Any]] = []
    corrupt: list[dict[str, Any]] = []
    unfinished_shards: list[dict[str, Any]] = []
    candidates = sorted(root.rglob("*.manifest.json")) if root.exists() else []
    for path in candidates:
        try:
            value = read_json(path)
        except (OSError, ValueError) as exc:
            corrupt.append({"manifest": str(path), "error": str(exc)})
            continue
        if not isinstance(value, dict):
            corrupt.append({"manifest": str(path), "error": "manifest is not an object"})
            continue
        status = str(value.get("status", "unknown"))
        if status not in {"active", "running", "starting", "restored", "materializing", "ready_to_commit"}:
            continue
        item = {
            "manifest": str(path),
            "run_id": value.get("run_id"),
            "status": status,
            "active_mutant": value.get("active_mutant"),
            "lock_path": value.get("lock_path"),
        }
        active.append(item)
        pid = value.get("coordinator_pid") or value.get("pid")
        alive = owner_is_alive(value) if pid is not None else False
        if pid is not None and not alive:
            orphaned.append({**item, "reason": "coordinator_owner_not_alive"})
        for worker in value.get("workers", []):
            if not isinstance(worker, dict):
                continue
            worker_status = str(worker.get("status", "unknown"))
            if worker_status not in {"complete", "error", "cancelled", "baseline_failed", "restore_error"}:
                unfinished_shards.append(
                    {"manifest": str(path), "worker_id": worker.get("worker_id"), "status": worker_status}
                )
    return {
        "reports_dir": str(root),
        "active_campaigns": active,
        "orphaned_campaigns": orphaned,
        "unfinished_shards": unfinished_shards,
        "corrupt_manifests": corrupt,
    }