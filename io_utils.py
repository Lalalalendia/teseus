"""Filesystem and hashing helpers with conservative write semantics."""
from __future__ import annotations
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from .models import PerformanceMetrics
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()
def sha256_text(value: str) -> str:
    return sha256_bytes(value.encode("utf-8"))
def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
def stable_hash(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha256_text(payload)
def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path
def _fsync_directory(path: Path) -> None:
    # Persist the directory entry after a critical POSIX atomic replacement.
    if os.name == "nt":
        return
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)
def atomic_write_bytes(
    path: Path,
    data: bytes,
    *,
    mode: int | None = None,
    durability: str = "critical",
    category: str = "other",
    metrics: PerformanceMetrics | None = None,
) -> int:
    # Replace one file atomically while recording its durability and write category.
    """Replace a file through a sibling temporary file and an atomic rename."""
    ensure_dir(path.parent)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            if durability == "critical":
                os.fsync(handle.fileno())
        if mode is not None:
            try:
                os.chmod(temp_path, mode)
            except OSError:
                pass
        os.replace(temp_path, path)
        if durability == "critical":
            _fsync_directory(path.parent)
    finally:
        if temp_path.exists():
            temp_path.unlink()
    if metrics is not None:
        metrics.record_write(len(data), durability=durability, category=category)
    return len(data)
def atomic_write_text(
    path: Path,
    text: str,
    *,
    durability: str = "critical",
    category: str = "other",
    metrics: PerformanceMetrics | None = None,
) -> int:
    # Encode text once and delegate to the shared atomic byte writer.
    return atomic_write_bytes(
        path,
        text.encode("utf-8"),
        durability=durability,
        category=category,
        metrics=metrics,
    )
def atomic_write_json(
    path: Path,
    value: Any,
    *,
    durability: str = "critical",
    category: str = "other",
    metrics: PerformanceMetrics | None = None,
) -> int:
    # Stream JSON into the atomic temporary file so large prepared snapshots do not require
    # a second in-memory copy of the complete serialized document.
    ensure_dir(path.parent)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp_path = Path(temp_name)
    total = 0
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            if durability == "critical":
                os.fsync(handle.fileno())
        total = temp_path.stat().st_size
        os.replace(temp_path, path)
        if durability == "critical":
            _fsync_directory(path.parent)
    finally:
        if temp_path.exists():
            temp_path.unlink()
    if metrics is not None:
        metrics.record_write(total, durability=durability, category=category)
    return total
def append_json_line(
    path: Path,
    value: Any,
    *,
    durability: str = "normal",
    category: str = "other",
    metrics: PerformanceMetrics | None = None,
) -> int:
    # Append one bounded journal record without rewriting earlier campaign results.
    """Append one journal record without rewriting earlier campaign results."""
    payload = (json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
    ensure_dir(path.parent)
    with path.open("ab") as handle:
        handle.write(payload)
        handle.flush()
        if durability == "critical":
            os.fsync(handle.fileno())
    if metrics is not None:
        metrics.record_write(len(payload), durability=durability, category=category)
    return len(payload)
def copy_file_atomic(
    source: Path,
    target: Path,
    *,
    durability: str = "normal",
    category: str = "other",
    metrics: PerformanceMetrics | None = None,
) -> int:
    # Copy a large artifact atomically and classify the resulting destination write.
    """Copy a potentially large artifact without materializing it in memory."""
    ensure_dir(target.parent)
    fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    temp_path = Path(temp_name)
    total = 0
    try:
        with source.open("rb") as source_handle, os.fdopen(fd, "wb") as target_handle:
            while True:
                chunk = source_handle.read(1024 * 1024)
                if not chunk:
                    break
                target_handle.write(chunk)
                total += len(chunk)
            target_handle.flush()
            if durability == "critical":
                os.fsync(target_handle.fileno())
        os.replace(temp_path, target)
        if durability == "critical":
            _fsync_directory(target.parent)
    finally:
        if temp_path.exists():
            temp_path.unlink()
    if metrics is not None:
        metrics.record_write(total, durability=durability, category=category)
    return total
def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))
def iter_json_lines(path: Path) -> Iterator[dict[str, Any]]:
    # Stream valid object records from a JSONL journal without materializing the file.
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                value = json.loads(line)
            except (json.JSONDecodeError, TypeError):
                continue
            if isinstance(value, dict):
                yield value
def rel_path(path: Path, root: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()
def truncate_output(output: str, limit: int = 32_000) -> str:
    if len(output) <= limit:
        return output
    return output[:limit] + f"\n...[truncated {len(output) - limit} characters]"
class FileLock:
    """Exclusive lock file used to serialize mutation runs per checkout target."""
    def __init__(self, path: Path, payload: dict[str, Any] | None = None) -> None:
        self.path = path
        self.payload = payload or {}
        self._held = False
    def __enter__(self) -> "FileLock":
        ensure_dir(self.path.parent)
        data = json.dumps({"pid": os.getpid(), "created_at": utc_now_iso(), **self.payload})
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise RuntimeError(
                f"mutation lock already exists: {self.path}; use recover only after verifying no runner is active"
            ) from exc
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(data)
        self._held = True
        return self
    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self._held:
            try:
                self.path.unlink()
            except FileNotFoundError:
                pass
            self._held = False
def iter_lines(path: Path) -> Iterator[str]:
    with path.open("r", encoding="utf-8-sig") as handle:
        for line in handle:
            value = line.strip()
            if value and not value.startswith("#"):
                yield value
def build_mutant_spool_frame(
    *,
    assignment: dict[str, Any],
    worker: dict[str, Any],
    source_sha256: str,
    mutant_result: dict[str, Any],
    source_event_id: str | None = None,
) -> dict[str, Any]:
    # Build one immutable per-mutant evidence frame without depending on worker runtime modules.
    campaign_id = str(assignment.get("campaign_id", ""))
    shard_id = str(assignment.get("shard_id", ""))
    lease_id = str(assignment.get("lease_id", ""))
    try:
        attempt = int(assignment.get("attempt", 0))
    except (TypeError, ValueError) as exc:
        raise ValueError("per-mutant spool attempt must be an integer") from exc
    if attempt < 0:
        raise ValueError("per-mutant spool attempt must not be negative")
    worker_id = str(worker.get("worker_id", ""))
    worker_instance_id = str(worker.get("instance_id", ""))
    try:
        worker_process_id = int(worker.get("process_id", 0))
    except (TypeError, ValueError) as exc:
        raise ValueError("per-mutant spool worker process_id must be an integer") from exc
    worker_process_birth_token = str(worker.get("process_birth_token", ""))
    mutant_id = str(mutant_result.get("mutant_id", ""))
    execution_id = str(mutant_result.get("execution_id", ""))
    if not all(
        (
            campaign_id,
            shard_id,
            lease_id,
            worker_id,
            worker_instance_id,
            worker_process_id > 0,
            worker_process_birth_token,
            source_sha256,
            mutant_id,
            execution_id,
        )
    ):
        raise ValueError("per-mutant spool identity fields must be non-empty")
    if not bool(mutant_result.get("restore_verified", False)):
        raise ValueError(f"cannot commit un-restored mutant result: {mutant_id}")
    try:
        result_attempt = int(mutant_result.get("attempt", -1))
    except (TypeError, ValueError) as exc:
        raise ValueError("per-mutant result attempt must be an integer") from exc
    result_lease_id = str(mutant_result.get("lease_id", ""))
    if result_attempt != attempt or result_lease_id != lease_id:
        raise ValueError(
            "per-mutant result ownership conflicts with assignment: "
            f"assignment_lease={lease_id!r}; result_lease={result_lease_id!r}; "
            f"assignment_attempt={attempt}; result_attempt={result_attempt}"
        )
    payload = {
        "assignment": {
            "campaign_id": campaign_id,
            "shard_id": shard_id,
            "lease_id": lease_id,
            "attempt": attempt,
        },
        "worker": dict(worker),
        "source_sha256": str(source_sha256),
        "mutant_result": dict(mutant_result),
        "source_event_id": str(source_event_id) if source_event_id else None,
    }
    event_id = stable_hash(
        {
            "schema_version": 2,
            "campaign_id": campaign_id,
            "shard_id": shard_id,
            "lease_id": lease_id,
            "attempt": attempt,
            "worker_instance_id": worker_instance_id,
            "mutant_id": mutant_id,
            "execution_id": execution_id,
        }
    )[:32]
    return {
        "kind": "mutant_event",
        "schema_version": 2,
        "event_id": event_id,
        "campaign_id": campaign_id,
        "shard_id": shard_id,
        "lease_id": lease_id,
        "attempt": attempt,
        "worker_id": worker_id,
        "worker_instance_id": worker_instance_id,
        "mutant_id": mutant_id,
        "execution_id": execution_id,
        "source_sha256": str(source_sha256),
        "source_event_id": str(source_event_id) if source_event_id else None,
        "payload_sha256": stable_hash(payload),
        "payload": payload,
    }
def _repair_jsonl_tail(path: Path) -> None:
    # Normalize only the final incomplete JSONL record before another durable append.
    ensure_dir(path.parent)
    path.touch(exist_ok=True)
    with path.open("r+b") as handle:
        handle.seek(0, os.SEEK_END)
        size = handle.tell()
        if size == 0:
            return
        handle.seek(size - 1)
        if handle.read(1) in {b"\n", b"\r"}:
            return
        cursor = size
        last_break = -1
        while cursor > 0 and last_break < 0:
            chunk_size = min(64 * 1024, cursor)
            cursor -= chunk_size
            handle.seek(cursor)
            chunk = handle.read(chunk_size)
            last_break = max(chunk.rfind(b"\n"), chunk.rfind(b"\r"))
            if last_break >= 0:
                last_break += cursor
        tail_start = last_break + 1
        handle.seek(tail_start)
        tail = handle.read(size - tail_start)
        try:
            value = json.loads(tail.decode("utf-8"))
        except (UnicodeDecodeError, TypeError, ValueError):
            handle.truncate(tail_start)
        else:
            if not isinstance(value, dict):
                raise RuntimeError("final per-mutant worker event frame is not an object")
            handle.seek(0, os.SEEK_END)
            handle.write(b"\n")
        handle.flush()
        os.fsync(handle.fileno())
def _last_jsonl_object(path: Path) -> dict[str, Any] | None:
    # Read only the final complete JSONL object for crash-window deduplication.
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            end = handle.tell()
            if end == 0:
                return None
            cursor = end
            payload = b""
            while cursor > 0:
                chunk_size = min(64 * 1024, cursor)
                cursor -= chunk_size
                handle.seek(cursor)
                chunk = handle.read(chunk_size)
                payload = chunk + payload
                lines = payload.splitlines()
                if len(lines) >= 2 or cursor == 0:
                    candidate = lines[-1] if lines else b""
                    if not candidate and len(lines) >= 2:
                        candidate = lines[-2]
                    if not candidate:
                        return None
                    value = json.loads(candidate.decode("utf-8"))
                    return dict(value) if isinstance(value, dict) else None
    except (OSError, UnicodeDecodeError, TypeError, ValueError):
        return None
    return None
def _write_exclusive_durable(path: Path, payload: bytes) -> bool:
    # Create one immutable sidecar exactly once and fsync its directory entry.
    ensure_dir(path.parent)
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return False
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        _fsync_directory(path.parent)
    except BaseException:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        raise
    return True
def append_mutant_spool_frame(path: Path, frame: dict[str, Any]) -> str:
    # Commit one immutable event in O(1) normal time and repair only a torn journal tail.
    event_id = str(frame.get("event_id", ""))
    if not event_id:
        raise ValueError("mutant spool event_id is required")
    canonical = (json.dumps(frame, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    entries_root = path.parent / f"{path.name}.entries"
    markers_root = path.parent / f"{path.name}.journaled"
    entry_path = entries_root / f"{event_id}.json"
    marker_path = markers_root / f"{event_id}.done"
    created = _write_exclusive_durable(entry_path, canonical)
    if not created:
        try:
            existing = entry_path.read_bytes()
        except OSError as exc:
            raise RuntimeError(f"cannot read immutable per-mutant event: {exc}") from exc
        if existing != canonical:
            raise RuntimeError(f"per-mutant worker event conflict: {event_id}")
    if marker_path.is_file():
        return event_id
    _repair_jsonl_tail(path)
    previous = _last_jsonl_object(path)
    if previous != frame:
        append_json_line(
            path,
            frame,
            durability="critical",
            category="recovery",
        )
    _write_exclusive_durable(marker_path, (event_id + "\n").encode("ascii"))
    return event_id
