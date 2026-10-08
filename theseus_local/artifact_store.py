"""Durable content-addressed artifacts and verified project snapshots."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from threading import RLock
from typing import Any, Iterable, Iterator, Mapping

from theseus_contracts.serialization import dumps
from theseus_local.locking import InterProcessFileLock


_CAS_INDEX_LOCK = RLock()
_InterProcessLock = InterProcessFileLock
ARTIFACT_INDEX_SCHEMA_VERSION = 1


class ArtifactIntegrityError(RuntimeError):
    """Raised when an artifact is missing, corrupt, or published under the wrong identity."""


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _safe_relative(value: str) -> str:
    normalized = str(value).replace("\\", "/").strip()
    path = PurePosixPath(normalized)
    if not normalized or path.is_absolute() or ".." in path.parts:
        raise ArtifactIntegrityError(f"unsafe artifact-relative path: {value!r}")
    return path.as_posix()


@dataclass(frozen=True, slots=True)
class ArtifactRecord:
    artifact_id: str
    size_bytes: int
    created_at: float
    pinned: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_id": self.artifact_id,
            "size_bytes": int(self.size_bytes),
            "created_at": float(self.created_at),
            "pinned": int(self.pinned),
        }


@dataclass(frozen=True, slots=True)
class ProjectSnapshot:
    snapshot_id: str
    files: Mapping[str, str]
    total_size_bytes: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "files": {str(key): str(value) for key, value in sorted(self.files.items())},
            "total_size_bytes": int(self.total_size_bytes),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ProjectSnapshot":
        files = value.get("files")
        if not isinstance(files, Mapping):
            raise ArtifactIntegrityError("snapshot files must be an object")
        normalized = {_safe_relative(str(key)): str(item) for key, item in files.items()}
        snapshot = cls(
            snapshot_id=str(value.get("snapshot_id", "")),
            files=normalized,
            total_size_bytes=int(value.get("total_size_bytes", 0)),
        )
        expected = _sha256_bytes(
            dumps(
                {
                    "files": dict(sorted(normalized.items())),
                    "total_size_bytes": int(snapshot.total_size_bytes),
                }
            ).encode("utf-8")
        )
        if snapshot.snapshot_id != expected:
            raise ArtifactIntegrityError("project snapshot identity does not match its file map")
        return snapshot


@dataclass(frozen=True, slots=True)
class ArtifactTransferStats:
    artifact_ids: tuple[str, ...]
    bytes_transferred: int
    cache_hits: int
    cache_misses: int

    @property
    def cache_hit_rate(self) -> float:
        total = self.cache_hits + self.cache_misses
        return float(self.cache_hits / total) if total else 1.0


class ContentAddressedArtifactStore:
    """A small durable CAS with atomic publication and pin-aware eviction."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._index_path = self.root / "index.json"
        self._lock_path = self.root / ".index.lock"
        self._lock = RLock()
        self._records: dict[str, ArtifactRecord] = {}
        with _CAS_INDEX_LOCK, _InterProcessLock(self._lock_path):
            self._load_index()

    def _artifact_path(self, artifact_id: str) -> Path:
        if len(artifact_id) != 64 or any(char not in "0123456789abcdef" for char in artifact_id):
            raise ArtifactIntegrityError(f"invalid artifact id: {artifact_id!r}")
        return self.root / artifact_id

    def _load_index(self) -> None:
        self._records = {}
        if not self._index_path.is_file():
            return
        try:
            raw = json.loads(self._index_path.read_text(encoding="utf-8"))
            if not isinstance(raw, Mapping):
                raise ValueError("index is not an object")
            if raw.get("schema_version") != ARTIFACT_INDEX_SCHEMA_VERSION:
                raise ValueError(
                    f"unsupported artifact index schema version: {raw.get('schema_version')!r}"
                )
            artifacts = raw.get("artifacts", {})
            if not isinstance(artifacts, Mapping):
                raise ValueError("artifact index artifacts section must be an object")
            for key, value in artifacts.items():
                if not isinstance(value, Mapping):
                    continue
                record = ArtifactRecord(
                    artifact_id=str(key),
                    size_bytes=int(value.get("size_bytes", 0)),
                    created_at=float(value.get("created_at", 0.0)),
                    pinned=max(0, int(value.get("pinned", 0))),
                )
                if self._artifact_path(record.artifact_id).is_file():
                    self._records[record.artifact_id] = record
        except (OSError, ValueError, TypeError, ArtifactIntegrityError) as exc:
            raise ArtifactIntegrityError(f"artifact index is corrupt: {self._index_path}") from exc

    def _save_index_locked(self) -> None:
        """Persist the already refreshed index while the CAS lock is held."""

        payload = {
            "schema_version": ARTIFACT_INDEX_SCHEMA_VERSION,
            "artifacts": {
                key: value.to_dict() for key, value in sorted(self._records.items())
            },
        }
        temporary = self._index_path.with_name(f".{self._index_path.name}.{uuid.uuid4().hex}.tmp")
        temporary.write_text(dumps(payload) + "\n", encoding="utf-8", newline="\n")
        os.replace(temporary, self._index_path)

    def _save_index(self) -> None:
        """Persist the index for compatibility with non-transactional callers."""

        with _CAS_INDEX_LOCK, _InterProcessLock(self._lock_path):
            self._load_index()
            self._save_index_locked()

    @contextmanager
    def _index_transaction(self) -> Iterator[None]:
        """Refresh, mutate and publish the CAS index as one cross-process transaction."""

        with _CAS_INDEX_LOCK, _InterProcessLock(self._lock_path):
            self._load_index()
            yield
            self._save_index_locked()

    def _verified_record_locked(self, artifact_id: str) -> ArtifactRecord:
        path = self._artifact_path(str(artifact_id))
        if not path.is_file():
            raise ArtifactIntegrityError(f"artifact is unavailable: {artifact_id}")
        actual, size = _sha256_file(path)
        if actual != str(artifact_id):
            raise ArtifactIntegrityError(f"artifact hash mismatch: {artifact_id}")
        existing = self._records.get(str(artifact_id))
        if existing is None:
            return self._record(str(artifact_id), size)
        if existing.size_bytes != size:
            raise ArtifactIntegrityError(f"artifact size mismatch: {artifact_id}")
        return existing

    def _record(self, artifact_id: str, size: int) -> ArtifactRecord:
        return self._records.setdefault(
            artifact_id,
            ArtifactRecord(artifact_id, int(size), time.time(), 0),
        )

    def put_bytes(self, value: bytes, *, expected_id: str | None = None, pin: bool = False) -> ArtifactRecord:
        """Publish bytes atomically; a partial transfer never becomes a valid CAS entry."""

        payload = bytes(value)
        artifact_id = _sha256_bytes(payload)
        if expected_id is not None and str(expected_id) != artifact_id:
            raise ArtifactIntegrityError("artifact bytes do not match expected content identity")
        with self._lock, self._index_transaction():
            destination = self._artifact_path(artifact_id)
            if destination.is_file():
                actual, size = _sha256_file(destination)
                if actual != artifact_id:
                    raise ArtifactIntegrityError(f"corrupt existing artifact: {artifact_id}")
            else:
                temporary = self.root / f".{artifact_id}.{uuid.uuid4().hex}.part"
                try:
                    temporary.write_bytes(payload)
                    os.replace(temporary, destination)
                finally:
                    temporary.unlink(missing_ok=True)
                size = len(payload)
            record = self._record(artifact_id, size)
            if pin:
                record = ArtifactRecord(record.artifact_id, record.size_bytes, record.created_at, record.pinned + 1)
                self._records[artifact_id] = record
            return record

    def put_file(self, path: Path, *, expected_id: str | None = None, pin: bool = False) -> ArtifactRecord:
        """Stream a file into an atomic CAS publication without loading it twice in memory."""

        source = Path(path)
        if not source.is_file():
            raise FileNotFoundError(source)
        digest = hashlib.sha256()
        size = 0
        temporary = self.root / f".upload-{uuid.uuid4().hex}.part"
        try:
            with source.open("rb") as reader, temporary.open("xb") as writer:
                for chunk in iter(lambda: reader.read(1024 * 1024), b""):
                    digest.update(chunk)
                    size += len(chunk)
                    writer.write(chunk)
                writer.flush()
                os.fsync(writer.fileno())
            artifact_id = digest.hexdigest()
            if expected_id is not None and str(expected_id) != artifact_id:
                raise ArtifactIntegrityError("file bytes do not match expected content identity")
            with self._lock, self._index_transaction():
                destination = self._artifact_path(artifact_id)
                if destination.is_file():
                    actual, _ = _sha256_file(destination)
                    if actual != artifact_id:
                        raise ArtifactIntegrityError(f"corrupt existing artifact: {artifact_id}")
                    temporary.unlink(missing_ok=True)
                else:
                    os.replace(temporary, destination)
                record = self._record(artifact_id, size)
                if pin:
                    record = ArtifactRecord(record.artifact_id, record.size_bytes, record.created_at, record.pinned + 1)
                    self._records[artifact_id] = record
                return record
        finally:
            temporary.unlink(missing_ok=True)

    def path_for(self, artifact_id: str, *, verify: bool = True) -> Path:
        with self._lock:
            path = self._artifact_path(str(artifact_id))
            if not path.is_file():
                raise ArtifactIntegrityError(f"artifact is unavailable: {artifact_id}")
            if verify:
                actual, _ = _sha256_file(path)
                if actual != str(artifact_id):
                    raise ArtifactIntegrityError(f"artifact hash mismatch: {artifact_id}")
            return path

    def get_bytes(self, artifact_id: str) -> bytes:
        return self.path_for(artifact_id).read_bytes()

    def has(self, artifact_id: str, *, verify: bool = True) -> bool:
        try:
            self.path_for(artifact_id, verify=verify)
        except ArtifactIntegrityError:
            return False
        return True
    def has_many(self, artifact_ids: Iterable[str], *, verify: bool = True) -> tuple[str, ...]:
        """Check a deduplicated artifact batch under one store lock."""
        ordered = tuple(dict.fromkeys(str(item) for item in artifact_ids))
        present: list[str] = []
        with self._lock:
            for artifact_id in ordered:
                try:
                    path = self._artifact_path(artifact_id)
                    if not path.is_file():
                        continue
                    if verify:
                        actual, _ = _sha256_file(path)
                        if actual != artifact_id:
                            continue
                    present.append(artifact_id)
                except ArtifactIntegrityError:
                    continue
        return tuple(present)
    def missing_many(self, artifact_ids: Iterable[str]) -> tuple[str, ...]:
        """Return missing or corrupt IDs after one verified CAS/index pass."""
        ordered = tuple(dict.fromkeys(str(item) for item in artifact_ids))
        missing: list[str] = []
        with self._lock, self._index_transaction():
            for artifact_id in ordered:
                try:
                    path = self._artifact_path(artifact_id)
                except ArtifactIntegrityError:
                    missing.append(artifact_id)
                    continue
                if not path.is_file():
                    self._records.pop(artifact_id, None)
                    missing.append(artifact_id)
                    continue
                actual, _ = _sha256_file(path)
                if actual == artifact_id:
                    continue
                path.unlink(missing_ok=True)
                self._records.pop(artifact_id, None)
                missing.append(artifact_id)
        return tuple(missing)


    def transfer_from(
        self,
        source: "ContentAddressedArtifactStore",
        artifact_ids: Iterable[str],
        *,
        pin: bool = False,
    ) -> ArtifactTransferStats:
        """Copy only missing verified bytes; an existing corrupt entry is treated as a miss."""

        ordered = tuple(dict.fromkeys(str(item) for item in artifact_ids))
        transferred = 0
        hits = 0
        misses = 0
        for artifact_id in ordered:
            if self.has(artifact_id, verify=True):
                hits += 1
                if pin:
                    self.pin(artifact_id)
                continue
            # A corrupt local entry is a cache miss, never a reason to make a
            # valid source artifact permanently unavailable. Remove only this
            # exact content-addressed path before refetching it.
            with self._lock:
                local_path = self._artifact_path(artifact_id)
                if local_path.is_file():
                    with self._index_transaction():
                        local_path.unlink()
                        self._records.pop(artifact_id, None)
            payload = source.get_bytes(artifact_id)
            record = self.put_bytes(payload, expected_id=artifact_id, pin=pin)
            transferred += record.size_bytes
            misses += 1
        return ArtifactTransferStats(ordered, transferred, hits, misses)

    def transfer_snapshot(
        self,
        source: "ContentAddressedArtifactStore",
        snapshot_id: str,
        *,
        prepared_artifact_ids: Iterable[str] = (),
        pin: bool = False,
    ) -> ArtifactTransferStats:
        """Transfer a baseline manifest, only missing file blobs, and prepared artifacts."""

        snapshot = source.load_snapshot(snapshot_id)
        manifest_ids = (str(snapshot_id), *tuple(snapshot.files.values()), *tuple(prepared_artifact_ids))
        # The source manifest itself is content-addressed by snapshot_id.
        return self.transfer_from(source, manifest_ids, pin=pin)

    def verify(self, artifact_id: str) -> ArtifactRecord:
        with self._lock, self._index_transaction():
            return self._verified_record_locked(artifact_id)

    def pin(self, artifact_id: str) -> ArtifactRecord:
        with self._lock, self._index_transaction():
            record = self._verified_record_locked(artifact_id)
            updated = ArtifactRecord(record.artifact_id, record.size_bytes, record.created_at, record.pinned + 1)
            self._records[record.artifact_id] = updated
            return updated
    def pin_many(self, artifact_ids: Iterable[str]) -> tuple[ArtifactRecord, ...]:
        """Pin a verified artifact batch with one index transaction."""
        ordered = tuple(dict.fromkeys(str(item) for item in artifact_ids))
        with self._lock, self._index_transaction():
            records: list[ArtifactRecord] = []
            for artifact_id in ordered:
                record = self._verified_record_locked(artifact_id)
                updated = ArtifactRecord(
                    record.artifact_id,
                    record.size_bytes,
                    record.created_at,
                    record.pinned + 1,
                )
                self._records[artifact_id] = updated
                records.append(updated)
            return tuple(records)



    def release(self, artifact_id: str) -> ArtifactRecord:
        with self._lock, self._index_transaction():
            record = self._verified_record_locked(artifact_id)
            updated = ArtifactRecord(record.artifact_id, record.size_bytes, record.created_at, max(0, record.pinned - 1))
            self._records[record.artifact_id] = updated
            return updated

    def release_many(self, artifact_ids: Iterable[str]) -> tuple[ArtifactRecord, ...]:
        """Release a verified artifact batch with one index transaction."""
        ordered = tuple(dict.fromkeys(str(item) for item in artifact_ids))
        with self._lock, self._index_transaction():
            records: list[ArtifactRecord] = []
            for artifact_id in ordered:
                record = self._verified_record_locked(artifact_id)
                updated = ArtifactRecord(
                    record.artifact_id,
                    record.size_bytes,
                    record.created_at,
                    max(0, record.pinned - 1),
                )
                self._records[artifact_id] = updated
                records.append(updated)
    def discard_corrupt(self, artifact_id: str) -> bool:
        """Remove exactly one invalid CAS path so a verified refetch can replace it."""

        identifier = str(artifact_id)
        with self._lock, self._index_transaction():
            path = self._artifact_path(identifier)
            if not path.is_file():
                self._records.pop(identifier, None)
                return False
            actual, _ = _sha256_file(path)
            if actual == identifier:
                return False
            path.unlink(missing_ok=True)
            self._records.pop(identifier, None)
            return True

    def evict(self, *, max_bytes: int | None = None, older_than_seconds: float | None = None) -> tuple[str, ...]:
        """Remove only unpinned artifacts and never the durable index itself."""

        with self._lock, self._index_transaction():
            now = time.time()
            candidates = [
                record
                for record in self._records.values()
                if record.pinned == 0
                and (
                    older_than_seconds is None
                    or now - record.created_at >= max(0.0, float(older_than_seconds))
                )
            ]
            candidates.sort(key=lambda item: (item.created_at, item.artifact_id))
            current = sum(record.size_bytes for record in self._records.values())
            removed: list[str] = []
            for record in candidates:
                if max_bytes is not None and current <= int(max_bytes):
                    break
                self._artifact_path(record.artifact_id).unlink(missing_ok=True)
                self._records.pop(record.artifact_id, None)
                current -= record.size_bytes
                removed.append(record.artifact_id)
            return tuple(removed)

    def create_snapshot(self, root: Path, *, exclude: Iterable[str] = ()) -> ProjectSnapshot:
        """Store a verified baseline snapshot whose identity is independent of its filesystem path."""

        project_root = Path(root).resolve()
        excluded = {_safe_relative(item) for item in exclude}
        files: dict[str, str] = {}
        total = 0
        for path in sorted(project_root.rglob("*"), key=lambda item: item.as_posix()):
            if not path.is_file():
                continue
            relative = path.relative_to(project_root).as_posix()
            if relative in excluded:
                continue
            record = self.put_file(path)
            files[relative] = record.artifact_id
            total += record.size_bytes
        snapshot_id = _sha256_bytes(
            dumps(
                {
                    "files": dict(sorted(files.items())),
                    "total_size_bytes": int(total),
                }
            ).encode("utf-8")
        )
        return ProjectSnapshot(snapshot_id, dict(sorted(files.items())), total)

    def persist_snapshot(self, snapshot: ProjectSnapshot, *, pin: bool = False) -> ArtifactRecord:
        """Publish a snapshot manifest as a normal immutable artifact."""

        checked = ProjectSnapshot.from_dict(snapshot.to_dict())
        payload = dumps(
            {
                "files": dict(sorted(checked.files.items())),
                "total_size_bytes": int(checked.total_size_bytes),
            }
        ).encode("utf-8")
        return self.put_bytes(payload, expected_id=checked.snapshot_id, pin=pin)

    def load_snapshot(self, manifest_artifact_id: str) -> ProjectSnapshot:
        """Load and verify a snapshot manifest before materialization."""

        try:
            payload = json.loads(self.get_bytes(manifest_artifact_id).decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ArtifactIntegrityError("snapshot manifest is unavailable or invalid") from exc
        if not isinstance(payload, Mapping):
            raise ArtifactIntegrityError("snapshot manifest must be an object")
        payload = dict(payload)
        payload["snapshot_id"] = str(manifest_artifact_id)
        snapshot = ProjectSnapshot.from_dict(payload)
        for artifact_id in snapshot.files.values():
            self.verify(artifact_id)
        return snapshot

    def materialize_snapshot(self, snapshot: ProjectSnapshot, destination: Path) -> None:
        """Materialize and re-verify every snapshot member before it is executable."""

        checked = ProjectSnapshot.from_dict(snapshot.to_dict())
        target_root = Path(destination).resolve()
        target_root.mkdir(parents=True, exist_ok=True)
        for relative, artifact_id in sorted(checked.files.items()):
            target = (target_root / relative).resolve()
            if not target.is_relative_to(target_root):
                raise ArtifactIntegrityError("snapshot materialization escaped destination")
            target.parent.mkdir(parents=True, exist_ok=True)
            source = self.path_for(artifact_id)
            temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
            try:
                shutil.copyfile(source, temporary)
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)
            actual, _ = _sha256_file(target)
            if actual != artifact_id:
                raise ArtifactIntegrityError(f"materialized snapshot artifact mismatch: {relative}")

    def records(self) -> tuple[ArtifactRecord, ...]:
        with self._lock, _CAS_INDEX_LOCK, _InterProcessLock(self._lock_path):
            self._load_index()
            return tuple(self._records[key] for key in sorted(self._records))


__all__ = [
    "ArtifactIntegrityError",
    "ArtifactRecord",
    "ArtifactTransferStats",
    "ContentAddressedArtifactStore",
    "ProjectSnapshot",
]
