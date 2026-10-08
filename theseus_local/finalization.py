"""Content-addressed publication and recovery helpers for campaign finalization."""
from __future__ import annotations

import hashlib
import os
import shutil
import uuid
from collections import Counter
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Mapping, Sequence

from theseus_contracts import (
    ArtifactRegistryEntry,
    CampaignId,
    FinalizationIntent,
    deterministic_id,
)
from theseus_contracts.serialization import utc_now
from test_intelligence_unified_v1.io_utils import atomic_write_bytes, stable_hash


class FinalizationArtifactError(RuntimeError):
    """Raised when staged, logical, content-addressed, and registry evidence disagree."""


@dataclass(frozen=True, slots=True)
class ArtifactSource:
    """Local source used to derive one immutable registry entry before publication."""
    logical_key: str
    logical_role: str
    logical_path: Path
    producer: str
    schema_version: int
    payload: bytes | None = None
    source_path: Path | None = None
    shard_id: str | None = None
    execution_id: str | None = None
    metadata: Mapping[str, Any] | None = None
    required: bool = False
    def __post_init__(self) -> None:
        # Require exactly one byte or file source and a non-empty logical identity.
        if not all((self.logical_key, self.logical_role, self.producer)):
            raise ValueError("artifact source identity fields must be non-empty")
        if (self.payload is None) == (self.source_path is None):
            raise ValueError("artifact source requires exactly one of payload or source_path")
        if int(self.schema_version) < 1:
            raise ValueError("artifact source schema_version must be positive")


def _sha256_file(path: Path) -> tuple[str, int]:
    # Hash one artifact through bounded reads and return its exact persisted byte size.
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _fsync_directory(path: Path) -> None:
    # Persist a newly created content or logical directory entry on POSIX filesystems.
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


def _resolve_under_root(
    root: Path,
    serialized_path: str,
    *,
    field_name: str,
    logical_key: str,
) -> Path:
    # Resolve one portable registry path while rejecting absolute and parent-traversal identities.
    raw = str(serialized_path).strip()
    normalized = raw.replace("\\", "/")
    portable = PurePosixPath(normalized)
    if (
        not raw
        or portable.is_absolute()
        or PureWindowsPath(raw).is_absolute()
        or ".." in portable.parts
    ):
        raise FinalizationArtifactError(
            f"unsafe {field_name} in finalization registry: "
            f"logical_key={logical_key}; value={serialized_path!r}; root={root.resolve()}"
        )
    resolved_root = root.resolve()
    candidate = (resolved_root / Path(*portable.parts)).resolve()
    if candidate != resolved_root and resolved_root not in candidate.parents:
        raise FinalizationArtifactError(
            f"{field_name} escapes reports root: logical_key={logical_key}; "
            f"value={serialized_path!r}; resolved={candidate}; root={resolved_root}"
        )
    return candidate


def _relative_path(root: Path, path: Path, *, field_name: str) -> str:
    # Convert one canonical path to a safe portable relative path under the reports root.
    root = root.resolve()
    path = path.resolve()
    if path != root and root not in path.parents:
        raise FinalizationArtifactError(
            f"{field_name} escapes reports root: root={root}; path={path}"
        )
    return path.relative_to(root).as_posix()


def _validate_file(path: Path, entry: ArtifactRegistryEntry, *, label: str) -> None:
    # Verify one physical file against the immutable size and content identity in SQLite.
    if not path.is_file():
        raise FinalizationArtifactError(
            f"{label} artifact is missing: logical_key={entry.logical_key}; "
            f"expected_path={path}; sha256={entry.content_sha256}; size={entry.size_bytes}"
        )
    actual_sha256, actual_size = _sha256_file(path)
    if actual_sha256 != entry.content_sha256 or actual_size != entry.size_bytes:
        raise FinalizationArtifactError(
            f"{label} artifact conflicts with immutable registry identity: "
            f"logical_key={entry.logical_key}; path={path}; "
            f"expected_sha256={entry.content_sha256}; actual_sha256={actual_sha256}; "
            f"expected_size={entry.size_bytes}; actual_size={actual_size}"
        )


def _copy_to_exclusive_target(source: Path, destination: Path) -> None:
    # Publish a new file without replacing an existing logical or content identity.
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")
    try:
        with source.open("rb") as reader, temporary.open("xb") as writer:
            shutil.copyfileobj(reader, writer, length=1024 * 1024)
            writer.flush()
            os.fsync(writer.fileno())
        try:
            os.link(temporary, destination)
        except FileExistsError:
            return
        except OSError:
            descriptor: int | None = None
            try:
                descriptor = os.open(
                    destination,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o644,
                )
                with os.fdopen(descriptor, "wb") as writer, source.open("rb") as reader:
                    descriptor = None
                    shutil.copyfileobj(reader, writer, length=1024 * 1024)
                    writer.flush()
                    os.fsync(writer.fileno())
            except FileExistsError:
                return
            finally:
                if descriptor is not None:
                    os.close(descriptor)
            _fsync_directory(destination.parent)
        else:
            _fsync_directory(destination.parent)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _publish_entry(
    reports_root: Path,
    entry: ArtifactRegistryEntry,
    source: Path,
    *,
    repair_unregistered: bool,
) -> None:
    # Publish and verify both content-addressed storage and the stable logical alias.
    content_path = _resolve_under_root(
        reports_root,
        entry.content_path,
        field_name="content_path",
        logical_key=entry.logical_key,
    )
    logical_path = _resolve_under_root(
        reports_root,
        entry.logical_path,
        field_name="logical_path",
        logical_key=entry.logical_key,
    )
    for target, label in ((content_path, "content-addressed"), (logical_path, "logical")):
        if target.exists():
            try:
                _validate_file(target, entry, label=label)
                continue
            except FinalizationArtifactError:
                if not repair_unregistered:
                    raise
                try:
                    target.unlink()
                except OSError as exc:
                    raise FinalizationArtifactError(
                        f"cannot repair unregistered partial artifact: path={target}; error={exc}"
                    ) from exc
        _copy_to_exclusive_target(source, target)
        _validate_file(target, entry, label=label)


def build_finalization_intent(
    database_path: Path,
    campaign_id: CampaignId,
    sources: Sequence[ArtifactSource],
    *,
    canonical_report_key: str,
    result_fingerprint: str,
) -> FinalizationIntent:
    # Stage generated bytes and freeze the complete expected artifact set before publication.
    database_path = Path(database_path).resolve()
    reports_root = database_path.parent.parent.resolve()
    report_root = database_path.parent.resolve()
    created_at = utc_now()
    provisional = [
        {
            "logical_key": source.logical_key,
            "logical_role": source.logical_role,
            "logical_path": _relative_path(
                reports_root,
                source.logical_path,
                field_name="logical_path",
            ),
            "producer": source.producer,
            "schema_version": int(source.schema_version),
            "required": bool(source.required),
        }
        for source in sources
    ]
    intent_id = deterministic_id(
        "finalization-intent",
        campaign_id.value,
        result_fingerprint,
        stable_hash(provisional),
    )
    staging_root = report_root / ".finalization" / intent_id
    entries: list[ArtifactRegistryEntry] = []
    logical_keys: set[str] = set()
    for source in sorted(sources, key=lambda item: item.logical_key):
        if source.logical_key in logical_keys:
            raise FinalizationArtifactError(
                f"duplicate finalization logical key: {source.logical_key}"
            )
        logical_keys.add(source.logical_key)
        metadata = dict(source.metadata or {})
        if source.payload is not None:
            staged_path = _resolve_under_root(
                staging_root,
                source.logical_key,
                field_name="logical_key staging path",
                logical_key=source.logical_key,
            )
            atomic_write_bytes(
                staged_path,
                source.payload,
                durability="critical",
                category="finalization_staging",
            )
            source_path = staged_path
            metadata["staging_path"] = _relative_path(
                reports_root,
                staged_path,
                field_name="staging_path",
            )
        else:
            source_path = Path(source.source_path).resolve()
            if not source_path.is_file():
                raise FinalizationArtifactError(
                    f"finalization source artifact is missing: logical_key={source.logical_key}; "
                    f"source={source_path}"
                )
            metadata["source_path"] = _relative_path(
                reports_root,
                source_path,
                field_name="source_path",
            )
        content_sha256, size_bytes = _sha256_file(source_path)
        content_path = (
            reports_root
            / "artifacts"
            / "sha256"
            / content_sha256[:2]
            / content_sha256
        )
        entries.append(
            ArtifactRegistryEntry(
                campaign_id=campaign_id,
                logical_key=source.logical_key,
                logical_role=source.logical_role,
                content_sha256=content_sha256,
                size_bytes=size_bytes,
                schema_version=int(source.schema_version),
                producer=source.producer,
                content_path=_relative_path(
                    reports_root,
                    content_path,
                    field_name="content_path",
                ),
                logical_path=_relative_path(
                    reports_root,
                    source.logical_path,
                    field_name="logical_path",
                ),
                created_at=created_at,
                shard_id=source.shard_id,
                execution_id=source.execution_id,
                metadata={**metadata, "required": bool(source.required)},
            )
        )
    required = tuple(
        sorted(item.logical_key for item in entries if bool(item.metadata.get("required", False)))
    )
    return FinalizationIntent(
        intent_id=intent_id,
        campaign_id=campaign_id,
        status="created",
        required_logical_keys=required,
        canonical_report_key=canonical_report_key,
        artifacts=tuple(entries),
        result_fingerprint=result_fingerprint,
        created_at=created_at,
        updated_at=created_at,
    )


def publish_finalization_intent(
    database_path: Path,
    intent: FinalizationIntent,
) -> tuple[ArtifactRegistryEntry, ...]:
    # Replay publication from durable staging or existing logical sources and verify every result.
    database_path = Path(database_path).resolve()
    reports_root = database_path.parent.parent.resolve()
    for entry in intent.artifacts:
        if intent.status in {"registered", "completed"}:
            _validate_file(
                _resolve_under_root(
                    reports_root,
                    entry.content_path,
                    field_name="content_path",
                    logical_key=entry.logical_key,
                ),
                entry,
                label="content-addressed",
            )
            _validate_file(
                _resolve_under_root(
                    reports_root,
                    entry.logical_path,
                    field_name="logical_path",
                    logical_key=entry.logical_key,
                ),
                entry,
                label="logical",
            )
            continue
        metadata = dict(entry.metadata)
        source_value = metadata.get("staging_path") or metadata.get("source_path")
        if not source_value:
            raise FinalizationArtifactError(
                f"finalization intent has no replay source: logical_key={entry.logical_key}; "
                f"intent_id={intent.intent_id}; status={intent.status}"
            )
        source = _resolve_under_root(
            reports_root,
            str(source_value),
            field_name="staging/source path",
            logical_key=entry.logical_key,
        )
        _validate_file(source, entry, label="staging/source")
        _publish_entry(
            reports_root,
            entry,
            source,
            repair_unregistered=True,
        )
    return intent.artifacts


def validate_registered_artifacts(
    database_path: Path,
    intent: FinalizationIntent,
    registry: Sequence[ArtifactRegistryEntry],
) -> None:
    # Require exact registry identity and matching logical/content bytes before completion or recovery success.
    reports_root = Path(database_path).resolve().parent.parent.resolve()
    expected = {item.logical_key: item for item in intent.artifacts}
    actual = {item.logical_key: item for item in registry}
    registry_key_counts = Counter(item.logical_key for item in registry)
    duplicate_registry_keys = tuple(
        sorted(key for key, count in registry_key_counts.items() if count > 1)
    )
    missing = tuple(sorted(set(expected) - set(actual)))
    unexpected = tuple(sorted(set(actual) - set(expected)))
    conflicts = tuple(
        sorted(
            key
            for key in set(expected) & set(actual)
            if expected[key] != actual[key]
        )
    )
    if duplicate_registry_keys or missing or unexpected or conflicts:
        raise FinalizationArtifactError(
            "artifact registry does not match finalization intent: "
            f"campaign_id={intent.campaign_id.value}; intent_id={intent.intent_id}; "
            f"duplicate_keys={duplicate_registry_keys}; missing={missing}; "
            f"unexpected={unexpected}; conflicts={conflicts}"
        )
    for logical_key in sorted(expected):
        entry = actual[logical_key]
        _validate_file(
            _resolve_under_root(
                reports_root,
                entry.content_path,
                field_name="content_path",
                logical_key=logical_key,
            ),
            entry,
            label="content-addressed",
        )
        _validate_file(
            _resolve_under_root(
                reports_root,
                entry.logical_path,
                field_name="logical_path",
                logical_key=logical_key,
            ),
            entry,
            label="logical",
        )


def cleanup_finalization_staging(database_path: Path, *, keep_intent_id: str | None) -> tuple[str, ...]:
    # Remove only dedicated staging directories that are not owned by the active durable intent.
    staging_parent = Path(database_path).resolve().parent / ".finalization"
    if not staging_parent.is_dir():
        return ()
    removed: list[str] = []
    for candidate in sorted(staging_parent.iterdir(), key=lambda item: item.name):
        if not candidate.is_dir() or candidate.name == keep_intent_id:
            continue
        shutil.rmtree(candidate)
        removed.append(str(candidate))
    try:
        if not any(staging_parent.iterdir()):
            staging_parent.rmdir()
    except (FileNotFoundError, OSError):
        pass
    return tuple(removed)


__all__ = [
    "ArtifactSource",
    "FinalizationArtifactError",
    "build_finalization_intent",
    "cleanup_finalization_staging",
    "publish_finalization_intent",
    "validate_registered_artifacts",
]
