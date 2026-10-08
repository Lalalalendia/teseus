"""Bounded content-addressed artifact transfer over a framed connection."""

from __future__ import annotations

import base64
import time
import hashlib
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .artifact_store import ArtifactIntegrityError, ContentAddressedArtifactStore
from .network_transport import TransportConnection, TransportMessage


NETWORK_ARTIFACT_CHUNK_BYTES = 64 * 1024


class NetworkArtifactError(RuntimeError):
    """Raised when a network artifact cannot be verified and atomically published."""


@dataclass(frozen=True, slots=True)
class NetworkArtifactEntry:
    artifact_id: str
    size_bytes: int

    def to_dict(self) -> dict[str, Any]:
        return {"artifact_id": self.artifact_id, "size_bytes": int(self.size_bytes)}


@dataclass(frozen=True, slots=True)
class NetworkArtifactTransferStats:
    artifact_ids: tuple[str, ...]
    bytes_transferred: int
    cache_hits: int
    cache_misses: int
    chunks: int
    round_trips: int = 0
    transfer_seconds: float = 0.0

    @property
    def cache_hit_rate(self) -> float:
        total = self.cache_hits + self.cache_misses
        return self.cache_hits / total if total else 1.0


def manifest_for(store: ContentAddressedArtifactStore, artifact_ids: Sequence[str]) -> tuple[NetworkArtifactEntry, ...]:
    entries: list[NetworkArtifactEntry] = []
    for artifact_id in dict.fromkeys(str(item) for item in artifact_ids):
        try:
            path = store.path_for(artifact_id, verify=True)
            entries.append(NetworkArtifactEntry(artifact_id, path.stat().st_size))
        except (ArtifactIntegrityError, OSError) as exc:
            raise NetworkArtifactError(f"source artifact is unavailable: {artifact_id}") from exc
    return tuple(entries)


def _reply_payload(message: Mapping[str, Any]) -> tuple[str, Mapping[str, Any]]:
    message_id = message.get("message_id")
    payload = message.get("payload")
    if not isinstance(message_id, str) or not isinstance(payload, Mapping):
        raise NetworkArtifactError("artifact response is malformed")
    return message_id, payload


class NetworkArtifactSender:
    """Send only missing CAS objects and verify every final hash on the worker."""

    def __init__(
        self,
        connection: TransportConnection,
        store: ContentAddressedArtifactStore,
        *,
        on_message: Callable[[TransportMessage], bool] | None = None,
    ) -> None:
        self.connection = connection
        self.store = store
        self.on_message = on_message

    def _wait_for(self, message_type: str, request_id: str, *, deadline: float | None) -> Mapping[str, Any]:
        while True:
            message = self.connection.receive_message(deadline=deadline)
            if message.message_type != message_type or message.reply_to != request_id:
                if self.on_message is not None and self.on_message(message):
                    continue
                raise NetworkArtifactError(
                    f"unexpected artifact response: expected={message_type}/{request_id}, "
                    f"received={message.message_type}/{message.reply_to}"
                )
            return message.payload

    def transfer(self, artifact_ids: Sequence[str], *, deadline: float | None = None) -> NetworkArtifactTransferStats:
        started = time.perf_counter()
        entries = manifest_for(self.store, artifact_ids)
        manifest_id = self.connection.send_message(
            "artifact.manifest",
            {"artifacts": [entry.to_dict() for entry in entries]},
            deadline=deadline,
        )
        response = self._wait_for("artifact.missing", manifest_id, deadline=deadline)
        missing = response.get("missing")
        if not isinstance(missing, list) or any(not isinstance(item, str) for item in missing):
            raise NetworkArtifactError("artifact missing response is invalid")
        entry_by_id = {entry.artifact_id: entry for entry in entries}
        missing_set = set(missing)
        round_trips = 1
        if not missing_set.issubset(entry_by_id):
            raise NetworkArtifactError("worker reported an artifact outside the manifest")
        transferred = 0
        chunks = 0
        for artifact_id in (entry.artifact_id for entry in entries if entry.artifact_id in missing_set):
            entry = entry_by_id[artifact_id]
            path = self.store.path_for(artifact_id, verify=True)
            begin_id = self.connection.send_message(
                "artifact.begin",
                {"artifact_id": artifact_id, "size_bytes": entry.size_bytes, "transfer_id": uuid.uuid4().hex},
                deadline=deadline,
            )
            # The worker acknowledges begin only after it has opened a disposable part file.
            self._wait_for("artifact.ready", begin_id, deadline=deadline)
            round_trips += 1
            offset = 0
            with path.open("rb") as reader:
                while True:
                    chunk = reader.read(NETWORK_ARTIFACT_CHUNK_BYTES)
                    if not chunk:
                        break
                    self.connection.send_message(
                        "artifact.chunk",
                        {
                            "artifact_id": artifact_id,
                            "offset": offset,
                            "data": base64.b64encode(chunk).decode("ascii"),
                        },
                        deadline=deadline,
                    )
                    offset += len(chunk)
                    transferred += len(chunk)
                    chunks += 1
            end_id = self.connection.send_message(
                "artifact.end",
                {"artifact_id": artifact_id, "size_bytes": offset},
                deadline=deadline,
            )
            result = self._wait_for("artifact.accepted", end_id, deadline=deadline)
            round_trips += 1
            if result.get("artifact_id") != artifact_id or result.get("size_bytes") != offset:
                raise NetworkArtifactError("worker accepted an artifact with mismatched identity")
        return NetworkArtifactTransferStats(
            artifact_ids=tuple(entry.artifact_id for entry in entries),
            bytes_transferred=transferred,
            cache_hits=len(entries) - len(missing_set),
            cache_misses=len(missing_set),
            chunks=chunks,
            round_trips=round_trips,
            transfer_seconds=max(0.0, time.perf_counter() - started),
        )


class NetworkArtifactReceiver:
    """Worker-side receiver with bounded chunks, hash verification, and atomic CAS publication."""

    def __init__(self, connection: TransportConnection, store: ContentAddressedArtifactStore) -> None:
        self.connection = connection
        self.store = store
        self._entries: dict[str, NetworkArtifactEntry] = {}
        self._temporary: dict[str, tuple[Path, Any, hashlib._Hash]] = {}

    def handle(self, message: Any) -> bool:
        message_type = message.message_type
        payload = message.payload
        if message_type == "artifact.manifest":
            raw_entries = payload.get("artifacts")
            if not isinstance(raw_entries, list):
                raise NetworkArtifactError("artifact manifest must contain an array")
            entries: list[NetworkArtifactEntry] = []
            for raw in raw_entries:
                if not isinstance(raw, Mapping):
                    raise NetworkArtifactError("artifact manifest entry must be an object")
                artifact_id = str(raw.get("artifact_id", ""))
                size = raw.get("size_bytes")
                if len(artifact_id) != 64 or not isinstance(size, int) or size < 0:
                    raise NetworkArtifactError("artifact manifest entry is invalid")
                entries.append(NetworkArtifactEntry(artifact_id, size))
            self._entries = {entry.artifact_id: entry for entry in entries}
            missing = list(self.store.missing_many(entry.artifact_id for entry in entries))
            self.connection.send_message("artifact.missing", {"missing": missing}, reply_to=message.message_id)
            return True
        if message_type == "artifact.begin":
            artifact_id = str(payload.get("artifact_id", ""))
            entry = self._entries.get(artifact_id)
            if entry is None or payload.get("size_bytes") != entry.size_bytes:
                raise NetworkArtifactError("artifact begin is not bound to the manifest")
            if artifact_id in self._temporary:
                raise NetworkArtifactError("artifact already has an active transfer")
            temporary = self.store.root / f".network-{artifact_id}-{uuid.uuid4().hex}.part"
            handle = temporary.open("xb")
            self._temporary[artifact_id] = (temporary, handle, hashlib.sha256())
            self.connection.send_message("artifact.ready", {"artifact_id": artifact_id}, reply_to=message.message_id)
            return True
        if message_type == "artifact.chunk":
            artifact_id = str(payload.get("artifact_id", ""))
            current = self._temporary.get(artifact_id)
            if current is None or not isinstance(payload.get("offset"), int):
                raise NetworkArtifactError("artifact chunk has no active transfer")
            temporary, handle, digest = current
            offset = int(payload["offset"])
            if offset != handle.tell():
                raise NetworkArtifactError("artifact chunk offset is not contiguous")
            raw_data = payload.get("data")
            if not isinstance(raw_data, str):
                raise NetworkArtifactError("artifact chunk data must be base64 text")
            try:
                chunk = base64.b64decode(raw_data.encode("ascii"), validate=True)
            except (ValueError, UnicodeError) as exc:
                raise NetworkArtifactError("artifact chunk is not valid base64") from exc
            if len(chunk) > NETWORK_ARTIFACT_CHUNK_BYTES:
                raise NetworkArtifactError("artifact chunk exceeds bounded size")
            entry = self._entries.get(artifact_id)
            if entry is None or offset + len(chunk) > entry.size_bytes:
                raise NetworkArtifactError("artifact transfer exceeds manifest size")
            handle.write(chunk)
            digest.update(chunk)
            return True
        if message_type == "artifact.end":
            artifact_id = str(payload.get("artifact_id", ""))
            current = self._temporary.pop(artifact_id, None)
            entry = self._entries.get(artifact_id)
            if current is None or entry is None:
                raise NetworkArtifactError("artifact end has no active transfer")
            temporary, handle, digest = current
            try:
                handle.flush()
                os.fsync(handle.fileno())
                handle.close()
                size = temporary.stat().st_size
                if size != entry.size_bytes or payload.get("size_bytes") != size or digest.hexdigest() != artifact_id:
                    raise NetworkArtifactError("received artifact failed size or SHA-256 verification")
                record = self.store.put_file(temporary, expected_id=artifact_id)
            finally:
                temporary.unlink(missing_ok=True)
            self.connection.send_message(
                "artifact.accepted",
                {"artifact_id": record.artifact_id, "size_bytes": record.size_bytes},
                reply_to=message.message_id,
            )
            return True
        return False

    def close(self) -> None:
        for temporary, handle, _ in tuple(self._temporary.values()):
            try:
                handle.close()
            finally:
                temporary.unlink(missing_ok=True)
        self._temporary.clear()


__all__ = [
    "NETWORK_ARTIFACT_CHUNK_BYTES",
    "NetworkArtifactEntry",
    "NetworkArtifactError",
    "NetworkArtifactReceiver",
    "NetworkArtifactSender",
    "NetworkArtifactTransferStats",
    "manifest_for",
]
