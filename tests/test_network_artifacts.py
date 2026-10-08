from __future__ import annotations

import base64
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from theseus_local.artifact_store import ContentAddressedArtifactStore
from theseus_local.network_artifacts import (
    NETWORK_ARTIFACT_CHUNK_BYTES,
    NetworkArtifactError,
    NetworkArtifactReceiver,
    NetworkArtifactSender,
)
from theseus_local.network_transport import FramedSocket, TransportClosed, TransportError


class _FaultySenderConnection:
    def __init__(self, connection: FramedSocket, *, drop_after: int | None = None, corrupt_chunk: int | None = None, control_delay: float = 0.0) -> None:
        self.connection = connection
        self.drop_after = drop_after
        self.corrupt_chunk = corrupt_chunk
        self.control_delay = float(control_delay)
        self.chunks = 0

    def send_message(self, message_type: str, payload: dict[str, object], **kwargs: object) -> str:
        if self.control_delay and message_type in {"artifact.manifest", "artifact.begin", "artifact.end"}:
            time.sleep(self.control_delay)
        if message_type == "artifact.chunk":
            self.chunks += 1
            if self.drop_after is not None and self.chunks > self.drop_after:
                self.connection.close()
                raise TransportClosed("injected transfer drop")
            if self.corrupt_chunk == self.chunks:
                raw = base64.b64decode(str(payload["data"]).encode("ascii"))
                payload = dict(payload)
                payload["data"] = base64.b64encode(bytes([raw[0] ^ 1]) + raw[1:]).decode("ascii")
        return self.connection.send_message(message_type, payload, **kwargs)

    def receive_message(self, **kwargs: object):
        return self.connection.receive_message(**kwargs)

    def close(self) -> None:
        self.connection.close()


def _transfer(
    source: ContentAddressedArtifactStore,
    destination: ContentAddressedArtifactStore,
    artifact_id: str,
    *,
    drop_after: int | None = None,
    corrupt_chunk: int | None = None,
    control_delay: float = 0.0,
) -> tuple[object | None, tuple[Exception, ...]]:
    left, right = socket.socketpair()
    sender_connection = FramedSocket(left)
    receiver_connection = FramedSocket(right)
    sender = _FaultySenderConnection(sender_connection, drop_after=drop_after, corrupt_chunk=corrupt_chunk, control_delay=control_delay)
    receiver = NetworkArtifactReceiver(receiver_connection, destination)
    errors: list[Exception] = []
    finished = threading.Event()

    def serve() -> None:
        try:
            while not finished.is_set():
                message = receiver_connection.receive_message(deadline=time.monotonic() + 5.0)
                receiver.handle(message)
        except (TransportError, NetworkArtifactError) as exc:
            if not finished.is_set():
                errors.append(exc)
        finally:
            receiver.close()

    thread = threading.Thread(target=serve)
    thread.start()
    stats: object | None = None
    try:
        stats = NetworkArtifactSender(sender, source).transfer((artifact_id,), deadline=time.monotonic() + 5.0)
    except Exception as exc:
        errors.append(exc)
    finally:
        finished.set()
        sender.close()
        receiver_connection.close()
        thread.join(timeout=5.0)
    return stats, tuple(errors)


def test_network_artifact_stream_is_bounded_and_content_addressed(tmp_path: Path) -> None:
    source = ContentAddressedArtifactStore(tmp_path / "source")
    destination = ContentAddressedArtifactStore(tmp_path / "destination")
    record = source.put_bytes(b"x" * (NETWORK_ARTIFACT_CHUNK_BYTES * 3 + 17))
    stats, errors = _transfer(source, destination, record.artifact_id)
    assert not errors
    assert stats is not None
    assert stats.bytes_transferred == record.size_bytes
    assert stats.chunks == 4
    assert destination.get_bytes(record.artifact_id) == source.get_bytes(record.artifact_id)
    assert all(not item.name.endswith(".part") for item in destination.root.iterdir())


def test_network_artifact_manifest_uses_one_batch_existence_pass(tmp_path: Path) -> None:
    source = ContentAddressedArtifactStore(tmp_path / "source")
    destination = ContentAddressedArtifactStore(tmp_path / "destination")
    record = source.put_bytes(b"batch-check")
    calls: list[tuple[str, ...]] = []
    original = destination.missing_many

    def wrapped(artifact_ids):
        ids = tuple(artifact_ids)
        calls.append(ids)
        return original(ids)

    destination.missing_many = wrapped
    stats, errors = _transfer(source, destination, record.artifact_id)
    assert not errors
    assert stats is not None
    assert calls == [(record.artifact_id,)]


    source = ContentAddressedArtifactStore(tmp_path / "source")
    destination = ContentAddressedArtifactStore(tmp_path / "destination")
    record = source.put_bytes(b"y" * (NETWORK_ARTIFACT_CHUNK_BYTES * 4))
    stats, errors = _transfer(source, destination, record.artifact_id, drop_after=3)
    assert stats is None
    assert errors
    assert not destination.has(record.artifact_id)
    assert not tuple(destination.root.glob(".network-*.part"))


def test_network_artifact_corruption_cannot_be_published(tmp_path: Path) -> None:
    source = ContentAddressedArtifactStore(tmp_path / "source")
    destination = ContentAddressedArtifactStore(tmp_path / "destination")
    record = source.put_bytes(b"z" * (NETWORK_ARTIFACT_CHUNK_BYTES * 2 + 1))
    stats, errors = _transfer(source, destination, record.artifact_id, corrupt_chunk=2)
    assert stats is None
    assert errors
    assert not destination.has(record.artifact_id)
    assert not tuple(destination.root.glob(".network-*.part"))


def test_concurrent_same_artifact_transfers_deduplicate_atomically(tmp_path: Path) -> None:
    source = ContentAddressedArtifactStore(tmp_path / "source")
    destination = ContentAddressedArtifactStore(tmp_path / "destination")
    record = source.put_bytes(b"concurrent" * (NETWORK_ARTIFACT_CHUNK_BYTES // 10))

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = tuple(pool.submit(_transfer, source, destination, record.artifact_id) for _ in range(2))
        results = tuple(future.result(timeout=15.0) for future in futures)
    assert all(not errors for _, errors in results)
    assert destination.get_bytes(record.artifact_id) == source.get_bytes(record.artifact_id)
    assert destination.verify(record.artifact_id).artifact_id == record.artifact_id
def test_network_artifact_transfer_reports_round_trips_under_control_latency(tmp_path: Path) -> None:
    source = ContentAddressedArtifactStore(tmp_path / "source")
    destination = ContentAddressedArtifactStore(tmp_path / "destination")
    record = source.put_bytes(b"latency" * NETWORK_ARTIFACT_CHUNK_BYTES)
    stats, errors = _transfer(source, destination, record.artifact_id, control_delay=0.02)
    assert not errors
    assert stats is not None
    assert stats.round_trips == 3
    assert stats.transfer_seconds >= 0.05
