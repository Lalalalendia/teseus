"""Transport-neutral framed TCP adapter for the versioned remote contracts.

The adapter deliberately knows nothing about mutation semantics, leases, retry
policy, or result authority.  It only provides a bounded, message-oriented
connection that can carry the existing JSON contracts over a real socket.
"""

from __future__ import annotations

import socket
import struct
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Protocol

from theseus_contracts.serialization import dumps, loads_object


TRANSPORT_PROTOCOL_VERSION = 1
DEFAULT_MAX_FRAME_BYTES = 16 * 1024 * 1024
FRAME_HEADER_BYTES = 8


class TransportError(RuntimeError):
    """Base error for connection, framing, and transport lifecycle failures."""


class TransportClosed(TransportError):
    """Raised when the peer closes the socket before a complete frame arrives."""


class TransportTimeout(TransportError):
    """Raised when a deadline expires while reading or writing a frame."""


class TransportFramingError(TransportError):
    """Raised for malformed, oversized, or incompatible transport frames."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _deadline_timeout(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    remaining = float(deadline) - time.monotonic()
    if remaining <= 0:
        raise TransportTimeout("transport deadline expired")
    return remaining


@dataclass(frozen=True, slots=True)
class TransportMessage:
    """One validated envelope; payload semantics belong to a higher layer."""

    message_type: str
    message_id: str
    payload: Mapping[str, Any]
    reply_to: str | None = None
    transport_version: int = TRANSPORT_PROTOCOL_VERSION
    created_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "transport_version": int(self.transport_version),
            "message_type": self.message_type,
            "message_id": self.message_id,
            "reply_to": self.reply_to,
            "created_at": self.created_at or _utc_now(),
            "payload": dict(self.payload),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "TransportMessage":
        if not isinstance(value, Mapping):
            raise TransportFramingError("transport envelope must be an object")
        allowed = {"transport_version", "message_type", "message_id", "reply_to", "created_at", "payload"}
        unknown = sorted(str(key) for key in value if str(key) not in allowed)
        if unknown:
            raise TransportFramingError(f"unknown transport fields: {unknown}")
        version = value.get("transport_version")
        if isinstance(version, bool) or not isinstance(version, int) or version != TRANSPORT_PROTOCOL_VERSION:
            raise TransportFramingError(f"unsupported transport version: {version!r}")
        message_type = value.get("message_type")
        message_id = value.get("message_id")
        payload = value.get("payload")
        if not isinstance(message_type, str) or not message_type.strip():
            raise TransportFramingError("transport message_type must be non-empty")
        if not isinstance(message_id, str) or not message_id.strip():
            raise TransportFramingError("transport message_id must be non-empty")
        if not isinstance(payload, Mapping):
            raise TransportFramingError("transport payload must be an object")
        reply_to = value.get("reply_to")
        if reply_to is not None and not isinstance(reply_to, str):
            raise TransportFramingError("transport reply_to must be a string or null")
        created_at = value.get("created_at")
        if not isinstance(created_at, str) or not created_at.strip():
            raise TransportFramingError("transport created_at must be non-empty")
        return cls(message_type, message_id, dict(payload), reply_to, version, created_at)


class TransportConnection(Protocol):
    """Minimal transport SPI consumed by control and artifact adapters."""

    def send_message(
        self,
        message_type: str,
        payload: Mapping[str, Any],
        *,
        message_id: str | None = None,
        reply_to: str | None = None,
        deadline: float | None = None,
    ) -> str: ...

    def receive_message(self, *, deadline: float | None = None) -> TransportMessage: ...

    def close(self) -> None: ...


class FramedSocket:
    """Length-prefixed socket with exact-read semantics and bounded frames."""

    def __init__(self, sock: socket.socket, *, max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES) -> None:
        if int(max_frame_bytes) < FRAME_HEADER_BYTES:
            raise ValueError("max_frame_bytes is too small")
        self.socket = sock
        self.max_frame_bytes = int(max_frame_bytes)
        self._send_lock = threading.RLock()
        self._receive_buffer = bytearray()
        self._closed = False

    def _set_timeout(self, deadline: float | None) -> None:
        try:
            self.socket.settimeout(_deadline_timeout(deadline))
        except socket.timeout as exc:
            raise TransportTimeout("transport deadline expired") from exc
        except OSError as exc:
            raise TransportClosed("socket is closed") from exc

    def send_bytes(self, payload: bytes, *, deadline: float | None = None) -> None:
        data = bytes(payload)
        if len(data) > self.max_frame_bytes:
            raise TransportFramingError(f"frame exceeds limit: {len(data)} > {self.max_frame_bytes}")
        with self._send_lock:
            if self._closed:
                raise TransportClosed("socket is closed")
            try:
                self._set_timeout(deadline)
                self.socket.sendall(struct.pack("!Q", len(data)) + data)
            except socket.timeout as exc:
                raise TransportTimeout("transport send deadline expired") from exc
            except OSError as exc:
                raise TransportClosed("socket send failed") from exc

    def _fill_receive_buffer(self, size: int, *, deadline: float | None = None) -> None:
        while len(self._receive_buffer) < int(size):
            try:
                self._set_timeout(deadline)
                chunk = self.socket.recv(max(1, int(size) - len(self._receive_buffer)))
            except socket.timeout as exc:
                raise TransportTimeout("transport receive deadline expired") from exc
            except OSError as exc:
                raise TransportClosed("socket receive failed") from exc
            if not chunk:
                raise TransportClosed("peer closed the connection mid-frame")
            self._receive_buffer.extend(chunk)

    def receive_bytes(self, *, deadline: float | None = None) -> bytes:
        if self._closed:
            raise TransportClosed("socket is closed")
        # Keep partial headers/bodies across deadline timeouts.  A polling
        # control loop must never desynchronize the stream merely because a
        # frame arrived between two short receive deadlines.
        self._fill_receive_buffer(FRAME_HEADER_BYTES, deadline=deadline)
        (size,) = struct.unpack("!Q", bytes(self._receive_buffer[:FRAME_HEADER_BYTES]))
        if size > self.max_frame_bytes:
            raise TransportFramingError(f"frame exceeds limit: {size} > {self.max_frame_bytes}")
        total = FRAME_HEADER_BYTES + int(size)
        self._fill_receive_buffer(total, deadline=deadline)
        payload = bytes(self._receive_buffer[FRAME_HEADER_BYTES:total])
        del self._receive_buffer[:total]
        return payload

    def send_message(
        self,
        message_type: str,
        payload: Mapping[str, Any],
        *,
        message_id: str | None = None,
        reply_to: str | None = None,
        deadline: float | None = None,
    ) -> str:
        identifier = str(message_id or uuid.uuid4().hex)
        message = TransportMessage(
            message_type=str(message_type),
            message_id=identifier,
            payload=dict(payload),
            reply_to=reply_to,
            created_at=_utc_now(),
        )
        self.send_bytes(dumps(message.to_dict()).encode("utf-8"), deadline=deadline)
        return identifier

    def receive_message(self, *, deadline: float | None = None) -> TransportMessage:
        try:
            raw = self.receive_bytes(deadline=deadline)
            return TransportMessage.from_dict(loads_object(raw))
        except TransportError:
            raise
        except (TypeError, ValueError) as exc:
            raise TransportFramingError(f"invalid transport JSON: {exc}") from exc

    @property
    def closed(self) -> bool:
        return bool(self._closed)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.socket.close()
        except OSError:
            pass

    def __enter__(self) -> "FramedSocket":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def connect_tcp(host: str, port: int, *, timeout_seconds: float = 10.0, max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES) -> FramedSocket:
    """Open one bounded TCP connection without changing protocol semantics."""

    try:
        sock = socket.create_connection((str(host), int(port)), timeout=max(0.001, float(timeout_seconds)))
    except OSError as exc:
        raise TransportClosed(f"cannot connect to {host}:{port}: {exc}") from exc
    return FramedSocket(sock, max_frame_bytes=max_frame_bytes)


class TcpTransportServer:
    """Small threaded TCP acceptor; each callback owns its connection lifecycle."""

    def __init__(
        self,
        host: str,
        port: int,
        on_connection: Callable[[FramedSocket, tuple[str, int]], None],
        *,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        backlog: int = 32,
    ) -> None:
        self.host = str(host)
        self.port = int(port)
        self.on_connection = on_connection
        self.max_frame_bytes = int(max_frame_bytes)
        self.backlog = max(1, int(backlog))
        self._socket: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._connections: set[FramedSocket] = set()
        self._lock = threading.RLock()

    @property
    def address(self) -> tuple[str, int]:
        with self._lock:
            if self._socket is None:
                return (self.host, self.port)
            raw = self._socket.getsockname()
            return (str(raw[0]), int(raw[1]))

    def start(self) -> tuple[str, int]:
        with self._lock:
            if self._socket is not None:
                return self.address
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen(self.backlog)
            sock.settimeout(0.2)
            self._socket = sock
            self._stop.clear()
            self._thread = threading.Thread(target=self._accept_loop, name="theseus-network-accept", daemon=True)
            self._thread.start()
            return self.address

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            with self._lock:
                sock = self._socket
            if sock is None:
                return
            try:
                accepted, address = sock.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            connection = FramedSocket(accepted, max_frame_bytes=self.max_frame_bytes)
            with self._lock:
                self._connections.add(connection)
            threading.Thread(
                target=self._run_connection,
                args=(connection, (str(address[0]), int(address[1]))),
                name="theseus-network-session",
                daemon=True,
            ).start()

    def _run_connection(self, connection: FramedSocket, address: tuple[str, int]) -> None:
        try:
            self.on_connection(connection, address)
        finally:
            with self._lock:
                self._connections.discard(connection)
            connection.close()

    def stop(self) -> None:
        with self._lock:
            self._stop.set()
            sock = self._socket
            self._socket = None
            connections = tuple(self._connections)
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass
        for connection in connections:
            connection.close()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)
        self._thread = None


__all__ = [
    "DEFAULT_MAX_FRAME_BYTES",
    "FRAME_HEADER_BYTES",
    "FramedSocket",
    "TcpTransportServer",
    "TransportClosed",
    "TransportError",
    "TransportFramingError",
    "TransportConnection",
    "TransportMessage",
    "TransportTimeout",
    "TRANSPORT_PROTOCOL_VERSION",
    "connect_tcp",
]
