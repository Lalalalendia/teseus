"""Durable worker identity and minimal enrollment/trust boundary."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from theseus_contracts.serialization import dumps, utc_now
from theseus_local.locking import InterProcessFileLock, InterProcessLockError


ENROLLMENT_SCHEMA_VERSION = 1
ENROLLMENT_PROTOCOL_VERSION = 1
WORKER_IDENTITY_SCHEMA_VERSION = 1


class EnrollmentError(RuntimeError):
    """Raised when a worker cannot cross the admission boundary."""


def _required(value: Mapping[str, Any], name: str) -> str:
    item = value.get(name)
    if not isinstance(item, str) or not item.strip():
        raise EnrollmentError(f"{name} must be a non-empty string")
    return item


@dataclass(frozen=True, slots=True)
class WorkerIdentity:
    """Durable host identity; PID and network session are intentionally absent."""

    worker_id: str
    identity_id: str
    created_at: str
    schema_version: int = WORKER_IDENTITY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not self.worker_id.strip() or not self.identity_id.strip() or not self.created_at.strip():
            raise EnrollmentError("worker identity fields must be non-empty")
        if self.schema_version != WORKER_IDENTITY_SCHEMA_VERSION:
            raise EnrollmentError("unsupported worker identity schema")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": int(self.schema_version),
            "worker_id": self.worker_id,
            "identity_id": self.identity_id,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WorkerIdentity":
        if value.get("schema_version") != WORKER_IDENTITY_SCHEMA_VERSION:
            raise EnrollmentError("unsupported worker identity schema")
        return cls(
            worker_id=_required(value, "worker_id"),
            identity_id=_required(value, "identity_id"),
            created_at=_required(value, "created_at"),
        )


@dataclass(frozen=True, slots=True)
class WorkerSessionIdentity:
    """Ephemeral connection identity; reconnecting keeps worker identity but changes session."""

    worker_id: str
    identity_id: str
    session_id: str
    process_id: int
    started_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "identity_id": self.identity_id,
            "session_id": self.session_id,
            "process_id": int(self.process_id),
            "started_at": self.started_at,
        }


def load_or_create_worker_identity(root: Path, worker_id: str) -> WorkerIdentity:
    """Load one stable identity or create it atomically in the worker root."""

    worker_root = Path(root).resolve()
    worker_root.mkdir(parents=True, exist_ok=True)
    path = worker_root / "worker-identity.json"
    lock_path = worker_root / ".worker-identity.lock"
    try:
        with InterProcessFileLock(lock_path):
            if path.is_file():
                raw = json.loads(path.read_text(encoding="utf-8"))
                identity = WorkerIdentity.from_dict(raw if isinstance(raw, Mapping) else {})
                if identity.worker_id != str(worker_id):
                    raise EnrollmentError("durable worker identity belongs to another worker_id")
                return identity
            identity = WorkerIdentity(str(worker_id), uuid.uuid4().hex, utc_now())
            temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
            try:
                temporary.write_text(dumps(identity.to_dict()) + "\n", encoding="utf-8", newline="\n")
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
            return identity
    except (OSError, ValueError, TypeError, InterProcessLockError) as exc:
        raise EnrollmentError(f"worker identity is unavailable: {path}") from exc


def _secret_bytes(secret: str | bytes) -> bytes:
    value = secret.encode("utf-8") if isinstance(secret, str) else bytes(secret)
    if len(value) < 16:
        raise ValueError("enrollment secret must contain at least 16 bytes")
    return value


def enrollment_proof(secret: str | bytes, payload: Mapping[str, Any]) -> str:
    """Create an HMAC proof over the exact canonical enrollment payload."""

    return hmac.new(_secret_bytes(secret), dumps(dict(payload)).encode("utf-8"), hashlib.sha256).hexdigest()


@dataclass(frozen=True, slots=True)
class EnrollmentDecision:
    accepted: bool
    reason: str
    worker_id: str
    runtime_fingerprint: str | None = None


class EnrollmentAuthority:
    """Shared-secret admission registry with durable authorization and revocation."""

    def __init__(self, registry_path: Path, shared_secret: str | bytes) -> None:
        self.registry_path = Path(registry_path).resolve()
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.registry_path.with_name(f".{self.registry_path.name}.lock")
        self._secret = _secret_bytes(shared_secret)

    def _load(self) -> dict[str, Any]:
        if not self.registry_path.is_file():
            return {"schema_version": ENROLLMENT_SCHEMA_VERSION, "workers": {}}
        raw = json.loads(self.registry_path.read_text(encoding="utf-8"))
        if not isinstance(raw, Mapping) or raw.get("schema_version") != ENROLLMENT_SCHEMA_VERSION:
            raise EnrollmentError("enrollment registry schema is unsupported")
        workers = raw.get("workers")
        if not isinstance(workers, Mapping):
            raise EnrollmentError("enrollment registry workers section is invalid")
        return {"schema_version": ENROLLMENT_SCHEMA_VERSION, "workers": dict(workers)}

    def _save(self, value: Mapping[str, Any]) -> None:
        temporary = self.registry_path.with_name(f".{self.registry_path.name}.{uuid.uuid4().hex}.tmp")
        try:
            temporary.write_text(dumps(dict(value)) + "\n", encoding="utf-8", newline="\n")
            os.replace(temporary, self.registry_path)
        finally:
            temporary.unlink(missing_ok=True)

    def authorize(
        self,
        worker_id: str,
        *,
        runtime_fingerprint: str | None = None,
        slots: int = 1,
        credential_expires_at: str | None = None,
    ) -> None:
        identifier = str(worker_id).strip()
        if not identifier:
            raise ValueError("worker_id must be non-empty")
        if credential_expires_at is not None:
            _parse_expiry(credential_expires_at)
        with InterProcessFileLock(self.lock_path):
            state = self._load()
            workers = dict(state["workers"])
            workers[identifier] = {
                "worker_id": identifier,
                "revoked": False,
                "runtime_fingerprint": runtime_fingerprint,
                "slots": max(1, int(slots)),
                "credential_expires_at": credential_expires_at,
                "updated_at": utc_now(),
            }
            self._save({"schema_version": ENROLLMENT_SCHEMA_VERSION, "workers": workers})

    def revoke(self, worker_id: str) -> None:
        with InterProcessFileLock(self.lock_path):
            state = self._load()
            workers = dict(state["workers"])
            record = dict(workers.get(str(worker_id), {"worker_id": str(worker_id)}))
            record["revoked"] = True
            record["updated_at"] = utc_now()
            workers[str(worker_id)] = record
            self._save({"schema_version": ENROLLMENT_SCHEMA_VERSION, "workers": workers})

    def restore(self, worker_id: str) -> None:
        with InterProcessFileLock(self.lock_path):
            state = self._load()
            workers = dict(state["workers"])
            if str(worker_id) not in workers:
                raise EnrollmentError("worker is not enrolled")
            record = dict(workers[str(worker_id)])
            record["revoked"] = False
            record["updated_at"] = utc_now()
            workers[str(worker_id)] = record
            self._save({"schema_version": ENROLLMENT_SCHEMA_VERSION, "workers": workers})

    def record_session(self, worker_id: str, payload: Mapping[str, Any]) -> None:
        """Persist diagnostics for an admitted session without changing authority."""

        identifier = str(worker_id).strip()
        with InterProcessFileLock(self.lock_path):
            state = self._load()
            record = state["workers"].get(identifier)
            if not isinstance(record, Mapping):
                raise EnrollmentError("worker is not enrolled")
            updated = dict(record)
            updated.update(
                {
                    "identity_id": str(payload.get("identity_id", "")),
                    "session_id": str(payload.get("session_id", "")),
                    "runtime_identity": dict(payload.get("runtime_identity", {}))
                    if isinstance(payload.get("runtime_identity"), Mapping)
                    else None,
                    "capabilities": dict(payload.get("capabilities", {}))
                    if isinstance(payload.get("capabilities"), Mapping)
                    else None,
                    "platform": str(payload.get("platform", "")),
                    "execution_backends": list(payload.get("execution_backends", [])),
                    "available_slots": max(1, int(payload.get("available_slots", 1))),
                    "last_heartbeat_at": utc_now(),
                    "updated_at": utc_now(),
                }
            )
            workers = dict(state["workers"])
            workers[identifier] = updated
            self._save({"schema_version": ENROLLMENT_SCHEMA_VERSION, "workers": workers})

    def record_heartbeat(self, worker_id: str, *, sequence: int) -> None:
        """Persist the last accepted heartbeat as a diagnostic projection."""

        identifier = str(worker_id).strip()
        with InterProcessFileLock(self.lock_path):
            state = self._load()
            record = state["workers"].get(identifier)
            if not isinstance(record, Mapping):
                raise EnrollmentError("worker is not enrolled")
            updated = dict(record)
            updated["last_heartbeat_sequence"] = int(sequence)
            updated["last_heartbeat_at"] = utc_now()
            updated["updated_at"] = utc_now()
            workers = dict(state["workers"])
            workers[identifier] = updated
            self._save({"schema_version": ENROLLMENT_SCHEMA_VERSION, "workers": workers})

    def workers(self) -> tuple[Mapping[str, Any], ...]:
        """Return a deterministic projection of enrolled worker records."""

        try:
            with InterProcessFileLock(self.lock_path):
                state = self._load()
                return tuple(dict(state["workers"][key]) for key in sorted(state["workers"]))
        except (OSError, ValueError, TypeError, InterProcessLockError) as exc:
            raise EnrollmentError("enrollment registry is unavailable") from exc

    def verify(self, payload: Mapping[str, Any], proof: str) -> EnrollmentDecision:
        worker_id = str(payload.get("worker_id", "")).strip()
        runtime = payload.get("runtime_identity")
        runtime_fingerprint = runtime.get("runtime_fingerprint") if isinstance(runtime, Mapping) else None
        try:
            with InterProcessFileLock(self.lock_path):
                state = self._load()
                record = state["workers"].get(worker_id)
                if not isinstance(record, Mapping):
                    return EnrollmentDecision(False, "unknown_worker", worker_id, runtime_fingerprint)
                if bool(record.get("revoked")):
                    return EnrollmentDecision(False, "revoked_worker", worker_id, runtime_fingerprint)
                if payload.get("schema_version") != ENROLLMENT_SCHEMA_VERSION:
                    return EnrollmentDecision(False, "protocol_mismatch", worker_id, runtime_fingerprint)
                if payload.get("protocol_version") != ENROLLMENT_PROTOCOL_VERSION:
                    return EnrollmentDecision(False, "protocol_mismatch", worker_id, runtime_fingerprint)
                required = ("identity_id", "session_id", "instance_id", "platform")
                if any(not isinstance(payload.get(name), str) or not str(payload.get(name)).strip() for name in required):
                    return EnrollmentDecision(False, "malformed_enrollment", worker_id, runtime_fingerprint)
                if not isinstance(payload.get("capabilities"), Mapping):
                    return EnrollmentDecision(False, "malformed_enrollment", worker_id, runtime_fingerprint)
                if not isinstance(payload.get("available_slots"), int) or isinstance(payload.get("available_slots"), bool) or int(payload["available_slots"]) < 1:
                    return EnrollmentDecision(False, "malformed_enrollment", worker_id, runtime_fingerprint)
                expires_at = record.get("credential_expires_at")
                if expires_at is not None and _parse_expiry(str(expires_at)) <= datetime.now(timezone.utc):
                    return EnrollmentDecision(False, "expired_credential", worker_id, runtime_fingerprint)
                expected = enrollment_proof(self._secret, payload)
                if not isinstance(proof, str) or not hmac.compare_digest(expected, proof):
                    return EnrollmentDecision(False, "invalid_credential", worker_id, runtime_fingerprint)
                expected_runtime = record.get("runtime_fingerprint")
                if expected_runtime and expected_runtime != runtime_fingerprint:
                    return EnrollmentDecision(False, "runtime_mismatch", worker_id, runtime_fingerprint)
                bound_identity = record.get("identity_id")
                if bound_identity and bound_identity != payload.get("identity_id"):
                    return EnrollmentDecision(False, "duplicate_worker_identity", worker_id, runtime_fingerprint)
                if int(payload["available_slots"]) > max(1, int(record.get("slots", 1))):
                    return EnrollmentDecision(False, "capacity_mismatch", worker_id, runtime_fingerprint)
                return EnrollmentDecision(True, "accepted", worker_id, runtime_fingerprint)
        except (OSError, ValueError, TypeError, InterProcessLockError) as exc:
            raise EnrollmentError("enrollment registry is unavailable") from exc


def _parse_expiry(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("credential_expires_at must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError("credential_expires_at must include a timezone")
    return parsed.astimezone(timezone.utc)


__all__ = [
    "ENROLLMENT_SCHEMA_VERSION",
    "ENROLLMENT_PROTOCOL_VERSION",
    "EnrollmentAuthority",
    "EnrollmentDecision",
    "EnrollmentError",
    "WORKER_IDENTITY_SCHEMA_VERSION",
    "WorkerIdentity",
    "WorkerSessionIdentity",
    "enrollment_proof",
    "load_or_create_worker_identity",
]
