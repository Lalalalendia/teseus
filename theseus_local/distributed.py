"""Durable at-least-once scheduler for one coordinator and N remote workers."""

from __future__ import annotations

import json
import os
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Mapping

from theseus_contracts import RemoteExecutionRequest, RemoteExecutionResult
from theseus_contracts.serialization import dumps, utc_now
from theseus_local.remote_worker import RemoteWorkerRegistration, RemoteWorkerState
from theseus_local.locking import InterProcessFileLock, InterProcessLockError
from theseus_local.runtime_identity import RuntimeCompatibilityError, RuntimeIdentity, assert_runtime_compatible


class SchedulerError(RuntimeError):
    """Raised when a scheduler operation would violate durable ownership."""


SCHEDULER_STATE_SCHEMA_VERSION = 1


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _future(seconds: float) -> str:
    return (_now() + timedelta(seconds=max(0.001, float(seconds)))).isoformat().replace("+00:00", "Z")


def _atomic_write(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(dumps(dict(payload)) + "\n", encoding="utf-8", newline="\n")
    os.replace(temporary, path)


@dataclass(frozen=True, slots=True)
class WorkerLease:
    lease_id: str
    worker_id: str
    worker_instance_id: str
    evidence_identity: str
    execution_attempt_id: str
    attempt: int
    expires_at: str
    request: RemoteExecutionRequest
    status: str = "active"

    def to_dict(self) -> dict[str, Any]:
        return {
            "lease_id": self.lease_id,
            "worker_id": self.worker_id,
            "worker_instance_id": self.worker_instance_id,
            "evidence_identity": self.evidence_identity,
            "execution_attempt_id": self.execution_attempt_id,
            "attempt": int(self.attempt),
            "expires_at": self.expires_at,
            "request": self.request.to_dict(),
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WorkerLease":
        raw_request = value.get("request")
        if not isinstance(raw_request, Mapping):
            raise SchedulerError("durable lease request is missing")
        return cls(
            lease_id=str(value.get("lease_id", "")),
            worker_id=str(value.get("worker_id", "")),
            worker_instance_id=str(value.get("worker_instance_id", "")),
            evidence_identity=str(value.get("evidence_identity", "")),
            execution_attempt_id=str(value.get("execution_attempt_id", "")),
            attempt=int(value.get("attempt", 0)),
            expires_at=str(value.get("expires_at", "")),
            request=RemoteExecutionRequest.from_dict(raw_request),
            status=str(value.get("status", "active")),
        )


@dataclass(frozen=True, slots=True)
class SchedulerCompletion:
    accepted: bool
    authoritative: bool
    reason: str
    result: RemoteExecutionResult | None = None


class DistributedScheduler:
    """A small durable scheduler with explicit leases and one result per evidence identity."""

    def __init__(
        self,
        state_path: Path,
        *,
        lease_seconds: float = 60.0,
        expected_runtime_identity: RuntimeIdentity | None = None,
    ) -> None:
        if float(lease_seconds) <= 0:
            raise ValueError("lease_seconds must be positive")
        self.state_path = Path(state_path).resolve()
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_path = self.state_path.with_name(f".{self.state_path.name}.lock")
        self.lease_seconds = float(lease_seconds)
        self.expected_runtime_identity = expected_runtime_identity
        self._lock = RLock()
        self._workers: dict[str, dict[str, Any]] = {}
        self._pending: dict[str, RemoteExecutionRequest] = {}
        self._leases: dict[str, WorkerLease] = {}
        self._attempts: dict[str, int] = {}
        self._authoritative: dict[str, RemoteExecutionResult] = {}
        self._stale: list[dict[str, Any]] = []
        try:
            # Initial reads must share the same lock as os.replace; otherwise a
            # second Windows process can keep the state file open while the
            # first process publishes its next snapshot.
            with self._lock, InterProcessFileLock(self._lock_path):
                self._load()
        except InterProcessLockError as exc:
            raise SchedulerError(str(exc)) from exc

    @contextmanager
    def _transaction(self):
        """Refresh and publish one scheduler state transition across processes."""

        try:
            with self._lock, InterProcessFileLock(self._lock_path):
                self._load()
                yield
                self._save()
        except InterProcessLockError as exc:
            raise SchedulerError(str(exc)) from exc

    @contextmanager
    def _read_state(self):
        """Read the latest durable state without publishing a second copy."""

        try:
            with self._lock, InterProcessFileLock(self._lock_path):
                self._load()
                yield
        except InterProcessLockError as exc:
            raise SchedulerError(str(exc)) from exc

    def _load(self) -> None:
        if not self.state_path.is_file():
            return
        try:
            raw = json.loads(self.state_path.read_text(encoding="utf-8"))
            if not isinstance(raw, Mapping):
                raise ValueError("scheduler state must be an object")
            if raw.get("schema_version") != SCHEDULER_STATE_SCHEMA_VERSION:
                raise ValueError(
                    f"unsupported scheduler state schema version: {raw.get('schema_version')!r}"
                )
            raw_runtime = raw.get("runtime_identity")
            if raw_runtime is not None:
                if not isinstance(raw_runtime, Mapping):
                    raise ValueError("scheduler runtime identity must be an object")
                persisted_runtime = RuntimeIdentity.from_dict(raw_runtime)
                if self.expected_runtime_identity is not None:
                    assert_runtime_compatible(self.expected_runtime_identity, persisted_runtime)
            sections: dict[str, Mapping[str, Any]] = {}
            for name in ("workers", "pending", "leases", "attempts", "authoritative"):
                section = raw.get(name, {})
                if not isinstance(section, Mapping):
                    raise ValueError(f"scheduler state section {name!r} must be an object")
                sections[name] = section
            raw_stale = raw.get("stale", [])
            if not isinstance(raw_stale, (list, tuple)):
                raise ValueError("scheduler stale records must be an array")
            self._workers = {
                str(key): dict(value)
                for key, value in sections["workers"].items()
                if isinstance(value, Mapping)
            }
            self._pending = {
                str(key): RemoteExecutionRequest.from_dict(value)
                for key, value in sections["pending"].items()
                if isinstance(value, Mapping)
            }
            self._leases = {
                str(key): WorkerLease.from_dict(value)
                for key, value in sections["leases"].items()
                if isinstance(value, Mapping)
            }
            self._attempts = {str(key): int(value) for key, value in sections["attempts"].items()}
            self._authoritative = {
                str(key): RemoteExecutionResult.from_dict(value)
                for key, value in sections["authoritative"].items()
                if isinstance(value, Mapping)
            }
            self._stale = [dict(item) for item in raw_stale if isinstance(item, Mapping)]
        except (OSError, ValueError, TypeError, RuntimeCompatibilityError) as exc:
            raise SchedulerError(f"scheduler state is corrupt: {self.state_path}") from exc

    def _save(self) -> None:
        _atomic_write(
            self.state_path,
            {
                "schema_version": SCHEDULER_STATE_SCHEMA_VERSION,
                "lease_seconds": self.lease_seconds,
                "runtime_identity": (
                    self.expected_runtime_identity.to_dict()
                    if self.expected_runtime_identity is not None
                    else None
                ),
                "workers": {key: value for key, value in sorted(self._workers.items())},
                "pending": {key: value.to_dict() for key, value in sorted(self._pending.items())},
                "leases": {key: value.to_dict() for key, value in sorted(self._leases.items())},
                "attempts": {key: int(value) for key, value in sorted(self._attempts.items())},
                "authoritative": {
                    key: value.to_dict() for key, value in sorted(self._authoritative.items())
                },
                "stale": list(self._stale[-1000:]),
            },
        )

    def register_worker(self, registration: RemoteWorkerRegistration | Mapping[str, Any]) -> None:
        """Register/reconnect a worker and replace only its capacity advertisement."""

        if isinstance(registration, RemoteWorkerRegistration):
            value = registration.to_dict()
        else:
            value = dict(registration)
        worker_id = str(value.get("worker_id", "")).strip()
        instance_id = str(value.get("instance_id", "")).strip()
        if not worker_id or not instance_id:
            raise SchedulerError("worker registration requires worker and instance identity")
        state = str(value.get("state", RemoteWorkerState.READY.value))
        if state not in {item.value for item in RemoteWorkerState}:
            raise SchedulerError(f"unknown worker state: {state}")
        raw_runtime = value.get("runtime_identity")
        if self.expected_runtime_identity is not None:
            if not isinstance(raw_runtime, Mapping):
                raise SchedulerError("worker registration has no runtime identity")
            try:
                assert_runtime_compatible(
                    self.expected_runtime_identity,
                    RuntimeIdentity.from_dict(raw_runtime),
                )
            except (RuntimeCompatibilityError, ValueError) as exc:
                raise SchedulerError(f"worker runtime is incompatible: {exc}") from exc
        slots = max(1, int(value.get("available_slots", 1)))
        with self._transaction():
            old = self._workers.get(worker_id)
            if old is not None and old.get("instance_id") != instance_id:
                self._requeue_worker_leases(worker_id, reason="worker_reconnected")
            self._workers[worker_id] = {
                "worker_id": worker_id,
                "instance_id": instance_id,
                "available_slots": slots,
                "state": state,
                "runtime_identity": dict(raw_runtime) if isinstance(raw_runtime, Mapping) else None,
                "registered_at": str(value.get("registered_at") or utc_now()),
            }

    def _requeue_worker_leases(self, worker_id: str, *, reason: str) -> None:
        for lease_id, lease in list(self._leases.items()):
            if lease.worker_id != worker_id or lease.status != "active":
                continue
            self._pending.setdefault(lease.evidence_identity, replace(lease.request, execution_attempt_id=lease.request.execution_attempt_id))
            self._stale.append({"lease_id": lease_id, "evidence_identity": lease.evidence_identity, "reason": reason})
            self._leases.pop(lease_id, None)

    def _release_worker_if_idle(self, worker_id: str) -> None:
        worker = self._workers.get(str(worker_id))
        if worker is None or worker.get("state") != RemoteWorkerState.BUSY.value:
            return
        if not any(
            lease.worker_id == str(worker_id) and lease.status == "active"
            for lease in self._leases.values()
        ):
            worker["state"] = RemoteWorkerState.READY.value

    def unregister_worker(self, worker_id: str, *, reason: str = "worker_disconnected") -> None:
        with self._transaction():
            self._requeue_worker_leases(str(worker_id), reason=reason)
            self._workers.pop(str(worker_id), None)

    def submit(self, request: RemoteExecutionRequest) -> bool:
        """Queue one evidence identity; duplicate delivery is idempotent, conflicts are rejected."""

        with self._transaction():
            if request.evidence_identity in self._authoritative:
                return False
            existing = self._pending.get(request.evidence_identity)
            if existing is not None:
                if not self._same_logical_request(existing, request):
                    raise SchedulerError("evidence identity already has a conflicting request")
                return False
            for lease in self._leases.values():
                if lease.evidence_identity == request.evidence_identity:
                    if not self._same_logical_request(lease.request, request):
                        raise SchedulerError("leased evidence identity has a conflicting request")
                    return False
            self._pending[request.evidence_identity] = request
            self._attempts.setdefault(request.evidence_identity, 0)
            return True

    @staticmethod
    def _same_logical_request(left: RemoteExecutionRequest, right: RemoteExecutionRequest) -> bool:
        """Compare semantic request bindings while ignoring a physical retry attempt id."""

        first = left.to_dict()
        second = right.to_dict()
        first.pop("execution_attempt_id", None)
        second.pop("execution_attempt_id", None)
        return first == second

    def expire_leases(self, *, now: datetime | None = None) -> tuple[str, ...]:
        """Requeue expired ownership without accepting any late result."""

        with self._transaction():
            return self._expire_leases_locked(now=now)

    def _expire_leases_locked(self, *, now: datetime | None = None) -> tuple[str, ...]:
        """Expire leases while the caller owns the scheduler transaction."""

        expired: list[str] = []
        current_time = now or _now()
        for lease_id, lease in list(self._leases.items()):
            if lease.status != "active" or _parse_time(lease.expires_at) > current_time:
                continue
            expired.append(lease_id)
            self._pending.setdefault(lease.evidence_identity, lease.request)
            self._stale.append({"lease_id": lease_id, "evidence_identity": lease.evidence_identity, "reason": "lease_expired"})
            self._leases.pop(lease_id, None)
            self._release_worker_if_idle(lease.worker_id)
        return tuple(expired)

    def claim(self, worker_id: str) -> WorkerLease | None:
        """Claim at most one queued request when the worker has capacity."""

        with self._transaction():
            self._expire_leases_locked()
            worker = self._workers.get(str(worker_id))
            if worker is None:
                raise SchedulerError("worker is not registered")
            if worker.get("state") not in {RemoteWorkerState.READY.value, RemoteWorkerState.BUSY.value}:
                return None
            active = sum(
                1
                for lease in self._leases.values()
                if lease.worker_id == str(worker_id) and lease.status == "active"
            )
            if active >= max(1, int(worker.get("available_slots", 1))):
                return None
            if not self._pending:
                return None
            evidence_identity = sorted(self._pending)[0]
            base_request = self._pending.pop(evidence_identity)
            attempt = int(self._attempts.get(evidence_identity, 0))
            execution_id = base_request.execution_attempt_id
            if attempt:
                execution_id = f"{execution_id}:retry-{attempt}"
            request = replace(base_request, execution_attempt_id=execution_id)
            lease = WorkerLease(
                lease_id=uuid.uuid4().hex,
                worker_id=str(worker_id),
                worker_instance_id=str(worker["instance_id"]),
                evidence_identity=evidence_identity,
                execution_attempt_id=execution_id,
                attempt=attempt,
                expires_at=_future(self.lease_seconds),
                request=request,
            )
            self._leases[lease.lease_id] = lease
            self._attempts[evidence_identity] = attempt + 1
            worker["state"] = RemoteWorkerState.BUSY.value
            return lease

    def claim_available(self, worker_id: str, *, limit: int | None = None) -> tuple[WorkerLease, ...]:
        """Bound dispatch to the advertised worker capacity and an optional caller limit."""

        leases: list[WorkerLease] = []
        bound = None if limit is None else max(0, int(limit))
        while bound is None or len(leases) < bound:
            lease = self.claim(worker_id)
            if lease is None:
                break
            leases.append(lease)
        return tuple(leases)

    def heartbeat(self, worker_id: str, lease_id: str, *, worker_instance_id: str) -> bool:
        with self._transaction():
            self._expire_leases_locked()
            lease = self._leases.get(str(lease_id))
            if lease is None or lease.status != "active":
                return False
            if lease.worker_id != str(worker_id) or lease.worker_instance_id != str(worker_instance_id):
                return False
            if _parse_time(lease.expires_at) <= _now():
                self._expire_leases_locked()
                return False
            self._leases[lease.lease_id] = replace(lease, expires_at=_future(self.lease_seconds))
            return True

    def reject(self, worker_id: str, lease_id: str, *, reason: str = "worker_rejected") -> bool:
        """Return one still-owned lease to the queue without inventing a result."""

        with self._transaction():
            lease = self._leases.get(str(lease_id))
            if lease is None or lease.worker_id != str(worker_id) or lease.status != "active":
                return False
            self._leases.pop(str(lease_id), None)
            self._pending.setdefault(lease.evidence_identity, lease.request)
            self._stale.append({"lease_id": str(lease_id), "evidence_identity": lease.evidence_identity, "reason": str(reason)})
            self._release_worker_if_idle(lease.worker_id)
            return True

    def complete(
        self,
        worker_id: str,
        lease_id: str,
        result: RemoteExecutionResult,
    ) -> SchedulerCompletion:
        """Accept one result only while lease ownership is current and evidence is still open."""

        with self._transaction():
            lease = self._leases.get(str(lease_id))
            if lease is None:
                self._stale.append({"lease_id": str(lease_id), "reason": "unknown_or_late_lease"})
                return SchedulerCompletion(False, False, "unknown_or_late_lease")
            if lease.worker_id != str(worker_id) or _parse_time(lease.expires_at) <= _now():
                if _parse_time(lease.expires_at) <= _now():
                    self._expire_leases_locked()
                self._stale.append({"lease_id": lease.lease_id, "reason": "stale_lease"})
                return SchedulerCompletion(False, False, "stale_lease")
            if (
                result.execution_attempt_id != lease.execution_attempt_id
                or result.evidence_identity != lease.evidence_identity
                or result.mutation_identity != lease.request.mutation_identity
            ):
                self._stale.append({"lease_id": lease.lease_id, "reason": "result_identity_mismatch"})
                return SchedulerCompletion(False, False, "result_identity_mismatch")
            registered = self._workers.get(lease.worker_id, {})
            registered_runtime = registered.get("runtime_identity")
            if isinstance(registered_runtime, Mapping) and str(
                registered_runtime.get("runtime_fingerprint", "")
            ) != result.worker_runtime_fingerprint:
                self._stale.append({"lease_id": lease.lease_id, "reason": "result_runtime_mismatch"})
                return SchedulerCompletion(False, False, "result_runtime_mismatch")
            if result.prepared_artifact_sha256 != lease.request.prepared_artifact_id:
                self._stale.append({"lease_id": lease.lease_id, "reason": "result_artifact_mismatch"})
                return SchedulerCompletion(False, False, "result_artifact_mismatch")
            if result.source_sha256 != lease.request.expected_source_sha256:
                self._stale.append({"lease_id": lease.lease_id, "reason": "result_source_mismatch"})
                return SchedulerCompletion(False, False, "result_source_mismatch")
            existing = self._authoritative.get(result.evidence_identity)
            if existing is not None:
                self._leases.pop(lease.lease_id, None)
                self._stale.append({"lease_id": lease.lease_id, "reason": "duplicate_authoritative_evidence"})
                return SchedulerCompletion(False, False, "duplicate_authoritative_evidence", existing)
            self._authoritative[result.evidence_identity] = result
            self._leases.pop(lease.lease_id, None)
            worker = self._workers.get(lease.worker_id)
            if worker is not None:
                worker["state"] = (
                    RemoteWorkerState.BUSY.value
                    if any(
                        item.worker_id == lease.worker_id and item.status == "active"
                        for item in self._leases.values()
                    )
                    else RemoteWorkerState.READY.value
                )
            return SchedulerCompletion(True, True, "accepted", result)

    def cancel(self, evidence_identity: str, *, reason: str = "cancelled") -> bool:
        with self._transaction():
            removed = self._pending.pop(str(evidence_identity), None) is not None
            for lease_id, lease in list(self._leases.items()):
                if lease.evidence_identity == str(evidence_identity):
                    self._leases.pop(lease_id, None)
                    self._stale.append({"lease_id": lease_id, "reason": reason})
                    self._release_worker_if_idle(lease.worker_id)
                    removed = True
            return removed

    def authoritative(self, evidence_identity: str) -> RemoteExecutionResult | None:
        with self._read_state():
            return self._authoritative.get(str(evidence_identity))

    def pending_count(self) -> int:
        with self._read_state():
            return len(self._pending)

    def active_lease_count(self) -> int:
        with self._read_state():
            return sum(1 for item in self._leases.values() if item.status == "active")

    def lease_is_active(self, lease_id: str) -> bool:
        """Return current durable ownership for one lease without extending it."""

        with self._read_state():
            lease = self._leases.get(str(lease_id))
            return lease is not None and lease.status == "active"

    def stale_records(self) -> tuple[Mapping[str, Any], ...]:
        with self._read_state():
            return tuple(dict(item) for item in self._stale)

    def snapshot(self) -> dict[str, Any]:
        with self._read_state():
            return {
                "workers": {key: dict(value) for key, value in sorted(self._workers.items())},
                "pending": tuple(sorted(self._pending)),
                "leases": tuple(item.to_dict() for item in self._leases.values()),
                "authoritative": tuple(sorted(self._authoritative)),
                "stale_count": len(self._stale),
            }


__all__ = ["DistributedScheduler", "SchedulerCompletion", "SchedulerError", "WorkerLease"]
