"""Real-socket coordinator/worker control plane built on the PR55 contracts."""

from __future__ import annotations

import os
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from theseus_contracts import RemoteExecutionRequest, RemoteExecutionResult, WorkerCapabilities
from theseus_contracts.serialization import utc_now
from theseus_local.artifact_store import ContentAddressedArtifactStore
from theseus_local.distributed import DistributedScheduler, SchedulerError, WorkerLease
from theseus_local.remote_campaign import project_semantic_outcomes
from theseus_local.remote_worker import RemoteWorkerRegistration, RemoteWorkerRuntime, RemoteWorkerState
from theseus_local.runtime_identity import RuntimeIdentity, current_runtime_identity

from .coordinator_ha import CoordinatorAuthority, LeadershipError
from .distributed_observability import CorrelationIds, DistributedMetrics, DistributedObservability
from .network_artifacts import NetworkArtifactError, NetworkArtifactSender, NetworkArtifactTransferStats
from .network_security import (
    ENROLLMENT_PROTOCOL_VERSION,
    ENROLLMENT_SCHEMA_VERSION,
    EnrollmentAuthority,
    EnrollmentError,
    WorkerSessionIdentity,
    enrollment_proof,
    load_or_create_worker_identity,
)
from .network_transport import (
    FramedSocket,
    TcpTransportServer,
    TransportClosed,
    TransportError,
    TransportFramingError,
    TransportTimeout,
    connect_tcp,
)


NETWORK_CONTROL_SCHEMA_VERSION = 1
NETWORK_ARTIFACT_TRANSFER_TIMEOUT_SECONDS = 30.0
NETWORK_MIN_WORKER_LEASE_SECONDS = 5.0


class NetworkControlError(RuntimeError):
    """Raised for network control-plane admission, dispatch, or lifecycle failures."""


def _merge_transfer_stats(
    previous: NetworkArtifactTransferStats | None,
    current: NetworkArtifactTransferStats,
) -> NetworkArtifactTransferStats:
    if previous is None:
        return current
    return NetworkArtifactTransferStats(
        artifact_ids=tuple(dict.fromkeys((*previous.artifact_ids, *current.artifact_ids))),
        bytes_transferred=previous.bytes_transferred + current.bytes_transferred,
        cache_hits=previous.cache_hits + current.cache_hits,
        cache_misses=previous.cache_misses + current.cache_misses,
        chunks=previous.chunks + current.chunks,
        round_trips=previous.round_trips + current.round_trips,
        transfer_seconds=previous.transfer_seconds + current.transfer_seconds,
    )


def _subtract_transfer_stats(
    current: NetworkArtifactTransferStats,
    previous: NetworkArtifactTransferStats | None,
) -> NetworkArtifactTransferStats:
    if previous is None:
        return current
    return NetworkArtifactTransferStats(
        artifact_ids=current.artifact_ids,
        bytes_transferred=max(0, current.bytes_transferred - previous.bytes_transferred),
        cache_hits=max(0, current.cache_hits - previous.cache_hits),
        cache_misses=max(0, current.cache_misses - previous.cache_misses),
        chunks=max(0, current.chunks - previous.chunks),
        round_trips=max(0, current.round_trips - previous.round_trips),
        transfer_seconds=max(0.0, current.transfer_seconds - previous.transfer_seconds),
    )


def _parse_deadline(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.timestamp()
    except ValueError as exc:
        raise NetworkControlError("request deadline is malformed") from exc


def _registration_from_dict(value: Mapping[str, Any]) -> RemoteWorkerRegistration:
    runtime = value.get("runtime_identity")
    capabilities = value.get("capabilities")
    if not isinstance(runtime, Mapping) or not isinstance(capabilities, Mapping):
        raise NetworkControlError("worker registration is missing runtime/capability objects")
    try:
        state = RemoteWorkerState(str(value.get("state", RemoteWorkerState.READY.value)))
        return RemoteWorkerRegistration(
            worker_id=str(value["worker_id"]),
            instance_id=str(value["instance_id"]),
            runtime_identity=RuntimeIdentity.from_dict(runtime),
            capabilities=WorkerCapabilities.from_dict(capabilities),
            platform=str(value["platform"]),
            execution_backends=tuple(str(item) for item in value.get("execution_backends", [])),
            available_slots=max(1, int(value.get("available_slots", 1))),
            state=state,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise NetworkControlError("worker registration is malformed") from exc


@dataclass(frozen=True, slots=True)
class NetworkRunReport:
    results: tuple[RemoteExecutionResult, ...]
    semantic_outcomes: Mapping[str, str]
    scheduler_snapshot: Mapping[str, Any]
    wall_seconds: float
    leader_epoch: int | None
    metrics: DistributedMetrics
    transfer_stats: Mapping[str, NetworkArtifactTransferStats]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": NETWORK_CONTROL_SCHEMA_VERSION,
            "results": [item.to_dict() for item in self.results],
            "semantic_outcomes": dict(sorted(self.semantic_outcomes.items())),
            "scheduler": dict(self.scheduler_snapshot),
            "wall_seconds": float(self.wall_seconds),
            "leader_epoch": self.leader_epoch,
            "metrics": self.metrics.to_dict(),
            "transfer_stats": {
                key: {
                    "artifact_ids": list(value.artifact_ids),
                    "bytes_transferred": int(value.bytes_transferred),
                    "cache_hits": int(value.cache_hits),
                    "cache_misses": int(value.cache_misses),
                    "chunks": int(value.chunks),
                    "cache_hit_rate": value.cache_hit_rate,
                    "round_trips": int(value.round_trips),
                    "transfer_seconds": float(value.transfer_seconds),
                }
                for key, value in sorted(self.transfer_stats.items())
            },
        }


@dataclass
class _NetworkSession:
    connection: FramedSocket
    registration: RemoteWorkerRegistration
    identity_id: str
    session: WorkerSessionIdentity
    enrollment_payload: Mapping[str, Any]
    proof: str
    last_heartbeat_sequence: int = -1
    last_heartbeat_at: str | None = None
    connected: bool = True
    transfer_stats: NetworkArtifactTransferStats | None = None
    pinned_artifacts: dict[str, tuple[str, ...]] = field(default_factory=dict)
    dispatch_times: dict[str, float] = field(default_factory=dict)
    deferred_messages: deque[Any] = field(default_factory=deque)
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    @property
    def worker_id(self) -> str:
        return self.registration.worker_id


class NetworkCoordinator:
    """Single-authority network coordinator using one dispatch loop per worker host."""

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        state_path: Path,
        artifact_store: ContentAddressedArtifactStore,
        enrollment: EnrollmentAuthority,
        ha_state_path: Path | None = None,
        coordinator_id: str | None = None,
        lease_seconds: float = 60.0,
        observability: DistributedObservability | None = None,
        runtime_identity: RuntimeIdentity | None = None,
    ) -> None:
        self.host = str(host)
        self.port = int(port)
        self.artifact_store = artifact_store
        self.enrollment = enrollment
        self.runtime_identity = runtime_identity or current_runtime_identity()
        # A coordinator HA lease and a worker execution lease are different
        # authorities.  Keep the latter long enough for artifact delivery and
        # a fresh-process timeout even when HA tests intentionally use a very
        # short fencing lease.
        worker_lease_seconds = max(NETWORK_MIN_WORKER_LEASE_SECONDS, float(lease_seconds))
        self.scheduler = DistributedScheduler(
            Path(state_path),
            lease_seconds=worker_lease_seconds,
            expected_runtime_identity=self.runtime_identity,
        )
        self.coordinator_id = str(coordinator_id or f"coordinator-{uuid.uuid4().hex[:12]}")
        self.authority = CoordinatorAuthority(ha_state_path, lease_seconds=lease_seconds) if ha_state_path else None
        self.observability = observability or DistributedObservability()
        self.server = TcpTransportServer(self.host, self.port, self._handle_connection)
        self._leader_epoch: int | None = None
        self._campaign_id = "network-control-plane"
        self._stop = threading.Event()
        self._authority_thread: threading.Thread | None = None
        self._sessions: dict[str, _NetworkSession] = {}
        self._sessions_lock = threading.RLock()
        self._result_lock = threading.RLock()
        self._transfer_history: dict[str, NetworkArtifactTransferStats] = {}
        self._cancelled_evidence: dict[str, str] = {}

    @property
    def address(self) -> tuple[str, int]:
        return self.server.address

    @property
    def leader_epoch(self) -> int | None:
        return self._leader_epoch

    def start(self) -> tuple[str, int]:
        if self.authority is not None:
            try:
                self._leader_epoch = self.authority.acquire(self.coordinator_id).epoch
            except LeadershipError as exc:
                raise NetworkControlError(str(exc)) from exc
        self._stop.clear()
        address = self.server.start()
        if self.authority is not None:
            self._authority_thread = threading.Thread(
                target=self._renew_authority_loop,
                name=f"theseus-coordinator-authority-{self.coordinator_id}",
                daemon=True,
            )
            self._authority_thread.start()
        self.observability.record(
            "coordinator.started",
            CorrelationIds(campaign_id="network-control-plane"),
            {"host": address[0], "port": address[1], "leader_epoch": self._leader_epoch},
        )
        return address

    def _renew_authority_loop(self) -> None:
        if self.authority is None:
            return
        interval = max(0.05, self.authority.lease_seconds / 3.0)
        while not self._stop.wait(interval):
            epoch = self._leader_epoch
            if epoch is None:
                return
            try:
                self.authority.renew(self.coordinator_id, epoch)
            except LeadershipError as exc:
                self.observability.record(
                    "coordinator.fenced",
                    CorrelationIds("network-control-plane"),
                    {"error": str(exc), "leader_epoch": epoch},
                )
                self._stop.set()
                self.server.stop()
                return

    def _assert_leader(self) -> None:
        if self.authority is not None:
            if self._leader_epoch is None:
                raise NetworkControlError("coordinator has not acquired leadership")
            try:
                self.authority.assert_leader(self.coordinator_id, self._leader_epoch)
            except LeadershipError as exc:
                raise NetworkControlError(str(exc)) from exc

    def _payload_for_registration(self, session_payload: Mapping[str, Any]) -> Mapping[str, Any]:
        return session_payload

    def _handle_connection(self, connection: FramedSocket, address: tuple[str, int]) -> None:
        session: _NetworkSession | None = None
        try:
            first = connection.receive_message(deadline=time.monotonic() + 10.0)
            if first.message_type != "worker.enroll":
                connection.send_message("worker.enrollment_rejected", {"reason": "enrollment_required"}, reply_to=first.message_id)
                return
            payload = first.payload
            proof = payload.get("proof")
            enrollment_payload = payload.get("enrollment")
            registration_payload = payload.get("registration")
            if not isinstance(proof, str) or not isinstance(enrollment_payload, Mapping) or not isinstance(registration_payload, Mapping):
                connection.send_message("worker.enrollment_rejected", {"reason": "malformed_enrollment"}, reply_to=first.message_id)
                return
            decision = self.enrollment.verify(enrollment_payload, proof)
            if not decision.accepted:
                connection.send_message(
                    "worker.enrollment_rejected",
                    {"reason": decision.reason, "worker_id": decision.worker_id},
                    reply_to=first.message_id,
                )
                return
            registration = _registration_from_dict(registration_payload)
            worker_id = registration.worker_id
            identity_id = str(enrollment_payload.get("identity_id", ""))
            session_id = str(enrollment_payload.get("session_id", ""))
            if (
                worker_id != decision.worker_id
                or not identity_id
                or not session_id
                or str(enrollment_payload.get("instance_id", "")) != registration.instance_id
                or int(enrollment_payload.get("protocol_version", -1)) != ENROLLMENT_PROTOCOL_VERSION
                or int(enrollment_payload.get("schema_version", -1)) != ENROLLMENT_SCHEMA_VERSION
                or dict(enrollment_payload.get("runtime_identity", {})) != registration.runtime_identity.to_dict()
                or dict(enrollment_payload.get("capabilities", {})) != registration.capabilities.to_dict()
                or str(enrollment_payload.get("platform", "")) != registration.platform
                or tuple(enrollment_payload.get("execution_backends", ())) != registration.execution_backends
                or int(enrollment_payload.get("available_slots", 0)) != registration.available_slots
            ):
                raise NetworkControlError("enrollment identity does not match registration")
            new_session = WorkerSessionIdentity(worker_id, identity_id, session_id, os.getpid(), str(enrollment_payload.get("started_at", "")))
            with self._sessions_lock:
                old = self._sessions.get(worker_id)
                if old is not None and old.connected and old.session.session_id == session_id:
                    connection.send_message("worker.enrollment_rejected", {"reason": "duplicate_session"}, reply_to=first.message_id)
                    return
                if old is not None:
                    old.connected = False
                    old.connection.close()
                session = _NetworkSession(
                    connection,
                    registration,
                    identity_id,
                    new_session,
                    dict(enrollment_payload),
                    proof,
                    transfer_stats=self._transfer_history.get(worker_id),
                )
                self._sessions[worker_id] = session
            self._assert_leader()
            self.enrollment.record_session(worker_id, enrollment_payload)
            self.scheduler.register_worker(registration)
            connection.send_message(
                "worker.enrollment_accepted",
                {
                    "worker_id": worker_id,
                    "session_id": session_id,
                    "leader_epoch": self._leader_epoch,
                    "protocol_version": 1,
                    # Keep execution leases alive even when an operator chooses
                    # a short HA lease for failover tests or aggressive fencing.
                    "heartbeat_seconds": max(0.02, min(1.0, self.scheduler.lease_seconds / 3.0)),
                },
                reply_to=first.message_id,
            )
            self.observability.record(
                "worker.enrolled",
                CorrelationIds("network-control-plane", worker_id=worker_id, worker_session_id=session_id),
                {"address": f"{address[0]}:{address[1]}", "slots": registration.available_slots},
            )
            self._serve_session(session)
        except (TransportError, NetworkControlError, EnrollmentError, SchedulerError, ValueError, TypeError, OSError) as exc:
            self.observability.record(
                "worker.session_error",
                CorrelationIds(
                    "network-control-plane",
                    worker_id=session.worker_id if session is not None else None,
                    worker_session_id=session.session.session_id if session is not None else None,
                ),
                {"error": f"{type(exc).__name__}: {exc}"},
            )
            return
        finally:
            if session is not None:
                session.connected = False
                self._release_session_artifacts(session)
                was_current = False
                with self._sessions_lock:
                    if self._sessions.get(session.worker_id) is session:
                        self._sessions.pop(session.worker_id, None)
                        was_current = True
                if was_current:
                    try:
                        self.scheduler.unregister_worker(session.worker_id, reason="worker_disconnected")
                    except Exception:
                        pass

    def _expired_result(self, lease: WorkerLease) -> RemoteExecutionResult:
        return RemoteExecutionResult(
            execution_attempt_id=lease.execution_attempt_id,
            evidence_identity=lease.evidence_identity,
            mutation_identity=lease.request.mutation_identity,
            started=False,
            exit_code=None,
            timed_out=True,
            cancelled=False,
            elapsed_seconds=0.0,
            worker_runtime_fingerprint=lease.request.runtime_identity.get("runtime_fingerprint", "expired")
            if isinstance(lease.request.runtime_identity, Mapping)
            else "expired",
            workspace_integrity="not_started",
            source_sha256=lease.request.expected_source_sha256,
            prepared_artifact_sha256=lease.request.prepared_artifact_id,
            diagnostic_error="deadline expired before network dispatch",
        )

    def _send_assignment(self, session: _NetworkSession, lease: WorkerLease) -> NetworkArtifactTransferStats:
        # Artifact delivery is a control-plane phase and must not inherit a
        # deliberately short execution budget such as ``timeout_seconds=0.1``.
        # Keep it bounded independently, while preserving a small per-request
        # grace period for ordinary campaigns.
        transfer_budget = min(
            NETWORK_ARTIFACT_TRANSFER_TIMEOUT_SECONDS,
            max(5.0, float(lease.request.timeout_seconds) + 5.0),
        )
        deadline = time.monotonic() + transfer_budget
        snapshot = self.artifact_store.load_snapshot(lease.request.project_snapshot_id)
        artifact_ids = tuple(dict.fromkeys((
            lease.request.project_snapshot_id,
            *tuple(snapshot.files.values()),
            lease.request.prepared_artifact_id,
        )))
        pinned: list[str] = []
        transfer_started = time.perf_counter()
        try:
            stats = NetworkArtifactSender(
                session.connection,
                self.artifact_store,
                on_message=lambda message: self._handle_transfer_message(session, message),
            ).transfer(
                artifact_ids,
                deadline=deadline,
            )
            self._assert_leader()
            session.connection.send_message(
                "execution.assignment",
                {
                    "lease_id": lease.lease_id,
                    "attempt": lease.attempt,
                    "leader_epoch": self._leader_epoch,
                    "request": lease.request.to_dict(),
                },
                deadline=deadline,
            )
            with session.lock:
                session.pinned_artifacts[lease.lease_id] = tuple(pinned)
                session.dispatch_times[lease.lease_id] = time.perf_counter()
            self.observability.observe_timing("artifact_transfer_seconds", time.perf_counter() - transfer_started)
            self.observability.record(
                "artifact.transfer",
                CorrelationIds(
                    self._campaign_id,
                    evidence_identity=lease.evidence_identity,
                    attempt_identity=lease.execution_attempt_id,
                    worker_id=session.worker_id,
                    worker_session_id=session.session.session_id,
                    lease_id=lease.lease_id,
                ),
                {
                    "bytes_transferred": stats.bytes_transferred,
                    "cache_hits": stats.cache_hits,
                    "cache_misses": stats.cache_misses,
                    "chunks": stats.chunks,
                },
            )
            return stats
        except Exception:
            self._release_pinned_artifacts(pinned)
            raise

    def _release_pinned_artifacts(self, artifact_ids: Sequence[str]) -> None:
        if not artifact_ids:
            return
        try:
            self.artifact_store.release_many(artifact_ids)
            return
        except Exception:
            pass
        for artifact_id in artifact_ids:
            try:
                self.artifact_store.release(artifact_id)
            except Exception:
                # The CAS remains the authority; cleanup must not turn a
                # failed dispatch into an accepted execution result.
                pass

    def _release_lease_artifacts(self, session: _NetworkSession, lease_id: str) -> None:
        with session.lock:
            artifact_ids = session.pinned_artifacts.pop(str(lease_id), ())
            session.dispatch_times.pop(str(lease_id), None)
        self._release_pinned_artifacts(artifact_ids)

    def _release_session_artifacts(self, session: _NetworkSession) -> None:
        with session.lock:
            groups = tuple(session.pinned_artifacts.values())
            session.pinned_artifacts.clear()
            session.dispatch_times.clear()
        for artifact_ids in groups:
            self._release_pinned_artifacts(artifact_ids)

    def _handle_transfer_message(self, session: _NetworkSession, message: Any) -> bool:
        """Process asynchronous worker control frames while CAS is in flight."""

        if message.message_type == "worker.heartbeat":
            self._handle_heartbeat(session, message)
            return True
        if message.message_type == "execution.result":
            with session.lock:
                session.deferred_messages.append(message)
            return True
        return False

    def _handle_heartbeat(self, session: _NetworkSession, message: Any) -> None:
        self._assert_leader()
        sequence = message.payload.get("sequence")
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence <= session.last_heartbeat_sequence:
            session.connection.send_message(
                "worker.heartbeat_rejected",
                {"reason": "heartbeat_sequence_stale", "sequence": session.last_heartbeat_sequence},
                reply_to=message.message_id,
            )
            return
        decision = self.enrollment.verify(session.enrollment_payload, session.proof)
        if not decision.accepted:
            session.connection.send_message("worker.revoked", {"reason": decision.reason}, reply_to=message.message_id)
            session.connected = False
            return
        session.last_heartbeat_sequence = sequence
        session.last_heartbeat_at = utc_now()
        renewed = 0
        raw_leases = message.payload.get("leases", [])
        if isinstance(raw_leases, list):
            for raw_lease in raw_leases:
                lease_id = raw_lease.get("lease_id") if isinstance(raw_lease, Mapping) else raw_lease
                if not isinstance(lease_id, str) or not lease_id:
                    continue
                if self.scheduler.heartbeat(
                    session.worker_id,
                    lease_id,
                    worker_instance_id=session.registration.instance_id,
                ):
                    renewed += 1
        try:
            self.enrollment.record_heartbeat(session.worker_id, sequence=sequence)
        except EnrollmentError:
            # The diagnostic registry is non-authoritative; the durable scheduler
            # and this authenticated session remain the source of lease truth.
            pass
        session.connection.send_message(
            "worker.heartbeat_receipt",
            {"accepted": True, "lease_valid": True, "renewed_leases": renewed, "leader_epoch": self._leader_epoch},
            reply_to=message.message_id,
        )
        self.observability.record(
            "worker.heartbeat",
            CorrelationIds("network-control-plane", worker_id=session.worker_id, worker_session_id=session.session.session_id),
            {"sequence": sequence},
        )

    def _serve_session(self, session: _NetworkSession) -> None:
        inflight: dict[str, WorkerLease] = {}
        capacity = max(1, int(session.registration.available_slots))
        while session.connected and not self._stop.is_set():
            self._assert_leader()
            self.scheduler.expire_leases()
            for lease_id, lease in tuple(inflight.items()):
                if not self.scheduler.lease_is_active(lease_id):
                    self._release_lease_artifacts(session, lease_id)
                    inflight.pop(lease_id, None)
                    self.observability.record(
                        "lease.expired",
                        CorrelationIds(
                            "network-control-plane",
                            evidence_identity=lease.evidence_identity,
                            attempt_identity=lease.execution_attempt_id,
                            worker_id=session.worker_id,
                            worker_session_id=session.session.session_id,
                            lease_id=lease_id,
                        ),
                    )
            while len(inflight) < capacity:
                lease = self.scheduler.claim(session.worker_id)
                if lease is None:
                    break
                request_deadline = _parse_deadline(lease.request.deadline_utc)
                if request_deadline is not None and request_deadline <= datetime.now(timezone.utc).timestamp():
                    self.scheduler.complete(session.worker_id, lease.lease_id, self._expired_result(lease))
                    self.observability.record(
                        "lease.expired_before_dispatch",
                        CorrelationIds("network-control-plane", evidence_identity=lease.evidence_identity, attempt_identity=lease.execution_attempt_id, worker_id=session.worker_id, lease_id=lease.lease_id),
                    )
                    continue
                try:
                    session.transfer_stats = _merge_transfer_stats(
                        session.transfer_stats,
                        self._send_assignment(session, lease),
                    )
                    with self._sessions_lock:
                        self._transfer_history[session.worker_id] = session.transfer_stats
                    inflight[lease.lease_id] = lease
                    self.observability.record(
                        "assignment.sent",
                        CorrelationIds(
                            self._campaign_id,
                            evidence_identity=lease.evidence_identity,
                            attempt_identity=lease.execution_attempt_id,
                            worker_id=session.worker_id,
                            worker_session_id=session.session.session_id,
                            lease_id=lease.lease_id,
                        ),
                    )
                except (NetworkArtifactError, TransportError, OSError):
                    session.connected = False
                    break
            if not session.connected:
                break
            try:
                with session.lock:
                    message = session.deferred_messages.popleft() if session.deferred_messages else None
                if message is None:
                    message = session.connection.receive_message(deadline=time.monotonic() + 0.1)
            except TransportTimeout:
                self.scheduler.expire_leases()
                continue
            except TransportClosed:
                session.connected = False
                break
            if message.message_type == "worker.heartbeat":
                self._handle_heartbeat(session, message)
                continue
            if message.message_type == "execution.result":
                self._assert_leader()
                payload = message.payload
                raw_result = payload.get("result")
                lease_id = payload.get("lease_id")
                lease = inflight.get(str(lease_id)) if isinstance(lease_id, str) else None
                if not isinstance(raw_result, Mapping) or lease is None:
                    self.observability.record("result.stale", CorrelationIds(self._campaign_id, worker_id=session.worker_id), {"reason": "lease_binding"})
                    continue
                if payload.get("leader_epoch") != self._leader_epoch:
                    self.observability.record("result.stale", CorrelationIds(self._campaign_id, worker_id=session.worker_id, lease_id=str(lease_id)), {"reason": "leader_epoch"})
                    session.connected = False
                    break
                result = RemoteExecutionResult.from_dict(raw_result)
                with session.lock:
                    dispatched_at = session.dispatch_times.get(str(lease_id))
                if dispatched_at is not None:
                    self.observability.observe_timing("result_round_trip_seconds", time.perf_counter() - dispatched_at)
                    self.observability.observe_timing("worker_execution_seconds", result.elapsed_seconds)
                with self._result_lock:
                    completion = self.scheduler.complete(session.worker_id, str(lease_id), result)
                    self._release_lease_artifacts(session, str(lease_id))
                    self.observability.record(
                        "result.accepted" if completion.accepted else "result.stale",
                        CorrelationIds(self._campaign_id, evidence_identity=result.evidence_identity, attempt_identity=result.execution_attempt_id, worker_id=session.worker_id, worker_session_id=session.session.session_id, lease_id=str(lease_id)),
                        {"reason": completion.reason},
                    )
                inflight.pop(str(lease_id), None)
                continue
            if message.message_type == "execution.rejected":
                lease_id = str(message.payload.get("lease_id", ""))
                lease = inflight.pop(lease_id, None)
                if lease is not None:
                    self.scheduler.reject(session.worker_id, lease_id, reason=str(message.payload.get("reason", "worker_rejected")))
                    self._release_lease_artifacts(session, lease_id)
                    self.observability.record(
                        "assignment.rejected",
                        CorrelationIds(self._campaign_id, evidence_identity=lease.evidence_identity, attempt_identity=lease.execution_attempt_id, worker_id=session.worker_id, lease_id=lease_id),
                        {"reason": str(message.payload.get("reason", "worker_rejected"))},
                    )
                continue
            if message.message_type == "worker.shutdown":
                session.connected = False
                break

    def wait_for_workers(self, count: int = 1, *, timeout_seconds: float = 10.0) -> tuple[str, ...]:
        deadline = time.monotonic() + max(0.0, float(timeout_seconds))
        while time.monotonic() < deadline:
            with self._sessions_lock:
                workers = tuple(sorted(self._sessions))
            if len(workers) >= int(count):
                return workers
            time.sleep(0.01)
        with self._sessions_lock:
            return tuple(sorted(self._sessions))

    def cancel(self, evidence_identity: str, *, reason: str = "cancelled") -> bool:
        """Request cancellation and remove only the matching durable work item."""

        target = str(evidence_identity)
        snapshot = self.scheduler.snapshot()
        for raw_lease in snapshot.get("leases", ()):
            if not isinstance(raw_lease, Mapping) or raw_lease.get("evidence_identity") != target:
                continue
            worker_id = str(raw_lease.get("worker_id", ""))
            with self._sessions_lock:
                session = self._sessions.get(worker_id)
            if session is not None and session.connected:
                try:
                    session.connection.send_message(
                        "execution.cancel",
                        {
                            "evidence_identity": target,
                            "execution_attempt_id": str(raw_lease.get("execution_attempt_id", "")),
                            "lease_id": str(raw_lease.get("lease_id", "")),
                            "reason": str(reason),
                        },
                    )
                except TransportError:
                    session.connected = False
        removed = self.scheduler.cancel(target, reason=reason)
        if removed:
            self._cancelled_evidence[target] = str(reason)
            self.observability.record(
                "campaign.cancel_requested",
                CorrelationIds(self._campaign_id, evidence_identity=target),
                {"reason": str(reason)},
            )
        return removed

    @staticmethod
    def _cancelled_result(request: RemoteExecutionRequest, reason: str) -> RemoteExecutionResult:
        runtime_fingerprint = (
            str(request.runtime_identity.get("runtime_fingerprint", "cancelled"))
            if isinstance(request.runtime_identity, Mapping)
            else "cancelled"
        )
        return RemoteExecutionResult(
            execution_attempt_id=request.execution_attempt_id,
            evidence_identity=request.evidence_identity,
            mutation_identity=request.mutation_identity,
            started=False,
            exit_code=None,
            timed_out=False,
            cancelled=True,
            elapsed_seconds=0.0,
            worker_runtime_fingerprint=runtime_fingerprint,
            workspace_integrity="not_started",
            source_sha256=request.expected_source_sha256,
            prepared_artifact_sha256=request.prepared_artifact_id,
            diagnostic_error=f"network execution cancelled: {reason}",
        )

    def run(
        self,
        requests: Sequence[RemoteExecutionRequest],
        *,
        timeout_seconds: float = 120.0,
        coordinator_statuses: Mapping[str, str] | None = None,
        campaign_id: str | None = None,
    ) -> NetworkRunReport:
        self._assert_leader()
        self._campaign_id = str(campaign_id or f"network-campaign-{uuid.uuid4().hex[:12]}")
        with self._sessions_lock:
            transfer_baseline = dict(self._transfer_history)
        started = time.perf_counter()
        for request in requests:
            self.scheduler.submit(request)
            self.observability.record(
                "request.queued",
                CorrelationIds(
                    self._campaign_id,
                    evidence_identity=request.evidence_identity,
                    attempt_identity=request.execution_attempt_id,
                ),
            )
        deadline = time.monotonic() + max(0.1, float(timeout_seconds))
        while time.monotonic() < deadline:
            self._assert_leader()
            self.scheduler.expire_leases()
            if self.scheduler.pending_count() == 0 and self.scheduler.active_lease_count() == 0:
                with self._result_lock:
                    pass
                break
            time.sleep(0.01)
        if self.scheduler.pending_count() or self.scheduler.active_lease_count():
            raise NetworkControlError("network campaign deadline expired before all evidence became authoritative")
        ordered_rows: list[RemoteExecutionResult] = []
        for request in sorted(requests, key=lambda item: item.evidence_identity):
            result = self.scheduler.authoritative(request.evidence_identity)
            if result is None and request.evidence_identity in self._cancelled_evidence:
                result = self._cancelled_result(request, self._cancelled_evidence[request.evidence_identity])
            if result is not None:
                ordered_rows.append(result)
        ordered = tuple(ordered_rows)
        with self._sessions_lock:
            transfers = {
                worker_id: _subtract_transfer_stats(stats, transfer_baseline.get(worker_id))
                for worker_id, stats in self._transfer_history.items()
                if stats is not None
            }
        bytes_transferred = sum(item.bytes_transferred for item in transfers.values())
        cache_hits = sum(item.cache_hits for item in transfers.values())
        cache_misses = sum(item.cache_misses for item in transfers.values())
        round_trips = sum(item.round_trips for item in transfers.values())
        transfer_wall_seconds = sum(item.transfer_seconds for item in transfers.values())
        snapshot = self.scheduler.snapshot()
        stale = self.scheduler.stale_records()
        metrics = self.observability.snapshot(
            bytes_transferred=bytes_transferred,
            round_trips=round_trips,
            transfer_wall_seconds=transfer_wall_seconds,
            cache_hits=cache_hits,
            cache_misses=cache_misses,
            active_workers=len(snapshot["workers"]),
            available_slots=sum(int(item.get("available_slots", 0)) for item in snapshot["workers"].values()),
            active_leases=len(snapshot["leases"]),
            stale_results=len(stale),
            retries=sum(
                1
                for item in stale
                if str(item.get("reason", "")) in {"lease_expired", "worker_reconnected", "worker_disconnected", "worker_crash"}
            ),
        )
        return NetworkRunReport(
            results=ordered,
            semantic_outcomes=project_semantic_outcomes(ordered, coordinator_statuses=coordinator_statuses),
            scheduler_snapshot=snapshot,
            wall_seconds=max(0.0, time.perf_counter() - started),
            leader_epoch=self._leader_epoch,
            metrics=metrics,
            transfer_stats=transfers,
        )

    def _sessions_snapshot(self) -> dict[str, _NetworkSession]:
        with self._sessions_lock:
            return dict(self._sessions)

    def sessions(self) -> tuple[Mapping[str, Any], ...]:
        with self._sessions_lock:
            return tuple(
                {
                    "worker_id": session.worker_id,
                    "identity_id": session.identity_id,
                    "session_id": session.session.session_id,
                    "connected": bool(session.connected),
                    "last_heartbeat_sequence": int(session.last_heartbeat_sequence),
                    "last_heartbeat_at": session.last_heartbeat_at,
                    "available_slots": int(session.registration.available_slots),
                    "runtime_fingerprint": session.registration.runtime_identity.runtime_fingerprint,
                    "capabilities": session.registration.capabilities.to_dict(),
                    "platform": session.registration.platform,
                    "state": session.registration.state.value,
                }
                for session in sorted(self._sessions.values(), key=lambda item: item.worker_id)
            )

    def stop(self) -> None:
        self._stop.set()
        self.server.stop()
        if self._authority_thread is not None and self._authority_thread.is_alive():
            self._authority_thread.join(timeout=1.0)
        self._authority_thread = None
        with self._sessions_lock:
            sessions = tuple(self._sessions.values())
            self._sessions.clear()
        for session in sessions:
            session.connected = False
            self._release_session_artifacts(session)
            session.connection.close()
        if self.authority is not None and self._leader_epoch is not None:
            try:
                self.authority.release(self.coordinator_id, self._leader_epoch)
            except LeadershipError:
                pass
        self.observability.record("coordinator.stopped", CorrelationIds("network-control-plane"))

    def __enter__(self) -> "NetworkCoordinator":
        self.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.stop()


class NetworkWorkerAgent:
    """Worker-side socket agent that reuses RemoteWorkerRuntime for every attempt."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        shared_secret: str | bytes,
        worker_id: str,
        root: Path,
        slots: int = 1,
        runtime: RemoteWorkerRuntime | None = None,
        connect_timeout_seconds: float = 10.0,
        heartbeat_seconds: float = 0.5,
        reconnect_seconds: float = 0.25,
    ) -> None:
        self.host = str(host)
        self.port = int(port)
        self.shared_secret = shared_secret
        self.worker_id = str(worker_id)
        self.root = Path(root).resolve()
        self.runtime = runtime or RemoteWorkerRuntime(worker_id=self.worker_id, root=self.root, slots=slots)
        self.identity = load_or_create_worker_identity(self.root, self.worker_id)
        self.slots = max(1, int(slots))
        self.connect_timeout_seconds = max(0.1, float(connect_timeout_seconds))
        self.heartbeat_seconds = max(0.05, float(heartbeat_seconds))
        self._negotiated_heartbeat_seconds = self.heartbeat_seconds
        self.reconnect_seconds = max(0.05, float(reconnect_seconds))
        self.session = WorkerSessionIdentity(self.worker_id, self.identity.identity_id, uuid.uuid4().hex, os.getpid(), datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
        self.connection: FramedSocket | None = None
        self._stop = threading.Event()
        self._threads: set[threading.Thread] = set()
        self._threads_lock = threading.RLock()
        self._active: dict[str, RemoteExecutionRequest] = {}
        self._active_leases: dict[str, str] = {}
        self._active_lock = threading.RLock()
        self._leader_epoch: int | None = None
        self._artifact_receiver: Any = None
        self._session_stop = threading.Event()
        self._has_connected = False

    def _enrollment_payload(self) -> dict[str, Any]:
        registration = self.runtime.registration()
        return {
            "schema_version": ENROLLMENT_SCHEMA_VERSION,
            "protocol_version": ENROLLMENT_PROTOCOL_VERSION,
            "worker_id": self.worker_id,
            "identity_id": self.identity.identity_id,
            "instance_id": registration.instance_id,
            "session_id": self.session.session_id,
            "started_at": self.session.started_at,
            "runtime_identity": registration.runtime_identity.to_dict(),
            "capabilities": registration.capabilities.to_dict(),
            "platform": registration.platform,
            "execution_backends": list(registration.execution_backends),
            "available_slots": int(self.slots),
        }

    def _connect(self) -> None:
        if self._has_connected:
            with self._active_lock:
                for attempt_id in tuple(self._active):
                    self.runtime.cancel(attempt_id)
            self.runtime = self.runtime.restart()
        self.session = WorkerSessionIdentity(
            self.worker_id,
            self.identity.identity_id,
            uuid.uuid4().hex,
            os.getpid(),
            datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        )
        connection = connect_tcp(self.host, self.port, timeout_seconds=self.connect_timeout_seconds)
        self.connection = connection
        enrollment = self._enrollment_payload()
        request_id = connection.send_message(
            "worker.enroll",
            {
                "enrollment": enrollment,
                "proof": enrollment_proof(self.shared_secret, enrollment),
                "registration": self.runtime.registration().to_dict(),
            },
        )
        message = connection.receive_message(deadline=time.monotonic() + self.connect_timeout_seconds)
        if message.reply_to != request_id or message.message_type != "worker.enrollment_accepted":
            reason = message.payload.get("reason") if isinstance(message.payload, Mapping) else "rejected"
            connection.close()
            raise NetworkControlError(f"worker enrollment rejected: {reason}")
        self._leader_epoch = int(message.payload["leader_epoch"]) if message.payload.get("leader_epoch") is not None else None
        negotiated = message.payload.get("heartbeat_seconds")
        if isinstance(negotiated, (int, float)) and not isinstance(negotiated, bool) and float(negotiated) > 0:
            self._negotiated_heartbeat_seconds = max(0.02, min(self.heartbeat_seconds, float(negotiated)))
        else:
            self._negotiated_heartbeat_seconds = self.heartbeat_seconds
        from .network_artifacts import NetworkArtifactReceiver

        self._artifact_receiver = NetworkArtifactReceiver(connection, self.runtime.store)
        self._has_connected = True

    def _heartbeat_loop(self, session_stop: threading.Event) -> None:
        sequence = 0
        first = True
        while not self._stop.is_set():
            if not first and session_stop.wait(self._negotiated_heartbeat_seconds):
                return
            first = False
            connection = self.connection
            if connection is None:
                return
            try:
                connection.send_message(
                    "worker.heartbeat",
                    {
                        "worker_id": self.worker_id,
                        "identity_id": self.identity.identity_id,
                        "session_id": self.session.session_id,
                        "sequence": sequence,
                        "leases": [
                            {"lease_id": lease_id, "execution_attempt_id": attempt_id}
                            for attempt_id, lease_id in tuple(self._active_leases.items())
                        ],
                    },
                )
                sequence += 1
            except TransportError:
                session_stop.set()
                return

    def _execute_assignment(
        self,
        request: RemoteExecutionRequest,
        lease_id: str,
        attempt: int,
        leader_epoch: int | None,
        connection: FramedSocket,
        session_stop: threading.Event,
        runtime: RemoteWorkerRuntime,
    ) -> None:
        try:
            result = runtime.execute(request)
            if self.connection is connection and not self._stop.is_set() and not session_stop.is_set():
                connection.send_message(
                    "execution.result",
                    {"lease_id": lease_id, "attempt": int(attempt), "leader_epoch": leader_epoch, "result": result.to_dict()},
                )
        except Exception:
            # A worker failure is a disconnect/retry signal, never an invented semantic result.
            session_stop.set()
        finally:
            with self._active_lock:
                self._active.pop(request.execution_attempt_id, None)
                self._active_leases.pop(request.execution_attempt_id, None)
            with self._threads_lock:
                self._threads.discard(threading.current_thread())

    def _run_session(self, external_stop: threading.Event | None) -> None:
        session_stop = threading.Event()
        self._session_stop = session_stop
        self._connect()
        heartbeat = threading.Thread(
            target=self._heartbeat_loop,
            args=(session_stop,),
            name=f"theseus-heartbeat-{self.worker_id}",
            daemon=True,
        )
        heartbeat.start()
        try:
            while not self._stop.is_set() and not session_stop.is_set() and not (external_stop and external_stop.is_set()):
                connection = self.connection
                if connection is None:
                    session_stop.set()
                    break
                try:
                    message = connection.receive_message(deadline=time.monotonic() + 0.2)
                except TransportTimeout:
                    continue
                if message.message_type.startswith("artifact."):
                    if self._artifact_receiver is None or not self._artifact_receiver.handle(message):
                        raise NetworkControlError("unknown artifact control message")
                    continue
                if message.message_type == "execution.assignment":
                    payload = message.payload
                    raw_request = payload.get("request")
                    if not isinstance(raw_request, Mapping):
                        raise NetworkControlError("assignment request is malformed")
                    request = RemoteExecutionRequest.from_dict(raw_request)
                    lease_id = str(payload.get("lease_id", ""))
                    if not lease_id:
                        raise NetworkControlError("assignment lease_id is missing")
                    with self._active_lock:
                        if len(self._active) >= self.slots:
                            connection.send_message("execution.rejected", {"lease_id": lease_id, "reason": "capacity"}, reply_to=message.message_id)
                            continue
                        self._active[request.execution_attempt_id] = request
                        self._active_leases[request.execution_attempt_id] = lease_id
                    thread = threading.Thread(
                        target=self._execute_assignment,
                        args=(request, lease_id, int(payload.get("attempt", 0)), payload.get("leader_epoch"), connection, session_stop, self.runtime),
                        name=f"theseus-execution-{self.worker_id}",
                        daemon=True,
                    )
                    with self._threads_lock:
                        self._threads.add(thread)
                    thread.start()
                    continue
                if message.message_type == "execution.cancel":
                    attempt_id = str(message.payload.get("execution_attempt_id", ""))
                    if attempt_id:
                        self.runtime.cancel(attempt_id)
                    continue
                if message.message_type in {"worker.revoked", "worker.shutdown"}:
                    self._stop.set()
                    break
                if message.message_type in {"worker.heartbeat_receipt", "worker.heartbeat_rejected"}:
                    continue
                raise NetworkControlError(f"unknown network worker message: {message.message_type}")
        except (TransportClosed, TransportFramingError, TransportError, OSError):
            session_stop.set()
        except (NetworkControlError, ValueError):
            # A protocol/admission violation is not safe to retry blindly.
            self._stop.set()
        finally:
            session_stop.set()
            with self._active_lock:
                active_attempts = tuple(self._active)
            for attempt_id in active_attempts:
                self.runtime.cancel(attempt_id)
            if self._artifact_receiver is not None:
                self._artifact_receiver.close()
            if self.connection is not None:
                self.connection.close()
            self.connection = None
            if heartbeat.is_alive():
                heartbeat.join(timeout=1.0)
            with self._threads_lock:
                threads = tuple(self._threads)
            for thread in threads:
                if thread.is_alive():
                    thread.join(timeout=1.0)

    def run(self, *, stop_event: threading.Event | None = None) -> None:
        self._stop.clear()
        while not self._stop.is_set() and not (stop_event and stop_event.is_set()):
            try:
                self._run_session(stop_event)
            except NetworkControlError:
                # Enrollment/protocol rejection is an explicit operator action,
                # not a transient network outage.
                self._stop.set()
            except (TransportClosed, TransportFramingError, TransportError, OSError):
                # A coordinator restart or dropped TCP session is reconnectable.
                pass
            if self._stop.is_set() or (stop_event and stop_event.is_set()):
                break
            if stop_event is not None:
                stop_event.wait(self.reconnect_seconds)
            else:
                time.sleep(self.reconnect_seconds)

    def start_background(self) -> tuple[threading.Thread, threading.Event]:
        stop = threading.Event()
        thread = threading.Thread(target=self.run, kwargs={"stop_event": stop}, name=f"theseus-worker-{self.worker_id}", daemon=True)
        thread.start()
        return thread, stop


__all__ = [
    "NETWORK_CONTROL_SCHEMA_VERSION",
    "NetworkCoordinator",
    "NetworkControlError",
    "NetworkRunReport",
    "NetworkWorkerAgent",
]
