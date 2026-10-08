"""Persistent one-assignment worker agent with typed heartbeat and durable delivery."""
from __future__ import annotations
import platform
import sys
from dataclasses import dataclass
from enum import Enum
from threading import Event, RLock, Thread, current_thread
from typing import Any, Callable, Mapping
from theseus_contracts import (
    HeartbeatReceipt,
    ShardAssignment,
    WorkerCapabilities,
    WorkerHeartbeat,
    WorkerIdentity,
)
from theseus_contracts.serialization import utc_now
from test_intelligence_unified_v1.io_utils import stable_hash
from ..capabilities import detect_local_capabilities
from .spool import DurableExecutionSpool
class WorkerLeaseLost(RuntimeError):
    """Raised when the authoritative control plane rejects an agent heartbeat."""
class AgentState(str, Enum):
    """Durable lifecycle states visible to supervisor and recovery code."""
    IDLE = "idle"
    RUNNING = "running"
    DELIVERING = "delivering"
    STOPPED = "stopped"
    FAILED = "failed"
@dataclass(frozen=True, slots=True)
class AgentRun:
    """Delivery identity returned after evidence is durably spooled."""
    event_id: str
    payload_sha256: str
    payload: Mapping[str, Any]
    mutant_event_ids: tuple[str, ...] = ()
class WorkerAgent:
    """Long-lived worker boundary that can process one or more immutable assignments."""
    def __init__(
        self,
        *,
        identity: WorkerIdentity,
        capabilities: WorkerCapabilities,
        spool: DurableExecutionSpool,
        heartbeat_interval_seconds: float = 1.0,
    ) -> None:
        # Keep identity, capabilities and delivery state together for supervisor/recovery inspection.
        if heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be positive")
        self.identity = identity
        self.capabilities = capabilities
        self.spool = spool
        self.heartbeat_interval_seconds = float(heartbeat_interval_seconds)
        self._state = AgentState.IDLE
        self._state_lock = RLock()
        self._heartbeat_sequence = 0
        self._lease_lost = False
        self._stop_event = Event()
        self._heartbeat_thread: Thread | None = None
        self._heartbeat_errors: list[BaseException] = []
    @property
    def state(self) -> AgentState:
        # Return a lock-protected lifecycle snapshot for tests and supervisor polling.
        with self._state_lock:
            return self._state
    @property
    def lease_lost(self) -> bool:
        # Let the engine cancellation callback stop promptly after an authoritative heartbeat rejection.
        with self._state_lock:
            return self._lease_lost
    @property
    def heartbeat_active(self) -> bool:
        # Expose whether lease renewal is still alive through delivery and fan-in acknowledgement.
        thread = self._heartbeat_thread
        return thread is not None and thread.is_alive() and not self._stop_event.is_set()
    def _heartbeat_message(self, assignment: ShardAssignment, completed_mutants: int = 0) -> WorkerHeartbeat:
        # Build a monotonic typed heartbeat for the current immutable assignment.
        self._heartbeat_sequence += 1
        return WorkerHeartbeat(
            worker=self.identity,
            sequence=self._heartbeat_sequence,
            sent_at=utc_now(),
            current_campaign_id=assignment.campaign_id,
            current_shard_id=assignment.shard_id,
            current_lease_id=assignment.lease_id,
            current_attempt=assignment.attempt,
            completed_mutants=completed_mutants,
            workspace_healthy=True,
            child_process_id=self.identity.process_id,
            child_process_birth_token=self.identity.process_birth_token,
        )
    def _check_heartbeat(
        self,
        assignment: ShardAssignment,
        callback: Callable[[WorkerHeartbeat], HeartbeatReceipt],
    ) -> None:
        # Fail closed when the authoritative lease cannot be renewed or acknowledged.
        receipt = callback(self._heartbeat_message(assignment))
        if not receipt.accepted or not receipt.lease_valid or receipt.cancellation_requested:
            with self._state_lock:
                self._lease_lost = True
            raise WorkerLeaseLost(receipt.reason or "authoritative worker heartbeat rejected")
    def _stop_heartbeat(self) -> None:
        # Stop and join the assignment heartbeat without allowing a non-daemon thread leak.
        self._stop_event.set()
        thread = self._heartbeat_thread
        if thread is not None and thread is not current_thread() and thread.ident is not None:
            thread.join(timeout=max(2.0, self.heartbeat_interval_seconds * 4))
        if thread is not None and thread.is_alive():
            with self._state_lock:
                self._state = AgentState.FAILED
        self._heartbeat_thread = None
    def run_assignment(
        self,
        assignment: ShardAssignment,
        execute: Callable[[], Mapping[str, Any]],
        *,
        heartbeat: Callable[[WorkerHeartbeat], HeartbeatReceipt] | None = None,
        require_precommitted_mutants: bool = False,
    ) -> AgentRun:
        # Run one assignment and keep its heartbeat active until coordinator ACK or explicit stop.
        with self._state_lock:
            if self._state != AgentState.IDLE:
                raise RuntimeError(f"worker agent is not idle: {self._state.value}")
            if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
                raise RuntimeError("worker agent heartbeat from a previous assignment is still active")
            self._state = AgentState.RUNNING
            self._lease_lost = False
        self._stop_event = Event()
        self._heartbeat_errors = []
        def heartbeat_loop() -> None:
            # Keep the lease alive after engine completion until fan-in commits and acknowledges delivery.
            while not self._stop_event.wait(self.heartbeat_interval_seconds):
                try:
                    if heartbeat is not None:
                        self._check_heartbeat(assignment, heartbeat)
                except BaseException as exc:
                    self._heartbeat_errors.append(exc)
                    with self._state_lock:
                        self._lease_lost = True
                    self._stop_event.set()
                    return
        thread = Thread(
            target=heartbeat_loop,
            name=f"theseus-agent-heartbeat-{self.identity.worker_id}",
            daemon=False,
        )
        self._heartbeat_thread = thread
        try:
            if heartbeat is not None:
                self._check_heartbeat(assignment, heartbeat)
            thread.start()
            result = dict(execute())
            if self._heartbeat_errors:
                raise WorkerLeaseLost(str(self._heartbeat_errors[0]))
            event_id = stable_hash(
                {
                    "campaign_id": assignment.campaign_id,
                    "shard_id": assignment.shard_id,
                    "lease_id": assignment.lease_id,
                    "attempt": assignment.attempt,
                    "payload": result,
                }
            )[:32]
            payload = {
                "assignment": assignment.to_dict(),
                "worker": self.identity.to_dict(),
                "capabilities": self.capabilities.to_dict(),
                "result": result,
            }
            entry = self.spool.publish(event_id, payload)
            mutant_event_ids = self.spool.bind_mutant_events(
                entry.event_id,
                payload,
                allow_publish_missing=not require_precommitted_mutants,
            )
            with self._state_lock:
                self._state = AgentState.DELIVERING
            return AgentRun(entry.event_id, entry.payload_sha256, entry.payload, mutant_event_ids)
        except BaseException:
            self._stop_heartbeat()
            with self._state_lock:
                self._state = AgentState.FAILED
            raise
    def acknowledge(self, event_id: str) -> None:
        # ACK only after fan-in, then stop the lease heartbeat and allow a follow-on assignment.
        if self._heartbeat_errors or self.lease_lost:
            self._stop_heartbeat()
            with self._state_lock:
                self._state = AgentState.FAILED
            detail = str(self._heartbeat_errors[0]) if self._heartbeat_errors else "lease lost before acknowledgement"
            raise WorkerLeaseLost(detail)
        try:
            self.spool.acknowledge(event_id)
        except BaseException:
            self._stop_heartbeat()
            with self._state_lock:
                self._state = AgentState.FAILED
            raise
        self._stop_heartbeat()
        with self._state_lock:
            self._state = AgentState.IDLE
    def stop(self) -> None:
        # Stop lease renewal without deleting unacknowledged spool evidence needed for startup replay.
        self._stop_heartbeat()
        with self._state_lock:
            if self._state != AgentState.FAILED:
                self._state = AgentState.STOPPED
    @staticmethod
    def local_capabilities() -> WorkerCapabilities:
        # Advertise only capabilities that are directly observable by the current OS process.
        observed = detect_local_capabilities()
        return WorkerCapabilities(
            platform=sys.platform,
            architecture=platform.machine() or "unknown",
            python_versions=(platform.python_version(),),
            engine_protocol_versions=(1,),
            workspace_backends=observed.workspace_backends,
            cpu_count=max(1, int(__import__("os").cpu_count() or 1)),
        )
