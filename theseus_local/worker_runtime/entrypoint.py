"""Standalone persistent worker process for the local Theseus runtime."""
from __future__ import annotations
import argparse
import hashlib
import os
import shutil
import sys
import time
import uuid
from dataclasses import replace
from enum import Enum
from pathlib import Path
from threading import Event, RLock, Thread
from typing import Any, Mapping, TextIO
from theseus_contracts import (
    HOST_TO_WORKER_MESSAGE_TYPES,
    AcquireAssignment,
    AssignShard,
    EngineLifecycle,
    ExecutionAcknowledged,
    ExecutionEnvelope,
    ExecutionDelivery,
    ExecutionId,
    ExecutionReceipt,
    HeartbeatReceipt,
    MutantExecutionResult,
    NoAssignment,
    PreparedCampaign,
    RegisterWorker,
    RegisterWorkerReceipt,
    ShardAssignment,
    ShardExecutionResult,
    ShardId,
    WorkerHeartbeat,
    WorkerHeartbeatFrame,
    WorkerHeartbeatReceipt,
    WorkerExecutionMode,
    WorkerExecutionSpec,
    WorkerId,
    WorkerIdentity,
    WorkerMessageType,
    WorkerStatus,
    WorkerProtocolError,
    WorkerProtocolFrame,
    WorkerTerminated,
    decode_worker_frame,
    deterministic_id,
    encode_worker_frame,
)
from theseus_contracts.serialization import utc_now
from test_intelligence_unified_v1.recovery import current_process_birth_token
from test_intelligence_unified_v1.test_stats import canonical_nodeid
from ..process import EngineProcessError, EngineProcessSession
from ..runtime_identity import current_runtime_identity
from .agent import AgentRun, WorkerAgent
from .recovery import matching_pending_delivery
from .spool import DurableExecutionSpool
class WorkerProcessState(str, Enum):
    """Lifecycle states owned by the standalone worker process."""
    STARTING = "starting"
    REGISTERED = "registered"
    IDLE = "idle"
    CLAIMING = "claiming"
    RUNNING = "running"
    DELIVERING = "delivering"
    DRAINING = "draining"
    SHUTTING_DOWN = "shutting_down"
    TERMINATED = "terminated"
    FAILED = "failed"
class PersistentWorkerEntrypoint:
    """Run one worker identity across repeated assignments in a dedicated OS process."""
    def __init__(
        self,
        *,
        worker_id: str,
        spool_root: Path,
        instance_id: str | None = None,
        heartbeat_interval_seconds: float = 1.0,
        input_stream: TextIO | None = None,
        output_stream: TextIO | None = None,
    ) -> None:
        # Bind process identity, durable spool and typed control streams for the worker lifetime.
        if not worker_id.strip():
            raise ValueError("worker_id must be non-empty")
        if heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be positive")
        process_id = os.getpid()
        birth_token = current_process_birth_token(process_id) or f"pid-{process_id}-{time.time_ns()}"
        self.identity = WorkerIdentity(
            worker_id,
            str(instance_id or uuid.uuid4().hex),
            process_id,
            birth_token,
        )
        self.agent = WorkerAgent(
            identity=self.identity,
            capabilities=WorkerAgent.local_capabilities(),
            spool=DurableExecutionSpool(Path(spool_root)),
            heartbeat_interval_seconds=float(heartbeat_interval_seconds),
        )
        self.heartbeat_interval_seconds = float(heartbeat_interval_seconds)
        self.input_stream = input_stream or sys.stdin
        self.output_stream = output_stream or sys.stdout
        self._state = WorkerProcessState.STARTING
        self._state_lock = RLock()
        self._write_lock = RLock()
        self._sequence = 0
        self._heartbeat_sequence = 0
        self._pending_heartbeat_request_ids: set[str] = set()
        self._last_host_sequence = -1
        self._shutdown = Event()
        self._heartbeat_thread: Thread | None = None
        self._current_assignment: ShardAssignment | None = None
        self._current_correlation_id: str | None = None
        self._engine_session: EngineProcessSession | None = None
        self._engine_child_pid: int | None = None
        self._completed_assignments = 0
    @property
    def state(self) -> WorkerProcessState:
        # Return a lock-protected process lifecycle state for heartbeat and diagnostics.
        with self._state_lock:
            return self._state
    def _set_state(self, state: WorkerProcessState) -> None:
        # Move the private process lifecycle without exposing mutable state to the host.
        with self._state_lock:
            self._state = state
    def _emit(
        self,
        message_type: WorkerMessageType,
        payload: Any,
        *,
        correlation_id: str | None = None,
        request_id: str | None = None,
    ) -> WorkerProtocolFrame:
        # Write one complete typed frame under a lock so heartbeat and lifecycle events cannot interleave.
        with self._write_lock:
            self._sequence += 1
            frame = WorkerProtocolFrame.create(
                message_type,
                payload,
                worker_id=self.identity.worker_id,
                instance_id=self.identity.instance_id,
                process_id=self.identity.process_id,
                sequence=self._sequence,
                state=self.state.value,
                correlation_id=correlation_id,
                request_id=request_id,
            )
            if message_type == WorkerMessageType.WORKER_HEARTBEAT:
                with self._state_lock:
                    self._pending_heartbeat_request_ids.add(frame.message_id)
            self.output_stream.write(encode_worker_frame(frame) + "\n")
            self.output_stream.flush()
            return frame
    def _read_frame(self) -> WorkerProtocolFrame | None:
        # Decode one host-to-worker frame and verify immutable worker process identity before routing.
        raw = self.input_stream.readline()
        if raw == "":
            return None
        frame = decode_worker_frame(raw, allowed_types=HOST_TO_WORKER_MESSAGE_TYPES)
        frame.require_identity(
            worker_id=self.identity.worker_id,
            instance_id=self.identity.instance_id,
            process_id=self.identity.process_id,
        )
        if frame.sequence <= self._last_host_sequence:
            raise ValueError("host worker-protocol sequence did not advance")
        self._last_host_sequence = frame.sequence
        return frame
    def _read_control_frame(
        self,
        *,
        expected_request_id: str | None = None,
    ) -> WorkerProtocolFrame | None:
        # Consume asynchronous heartbeat receipts while preserving request identity for the awaited command.
        while True:
            frame = self._read_frame()
            if frame is None:
                return None
            if frame.message_type == WorkerMessageType.HEARTBEAT_RECEIPT:
                receipt_payload = frame.payload
                if not isinstance(receipt_payload, WorkerHeartbeatReceipt):
                    raise TypeError("heartbeat receipt payload is not typed")
                with self._state_lock:
                    if frame.request_id not in self._pending_heartbeat_request_ids:
                        raise ValueError("heartbeat receipt request_id is not pending")
                    self._pending_heartbeat_request_ids.discard(frame.request_id)
                receipt = receipt_payload.receipt
                if not receipt.accepted or not receipt.lease_valid or receipt.cancellation_requested:
                    raise RuntimeError(receipt.reason or "worker heartbeat rejected")
                continue
            if (
                expected_request_id is not None
                and frame.message_type not in {
                    WorkerMessageType.CANCEL_ASSIGNMENT,
                    WorkerMessageType.DRAIN_WORKER,
                    WorkerMessageType.SHUTDOWN_WORKER,
                }
                and frame.request_id != expected_request_id
            ):
                raise ValueError(
                    f"worker response request_id mismatch: expected {expected_request_id}, received {frame.request_id}"
                )
            return frame
    def _heartbeat_message(self) -> WorkerHeartbeat:
        # Build process-level liveness that includes the engine child currently owned by this agent.
        with self._state_lock:
            assignment = self._current_assignment
            child_process_id = self._engine_child_pid
            self._heartbeat_sequence += 1
            sequence = self._heartbeat_sequence
        return WorkerHeartbeat(
            worker=self.identity,
            sequence=sequence,
            sent_at=utc_now(),
            current_campaign_id=assignment.campaign_id if assignment is not None else None,
            current_shard_id=assignment.shard_id if assignment is not None else None,
            current_lease_id=assignment.lease_id if assignment is not None else None,
            current_attempt=assignment.attempt if assignment is not None else None,
            child_process_id=child_process_id,
            child_process_birth_token=(
                current_process_birth_token(child_process_id)
                if child_process_id is not None
                else None
            ),
            completed_mutants=0,
            workspace_healthy=True,
        )
    def _heartbeat_loop(self) -> None:
        # Publish worker-process liveness independently from any one assignment lifecycle.
        while not self._shutdown.wait(self.heartbeat_interval_seconds):
            self._emit(
                WorkerMessageType.WORKER_HEARTBEAT,
                WorkerHeartbeatFrame(
                    self._heartbeat_message(),
                    self._completed_assignments,
                ),
                correlation_id=self._current_correlation_id,
            )
    def _start_heartbeat(self) -> None:
        # Start exactly one non-daemon process heartbeat and retain it for clean shutdown.
        thread = Thread(
            target=self._heartbeat_loop,
            name=f"theseus-worker-process-heartbeat-{self.identity.worker_id}",
            daemon=False,
        )
        self._heartbeat_thread = thread
        thread.start()
    def _stop_heartbeat(self) -> None:
        # Stop and join the process heartbeat before emitting the terminal lifecycle event.
        self._shutdown.set()
        thread = self._heartbeat_thread
        if thread is not None:
            thread.join(timeout=max(2.0, self.heartbeat_interval_seconds * 4))
        if thread is not None and thread.is_alive():
            raise RuntimeError("worker process heartbeat did not stop")
        self._heartbeat_thread = None
    def _assignment_heartbeat(self, message: WorkerHeartbeat) -> HeartbeatReceipt:
        # Keep WorkerAgent fail-closed semantics while the parent renews Gallifrey from process heartbeats.
        del message
        if self._shutdown.is_set():
            return HeartbeatReceipt(False, False, cancellation_requested=True, reason="worker_shutting_down")
        return HeartbeatReceipt(True, True, lease_revision=self._heartbeat_sequence)
    @staticmethod
    def _attach_test_fingerprints(
        result: ShardExecutionResult,
        raw_fingerprints: Mapping[str, Any],
        workspace: Path,
    ) -> ShardExecutionResult:
        # Bind engine pytest observations to immutable fingerprints before the worker spools delivery.
        annotated = []
        for execution in result.results:
            raw_mutant_fingerprints = raw_fingerprints.get(execution.mutant_id.value, {})
            if not isinstance(raw_mutant_fingerprints, Mapping):
                raise RuntimeError(f"test fingerprints are missing for mutant {execution.mutant_id.value}")
            observations: list[dict[str, Any]] = []
            for item in execution.test_observations:
                observation = dict(item)
                test_id = canonical_nodeid(workspace, str(observation.get("test_id", "")))
                fingerprint = raw_mutant_fingerprints.get(test_id)
                if not test_id or not fingerprint:
                    raise RuntimeError(f"test fingerprint is missing for observed test {test_id or '<unknown>'}")
                observation["test_id"] = test_id
                observation["test_fingerprint"] = str(fingerprint)
                observations.append(observation)
            annotated.append(replace(execution, test_observations=tuple(observations)))
        return replace(result, results=tuple(annotated))
    @staticmethod
    def _file_sha256(path: Path) -> str:
        # Hash one artifact through bounded reads so conflict checks do not load reports into memory.
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
    @classmethod
    def _publish_file_atomic(cls, source: Path, destination: Path) -> None:
        # Publish one immutable worker artifact without exposing a partially copied canonical file.
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.is_file():
            same_size = source.stat().st_size == destination.stat().st_size
            if same_size and cls._file_sha256(source) == cls._file_sha256(destination):
                return
            raise RuntimeError(f"canonical engine artifact conflicts with worker evidence: {destination}")
        temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")
        try:
            with source.open("rb") as reader, temporary.open("xb") as writer:
                shutil.copyfileobj(reader, writer, length=1024 * 1024)
                writer.flush()
                os.fsync(writer.fileno())
            try:
                os.link(temporary, destination)
            except FileExistsError:
                same_size = source.stat().st_size == destination.stat().st_size
                if not same_size or cls._file_sha256(source) != cls._file_sha256(destination):
                    raise RuntimeError(
                        f"canonical engine artifact conflicts with worker evidence: {destination}"
                    )
            except OSError:
                if destination.exists():
                    same_size = source.stat().st_size == destination.stat().st_size
                    if not same_size or cls._file_sha256(source) != cls._file_sha256(destination):
                        raise RuntimeError(
                            f"canonical engine artifact conflicts with worker evidence: {destination}"
                        )
                else:
                    os.replace(temporary, destination)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
    @classmethod
    def _publish_engine_run_artifacts(
        cls,
        source_root: Path,
        destination_root: Path,
        run_id: str,
    ) -> tuple[str, ...]:
        # Mirror only one completed shard run into the canonical campaign artifact directory.
        if not source_root.is_dir():
            raise RuntimeError(f"worker engine artifact directory is missing: {source_root}")
        published: list[str] = []
        candidates = [item for item in source_root.glob(f"{run_id}*") if item.is_file()]
        for directory_name in ("recovery", "test_stats_events"):
            directory = source_root / directory_name / run_id
            if directory.is_dir():
                candidates.extend(item for item in directory.rglob("*") if item.is_file())
        for source in sorted(candidates, key=lambda item: item.as_posix()):
            relative = source.relative_to(source_root)
            cls._publish_file_atomic(source, destination_root / relative)
            published.append(relative.as_posix())
        if not any(Path(item).name == f"{run_id}.json" for item in published):
            raise RuntimeError(f"worker engine report was not materialized for {run_id}")
        return tuple(published)
    def _engine_cancelled(self, cancel_path: Path | None) -> bool:
        # Stop the engine child when the worker shuts down or the campaign cancellation marker appears.
        return self._shutdown.is_set() or (cancel_path is not None and cancel_path.is_file())
    @staticmethod
    def _retryable_prepare_transport_failure(error: EngineProcessError) -> bool:
        # Retry only a pre-execution transport loss, never an application or snapshot validation failure.
        message = str(error).lower()
        return any(
            marker in message
            for marker in (
                "exited before prepare response",
                "transport failed during prepare",
                "broken pipe",
            )
        )
    def _announce_engine_started(
        self,
        process: EngineProcessSession,
        assignment: ShardAssignment,
        *,
        correlation_id: str | None,
    ) -> int:
        # Publish one concrete child identity before the worker waits for its prepare response.
        process.start()
        child_process_id = process.pid
        if child_process_id is None:
            raise RuntimeError("worker-owned engine did not expose a child PID")
        with self._state_lock:
            self._engine_child_pid = int(child_process_id)
        self._emit(
            WorkerMessageType.ENGINE_STARTED,
            EngineLifecycle(
                int(child_process_id),
                assignment.campaign_id,
                assignment.shard_id,
            ),
            correlation_id=correlation_id,
        )
        return int(child_process_id)
    def _close_engine_attempt(
        self,
        process: EngineProcessSession,
        child_process_id: int,
        assignment: ShardAssignment,
        *,
        correlation_id: str | None,
    ) -> str | None:
        # Close one engine attempt, clear ownership, and return any cleanup failure as bounded diagnostics.
        close_error: str | None = None
        try:
            process.close()
        except BaseException as exc:
            close_error = f"{type(exc).__name__}: {exc}"
        finally:
            with self._state_lock:
                if self._engine_child_pid == int(child_process_id):
                    self._engine_child_pid = None
            if self._engine_session is process:
                self._engine_session = None
            self._emit(
                WorkerMessageType.ENGINE_STOPPED,
                EngineLifecycle(
                    int(child_process_id),
                    assignment.campaign_id,
                    assignment.shard_id,
                ),
                correlation_id=correlation_id,
            )
        return close_error
    def _carry_forward_mutant_results(
        self,
        assignment: ShardAssignment,
        *,
        source_sha256: str,
        mutant_ids: tuple[str, ...],
    ) -> dict[str, MutantExecutionResult]:
        # Rebind restored prior-attempt evidence to the current lease before fresh execution starts.
        carried: dict[str, MutantExecutionResult] = {}
        events = self.agent.spool.salvage_mutant_events(
            campaign_id=assignment.campaign_id,
            shard_id=assignment.shard_id,
            current_attempt=assignment.attempt,
            source_sha256=source_sha256,
            mutant_ids=mutant_ids,
        )
        for frame in events:
            raw_payload = frame.get("payload")
            raw_result = raw_payload.get("mutant_result") if isinstance(raw_payload, Mapping) else None
            if not isinstance(raw_result, Mapping):
                raise RuntimeError("salvage mutant event has no typed result payload")
            previous = MutantExecutionResult.from_dict(raw_result)
            current = replace(
                previous,
                execution_id=ExecutionId(
                    deterministic_id(
                        "exec",
                        assignment.campaign_id,
                        previous.mutant_id.value,
                        assignment.attempt,
                    )
                ),
                lease_id=assignment.lease_id,
                attempt=assignment.attempt,
            )
            current_event_id = self.agent.spool.publish_mutant_result(
                assignment=assignment.to_dict(),
                worker=self.identity.to_dict(),
                source_sha256=source_sha256,
                mutant_result=current.to_dict(),
                source_event_id=str(frame.get("event_id", "")),
            )
            source_event_id = str(frame.get("event_id", ""))
            if source_event_id and source_event_id != current_event_id:
                self.agent.spool.quarantine_mutant_event(
                    source_event_id,
                    reason=f"carried forward to attempt {assignment.attempt}",
                )
            carried[current.mutant_id.value] = current
        return carried
    def _execute_engine_payload(
        self,
        assignment: ShardAssignment,
        execution: WorkerExecutionSpec,
        *,
        correlation_id: str | None,
    ) -> Mapping[str, Any]:
        # Own engine execution while carrying forward fsynced mutant results from prior attempts.
        configuration = execution.configuration
        if configuration is None:
            raise ValueError("engine configuration is missing")
        workspace = Path(str(execution.workspace)).expanduser().resolve()
        report_root = Path(str(execution.report_root)).expanduser().resolve()
        publish_engine_root = (
            Path(execution.publish_engine_root).expanduser().resolve()
            if execution.publish_engine_root
            else None
        )
        if not workspace.is_dir():
            raise ValueError("engine.workspace must be an existing directory")
        report_root.mkdir(parents=True, exist_ok=True)
        command = execution.command
        command_timeouts = {
            str(key): max(0.1, float(value))
            for key, value in execution.command_timeouts.items()
        }
        expected_source_sha256 = str(execution.expected_source_sha256)
        prepared_snapshot_id = str(
            execution.prepared_snapshot_id or assignment.prepared_snapshot_id
        ).strip()
        if not prepared_snapshot_id:
            raise ValueError(
                "worker execution has no prepared snapshot identity: "
                f"campaign_id={assignment.campaign_id!r}, shard_id={assignment.shard_id!r}, "
                f"lease_id={assignment.lease_id!r}, attempt={assignment.attempt}"
            )
        if prepared_snapshot_id != assignment.prepared_snapshot_id:
            raise ValueError(
                "worker execution snapshot identity conflicts with assignment: "
                f"expected={assignment.prepared_snapshot_id!r}, received={prepared_snapshot_id!r}, "
                f"campaign_id={assignment.campaign_id!r}, shard_id={assignment.shard_id!r}, "
                f"lease_id={assignment.lease_id!r}, attempt={assignment.attempt}"
            )
        reports_value = str(configuration.get("reports_dir", "")).strip()
        worker_engine_root = (
            Path(reports_value).expanduser().resolve() / "engine" / assignment.campaign_id
            if reports_value
            else None
        )
        if publish_engine_root is not None and worker_engine_root is None:
            raise ValueError("engine configuration reports_dir must be non-empty when publishing artifacts")
        run_id = (
            f"engine-shard-{assignment.campaign_id}-{assignment.shard_id}"
            f"-attempt-{max(0, assignment.attempt)}"
        )
        expected_mutant_ids = set(execution.expected_mutant_ids)
        raw_execute_request = execution.execute_request
        raw_fingerprints = execution.test_fingerprints
        cancel_path = (
            Path(execution.cancel_path).expanduser().resolve()
            if execution.cancel_path
            else None
        )
        requested_ids: tuple[str, ...] = ()
        execute_request: dict[str, Any] | None = None
        if raw_execute_request is not None:
            execute_request = dict(raw_execute_request)
            nested_snapshot_id = str(
                execute_request.get("prepared_snapshot_id") or ""
            ).strip()
            if nested_snapshot_id and nested_snapshot_id != prepared_snapshot_id:
                raise ValueError(
                    "engine execute request snapshot identity conflicts with worker assignment: "
                    f"expected={prepared_snapshot_id!r}, received={nested_snapshot_id!r}, "
                    f"campaign_id={assignment.campaign_id!r}, shard_id={assignment.shard_id!r}, "
                    f"lease_id={assignment.lease_id!r}, attempt={assignment.attempt}"
                )
            execute_request["prepared_snapshot_id"] = prepared_snapshot_id
            raw_shard = execute_request.get("shard")
            if not isinstance(raw_shard, Mapping):
                raise ValueError("engine execute request has no shard object")
            raw_ids = raw_shard.get("mutant_ids", ())
            if not isinstance(raw_ids, (list, tuple)):
                raise ValueError("engine execute request mutant_ids must be an array")
            requested_ids = tuple(str(item) for item in raw_ids)
        carried = self._carry_forward_mutant_results(
            assignment,
            source_sha256=expected_source_sha256,
            mutant_ids=requested_ids,
        ) if requested_ids else {}
        remaining_ids = tuple(item for item in requested_ids if item not in carried)
        if execute_request is not None:
            raw_shard = dict(execute_request["shard"])
            raw_shard["mutant_ids"] = list(remaining_ids)
            raw_shard["estimated_cost"] = float(len(remaining_ids))
            execute_request["shard"] = raw_shard
            raw_overrides = execute_request.get("test_overrides", {})
            execute_request["test_overrides"] = {
                str(mutant_id): list(nodeids)
                for mutant_id, nodeids in raw_overrides.items()
                if mutant_id in remaining_ids and isinstance(nodeids, (list, tuple))
            } if isinstance(raw_overrides, Mapping) else {}
            execute_request.update(
                {
                    "prepared_snapshot_id": prepared_snapshot_id,
                    "worker_instance_id": self.identity.instance_id,
                    "worker_process_id": self.identity.process_id,
                    "worker_process_birth_token": self.identity.process_birth_token,
                    "mutant_spool_root": str(self.agent.spool.root),
                    "expected_source_sha256": expected_source_sha256,
                    "test_fingerprints": {
                        mutant_id: dict(raw_fingerprints.get(mutant_id, {}))
                        for mutant_id in remaining_ids
                    },
                }
            )
        if execute_request is not None and not remaining_ids:
            ordered = tuple(carried[item] for item in requested_ids if item in carried)
            return {
                "shard_result": ShardExecutionResult(
                    shard_id=ShardId(assignment.shard_id),
                    worker_id=WorkerId(self.identity.worker_id),
                    status=WorkerStatus.COMPLETE,
                    completed_mutants=len(ordered),
                    results=ordered,
                    report_path="spool://per-mutant-salvage",
                ).to_dict(),
                "engine_process_id": None,
                "published_engine_artifacts": [],
                "source_sha256": expected_source_sha256,
            }
        process: EngineProcessSession | None = None
        child_process_id: int | None = None
        worker_prepared: PreparedCampaign | None = None
        startup_failures: list[dict[str, Any]] = []
        for startup_attempt in (1, 2):
            candidate = EngineProcessSession(
                events_path=report_root / "protocol" / "events.jsonl",
                protocol_path=report_root / "protocol" / "responses.jsonl",
                stdout_path=report_root / "artifacts" / "stdout.log",
                stderr_path=report_root / "artifacts" / "stderr.log",
                cwd=workspace,
                command=command,
                command_timeout_seconds=command_timeouts,
                heartbeat_interval_seconds=self.heartbeat_interval_seconds,
                cancel_callback=lambda: self._engine_cancelled(cancel_path),
            )
            self._engine_session = candidate
            candidate_pid = self._announce_engine_started(
                candidate,
                assignment,
                correlation_id=correlation_id,
            )
            try:
                worker_prepared = PreparedCampaign.from_dict(
                    candidate.request("prepare", {"configuration": dict(configuration)})
                )
            except EngineProcessError as exc:
                diagnostics = candidate.diagnostics()
                cleanup_error = self._close_engine_attempt(
                    candidate,
                    candidate_pid,
                    assignment,
                    correlation_id=correlation_id,
                )
                startup_failures.append(
                    {
                        "attempt": startup_attempt,
                        "pid": candidate_pid,
                        "error": str(exc),
                        "returncode": diagnostics.get("returncode"),
                        "reader_error": diagnostics.get("reader_error"),
                        "stderr_tail": diagnostics.get("stderr_tail"),
                        "stdout_path": diagnostics.get("stdout_path"),
                        "stderr_path": diagnostics.get("stderr_path"),
                        "protocol_path": diagnostics.get("protocol_path"),
                        "cleanup_error": cleanup_error,
                    }
                )
                if (
                    startup_attempt == 1
                    and cleanup_error is None
                    and not self._engine_cancelled(cancel_path)
                    and self._retryable_prepare_transport_failure(exc)
                ):
                    continue
                raise RuntimeError(
                    "worker engine prepare transport failed: "
                    f"campaign_id={assignment.campaign_id!r}; "
                    f"shard_id={assignment.shard_id!r}; lease_id={assignment.lease_id!r}; "
                    f"attempt={assignment.attempt}; startup_attempts={startup_failures!r}"
                ) from exc
            process = candidate
            child_process_id = candidate_pid
            break
        if process is None or child_process_id is None or worker_prepared is None:
            raise RuntimeError(
                "worker engine prepare produced no usable session: "
                f"campaign_id={assignment.campaign_id!r}; shard_id={assignment.shard_id!r}; "
                f"startup_attempts={startup_failures!r}"
            )
        try:
            actual_ids = {item.mutant_id.value for item in worker_prepared.mutants}
            missing_mutant_ids = tuple(sorted(expected_mutant_ids - actual_ids))
            if missing_mutant_ids:
                raise RuntimeError(
                    "worker prepared snapshot is missing planned mutants: "
                    f"missing_mutant_ids={missing_mutant_ids!r}, "
                    f"expected_count={len(expected_mutant_ids)}, actual_count={len(actual_ids)}, "
                    f"campaign_id={assignment.campaign_id!r}, shard_id={assignment.shard_id!r}"
                )
            if worker_prepared.source_sha256 != expected_source_sha256:
                raise RuntimeError(
                    "worker prepared source hash conflicts with coordinator plan: "
                    f"expected={expected_source_sha256!r}, "
                    f"received={worker_prepared.source_sha256!r}, "
                    f"campaign_id={assignment.campaign_id!r}, shard_id={assignment.shard_id!r}"
                )
            worker_snapshot_id = str(worker_prepared.snapshot_id or "").strip()
            if worker_snapshot_id and worker_snapshot_id != prepared_snapshot_id:
                raise RuntimeError(
                    "worker prepared snapshot identity conflicts with coordinator plan: "
                    f"expected={prepared_snapshot_id!r}, received={worker_snapshot_id!r}, "
                    f"campaign_id={assignment.campaign_id!r}, shard_id={assignment.shard_id!r}, "
                    f"lease_id={assignment.lease_id!r}, attempt={assignment.attempt}"
                )
            if not worker_snapshot_id:
                # Older/fake engine prepare responses may omit snapshot_id. The authoritative
                # assignment identity remains safe because source hash and mutant membership
                # were independently verified above and execute-shard is explicitly fenced.
                worker_prepared = replace(worker_prepared, snapshot_id=prepared_snapshot_id)
            if execute_request is None:
                return {
                    "shard_result": None,
                    "engine_process_id": int(child_process_id),
                    "source_sha256": expected_source_sha256,
                    "engine_startup_retries": len(startup_failures),
                }
            fresh = ShardExecutionResult.from_dict(
                process.request("execute-shard", execute_request)
            )
            fresh = self._attach_test_fingerprints(fresh, raw_fingerprints, workspace)
            fresh_by_mutant = {item.mutant_id.value: item for item in fresh.results}
            combined = dict(carried)
            combined.update(fresh_by_mutant)
            ordered = tuple(combined[item] for item in requested_ids if item in combined)
            result = replace(
                fresh,
                completed_mutants=len(ordered),
                results=ordered,
            )
            published_artifacts: tuple[str, ...] = ()
            if publish_engine_root is not None and worker_engine_root is not None:
                published_artifacts = self._publish_engine_run_artifacts(
                    worker_engine_root,
                    publish_engine_root,
                    run_id,
                )
            return {
                "shard_result": result.to_dict(),
                "engine_process_id": int(child_process_id),
                "published_engine_artifacts": list(published_artifacts),
                "source_sha256": expected_source_sha256,
                "engine_startup_retries": len(startup_failures),
            }
        finally:
            cleanup_error = self._close_engine_attempt(
                process,
                child_process_id,
                assignment,
                correlation_id=correlation_id,
            )
            if cleanup_error is not None:
                raise RuntimeError(
                    "worker engine cleanup failed after execution: "
                    f"campaign_id={assignment.campaign_id!r}; "
                    f"shard_id={assignment.shard_id!r}; child_process_id={child_process_id}; "
                    f"cleanup_error={cleanup_error}"
                )
    def _execution_payload(
        self,
        assignment: ShardAssignment,
        execution: WorkerExecutionSpec,
        *,
        correlation_id: str | None,
    ) -> Mapping[str, Any]:
        # Execute one typed fixture or worker-owned engine specification after durable replay lookup.
        pending = matching_pending_delivery(
            self.agent.spool,
            campaign_id=assignment.campaign_id,
            shard_id=assignment.shard_id,
            lease_id=assignment.lease_id,
            attempt=assignment.attempt,
        )
        if pending is not None:
            pending_result = pending.payload.get("result")
            if not isinstance(pending_result, Mapping):
                raise RuntimeError("worker spool delivery has no result object")
            return dict(pending_result)
        if execution.mode == WorkerExecutionMode.ENGINE:
            return self._execute_engine_payload(
                assignment,
                execution,
                correlation_id=correlation_id,
            )
        if execution.mode != WorkerExecutionMode.FIXTURE:
            raise ValueError(f"unsupported worker execution mode: {execution.mode}")
        if execution.delay_seconds:
            time.sleep(execution.delay_seconds)
        return dict(execution.result)
    def _execution_envelopes(self, delivery: AgentRun) -> tuple[ExecutionEnvelope, ...]:
        # Reconstruct typed envelopes from the immutable mutant events linked to this shard delivery.
        envelopes: list[ExecutionEnvelope] = []
        for event_id in delivery.mutant_event_ids:
            frame = self.agent.spool.mutant_event(event_id)
            payload = frame.get("payload")
            raw_assignment = payload.get("assignment") if isinstance(payload, Mapping) else None
            raw_worker = payload.get("worker") if isinstance(payload, Mapping) else None
            row = payload.get("mutant_result") if isinstance(payload, Mapping) else None
            if (
                not isinstance(raw_assignment, Mapping)
                or not isinstance(raw_worker, Mapping)
                or not isinstance(row, Mapping)
            ):
                raise RuntimeError("durable mutant event is missing typed ownership or result data")
            campaign_id = str(
                frame.get("campaign_id") or raw_assignment.get("campaign_id") or ""
            ).strip()
            shard_id = str(
                frame.get("shard_id") or raw_assignment.get("shard_id") or ""
            ).strip()
            lease_id = str(
                frame.get("lease_id") or raw_assignment.get("lease_id") or ""
            ).strip()
            try:
                attempt = int(frame.get("attempt", raw_assignment.get("attempt", 0)))
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    "durable mutant event attempt is invalid: "
                    f"event_id={event_id!r}, top_level_attempt={frame.get('attempt')!r}, "
                    f"payload_attempt={raw_assignment.get('attempt')!r}"
                ) from exc
            identity_pairs = {
                "campaign_id": (frame.get("campaign_id"), raw_assignment.get("campaign_id")),
                "shard_id": (frame.get("shard_id"), raw_assignment.get("shard_id")),
                "lease_id": (frame.get("lease_id"), raw_assignment.get("lease_id")),
                "attempt": (frame.get("attempt"), raw_assignment.get("attempt")),
            }
            conflicts = {
                name: {"top_level": top, "payload": nested}
                for name, (top, nested) in identity_pairs.items()
                if top is not None and nested is not None and str(top) != str(nested)
            }
            if conflicts:
                raise RuntimeError(
                    "durable mutant event ownership identity conflicts between frame and payload: "
                    f"event_id={event_id!r}, conflicts={conflicts!r}"
                )
            if not all((campaign_id, shard_id, lease_id)) or attempt < 0:
                raise RuntimeError(
                    "durable mutant event ownership identity is incomplete: "
                    f"event_id={event_id!r}, campaign_id={campaign_id!r}, "
                    f"shard_id={shard_id!r}, lease_id={lease_id!r}, attempt={attempt!r}, "
                    f"frame_keys={tuple(sorted(str(key) for key in frame))!r}, "
                    f"assignment_keys={tuple(sorted(str(key) for key in raw_assignment))!r}"
                )
            worker = WorkerIdentity.from_dict(raw_worker)
            execution_id = str(row.get("execution_id") or "")
            mutant_id = str(row.get("mutant_id") or "")
            semantic_result = str(
                row.get("result")
                or row.get("semantic_result")
                or row.get("status")
                or "unknown"
            )
            if not execution_id or not mutant_id:
                raise RuntimeError("durable mutant result identity is incomplete")
            raw_observations = row.get("test_observations", [])
            raw_artifacts = row.get("artifact_refs")
            if raw_artifacts is None:
                raw_artifacts = [
                    {"path": str(item)} for item in row.get("artifact_paths", [])
                ]
            if not isinstance(raw_observations, (list, tuple)) or not isinstance(
                raw_artifacts,
                (list, tuple),
            ):
                raise RuntimeError("durable mutant evidence arrays are invalid")
            envelopes.append(
                ExecutionEnvelope(
                    event_id=str(event_id),
                    execution_id=execution_id,
                    campaign_id=campaign_id,
                    shard_id=shard_id,
                    mutant_id=mutant_id,
                    worker_id=worker.worker_id,
                    worker_instance_id=worker.instance_id,
                    lease_id=lease_id,
                    attempt=attempt,
                    semantic_result=semantic_result,
                    restore_verified=bool(row.get("restore_verified", False)),
                    test_observations=tuple(
                        dict(item) for item in raw_observations if isinstance(item, Mapping)
                    ),
                    artifact_refs=tuple(
                        dict(item) for item in raw_artifacts if isinstance(item, Mapping)
                    ),
                    payload_sha256=str(frame.get("payload_sha256", "")),
                )
            )
        return tuple(envelopes)
    def _run_assignment(self, frame: WorkerProtocolFrame) -> None:
        # Claim, execute and durably deliver one typed assignment without terminating the worker process.
        payload = frame.payload
        if not isinstance(payload, AssignShard):
            raise TypeError("shard assignment payload is not typed")
        assignment = payload.assignment
        execution = payload.execution
        correlation_id = frame.correlation_id
        if correlation_id is None:
            raise ValueError("shard assignment requires correlation_id")
        self._set_state(WorkerProcessState.CLAIMING)
        with self._state_lock:
            self._current_assignment = assignment
            self._current_correlation_id = correlation_id
        self._set_state(WorkerProcessState.RUNNING)
        delivery = self.agent.run_assignment(
            assignment,
            lambda: self._execution_payload(
                assignment,
                execution,
                correlation_id=correlation_id,
            ),
            heartbeat=self._assignment_heartbeat,
            require_precommitted_mutants=execution.mode == WorkerExecutionMode.ENGINE,
        )
        self._set_state(WorkerProcessState.DELIVERING)
        raw_result = delivery.payload.get("result")
        if not isinstance(raw_result, Mapping):
            raise RuntimeError("durable delivery result is not an object")
        raw_shard_result = raw_result.get("shard_result")
        shard_result = None
        if isinstance(raw_shard_result, Mapping):
            # Complete test-only fixture projections with authoritative assignment and worker identity.
            normalized_shard_result = dict(raw_shard_result)
            normalized_shard_result.setdefault("shard_id", assignment.shard_id)
            normalized_shard_result.setdefault("worker_id", self.identity.worker_id)
            normalized_shard_result.setdefault(
                "completed_mutants",
                len(normalized_shard_result.get("results", ())),
            )
            normalized_shard_result.setdefault(
                "status",
                str(raw_result.get("status") or "complete"),
            )
            shard_result = ShardExecutionResult.from_dict(normalized_shard_result)
        raw_engine_process_id = raw_result.get("engine_process_id")
        raw_published_artifacts = raw_result.get("published_engine_artifacts", [])
        if not isinstance(raw_published_artifacts, (list, tuple)):
            raise RuntimeError("published engine artifacts must be an array")
        delivery_frame = self._emit(
            WorkerMessageType.EXECUTION_DELIVERY,
            ExecutionDelivery(
                event_id=delivery.event_id,
                payload_sha256=delivery.payload_sha256,
                assignment=assignment,
                worker=self.identity,
                capabilities=self.agent.capabilities,
                shard_result=shard_result,
                engine_process_id=(
                    int(raw_engine_process_id)
                    if raw_engine_process_id is not None
                    else None
                ),
                published_engine_artifacts=tuple(
                    str(item) for item in raw_published_artifacts
                ),
                mutant_event_ids=delivery.mutant_event_ids,
                execution_envelopes=self._execution_envelopes(delivery),
                assignment_number=self._completed_assignments + 1,
            ),
            correlation_id=correlation_id,
            request_id=frame.message_id,
        )
        self._await_execution_receipt(
            delivery,
            delivery_frame=delivery_frame,
            correlation_id=correlation_id,
        )
    def _await_execution_receipt(
        self,
        delivery: AgentRun,
        *,
        delivery_frame: WorkerProtocolFrame,
        correlation_id: str,
    ) -> None:
        # Hold durable delivery until a typed authoritative execution receipt is received.
        while True:
            frame = self._read_control_frame(expected_request_id=delivery_frame.message_id)
            if frame is None:
                self.agent.stop()
                raise EOFError("worker control stream closed before execution receipt")
            frame.require_correlation(correlation_id)
            if frame.message_type == WorkerMessageType.EXECUTION_RECEIPT:
                payload = frame.payload
                if not isinstance(payload, ExecutionReceipt):
                    raise TypeError("execution receipt payload is not typed")
                if payload.event_id != delivery.event_id:
                    raise ValueError("execution receipt event_id mismatch")
                if not payload.accepted:
                    self.agent.stop()
                    raise RuntimeError(payload.reason or "execution delivery rejected")
                self.agent.acknowledge(delivery.event_id)
                self._completed_assignments += 1
                with self._state_lock:
                    self._current_assignment = None
                    self._current_correlation_id = None
                self._set_state(WorkerProcessState.IDLE)
                self._emit(
                    WorkerMessageType.EXECUTION_ACKNOWLEDGED,
                    ExecutionAcknowledged(
                        delivery.event_id,
                        self._completed_assignments,
                    ),
                    correlation_id=correlation_id,
                    request_id=frame.message_id,
                )
                return
            if frame.message_type in {
                WorkerMessageType.SHUTDOWN_WORKER,
                WorkerMessageType.DRAIN_WORKER,
            }:
                self._set_state(WorkerProcessState.DRAINING)
                self.agent.stop()
                self._shutdown.set()
                return
            if frame.message_type == WorkerMessageType.CANCEL_ASSIGNMENT:
                self.agent.stop()
                self._shutdown.set()
                raise RuntimeError("assignment cancelled before execution receipt")
            raise ValueError(
                f"worker is delivering and cannot accept {frame.message_type.value}"
            )
    def serve(self) -> int:
        # Register once, accept only typed host frames, and retain one process identity across assignments.
        exit_code = 0
        self._set_state(WorkerProcessState.REGISTERED)
        registration = self._emit(
            WorkerMessageType.REGISTER_WORKER,
            RegisterWorker(
                self.identity,
                self.agent.capabilities,
                str(self.agent.spool.root.resolve()),
                current_runtime_identity().to_dict(),
            ),
        )
        try:
            receipt_frame = self._read_control_frame(expected_request_id=registration.message_id)
            if receipt_frame is None:
                raise EOFError("worker control stream closed before registration receipt")
            if receipt_frame.message_type != WorkerMessageType.REGISTER_WORKER_RECEIPT:
                raise ValueError("worker registration requires RegisterWorkerReceipt")
            receipt = receipt_frame.payload
            if not isinstance(receipt, RegisterWorkerReceipt):
                raise TypeError("registration receipt payload is not typed")
            if not receipt.accepted:
                raise RuntimeError(receipt.reason or "worker registration rejected")
            self._start_heartbeat()
            while not self._shutdown.is_set():
                self._set_state(WorkerProcessState.IDLE)
                acquire = self._emit(
                    WorkerMessageType.ACQUIRE_ASSIGNMENT,
                    AcquireAssignment(self._completed_assignments),
                )
                frame = self._read_control_frame(expected_request_id=acquire.message_id)
                if frame is None:
                    self._set_state(WorkerProcessState.SHUTTING_DOWN)
                    break
                if frame.message_type == WorkerMessageType.SHARD_ASSIGNMENT:
                    self._run_assignment(frame)
                elif frame.message_type == WorkerMessageType.NO_ASSIGNMENT:
                    payload = frame.payload
                    if not isinstance(payload, NoAssignment):
                        raise TypeError("no-assignment payload is not typed")
                    if payload.wait_seconds:
                        self._shutdown.wait(payload.wait_seconds)
                elif frame.message_type in {
                    WorkerMessageType.DRAIN_WORKER,
                    WorkerMessageType.SHUTDOWN_WORKER,
                }:
                    self._set_state(WorkerProcessState.SHUTTING_DOWN)
                    self._shutdown.set()
                elif frame.message_type == WorkerMessageType.CANCEL_ASSIGNMENT:
                    raise ValueError("idle worker cannot cancel a non-existent assignment")
                else:
                    raise ValueError(
                        f"unsupported typed worker command: {frame.message_type.value}"
                    )
        except BaseException as exc:
            exit_code = 1
            self._set_state(WorkerProcessState.FAILED)
            try:
                self._emit(
                    WorkerMessageType.PROTOCOL_ERROR,
                    WorkerProtocolError(str(exc), type(exc).__name__),
                )
            except BaseException:
                pass
        finally:
            active_engine = self._engine_session
            if active_engine is not None:
                try:
                    active_engine.close()
                except BaseException:
                    exit_code = 1
            self.agent.stop()
            try:
                self._stop_heartbeat()
            except BaseException as exc:
                exit_code = 1
                self._set_state(WorkerProcessState.FAILED)
                try:
                    self._emit(
                        WorkerMessageType.PROTOCOL_ERROR,
                        WorkerProtocolError(str(exc), type(exc).__name__),
                    )
                except BaseException:
                    pass
            if self.state != WorkerProcessState.FAILED:
                self._set_state(WorkerProcessState.TERMINATED)
            try:
                self._emit(
                    WorkerMessageType.WORKER_TERMINATED,
                    WorkerTerminated(self._completed_assignments, exit_code),
                )
            except BaseException:
                exit_code = 1
        return exit_code
def build_parser() -> argparse.ArgumentParser:
    # Build the standalone worker CLI without coupling it to campaign CLI commands.
    parser = argparse.ArgumentParser(prog="theseus-worker")
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--instance-id")
    parser.add_argument("--spool-root", required=True)
    parser.add_argument("--heartbeat-interval", type=float, default=1.0)
    return parser
def main(argv: list[str] | None = None) -> int:
    # Start one persistent worker process using the versioned typed JSONL protocol.
    args = build_parser().parse_args(argv)
    runtime = PersistentWorkerEntrypoint(
        worker_id=args.worker_id,
        instance_id=args.instance_id,
        spool_root=Path(args.spool_root),
        heartbeat_interval_seconds=args.heartbeat_interval,
    )
    return runtime.serve()
if __name__ == "__main__":
    raise SystemExit(main())
