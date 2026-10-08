"""Parent-side host for the standalone persistent Theseus worker process."""
from __future__ import annotations
import errno
import os
import signal
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from queue import Empty, Queue
from threading import Lock, RLock, Thread, enumerate as enumerate_threads
from typing import Any, Callable, ClassVar, Iterator, Mapping, Sequence
from theseus_contracts import (
    WORKER_TO_HOST_MESSAGE_TYPES,
    AcquireAssignment,
    AssignShard,
    CancelAssignment,
    DrainWorker,
    EngineLifecycle,
    ExecutionDelivery,
    ExecutionReceipt,
    HeartbeatReceipt,
    NoAssignment,
    RegisterWorker,
    RegisterWorkerReceipt,
    ShardAssignment,
    ShutdownWorker,
    WorkerHeartbeatFrame,
    WorkerHeartbeatReceipt,
    WorkerExecutionSpec,
    WorkerMessageType,
    WorkerProtocolError,
    WorkerProtocolFrame,
    decode_worker_frame,
    encode_worker_frame,
)
from theseus_contracts.serialization import utc_now
from test_intelligence_unified_v1.commands import terminate_process_tree
from test_intelligence_unified_v1.recovery import current_process_birth_token
class WorkerProcessError(RuntimeError):
    """Raised when the persistent worker exits or violates the typed worker protocol."""
class PersistentWorkerProcess:
    """Launch and control one worker PID across multiple sequential assignments."""
    _active_pids: ClassVar[set[int]] = set()
    _active_pids_lock: ClassVar[Lock] = Lock()
    def __init__(
        self,
        *,
        worker_id: str,
        spool_root: Path,
        instance_id: str | None = None,
        heartbeat_interval_seconds: float = 0.1,
        command: Sequence[str] | None = None,
        cwd: Path | None = None,
        auto_accept_registration: bool = True,
    ) -> None:
        # Retain only immutable process launch inputs until start creates the child PID.
        if not worker_id.strip():
            raise ValueError("worker_id must be non-empty")
        if heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be positive")
        self.worker_id = worker_id
        self.instance_id = str(instance_id) if instance_id else None
        self.spool_root = Path(spool_root)
        self.heartbeat_interval_seconds = float(heartbeat_interval_seconds)
        self.command = tuple(command) if command is not None else (
            sys.executable,
            "-m",
            "theseus_local.worker_runtime",
        )
        self.cwd = Path(cwd).resolve() if cwd is not None else None
        self.auto_accept_registration = bool(auto_accept_registration)
        self._process: subprocess.Popen[str] | None = None
        self._worker_pid: int | None = None
        self._worker_process_birth_token: str | None = None
        self._engine_child_pid: int | None = None
        self._last_engine_child_pid: int | None = None
        self._events: Queue[WorkerProtocolFrame | BaseException | None] = Queue()
        self._outbound: Queue[tuple[WorkerProtocolFrame, str] | None] = Queue()
        self._stderr: list[str] = []
        self._reader_thread: Thread | None = None
        self._writer_thread: Thread | None = None
        self._stderr_thread: Thread | None = None
        self._write_lock = RLock()
        self._writer_error: BaseException | None = None
        self._backlog: list[WorkerProtocolFrame] = []
        self._heartbeat_handler: Callable[[WorkerProtocolFrame], HeartbeatReceipt | None] | None = None
        self._heartbeat_handler_lock = RLock()
        self._heartbeat_error: BaseException | None = None
        self._outbound_sequence = 0
        self._last_inbound_sequence = -1
        self._last_acquire_frame: WorkerProtocolFrame | None = None
        self._requests_by_correlation: dict[str, str] = {}
        self._delivery_frames: dict[str, WorkerProtocolFrame] = {}
    @classmethod
    def active_process_ids(cls) -> frozenset[int]:
        # Return every launcher or registered worker PID still owned by a live parent adapter.
        with cls._active_pids_lock:
            return frozenset(cls._active_pids)
    @classmethod
    def active_lifecycle_thread_names(cls) -> tuple[str, ...]:
        # Expose only Theseus worker transport threads for deterministic leak diagnostics.
        del cls
        return tuple(
            sorted(
                thread.name
                for thread in enumerate_threads()
                if thread.is_alive() and thread.name.startswith("theseus-worker-")
            )
        )
    @classmethod
    def _register_process_id(cls, process_id: int | None) -> None:
        # Track one positive process identity until verified process and transport cleanup completes.
        if process_id is None or int(process_id) <= 0:
            return
        with cls._active_pids_lock:
            cls._active_pids.add(int(process_id))
    @classmethod
    def _unregister_process_id(cls, process_id: int | None) -> None:
        # Remove one process identity only after the parent has observed terminal state.
        if process_id is None:
            return
        with cls._active_pids_lock:
            cls._active_pids.discard(int(process_id))
    @property
    def pid(self) -> int:
        # Return the authoritative worker PID after registration, falling back to the launcher PID during startup.
        process = self._process
        if process is None or process.pid is None:
            raise RuntimeError("worker process has not started")
        return int(self._worker_pid if self._worker_pid is not None else process.pid)
    @property
    def launcher_pid(self) -> int:
        # Expose the launcher PID separately because Windows virtual environments may redirect to another process.
        process = self._process
        if process is None or process.pid is None:
            raise RuntimeError("worker process has not started")
        return int(process.pid)
    @property
    def engine_child_pid(self) -> int | None:
        # Expose the currently running engine child owned by the worker process.
        return self._engine_child_pid
    @property
    def last_engine_child_pid(self) -> int | None:
        # Retain the last child identity after engine shutdown for registry and process-tree assertions.
        return self._last_engine_child_pid
    @property
    def returncode(self) -> int | None:
        # Expose subprocess terminal state without waiting or mutating lifecycle.
        return self._process.poll() if self._process is not None else None
    @property
    def stderr_text(self) -> str:
        # Return bounded diagnostic stderr captured by the host reader thread.
        return "".join(self._stderr)
    def set_heartbeat_handler(self, handler: Callable[[WorkerProtocolFrame], HeartbeatReceipt | None] | None) -> None:
        # Install the authoritative renewal callback before an assignment enters the worker process.
        with self._heartbeat_handler_lock:
            self._heartbeat_handler = handler
    def respond_registration(
        self,
        registration_frame: WorkerProtocolFrame,
        *,
        accepted: bool,
        reason: str | None = None,
    ) -> WorkerProtocolFrame:
        # Reply only after an external authoritative registry has accepted or rejected this process instance.
        if registration_frame.message_type != WorkerMessageType.REGISTER_WORKER:
            raise WorkerProcessError("registration response requires a RegisterWorker frame")
        return self._send_payload(
            WorkerMessageType.REGISTER_WORKER_RECEIPT,
            RegisterWorkerReceipt(bool(accepted), reason),
            request_id=registration_frame.message_id,
        )
    @contextmanager
    def heartbeat_barrier(self) -> Iterator[None]:
        # Serialize authoritative commit and renewal shutdown against any in-flight heartbeat callback.
        with self._heartbeat_handler_lock:
            yield
    def start(self) -> "PersistentWorkerProcess":
        # Spawn one unbuffered child and start independent stdout/stderr drain threads.
        if self._process is not None:
            raise RuntimeError("worker process already started")
        self.spool_root.mkdir(parents=True, exist_ok=True)
        argv = [
            *self.command,
            "--worker-id",
            self.worker_id,
            "--spool-root",
            str(self.spool_root),
            "--heartbeat-interval",
            str(self.heartbeat_interval_seconds),
        ]
        if self.instance_id:
            argv.extend(("--instance-id", self.instance_id))
        module_root = Path(__file__).resolve().parents[2]
        child_environment = dict(os.environ)
        inherited_pythonpath = child_environment.get("PYTHONPATH", "")
        pythonpath_parts = [str(module_root)]
        if inherited_pythonpath:
            pythonpath_parts.extend(item for item in inherited_pythonpath.split(os.pathsep) if item)
        child_environment["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)
        self._process = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            cwd=str(self.cwd) if self.cwd is not None else None,
            shell=False,
            start_new_session=os.name != "nt",
            env=child_environment,
        )
        self._register_process_id(self._process.pid)
        self._outbound = Queue()
        self._writer_error = None
        self._writer_thread = Thread(
            target=self._write_stdin,
            name=f"theseus-worker-writer-{self.worker_id}",
            daemon=False,
        )
        self._reader_thread = Thread(
            target=self._read_stdout,
            name=f"theseus-worker-reader-{self.worker_id}",
            daemon=False,
        )
        self._stderr_thread = Thread(
            target=self._read_stderr,
            name=f"theseus-worker-stderr-{self.worker_id}",
            daemon=False,
        )
        self._writer_thread.start()
        self._reader_thread.start()
        self._stderr_thread.start()
        return self
    def _send_payload(
        self,
        message_type: WorkerMessageType,
        payload: Any,
        *,
        correlation_id: str | None = None,
        request_id: str | None = None,
        record_correlation_request: bool = False,
    ) -> WorkerProtocolFrame:
        # Serialize sequence allocation, request tracking and writer-queue insertion in one host-side critical section.
        process = self._process
        if process is None or process.stdin is None:
            raise RuntimeError("worker process has not started")
        if self._heartbeat_error is not None:
            raise WorkerProcessError(f"worker heartbeat failed: {self._heartbeat_error}")
        if self._writer_error is not None:
            raise WorkerProcessError(f"worker writer failed: {self._writer_error}")
        if process.poll() is not None:
            raise WorkerProcessError(
                f"worker process exited with code {process.returncode}: {self.stderr_text}"
            )
        if self._worker_pid is None or self.instance_id is None:
            raise WorkerProcessError("worker identity is not registered")
        with self._write_lock:
            self._outbound_sequence += 1
            frame = WorkerProtocolFrame.create(
                message_type,
                payload,
                worker_id=self.worker_id,
                instance_id=self.instance_id,
                process_id=self._worker_pid,
                sequence=self._outbound_sequence,
                state="host",
                correlation_id=correlation_id,
                request_id=request_id,
            )
            if record_correlation_request:
                if correlation_id is None:
                    raise WorkerProcessError("tracked worker request requires correlation_id")
                self._requests_by_correlation[correlation_id] = frame.message_id
            raw = encode_worker_frame(frame) + "\n"
            self._outbound.put((frame, raw))
        return frame
    def _write_stdin(self) -> None:
        # Own every host-to-worker pipe write so the stdout reader can never deadlock on backpressure.
        process = self._process
        stream = process.stdin if process is not None else None
        if stream is None:
            error = WorkerProcessError("worker stdin is unavailable")
            self._writer_error = error
            self._events.put(error)
            return
        while True:
            item = self._outbound.get()
            if item is None:
                return
            frame, raw = item
            try:
                stream.write(raw)
                stream.flush()
            except (BrokenPipeError, OSError, ValueError) as exc:
                self._requests_by_correlation.pop(str(frame.correlation_id or ""), None)
                error = WorkerProcessError(
                    f"cannot write typed worker frame: {exc}: {self.stderr_text}"
                )
                self._writer_error = error
                self._events.put(error)
                return
    def _handle_process_frame(self, frame: WorkerProtocolFrame) -> bool:
        # Update process diagnostics, answer registration/heartbeats and retain typed delivery identity.
        payload = frame.payload
        if frame.message_type == WorkerMessageType.REGISTER_WORKER:
            if not isinstance(payload, RegisterWorker):
                raise WorkerProcessError("worker registration payload is not typed")
            self._worker_process_birth_token = payload.identity.process_birth_token
            if (
                payload.identity.worker_id != frame.worker_id
                or payload.identity.instance_id != frame.instance_id
                or payload.identity.process_id != frame.process_id
            ):
                raise WorkerProcessError("worker registration identity conflicts with frame identity")
            if self.auto_accept_registration:
                self.respond_registration(frame, accepted=True)
        elif frame.message_type == WorkerMessageType.ACQUIRE_ASSIGNMENT:
            if not isinstance(payload, AcquireAssignment):
                raise WorkerProcessError("acquire assignment payload is not typed")
            self._last_acquire_frame = frame
        elif frame.message_type == WorkerMessageType.ENGINE_STARTED:
            if not isinstance(payload, EngineLifecycle):
                raise WorkerProcessError("engine started payload is not typed")
            self._engine_child_pid = payload.child_process_id
            self._last_engine_child_pid = payload.child_process_id
        elif frame.message_type == WorkerMessageType.ENGINE_STOPPED:
            if not isinstance(payload, EngineLifecycle):
                raise WorkerProcessError("engine stopped payload is not typed")
            if self._engine_child_pid is not None and payload.child_process_id != self._engine_child_pid:
                raise WorkerProcessError("engine_stopped child_process_id does not match the owned engine")
            self._engine_child_pid = None
        elif frame.message_type == WorkerMessageType.EXECUTION_DELIVERY:
            if not isinstance(payload, ExecutionDelivery):
                raise WorkerProcessError("execution delivery payload is not typed")
            expected_request = self._requests_by_correlation.get(str(frame.correlation_id or ""))
            if expected_request is None or frame.request_id != expected_request:
                raise WorkerProcessError("execution delivery request_id does not match assignment")
            if (
                payload.worker.worker_id != self.worker_id
                or payload.worker.instance_id != self.instance_id
                or payload.worker.process_id != self._worker_pid
            ):
                raise WorkerProcessError("execution delivery worker identity does not match registration")
            self._delivery_frames[payload.event_id] = frame
        elif frame.message_type == WorkerMessageType.EXECUTION_ACKNOWLEDGED:
            payload_event_id = getattr(payload, "event_id", "")
            delivery = self._delivery_frames.pop(str(payload_event_id), None)
            if delivery is not None and delivery.correlation_id is not None:
                self._requests_by_correlation.pop(delivery.correlation_id, None)
        if frame.message_type != WorkerMessageType.WORKER_HEARTBEAT:
            return False
        if not isinstance(payload, WorkerHeartbeatFrame):
            raise WorkerProcessError("worker heartbeat payload is not typed")
        heartbeat_identity = payload.heartbeat.worker
        if (
            heartbeat_identity.worker_id != frame.worker_id
            or heartbeat_identity.instance_id != frame.instance_id
            or heartbeat_identity.process_id != frame.process_id
        ):
            raise WorkerProcessError("worker heartbeat identity conflicts with frame identity")
        try:
            with self._heartbeat_handler_lock:
                handler = self._heartbeat_handler
                authoritative_receipt = handler(frame) if handler is not None else None
            receipt = (
                authoritative_receipt
                if isinstance(authoritative_receipt, HeartbeatReceipt)
                else HeartbeatReceipt(True, True)
            )
        except BaseException as exc:
            receipt = HeartbeatReceipt(
                False,
                False,
                cancellation_requested=True,
                reason=str(exc),
            )
            try:
                self._send_payload(
                    WorkerMessageType.HEARTBEAT_RECEIPT,
                    WorkerHeartbeatReceipt(receipt),
                    correlation_id=frame.correlation_id,
                    request_id=frame.message_id,
                )
            finally:
                raise
        self._send_payload(
            WorkerMessageType.HEARTBEAT_RECEIPT,
            WorkerHeartbeatReceipt(receipt),
            correlation_id=frame.correlation_id,
            request_id=frame.message_id,
        )
        return handler is not None
    def _read_stdout(self) -> None:
        # Decode typed worker frames continuously and reject unknown, incompatible or foreign messages.
        process = self._process
        if process is None or process.stdout is None:
            self._events.put(WorkerProcessError("worker stdout is unavailable"))
            return
        try:
            for line in process.stdout:
                frame = decode_worker_frame(
                    line,
                    allowed_types=WORKER_TO_HOST_MESSAGE_TYPES,
                )
                if frame.sequence <= self._last_inbound_sequence:
                    raise WorkerProcessError("worker protocol sequence did not advance")
                self._last_inbound_sequence = frame.sequence
                if self._worker_pid is None:
                    if frame.message_type != WorkerMessageType.REGISTER_WORKER:
                        raise WorkerProcessError("first worker frame must be RegisterWorker")
                    if frame.worker_id != self.worker_id:
                        raise WorkerProcessError("registered worker_id does not match launched worker")
                    if self.instance_id is not None and frame.instance_id != self.instance_id:
                        raise WorkerProcessError("registered instance_id does not match launched worker")
                    self._worker_pid = frame.process_id
                    self._register_process_id(self._worker_pid)
                    self.instance_id = frame.instance_id
                else:
                    frame.require_identity(
                        worker_id=self.worker_id,
                        instance_id=str(self.instance_id),
                        process_id=self._worker_pid,
                    )
                try:
                    consumed = self._handle_process_frame(frame)
                except BaseException as exc:
                    self._heartbeat_error = exc
                    self._events.put(exc)
                    try:
                        self.terminate_tree()
                    except BaseException:
                        pass
                    return
                if not consumed:
                    self._events.put(frame)
        except BaseException as exc:
            self._heartbeat_error = exc
            self._events.put(exc)
            try:
                self.terminate_tree()
            except BaseException:
                pass
        finally:
            self._events.put(None)
    def _read_stderr(self) -> None:
        # Drain stderr continuously so diagnostics cannot block the typed control protocol.
        process = self._process
        if process is None or process.stderr is None:
            return
        for line in process.stderr:
            self._stderr.append(line)
            if len(self._stderr) > 200:
                del self._stderr[:-200]
    def _acquire_request_id(self) -> str:
        # Require a preceding typed acquire request before the host can answer with work or idle delay.
        frame = self._last_acquire_frame
        if frame is None:
            raise WorkerProcessError("worker has not requested an assignment")
        return frame.message_id
    def send_assignment(
        self,
        assignment: ShardAssignment,
        result: Mapping[str, Any],
        *,
        delay_seconds: float = 0.0,
        correlation_id: str | None = None,
    ) -> str:
        # Supply one typed immutable assignment for lifecycle-only tests.
        resolved_correlation = correlation_id or uuid.uuid4().hex
        self._send_payload(
            WorkerMessageType.SHARD_ASSIGNMENT,
            AssignShard(
                assignment,
                WorkerExecutionSpec.fixture(
                    result,
                    delay_seconds=delay_seconds,
                ),
            ),
            correlation_id=resolved_correlation,
            request_id=self._acquire_request_id(),
            record_correlation_request=True,
        )
        self._last_acquire_frame = None
        return resolved_correlation
    def send_engine_assignment(
        self,
        assignment: ShardAssignment,
        *,
        configuration: Mapping[str, Any],
        execute_request: Mapping[str, Any] | None,
        workspace: Path,
        report_root: Path,
        expected_source_sha256: str,
        expected_mutant_ids: Sequence[str],
        test_fingerprints: Mapping[str, Mapping[str, str]],
        command_timeouts: Mapping[str, float],
        publish_engine_root: Path | None = None,
        engine_command: Sequence[str] | None = None,
        cancel_path: Path | None = None,
        correlation_id: str | None = None,
        prepared_snapshot_id: str | None = None,
    ) -> str:
        # Dispatch one typed engine assignment while EngineProcessSession remains inside the worker PID.
        authoritative_snapshot_id = str(assignment.prepared_snapshot_id or "").strip()
        if not authoritative_snapshot_id:
            raise WorkerProcessError(
                "worker assignment has no authoritative prepared snapshot identity: "
                f"campaign_id={assignment.campaign_id!r}, shard_id={assignment.shard_id!r}, "
                f"lease_id={assignment.lease_id!r}, attempt={assignment.attempt}"
            )
        requested_snapshot_id = str(prepared_snapshot_id or "").strip()
        if requested_snapshot_id and requested_snapshot_id != authoritative_snapshot_id:
            raise WorkerProcessError(
                "worker host prepared snapshot identity conflicts with the authoritative shard assignment: "
                f"expected={authoritative_snapshot_id!r}, received={requested_snapshot_id!r}, "
                f"campaign_id={assignment.campaign_id!r}, shard_id={assignment.shard_id!r}, "
                f"lease_id={assignment.lease_id!r}, attempt={assignment.attempt}"
            )
        normalized_execute_request = dict(execute_request) if execute_request is not None else None
        if normalized_execute_request is not None:
            nested_snapshot_id = str(
                normalized_execute_request.get("prepared_snapshot_id") or ""
            ).strip()
            if nested_snapshot_id and nested_snapshot_id != authoritative_snapshot_id:
                raise WorkerProcessError(
                    "engine execute request prepared snapshot identity conflicts with the authoritative "
                    "shard assignment: "
                    f"expected={authoritative_snapshot_id!r}, received={nested_snapshot_id!r}, "
                    f"campaign_id={assignment.campaign_id!r}, shard_id={assignment.shard_id!r}, "
                    f"lease_id={assignment.lease_id!r}, attempt={assignment.attempt}"
                )
            normalized_execute_request["prepared_snapshot_id"] = authoritative_snapshot_id
        execution = WorkerExecutionSpec.engine(
            configuration=configuration,
            execute_request=normalized_execute_request,
            workspace=str(Path(workspace).resolve()),
            report_root=str(Path(report_root).resolve()),
            publish_engine_root=(
                str(Path(publish_engine_root).resolve())
                if publish_engine_root is not None
                else None
            ),
            expected_source_sha256=expected_source_sha256,
            prepared_snapshot_id=authoritative_snapshot_id,
            expected_mutant_ids=tuple(str(item) for item in expected_mutant_ids),
            test_fingerprints=test_fingerprints,
            command_timeouts=command_timeouts,
            command=tuple(str(item) for item in engine_command) if engine_command is not None else None,
            cancel_path=str(Path(cancel_path).resolve()) if cancel_path is not None else None,
        )
        resolved_correlation = correlation_id or uuid.uuid4().hex
        self._send_payload(
            WorkerMessageType.SHARD_ASSIGNMENT,
            AssignShard(assignment, execution),
            correlation_id=resolved_correlation,
            request_id=self._acquire_request_id(),
            record_correlation_request=True,
        )
        self._last_acquire_frame = None
        return resolved_correlation
    def acknowledge(
        self,
        event_id: str,
        *,
        correlation_id: str | None = None,
        accepted: bool = True,
        reason: str | None = None,
    ) -> str:
        # Send the authoritative typed execution receipt only after coordinator commit.
        if not event_id.strip():
            raise ValueError("event_id must be non-empty")
        delivery = self._delivery_frames.get(event_id)
        if delivery is None:
            raise WorkerProcessError("execution delivery is unknown or was not received")
        resolved_correlation = correlation_id or delivery.correlation_id
        if resolved_correlation is None:
            raise WorkerProcessError("execution delivery has no correlation_id")
        frame = self._send_payload(
            WorkerMessageType.EXECUTION_RECEIPT,
            ExecutionReceipt(
                event_id=event_id,
                accepted=bool(accepted),
                committed_at=utc_now() if accepted else None,
                reason=reason,
            ),
            correlation_id=resolved_correlation,
            request_id=delivery.message_id,
        )
        return frame.correlation_id or resolved_correlation
    def no_assignment(self, *, wait_seconds: float = 0.0) -> str:
        # Answer the latest typed acquire request with a bounded idle delay.
        frame = self._send_payload(
            WorkerMessageType.NO_ASSIGNMENT,
            NoAssignment(max(0.0, float(wait_seconds))),
            request_id=self._acquire_request_id(),
        )
        self._last_acquire_frame = None
        return frame.message_id
    def cancel_assignment(
        self,
        assignment: ShardAssignment,
        *,
        reason: str,
        correlation_id: str | None = None,
    ) -> str:
        # Send an identity-bound typed cancellation for the active assignment.
        frame = self._send_payload(
            WorkerMessageType.CANCEL_ASSIGNMENT,
            CancelAssignment(
                assignment.campaign_id,
                assignment.shard_id,
                assignment.lease_id,
                assignment.attempt,
                reason,
            ),
            correlation_id=correlation_id,
        )
        return frame.message_id
    def drain(self, *, reason: str = "drain_requested") -> str:
        # Prevent future assignment acquisition through an explicit typed drain command.
        return self._send_payload(
            WorkerMessageType.DRAIN_WORKER,
            DrainWorker(reason),
        ).message_id
    def shutdown(self) -> str | None:
        # Request graceful shutdown through the typed protocol without treating an exited child as an error.
        process = self._process
        if process is None or process.poll() is not None:
            return None
        if self._worker_pid is None or self.instance_id is None:
            return None
        return self._send_payload(
            WorkerMessageType.SHUTDOWN_WORKER,
            ShutdownWorker(),
        ).message_id
    @staticmethod
    def _terminate_pid_tree(pid: int) -> None:
        # Terminate a separately-sessioned engine tree before stopping its persistent worker parent.
        if int(pid) <= 0:
            return
        if os.name == "nt":
            result = subprocess.run(
                ("taskkill", "/PID", str(int(pid)), "/T", "/F"),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            if result.returncode != 0:
                try:
                    os.kill(int(pid), signal.SIGTERM)
                except (OSError, ProcessLookupError, PermissionError):
                    pass
            return
        try:
            os.killpg(int(pid), signal.SIGKILL)
        except ProcessLookupError:
            return
        except PermissionError:
            try:
                os.kill(int(pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                return
    def terminate_tree(self) -> None:
        # Kill engine descendants, the authoritative agent PID and any distinct launcher process.
        child_pid = self._engine_child_pid
        if child_pid is not None:
            self._terminate_pid_tree(int(child_pid))
        worker_pid = self._worker_pid
        process = self._process
        launcher_pid = int(process.pid) if process is not None and process.pid is not None else None
        if worker_pid is not None:
            self._terminate_pid_tree(int(worker_pid))
        if process is not None and process.poll() is None and launcher_pid != worker_pid:
            try:
                terminate_process_tree(process)
            except (OSError, subprocess.SubprocessError):
                pass
    def wait_for_frame(
        self,
        message_type: WorkerMessageType,
        *,
        timeout: float = 5.0,
        correlation_id: str | None = None,
    ) -> WorkerProtocolFrame:
        # Wait for one concrete typed worker frame while retaining unrelated frames for later consumers.
        deadline = time.monotonic() + max(0.01, float(timeout))
        for index, frame in enumerate(tuple(self._backlog)):
            if frame.message_type == message_type and (
                correlation_id is None or frame.correlation_id == correlation_id
            ):
                del self._backlog[index]
                return frame
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"worker frame timed out: {message_type.value}; stderr={self.stderr_text!r}"
                )
            try:
                item = self._events.get(timeout=remaining)
            except Empty as exc:
                raise TimeoutError(
                    f"worker frame timed out: {message_type.value}; stderr={self.stderr_text!r}"
                ) from exc
            if item is None:
                process = self._process
                code = process.poll() if process is not None else None
                raise WorkerProcessError(
                    f"worker frame stream closed before {message_type.value}; "
                    f"code={code}; stderr={self.stderr_text}"
                )
            if isinstance(item, BaseException):
                raise WorkerProcessError(str(item)) from item
            if item.message_type == WorkerMessageType.PROTOCOL_ERROR:
                payload = item.payload
                detail = payload.error if isinstance(payload, WorkerProtocolError) else "worker failed"
                raise WorkerProcessError(detail)
            if item.message_type == message_type and (
                correlation_id is None or item.correlation_id == correlation_id
            ):
                return item
            if item.message_type != WorkerMessageType.WORKER_HEARTBEAT:
                self._backlog.append(item)
    def wait_for(
        self,
        event: str,
        *,
        timeout: float = 5.0,
        correlation_id: str | None = None,
    ) -> Mapping[str, Any]:
        # Preserve the legacy event-shaped projection only for tests and transitional callers.
        event_types = {
            "registered": WorkerMessageType.REGISTER_WORKER,
            "acquire": WorkerMessageType.ACQUIRE_ASSIGNMENT,
            "heartbeat": WorkerMessageType.WORKER_HEARTBEAT,
            "delivery": WorkerMessageType.EXECUTION_DELIVERY,
            "acknowledged": WorkerMessageType.EXECUTION_ACKNOWLEDGED,
            "engine_started": WorkerMessageType.ENGINE_STARTED,
            "engine_stopped": WorkerMessageType.ENGINE_STOPPED,
            "terminated": WorkerMessageType.WORKER_TERMINATED,
            "error": WorkerMessageType.PROTOCOL_ERROR,
        }
        expected_type = event_types.get(event)
        if expected_type is None:
            raise ValueError(f"unknown worker event name: {event}")
        return self.wait_for_frame(
            expected_type,
            timeout=timeout,
            correlation_id=correlation_id,
        ).to_compat_dict()
    def wait(self, timeout: float = 5.0) -> int:
        # Wait for graceful process termination and join both pipe-drain threads.
        process = self._process
        if process is None:
            raise RuntimeError("worker process has not started")
        try:
            code = int(process.wait(timeout=max(0.01, float(timeout))))
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError("worker process did not terminate") from exc
        self._outbound.put(None)
        for thread in (self._writer_thread, self._reader_thread, self._stderr_thread):
            if thread is not None:
                thread.join(timeout=2.0)
        self._unregister_process_id(process.pid)
        if not self._registered_worker_is_alive():
            self._unregister_process_id(self._worker_pid)
        return code
    def _registered_worker_is_alive(self) -> bool:
        # Match the registered PID to its immutable birth token so PID reuse is never terminated accidentally.
        process_id = self._worker_pid
        birth_token = self._worker_process_birth_token
        return (
            process_id is not None
            and bool(birth_token)
            and current_process_birth_token(int(process_id)) == birth_token
        )
    def _terminate_registered_worker(self, timeout: float = 3.0) -> bool:
        # Terminate an exact redirected worker incarnation and wait until its birth token disappears.
        if not self._registered_worker_is_alive() or self._worker_pid is None:
            return True
        self._terminate_pid_tree(int(self._worker_pid))
        deadline = time.monotonic() + max(0.1, float(timeout))
        while time.monotonic() < deadline:
            if not self._registered_worker_is_alive():
                self._unregister_process_id(self._worker_pid)
                return True
            time.sleep(0.05)
        return not self._registered_worker_is_alive()
    @staticmethod
    def _is_idempotent_pipe_close_error(
        process: subprocess.Popen[str],
        error: OSError,
    ) -> bool:
        # Accept an invalid pipe descriptor as already closed only after the child has exited.
        return process.poll() is not None and error.errno in {errno.EBADF, errno.EINVAL}
    @staticmethod
    def _close_streams(process: subprocess.Popen[str]) -> tuple[str, ...]:
        # Close every parent-side pipe and return bounded stream-specific cleanup failures.
        errors: list[str] = []
        for name, stream in (
            ("stdin", process.stdin),
            ("stdout", process.stdout),
            ("stderr", process.stderr),
        ):
            if stream is None or stream.closed:
                continue
            try:
                stream.close()
            except OSError as exc:
                if PersistentWorkerProcess._is_idempotent_pipe_close_error(process, exc):
                    continue
                errors.append(f"{name}: {exc}")
            except ValueError as exc:
                errors.append(f"{name}: {exc}")
        return tuple(errors)
    def _join_lifecycle_threads(self, timeout: float) -> tuple[str, ...]:
        # Join every worker transport thread and return names that remain alive.
        for thread in (self._writer_thread, self._reader_thread, self._stderr_thread):
            if thread is not None:
                thread.join(timeout=max(0.01, float(timeout)))
        return tuple(
            thread.name
            for thread in (self._writer_thread, self._reader_thread, self._stderr_thread)
            if thread is not None and thread.is_alive()
        )
    def close(self) -> None:
        # Gracefully stop the child and fail closed if any process, pipe, or transport thread survives cleanup.
        process = self._process
        if process is None:
            return
        cleanup_errors: list[str] = []
        cleanup_diagnostics: list[str] = []
        if process.poll() is None:
            try:
                self.shutdown()
                process.wait(timeout=5.0)
            except (OSError, TimeoutError, subprocess.TimeoutExpired, WorkerProcessError) as exc:
                cleanup_diagnostics.append(f"graceful shutdown: {exc}")
                try:
                    self.terminate_tree()
                    process.wait(timeout=3.0)
                except (OSError, subprocess.TimeoutExpired) as terminate_exc:
                    cleanup_diagnostics.append(f"tree termination: {terminate_exc}")
        if self._registered_worker_is_alive() and not self._terminate_registered_worker():
            cleanup_errors.append(
                "registered worker remained alive after exact PID/birth-token termination: "
                f"worker_pid={self._worker_pid}; birth_token={self._worker_process_birth_token}"
            )
        if process.poll() is None:
            try:
                process.kill()
                process.wait(timeout=2.0)
            except (OSError, subprocess.TimeoutExpired) as exc:
                cleanup_diagnostics.append(f"direct kill: {exc}")
        self._outbound.put(None)
        alive_threads = self._join_lifecycle_threads(2.0)
        if alive_threads:
            cleanup_errors.extend(self._close_streams(process))
            alive_threads = self._join_lifecycle_threads(2.0)
        cleanup_errors.extend(self._close_streams(process))
        if process.poll() is None:
            cleanup_errors.append(
                f"worker process remained alive: pid={process.pid}; diagnostics={cleanup_diagnostics}"
            )
        else:
            self._unregister_process_id(process.pid)
            if not self._registered_worker_is_alive():
                self._unregister_process_id(self._worker_pid)
        if alive_threads:
            cleanup_errors.append("worker transport threads remained alive: " + ", ".join(alive_threads))
        if cleanup_errors:
            raise WorkerProcessError(
                "persistent worker cleanup failed: "
                f"launcher_pid={process.pid}; worker_pid={self._worker_pid}; "
                f"returncode={process.poll()}; errors={cleanup_errors}; "
                f"diagnostics={cleanup_diagnostics}; stderr={self.stderr_text!r}"
            )
    def __enter__(self) -> "PersistentWorkerProcess":
        # Start the subprocess when entering a lifecycle-safe test or coordinator adapter scope.
        return self.start()
    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        # Always release the child process and its pipe-drain threads on scope exit.
        del exc_type, exc, traceback
        self.close()
__all__ = ["PersistentWorkerProcess", "WorkerProcessError"]
