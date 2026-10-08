"""Single-worker remote execution runtime built on the proven local backend."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from threading import RLock
from typing import Any, Mapping, TextIO

from theseus_contracts import RemoteExecutionRequest, RemoteExecutionResult, WorkerCapabilities
from theseus_local.artifact_store import ArtifactIntegrityError, ContentAddressedArtifactStore
from theseus_local.capabilities import detect_local_capabilities
from theseus_local.execution_backend import ExecutionRequest, LocalProcessBackend
from theseus_local.isolation import ExecutionPolicy, IsolationPolicyError, resolve_workspace
from theseus_local.locking import InterProcessFileLock, InterProcessLockError
from theseus_local.runtime_identity import (
    RuntimeCompatibilityError,
    RuntimeIdentity,
    assert_runtime_compatible,
    current_runtime_identity,
)


class RemoteWorkerError(RuntimeError):
    """Raised when a remote worker cannot safely bind or execute a request."""


WORKER_JOURNAL_SCHEMA_VERSION = 1


class RemoteWorkerState(str, Enum):
    STARTING = "starting"
    READY = "ready"
    BUSY = "busy"
    DRAINING = "draining"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class RemoteWorkerRegistration:
    worker_id: str
    instance_id: str
    runtime_identity: RuntimeIdentity
    capabilities: WorkerCapabilities
    platform: str
    execution_backends: tuple[str, ...]
    available_slots: int
    state: RemoteWorkerState

    def to_dict(self) -> dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "instance_id": self.instance_id,
            "runtime_identity": self.runtime_identity.to_dict(),
            "capabilities": self.capabilities.to_dict(),
            "platform": self.platform,
            "execution_backends": list(self.execution_backends),
            "available_slots": int(self.available_slots),
            "state": self.state.value,
        }


class RemoteWorkerRuntime:
    """A reference one-worker runtime for protocol and distributed-scheduler tests.

    The worker daemon may be long-lived, but each request is delegated to the
    existing ``LocalProcessBackend`` and therefore still starts a fresh pytest
    process.  The class is transport-neutral: a JSONL, socket, or test harness
    can call the same registration/execute methods.
    """

    def __init__(
        self,
        *,
        worker_id: str,
        root: Path,
        artifact_store: ContentAddressedArtifactStore | None = None,
        backend: LocalProcessBackend | None = None,
        policy: ExecutionPolicy | None = None,
        slots: int = 1,
        runtime_identity: RuntimeIdentity | None = None,
    ) -> None:
        if not str(worker_id).strip():
            raise ValueError("worker_id must be non-empty")
        if int(slots) < 1:
            raise ValueError("slots must be positive")
        self.worker_id = str(worker_id)
        self.instance_id = uuid.uuid4().hex
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.store = artifact_store or ContentAddressedArtifactStore(self.root / "cache")
        self.backend = backend or LocalProcessBackend()
        self.policy = policy or ExecutionPolicy()
        self.slots = int(slots)
        self.runtime_identity = runtime_identity or current_runtime_identity()
        self._state = RemoteWorkerState.STARTING
        self._lock = RLock()
        self._cancelled: set[str] = set()
        self._completed: dict[str, tuple[str, RemoteExecutionResult]] = {}
        self._active: set[str] = set()
        self._journal_path = self.root / "worker-results.json"
        self._journal_lock_path = self.root / ".worker-results.lock"
        self._load_journal()
        self._set_state(RemoteWorkerState.READY)

    @property
    def state(self) -> RemoteWorkerState:
        with self._lock:
            return self._state

    @property
    def completed_attempt_count(self) -> int:
        """Return the durable-result journal cardinality for bounded run metrics."""

        with self._lock:
            return len(self._completed)

    def _set_state(self, state: RemoteWorkerState) -> None:
        with self._lock:
            self._state = state

    def _load_journal(self) -> None:
        """Restore only durably completed attempts; an interrupted attempt has no fake result."""

        try:
            with InterProcessFileLock(self._journal_lock_path):
                if not self._journal_path.is_file():
                    return
                value = json.loads(self._journal_path.read_text(encoding="utf-8"))
                if not isinstance(value, Mapping):
                    raise ValueError("worker result journal must be an object")
                if value.get("schema_version") != WORKER_JOURNAL_SCHEMA_VERSION:
                    raise ValueError(
                        f"unsupported worker journal schema version: {value.get('schema_version')!r}"
                    )
                persisted_fingerprint = value.get("runtime_fingerprint")
                if not isinstance(persisted_fingerprint, str) or not persisted_fingerprint.strip():
                    raise ValueError("worker result journal has no runtime fingerprint")
                if persisted_fingerprint != self.runtime_identity.runtime_fingerprint:
                    raise RuntimeCompatibilityError(
                        "worker result journal runtime fingerprint does not match the current worker"
                    )
                completed = value.get("completed", {})
                if not isinstance(completed, Mapping):
                    raise ValueError("worker result journal completed section must be an object")
                for attempt_id, row in completed.items():
                    if not isinstance(row, Mapping) or not isinstance(row.get("result"), Mapping):
                        raise ValueError("worker result journal row is invalid")
                    self._completed[str(attempt_id)] = (
                        str(row.get("request_json", "")),
                        RemoteExecutionResult.from_dict(row["result"]),
                    )
        except (OSError, ValueError, TypeError, RuntimeCompatibilityError, InterProcessLockError) as exc:
            raise RemoteWorkerError(f"worker result journal is corrupt: {self._journal_path}") from exc

    def _save_journal(self) -> None:
        try:
            with InterProcessFileLock(self._journal_lock_path):
                temporary = self._journal_path.with_name(f".{self._journal_path.name}.{uuid.uuid4().hex}.tmp")
                try:
                    temporary.write_text(
                        json.dumps(
                            {
                                "schema_version": WORKER_JOURNAL_SCHEMA_VERSION,
                                "runtime_fingerprint": self.runtime_identity.runtime_fingerprint,
                                "completed": {
                                    key: {"request_json": value[0], "result": value[1].to_dict()}
                                    for key, value in sorted(self._completed.items())
                                },
                            },
                            ensure_ascii=False,
                            sort_keys=True,
                        )
                        + "\n",
                        encoding="utf-8",
                        newline="\n",
                    )
                    os.replace(temporary, self._journal_path)
                finally:
                    temporary.unlink(missing_ok=True)
        except InterProcessLockError as exc:
            raise RemoteWorkerError(f"worker result journal lock failed: {self._journal_path}") from exc

    @staticmethod
    def _worker_capabilities() -> WorkerCapabilities:
        observed = detect_local_capabilities()
        import os
        import platform
        from platform import python_version

        return WorkerCapabilities(
            platform=platform.system().lower() or os.name,
            architecture=platform.machine() or "unknown",
            python_versions=(python_version(),),
            engine_protocol_versions=(1,),
            workspace_backends=observed.workspace_backends,
            cpu_count=max(1, int(os.cpu_count() or 1)),
        )

    def registration(self) -> RemoteWorkerRegistration:
        """Return the immutable capability/runtime registration sent to a coordinator."""

        return RemoteWorkerRegistration(
            worker_id=self.worker_id,
            instance_id=self.instance_id,
            runtime_identity=self.runtime_identity,
            capabilities=self._worker_capabilities(),
            platform=self.runtime_identity.platform,
            execution_backends=("local-process",),
            available_slots=max(0, self.slots - len(self._active)),
            state=self.state,
        )

    def accept_registration(self, expected_runtime: RuntimeIdentity) -> RemoteWorkerRegistration:
        """Validate version/fingerprint authority before accepting assignments."""

        try:
            assert_runtime_compatible(expected_runtime, self.runtime_identity)
        except RuntimeCompatibilityError as exc:
            self._set_state(RemoteWorkerState.FAILED)
            raise RemoteWorkerError(str(exc)) from exc
        if self.state in {RemoteWorkerState.STOPPED, RemoteWorkerState.FAILED}:
            raise RemoteWorkerError(f"worker is not registerable: {self.state.value}")
        self._set_state(RemoteWorkerState.READY)
        return self.registration()

    def cancel(self, execution_attempt_id: str) -> None:
        """Request cancellation; the local backend terminates the complete child tree."""

        with self._lock:
            self._cancelled.add(str(execution_attempt_id))

    def drain(self) -> None:
        with self._lock:
            if self._state in {RemoteWorkerState.READY, RemoteWorkerState.BUSY}:
                self._state = RemoteWorkerState.DRAINING
                return
            if self._state is RemoteWorkerState.DRAINING:
                return
            raise RemoteWorkerError(f"worker cannot drain from state {self._state.value}")

    def stop(self) -> None:
        with self._lock:
            if self._active:
                self._state = RemoteWorkerState.FAILED
                raise RemoteWorkerError("cannot stop a worker with active executions")
            if self._state is RemoteWorkerState.FAILED:
                raise RemoteWorkerError("cannot stop a failed worker; restart it first")
            self._state = RemoteWorkerState.STOPPED

    def restart(self) -> "RemoteWorkerRuntime":
        """Create a new instance identity while retaining the durable CAS cache."""

        return RemoteWorkerRuntime(
            worker_id=self.worker_id,
            root=self.root,
            artifact_store=ContentAddressedArtifactStore(self.store.root),
            backend=self.backend,
            policy=self.policy,
            slots=self.slots,
            runtime_identity=self.runtime_identity,
        )

    def _preflight(self, request: RemoteExecutionRequest) -> tuple[Path, str, Path]:
        try:
            expected_runtime = RuntimeIdentity.from_dict(request.runtime_identity)
            assert_runtime_compatible(expected_runtime, self.runtime_identity)
            if request.deadline_utc is not None:
                deadline = datetime.fromisoformat(request.deadline_utc.replace("Z", "+00:00"))
                if deadline <= datetime.now(timezone.utc):
                    raise RemoteWorkerError("remote request deadline has expired")
            snapshot = self.store.load_snapshot(request.project_snapshot_id)
            self.store.verify(request.prepared_artifact_id)
            if snapshot.files.get(request.source_path) != request.expected_source_sha256:
                raise ArtifactIntegrityError("prepared mutation source does not match the verified project snapshot")
        except (ArtifactIntegrityError, RuntimeCompatibilityError, ValueError, OSError) as exc:
            raise RemoteWorkerError(f"remote request preflight rejected: {exc}") from exc
        workspace_base = resolve_workspace(self.root / "workspaces", request.workspace_relative)
        attempt_directory = hashlib.sha256(request.execution_attempt_id.encode("utf-8")).hexdigest()[:16]
        # Put the actual workspace inside a disposable attempt container.  A
        # child using ``../`` can therefore never leave residue beside the
        # attempt directory; the whole container is removed in one finally.
        attempt_container = resolve_workspace(workspace_base, f".attempt-{attempt_directory}")
        workspace = resolve_workspace(attempt_container, "workspace")
        attempt_container.mkdir(parents=True, exist_ok=True)
        workspace.mkdir(parents=True, exist_ok=True)
        try:
            self.store.materialize_snapshot(snapshot, workspace)
        except (ArtifactIntegrityError, OSError) as exc:
            shutil.rmtree(attempt_container, ignore_errors=True)
            raise RemoteWorkerError(f"remote snapshot materialization rejected: {exc}") from exc
        return workspace, str(snapshot.files[request.source_path]), attempt_container

    def _verify_workspace(self, workspace: Path, snapshot_files: Mapping[str, str]) -> bool:
        for relative, expected_id in snapshot_files.items():
            candidate = resolve_workspace(workspace, relative)
            if not candidate.is_file():
                return False
            digest = hashlib.sha256()
            with candidate.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            if digest.hexdigest() != expected_id:
                return False
        return True

    def execute(self, request: RemoteExecutionRequest) -> RemoteExecutionResult:
        """Execute one request or replay its already-completed physical result idempotently."""

        request_json = request.to_json()
        with self._lock:
            prior = self._completed.get(request.execution_attempt_id)
            if prior is not None:
                if prior[0] != request_json:
                    raise RemoteWorkerError("duplicate attempt identity has conflicting request bytes")
                return prior[1]
            if self._state not in {RemoteWorkerState.READY, RemoteWorkerState.BUSY}:
                raise RemoteWorkerError(f"worker cannot accept assignment in state {self._state.value}")
            if len(self._active) >= self.slots:
                raise RemoteWorkerError("worker capacity is exhausted")
            self._active.add(request.execution_attempt_id)
            self._state = RemoteWorkerState.BUSY

        workspace: Path | None = None
        attempt_container: Path | None = None
        output_path: Path | None = None
        started = time.perf_counter()
        try:
            workspace, source_hash, attempt_container = self._preflight(request)
            snapshot = self.store.load_snapshot(request.project_snapshot_id)
            output_path = self.root / "outputs" / f"{request.execution_attempt_id}.log"
            result = self.backend.execute(
                ExecutionRequest(
                    execution_id=request.execution_attempt_id,
                    argv=request.argv,
                    cwd=workspace,
                    environment=self.policy.environment(request.environment),
                    timeout_seconds=min(float(request.timeout_seconds), self.policy.timeout_seconds),
                    output_artifact=output_path,
                    cancellation=lambda: request.execution_attempt_id in self._cancelled,
                    deadline_monotonic=time.monotonic()
                    + min(float(request.timeout_seconds), self.policy.timeout_seconds),
                )
            )
            self.policy.validate_result(result)
            healthy = self._verify_workspace(workspace, snapshot.files)
            if not healthy:
                raise RemoteWorkerError("workspace integrity changed during execution")
            stdout_id = self.store.put_file(output_path).artifact_id if output_path.is_file() else None
            remote_result = RemoteExecutionResult(
                execution_attempt_id=request.execution_attempt_id,
                evidence_identity=request.evidence_identity,
                mutation_identity=request.mutation_identity,
                started=True,
                exit_code=result.exit_code,
                timed_out=bool(result.timed_out),
                cancelled=bool(
                    request.execution_attempt_id in self._cancelled
                    or (result.termination or {}).get("reason") == "cancelled"
                ),
                elapsed_seconds=max(0.0, time.perf_counter() - started),
                worker_runtime_fingerprint=self.runtime_identity.runtime_fingerprint,
                workspace_integrity="verified",
                source_sha256=source_hash,
                prepared_artifact_sha256=request.prepared_artifact_id,
                stdout_artifact_id=stdout_id,
                diagnostic_error=None,
            )
        except (RemoteWorkerError, IsolationPolicyError) as exc:
            remote_result = RemoteExecutionResult(
                execution_attempt_id=request.execution_attempt_id,
                evidence_identity=request.evidence_identity,
                mutation_identity=request.mutation_identity,
                started=workspace is not None,
                exit_code=None,
                timed_out=False,
                cancelled=request.execution_attempt_id in self._cancelled,
                elapsed_seconds=max(0.0, time.perf_counter() - started),
                worker_runtime_fingerprint=self.runtime_identity.runtime_fingerprint,
                workspace_integrity="failed",
                source_sha256=request.expected_source_sha256,
                prepared_artifact_sha256=request.prepared_artifact_id,
                diagnostic_error=str(exc),
            )
        finally:
            if output_path is not None:
                output_path.unlink(missing_ok=True)
            if workspace is not None:
                shutil.rmtree(attempt_container or workspace, ignore_errors=True)
            with self._lock:
                self._active.discard(request.execution_attempt_id)
                if self._state == RemoteWorkerState.BUSY:
                    self._state = RemoteWorkerState.READY
                self._completed[request.execution_attempt_id] = (request_json, remote_result)
                self._save_journal()
        return remote_result

    def serve_jsonl(self, input_stream: TextIO | None = None, output_stream: TextIO | None = None) -> int:
        """Serve a minimal newline-delimited transport without introducing a second execution path."""

        source = input_stream or sys.stdin
        target = output_stream or sys.stdout
        target.write(json.dumps({"message_type": "registration", "registration": self.registration().to_dict()}, ensure_ascii=False, sort_keys=True) + "\n")
        target.flush()
        for line in source:
            if not line.strip():
                continue
            try:
                value = json.loads(line)
                if isinstance(value, Mapping) and value.get("message_type") == "shutdown":
                    self.stop()
                    break
                request = RemoteExecutionRequest.from_dict(value if isinstance(value, Mapping) else {})
                result = self.execute(request)
                payload = {"message_type": "result", "result": result.to_dict()}
            except Exception as exc:
                payload = {
                    "message_type": "error",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            target.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
            target.flush()
        if self.state not in {RemoteWorkerState.STOPPED, RemoteWorkerState.FAILED}:
            self.stop()
        return 0 if self.state is RemoteWorkerState.STOPPED else 1


def main(argv: list[str] | None = None) -> int:
    """Run the transport-neutral reference worker for local integration tests."""

    import argparse

    parser = argparse.ArgumentParser(prog="theseus-remote-worker")
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--root", required=True)
    args = parser.parse_args(argv)
    return RemoteWorkerRuntime(worker_id=args.worker_id, root=Path(args.root)).serve_jsonl()


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "RemoteWorkerError",
    "RemoteWorkerRegistration",
    "RemoteWorkerRuntime",
    "RemoteWorkerState",
    "main",
]
