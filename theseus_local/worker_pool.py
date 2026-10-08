"""Coordinator-owned worker workspace and process registry for local E13 execution."""
from __future__ import annotations
import os
import shutil
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Mapping, MutableMapping
from test_intelligence_unified_v1.io_utils import atomic_write_json, read_json, stable_hash, utc_now_iso
from theseus_contracts import CampaignId, HeartbeatReceipt, ShardAssignment, WorkerHeartbeat
from gallifrey_mutation import MutationWorkerState, Success
from .capabilities import detect_local_capabilities, select_workspace_backend
from .locking import InterProcessFileLock
from .workspace import WorkspaceProvider, _tree_fingerprint
from .worker_runtime import AgentRun, SpoolEntry, WorkerAgent

def _metric_add(metrics: MutableMapping[str, float] | None, name: str, value: float) -> None:
    # Keep optional data-plane telemetry additive so legacy callers retain the path-only API.
    if metrics is None:
        return
    try:
        normalized = max(0.0, float(value))
    except (TypeError, ValueError):
        return
    metrics[name] = float(metrics.get(name, 0.0)) + normalized

def _tree_file_bytes(root: Path) -> int:
    # Estimate logical file bytes from stable metadata without rereading file contents.
    total = 0
    try:
        paths = root.rglob("*")
        for path in paths:
            try:
                if path.is_file():
                    total += max(0, int(path.stat().st_size))
            except OSError:
                continue
    except OSError:
        return 0
    return total
class WorkerRegistryProjection:
    """Read-only JSON projection rebuilt exclusively from authoritative Gallifrey worker aggregates."""
    def __init__(self, path: Path, *, campaign_id: str, store: Any) -> None:
        # Retain only the projection destination and authoritative repository dependency.
        self.path = Path(path)
        self.campaign_id = str(campaign_id)
        self.store = store
        self.path.parent.mkdir(parents=True, exist_ok=True)
    @staticmethod
    def _state(worker: Any) -> str:
        # Preserve the established operator projection while keeping domain state authoritative.
        if worker.status == MutationWorkerState.STOPPED:
            return "complete"
        if worker.status == MutationWorkerState.FAILED:
            return "error"
        return worker.status.value
    @classmethod
    def _row(cls, worker: Any) -> dict[str, Any]:
        # Convert one immutable aggregate into the legacy diagnostic JSON shape.
        child_process_id = worker.child_process_id
        child_process_birth_token = worker.child_process_birth_token
        if child_process_id is None:
            child_process_id = worker.last_child_process_id
            child_process_birth_token = worker.last_child_process_birth_token
        return {
            "worker_id": worker.worker_id,
            "campaign_id": worker.campaign_id.value,
            "instance_id": worker.identity.instance_id,
            "process_birth_token": worker.identity.process_birth_token,
            "pid": worker.identity.process_id,
            "launcher_pid": worker.launcher_process_id,
            "launcher_process_birth_token": worker.launcher_process_birth_token,
            "child_process_id": child_process_id,
            "child_process_birth_token": child_process_birth_token,
            "workspace": worker.workspace,
            "spool_path": worker.spool_path,
            "capabilities": worker.capabilities.to_dict(),
            "state": cls._state(worker),
            "heartbeat_seq": worker.heartbeat_sequence,
            "last_heartbeat_at": worker.last_heartbeat_at or None,
            "current_shard_id": worker.current_shard_id,
            "current_mutant_id": worker.current_mutant_id,
            "lease_id": worker.current_lease_id,
            "attempt": worker.current_attempt,
            "completed_mutants": worker.completed_mutants,
            "completed_assignments": worker.completed_assignments,
            "workspace_healthy": worker.workspace_healthy,
            "revision_number": worker.revision_number,
        }
    def refresh(self) -> tuple[dict[str, Any], ...]:
        # Rebuild the complete file from SQLite so deletion or corruption never changes scheduling decisions.
        result = self.store.list_workers(CampaignId(self.campaign_id))
        if not isinstance(result, Success):
            code = getattr(result, "code", "worker_registry_projection_failed")
            message = getattr(result, "message", str(result))
            raise RuntimeError(f"{code}: {message}")
        rows = tuple(self._row(worker) for worker in result.value)
        atomic_write_json(
            self.path,
            {
                "schema_version": 2,
                "source": "gallifrey.mutation_workers",
                "campaign_id": self.campaign_id,
                "registry_fingerprint": stable_hash(rows),
                "updated_at": utc_now_iso(),
                "workers": list(rows),
            },
            durability="normal",
            category="worker_registry_projection",
        )
        return rows
    def snapshot(self) -> tuple[dict[str, Any], ...]:
        # Read a fresh authoritative snapshot instead of trusting existing JSON bytes.
        return self.refresh()
WorkerRegistry = WorkerRegistryProjection
class PersistentWorkerSupervisor:
    """Startup replay and repeated assignment acquisition for one long-lived WorkerAgent."""
    def __init__(self, agent: WorkerAgent) -> None:
        # Keep one agent instance alive across startup replay, work stealing and follow-on shards.
        self.agent = agent
    def replay_pending(self, commit: Callable[[SpoolEntry], None]) -> tuple[str, ...]:
        # Replay every pending durable delivery and validate per-mutant salvage events before new work.
        self.agent.spool.mutant_events()
        replayed: list[str] = []
        for entry in self.agent.spool.pending():
            commit(entry)
            self.agent.spool.acknowledge(entry.event_id)
            replayed.append(entry.event_id)
        return tuple(replayed)
    def acquire_loop(
        self,
        acquire: Callable[[], ShardAssignment | None],
        execute: Callable[[ShardAssignment], Mapping[str, Any]],
        commit: Callable[[ShardAssignment, AgentRun], None],
        *,
        heartbeat: Callable[[WorkerHeartbeat], HeartbeatReceipt] | None = None,
        max_assignments: int | None = None,
    ) -> tuple[AgentRun, ...]:
        # Acquire the next shard repeatedly so an idle worker can perform deterministic work stealing.
        completed: list[AgentRun] = []
        while max_assignments is None or len(completed) < max(0, int(max_assignments)):
            assignment = acquire()
            if assignment is None:
                break
            delivery = self.agent.run_assignment(
                assignment,
                lambda assignment=assignment: execute(assignment),
                heartbeat=heartbeat,
            )
            try:
                commit(assignment, delivery)
                self.agent.acknowledge(delivery.event_id)
            except BaseException:
                self.agent.stop()
                raise
            completed.append(delivery)
        return tuple(completed)
def _campaign_workspace_fingerprint(source: Path, campaign_id: str) -> str | None:
    # Reuse the coordinator-verified campaign workspace identity when its sibling ownership manifest matches exactly.
    if source.name != str(campaign_id):
        return None
    ownership_manifest = source.parent / f"{campaign_id}.ownership.json"
    if ownership_manifest.is_symlink() or not ownership_manifest.is_file():
        return None
    value = read_json(ownership_manifest)
    if not isinstance(value, Mapping) or value.get("campaign_id") != str(campaign_id):
        return None
    fingerprint = value.get("workspace_fingerprint")
    return str(fingerprint) if isinstance(fingerprint, str) and fingerprint else None
def prepare_worker_workspace(
    source: Path,
    destination: Path,
    *,
    worker_id: str,
    campaign_id: str,
    metrics: MutableMapping[str, float] | None = None,
) -> Path:
    # Materialize one worker workspace while exposing bytes and backend evidence without changing isolation semantics.
    source = source.resolve()
    destination = destination.resolve()
    started = time.perf_counter()
    if destination.is_symlink():
        raise ValueError("worker workspace must not be a symlink")
    manifest = destination.parent / f"{destination.name}.ownership.json"
    expected_source_fingerprint = _campaign_workspace_fingerprint(source, campaign_id)
    if destination.exists():
        source_fingerprint = expected_source_fingerprint or _tree_fingerprint(source)
        if not manifest.is_file():
            raise ValueError("worker workspace has no ownership manifest")
        value = read_json(manifest)
        if not isinstance(value, Mapping):
            raise ValueError("worker workspace ownership manifest is invalid")
        if value.get("worker_id") != str(worker_id) or value.get("campaign_id") != str(campaign_id):
            raise ValueError("worker workspace ownership mismatch")
        if value.get("source_fingerprint") != source_fingerprint:
            raise ValueError("worker workspace source changed")
        workspace_fingerprint = _tree_fingerprint(destination)
        if value.get("workspace_fingerprint") != workspace_fingerprint or workspace_fingerprint != source_fingerprint:
            raise ValueError("worker workspace integrity failed")
        _metric_add(metrics, "workspace_cache_hits", 1.0)
        _metric_add(metrics, "workspace_setup_seconds", time.perf_counter() - started)
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    _metric_add(metrics, "workspace_cache_misses", 1.0)
    capabilities = detect_local_capabilities(destination.parent)
    selected_backend = select_workspace_backend(capabilities, "hardlink-cow")
    try:
        if selected_backend == "hardlink-cow":
            try:
                linked_files, copied_files = WorkspaceProvider._hardlink_tree(source, destination)
            except OSError:
                # A capability can change between the disposable probe and the tree operation; fall back before attempt.
                shutil.rmtree(destination, ignore_errors=True)
                WorkspaceProvider._copy_tree(source, destination)
                selected_backend = "copy"
                linked_files = 0
                copied_files = sum(1 for path in destination.rglob("*") if path.is_file())
            # A mixed tree is not a valid hardlink-cow proof: rebuild it as a plain copy.
            if linked_files and copied_files:
                shutil.rmtree(destination, ignore_errors=True)
                WorkspaceProvider._copy_tree(source, destination)
                selected_backend = "copy"
                linked_files = 0
                copied_files = sum(1 for path in destination.rglob("*") if path.is_file())
        else:
            WorkspaceProvider._copy_tree(source, destination)
            linked_files = 0
            copied_files = sum(1 for path in destination.rglob("*") if path.is_file())
        source_fingerprint = _tree_fingerprint(source)
        if expected_source_fingerprint is not None and source_fingerprint != expected_source_fingerprint:
            raise ValueError("worker workspace copy does not match campaign workspace identity")
        workspace_backend = selected_backend if linked_files else "copy"
        workspace_fingerprint = source_fingerprint
        if workspace_backend == "copy":
            workspace_fingerprint = _tree_fingerprint(destination)
            if workspace_fingerprint != source_fingerprint:
                raise ValueError("worker workspace copy does not match campaign workspace identity")
        logical_bytes = float(_tree_file_bytes(source))
        copied_bytes = float(_tree_file_bytes(destination)) if workspace_backend == "copy" else 0.0
        _metric_add(metrics, "workspace_bytes_read", logical_bytes + copied_bytes)
        _metric_add(metrics, "workspace_bytes_written", copied_bytes)
        _metric_add(metrics, "workspace_physical_bytes_allocated", copied_bytes)
        _metric_add(metrics, "workspace_linked_files", float(linked_files))
        _metric_add(metrics, "workspace_copied_files", float(copied_files))
    except BaseException:
        cleanup_started = time.perf_counter()
        shutil.rmtree(destination, ignore_errors=True)
        _metric_add(metrics, "workspace_cleanup_seconds", time.perf_counter() - cleanup_started)
        raise
    try:
        atomic_write_json(
            manifest,
            {
                "schema_version": 2,
                "worker_id": str(worker_id),
                "campaign_id": str(campaign_id),
                "source_workspace": str(source),
                "source_fingerprint": source_fingerprint,
                "workspace_fingerprint": workspace_fingerprint,
                "workspace_backend": workspace_backend,
                "linked_files": linked_files,
                "copied_files": copied_files,
                "capability_proof": {
                    **capabilities.to_dict(),
                    "selected_workspace_backend": workspace_backend,
                },
                "created_at": utc_now_iso(),
            },
            durability="critical",
            category="worker_workspace_manifest",
        )
    except BaseException:
        cleanup_started = time.perf_counter()
        shutil.rmtree(destination, ignore_errors=True)
        manifest.unlink(missing_ok=True)
        _metric_add(metrics, "workspace_cleanup_seconds", time.perf_counter() - cleanup_started)
        raise
    _metric_add(metrics, "workspace_setup_seconds", time.perf_counter() - started)
    return destination
def worker_workspace_backend(workspace: Path) -> str:
    # Read the persisted worker materialization backend without trusting directory names or scheduler assumptions.
    resolved = Path(workspace).resolve()
    manifest = resolved.parent / f"{resolved.name}.ownership.json"
    if not manifest.is_file() or manifest.is_symlink():
        return "copy"
    value = read_json(manifest)
    if not isinstance(value, Mapping):
        return "copy"
    backend = str(value.get("workspace_backend") or "copy")
    return backend if backend in {"copy", "hardlink-cow"} else "copy"
def materialize_prepared_snapshot(
    source: Path,
    destination: Path,
    *,
    expected_snapshot_id: str | None = None,
    cache_root: Path | None = None,
    metrics: MutableMapping[str, float] | None = None,
) -> Path:
    # Reuse one verified immutable snapshot across attempts while keeping each destination read-only and atomic.
    started = time.perf_counter()
    source = Path(source).resolve()
    destination = Path(destination).resolve()
    if not source.is_file():
        raise ValueError("coordinator prepared snapshot is missing")
    destination.parent.mkdir(parents=True, exist_ok=True)
    expected = str(expected_snapshot_id or "").strip()
    cache_path: Path | None = None
    raw: Mapping[str, Any] | None = None
    cache_hit = False
    cache_miss = False
    lookup_started = time.perf_counter()
    if cache_root is not None and expected:
        resolved_cache_root = Path(cache_root).resolve()
        resolved_cache_root.mkdir(parents=True, exist_ok=True)
        cache_key = stable_hash({"prepared_snapshot_id": expected})[:32]
        cache_path = resolved_cache_root / f"{cache_key}.snapshot.json"
        with InterProcessFileLock(resolved_cache_root / ".prepared-snapshot.lock"):
            if cache_path.is_file():
                try:
                    cached = read_json(cache_path)
                    if (
                        isinstance(cached, Mapping)
                        and stable_hash(cached)[:32] == expected
                    ):
                        raw = cached
                        cache_hit = True
                    else:
                        cache_path.unlink(missing_ok=True)
                except (OSError, TypeError, ValueError):
                    cache_path.unlink(missing_ok=True)
            if raw is None:
                candidate = read_json(source)
                if not isinstance(candidate, Mapping):
                    raise ValueError("coordinator prepared snapshot must be an object")
                if stable_hash(candidate)[:32] != expected:
                    raise ValueError("coordinator prepared snapshot identity does not match the execution plan")
                atomic_write_json(
                    cache_path,
                    candidate,
                    durability="critical",
                    category="prepared_snapshot_cache",
                )
                try:
                    cache_path.chmod(0o444)
                except OSError:
                    pass
                raw = candidate
                cache_miss = True
    else:
        raw_value = read_json(source)
        if not isinstance(raw_value, Mapping):
            raise ValueError("coordinator prepared snapshot must be an object")
        raw = raw_value
    _metric_add(metrics, "prepared_snapshot_lookup_seconds", time.perf_counter() - lookup_started)
    if expected and stable_hash(raw)[:32] != expected:
        raise ValueError("coordinator prepared snapshot identity does not match the execution plan")
    if cache_hit:
        _metric_add(metrics, "prepared_snapshot_cache_hits", 1.0)
    if cache_miss:
        _metric_add(metrics, "prepared_snapshot_cache_misses", 1.0)
    if cache_path is not None and cache_path.is_file():
        _metric_add(metrics, "prepared_snapshot_bytes_read", float(cache_path.stat().st_size))
        if cache_miss:
            _metric_add(metrics, "prepared_snapshot_bytes_written", float(cache_path.stat().st_size))
    if destination.is_file():
        existing = read_json(destination)
        if existing != raw:
            raise ValueError("worker prepared snapshot conflicts with coordinator snapshot")
        _metric_add(metrics, "prepared_snapshot_destination_hits", 1.0)
    else:
        if cache_path is not None and cache_path.is_file():
            try:
                os.link(cache_path, destination)
                _metric_add(metrics, "prepared_snapshot_hardlink_hits", 1.0)
            except OSError:
                temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.part")
                try:
                    shutil.copyfile(cache_path, temporary)
                    os.replace(temporary, destination)
                finally:
                    temporary.unlink(missing_ok=True)
                _metric_add(metrics, "prepared_snapshot_bytes_written", float(cache_path.stat().st_size))
        else:
            atomic_write_json(destination, raw, durability="critical", category="prepared_snapshot")
            try:
                _metric_add(metrics, "prepared_snapshot_bytes_written", float(destination.stat().st_size))
            except OSError:
                pass
    try:
        destination.chmod(0o444)
    except OSError:
        pass
    _metric_add(metrics, "prepared_snapshot_materialization_seconds", time.perf_counter() - started)
    return destination
__all__ = ["PersistentWorkerSupervisor", "WorkerRegistry", "materialize_prepared_snapshot", "prepare_worker_workspace", "worker_workspace_backend"]
