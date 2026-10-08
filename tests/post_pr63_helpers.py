from __future__ import annotations

import hashlib
import sys
from pathlib import Path
from typing import Iterable

from theseus_contracts import RemoteExecutionRequest, RemoteExecutionResult
from theseus_local.acceptance import assert_resource_quiescence
from theseus_local.artifact_store import ContentAddressedArtifactStore
from theseus_local.distributed import DistributedScheduler
from theseus_local.remote_worker import RemoteWorkerRuntime
from theseus_local.runtime_identity import current_runtime_identity


def remote_fixture(
    tmp_path: Path,
    *,
    command: tuple[str, ...] | None = None,
    timeout_seconds: float = 5.0,
    evidence_identity: str = "evidence-0",
    mutation_identity: str = "mutation-0",
    attempt_id: str = "attempt-0",
) -> tuple[ContentAddressedArtifactStore, RemoteExecutionRequest, RemoteWorkerRuntime]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    project = tmp_path / "project"
    project.mkdir()
    (project / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
    store = ContentAddressedArtifactStore(tmp_path / "coordinator-cache")
    snapshot = store.create_snapshot(project)
    store.persist_snapshot(snapshot)
    prepared = store.put_bytes(b"prepared immutable mutation")
    request = RemoteExecutionRequest(
        execution_attempt_id=attempt_id,
        evidence_identity=evidence_identity,
        mutation_identity=mutation_identity,
        runtime_identity=current_runtime_identity().to_dict(),
        project_snapshot_id=snapshot.snapshot_id,
        prepared_artifact_id=prepared.artifact_id,
        test_plan_identity="test-plan-0",
        argv=command or (sys.executable, "-c", "print('remote-ok')"),
        source_path="module.py",
        expected_source_sha256=snapshot.files["module.py"],
        timeout_seconds=timeout_seconds,
    )
    worker = RemoteWorkerRuntime(
        worker_id="worker-0",
        root=tmp_path / "worker",
        artifact_store=store,
    )
    return store, request, worker


def physical_result(
    request: RemoteExecutionRequest,
    *,
    worker_fingerprint: str | None = None,
    exit_code: int | None = 0,
    started: bool = True,
    timed_out: bool = False,
    cancelled: bool = False,
    workspace_integrity: str = "verified",
) -> RemoteExecutionResult:
    return RemoteExecutionResult(
        execution_attempt_id=request.execution_attempt_id,
        evidence_identity=request.evidence_identity,
        mutation_identity=request.mutation_identity,
        started=started,
        exit_code=exit_code,
        timed_out=timed_out,
        cancelled=cancelled,
        elapsed_seconds=0.001,
        worker_runtime_fingerprint=worker_fingerprint or current_runtime_identity().runtime_fingerprint,
        workspace_integrity=workspace_integrity,
        source_sha256=request.expected_source_sha256,
        prepared_artifact_sha256=request.prepared_artifact_id,
    )


def scheduler_with_worker(
    tmp_path: Path,
    request: RemoteExecutionRequest,
    *,
    lease_seconds: float = 5.0,
    worker_id: str = "worker-0",
) -> tuple[DistributedScheduler, RemoteWorkerRuntime]:
    worker = RemoteWorkerRuntime(worker_id=worker_id, root=tmp_path / worker_id)
    scheduler = DistributedScheduler(tmp_path / "scheduler.json", lease_seconds=lease_seconds)
    scheduler.register_worker(worker.registration())
    assert scheduler.submit(request)
    return scheduler, worker


def assert_single_authoritative_evidence(scheduler: DistributedScheduler, evidence_identity: str) -> None:
    assert scheduler.snapshot()["authoritative"].count(evidence_identity) <= 1


def assert_no_live_attempt_processes() -> None:
    assert_resource_quiescence()


def assert_campaign_accounted(scheduler: DistributedScheduler, evidence_identities: set[str]) -> None:
    snapshot = scheduler.snapshot()
    assert set(snapshot["authoritative"]) <= evidence_identities
    assert set(snapshot["pending"]) <= evidence_identities
    assert scheduler.pending_count() == 0
    assert scheduler.active_lease_count() == 0
    assert set(snapshot["authoritative"]) == evidence_identities
    assert len(snapshot["authoritative"]) == len(evidence_identities)


def assert_artifact_verified(store: ContentAddressedArtifactStore, artifact_id: str) -> None:
    record = store.verify(artifact_id)
    assert record.artifact_id == artifact_id
    assert hashlib.sha256(store.get_bytes(artifact_id)).hexdigest() == artifact_id


def assert_no_active_stale_leases(scheduler: DistributedScheduler) -> None:
    assert scheduler.active_lease_count() == 0
    assert all(str(item.get("reason", "")).strip() for item in scheduler.stale_records())


def snapshot_files(root: Path, *, exclude: Iterable[str] = ()) -> dict[str, str]:
    """Capture content identities for a fixture tree without depending on its absolute path."""

    excluded = {str(item).replace("\\", "/") for item in exclude}
    result: dict[str, str] = {}
    for path in sorted(Path(root).rglob("*"), key=lambda item: item.as_posix()):
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix()
        if relative in excluded:
            continue
        result[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def assert_source_intact(root: Path, expected: dict[str, str]) -> None:
    """Require a failed/hostile scenario to preserve every baseline source byte."""

    assert snapshot_files(root) == expected
