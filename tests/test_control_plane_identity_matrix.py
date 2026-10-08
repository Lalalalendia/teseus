from __future__ import annotations

import sys
import time
from dataclasses import replace
from pathlib import Path

import pytest

from theseus_contracts.ids import ArtifactId, CampaignId, ExecutionId, MutantId, WorkerId
from theseus_contracts.remote_protocol import RemoteProtocolError
from theseus_local.distributed import DistributedScheduler, SchedulerError
from theseus_local.remote_worker import RemoteWorkerRuntime
from theseus_local.runtime_identity import current_runtime_identity

from post_pr63_helpers import physical_result, remote_fixture


def test_identifier_domains_do_not_compare_equal() -> None:
    assert MutantId("same") != ExecutionId("same")
    assert WorkerId("same") != ArtifactId("same")
    assert CampaignId("same") == CampaignId("same")


def test_retry_preserves_evidence_identity_but_changes_execution_attempt_identity(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    scheduler = DistributedScheduler(tmp_path / "scheduler.json", lease_seconds=0.01)
    scheduler.register_worker(worker.registration())
    assert scheduler.submit(request)
    first = scheduler.claim(worker.worker_id)
    assert first is not None
    time.sleep(0.03)
    assert scheduler.expire_leases()
    second = scheduler.claim(worker.worker_id)
    assert second is not None
    assert first.evidence_identity == second.evidence_identity == request.evidence_identity
    assert first.execution_attempt_id != second.execution_attempt_id

    late = physical_result(first.request, worker_fingerprint=worker.runtime_identity.runtime_fingerprint)
    assert scheduler.complete(worker.worker_id, first.lease_id, late).authoritative is False
    accepted = scheduler.complete(
        worker.worker_id,
        second.lease_id,
        physical_result(second.request, worker_fingerprint=worker.runtime_identity.runtime_fingerprint),
    )
    assert accepted.authoritative is True
    assert scheduler.snapshot()["authoritative"] == (request.evidence_identity,)


def test_retry_can_move_to_a_different_worker_without_changing_semantic_identity(tmp_path: Path) -> None:
    _, request, first_worker = remote_fixture(tmp_path)
    second_worker = RemoteWorkerRuntime(worker_id="worker-1", root=tmp_path / "worker-1")
    scheduler = DistributedScheduler(tmp_path / "scheduler.json", lease_seconds=0.01)
    scheduler.register_worker(first_worker.registration())
    scheduler.register_worker(second_worker.registration())
    assert scheduler.submit(request)
    first = scheduler.claim(first_worker.worker_id)
    assert first is not None
    scheduler.unregister_worker(first_worker.worker_id, reason="crash")
    retry = scheduler.claim(second_worker.worker_id)
    assert retry is not None
    assert retry.evidence_identity == first.evidence_identity
    assert retry.request.mutation_identity == first.request.mutation_identity
    result = second_worker.execute(retry.request)
    assert scheduler.complete(second_worker.worker_id, retry.lease_id, result).authoritative is True


def test_mutation_identity_is_independent_of_temporary_project_root(tmp_path: Path) -> None:
    from theseus_local.ai_mutation import MutationCandidate, prepare_candidate

    roots = [tmp_path / "a", tmp_path / "b"]
    for root in roots:
        root.mkdir()
        (root / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
    candidate = MutationCandidate("module.py", 1, 8, "1", "2")
    first = prepare_candidate(candidate, roots[0])
    second = prepare_candidate(candidate, roots[1])
    assert first.mutation_identity == second.mutation_identity
    assert first.source_sha256_before == second.source_sha256_before


def test_runtime_mismatch_is_rejected_before_process_start(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    mismatch = current_runtime_identity(theseus_version="99.0.0")
    scheduler = DistributedScheduler(
        tmp_path / "scheduler.json",
        expected_runtime_identity=current_runtime_identity(),
    )
    with pytest.raises(SchedulerError, match="incompatible"):
        scheduler.register_worker(
            replace(worker.registration(), runtime_identity=mismatch).to_dict()
        )
    mismatched_request = replace(request, runtime_identity=mismatch.to_dict())
    result = worker.execute(mismatched_request)
    assert result.started is False
    assert result.workspace_integrity == "failed"


def test_artifact_identity_mismatch_has_zero_execution(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path, command=(sys.executable, "-c", "raise SystemExit(91)"))
    mismatch = replace(request, prepared_artifact_id="0" * 64)
    result = worker.execute(mismatch)
    assert result.started is False
    assert result.exit_code is None
    assert "artifact" in (result.diagnostic_error or "").lower()


def test_required_protocol_identity_fields_fail_closed(tmp_path: Path) -> None:
    _, request, _ = remote_fixture(tmp_path)
    payload = request.to_dict()
    for field in (
        "execution_attempt_id",
        "evidence_identity",
        "mutation_identity",
        "project_snapshot_id",
        "prepared_artifact_id",
        "source_path",
        "expected_source_sha256",
    ):
        candidate = dict(payload)
        candidate.pop(field)
        with pytest.raises((RemoteProtocolError, ValueError, TypeError)):
            type(request).from_dict(candidate)
