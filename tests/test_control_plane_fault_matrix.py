from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import pytest

from theseus_local.ai_analysis import analyze_report, write_advisory_analysis
from theseus_local.artifact_store import ArtifactIntegrityError, ContentAddressedArtifactStore
from theseus_local.distributed import DistributedScheduler
from theseus_local.remote_worker import RemoteWorkerRuntime

from post_pr63_helpers import (
    assert_artifact_verified,
    assert_campaign_accounted,
    assert_no_active_stale_leases,
    assert_no_live_attempt_processes,
    assert_single_authoritative_evidence,
    physical_result,
    remote_fixture,
)


def test_fault_matrix_pre_execution_artifact_unavailable_has_zero_execution(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    missing = replace(request, prepared_artifact_id="f" * 64)
    result = worker.execute(missing)
    assert result.started is False
    assert result.workspace_integrity == "failed"
    assert_no_live_attempt_processes()


def test_fault_matrix_worker_disappears_then_retry_has_one_authority(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    scheduler = DistributedScheduler(tmp_path / "scheduler.json")
    scheduler.register_worker(worker.registration())
    scheduler.submit(request)
    first = scheduler.claim(worker.worker_id)
    assert first is not None
    scheduler.unregister_worker(worker.worker_id, reason="worker_disappeared")
    replacement = RemoteWorkerRuntime(worker_id="replacement", root=tmp_path / "replacement")
    scheduler.register_worker(replacement.registration())
    retry = scheduler.claim(replacement.worker_id)
    assert retry is not None
    accepted = scheduler.complete(replacement.worker_id, retry.lease_id, replacement.execute(retry.request))
    assert accepted.authoritative is True
    assert_single_authoritative_evidence(scheduler, request.evidence_identity)
    assert_campaign_accounted(scheduler, {request.evidence_identity})
    assert_no_active_stale_leases(scheduler)


def test_fault_matrix_duplicate_transport_messages_are_safe(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    scheduler = DistributedScheduler(tmp_path / "scheduler.json")
    scheduler.register_worker(worker.registration())
    assert scheduler.submit(request)
    assert scheduler.submit(request) is False
    lease = scheduler.claim(worker.worker_id)
    assert lease is not None
    result = physical_result(lease.request, worker_fingerprint=worker.runtime_identity.runtime_fingerprint)
    assert scheduler.complete(worker.worker_id, lease.lease_id, result).authoritative
    assert scheduler.complete(worker.worker_id, lease.lease_id, result).authoritative is False
    assert_single_authoritative_evidence(scheduler, request.evidence_identity)


def test_fault_matrix_partial_jsonl_record_is_an_error_not_execution(tmp_path: Path) -> None:
    store = ContentAddressedArtifactStore(tmp_path / "store")
    worker = RemoteWorkerRuntime(worker_id="jsonl", root=tmp_path / "worker", artifact_store=store)
    output = StringIO()
    assert worker.serve_jsonl(StringIO("{\"protocol_version\":\n{\"message_type\":\"shutdown\"}\n"), output) == 0
    lines = [json.loads(line) for line in output.getvalue().splitlines() if line]
    assert lines[1]["message_type"] == "error"
    assert all(item["message_type"] != "result" for item in lines[1:])


def test_fault_matrix_projection_provider_failure_preserves_report(tmp_path: Path) -> None:
    report = {"campaign_id": "fault", "status": "complete", "counts": {"killed": 1}}

    class BrokenProvider:
        name = "broken"

        def analyze(self, value):
            raise RuntimeError("provider down")

    analysis = analyze_report(report, provider=BrokenProvider())
    assert analysis.status == "failed"
    assert report == {"campaign_id": "fault", "status": "complete", "counts": {"killed": 1}}
    assert analysis.to_dict()["advisory"] is True


def test_fault_matrix_sidecar_write_failure_does_not_corrupt_canonical_report(tmp_path: Path) -> None:
    report = {"campaign_id": "sidecar", "status": "complete", "counts": {"survived": 1}}
    analysis = analyze_report(report)
    target = tmp_path / "sidecar-target"
    target.mkdir()
    with pytest.raises(OSError):
        write_advisory_analysis(target, analysis)
    assert report["status"] == "complete"


def test_fault_matrix_artifact_publication_and_reconstruction_postconditions(tmp_path: Path) -> None:
    store = ContentAddressedArtifactStore(tmp_path / "cas")
    record = store.put_bytes(b"durable")
    assert_artifact_verified(store, record.artifact_id)
    reopened = ContentAddressedArtifactStore(store.root)
    assert_artifact_verified(reopened, record.artifact_id)
    with pytest.raises(ArtifactIntegrityError):
        reopened.get_bytes("0" * 64)
    assert_no_live_attempt_processes()


def test_fault_matrix_scheduler_submit_commit_failure_leaves_no_phantom_pending(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    state = tmp_path / "scheduler.json"
    scheduler = DistributedScheduler(state)
    scheduler.register_worker(worker.registration())
    with patch("theseus_local.distributed._atomic_write", side_effect=OSError("state publish failed")):
        with pytest.raises(OSError, match="state publish failed"):
            scheduler.submit(request)
    reopened = DistributedScheduler(state)
    assert reopened.pending_count() == 0
    assert reopened.active_lease_count() == 0


def test_fault_matrix_scheduler_completion_commit_failure_is_recoverable(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    state = tmp_path / "scheduler.json"
    scheduler = DistributedScheduler(state, lease_seconds=60.0)
    scheduler.register_worker(worker.registration())
    scheduler.submit(request)
    lease = scheduler.claim(worker.worker_id)
    assert lease is not None
    result = physical_result(lease.request, worker_fingerprint=worker.runtime_identity.runtime_fingerprint)
    with patch("theseus_local.distributed._atomic_write", side_effect=OSError("completion publish failed")):
        with pytest.raises(OSError, match="completion publish failed"):
            scheduler.complete(worker.worker_id, lease.lease_id, result)
    reopened = DistributedScheduler(state, lease_seconds=60.0)
    assert reopened.active_lease_count() == 1
    assert reopened.expire_leases(now=datetime.now(timezone.utc) + timedelta(hours=1)) == (lease.lease_id,)
    retry = reopened.claim(worker.worker_id)
    assert retry is not None
    accepted = reopened.complete(
        worker.worker_id,
        retry.lease_id,
        physical_result(retry.request, worker_fingerprint=worker.runtime_identity.runtime_fingerprint),
    )
    assert accepted.authoritative


def test_fault_matrix_worker_result_ack_failure_does_not_create_durable_duplicate(tmp_path: Path) -> None:
    store, request, worker = remote_fixture(tmp_path)
    with patch.object(worker, "_save_journal", side_effect=OSError("journal publish failed")):
        with pytest.raises(OSError, match="journal publish failed"):
            worker.execute(request)
    reopened = RemoteWorkerRuntime(
        worker_id="worker-0",
        root=tmp_path / "worker",
        artifact_store=ContentAddressedArtifactStore(store.root),
    )
    # The failed journal publication is not treated as an acknowledged result;
    # the fresh runtime may safely execute the same attempt again.
    assert reopened.execute(request).started is True
