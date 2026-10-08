from __future__ import annotations

import sys
import time
from dataclasses import replace
from io import StringIO
from pathlib import Path

import pytest

from theseus_contracts import RemoteExecutionRequest
from theseus_local.ai_analysis import (
    DeterministicAnalysisProvider,
    analyze_report,
)
from theseus_local.ai_mutation import (
    MutationCandidate,
    MutationCandidateError,
    deduplicate_candidates,
    prepare_candidate,
)
from theseus_local.artifact_store import (
    ArtifactIntegrityError,
    ContentAddressedArtifactStore,
)
from theseus_local.distributed import DistributedScheduler
from theseus_local.isolation import ExecutionPolicy, IsolationPolicyError, resolve_workspace
from theseus_local.acceptance import resource_quiescence
from theseus_local.operations import CampaignOperationalStatus, project_campaign
from theseus_local.remote_worker import RemoteWorkerRuntime
from theseus_local.remote_campaign import compare_execution_semantics, run_distributed_requests
from theseus_local.release import (
    describe_artifact,
    read_installation_identity,
    require_installation_compatibility,
    write_installation_identity,
)
from theseus_local.runtime_identity import (
    RuntimeCompatibilityError,
    assert_runtime_compatible,
    current_runtime_identity,
)


def _remote_fixture(tmp_path: Path) -> tuple[ContentAddressedArtifactStore, RemoteExecutionRequest, RemoteWorkerRuntime]:
    project = tmp_path / "project"
    project.mkdir()
    (project / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
    store = ContentAddressedArtifactStore(tmp_path / "cache")
    snapshot = store.create_snapshot(project)
    store.persist_snapshot(snapshot)
    prepared = store.put_bytes(b"prepared immutable mutation")
    identity = current_runtime_identity()
    request = RemoteExecutionRequest(
        execution_attempt_id="attempt-0",
        evidence_identity="evidence-0",
        mutation_identity="mutation-0",
        runtime_identity=identity.to_dict(),
        project_snapshot_id=snapshot.snapshot_id,
        prepared_artifact_id=prepared.artifact_id,
        test_plan_identity="test-plan-0",
        argv=(sys.executable, "-c", "print('remote-ok')"),
        source_path="module.py",
        expected_source_sha256=snapshot.files["module.py"],
    )
    worker = RemoteWorkerRuntime(
        worker_id="worker-0",
        root=tmp_path / "worker",
        artifact_store=store,
    )
    return store, request, worker


def test_runtime_identity_is_path_independent_and_rejects_version_mismatch() -> None:
    identity = current_runtime_identity()
    assert identity == type(identity).from_dict(identity.to_dict())
    changed = current_runtime_identity(theseus_version="99.0.0")
    assert changed.runtime_fingerprint != identity.runtime_fingerprint
    with pytest.raises(RuntimeCompatibilityError):
        assert_runtime_compatible(identity, changed)


def test_release_identity_and_artifact_description_are_durable(tmp_path: Path) -> None:
    identity = current_runtime_identity()
    artifact = tmp_path / "theseus.whl"
    artifact.write_bytes(b"wheel-bytes")
    description = describe_artifact(artifact, runtime_identity=identity)
    assert description.size_bytes == len(b"wheel-bytes")
    state = tmp_path / "runtime.identity.json"
    write_installation_identity(state, identity)
    assert read_installation_identity(state) == identity
    assert require_installation_compatibility(state, identity) == identity


def test_remote_request_round_trip_is_deterministic_and_rejects_unknown_fields(tmp_path: Path) -> None:
    _, request, _ = _remote_fixture(tmp_path)
    assert RemoteExecutionRequest.from_json(request.to_json()).to_json() == request.to_json()
    raw = request.to_dict()
    raw["pickle"] = "forbidden"
    with pytest.raises(ValueError, match="unknown"):
        RemoteExecutionRequest.from_dict(raw)


def test_artifact_store_snapshot_and_pin_aware_eviction(tmp_path: Path) -> None:
    store = ContentAddressedArtifactStore(tmp_path / "cas")
    first = store.put_bytes(b"first", pin=True)
    second = store.put_bytes(b"second")
    assert store.get_bytes(first.artifact_id) == b"first"
    removed = store.evict(max_bytes=0)
    assert second.artifact_id in removed
    assert first.artifact_id not in removed
    with pytest.raises(ArtifactIntegrityError):
        store.get_bytes(second.artifact_id)


def test_remote_worker_uses_local_backend_and_replays_duplicate_attempt(tmp_path: Path) -> None:
    _, request, worker = _remote_fixture(tmp_path)
    result = worker.execute(request)
    assert result.started is True
    assert result.exit_code == 0
    assert result.workspace_integrity == "verified"
    assert worker.execute(request) == result
    assert worker.state.value == "ready"


def test_remote_worker_restart_replays_only_durable_completed_attempt(tmp_path: Path) -> None:
    _, request, worker = _remote_fixture(tmp_path)
    result = worker.execute(request)
    restarted = worker.restart()
    assert restarted.execute(request) == result
    assert restarted.registration().instance_id != worker.registration().instance_id


def test_remote_worker_timeout_reaps_process_tree_and_cleans_workspace(tmp_path: Path) -> None:
    store, request, worker = _remote_fixture(tmp_path)
    worker.policy = ExecutionPolicy(timeout_seconds=0.1)
    timeout_request = replace(request, timeout_seconds=0.1, argv=(sys.executable, "-c", "import time; time.sleep(10)"))
    result = worker.execute(timeout_request)
    assert result.started is True
    assert result.timed_out is True
    assert result.workspace_integrity == "verified"
    assert resource_quiescence().clean


def test_remote_worker_rejects_corrupt_artifact_before_process(tmp_path: Path) -> None:
    store, request, worker = _remote_fixture(tmp_path)
    artifact_path = store.path_for(request.prepared_artifact_id, verify=False)
    artifact_path.write_bytes(b"corrupt")
    result = worker.execute(request)
    assert result.started is False
    assert result.workspace_integrity == "failed"
    assert "artifact" in (result.diagnostic_error or "").lower()


def test_remote_jsonl_transport_registers_and_executes_without_pickle(tmp_path: Path) -> None:
    store, request, _ = _remote_fixture(tmp_path)
    worker = RemoteWorkerRuntime(worker_id="jsonl-worker", root=tmp_path / "jsonl-worker", artifact_store=store)
    input_stream = StringIO(request.to_json() + "\n{" + '"message_type":"shutdown"' + "}\n")
    output_stream = StringIO()
    assert worker.serve_jsonl(input_stream, output_stream) == 0
    lines = [line for line in output_stream.getvalue().splitlines() if line]
    assert '"message_type": "registration"' in lines[0]
    assert '"message_type": "result"' in lines[1]


def test_distributed_run_transfers_only_missing_artifacts_and_compares_semantics(tmp_path: Path) -> None:
    store, request, _ = _remote_fixture(tmp_path)
    second = replace(
        request,
        execution_attempt_id="attempt-1",
        evidence_identity="evidence-1",
        mutation_identity="mutation-1",
    )
    worker_a = RemoteWorkerRuntime(worker_id="a", root=tmp_path / "a")
    worker_b = RemoteWorkerRuntime(worker_id="b", root=tmp_path / "b")
    report = run_distributed_requests(
        (request, second),
        coordinator_store=store,
        workers=(worker_a, worker_b),
        scheduler=DistributedScheduler(tmp_path / "distributed.json"),
    )
    assert len(report.results) == 2
    assert report.transfer_stats["a"].bytes_transferred > 0
    assert report.transfer_stats["b"].bytes_transferred > 0
    equivalent, differences = compare_execution_semantics(report.results, report.results)
    assert equivalent is True
    assert differences == ()


def test_scheduler_rejects_late_lease_and_accepts_only_one_authoritative_result(tmp_path: Path) -> None:
    _, request, worker = _remote_fixture(tmp_path)
    scheduler = DistributedScheduler(tmp_path / "scheduler.json", lease_seconds=0.01)
    scheduler.register_worker(worker.registration())
    assert scheduler.submit(request) is True
    lease = scheduler.claim("worker-0")
    assert lease is not None
    time.sleep(0.03)
    assert scheduler.expire_leases() == (lease.lease_id,)
    scheduler.lease_seconds = 1.0
    retry = scheduler.claim("worker-0")
    assert retry is not None
    assert scheduler.heartbeat(
        "worker-0",
        retry.lease_id,
        worker_instance_id=retry.worker_instance_id,
    ) is True
    stale = worker.execute(lease.request)
    rejected = scheduler.complete("worker-0", lease.lease_id, stale)
    assert rejected.authoritative is False
    accepted = scheduler.complete("worker-0", retry.lease_id, worker.execute(retry.request))
    assert accepted.authoritative is True
    duplicate = scheduler.complete("worker-0", retry.lease_id, worker.execute(retry.request))
    assert duplicate.authoritative is False
    assert len(scheduler.snapshot()["authoritative"]) == 1


def test_scheduler_state_survives_restart(tmp_path: Path) -> None:
    _, request, worker = _remote_fixture(tmp_path)
    path = tmp_path / "scheduler.json"
    scheduler = DistributedScheduler(path)
    scheduler.register_worker(worker.registration())
    scheduler.submit(request)
    assert scheduler.claim("worker-0") is not None
    restarted = DistributedScheduler(path)
    assert restarted.active_lease_count() == 1


def test_isolation_policy_does_not_forward_unlisted_environment() -> None:
    policy = ExecutionPolicy(required_environment={"TEST_REQUIRED": "yes"})
    env = policy.environment({"SECRET_TOKEN": "do-not-forward"})
    assert env["TEST_REQUIRED"] == "yes"
    assert "SECRET_TOKEN" not in env
    with pytest.raises(IsolationPolicyError):
        resolve_workspace(Path("C:/worker"), "../escape")


def test_advisory_analysis_is_optional_and_does_not_change_authority() -> None:
    report = {
        "campaign_id": "campaign-1",
        "status": "completed",
        "counts": {"survived": 1},
        "mutants": [{"mutant_id": "m1", "status": "survived"}],
    }
    original = dict(report)
    result = analyze_report(report, provider=DeterministicAnalysisProvider())
    unavailable = analyze_report(report)
    assert result.status == "available"
    assert unavailable.status == "unavailable"
    assert result.to_dict()["advisory"] is True
    assert report == original
    assert report["counts"]["survived"] == 1


def test_ai_candidate_uses_normal_syntax_and_identity_pipeline(tmp_path: Path) -> None:
    source = tmp_path / "module.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")
    candidate = MutationCandidate("module.py", 1, 8, "1", "2")
    prepared = prepare_candidate(candidate, tmp_path, allowed_source_paths=("module.py",))
    assert prepared.prepared_mutant.compiled is True
    assert prepared.rendered_source == b"VALUE = 2\n"
    assert deduplicate_candidates((prepared, prepared)) == (prepared,)
    with pytest.raises(MutationCandidateError):
        prepare_candidate(MutationCandidate("../module.py", 1, 0, "1", "2"), tmp_path)


def test_operations_view_is_projection_only() -> None:
    view = project_campaign(
        {
            "campaign_id": "c1",
            "status": "completed",
            "summary": {"total_mutants": 3, "completed_mutants": 3},
            "counts": {"killed": 2, "survived": 1},
        }
    )
    assert view.status is CampaignOperationalStatus.COMPLETED
    assert view.killed == 2
    assert view.survived == 1
