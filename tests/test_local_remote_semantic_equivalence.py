from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path

from theseus_contracts import RemoteExecutionResult
from theseus_local.distributed import DistributedScheduler
from theseus_local.execution_backend import ExecutionRequest, LocalProcessBackend
from theseus_local.remote_campaign import (
    classify_physical_result,
    compare_execution_semantics,
    project_semantic_outcomes,
    run_distributed_requests,
)
from theseus_local.runtime_identity import current_runtime_identity

from post_pr63_helpers import remote_fixture


def _local_result(tmp_path: Path, request) -> RemoteExecutionResult:
    process = LocalProcessBackend().execute(
        ExecutionRequest(
            execution_id=request.execution_attempt_id,
            argv=request.argv,
            cwd=tmp_path / "project",
            timeout_seconds=request.timeout_seconds,
            output_artifact=tmp_path / f"{request.execution_attempt_id}.local.log",
        )
    )
    return RemoteExecutionResult(
        execution_attempt_id=request.execution_attempt_id,
        evidence_identity=request.evidence_identity,
        mutation_identity=request.mutation_identity,
        started=True,
        exit_code=process.exit_code,
        timed_out=bool(process.timed_out),
        cancelled=bool(process.termination and process.termination.get("reason") == "cancelled"),
        elapsed_seconds=process.elapsed_seconds,
        worker_runtime_fingerprint=current_runtime_identity().runtime_fingerprint,
        workspace_integrity="verified",
    )


def test_local_and_remote_match_for_one_two_and_four_workers(tmp_path: Path) -> None:
    store, request, _ = remote_fixture(tmp_path / "fixture")
    requests = tuple(
        replace(
            request,
            execution_attempt_id=f"attempt-{index}",
            evidence_identity=f"evidence-{index}",
            mutation_identity=f"mutation-{index}",
        )
        for index in range(4)
    )
    local = tuple(_local_result(tmp_path / "fixture", item) for item in requests)
    for worker_count in (1, 2, 4):
        workers = tuple(
            __import__("theseus_local.remote_worker", fromlist=["RemoteWorkerRuntime"]).RemoteWorkerRuntime(
                worker_id=f"remote-{index}",
                root=tmp_path / f"remote-{worker_count}-{index}",
            )
            for index in range(worker_count)
        )
        report = run_distributed_requests(
            requests,
            coordinator_store=store,
            workers=workers,
            scheduler=DistributedScheduler(tmp_path / f"scheduler-{worker_count}.json"),
        )
        equivalent, differences = compare_execution_semantics(local, report.results)
        assert equivalent, differences
        assert {item.evidence_identity for item in report.results} == {
            item.evidence_identity for item in local
        }
        assert len(report.results) == len(requests)
        assert sum(report.worker_assignments.values()) == len(requests)
        assert round(sum(report.worker_utilization.values()), 6) == 1.0
        assert sum(report.journal_completed_attempts.values()) == len(requests)


def test_timeout_semantics_are_transport_invariant(tmp_path: Path) -> None:
    command = (sys.executable, "-c", "import time; time.sleep(10)")
    store, request, _ = remote_fixture(tmp_path / "timeout", command=command, timeout_seconds=0.1)
    local = (_local_result(tmp_path / "timeout", request),)
    from theseus_local.remote_worker import RemoteWorkerRuntime

    worker = RemoteWorkerRuntime(worker_id="timeout-worker", root=tmp_path / "timeout-remote")
    report = run_distributed_requests(
        (request,),
        coordinator_store=store,
        workers=(worker,),
        scheduler=DistributedScheduler(tmp_path / "timeout-scheduler.json"),
    )
    equivalent, differences = compare_execution_semantics(local, report.results)
    assert equivalent, differences
    assert report.results[0].timed_out is True


def test_failure_and_timeout_facts_match_across_local_and_remote_boundaries(tmp_path: Path) -> None:
    cases = (
        ("success", (sys.executable, "-c", "print('ok')"), 5.0, 0, False),
        ("failure", (sys.executable, "-c", "raise SystemExit(17)"), 5.0, 17, False),
        ("timeout", (sys.executable, "-c", "import time; time.sleep(10)"), 0.1, None, True),
    )
    for name, command, timeout_seconds, exit_code, timed_out in cases:
        case = tmp_path / name
        store, request, _ = remote_fixture(case, command=command, timeout_seconds=timeout_seconds)
        local = (_local_result(case, request),)
        from theseus_local.remote_worker import RemoteWorkerRuntime

        worker = RemoteWorkerRuntime(worker_id=f"worker-{name}", root=case / "remote")
        report = run_distributed_requests(
            (request,),
            coordinator_store=store,
            workers=(worker,),
            scheduler=DistributedScheduler(case / "scheduler.json"),
        )
        equivalent, differences = compare_execution_semantics(local, report.results)
        assert equivalent, (name, differences)
        assert report.results[0].exit_code == exit_code
        assert report.results[0].timed_out is timed_out


def test_semantic_projection_covers_invalid_without_trusting_worker_status(tmp_path: Path) -> None:
    _, request, _ = remote_fixture(tmp_path)
    physical = RemoteExecutionResult(
        execution_attempt_id=request.execution_attempt_id,
        evidence_identity=request.evidence_identity,
        mutation_identity=request.mutation_identity,
        started=False,
        exit_code=None,
        timed_out=False,
        cancelled=False,
        elapsed_seconds=0.0,
        worker_runtime_fingerprint=current_runtime_identity().runtime_fingerprint,
        workspace_integrity="failed",
    )
    assert "killed" not in physical.to_dict()
    assert classify_physical_result(physical) == "infrastructure_error"
    assert project_semantic_outcomes((physical,), coordinator_statuses={request.evidence_identity: "invalid"}) == {
        request.evidence_identity: "invalid"
    }
    equivalent, differences = compare_execution_semantics(
        (physical,),
        (physical,),
        left_coordinator_statuses={request.evidence_identity: "invalid"},
        right_coordinator_statuses={request.evidence_identity: "invalid"},
    )
    assert equivalent, differences
    equivalent, differences = compare_execution_semantics(
        (physical,),
        (physical,),
        left_coordinator_statuses={request.evidence_identity: "invalid"},
        right_coordinator_statuses={request.evidence_identity: "survived"},
    )
    assert not equivalent
    assert differences == (f"different:{request.evidence_identity}",)
