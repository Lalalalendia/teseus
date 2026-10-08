from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import os
from threading import Barrier
from pathlib import Path
import subprocess
import sys

from theseus_local.distributed import DistributedScheduler
from theseus_local.remote_worker import RemoteWorkerRuntime

from post_pr63_helpers import physical_result, remote_fixture


def test_duplicate_submission_race_is_idempotent(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    scheduler = DistributedScheduler(tmp_path / "scheduler.json")
    scheduler.register_worker(worker.registration())
    barrier = Barrier(2)

    def submit() -> bool:
        barrier.wait()
        return scheduler.submit(request)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(lambda _: submit(), range(2)))
    assert sorted(results) == [False, True]
    assert scheduler.pending_count() == 1


def test_cross_process_submission_does_not_lose_durable_state_updates(tmp_path: Path) -> None:
    state = tmp_path / "scheduler.json"
    _, first_request, _ = remote_fixture(tmp_path / "first", evidence_identity="e-first", attempt_id="a-first")
    _, second_request, _ = remote_fixture(tmp_path / "second", evidence_identity="e-second", attempt_id="a-second")
    first_json = tmp_path / "first-request.json"
    second_json = tmp_path / "second-request.json"
    first_json.write_text(first_request.to_json(), encoding="utf-8")
    second_json.write_text(second_request.to_json(), encoding="utf-8")
    script = (
        "from pathlib import Path; "
        "import sys; "
        "from theseus_contracts import RemoteExecutionRequest; "
        "from theseus_local.distributed import DistributedScheduler; "
        "scheduler = DistributedScheduler(Path(sys.argv[1])); "
        "request = RemoteExecutionRequest.from_json(Path(sys.argv[2]).read_text(encoding='utf-8')); "
        "print(scheduler.submit(request))"
    )
    environment = dict(os.environ)
    processes = [
        subprocess.Popen(
            (sys.executable, "-c", script, str(state), str(request_path)),
            cwd=str(Path(__file__).resolve().parents[1]),
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for request_path in (first_json, second_json)
    ]
    results = [process.communicate(timeout=30) for process in processes]
    assert all(process.returncode == 0 for process in processes), results
    reopened = DistributedScheduler(state)
    assert reopened.pending_count() == 2
    assert reopened.snapshot()["pending"] == ("e-first", "e-second")


def test_duplicate_result_race_creates_one_authoritative_evidence(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    scheduler = DistributedScheduler(tmp_path / "scheduler.json")
    scheduler.register_worker(worker.registration())
    scheduler.submit(request)
    lease = scheduler.claim(worker.worker_id)
    assert lease is not None
    result = physical_result(lease.request, worker_fingerprint=worker.runtime_identity.runtime_fingerprint)
    barrier = Barrier(2)

    def complete() -> bool:
        barrier.wait()
        return scheduler.complete(worker.worker_id, lease.lease_id, result).authoritative

    with ThreadPoolExecutor(max_workers=2) as pool:
        accepted = tuple(pool.map(lambda _: complete(), range(2)))
    assert sum(accepted) == 1
    assert scheduler.snapshot()["authoritative"] == (request.evidence_identity,)


def test_cross_process_duplicate_completion_has_one_authority(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    state = tmp_path / "scheduler.json"
    scheduler = DistributedScheduler(state)
    scheduler.register_worker(worker.registration())
    scheduler.submit(request)
    lease = scheduler.claim(worker.worker_id)
    assert lease is not None
    result_path = tmp_path / "result.json"
    result_path.write_text(
        physical_result(lease.request, worker_fingerprint=worker.runtime_identity.runtime_fingerprint).to_json(),
        encoding="utf-8",
    )
    script = (
        "from pathlib import Path; import sys; "
        "from theseus_contracts import RemoteExecutionResult; "
        "from theseus_local.distributed import DistributedScheduler; "
        "s=DistributedScheduler(Path(sys.argv[1])); "
        "r=RemoteExecutionResult.from_json(Path(sys.argv[2]).read_text(encoding='utf-8')); "
        "print(int(s.complete(sys.argv[3], sys.argv[4], r).authoritative))"
    )
    processes = [
        subprocess.Popen(
            (sys.executable, "-c", script, str(state), str(result_path), worker.worker_id, lease.lease_id),
            cwd=str(Path(__file__).resolve().parents[1]),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(2)
    ]
    outputs = [process.communicate(timeout=30) for process in processes]
    assert all(process.returncode == 0 for process in processes), outputs
    assert sorted(int(stdout.strip()) for stdout, _ in outputs) == [0, 1]
    assert DistributedScheduler(state).snapshot()["authoritative"] == (request.evidence_identity,)


def test_cross_process_cancel_and_completion_race_preserves_single_writer_invariant(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    state = tmp_path / "scheduler.json"
    scheduler = DistributedScheduler(state)
    scheduler.register_worker(worker.registration())
    scheduler.submit(request)
    lease = scheduler.claim(worker.worker_id)
    assert lease is not None
    result_path = tmp_path / "result.json"
    result_path.write_text(
        physical_result(lease.request, worker_fingerprint=worker.runtime_identity.runtime_fingerprint).to_json(),
        encoding="utf-8",
    )
    script = (
        "from pathlib import Path; import sys; "
        "from theseus_contracts import RemoteExecutionResult; "
        "from theseus_local.distributed import DistributedScheduler; "
        "s=DistributedScheduler(Path(sys.argv[1])); "
        "e=sys.argv[3]; "
        "print('cancel' if sys.argv[4]=='cancel' and s.cancel(e) else "
        "'complete' if sys.argv[4]=='complete' and s.complete(sys.argv[5], sys.argv[6], "
        "RemoteExecutionResult.from_json(Path(sys.argv[2]).read_text(encoding='utf-8'))).authoritative else 'stale')"
    )
    processes = [
        subprocess.Popen(
            (
                sys.executable,
                "-c",
                script,
                str(state),
                str(result_path),
                request.evidence_identity,
                mode,
                worker.worker_id,
                lease.lease_id,
            ),
            cwd=str(Path(__file__).resolve().parents[1]),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for mode in ("cancel", "complete")
    ]
    outputs = [process.communicate(timeout=30) for process in processes]
    assert all(process.returncode == 0 for process in processes), outputs
    final = DistributedScheduler(state)
    assert len(final.snapshot()["authoritative"]) <= 1
    assert final.active_lease_count() == 0
    assert final.pending_count() == 0


def test_completion_and_expiry_interleavings_are_both_safe(tmp_path: Path) -> None:
    for name, complete_first in (("complete-first", True), ("expire-first", False)):
        case = tmp_path / name
        case.mkdir()
        _, request, worker = remote_fixture(case)
        scheduler = DistributedScheduler(case / "scheduler.json", lease_seconds=60.0)
        scheduler.register_worker(worker.registration())
        scheduler.submit(request)
        lease = scheduler.claim(worker.worker_id)
        assert lease is not None
        result = physical_result(lease.request, worker_fingerprint=worker.runtime_identity.runtime_fingerprint)
        if complete_first:
            assert scheduler.complete(worker.worker_id, lease.lease_id, result).authoritative
            assert scheduler.expire_leases(now=datetime.now(timezone.utc) + timedelta(hours=1)) == ()
        else:
            assert scheduler.expire_leases(now=datetime.now(timezone.utc) + timedelta(hours=1)) == (lease.lease_id,)
            assert scheduler.complete(worker.worker_id, lease.lease_id, result).authoritative is False
            retry = scheduler.claim(worker.worker_id)
            assert retry is not None
            retry_result = physical_result(retry.request, worker_fingerprint=worker.runtime_identity.runtime_fingerprint)
            assert scheduler.complete(worker.worker_id, retry.lease_id, retry_result).authoritative
        assert scheduler.snapshot()["authoritative"] == (request.evidence_identity,)
        assert scheduler.active_lease_count() == 0


def test_heartbeat_race_never_keeps_an_expired_lease_alive(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    scheduler = DistributedScheduler(tmp_path / "scheduler.json", lease_seconds=0.01)
    scheduler.register_worker(worker.registration())
    scheduler.submit(request)
    lease = scheduler.claim(worker.worker_id)
    assert lease is not None
    future = datetime.now(timezone.utc) + timedelta(seconds=1)
    assert scheduler.expire_leases(now=future) == (lease.lease_id,)
    assert scheduler.heartbeat(worker.worker_id, lease.lease_id, worker_instance_id=lease.worker_instance_id) is False
    assert scheduler.active_lease_count() == 0
    assert scheduler.snapshot()["workers"][worker.worker_id]["state"] == "ready"


def test_bounded_dispatch_never_exceeds_advertised_slots(tmp_path: Path) -> None:
    scheduler = DistributedScheduler(tmp_path / "scheduler.json")
    worker = RemoteWorkerRuntime(worker_id="slots", root=tmp_path / "worker", slots=4)
    scheduler.register_worker(worker.registration())
    requests = []
    for index in range(8):
        _, request, _ = remote_fixture(tmp_path / f"request-{index}", evidence_identity=f"e-{index}", attempt_id=f"a-{index}")
        requests.append(request)
        scheduler.submit(request)
    leases = scheduler.claim_available(worker.worker_id, limit=10)
    assert len(leases) == 4
    assert scheduler.active_lease_count() == 4
    assert scheduler.pending_count() == 4


def test_worker_loss_requeues_work_and_fairly_assigns_next_worker(tmp_path: Path) -> None:
    _, request, first = remote_fixture(tmp_path / "first")
    _, second_request, _ = remote_fixture(tmp_path / "second", evidence_identity="z-e-2", attempt_id="a-2")
    second = RemoteWorkerRuntime(worker_id="worker-1", root=tmp_path / "worker-1", slots=2)
    scheduler = DistributedScheduler(tmp_path / "scheduler.json")
    scheduler.register_worker(first.registration())
    scheduler.register_worker(second.registration())
    scheduler.submit(request)
    scheduler.submit(second_request)
    first_lease = scheduler.claim(first.worker_id)
    second_lease = scheduler.claim(second.worker_id)
    assert first_lease is not None and second_lease is not None
    scheduler.unregister_worker(first.worker_id, reason="worker_crash")
    assert scheduler.pending_count() == 1
    retry = scheduler.claim(second.worker_id)
    assert retry is not None
    assert retry.evidence_identity == request.evidence_identity
    assert retry.attempt == 1


def test_cancellation_fences_late_result_and_worker_reconnect(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    scheduler = DistributedScheduler(tmp_path / "scheduler.json")
    scheduler.register_worker(worker.registration())
    scheduler.submit(request)
    lease = scheduler.claim(worker.worker_id)
    assert lease is not None
    assert scheduler.cancel(request.evidence_identity, reason="campaign_cancelled")
    late = physical_result(lease.request, worker_fingerprint=worker.runtime_identity.runtime_fingerprint)
    assert scheduler.complete(worker.worker_id, lease.lease_id, late).authoritative is False
    scheduler.register_worker(worker.registration())
    assert scheduler.claim(worker.worker_id) is None
    assert scheduler.pending_count() == 0
    assert scheduler.active_lease_count() == 0
    assert scheduler.snapshot()["workers"][worker.worker_id]["state"] == "ready"


def test_scheduler_restart_preserves_lease_expiry_and_authority(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    state = tmp_path / "scheduler.json"
    scheduler = DistributedScheduler(state, lease_seconds=60.0)
    scheduler.register_worker(worker.registration())
    scheduler.submit(request)
    lease = scheduler.claim(worker.worker_id)
    assert lease is not None
    restarted = DistributedScheduler(state, lease_seconds=1.0)
    assert restarted.active_lease_count() == 1
    assert restarted.heartbeat(worker.worker_id, lease.lease_id, worker_instance_id=lease.worker_instance_id)
    assert restarted.expire_leases(now=datetime.now(timezone.utc) + timedelta(hours=1)) == (lease.lease_id,)
    retry = restarted.claim(worker.worker_id)
    assert retry is not None
    result = physical_result(retry.request, worker_fingerprint=worker.runtime_identity.runtime_fingerprint)
    assert restarted.complete(worker.worker_id, retry.lease_id, result).authoritative
    assert DistributedScheduler(state).snapshot()["authoritative"] == (request.evidence_identity,)
