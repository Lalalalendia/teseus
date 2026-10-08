from __future__ import annotations

import os
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from theseus_local.artifact_store import ContentAddressedArtifactStore
from theseus_local.acceptance import assert_resource_budget, process_resource_snapshot
from theseus_local.distributed import DistributedScheduler
from theseus_local.remote_campaign import run_distributed_requests
from theseus_local.remote_worker import RemoteWorkerRuntime

from post_pr63_helpers import assert_no_active_stale_leases, remote_fixture


def _requests(tmp_path: Path, count: int):
    store, template, _ = remote_fixture(tmp_path / "fixture")
    requests = tuple(
        replace(
            template,
            execution_attempt_id=f"scale-attempt-{index}",
            evidence_identity=f"scale-evidence-{index}",
            mutation_identity=f"scale-mutation-{index}",
        )
        for index in range(count)
    )
    return store, requests


@pytest.mark.scale
def test_24_request_campaign_is_accounted_for_at_one_two_and_four_workers(tmp_path: Path) -> None:
    store, requests = _requests(tmp_path, 24)
    for worker_count in (1, 2, 4):
        scheduler = DistributedScheduler(tmp_path / f"scale-{worker_count}.json")
        workers = tuple(
            RemoteWorkerRuntime(worker_id=f"scale-{worker_count}-{index}", root=tmp_path / f"worker-{worker_count}-{index}")
            for index in range(worker_count)
        )
        report = run_distributed_requests(
            requests,
            coordinator_store=store,
            workers=workers,
            scheduler=scheduler,
        )
        assert set(item.evidence_identity for item in report.results) == {item.evidence_identity for item in requests}
        assert len(report.results) == 24
        assert set(report.scheduler_snapshot["authoritative"]) == {item.evidence_identity for item in requests}
        assert_no_active_stale_leases(scheduler)


@pytest.mark.scale
def test_high_retry_rate_remains_deterministic(tmp_path: Path) -> None:
    _, requests = _requests(tmp_path, 30)
    scheduler = DistributedScheduler(tmp_path / "retry-scale.json", lease_seconds=60.0)
    worker = RemoteWorkerRuntime(worker_id="retry-worker", root=tmp_path / "retry-worker")
    replacement = RemoteWorkerRuntime(worker_id="replacement", root=tmp_path / "replacement")
    scheduler.register_worker(worker.registration())
    scheduler.register_worker(replacement.registration())
    for request in requests:
        scheduler.submit(request)
    lost = 0
    worker_online = True
    while scheduler.pending_count() or scheduler.active_lease_count():
        lease = scheduler.claim(worker.worker_id) if worker_online else None
        lease = lease or scheduler.claim(replacement.worker_id)
        assert lease is not None
        if lease.attempt == 0 and int(lease.evidence_identity.rsplit("-", 1)[-1]) % 3 == 0:
            scheduler.unregister_worker(lease.worker_id, reason="injected_attempt_loss")
            scheduler.register_worker(replacement.registration())
            if lease.worker_id == worker.worker_id:
                worker_online = False
            lost += 1
            continue
        owner = replacement if lease.worker_id == replacement.worker_id else worker
        assert scheduler.complete(owner.worker_id, lease.lease_id, owner.execute(lease.request)).authoritative
    assert lost >= 8
    assert len(scheduler.snapshot()["authoritative"]) == len(requests)
    assert_no_active_stale_leases(scheduler)


@pytest.mark.scale
def test_100_request_campaign_has_no_missing_or_duplicate_authority(tmp_path: Path) -> None:
    store, requests = _requests(tmp_path, 100)
    scheduler = DistributedScheduler(tmp_path / "hundred.json")
    workers = tuple(
        RemoteWorkerRuntime(worker_id=f"hundred-{index}", root=tmp_path / f"hundred-worker-{index}")
        for index in range(4)
    )
    for worker in workers:
        scheduler.register_worker(worker.registration())
    for request in requests:
        scheduler.submit(request)
    report = run_distributed_requests(
        requests,
        coordinator_store=store,
        workers=workers,
        scheduler=scheduler,
    )
    authoritative = report.scheduler_snapshot["authoritative"]
    assert len(authoritative) == 100
    assert set(authoritative) == {item.evidence_identity for item in requests}
    assert_no_active_stale_leases(scheduler)


@pytest.mark.scale
def test_worker_churn_does_not_lose_queued_work(tmp_path: Path) -> None:
    _, requests = _requests(tmp_path, 30)
    scheduler = DistributedScheduler(tmp_path / "churn.json")
    workers = [
        RemoteWorkerRuntime(worker_id=f"churn-{index}", root=tmp_path / f"churn-worker-{index}")
        for index in range(3)
    ]
    for worker in workers:
        scheduler.register_worker(worker.registration())
    for request in requests:
        scheduler.submit(request)
    index = 0
    while scheduler.pending_count() or scheduler.active_lease_count():
        slot = index % len(workers)
        worker = workers[slot]
        index += 1
        lease = scheduler.claim(worker.worker_id)
        if lease is None:
            continue
        if index % 5 == 0:
            scheduler.unregister_worker(worker.worker_id, reason="churn")
            replacement = RemoteWorkerRuntime(worker_id=worker.worker_id, root=tmp_path / f"replacement-{index}")
            workers[slot] = replacement
            scheduler.register_worker(replacement.registration())
            continue
        assert scheduler.complete(worker.worker_id, lease.lease_id, worker.execute(lease.request)).authoritative
    assert len(scheduler.snapshot()["authoritative"]) == 30
    assert_no_active_stale_leases(scheduler)


@pytest.mark.scale
def test_cache_pressure_preserves_pins_and_evicts_only_unpinned(tmp_path: Path) -> None:
    store = ContentAddressedArtifactStore(tmp_path / "cache")
    records = tuple(store.put_bytes(f"artifact-{index}".encode(), pin=index < 3) for index in range(20))
    removed = set(store.evict(max_bytes=sum(item.size_bytes for item in records[:3])))
    assert {item.artifact_id for item in records[:3]}.isdisjoint(removed)
    assert removed
    for item in records[:3]:
        assert store.has(item.artifact_id)


@pytest.mark.soak
@pytest.mark.skipif(os.environ.get("THESEUS_RUN_SOAK") != "1", reason="set THESEUS_RUN_SOAK=1 for the long soak gate")
def test_soak_repeated_remote_execution_leaves_no_workspace_spool_leak(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    before = process_resource_snapshot()
    for index in range(100):
        result = worker.execute(
            replace(
                request,
                execution_attempt_id=f"soak-{index}",
                evidence_identity=f"soak-evidence-{index}",
                mutation_identity=f"soak-mutation-{index}",
                argv=(sys.executable, "-c", "print('soak')"),
            )
        )
        assert result.exit_code == 0
    after = process_resource_snapshot()
    assert_resource_budget(before, after)
    assert not tuple((worker.root / "workspaces").rglob(".attempt-*"))
    assert not tuple((worker.root / "outputs").glob("*.log"))
