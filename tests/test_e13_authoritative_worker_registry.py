from __future__ import annotations

from theseus_contracts import (
    CampaignId,
    WorkerCapabilities,
    WorkerHeartbeat,
    WorkerIdentity,
)
from gallifrey_mutation import (
    InMemoryMutationStore,
    MutationCampaignService,
    MutationWorkerState,
    Rejected,
    Success,
)


def _identity(instance_id: str = "instance-a") -> WorkerIdentity:
    # Build one process-fenced worker identity for authoritative registry tests.
    return WorkerIdentity("worker-a", instance_id, 1234, f"birth-{instance_id}")


def _capabilities() -> WorkerCapabilities:
    # Advertise the production local engine and copy-workspace capabilities.
    return WorkerCapabilities(
        platform="win32",
        architecture="AMD64",
        python_versions=("3.14.2",),
        engine_protocol_versions=(1,),
        workspace_backends=("copy",),
        cpu_count=4,
    )


def test_authoritative_worker_lifecycle_is_persisted_in_gallifrey() -> None:
    # Exercise registration, assignment, heartbeat, delivery, release and clean stop through effects.
    campaign_id = CampaignId("campaign-workers")
    service = MutationCampaignService(InMemoryMutationStore())
    registered = service.register_worker(
        "effect.register.worker-a",
        campaign_id,
        _identity(),
        _capabilities(),
        workspace="D:/state/workers/worker-a/workspace",
        spool_path="D:/state/workers/worker-a/spool",
        launcher_process_id=1200,
    )
    assert isinstance(registered, Success)
    assert registered.value.status == MutationWorkerState.IDLE

    assigned = service.assign_worker(
        "effect.assign.worker-a",
        campaign_id,
        "worker-a",
        shard_id="shard-001",
        lease_id="lease-001",
        attempt=0,
        expected_revision=registered.value.revision_number,
    )
    assert isinstance(assigned, Success)
    assert assigned.value.status == MutationWorkerState.RUNNING

    heartbeat = WorkerHeartbeat(
        worker=_identity(),
        sequence=1,
        sent_at="2026-08-04T15:00:00Z",
        current_campaign_id=campaign_id.value,
        current_shard_id="shard-001",
        current_lease_id="lease-001",
        current_attempt=0,
        child_process_id=4321,
        completed_mutants=1,
    )
    renewed = service.heartbeat_worker(
        "effect.heartbeat.worker-a.1",
        campaign_id,
        heartbeat,
        expected_revision=assigned.value.revision_number,
    )
    assert isinstance(renewed, Success)
    assert renewed.value.last_child_process_id == 4321
    stale_heartbeat = service.heartbeat_worker(
        "effect.heartbeat.worker-a.stale",
        campaign_id,
        heartbeat,
        expected_revision=renewed.value.revision_number,
    )
    assert isinstance(stale_heartbeat, Rejected)
    assert stale_heartbeat.code == "stale_worker_heartbeat"

    delivering = service.mark_worker_delivering(
        "effect.delivering.worker-a",
        campaign_id,
        "worker-a",
        expected_revision=renewed.value.revision_number,
    )
    assert isinstance(delivering, Success)
    released = service.release_worker(
        "effect.release.worker-a",
        campaign_id,
        "worker-a",
        expected_revision=delivering.value.revision_number,
    )
    assert isinstance(released, Success)
    assert released.value.status == MutationWorkerState.IDLE
    assert released.value.current_shard_id is None

    stopped = service.stop_worker(
        "effect.stop.worker-a",
        campaign_id,
        "worker-a",
        expected_revision=released.value.revision_number,
    )
    assert isinstance(stopped, Success)
    assert stopped.value.status == MutationWorkerState.STOPPED
    assert stopped.value.last_child_process_id == 4321


def test_worker_does_not_acquire_before_authoritative_registration_receipt(tmp_path) -> None:
    # Hold the worker at registration until an external authority explicitly accepts the process instance.
    from pathlib import Path

    import pytest

    from theseus_contracts import WorkerMessageType
    from theseus_local.worker_runtime import PersistentWorkerProcess

    with PersistentWorkerProcess(
        worker_id="worker-registration-gate",
        spool_root=tmp_path / "spool",
        heartbeat_interval_seconds=0.02,
        cwd=Path(__file__).parents[1],
        auto_accept_registration=False,
    ) as worker:
        registration = worker.wait_for_frame(WorkerMessageType.REGISTER_WORKER, timeout=5.0)
        with pytest.raises(TimeoutError):
            worker.wait_for_frame(WorkerMessageType.ACQUIRE_ASSIGNMENT, timeout=0.15)
        worker.respond_registration(registration, accepted=True)
        worker.wait_for_frame(WorkerMessageType.ACQUIRE_ASSIGNMENT, timeout=5.0)
        worker.shutdown()
        worker.wait_for_frame(WorkerMessageType.WORKER_TERMINATED, timeout=5.0)
        assert worker.wait(timeout=5.0) == 0
