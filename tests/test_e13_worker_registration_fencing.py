from __future__ import annotations

from theseus_contracts import CampaignId, WorkerCapabilities, WorkerHeartbeat, WorkerIdentity
from gallifrey_mutation import InMemoryMutationStore, MutationCampaignService, Rejected, Success


def _capabilities() -> WorkerCapabilities:
    # Build one deterministic capability set shared by restarted process instances.
    return WorkerCapabilities("win32", "AMD64", ("3.14.2",), (1,), ("copy",), 2)


def test_active_worker_slot_rejects_new_instance_and_old_instance_cannot_heartbeat() -> None:
    # Fence both premature replacement and late heartbeats from an obsolete process identity.
    campaign_id = CampaignId("campaign-worker-fencing")
    service = MutationCampaignService(InMemoryMutationStore())
    first_identity = WorkerIdentity("worker-a", "instance-a", 1001, "birth-a")
    second_identity = WorkerIdentity("worker-a", "instance-b", 1002, "birth-b")
    first = service.register_worker(
        "effect.register.instance-a",
        campaign_id,
        first_identity,
        _capabilities(),
        workspace="D:/worker-a",
        spool_path="D:/worker-a/spool",
    )
    assert isinstance(first, Success)

    replacement = service.register_worker(
        "effect.register.instance-b-active",
        campaign_id,
        second_identity,
        _capabilities(),
        workspace="D:/worker-b",
        spool_path="D:/worker-b/spool",
    )
    assert isinstance(replacement, Rejected)
    assert replacement.code == "worker_slot_in_use"

    foreign = service.heartbeat_worker(
        "effect.heartbeat.instance-b",
        campaign_id,
        WorkerHeartbeat(second_identity, 1, "2026-08-04T15:01:00Z"),
        expected_revision=first.value.revision_number,
    )
    assert isinstance(foreign, Rejected)
    assert foreign.code == "stale_worker_instance"

    stopped = service.stop_worker(
        "effect.stop.instance-a",
        campaign_id,
        "worker-a",
        expected_revision=first.value.revision_number,
    )
    assert isinstance(stopped, Success)
    restarted = service.register_worker(
        "effect.register.instance-b-terminal",
        campaign_id,
        second_identity,
        _capabilities(),
        workspace="D:/worker-b",
        spool_path="D:/worker-b/spool",
    )
    assert isinstance(restarted, Success)
    assert restarted.value.identity == second_identity
    assert restarted.value.revision_number == stopped.value.revision_number + 1
