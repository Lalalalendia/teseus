from __future__ import annotations

from theseus_contracts import CampaignId, WorkerCapabilities, WorkerIdentity
from gallifrey_mutation import InMemoryMutationStore, MutationCampaignService, Rejected, Success


def test_assignment_requires_capabilities_and_one_active_shard_per_worker() -> None:
    # Reject incompatible or concurrent assignments before any worker workspace is mutated.
    campaign_id = CampaignId("campaign-worker-capabilities")
    service = MutationCampaignService(InMemoryMutationStore())
    registered = service.register_worker(
        "effect.register.worker-a",
        campaign_id,
        WorkerIdentity("worker-a", "instance-a", 1234, "birth-a"),
        WorkerCapabilities("win32", "AMD64", ("3.14.2",), (1,), ("copy",), 2),
        workspace="D:/workspace",
        spool_path="D:/spool",
    )
    assert isinstance(registered, Success)

    incompatible = service.assign_worker(
        "effect.assign.protocol-2",
        campaign_id,
        "worker-a",
        shard_id="shard-001",
        lease_id="lease-001",
        attempt=0,
        expected_revision=registered.value.revision_number,
        engine_protocol_version=2,
    )
    assert isinstance(incompatible, Rejected)
    assert incompatible.code == "worker_capability_mismatch"

    assigned = service.assign_worker(
        "effect.assign.valid",
        campaign_id,
        "worker-a",
        shard_id="shard-001",
        lease_id="lease-001",
        attempt=0,
        expected_revision=registered.value.revision_number,
    )
    assert isinstance(assigned, Success)

    concurrent = service.assign_worker(
        "effect.assign.concurrent",
        campaign_id,
        "worker-a",
        shard_id="shard-002",
        lease_id="lease-002",
        attempt=0,
        expected_revision=assigned.value.revision_number,
    )
    assert isinstance(concurrent, Rejected)
    assert concurrent.code == "worker_not_idle"
