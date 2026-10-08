from __future__ import annotations

import json
from pathlib import Path

from theseus_contracts import CampaignId, WorkerCapabilities, WorkerHeartbeat, WorkerIdentity
from gallifrey_mutation import MutationCampaignService, SQLiteMutationStore, Success
from theseus_local.worker_pool import WorkerRegistryProjection


def test_sqlite_registry_survives_restart_and_recreates_deleted_json(tmp_path: Path) -> None:
    # Prove diagnostic JSON can be deleted and rebuilt without affecting authoritative worker state.
    campaign_id = CampaignId("campaign-worker-restart")
    database_path = tmp_path / "campaign.sqlite3"
    projection_path = tmp_path / "worker-registry.json"
    identity = WorkerIdentity("worker-a", "instance-a", 1234, "birth-a")
    capabilities = WorkerCapabilities("win32", "AMD64", ("3.14.2",), (1,), ("copy",), 2)

    store = SQLiteMutationStore(database_path)
    service = MutationCampaignService(store)
    registered = service.register_worker(
        "effect.register.worker-a",
        campaign_id,
        identity,
        capabilities,
        workspace=str(tmp_path / "workspace"),
        spool_path=str(tmp_path / "spool"),
        launcher_process_id=1200,
    )
    assert isinstance(registered, Success)
    heartbeat = service.heartbeat_worker(
        "effect.heartbeat.worker-a.1",
        campaign_id,
        WorkerHeartbeat(identity, 1, "2026-08-04T15:02:00Z"),
        expected_revision=registered.value.revision_number,
    )
    assert isinstance(heartbeat, Success)
    WorkerRegistryProjection(projection_path, campaign_id=campaign_id.value, store=store).refresh()
    projection_path.write_text(
        '{"source":"untrusted","workers":[{"worker_id":"worker-a","state":"failed"}]}',
        encoding="utf-8",
    )
    assigned = service.assign_worker(
        "effect.assign.worker-a",
        campaign_id,
        "worker-a",
        shard_id="shard-001",
        lease_id="lease-001",
        attempt=0,
        expected_revision=heartbeat.value.revision_number,
    )
    assert isinstance(assigned, Success)
    projection_path.unlink()
    store.close()

    reopened = SQLiteMutationStore(database_path)
    try:
        workers = MutationCampaignService(reopened).list_workers(campaign_id)
        assert isinstance(workers, Success)
        assert workers.value == (assigned.value,)
        rows = WorkerRegistryProjection(
            projection_path,
            campaign_id=campaign_id.value,
            store=reopened,
        ).refresh()
        assert projection_path.is_file()
        assert rows[0]["instance_id"] == "instance-a"
        payload = json.loads(projection_path.read_text(encoding="utf-8"))
        assert payload["source"] == "gallifrey.mutation_workers"
        assert payload["workers"] == list(rows)
    finally:
        reopened.close()
