from __future__ import annotations
from pathlib import Path
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    MutationScope,
    ProjectDescriptor,
    ProjectId,
    ShardDescriptor,
    ShardId,
    ShardLease,
    WorkerCapabilities,
    WorkerHeartbeat,
    WorkerId,
    WorkerIdentity,
    WorkerStatus,
)
from gallifrey_mutation import (
    InMemoryMutationStore,
    MutationCampaignService,
    MutationShard,
    Rejected,
    SQLiteMutationStore,
    Success,
)

def _identity() -> WorkerIdentity:
    # Build one process-fenced worker identity shared by unified lease tests.
    return WorkerIdentity("worker-lease", "instance-lease", 4101, "birth-lease")

def _capabilities() -> WorkerCapabilities:
    # Advertise the local engine and copy workspace required by assignment.
    return WorkerCapabilities("win32", "AMD64", ("3.14.2",), (1,), ("copy",), 2)

def _configuration(campaign_id: CampaignId) -> CampaignConfiguration:
    # Build one complete public campaign contract for fan-in validation tests.
    return CampaignConfiguration(
        campaign_id=campaign_id,
        project=ProjectDescriptor(
            project_id=ProjectId("project-unified-lease"),
            display_name="Unified lease fixture",
            root_path=".",
        ),
        scope=MutationScope(source_path="app.py"),
        budget=CampaignBudget(max_mutants=1, max_workers=1),
    )

def _setup(store):
    # Create one shard, registered worker and authoritative bound lease generation.
    service = MutationCampaignService(store)
    campaign_id = CampaignId("campaign-unified-lease")
    shard = MutationShard.from_descriptor(
        campaign_id,
        ShardDescriptor(ShardId("shard-unified-lease"), ("mutant-1",)),
        ordinal=0,
    )
    assert isinstance(store.save_shard(shard, expected_revision=None), Success)
    worker = service.register_worker(
        "effect.register.unified",
        campaign_id,
        _identity(),
        _capabilities(),
        workspace="D:/state/worker/workspace",
        spool_path="D:/state/worker/spool",
    )
    assert isinstance(worker, Success)
    public_lease = ShardLease(
        WorkerId("worker-lease"),
        "lease-unified-1",
        WorkerStatus.LEASED,
        30.0,
        "2026-08-04T15:00:00Z",
        heartbeat_seq=0,
        attempt=0,
        worker_instance_id="instance-lease",
    )
    claimed = service.claim_shard_lease(
        "effect.lease.claim",
        shard.shard_id,
        public_lease,
        expected_shard_revision=0,
    )
    assert isinstance(claimed, Success)
    pre = service.renew_claimed_lease(
        "effect.lease.pre-heartbeat",
        public_lease.lease_id,
        heartbeat_at="2026-08-04T15:00:01Z",
        heartbeat_sequence=1,
        expected_lease_revision=claimed.value.lease.revision_number,
        expected_shard_revision=claimed.value.shard.revision_number,
        now="2026-08-04T15:00:01Z",
    )
    assert isinstance(pre, Success)
    bound = service.bind_worker_lease(
        "effect.lease.bind",
        campaign_id,
        public_lease.lease_id,
        _identity(),
        expected_lease_revision=pre.value.lease.revision_number,
        expected_shard_revision=pre.value.shard.revision_number,
        expected_worker_revision=worker.value.revision_number,
    )
    assert isinstance(bound, Success)
    return service, campaign_id, bound.value


def test_worker_and_shard_do_not_advance_when_lease_heartbeat_is_stale() -> None:
    # Reject one stale heartbeat before any of the three authoritative projections can change.
    service, campaign_id, state = _setup(InMemoryMutationStore())
    assert state.worker is not None
    heartbeat = WorkerHeartbeat(
        _identity(),
        1,
        "2026-08-04T15:00:02Z",
        current_campaign_id=campaign_id.value,
        current_shard_id=state.shard.shard_id.value,
        current_lease_id=state.lease.lease_id,
        current_attempt=state.lease.attempt,
    )
    accepted = service.renew_worker_lease(
        "effect.lease.heartbeat.1",
        campaign_id,
        heartbeat,
        expected_lease_revision=state.lease.revision_number,
        expected_shard_revision=state.shard.revision_number,
        expected_worker_revision=state.worker.revision_number,
        now="2026-08-04T15:00:02Z",
    )
    assert isinstance(accepted, Success)
    replay = service.renew_worker_lease(
        "effect.lease.heartbeat.1",
        campaign_id,
        heartbeat,
        expected_lease_revision=state.lease.revision_number,
        expected_shard_revision=state.shard.revision_number,
        expected_worker_revision=state.worker.revision_number,
        now="2026-08-04T15:00:02Z",
    )
    assert isinstance(replay, Success) and replay.duplicate
    rejected = service.renew_worker_lease(
        "effect.lease.stale-heartbeat",
        campaign_id,
        heartbeat,
        expected_lease_revision=accepted.value.lease.revision_number,
        expected_shard_revision=accepted.value.shard.revision_number,
        expected_worker_revision=accepted.value.worker.revision_number,
        now="2026-08-04T15:00:02Z",
    )
    assert isinstance(rejected, Rejected)
    assert rejected.code == "stale_lease_heartbeat"
    lease = service.get_lease(state.lease.lease_id)
    worker = service.get_worker(campaign_id, state.worker.worker_id)
    shard = service.store.get_shard(state.shard.shard_id)
    assert isinstance(lease, Success) and lease.value == accepted.value.lease
    assert isinstance(worker, Success) and worker.value == accepted.value.worker
    assert isinstance(shard, Success) and shard.value == accepted.value.shard

def test_transport_returns_authoritative_heartbeat_receipt() -> None:
    # Keep lease revision and expiration returned by Gallifrey instead of replacing them with a synthetic ACK.
    from pathlib import Path
    source = (Path(__file__).parents[1] / "theseus_local" / "worker_runtime" / "process.py").read_text(encoding="utf-8")
    assert "authoritative_receipt = handler(frame)" in source
    assert "if isinstance(authoritative_receipt, HeartbeatReceipt)" in source

def test_sqlite_cas_failure_rolls_back_every_lease_projection(tmp_path: Path) -> None:
    # Roll back lease and shard writes when the worker compare-and-swap fails in the same transaction.
    from gallifrey_mutation import EffectReceipt
    path = tmp_path / "atomic-heartbeat.sqlite3"
    store = SQLiteMutationStore(path)
    service, campaign_id, state = _setup(store)
    assert state.worker is not None
    heartbeat = WorkerHeartbeat(
        _identity(),
        1,
        "2026-08-04T15:00:02Z",
        current_campaign_id=campaign_id.value,
        current_shard_id=state.shard.shard_id.value,
        current_lease_id=state.lease.lease_id,
        current_attempt=state.lease.attempt,
    )
    lease_candidate = state.lease.renew(
        heartbeat,
        expected_revision=state.lease.revision_number,
        now="2026-08-04T15:00:02Z",
    )
    assert isinstance(lease_candidate, Success)
    shard_candidate = state.shard.renew_lease(
        lease_candidate.value.to_shard_lease(),
        expected_revision=state.shard.revision_number,
        now="2026-08-04T15:00:02Z",
    )
    worker_candidate = state.worker.heartbeat(
        heartbeat,
        expected_revision=state.worker.revision_number,
    )
    assert isinstance(shard_candidate, Success)
    assert isinstance(worker_candidate, Success)
    committed = store.commit_effect(
        receipt=EffectReceipt(
            "effect.atomic.rollback",
            "mutation.lease.heartbeat",
            campaign_id,
            {"lease": lease_candidate.value.to_dict()},
        ),
        lease=lease_candidate.value,
        lease_expected_revision=state.lease.revision_number,
        shard=shard_candidate.value,
        shard_expected_revision=state.shard.revision_number,
        worker=worker_candidate.value,
        worker_expected_revision=state.worker.revision_number + 99,
    )
    assert isinstance(committed, Rejected)
    assert store.get_lease(state.lease.lease_id).value == state.lease
    assert store.get_shard(state.shard.shard_id).value == state.shard
    assert store.get_worker(campaign_id, state.worker.worker_id).value == state.worker
    store.close()

def test_process_forwards_authoritative_receipt_payload(monkeypatch) -> None:
    # Forward the exact Gallifrey lease receipt instead of synthesizing an unconditional success.
    from theseus_contracts import (
        WorkerHeartbeatFrame,
        WorkerHeartbeatReceipt,
        WorkerMessageType,
        WorkerProtocolFrame,
    )
    from theseus_local.worker_runtime import PersistentWorkerProcess
    worker = PersistentWorkerProcess(
        worker_id="worker-lease",
        instance_id="instance-lease",
        spool_root=Path("spool-not-started"),
    )
    worker._worker_pid = 4101
    expected = __import__("theseus_contracts").HeartbeatReceipt(
        True,
        True,
        lease_revision=17,
        lease_expires_at="2026-08-04T15:01:00Z",
    )
    worker.set_heartbeat_handler(lambda frame: expected)
    captured = {}
    def capture(message_type, payload, **kwargs):
        # Capture one host response without requiring a real subprocess pipe.
        captured["message_type"] = message_type
        captured["payload"] = payload
        captured["kwargs"] = kwargs
        return None
    monkeypatch.setattr(worker, "_send_payload", capture)
    heartbeat = WorkerHeartbeat(
        _identity(),
        1,
        "2026-08-04T15:00:30Z",
        current_campaign_id="campaign-unified-lease",
        current_shard_id="shard-unified-lease",
        current_lease_id="lease-unified-1",
        current_attempt=0,
    )
    frame = WorkerProtocolFrame.create(
        WorkerMessageType.WORKER_HEARTBEAT,
        WorkerHeartbeatFrame(heartbeat, 0),
        worker_id="worker-lease",
        instance_id="instance-lease",
        process_id=4101,
        sequence=2,
        state="running",
    )
    assert worker._handle_process_frame(frame) is True
    assert captured["message_type"] == WorkerMessageType.HEARTBEAT_RECEIPT
    assert isinstance(captured["payload"], WorkerHeartbeatReceipt)
    assert captured["payload"].receipt == expected
