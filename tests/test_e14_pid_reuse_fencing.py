from __future__ import annotations
import os
from theseus_local.worker_runtime import recorded_process_is_alive, terminate_recorded_process
from test_intelligence_unified_v1.recovery import current_process_birth_token

def test_reused_pid_token_is_never_treated_as_the_recorded_worker() -> None:
    # Refuse both liveness and termination when a live PID has a different process birth token.
    actual = current_process_birth_token(os.getpid())
    assert actual is not None
    stale = f"{actual}-stale"
    assert recorded_process_is_alive(os.getpid(), stale) is False
    assert terminate_recorded_process(os.getpid(), stale) is False
    assert recorded_process_is_alive(os.getpid(), actual) is True

def test_worker_projection_preserves_launcher_and_engine_birth_tokens() -> None:
    # Keep all three process identities durable so restart recovery never trusts a bare PID.
    from theseus_contracts import CampaignId, WorkerCapabilities, WorkerHeartbeat, WorkerIdentity
    from gallifrey_mutation import MutationWorker
    identity = WorkerIdentity("worker-token", "instance-token", 5001, "birth-worker")
    worker = MutationWorker.register(
        CampaignId("campaign-token"),
        identity,
        WorkerCapabilities("win32", "AMD64", ("3.14.2",), (1,), ("copy",), 2),
        workspace="D:/workers/token/workspace",
        spool_path="D:/workers/token/spool",
        launcher_process_id=5000,
        launcher_process_birth_token="birth-launcher",
    )
    assigned = worker.assign(
        shard_id="shard-token",
        lease_id="lease-token",
        attempt=0,
        expected_revision=worker.revision_number,
    )
    heartbeat = WorkerHeartbeat(
        identity,
        1,
        "2026-08-05T00:00:00Z",
        current_campaign_id="campaign-token",
        current_shard_id="shard-token",
        current_lease_id="lease-token",
        current_attempt=0,
        child_process_id=5002,
        child_process_birth_token="birth-child",
    )
    renewed = assigned.value.heartbeat(
        heartbeat,
        expected_revision=assigned.value.revision_number,
    )
    restored = MutationWorker.from_dict(renewed.value.to_dict())
    assert restored == renewed.value
    assert restored.launcher_process_birth_token == "birth-launcher"
    assert restored.child_process_birth_token == "birth-child"
    assert restored.last_child_process_birth_token == "birth-child"
