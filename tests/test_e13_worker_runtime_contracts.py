from __future__ import annotations

from theseus_contracts import (
    ExecutionEnvelope,
    HeartbeatReceipt,
    ShardAssignment,
    WorkerCapabilities,
    WorkerHeartbeat,
    WorkerIdentity,
)


def test_worker_identity_and_capabilities_roundtrip() -> None:
    # Keep slot identity, process birth and scheduling capabilities explicit on the wire.
    identity = WorkerIdentity("local-worker-001", "instance-abc", 1234, "birth-xyz")
    capabilities = WorkerCapabilities(
        platform="linux",
        architecture="x86_64",
        python_versions=("3.12",),
        engine_protocol_versions=(1,),
        workspace_backends=("copy",),
        cpu_count=2,
        memory_limit_bytes=1024,
    )
    assert WorkerIdentity.from_dict(identity.to_dict()) == identity
    assert WorkerCapabilities.from_dict(capabilities.to_dict()) == capabilities


def test_assignment_heartbeat_and_receipt_are_typed() -> None:
    # Ensure an agent can carry assignment progress and stop on an authoritative negative receipt.
    assignment = ShardAssignment(
        campaign_id="campaign-1",
        shard_id="shard-001",
        lease_id="lease-001",
        attempt=2,
        mutant_ids=("m1", "m2"),
        prepared_snapshot_id="snapshot-1",
        workspace_descriptor_id="workspace-1",
        expires_at="2026-08-03T12:00:00Z",
    )
    heartbeat = WorkerHeartbeat(
        worker=WorkerIdentity("local-worker-001", "instance-abc", 1234, "birth-xyz"),
        sequence=4,
        sent_at="2026-08-03T12:00:00Z",
        current_campaign_id="campaign-1",
        current_shard_id="shard-001",
        current_lease_id="lease-001",
        current_attempt=2,
        completed_mutants=1,
    )
    receipt = HeartbeatReceipt(False, False, reason="stale_worker_instance")
    assert ShardAssignment.from_dict(assignment.to_dict()) == assignment
    assert WorkerHeartbeat.from_dict(heartbeat.to_dict()) == heartbeat
    assert HeartbeatReceipt.from_dict(receipt.to_dict()) == receipt


def test_execution_envelope_requires_identity() -> None:
    # Prevent an unbound result from entering a worker spool or authoritative delivery boundary.
    envelope = ExecutionEnvelope(
        event_id="event-1",
        execution_id="execution-1",
        campaign_id="campaign-1",
        shard_id="shard-001",
        mutant_id="m1",
        worker_id="local-worker-001",
        worker_instance_id="instance-abc",
        lease_id="lease-001",
        attempt=0,
        semantic_result="killed",
        restore_verified=True,
        test_observations=(),
        artifact_refs=(),
        payload_sha256="sha256",
    )
    assert envelope.to_dict()["worker_instance_id"] == "instance-abc"
