from __future__ import annotations

import inspect

from theseus_contracts import (
    WORKER_TO_HOST_MESSAGE_TYPES,
    AcquireAssignment,
    RegisterWorker,
    WorkerCapabilities,
    WorkerIdentity,
    WorkerMessageType,
    WorkerProtocolFrame,
    decode_message,
    decode_worker_frame,
    encode_worker_frame,
)
from theseus_local.worker_runtime import PersistentWorkerEntrypoint, PersistentWorkerProcess


def test_worker_registration_roundtrips_as_a_typed_versioned_frame() -> None:
    # Keep registration identity and capabilities typed through canonical encode/decode.
    identity = WorkerIdentity("worker-typed", "instance-typed", 1234, "birth-typed")
    capabilities = WorkerCapabilities(
        platform="linux",
        architecture="x86_64",
        python_versions=("3.13",),
        engine_protocol_versions=(1,),
        workspace_backends=("copy",),
        cpu_count=4,
    )
    frame = WorkerProtocolFrame.create(
        WorkerMessageType.REGISTER_WORKER,
        RegisterWorker(identity, capabilities, "/tmp/spool"),
        worker_id=identity.worker_id,
        instance_id=identity.instance_id,
        process_id=identity.process_id,
        sequence=1,
        state="registered",
    )
    encoded = encode_worker_frame(frame)
    decoded = decode_worker_frame(encoded, allowed_types=WORKER_TO_HOST_MESSAGE_TYPES)
    generic = decode_message(encoded)
    assert decoded == frame
    assert generic == frame
    assert isinstance(decoded.payload, RegisterWorker)
    assert decoded.payload.identity == identity
    assert decoded.message_id == decoded.request_id


def test_acquire_frame_carries_request_identity_and_compatibility_projection() -> None:
    # Preserve existing coordinator reads as a projection over a typed frame rather than a private dict wire format.
    frame = WorkerProtocolFrame.create(
        WorkerMessageType.ACQUIRE_ASSIGNMENT,
        AcquireAssignment(3),
        worker_id="worker-typed",
        instance_id="instance-typed",
        process_id=1234,
        sequence=7,
        state="idle",
    )
    projection = frame.to_compat_dict()
    assert projection["message_type"] == "worker.acquire_assignment"
    assert projection["event"] == "acquire"
    assert projection["payload"]["completed_assignments"] == 3
    assert projection["request_id"] == frame.message_id


def test_private_worker_command_event_wire_is_removed_from_production_runtime() -> None:
    # Prevent the stringly typed PR 1 adapter from becoming reachable again.
    source = inspect.getsource(PersistentWorkerEntrypoint) + inspect.getsource(PersistentWorkerProcess)
    assert "_PRIVATE_PROTOCOL_VERSION" not in source
    assert "WorkerProcessEvent" not in source
    assert "json.loads(line)" not in source
    assert "unsupported private worker" not in source


def test_shard_assignment_roundtrips_with_explicit_execution_spec() -> None:
    # Prevent the old opaque assignment command dictionary from returning inside a typed outer frame.
    from datetime import datetime, timedelta, timezone

    from theseus_contracts import (
        HOST_TO_WORKER_MESSAGE_TYPES,
        AssignShard,
        ShardAssignment,
        WorkerExecutionMode,
        WorkerExecutionSpec,
    )

    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat().replace("+00:00", "Z")
    assignment = ShardAssignment(
        campaign_id="campaign-typed",
        shard_id="shard-typed",
        lease_id="lease-typed",
        attempt=0,
        mutant_ids=("m1",),
        prepared_snapshot_id="snapshot-typed",
        workspace_descriptor_id="workspace-typed",
        expires_at=expires_at,
    )
    execution = WorkerExecutionSpec.engine(
        configuration={"campaign_id": "campaign-typed"},
        execute_request=None,
        workspace="/tmp/workspace",
        report_root="/tmp/reports",
        publish_engine_root=None,
        expected_source_sha256="source-sha",
        expected_mutant_ids=("m1",),
        test_fingerprints={"m1": {}},
        command_timeouts={"prepare": 5.0},
        command=None,
        cancel_path=None,
    )
    frame = WorkerProtocolFrame.create(
        WorkerMessageType.SHARD_ASSIGNMENT,
        AssignShard(assignment, execution),
        worker_id="worker-typed",
        instance_id="instance-typed",
        process_id=1234,
        sequence=2,
        state="host",
        correlation_id="correlation-typed",
    )
    decoded = decode_worker_frame(frame.to_json(), allowed_types=HOST_TO_WORKER_MESSAGE_TYPES)
    assert isinstance(decoded.payload, AssignShard)
    assert isinstance(decoded.payload.execution, WorkerExecutionSpec)
    assert decoded.payload.execution.mode == WorkerExecutionMode.ENGINE
    assert decoded.payload.execution.expected_mutant_ids == ("m1",)


def test_production_coordinator_consumes_typed_frames_not_compatibility_dicts() -> None:
    # Keep the compatibility projection outside the production campaign execution path.
    root = __import__("pathlib").Path(__file__).parents[1]
    source = (root / "theseus_local" / "coordinator.py").read_text(encoding="utf-8")
    assert "worker.wait_for_frame(" in source
    assert "worker.wait_for(" not in source
    assert "WorkerHeartbeatFrame" in source
    assert "ExecutionDelivery" in source


def test_execution_delivery_roundtrips_without_an_opaque_result_mapping() -> None:
    # Keep durable shard output and transport metadata in concrete public DTOs.
    from datetime import datetime, timedelta, timezone

    from theseus_contracts import (
        ExecutionDelivery,
        ShardAssignment,
        ShardExecutionResult,
        ShardId,
        WorkerId,
        WorkerStatus,
    )

    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat().replace("+00:00", "Z")
    identity = WorkerIdentity("worker-delivery", "instance-delivery", 1234, "birth-delivery")
    capabilities = WorkerCapabilities(
        platform="linux",
        architecture="x86_64",
        python_versions=("3.13",),
        engine_protocol_versions=(1,),
        workspace_backends=("copy",),
        cpu_count=2,
    )
    assignment = ShardAssignment(
        campaign_id="campaign-delivery",
        shard_id="shard-delivery",
        lease_id="lease-delivery",
        attempt=0,
        mutant_ids=("m1",),
        prepared_snapshot_id="snapshot-delivery",
        workspace_descriptor_id="workspace-delivery",
        expires_at=expires_at,
    )
    payload = ExecutionDelivery(
        event_id="event-delivery",
        payload_sha256="payload-sha",
        assignment=assignment,
        worker=identity,
        capabilities=capabilities,
        shard_result=ShardExecutionResult(
            shard_id=ShardId("shard-delivery"),
            worker_id=WorkerId("worker-delivery"),
            status=WorkerStatus.COMPLETE,
            completed_mutants=0,
        ),
        engine_process_id=5678,
        published_engine_artifacts=("engine-shard.json",),
        mutant_event_ids=(),
        execution_envelopes=(),
        assignment_number=1,
    )
    frame = WorkerProtocolFrame.create(
        WorkerMessageType.EXECUTION_DELIVERY,
        payload,
        worker_id=identity.worker_id,
        instance_id=identity.instance_id,
        process_id=identity.process_id,
        sequence=8,
        state="delivering",
        correlation_id="correlation-delivery",
    )
    decoded = decode_worker_frame(frame.to_json(), allowed_types=WORKER_TO_HOST_MESSAGE_TYPES)
    assert isinstance(decoded.payload, ExecutionDelivery)
    assert isinstance(decoded.payload.shard_result, ShardExecutionResult)
    assert decoded.payload.engine_process_id == 5678
    assert not hasattr(decoded.payload, "payload")
