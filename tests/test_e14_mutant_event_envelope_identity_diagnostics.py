from __future__ import annotations

import io
import os

import pytest

from theseus_local.worker_runtime.agent import AgentRun
from theseus_local.worker_runtime.entrypoint import PersistentWorkerEntrypoint


def _mutant_result() -> dict[str, object]:
    # Build one restored engine result with the minimum durable evidence contract.
    return {
        "execution_id": "execution-partial-assignment",
        "mutant_id": "mutant-partial-assignment",
        "status": "killed",
        "classification_reason": "diagnostic fixture",
        "restore_verified": True,
        "level_results": [],
        "artifact_paths": [],
        "lease_id": "lease-partial-assignment",
        "attempt": 0,
        "test_observations": [],
    }


def _runtime(tmp_path) -> PersistentWorkerEntrypoint:
    # Create an isolated runtime without starting its control loop or engine child.
    return PersistentWorkerEntrypoint(
        worker_id="worker-partial-assignment",
        instance_id="instance-partial-assignment",
        spool_root=tmp_path / "spool",
        input_stream=io.StringIO(),
        output_stream=io.StringIO(),
    )


def test_engine_committed_partial_assignment_builds_typed_execution_envelope(tmp_path) -> None:
    # Prove per-mutant engine events need only their documented compact ownership identity.
    runtime = _runtime(tmp_path)
    event_id = runtime.agent.spool.publish_mutant_result(
        assignment={
            "campaign_id": "campaign-partial-assignment",
            "shard_id": "shard-partial-assignment",
            "lease_id": "lease-partial-assignment",
            "attempt": 0,
        },
        worker={
            "worker_id": runtime.identity.worker_id,
            "instance_id": runtime.identity.instance_id,
            "process_id": os.getpid(),
            "process_birth_token": "birth-partial-assignment",
        },
        source_sha256="source-partial-assignment",
        mutant_result=_mutant_result(),
    )
    delivery = AgentRun(
        event_id="delivery-partial-assignment",
        payload_sha256="delivery-payload-sha",
        payload={},
        mutant_event_ids=(event_id,),
    )

    envelopes = runtime._execution_envelopes(delivery)

    assert len(envelopes) == 1, (
        "E14 per-mutant envelope invariant violated: one committed mutant event must "
        "produce exactly one typed execution envelope.\n"
        f"event_id={event_id!r}\n"
        f"envelope_count={len(envelopes)!r}\n"
        f"spool_root={str(runtime.agent.spool.root)!r}"
    )
    envelope = envelopes[0]
    assert (
        envelope.campaign_id,
        envelope.shard_id,
        envelope.lease_id,
        envelope.attempt,
    ) == (
        "campaign-partial-assignment",
        "shard-partial-assignment",
        "lease-partial-assignment",
        0,
    ), (
        "E14 ownership projection changed while converting a compact per-mutant event.\n"
        f"event_id={event_id!r}\n"
        f"actual={(envelope.campaign_id, envelope.shard_id, envelope.lease_id, envelope.attempt)!r}"
    )


def test_conflicting_frame_and_payload_identity_reports_the_exact_event(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Diagnose corruption with the event ID and both conflicting ownership projections.
    runtime = _runtime(tmp_path)
    event_id = "event-conflicting-assignment"
    frame = {
        "event_id": event_id,
        "campaign_id": "campaign-top-level",
        "shard_id": "shard-conflict",
        "lease_id": "lease-conflict",
        "attempt": 0,
        "payload_sha256": "payload-sha",
        "payload": {
            "assignment": {
                "campaign_id": "campaign-payload",
                "shard_id": "shard-conflict",
                "lease_id": "lease-conflict",
                "attempt": 0,
            },
            "worker": {
                "worker_id": runtime.identity.worker_id,
                "instance_id": runtime.identity.instance_id,
                "process_id": os.getpid(),
                "process_birth_token": "birth-conflict",
            },
            "mutant_result": _mutant_result(),
        },
    }
    monkeypatch.setattr(runtime.agent.spool, "mutant_event", lambda _: frame)
    delivery = AgentRun(
        event_id="delivery-conflict",
        payload_sha256="delivery-payload-sha",
        payload={},
        mutant_event_ids=(event_id,),
    )

    with pytest.raises(RuntimeError) as captured:
        runtime._execution_envelopes(delivery)

    message = str(captured.value)
    assert event_id in message and "campaign-top-level" in message and "campaign-payload" in message, (
        "E14 corruption diagnostics omitted the event or one side of the identity conflict.\n"
        f"event_id={event_id!r}\n"
        f"diagnostic={message!r}"
    )
