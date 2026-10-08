from __future__ import annotations

import json
from io import StringIO
import random
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from theseus_contracts.remote_protocol import RemoteExecutionRequest, RemoteExecutionResult, RemoteProtocolError
from theseus_contracts.serialization import SerializationError
from theseus_local.distributed import DistributedScheduler

from post_pr63_helpers import physical_result, remote_fixture


@pytest.mark.parametrize(
    "raw",
    (
        "",
        "{",
        "[]",
        "null",
        "\xff".encode("latin1"),
        '{"protocol_version": 1, "protocol_version": 1}',
    ),
)
def test_malformed_remote_frames_fail_closed(raw: str | bytes, tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    if raw == "":
        payload = raw
    elif raw == "\xff".encode("latin1"):
        payload = raw
    elif raw == '{"protocol_version": 1, "protocol_version": 1}':
        payload = raw
    else:
        payload = raw
    with pytest.raises((RemoteProtocolError, SerializationError, ValueError, TypeError)):
        RemoteExecutionRequest.from_json(payload)
    assert worker.state.value == "ready"
    assert request.execution_attempt_id == "attempt-0"


def test_jsonl_malformed_frame_is_reported_without_execution(tmp_path: Path) -> None:
    _, _, worker = remote_fixture(tmp_path)
    input_stream = StringIO('{"message_type":"request","argv":[]}\n{"message_type":"shutdown"}\n')
    output_stream = StringIO()
    assert worker.serve_jsonl(input_stream, output_stream) == 0
    frames = [json.loads(line) for line in output_stream.getvalue().splitlines()]
    assert frames[0]["message_type"] == "registration"
    assert frames[1]["message_type"] == "error"
    assert "unknown" in frames[1]["error"] or "execution_attempt_id" in frames[1]["error"]
    assert frames[-1]["message_type"] != "result"
    assert worker.state.value == "stopped"


def test_schema_version_and_field_type_mismatch_are_controlled_errors(tmp_path: Path) -> None:
    _, request, _ = remote_fixture(tmp_path)
    payload = request.to_dict()
    missing = dict(payload)
    missing.pop("schema_version")
    with pytest.raises((RemoteProtocolError, ValueError)):
        RemoteExecutionRequest.from_dict(missing)
    future = dict(payload)
    future["schema_version"] = 99
    with pytest.raises(RemoteProtocolError):
        RemoteExecutionRequest.from_dict(future)
    wrong_type = dict(payload)
    wrong_type["argv"] = "not-an-array"
    with pytest.raises(RemoteProtocolError):
        RemoteExecutionRequest.from_dict(wrong_type)


def test_deadline_requires_utc_and_expired_deadline_does_not_execute(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    with pytest.raises(RemoteProtocolError, match="UTC"):
        replace(request, deadline_utc="2030-01-01T00:00:00+03:00")
    with pytest.raises(RemoteProtocolError, match="UTC"):
        replace(request, deadline_utc="2030-01-01T00:00:00")
    expired = replace(request, deadline_utc="2000-01-01T00:00:00Z")
    result = worker.execute(expired)
    assert result.started is False
    assert result.workspace_integrity == "failed"
    future = replace(
        request,
        execution_attempt_id="future-deadline",
        evidence_identity="future-deadline",
        deadline_utc=(datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
    )
    assert worker.execute(future).exit_code == 0


def test_result_spoof_mismatch_is_rejected_before_authority(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    scheduler = DistributedScheduler(tmp_path / "scheduler.json")
    scheduler.register_worker(worker.registration())
    scheduler.submit(request)
    lease = scheduler.claim(worker.worker_id)
    assert lease is not None
    wrong_worker = physical_result(lease.request, worker_fingerprint="wrong-runtime")
    assert scheduler.complete(worker.worker_id, lease.lease_id, wrong_worker).reason == "result_runtime_mismatch"
    wrong_artifact = replace(
        physical_result(lease.request, worker_fingerprint=worker.runtime_identity.runtime_fingerprint),
        prepared_artifact_sha256="0" * 64,
    )
    assert scheduler.complete(worker.worker_id, lease.lease_id, wrong_artifact).reason == "result_artifact_mismatch"
    accepted = scheduler.complete(
        worker.worker_id,
        lease.lease_id,
        replace(
            physical_result(lease.request, worker_fingerprint=worker.runtime_identity.runtime_fingerprint),
            prepared_artifact_sha256=request.prepared_artifact_id,
        ),
    )
    assert accepted.authoritative is True


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    (
        ("execution_attempt_id", "spoofed-attempt", "result_identity_mismatch"),
        ("evidence_identity", "spoofed-evidence", "result_identity_mismatch"),
        ("mutation_identity", "spoofed-mutation", "result_identity_mismatch"),
        ("source_sha256", "0" * 64, "result_source_mismatch"),
        ("prepared_artifact_sha256", "0" * 64, "result_artifact_mismatch"),
    ),
)
def test_every_result_binding_identity_is_fenced_before_authority(
    tmp_path: Path,
    field: str,
    value: str,
    reason: str,
) -> None:
    _, request, worker = remote_fixture(tmp_path / field)
    scheduler = DistributedScheduler(tmp_path / field / "scheduler.json")
    scheduler.register_worker(worker.registration())
    scheduler.submit(request)
    lease = scheduler.claim(worker.worker_id)
    assert lease is not None
    spoofed = replace(
        physical_result(lease.request, worker_fingerprint=worker.runtime_identity.runtime_fingerprint),
        **{field: value},
    )
    completion = scheduler.complete(worker.worker_id, lease.lease_id, spoofed)
    assert completion.authoritative is False
    assert completion.reason == reason
    assert scheduler.authoritative(request.evidence_identity) is None


def test_duplicate_delivery_count_does_not_change_semantic_authority(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    scheduler = DistributedScheduler(tmp_path / "scheduler.json")
    scheduler.register_worker(worker.registration())
    assert scheduler.submit(request)
    assert all(scheduler.submit(request) is False for _ in range(9))
    lease = scheduler.claim(worker.worker_id)
    assert lease is not None
    result = physical_result(lease.request, worker_fingerprint=worker.runtime_identity.runtime_fingerprint)
    assert scheduler.complete(worker.worker_id, lease.lease_id, result).authoritative
    assert scheduler.snapshot()["authoritative"] == (request.evidence_identity,)


def test_protocol_validator_property_smoke_never_leaks_uncontrolled_exception() -> None:
    randomizer = random.Random(20260811)
    values = [None, [], "text", 1, True]
    for _ in range(250):
        values.append(
            {
                randomizer.choice(("protocol_version", "schema_version", "argv", "runtime_identity", "timeout_seconds")): randomizer.choice(
                    (None, [], {}, "text", -1, 0, randomizer.random())
                )
            }
        )
    for value in values:
        try:
            RemoteExecutionRequest.from_dict(value)  # type: ignore[arg-type]
        except (RemoteProtocolError, SerializationError, ValueError, TypeError, KeyError, OverflowError):
            continue
        except Exception as exc:  # pragma: no cover - the assertion documents the fail-closed boundary
            raise AssertionError(f"uncontrolled protocol exception: {type(exc).__name__}: {exc}") from exc


def test_result_contract_round_trip_rejects_unknown_authority_fields(tmp_path: Path) -> None:
    _, request, _ = remote_fixture(tmp_path)
    result = physical_result(request)
    payload = json.loads(result.to_json())
    payload["killed"] = True
    with pytest.raises(RemoteProtocolError, match="unknown"):
        RemoteExecutionResult.from_dict(payload)
