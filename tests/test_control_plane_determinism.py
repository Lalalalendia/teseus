from __future__ import annotations

import json
from pathlib import Path

import pytest

from theseus_contracts import RemoteExecutionRequest, RemoteExecutionResult
from theseus_contracts.serialization import dumps
from theseus_local.artifact_store import ContentAddressedArtifactStore
from theseus_local.canonical_report import canonical_json_text
from theseus_local.runtime_identity import current_runtime_identity
from theseus_local.runtime_identity import RuntimeCompatibilityError, RuntimeIdentity

from post_pr63_helpers import remote_fixture


def test_runtime_fingerprint_is_stable_under_mapping_order_and_volatile_metadata() -> None:
    first = current_runtime_identity(
        protocol_versions={"remote_execution": 1, "core": 1, "worker": 1},
        schema_versions={"worker": 1, "core": 1, "remote_execution": 1},
    )
    second = current_runtime_identity(
        protocol_versions={"worker": 1, "core": 1, "remote_execution": 1},
        schema_versions={"remote_execution": 1, "worker": 1, "core": 1},
    )
    assert first.runtime_fingerprint == second.runtime_fingerprint
    assert first.to_dict() == second.to_dict()


def test_meaningful_runtime_contract_changes_invalidate_fingerprint() -> None:
    baseline = current_runtime_identity()
    assert current_runtime_identity(theseus_version="99.0.0").runtime_fingerprint != baseline.runtime_fingerprint
    changed_protocol = dict(baseline.protocol_versions)
    changed_protocol["remote_execution"] += 1
    assert current_runtime_identity(protocol_versions=changed_protocol).runtime_fingerprint != baseline.runtime_fingerprint
    changed_schema = dict(baseline.schema_versions)
    changed_schema["remote_execution"] += 1
    assert current_runtime_identity(schema_versions=changed_schema).runtime_fingerprint != baseline.runtime_fingerprint


def test_runtime_identity_decoder_rejects_type_drift_and_unknown_fields() -> None:
    identity = current_runtime_identity().to_dict()
    wrong_schema = dict(identity, schema_version="1")
    with pytest.raises(RuntimeCompatibilityError):
        RuntimeIdentity.from_dict(wrong_schema)
    unknown = dict(identity, future_field=True)
    with pytest.raises(RuntimeCompatibilityError, match="unknown"):
        RuntimeIdentity.from_dict(unknown)


def test_request_and_result_json_round_trip_is_canonical(tmp_path: Path) -> None:
    _, request, _ = remote_fixture(tmp_path)
    shuffled = dict(request.to_dict())
    shuffled["environment"] = {"Z_LAST": "2", "A_FIRST": "1"}
    shuffled["runtime_identity"] = {
        key: request.runtime_identity[key]
        for key in reversed(tuple(request.runtime_identity))
    }
    request_with_env = RemoteExecutionRequest.from_dict(shuffled)
    assert request_with_env.to_json() == RemoteExecutionRequest.from_dict(
        json.loads(request_with_env.to_json())
    ).to_json()
    result = RemoteExecutionResult(
        execution_attempt_id=request.execution_attempt_id,
        evidence_identity=request.evidence_identity,
        mutation_identity=request.mutation_identity,
        started=True,
        exit_code=0,
        timed_out=False,
        cancelled=False,
        elapsed_seconds=0.1,
        worker_runtime_fingerprint=current_runtime_identity().runtime_fingerprint,
        workspace_integrity="verified",
    )
    assert result.to_json() == RemoteExecutionResult.from_json(result.to_json()).to_json()


def test_wire_set_and_nested_worker_order_are_canonical() -> None:
    assert dumps({"values": frozenset({"z-last", "a-first"})}) == (
        '{"values":["a-first","z-last"]}'
    )
    from theseus_local.operations import project_campaign

    report = {"campaign_id": "c1", "status": "complete", "counts": {}}
    first = project_campaign(
        report,
        workers=(
            {"worker_id": "worker-z", "state": "ready"},
            {"worker_id": "worker-a", "state": "ready"},
        ),
    )
    second = project_campaign(report, workers=tuple(reversed(first.worker_states)))
    assert first.to_dict() == second.to_dict()


def test_snapshot_identity_is_independent_of_root_path_and_file_creation_order(tmp_path: Path) -> None:
    first_root = tmp_path / "a"
    second_root = tmp_path / "b"
    first_root.mkdir()
    second_root.mkdir()
    (first_root / "z.py").write_text("Z = 1\n", encoding="utf-8")
    (first_root / "a.py").write_text("A = 1\n", encoding="utf-8")
    (second_root / "a.py").write_text("A = 1\n", encoding="utf-8")
    (second_root / "z.py").write_text("Z = 1\n", encoding="utf-8")
    store = ContentAddressedArtifactStore(tmp_path / "cas")
    first = store.create_snapshot(first_root)
    second = store.create_snapshot(second_root)
    assert first.snapshot_id == second.snapshot_id
    assert first.files == second.files


def test_canonical_report_projection_is_reproducible_after_nested_order_shuffle() -> None:
    report = {
        "campaign_id": "c1",
        "status": "complete",
        "counts": {"survived": 1, "killed": 1},
        "results": [
            {"status": "killed", "mutant": {"mutant_id": "m1", "source_path": "a.py"}},
            {"status": "survived", "mutant": {"source_path": "b.py", "mutant_id": "m2"}},
        ],
    }
    shuffled = {
        "results": list(report["results"]),
        "counts": {"killed": 1, "survived": 1},
        "status": "complete",
        "campaign_id": "c1",
    }
    assert dumps(report) == dumps(shuffled)
    first = canonical_json_text(report)
    second = canonical_json_text(json.loads(first))
    assert first == second
