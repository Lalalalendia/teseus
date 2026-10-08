"""Diagnostic regressions for E14 prepared snapshot identity transport."""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
import pytest
from theseus_contracts import AssignShard, ShardAssignment, WorkerExecutionSpec
from theseus_local.worker_runtime import PersistentWorkerProcess, WorkerProcessError


def _assignment(snapshot_id: str = "snapshot-authoritative") -> ShardAssignment:
    # Build one immutable assignment with an explicit authoritative prepared snapshot identity.
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat().replace(
        "+00:00",
        "Z",
    )
    return ShardAssignment(
        campaign_id="campaign-snapshot-transport",
        shard_id="shard-000",
        lease_id="lease-snapshot-transport",
        attempt=2,
        mutant_ids=("m1",),
        prepared_snapshot_id=snapshot_id,
        workspace_descriptor_id="workspace-snapshot-transport",
        expires_at=expires_at,
    )


def _engine_spec(*, nested_snapshot_id: str | None) -> WorkerExecutionSpec:
    # Build the typed engine specification used to verify nested request normalization.
    execute_request: dict[str, Any] = {
        "campaign_id": "campaign-snapshot-transport",
        "shard": {"shard_id": "shard-000", "mutant_ids": ["m1"]},
        "attempt": 2,
        "worker_id": "worker-snapshot-transport",
        "lease_id": "lease-snapshot-transport",
        "test_overrides": {},
    }
    if nested_snapshot_id is not None:
        execute_request["prepared_snapshot_id"] = nested_snapshot_id
    return WorkerExecutionSpec.engine(
        configuration={"campaign_id": "campaign-snapshot-transport"},
        execute_request=execute_request,
        workspace="workspace",
        report_root="reports",
        publish_engine_root=None,
        expected_source_sha256="source-sha256",
        prepared_snapshot_id="snapshot-authoritative",
        expected_mutant_ids=("m1",),
        test_fingerprints={"m1": {}},
        command_timeouts={"prepare": 5.0, "execute-shard": 5.0, "shutdown": 2.0},
        command=None,
        cancel_path=None,
    )


def test_typed_worker_spec_injects_snapshot_into_nested_execute_request() -> None:
    # Prove the typed wire model cannot silently drop the authoritative snapshot at the nested engine boundary.
    specification = _engine_spec(nested_snapshot_id=None)
    restored = WorkerExecutionSpec.from_dict(specification.to_dict())

    assert restored.prepared_snapshot_id == "snapshot-authoritative", (
        "E14 snapshot transport invariant failed at WorkerExecutionSpec outer field: "
        f"expected='snapshot-authoritative', received={restored.prepared_snapshot_id!r}"
    )
    assert restored.execute_request is not None, (
        "E14 snapshot transport invariant failed: execute_request disappeared during wire round-trip"
    )
    assert restored.execute_request.get("prepared_snapshot_id") == "snapshot-authoritative", (
        "E14 snapshot transport invariant failed at WorkerExecutionSpec.execute_request: "
        "the authoritative snapshot was not injected before serialization; "
        f"execute_request={restored.execute_request!r}"
    )


def test_typed_worker_spec_reports_both_snapshot_identities_on_conflict() -> None:
    # Require an actionable error containing the expected and received identities and failing boundary.
    with pytest.raises(ValueError) as captured:
        _engine_spec(nested_snapshot_id="snapshot-stale")

    message = str(captured.value)
    assert "WorkerExecutionSpec.engine" in message, (
        "Snapshot conflict diagnostic omitted the failing protocol boundary: "
        f"message={message!r}"
    )
    assert "snapshot-authoritative" in message and "snapshot-stale" in message, (
        "Snapshot conflict diagnostic must include expected and received identities: "
        f"message={message!r}"
    )


def test_worker_host_injects_assignment_snapshot_before_protocol_serialization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Verify the host repairs a legacy nested request before it crosses into the persistent worker process.
    assignment = _assignment()
    worker = PersistentWorkerProcess(
        worker_id="worker-snapshot-transport",
        instance_id="instance-snapshot-transport",
        spool_root=tmp_path / "spool",
    )
    captured: dict[str, Any] = {}

    monkeypatch.setattr(worker, "_acquire_request_id", lambda: "acquire-request-id")

    def capture_payload(message_type: Any, payload: Any, **kwargs: Any) -> Any:
        # Capture the typed assignment without requiring a real subprocess for this boundary unit test.
        captured["message_type"] = message_type
        captured["payload"] = payload
        captured["kwargs"] = kwargs
        return SimpleNamespace(message_id="message-id")

    monkeypatch.setattr(worker, "_send_payload", capture_payload)

    correlation_id = worker.send_engine_assignment(
        assignment,
        configuration={"campaign_id": assignment.campaign_id},
        execute_request={
            "campaign_id": assignment.campaign_id,
            "shard": {"shard_id": assignment.shard_id, "mutant_ids": ["m1"]},
            "attempt": assignment.attempt,
            "worker_id": worker.worker_id,
            "lease_id": assignment.lease_id,
            "test_overrides": {},
        },
        workspace=tmp_path,
        report_root=tmp_path / "reports",
        expected_source_sha256="source-sha256",
        expected_mutant_ids=("m1",),
        test_fingerprints={"m1": {}},
        command_timeouts={"prepare": 5.0, "execute-shard": 5.0, "shutdown": 2.0},
    )

    payload = captured.get("payload")
    assert isinstance(payload, AssignShard), (
        "Worker host did not publish a typed AssignShard payload: "
        f"captured_payload_type={type(payload).__name__}, correlation_id={correlation_id!r}"
    )
    nested_request = payload.execution.execute_request
    assert nested_request is not None, (
        "Worker host dropped execute_request before protocol serialization: "
        f"campaign_id={assignment.campaign_id!r}, shard_id={assignment.shard_id!r}"
    )
    assert nested_request.get("prepared_snapshot_id") == assignment.prepared_snapshot_id, (
        "E14 host-to-worker snapshot invariant failed: "
        f"expected={assignment.prepared_snapshot_id!r}, "
        f"received={nested_request.get('prepared_snapshot_id')!r}, "
        f"campaign_id={assignment.campaign_id!r}, shard_id={assignment.shard_id!r}, "
        f"lease_id={assignment.lease_id!r}, attempt={assignment.attempt}"
    )


def test_worker_host_rejects_conflicting_nested_snapshot_with_assignment_context(
    tmp_path: Path,
) -> None:
    # Ensure a stale nested request fails before any protocol frame is emitted and names its ownership context.
    assignment = _assignment()
    worker = PersistentWorkerProcess(
        worker_id="worker-snapshot-transport",
        instance_id="instance-snapshot-transport",
        spool_root=tmp_path / "spool",
    )

    with pytest.raises(WorkerProcessError) as captured:
        worker.send_engine_assignment(
            assignment,
            configuration={"campaign_id": assignment.campaign_id},
            execute_request={
                "campaign_id": assignment.campaign_id,
                "shard": {"shard_id": assignment.shard_id, "mutant_ids": ["m1"]},
                "prepared_snapshot_id": "snapshot-stale",
            },
            workspace=tmp_path,
            report_root=tmp_path / "reports",
            expected_source_sha256="source-sha256",
            expected_mutant_ids=("m1",),
            test_fingerprints={"m1": {}},
            command_timeouts={"prepare": 5.0, "execute-shard": 5.0, "shutdown": 2.0},
        )

    message = str(captured.value)
    for expected_fragment in (
        "snapshot-authoritative",
        "snapshot-stale",
        assignment.campaign_id,
        assignment.shard_id,
        assignment.lease_id,
        "attempt=2",
    ):
        assert expected_fragment in message, (
            "Worker host snapshot conflict diagnostic is incomplete: "
            f"missing_fragment={expected_fragment!r}, message={message!r}"
        )
