from __future__ import annotations

from theseus_contracts import (
    CampaignId,
    ExecuteShardRequest,
    ShardDescriptor,
    ShardId,
    WorkerExecutionMode,
    WorkerExecutionSpec,
    WorkerId,
)


def test_engine_execution_protocol_preserves_prepared_snapshot_identity() -> None:
    # Keep the outer assignment and nested engine request on one immutable prepared snapshot.
    execution = WorkerExecutionSpec.engine(
        configuration={"campaign_id": "campaign-snapshot"},
        execute_request={"campaign_id": "campaign-snapshot"},
        workspace="D:/state/workspace",
        report_root="D:/state/reports",
        publish_engine_root=None,
        expected_source_sha256="source-sha",
        prepared_snapshot_id="snapshot-123",
        expected_mutant_ids=("mutant-1",),
        test_fingerprints={},
        command_timeouts={"execute-shard": 30.0},
        command=None,
        cancel_path=None,
    )
    restored = WorkerExecutionSpec.from_dict(execution.to_dict())
    assert restored.mode == WorkerExecutionMode.ENGINE
    assert restored.prepared_snapshot_id == "snapshot-123"
    request = ExecuteShardRequest(
        CampaignId("campaign-snapshot"),
        ShardDescriptor(ShardId("shard-1"), ("mutant-1",)),
        worker_id=WorkerId("worker-1"),
        prepared_snapshot_id="snapshot-123",
    )
    assert request.to_dict()["prepared_snapshot_id"] == "snapshot-123"
