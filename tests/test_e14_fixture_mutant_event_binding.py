from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from theseus_contracts import ShardAssignment, WorkerIdentity
from theseus_local.worker_runtime import DurableExecutionSpool, WorkerAgent


def test_fixture_nested_shard_result_creates_mutant_event_without_legacy_restore_field(
    tmp_path: Path,
) -> None:
    # Preserve fixture compatibility while engine rows remain precommitted and restore-verified.
    assignment = ShardAssignment(
        campaign_id="campaign-fixture",
        shard_id="shard-fixture",
        lease_id="lease-fixture",
        attempt=0,
        mutant_ids=("mutant-1",),
        prepared_snapshot_id="snapshot-fixture",
        workspace_descriptor_id="workspace-fixture",
        expires_at=(datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat(),
    )
    agent = WorkerAgent(
        identity=WorkerIdentity("worker-fixture", "instance-fixture", os.getpid(), "birth-fixture"),
        capabilities=WorkerAgent.local_capabilities(),
        spool=DurableExecutionSpool(tmp_path / "spool"),
        heartbeat_interval_seconds=0.05,
    )
    delivery = agent.run_assignment(
        assignment,
        lambda: {
            "shard_result": {
                "status": "complete",
                "completed_mutants": 1,
                "results": [
                    {
                        "execution_id": "execution-1",
                        "mutant_id": "mutant-1",
                        "status": "killed",
                    }
                ],
            }
        },
    )
    assert len(delivery.mutant_event_ids) == 1
    assert len(agent.spool.mutant_events(parent_event_id=delivery.event_id)) == 1
    agent.acknowledge(delivery.event_id)
