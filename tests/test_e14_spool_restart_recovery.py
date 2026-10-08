from __future__ import annotations

import os
from pathlib import Path

from theseus_local.worker_runtime import DurableExecutionSpool


def test_restart_replays_committed_and_delivery_state_with_torn_tail(tmp_path: Path) -> None:
    # Recover complete records and ignore only an interrupted final JSONL append.
    root = tmp_path / "spool"
    spool = DurableExecutionSpool(root)
    event_id = spool.publish_mutant_result(
        assignment={
            "campaign_id": "campaign-restart",
            "shard_id": "shard-restart",
            "lease_id": "lease-restart",
            "attempt": 0,
        },
        worker={
            "worker_id": "worker-restart",
            "instance_id": "instance-restart",
            "process_id": os.getpid(),
            "process_birth_token": "birth-restart",
        },
        source_sha256="source-sha",
        mutant_result={
            "execution_id": "execution-restart",
            "mutant_id": "mutant-restart",
            "status": "killed",
            "classification_reason": "assertion",
            "restore_verified": True,
            "level_results": [],
            "artifact_paths": [],
            "lease_id": "lease-restart",
            "attempt": 0,
            "test_observations": [],
        },
    )
    spool.include_mutant_in_delivery(event_id, "delivery-restart")
    with spool.mutant_events_path.open("ab") as handle:
        handle.write(b'{"kind":"mutant_event","event_id":"torn"')
    reopened = DurableExecutionSpool(root)
    assert reopened.mutant_event(event_id)["mutant_id"] == "mutant-restart"
    assert tuple(item["event_id"] for item in reopened.mutant_events(parent_event_id="delivery-restart")) == (event_id,)
    second_event_id = reopened.publish_mutant_result(
        assignment={
            "campaign_id": "campaign-restart",
            "shard_id": "shard-restart",
            "lease_id": "lease-restart",
            "attempt": 0,
        },
        worker={
            "worker_id": "worker-restart",
            "instance_id": "instance-restart",
            "process_id": os.getpid(),
            "process_birth_token": "birth-restart",
        },
        source_sha256="source-sha",
        mutant_result={
            "execution_id": "execution-restart-2",
            "mutant_id": "mutant-restart-2",
            "status": "survived",
            "classification_reason": "passed",
            "restore_verified": True,
            "level_results": [],
            "artifact_paths": [],
            "lease_id": "lease-restart",
            "attempt": 0,
            "test_observations": [],
        },
    )
    assert second_event_id != event_id
    assert len(DurableExecutionSpool(root).mutant_events()) == 2
