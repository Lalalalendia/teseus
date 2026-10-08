from __future__ import annotations

import io
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from theseus_contracts import ShardAssignment
from theseus_local.worker_runtime import DurableExecutionSpool, PersistentWorkerEntrypoint


def _assignment(attempt: int, lease_id: str) -> ShardAssignment:
    # Build one current lease generation for salvage carry-forward.
    expires_at = (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
    return ShardAssignment(
        campaign_id="campaign-salvage",
        shard_id="shard-salvage",
        lease_id=lease_id,
        attempt=attempt,
        mutant_ids=("mutant-1", "mutant-2"),
        prepared_snapshot_id="snapshot-salvage",
        workspace_descriptor_id=f"workspace-{attempt}",
        expires_at=expires_at,
    )


def _result(mutant_id: str, attempt: int, lease_id: str) -> dict[str, object]:
    # Build one restored result from an interrupted shard attempt.
    return {
        "execution_id": f"execution-{mutant_id}-attempt-{attempt}",
        "mutant_id": mutant_id,
        "status": "killed",
        "classification_reason": "assertion",
        "restore_verified": True,
        "level_results": [],
        "artifact_paths": [],
        "lease_id": lease_id,
        "attempt": attempt,
        "test_observations": [],
    }


def test_reassignment_carries_forward_completed_mutant_and_fences_old_event(tmp_path: Path) -> None:
    # Rebind prior restored evidence to attempt+1 so only missing mutants need fresh execution.
    spool_root = tmp_path / "spool"
    spool = DurableExecutionSpool(spool_root)
    source_event_id = spool.publish_mutant_result(
        assignment=_assignment(0, "lease-0").to_dict(),
        worker={
            "worker_id": "worker-salvage",
            "instance_id": "instance-0",
            "process_id": os.getpid(),
            "process_birth_token": "birth-0",
        },
        source_sha256="source-sha",
        mutant_result=_result("mutant-1", 0, "lease-0"),
    )
    runtime = PersistentWorkerEntrypoint(
        worker_id="worker-salvage",
        instance_id="instance-1",
        spool_root=spool_root,
        input_stream=io.StringIO(),
        output_stream=io.StringIO(),
    )
    current = _assignment(1, "lease-1")
    carried = runtime._carry_forward_mutant_results(
        current,
        source_sha256="source-sha",
        mutant_ids=current.mutant_ids,
    )
    assert tuple(carried) == ("mutant-1",)
    assert carried["mutant-1"].attempt == 1
    assert carried["mutant-1"].lease_id == "lease-1"
    assert carried["mutant-1"].execution_id.value != "execution-mutant-1-attempt-0"
    assert spool.mutant_quarantined(source_event_id)
    current_events = [
        item for item in spool.mutant_events(include_quarantined=False)
        if item.get("attempt") == 1
    ]
    assert len(current_events) == 1
    assert current_events[0]["source_event_id"] == source_event_id
