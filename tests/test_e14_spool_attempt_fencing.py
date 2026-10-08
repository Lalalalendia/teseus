from __future__ import annotations

import os
from pathlib import Path

import pytest

from theseus_local.worker_runtime import DurableExecutionSpool, SpoolError


def test_old_attempt_event_cannot_bind_directly_to_new_lease_delivery(tmp_path: Path) -> None:
    # Require an explicit carry-forward event before prior-attempt evidence enters current fan-in.
    spool = DurableExecutionSpool(tmp_path / "spool")
    result = {
        "execution_id": "execution-attempt-0",
        "mutant_id": "mutant-attempt",
        "status": "killed",
        "classification_reason": "assertion",
        "restore_verified": True,
        "level_results": [],
        "artifact_paths": [],
        "lease_id": "lease-0",
        "attempt": 0,
        "test_observations": [],
    }
    spool.publish_mutant_result(
        assignment={
            "campaign_id": "campaign-attempt",
            "shard_id": "shard-attempt",
            "lease_id": "lease-0",
            "attempt": 0,
        },
        worker={
            "worker_id": "worker-attempt",
            "instance_id": "instance-0",
            "process_id": os.getpid(),
            "process_birth_token": "birth-0",
        },
        source_sha256="source-sha",
        mutant_result=result,
    )
    current_payload = {
        "assignment": {
            "campaign_id": "campaign-attempt",
            "shard_id": "shard-attempt",
            "lease_id": "lease-1",
            "attempt": 1,
        },
        "worker": {
            "worker_id": "worker-attempt",
            "instance_id": "instance-1",
            "process_id": os.getpid(),
            "process_birth_token": "birth-1",
        },
        "result": {
            "source_sha256": "source-sha",
            "shard_result": {
                "shard_id": "shard-attempt",
                "worker_id": "worker-attempt",
                "status": "complete",
                "completed_mutants": 1,
                "results": [{**result, "lease_id": "lease-1", "attempt": 1}],
            },
        },
    }
    with pytest.raises(SpoolError, match="precommitted"):
        spool.bind_mutant_events(
            "delivery-attempt-1",
            current_payload,
            allow_publish_missing=False,
        )
