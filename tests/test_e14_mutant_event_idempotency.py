from __future__ import annotations

import os
from pathlib import Path

from theseus_local.worker_runtime import DurableExecutionSpool


def _publish(spool: DurableExecutionSpool) -> str:
    # Publish one fixed execution identity to exercise duplicate replay semantics.
    return spool.publish_mutant_result(
        assignment={
            "campaign_id": "campaign-idempotent",
            "shard_id": "shard-idempotent",
            "lease_id": "lease-idempotent",
            "attempt": 0,
        },
        worker={
            "worker_id": "worker-idempotent",
            "instance_id": "instance-idempotent",
            "process_id": os.getpid(),
            "process_birth_token": "birth-idempotent",
        },
        source_sha256="source-sha",
        mutant_result={
            "execution_id": "execution-idempotent",
            "mutant_id": "mutant-idempotent",
            "status": "survived",
            "classification_reason": "passed",
            "restore_verified": True,
            "level_results": [],
            "artifact_paths": [],
            "lease_id": "lease-idempotent",
            "attempt": 0,
            "test_observations": [],
        },
    )


def test_identical_mutant_event_is_written_once_and_reopens_cleanly(tmp_path: Path) -> None:
    # Keep retries idempotent without duplicating immutable evidence bytes.
    spool = DurableExecutionSpool(tmp_path / "spool")
    first = _publish(spool)
    second = _publish(spool)
    assert first == second
    assert len(spool.mutant_events_path.read_text(encoding="utf-8").splitlines()) == 1
    assert tuple(item["event_id"] for item in DurableExecutionSpool(spool.root).mutant_events()) == (first,)
