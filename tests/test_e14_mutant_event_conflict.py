from __future__ import annotations

import os
from pathlib import Path

import pytest

from theseus_local.worker_runtime import DurableExecutionSpool, SpoolError


def _result(status: str) -> dict[str, object]:
    # Build conflicting semantic payloads under one immutable execution identity.
    return {
        "execution_id": "execution-conflict",
        "mutant_id": "mutant-conflict",
        "status": status,
        "classification_reason": status,
        "restore_verified": True,
        "level_results": [],
        "artifact_paths": [],
        "lease_id": "lease-conflict",
        "attempt": 0,
        "test_observations": [],
    }


def test_same_execution_identity_with_different_payload_stops_recovery(tmp_path: Path) -> None:
    # Treat a contradictory duplicate as corruption rather than choosing one result silently.
    spool = DurableExecutionSpool(tmp_path / "spool")
    arguments = {
        "assignment": {
            "campaign_id": "campaign-conflict",
            "shard_id": "shard-conflict",
            "lease_id": "lease-conflict",
            "attempt": 0,
        },
        "worker": {
            "worker_id": "worker-conflict",
            "instance_id": "instance-conflict",
            "process_id": os.getpid(),
            "process_birth_token": "birth-conflict",
        },
        "source_sha256": "source-sha",
    }
    spool.publish_mutant_result(**arguments, mutant_result=_result("killed"))
    with pytest.raises(SpoolError, match="conflict"):
        spool.publish_mutant_result(**arguments, mutant_result=_result("survived"))
    assert '"state":"conflicted"' in spool.mutant_states_path.read_text(encoding="utf-8")
