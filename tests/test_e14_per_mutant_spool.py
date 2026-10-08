from __future__ import annotations

import os
from pathlib import Path

import pytest

from theseus_local.worker_runtime import DurableExecutionSpool, SpoolError


def _assignment(*, attempt: int = 0, lease_id: str = "lease-0") -> dict[str, object]:
    # Build one immutable assignment identity used by per-mutant spool tests.
    return {
        "campaign_id": "campaign-spool",
        "shard_id": "shard-spool",
        "lease_id": lease_id,
        "attempt": attempt,
    }


def _worker(*, instance_id: str = "instance-0") -> dict[str, object]:
    # Build one process-fenced worker identity without starting a subprocess.
    return {
        "worker_id": "worker-spool",
        "instance_id": instance_id,
        "process_id": os.getpid(),
        "process_birth_token": f"birth-{instance_id}",
    }


def _result(mutant_id: str = "mutant-1", *, status: str = "killed") -> dict[str, object]:
    # Build one restored public mutant result that is safe to commit durably.
    return {
        "execution_id": f"execution-{mutant_id}",
        "mutant_id": mutant_id,
        "status": status,
        "classification_reason": "assertion",
        "restore_verified": True,
        "level_results": [],
        "artifact_paths": [],
        "lease_id": "lease-0",
        "attempt": 0,
        "test_observations": [],
    }


def test_mutant_event_is_fsynced_before_any_shard_delivery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Require the engine-side primitive to fsync evidence while the shard journal is still empty.
    calls: list[int] = []
    monkeypatch.setattr("test_intelligence_unified_v1.io_utils.os.fsync", lambda fd: calls.append(fd))
    spool = DurableExecutionSpool(tmp_path / "spool")
    event_id = spool.publish_mutant_result(
        assignment=_assignment(),
        worker=_worker(),
        source_sha256="source-sha",
        mutant_result=_result(),
    )
    assert event_id
    assert calls
    assert spool.pending() == ()
    assert len(spool.mutant_events()) == 1


def test_unrestored_mutant_is_never_committed(tmp_path: Path) -> None:
    # Refuse evidence until production source restoration has been verified.
    spool = DurableExecutionSpool(tmp_path / "spool")
    result = {**_result(), "restore_verified": False}
    with pytest.raises(SpoolError, match="un-restored"):
        spool.publish_mutant_result(
            assignment=_assignment(),
            worker=_worker(),
            source_sha256="source-sha",
            mutant_result=result,
        )
    assert spool.mutant_events() == ()
