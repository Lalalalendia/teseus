from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

from test_intelligence_unified_v1.io_utils import append_json_line, atomic_write_json
from test_intelligence_unified_v1.models import CampaignAccumulator
from test_intelligence_unified_v1.mutations import RestoreError, create_snapshot, recover_manifest, write_manifest
from test_intelligence_unified_v1.recovery import (
    JournalReplayError,
    current_process_birth_token,
    journal_metadata,
    replay_campaign,
    result_event_id,
    result_identity_digests,
)
from test_intelligence_unified_v1.runner import MutationConfig, MutationRunner
from test_intelligence_unified_v1.workers import ResumeError, resume_serial_campaign


def _event(mutant_id: str, status: str) -> dict[str, object]:
    # Build one complete v2 completion event with immutable execution ownership.
    row: dict[str, object] = {
        "event_type": "mutant_completed",
        "event_schema_version": 2,
        "execution_id": f"execution-{mutant_id}",
        "attempt": 0,
        "lease_id": "lease-1",
        "worker_id": "worker-1",
        "status": status,
        "mutant": {"mutant_id": mutant_id, "mutation": "plus_to_minus"},
    }
    row["event_id"] = result_event_id("campaign-1", row)
    return row


def test_event_identity_does_not_change_with_result_payload() -> None:
    # Keep retries and corruption detection keyed to execution ownership rather than outcome text.
    killed = _event("m1", "killed")
    survived = _event("m1", "survived")

    assert killed["event_id"] == survived["event_id"]


def test_suffix_replay_rejects_conflict_with_authenticated_checkpoint_prefix(tmp_path: Path) -> None:
    # Authenticate checkpoint identities before accepting a suffix record with a reused execution ID.
    journal = tmp_path / "campaign.results.jsonl"
    first = _event("m1", "killed")
    append_json_line(journal, first)
    metadata = journal_metadata(journal)
    accumulator = CampaignAccumulator()
    accumulator.add_result(first)
    state = tmp_path / "campaign.state.json"
    atomic_write_json(
        state,
        {
            "checkpoint_schema_version": 2,
            "status": "running",
            "completed_mutants": 1,
            "total_mutants": 1,
            "results_journal": str(journal),
            "journal_offset": metadata["journal_size"],
            "completed_execution_ids": ["execution-m1"],
            "completed_mutant_ids": ["m1"],
            "completed_identity_digests": result_identity_digests((first,)),
            "accumulator": accumulator.checkpoint_dict(),
            **metadata,
        },
        durability="normal",
        category="state_checkpoint",
    )
    append_json_line(journal, _event("m1", "survived"))

    with pytest.raises(JournalReplayError, match="conflicting journal record identity"):
        replay_campaign(state)


def test_recovery_refuses_live_owner_even_when_force_is_requested(tmp_path: Path) -> None:
    # Prevent a forced takeover from racing the process that still owns the target lock.
    target = tmp_path / "app.py"
    target.write_text("value = 1\n", encoding="utf-8")
    snapshot = create_snapshot(target, tmp_path / "recovery")
    manifest_path = tmp_path / "campaign.manifest.json"
    write_manifest(
        manifest_path,
        snapshot,
        extra={
            "run_id": "live-campaign",
            "coordinator_pid": os.getpid(),
            "process_birth_token": current_process_birth_token(),
        },
    )

    with pytest.raises(RestoreError, match="owner is still alive"):
        recover_manifest(manifest_path, force=True)


def test_serial_resume_rejects_repository_fingerprint_change(tmp_path: Path) -> None:
    # Start a new campaign instead of mixing results after an unrelated repository file changes.
    source = tmp_path / "app.py"
    source.write_text("def choose(value):\n    return value + 1\n", encoding="utf-8")
    reports = tmp_path / "reports"
    report = MutationRunner(
        MutationConfig(
            project_root=tmp_path,
            source="app.py",
            function="choose",
            test_command_argv=(sys.executable, "-B", "-c", "from app import choose; assert choose(1) == 2"),
            operators=("plus_to_minus",),
            max_mutants=1,
            no_escalation=True,
            use_baseline_cache=False,
            reports_dir=reports,
        )
    ).run()
    assert report["status"] == "complete"
    (tmp_path / "unrelated.py").write_text("changed = True\n", encoding="utf-8")
    manifest_path = Path(report["recovery_manifest"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.update({"status": "active", "coordinator_pid": 999999})
    atomic_write_json(manifest_path, manifest, durability="normal", category="manifest")

    with pytest.raises(ResumeError, match="input fingerprint mismatch"):
        resume_serial_campaign(manifest_path, force=True)
