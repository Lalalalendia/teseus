from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path

import pytest

from test_intelligence_unified_v1.io_utils import append_json_line, atomic_write_json
from test_intelligence_unified_v1.recovery import (
    JournalReplayError,
    accumulator_from_results,
    current_process_birth_token,
    merge_authoritative_results,
    owner_is_alive,
    replay_campaign,
    replay_result_journal,
)
from test_intelligence_unified_v1.workers import ResumeError, resume_campaign
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    ProjectDescriptor,
    ProjectId,
    MutationScope,
    TestCommandDescriptor as CommandDescriptor,
)
from theseus_local import LocalCampaignCoordinator
from theseus_local.workspace import WorkspaceProvider
from theseus_knowledge import KnowledgePlaneStore


def _event(mutant_id: str, *, event_id: str, execution_id: str, attempt: int, status: str) -> dict[str, object]:
    # Build a strict v2 completion event with independent execution identity fields.
    return {
        "event_type": "mutant_completed",
        "event_schema_version": 2,
        "event_id": event_id,
        "execution_id": execution_id,
        "attempt": attempt,
        "lease_id": f"lease-{attempt}",
        "worker_id": f"worker-{attempt}",
        "status": status,
        "mutant": {"mutant_id": mutant_id, "mutation": "plus_to_minus", "operator_version": "m2"},
        "level_results": [],
        "selection": {"levels": []},
    }


def test_v2_replay_rejects_conflicting_event_identity(tmp_path: Path) -> None:
    # Never hide a changed payload behind event or execution deduplication.
    journal = tmp_path / "results.jsonl"
    append_json_line(journal, _event("m1", event_id="event-1", execution_id="exec-1", attempt=0, status="error"))
    append_json_line(journal, _event("m1", event_id="event-1", execution_id="exec-1", attempt=0, status="killed"))
    with pytest.raises(JournalReplayError, match="conflicting journal record identity"):
        replay_result_journal(journal)


def test_retry_attempts_are_kept_and_authoritative_policy_uses_latest_attempt(tmp_path: Path) -> None:
    # Preserve both attempts in replay while choosing the newest non-infrastructure result for fan-in.
    journal = tmp_path / "results.jsonl"
    first = _event("m1", event_id="event-1", execution_id="exec-1", attempt=0, status="error")
    second = _event("m1", event_id="event-2", execution_id="exec-2", attempt=1, status="killed")
    append_json_line(journal, first)
    append_json_line(journal, second)
    replay = replay_result_journal(journal)
    assert len(replay.rows) == 2
    selected = merge_authoritative_results(replay.rows)
    assert len(selected) == 1
    assert selected[0]["attempt"] == 1
    assert selected[0]["status"] == "killed"


def test_checkpoint_v2_replays_only_suffix_into_accumulator(tmp_path: Path) -> None:
    # Reconstruct counters from the checkpoint accumulator and consume only the appended suffix.
    journal = tmp_path / "results.jsonl"
    first = _event("m1", event_id="event-1", execution_id="exec-1", attempt=0, status="killed")
    second = _event("m2", event_id="event-2", execution_id="exec-2", attempt=0, status="survived")
    append_json_line(journal, first)
    prefix = journal.read_bytes()
    accumulator = replay_result_journal(journal).rows
    checkpoint_accumulator = accumulator_from_results(accumulator)
    append_json_line(journal, second)
    state = tmp_path / "run.state.json"
    atomic_write_json(
        state,
        {
            "checkpoint_schema_version": 2,
            "status": "running",
            "results_journal": str(journal),
            "journal_offset": len(prefix),
            "journal_size": len(prefix),
            "journal_sha256": hashlib.sha256(prefix).hexdigest(),
            "accumulator": checkpoint_accumulator.checkpoint_dict(),
        },
    )
    replay = replay_campaign(state)
    assert replay.accumulator.total_results == 2
    assert replay.accumulator.counts["killed"] == 1
    assert replay.accumulator.counts["survived"] == 1


def test_resume_rejects_a_live_owner_even_with_force(tmp_path: Path) -> None:
    # Force must never turn a live-owner race into two writers on one campaign.
    birth_token = current_process_birth_token()
    report = tmp_path / "run.json"
    atomic_write_json(report, {"results": [], "mutants": []})
    manifest = tmp_path / "run.manifest.json"
    atomic_write_json(
        manifest,
        {
            "status": "active",
            "report_path": str(report),
            "coordinator_pid": os.getpid(),
            "process_birth_token": birth_token,
        },
    )
    with pytest.raises(ResumeError, match="owner is still alive"):
        resume_campaign(manifest, force=True)


def test_resume_rejects_pid_reuse_with_a_different_birth_token() -> None:
    # A recycled PID must not be accepted as the original campaign owner.
    assert owner_is_alive(
        {
            "coordinator_pid": os.getpid(),
            "process_birth_token": "different-process-start-token",
        }
    ) is False


def test_local_stages_persist_authoritative_artifacts_and_prepared_context(tmp_path: Path) -> None:
    # Verify collect, index, baseline and shard execution share one durable prepared context.
    (tmp_path / "app.py").write_text(
        "def choose(value):\n    if value > 0:\n        return 1\n    return 0\n",
        encoding="utf-8",
    )
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_app.py").write_text(
        "from app import choose\n\ndef test_choose():\n    assert choose(1) == 1\n",
        encoding="utf-8",
    )
    configuration = CampaignConfiguration(
        campaign_id=CampaignId("cmp_e14_stages"),
        project=ProjectDescriptor(
            project_id=ProjectId("project_e14_stages"),
            display_name="E14 stages",
            root_path=str(tmp_path),
            test_command=CommandDescriptor((sys.executable, "-m", "pytest", "tests", "-q")),
        ),
        scope=MutationScope(source_path="app.py", function="choose", operators=("condition_to_not",)),
        budget=CampaignBudget(max_mutants=1, max_workers=1),
        no_escalation=True,
        reports_dir=str(tmp_path / "reports"),
    )
    result = LocalCampaignCoordinator().run(configuration)
    assert result.succeeded
    reports_root = WorkspaceProvider(configuration).reports_root
    artifact_dir = reports_root / "engine" / "cmp_e14_stages"
    for name in (
        "prepared.snapshot.json",
        "collection.snapshot.json",
        "selection.snapshot.json",
        "index.snapshot.json",
        "index.payload.json",
        "baseline.json",
    ):
        assert (artifact_dir / name).is_file(), name
    prepared = json.loads((artifact_dir / "prepared.snapshot.json").read_text(encoding="utf-8"))
    collection = json.loads((artifact_dir / "collection.snapshot.json").read_text(encoding="utf-8"))
    assert prepared["internal_mutants"]
    assert any(str(item).endswith("tests/test_app.py::test_choose") for item in collection["nodeids"])
    knowledge = KnowledgePlaneStore(reports_root / "cmp_e14_stages" / "knowledge.sqlite3")
    assert knowledge.summarize_campaign("cmp_e14_stages").executions == 1
    knowledge.close()
