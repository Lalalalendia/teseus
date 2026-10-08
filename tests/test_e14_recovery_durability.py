import json
import sys
from pathlib import Path

import pytest

from test_intelligence_unified_v1.io_utils import append_json_line, atomic_write_json
from test_intelligence_unified_v1.recovery import (
    JournalReplayError,
    inspect_campaign_recovery,
    journal_metadata,
    replay_campaign,
    replay_result_journal,
    verify_checkpoint,
)
from test_intelligence_unified_v1.runner import MutationConfig, MutationRunner
from test_intelligence_unified_v1.workers import resume_campaign


def _result(mutant_id: str, status: str = "killed") -> dict[str, object]:
    # Build the smallest result-shaped event accepted by the campaign accumulator.
    return {
        "status": status,
        "mutant": {"mutant_id": mutant_id, "mutation": "plus_to_minus"},
        "level_results": [],
        "selection": {"levels": []},
    }


def test_result_replay_ignores_incomplete_tail_and_deduplicates(tmp_path: Path) -> None:
    # Treat only the final partial write as recoverable corruption and keep one row per mutant.
    journal = tmp_path / "run.results.jsonl"
    row = _result("m1")
    append_json_line(journal, row)
    append_json_line(journal, row)
    with journal.open("ab") as handle:
        handle.write(b'{"status":"survived","mutant":{"mutant_id":"m2"')

    replay = replay_result_journal(journal)

    assert [item["mutant"]["mutant_id"] for item in replay.rows] == ["m1"]
    assert replay.duplicate_events == 1
    assert replay.ignored_tail is True
    assert replay.last_valid_offset < journal.stat().st_size


def test_checkpoint_verifies_prefix_after_append_and_rejects_tampering(tmp_path: Path) -> None:
    # A later append is allowed, while changing bytes before the checkpoint is a hard stop.
    journal = tmp_path / "run.results.jsonl"
    append_json_line(journal, _result("m1"))
    state = tmp_path / "run.state.json"
    atomic_write_json(
        state,
        {
            "checkpoint_schema_version": 1,
            "status": "running",
            "completed_mutants": 1,
            "results_journal": str(journal),
            **journal_metadata(journal),
        },
        durability="normal",
        category="state_checkpoint",
    )
    append_json_line(journal, _result("m2", "survived"))

    verified = verify_checkpoint(state)
    replay = replay_campaign(state)

    assert verified["verified"] is True
    assert replay.accumulator.total_results == 2
    original = journal.read_bytes()
    journal.write_bytes(b"X" + original[1:])
    with pytest.raises(JournalReplayError, match="SHA mismatch"):
        verify_checkpoint(state)


def _write_serial_project(root: Path) -> str:
    # Create two deterministic mutations so resume can prove that one completed row is retained.
    source = "def calculate(value):\n    return value + 1 + 2\n"
    (root / "app.py").write_text(source, encoding="utf-8")
    return source


def test_serial_resume_replays_completed_rows_and_restores_source(tmp_path: Path) -> None:
    # Recover a running serial campaign from one durable event and run only its missing mutant.
    source = _write_serial_project(tmp_path)
    reports = tmp_path / "reports"
    report = MutationRunner(
        MutationConfig(
            project_root=tmp_path,
            source="app.py",
            function="calculate",
            test_command_argv=(sys.executable, "-B", "-c", "from app import calculate; assert calculate(1) == 4"),
            operators=("plus_to_minus",),
            max_mutants=2,
            no_escalation=True,
            use_baseline_cache=False,
            reports_dir=reports,
        )
    ).run()
    assert report["status"] == "complete"
    report_path = Path(report["report_path"])
    manifest_path = Path(report["recovery_manifest"])
    journal_path = Path(report["results_journal"])
    original_rows = list(replay_result_journal(journal_path).rows)
    assert len(original_rows) == 2
    journal_path.write_text("", encoding="utf-8")
    append_json_line(journal_path, original_rows[0])
    state_path = Path(report["state_path"])
    atomic_write_json(
        state_path,
        {
            "checkpoint_schema_version": 1,
            "status": "running",
            "completed_mutants": 1,
            "total_mutants": 2,
            "results_journal": str(journal_path),
            **journal_metadata(journal_path),
        },
        durability="normal",
        category="state_checkpoint",
    )
    persisted = json.loads(report_path.read_text(encoding="utf-8"))
    persisted["status"] = "running"
    persisted["results"] = []
    report_path.write_text(json.dumps(persisted, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.update({"status": "active", "phase": "running", "coordinator_pid": 999999})
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    resumed = resume_campaign(manifest_path, force=True)

    assert resumed["status"] == "complete"
    assert len(resumed["results"]) == 2
    assert source == (tmp_path / "app.py").read_text(encoding="utf-8")
    assert json.loads(manifest_path.read_text(encoding="utf-8"))["status"] == "resumed"


def test_recovery_inspection_reports_orphan_and_unfinished_shard(tmp_path: Path) -> None:
    # Expose active orphaned work without changing its manifest or deleting its workspace.
    reports = tmp_path / "reports"
    reports.mkdir()
    manifest = reports / "campaign.workers.manifest.json"
    atomic_write_json(
        manifest,
        {
            "status": "active",
            "run_id": "run-1",
            "coordinator_pid": 999999,
            "workers": [{"worker_id": "w1", "status": "leased"}],
        },
        durability="critical",
        category="manifest",
    )

    diagnostics = inspect_campaign_recovery(reports)

    assert len(diagnostics["orphaned_campaigns"]) == 1
    assert diagnostics["unfinished_shards"] == [
        {"manifest": str(manifest), "worker_id": "w1", "status": "leased"}
    ]
