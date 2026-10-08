import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from test_intelligence_unified_v1 import runner as runner_module
from test_intelligence_unified_v1 import test_stats as stats_module
from test_intelligence_unified_v1.mutations import (
    RestoreError,
    apply_prepared_mutant,
    create_snapshot,
    generate_mutants,
    prepare_mutant,
    recover_manifest,
    write_manifest,
)
from test_intelligence_unified_v1.runner import MutationConfig, MutationRunner
from test_intelligence_unified_v1.test_stats import ingest_test_stats, merge_test_stats_databases, stats_db_path


def test_manifest_failure_does_not_mutate_source(tmp_path: Path, monkeypatch) -> None:
    # Fail at the recovery-manifest boundary and verify that source mutation never starts.
    source = tmp_path / "app.py"
    original = b"def choose(value):\n    return value + 1\n"
    source.write_bytes(original)
    reports = tmp_path / "reports"

    def fail_manifest(*args, **kwargs):
        # Simulate a full-disk or permission failure while arming recovery.
        raise OSError("injected manifest failure")

    monkeypatch.setattr(runner_module, "write_manifest", fail_manifest)
    report = MutationRunner(
        MutationConfig(
            project_root=tmp_path,
            source="app.py",
            function="choose",
            test_command_argv=("python", "-c", "from app import choose; assert choose(1) == 2"),
            reports_dir=reports,
            operators=("plus_to_minus",),
            max_mutants=1,
            no_escalation=True,
            use_baseline_cache=False,
        )
    ).run()

    assert report["status"] == "error"
    assert "injected manifest failure" in report["error"]
    assert source.read_bytes() == original
    assert report["results"] == []


def test_recovery_manifest_refuses_unknown_external_change(tmp_path: Path) -> None:
    # Preserve an external edit instead of overwriting it during recovery.
    target = tmp_path / "app.py"
    target.write_text("value = 1\n", encoding="utf-8")
    snapshot = create_snapshot(target, tmp_path / "recovery")
    manifest_path = tmp_path / "campaign.manifest.json"
    write_manifest(manifest_path, snapshot)
    external = b"value = 99\n"
    target.write_bytes(external)

    with pytest.raises(RestoreError, match="not listed in manifest"):
        recover_manifest(manifest_path)

    assert target.read_bytes() == external


def test_prepared_mutant_snapshot_mismatch_does_not_write(tmp_path: Path) -> None:
    # Reject bytes prepared for another snapshot before the atomic source write.
    target = tmp_path / "app.py"
    target.write_text("def calculate(value):\n    return value + 1\n", encoding="utf-8")
    snapshot = create_snapshot(target, tmp_path / "recovery")
    mutant = generate_mutants(snapshot.text, operators=("plus_to_minus",))[0]
    prepared = replace(prepare_mutant(snapshot, mutant), original_sha256="wrong-snapshot")

    with pytest.raises(RestoreError, match="another snapshot"):
        apply_prepared_mutant(snapshot, prepared)

    assert target.read_bytes() == snapshot.original_bytes


def test_worker_stats_merge_rolls_back_injected_failure(tmp_path: Path, monkeypatch) -> None:
    # Roll back schema and rows when a worker database merge fails mid-transaction.
    events = tmp_path / "events"
    events.mkdir()
    (events / "worker.jsonl").write_text(
        json.dumps(
            {
                "event_id": "event-1",
                "run_id": "worker-run",
                "phase": "baseline",
                "level": "L1",
                "nodeid": "tests/test_app.py::test_one",
                "outcome": "passed",
                "duration_ms": 1.0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    source_db = stats_db_path(tmp_path / "worker-reports")
    ingest_test_stats(events, source_db, project_root=tmp_path, run_id="worker-run", source_path="app.py")
    target_db = stats_db_path(tmp_path / "merged-reports")

    def fail_insert(connection, rows):
        # Inject a worker merge failure after the target transaction is open.
        raise RuntimeError("injected merge failure")

    monkeypatch.setattr(stats_module, "_insert_stats_batch", fail_insert)
    with pytest.raises(RuntimeError, match="injected merge failure"):
        merge_test_stats_databases([source_db], target_db, project_root=tmp_path)

    connection = sqlite3.connect(target_db)
    try:
        table_exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'test_attempts'"
        ).fetchone()
        count = (
            connection.execute("SELECT COUNT(*) FROM test_attempts").fetchone()[0]
            if table_exists
            else 0
        )
    finally:
        connection.close()
    assert count == 0
