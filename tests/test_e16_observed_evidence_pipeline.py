from __future__ import annotations

import json
from pathlib import Path

from test_intelligence_unified_v1.test_stats import (
    ingest_test_stats,
    load_mutant_test_observations,
    stats_db_path,
)


def _write_mutant_event(event_dir: Path, *, run_id: str) -> Path:
    # Write one real plugin-shaped mutant event into the exact engine attempt directory.
    event_dir.mkdir(parents=True, exist_ok=True)
    event = {
        "event_id": f"{run_id}:mutant:L1:m-observed:0:1:main:0:test_app.py::test_choose",
        "run_id": run_id,
        "phase": "mutant",
        "level": "L1",
        "mutant_id": "m-observed",
        "source_path": "app.py",
        "target_sha256": "mutated",
        "nodeid": "test_app.py::test_choose",
        "outcome": "failed",
        "duration_ms": 4.5,
        "first_failure": True,
        "worker_id": "main",
        "retry": False,
        "recorded_at": "2026-08-04T00:00:00+00:00",
        "runtime_dependencies": [
            {
                "path": "fixture.json",
                "relative_path": "fixture.json",
                "sha256": "fixture",
                "content_sha256": "fixture",
                "size_bytes": 2,
                "outside_workspace": False,
                "access_mode": "read",
                "owner": "test_app.py::test_choose",
            }
        ],
        "runtime_dependency_complete": True,
        "runtime_dependency_blockers": [],
        "environment_reads": [],
        "environment_dependency_blockers": [],
    }
    path = event_dir / f"{run_id}.attempt.main.1.jsonl"
    path.write_text(json.dumps(event) + "\n", encoding="utf-8")
    return path


def test_exact_engine_event_directory_recovers_observation_without_sqlite(tmp_path: Path) -> None:
    # Prove the current JSONL attempt remains authoritative when the aggregate database is absent.
    root = tmp_path / "project"
    root.mkdir()
    run_id = "engine-shard-observed"
    event_dir = tmp_path / "engine" / "campaign" / "test_stats_events" / run_id
    _write_mutant_event(event_dir, run_id=run_id)

    observations = load_mutant_test_observations(
        tmp_path / "missing.sqlite",
        run_id=run_id,
        mutant_ids=("m-observed",),
        root=root,
        event_dir=event_dir,
    )

    assert observations["m-observed"][0]["test_id"] == "test_app.py::test_choose"
    assert observations["m-observed"][0]["outcome"] == "failed"
    assert observations["m-observed"][0]["evidence_kind"] == "pytest_test_event"
    assert observations["m-observed"][0]["runtime_dependencies"][0]["path"] == "fixture.json"


def test_exact_engine_event_directory_deduplicates_ingested_sqlite_row(tmp_path: Path) -> None:
    # Prove one event read from JSONL and SQLite remains one observation rather than duplicated proof.
    root = tmp_path / "project"
    root.mkdir()
    run_id = "engine-shard-deduplicated"
    event_dir = tmp_path / "engine" / "campaign" / "test_stats_events" / run_id
    journal = _write_mutant_event(event_dir, run_id=run_id)
    database = stats_db_path(tmp_path / "reports")
    ingest_test_stats(
        event_dir,
        database,
        project_root=root,
        run_id=run_id,
        source_path="app.py",
        event_files=(journal,),
    )

    observations = load_mutant_test_observations(
        database,
        run_id=run_id,
        mutant_ids=("m-observed",),
        root=root,
        event_dir=event_dir,
    )

    assert len(observations["m-observed"]) == 1
    assert observations["m-observed"][0]["first_failure"] is True
