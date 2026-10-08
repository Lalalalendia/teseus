import json
from pathlib import Path
from types import SimpleNamespace

from test_intelligence_unified_v1 import maintenance


def test_doctor_reuses_one_materialized_stats_summary(tmp_path: Path, monkeypatch) -> None:
    # Build health counters from the same rows that doctor already loaded for known tests.
    project_root = tmp_path / "project"
    reports_dir = tmp_path / "reports"
    project_root.mkdir()
    reports_dir.mkdir()
    stats_path = maintenance.stats_db_path(reports_dir)
    stats_path.write_bytes(b"sqlite-placeholder")
    rows = [
        {
            "nodeid": "tests/test_app.py::test_one",
            "health_status": "healthy",
            "baseline_failures": 0,
            "regression_failures": 0,
        }
    ]
    stats_calls: list[Path] = []
    health_calls: list[object] = []

    def fake_stats(*args, **kwargs):
        # Record the only database summary request issued by doctor.
        stats_calls.append(args[0])
        return rows

    def fake_health(value):
        # Confirm health is derived from the already materialized rows.
        health_calls.append(value)
        return {"tests": 1, "healthy": 1, "flaky": 0, "failing": 0, "insufficient_data": 0, "baseline_failures": 0, "regression_failures": 0}

    def fake_run(*args, **kwargs):
        # Replace subprocess probes with a deterministic successful diagnostic result.
        return SimpleNamespace(passed=True, output_tail="pytest", output="pytest")

    monkeypatch.setattr(maintenance, "summarize_test_stats", fake_stats)
    monkeypatch.setattr(maintenance, "summarize_test_health_rows", fake_health)
    monkeypatch.setattr(maintenance, "run_argv", fake_run)
    monkeypatch.setattr(maintenance, "_worker_campaign_diagnostics", lambda _reports: {})

    result = maintenance.doctor(project_root, reports_dir, deep=True)

    assert stats_calls == [stats_path]
    assert health_calls == [rows]
    assert result["test_stats"]["known_tests"] == 1
    assert result["test_stats"]["health"]["healthy"] == 1


def test_storage_manifest_accounts_categories_in_one_tree_scan(tmp_path: Path) -> None:
    # Keep storage accounting bounded to file metadata while separating report categories.
    reports_dir = tmp_path / "reports"
    files = {
        reports_dir / "run.json": b"report",
        reports_dir / "artifacts" / "run.txt": b"artifact",
        reports_dir / "recovery" / "run" / "original.bin": b"recovery",
        reports_dir / "test_stats_events" / "run" / "events.jsonl": b"stats",
        reports_dir / "workers" / "run" / "worker-000" / "project" / "app.py": b"worker",
    }
    for path, payload in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)

    manifest = maintenance.build_storage_manifest(reports_dir)

    assert manifest["files"] == len(files)
    assert manifest["bytes"] == sum(len(payload) for payload in files.values())
    assert manifest["categories"]["reports"] == {"files": 1, "bytes": 6}
    assert manifest["categories"]["artifacts"] == {"files": 1, "bytes": 8}
    assert manifest["categories"]["recovery"] == {"files": 1, "bytes": 8}
    assert manifest["categories"]["stats"] == {"files": 1, "bytes": 5}
    assert manifest["categories"]["worker_workspaces"] == {"files": 1, "bytes": 6}


def test_cached_storage_manifest_avoids_repeated_scan(tmp_path: Path, monkeypatch) -> None:
    # Reuse a valid accounting manifest until doctor explicitly requests a deep refresh.
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    fresh = maintenance.build_storage_manifest(reports_dir)
    maintenance.storage_manifest_path(reports_dir).write_text(
        json.dumps(fresh),
        encoding="utf-8",
    )

    def fail_scan(_reports_dir):
        # Make an accidental second filesystem scan fail the regression test.
        raise AssertionError("cached accounting should avoid a scan")

    monkeypatch.setattr(maintenance, "build_storage_manifest", fail_scan)

    cached = maintenance.load_storage_manifest(reports_dir)

    assert cached["source"] == "cached"
    assert cached["bytes"] == 0
