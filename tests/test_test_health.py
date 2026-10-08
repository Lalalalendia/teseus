import json
from pathlib import Path

from test_intelligence_unified_v1.index import build_index, plan_selection
from test_intelligence_unified_v1.test_stats import (
    ingest_test_stats,
    load_selection_stats,
    stats_db_path,
    summarize_test_health,
    summarize_test_stats,
)


def test_health_separates_baseline_regression_and_mutant_failures(tmp_path: Path) -> None:
    # Classify ordinary health evidence without counting intentional mutant kills as regressions.
    root = tmp_path / "project"
    root.mkdir()
    events = tmp_path / "events"
    events.mkdir()
    payloads = [
        {"event_id": "baseline-pass", "run_id": "run-1", "phase": "baseline", "nodeid": "tests/test_app.py::test_load", "outcome": "passed", "duration_ms": 10},
        {"event_id": "baseline-fail", "run_id": "run-2", "phase": "baseline", "nodeid": "tests/test_app.py::test_load", "outcome": "failed", "duration_ms": 12},
        {"event_id": "standalone-fail", "run_id": "run-3", "phase": "standalone", "nodeid": "tests/test_app.py::test_load", "outcome": "failed", "duration_ms": 11},
        {"event_id": "mutant-kill", "run_id": "run-4", "phase": "mutant", "mutant_id": "m1", "nodeid": "tests/test_app.py::test_load", "outcome": "failed", "first_failure": True, "duration_ms": 13},
    ]
    (events / "events.jsonl").write_text("".join(json.dumps(item) + "\n" for item in payloads), encoding="utf-8")
    db = stats_db_path(tmp_path / "reports")
    ingest_test_stats(events, db, project_root=root, run_id="history", source_path="app.py")

    row = summarize_test_stats(db, project_root=root)[0]
    assert row["baseline_failures"] == 1
    assert row["regression_failures"] == 1
    assert row["mutant_kills"] == 1
    assert row["health_executions"] == 3
    assert row["health_failures"] == 2
    assert row["health_status"] == "flaky"
    assert row["flaky_rate"] == 2 / 3
    assert summarize_test_health(db, project_root=root)["flaky"] == 1


def test_health_ordering_prioritizes_failing_and_flaky_tests(tmp_path: Path) -> None:
    # Make the health view actionable by putting failing and flaky nodeids before healthy ones.
    root = tmp_path / "project"
    root.mkdir()
    events = tmp_path / "events"
    events.mkdir()
    payloads = [
        {"event_id": "healthy", "phase": "baseline", "nodeid": "tests/test_app.py::test_healthy", "outcome": "passed", "duration_ms": 5},
        {"event_id": "flaky-pass", "phase": "baseline", "nodeid": "tests/test_app.py::test_flaky", "outcome": "passed", "duration_ms": 5},
        {"event_id": "flaky-fail", "phase": "standalone", "nodeid": "tests/test_app.py::test_flaky", "outcome": "failed", "duration_ms": 5},
        {"event_id": "failing", "phase": "standalone", "nodeid": "tests/test_app.py::test_failing", "outcome": "failed", "duration_ms": 5},
    ]
    (events / "events.jsonl").write_text("".join(json.dumps(item) + "\n" for item in payloads), encoding="utf-8")
    db = stats_db_path(tmp_path / "reports")
    ingest_test_stats(events, db, project_root=root, run_id="history", source_path="app.py")
    rows = summarize_test_stats(db, project_root=root, order_by="health")
    assert [row["health_status"] for row in rows] == ["failing", "flaky", "insufficient_data"]


def test_selection_demotes_historically_flaky_tests(tmp_path: Path) -> None:
    # Keep the candidate set frozen while preferring a stable test over a flaky equivalent.
    root = tmp_path / "project"
    root.mkdir()
    (root / "app.py").write_text("def choose(value):\n    return value\n", encoding="utf-8")
    tests_dir = root / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_app.py").write_text(
        "def test_flaky():\n    pass\n\ndef test_stable():\n    pass\n",
        encoding="utf-8",
    )
    selected = root / "selected.txt"
    selected.write_text(
        "tests/test_app.py::test_flaky\ntests/test_app.py::test_stable\n",
        encoding="utf-8",
    )
    index = build_index(root, root / "index.sqlite")
    events = tmp_path / "events"
    events.mkdir()
    payloads = [
        {"event_id": "flaky-pass", "phase": "baseline", "nodeid": "tests/test_app.py::test_flaky", "outcome": "passed", "duration_ms": 5},
        {"event_id": "flaky-fail", "phase": "standalone", "nodeid": "tests/test_app.py::test_flaky", "outcome": "failed", "duration_ms": 5},
        {"event_id": "flaky-kill", "phase": "mutant", "mutant_id": "m1", "nodeid": "tests/test_app.py::test_flaky", "outcome": "failed", "first_failure": True, "duration_ms": 5},
        {"event_id": "stable-pass", "phase": "baseline", "nodeid": "tests/test_app.py::test_stable", "outcome": "passed", "duration_ms": 5},
        {"event_id": "stable-kill", "phase": "mutant", "mutant_id": "m2", "nodeid": "tests/test_app.py::test_stable", "outcome": "failed", "first_failure": True, "duration_ms": 5},
    ]
    (events / "events.jsonl").write_text("".join(json.dumps(item) + "\n" for item in payloads), encoding="utf-8")
    db = stats_db_path(root / "reports")
    ingest_test_stats(events, db, project_root=root, run_id="history", source_path="app.py")

    stats = load_selection_stats(db, root)
    assert stats["tests/test_app.py::test_flaky"]["test_health_status"] == "flaky"
    snapshot = plan_selection(root, index, "app.py", "choose", selected_tests_file=selected, test_stats_db=db)
    assert snapshot.selected_tests == (
        "tests/test_app.py::test_stable",
        "tests/test_app.py::test_flaky",
    )
