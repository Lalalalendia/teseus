from __future__ import annotations
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from test_intelligence_unified_v1 import pytest_plugin as plugin_module
from test_intelligence_unified_v1.pytest_plugin import _TestStatsPlugin
from test_intelligence_unified_v1.test_stats import load_mutant_test_observations, stats_db_path

def _record_passed_test(plugin: _TestStatsPlugin, nodeid: str) -> None:
    # Emit one complete passing pytest protocol through the normal stats hooks.
    for when in ("setup", "call", "teardown"):
        plugin.pytest_runtest_logreport(
            SimpleNamespace(
                nodeid=nodeid,
                when=when,
                outcome="passed",
                duration=0.001,
                wasxfail=False,
            )
        )

def test_mutant_stats_fast_path_skips_runtime_dependency_tracing(tmp_path: Path) -> None:
    # Preserve executable mutant observations while omitting dependency tracing already owned by baseline evidence.
    root = tmp_path / "project"
    root.mkdir()
    event_dir = tmp_path / "events"
    nodeid = "tests/test_app.py::test_value"
    with patch.dict(
        os.environ,
        {
            "TI_TEST_STATS_OUT_DIR": str(event_dir),
            "TI_TEST_STATS_RUN_ID": "run-pr49-fast",
            "TI_TEST_STATS_PHASE": "mutant",
            "TI_TEST_STATS_LEVEL": "L1",
            "TI_TEST_STATS_MUTANT_ID": "m1",
            "TI_TEST_STATS_SOURCE_PATH": "app.py",
            "TI_TEST_STATS_ATTEMPT": "attempt-pr49-fast",
            "TI_TEST_STATS_EXPECTED_PROJECT_ROOT": str(root),
        },
        clear=False,
    ), patch.object(plugin_module.sys, "addaudithook") as audit_hook, patch.object(
        _TestStatsPlugin,
        "_runtime_dependencies",
        side_effect=AssertionError("mutant fast path must not materialize runtime dependencies"),
    ), patch.object(
        _TestStatsPlugin,
        "_environment_observations",
        side_effect=AssertionError("mutant fast path must not rescan environment dependencies"),
    ):
        plugin = _TestStatsPlugin()
        _record_passed_test(plugin, nodeid)
        plugin.pytest_sessionfinish(SimpleNamespace(exitstatus=0), 0)
    audit_hook.assert_not_called()
    journal = next(event_dir.glob("*.jsonl"))
    event = json.loads(journal.read_text(encoding="utf-8"))
    assert event["phase"] == "mutant"
    assert event["outcome"] == "passed"
    assert event["runtime_dependencies"] == []
    assert event["runtime_dependency_complete"] is False
    assert event["runtime_dependency_blockers"] == ["mutant-runtime-evidence-not-collected"]
    assert event["environment_reads"] == []
    assert event["environment_dependency_complete"] is False
    assert event["environment_dependency_blockers"] == ["mutant-runtime-evidence-not-collected"]
    observations = load_mutant_test_observations(
        stats_db_path(tmp_path / "reports"),
        run_id="run-pr49-fast",
        mutant_ids=("m1",),
        root=root,
        event_dir=event_dir,
    )["m1"]
    assert len(observations) == 1
    assert observations[0]["test_id"] == nodeid
    assert observations[0]["outcome"] == "passed"
    assert observations[0]["evidence_kind"] == "pytest_test_event"
    assert observations[0]["runtime_dependency_complete"] is False
    assert observations[0]["runtime_dependency_blockers"] == ["mutant-runtime-evidence-not-collected"]

def test_baseline_stats_keeps_full_runtime_dependency_tracing(tmp_path: Path) -> None:
    # Keep baseline as the authoritative runtime dependency manifest used by test fingerprinting and reuse.
    root = tmp_path / "project"
    root.mkdir()
    event_dir = tmp_path / "events"
    nodeid = "tests/test_app.py::test_value"
    runtime_rows = [{"path": "data.json", "outside_workspace": False}]
    environment_rows = [{"name": "APP_MODE", "present": True, "value_hmac": "hash", "declared": True, "secret": False}]
    with patch.dict(
        os.environ,
        {
            "TI_TEST_STATS_OUT_DIR": str(event_dir),
            "TI_TEST_STATS_RUN_ID": "run-pr49-baseline",
            "TI_TEST_STATS_PHASE": "baseline",
            "TI_TEST_STATS_LEVEL": "L1",
            "TI_TEST_STATS_SOURCE_PATH": "app.py",
            "TI_TEST_STATS_ATTEMPT": "attempt-pr49-baseline",
            "TI_TEST_STATS_EXPECTED_PROJECT_ROOT": str(root),
        },
        clear=False,
    ), patch.object(plugin_module.sys, "addaudithook") as audit_hook, patch.object(
        _TestStatsPlugin,
        "_runtime_dependencies",
        return_value=(runtime_rows, ()),
    ) as runtime_dependencies, patch.object(
        _TestStatsPlugin,
        "_environment_observations",
        return_value=(environment_rows, ()),
    ) as environment_observations:
        plugin = _TestStatsPlugin()
        _record_passed_test(plugin, nodeid)
        plugin.pytest_sessionfinish(SimpleNamespace(exitstatus=0), 0)
    audit_hook.assert_called_once()
    runtime_dependencies.assert_called_once_with(nodeid)
    environment_observations.assert_called_once_with(nodeid)
    event = json.loads(next(event_dir.glob("*.jsonl")).read_text(encoding="utf-8"))
    assert event["runtime_dependencies"] == runtime_rows
    assert event["runtime_dependency_complete"] is True
    assert event["environment_reads"] == environment_rows
    assert event["environment_dependency_complete"] is True
