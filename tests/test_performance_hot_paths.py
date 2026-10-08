from pathlib import Path
from unittest.mock import patch

from test_intelligence_unified_v1.index import validate_test_nodeids
from test_intelligence_unified_v1.models import CampaignAccumulator
from test_intelligence_unified_v1.runner import MutationConfig, MutationRunner


def test_nodeid_validation_uses_indexed_base_lookup(tmp_path: Path) -> None:
    # Validate parametrized nodeids through one base-name set without nested scans.
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_app.py").write_text("def test_load(value):\n    pass\n", encoding="utf-8")
    index = {
        "tests": [
            {"nodeid": "tests/test_app.py::test_load[value]"},
            {"nodeid": "tests/test_app.py::test_other"},
        ]
    }
    valid, dropped = validate_test_nodeids(
        index,
        tmp_path,
        (
            "tests/test_app.py::test_load[one]",
            "tests/test_app.py::test_missing",
            "tests/test_app.py::test_load[one]",
        ),
    )
    assert valid == ("tests/test_app.py::test_load[one]",)
    assert dropped == ("tests/test_app.py::test_missing",)


def test_campaign_accumulator_preserves_report_metrics() -> None:
    # Accumulate result counters once and expose the existing mutation metric contract.
    accumulator = CampaignAccumulator()
    accumulator.add_result(
        {
            "status": "killed",
            "mutant": {"mutation": "condition_to_not", "operator_version": "m3"},
            "level_results": [{"level": "L1", "result": {"passed": True}}],
            "selection": {"levels": [{"source": "line-impact", "dropped_nodeids": ["old"]}]},
        }
    )
    accumulator.add_result(
        {
            "status": "survived",
            "mutant": {"mutation": "condition_to_not", "operator_version": "m3"},
            "level_results": [{"level": "L1", "result": {"passed": False}}],
            "selection": {"levels": [{"source": "line-impact", "dropped_nodeids": []}]},
        }
    )
    metrics = accumulator.metric_payload()
    assert metrics["counts"] == {"killed": 1, "survived": 1}
    assert metrics["mutation_score"] == 0.5
    assert metrics["selection_sources"] == {"line-impact": 2}
    assert metrics["dropped_nodeids"] == 1
    assert metrics["operator_stats"]["condition_to_not"]["total_mutants"] == 2


def test_runner_health_is_refreshed_only_on_explicit_checkpoint(tmp_path: Path) -> None:
    # Keep per-mutant metric checkpoints cheap while retaining an explicit final health refresh.
    (tmp_path / "app.py").write_text("def choose(value):\n    return value\n", encoding="utf-8")
    runner = MutationRunner(MutationConfig(project_root=tmp_path, source="app.py", reports_dir=tmp_path / "reports"))
    result = {
        "status": "killed",
        "mutant": {"mutation": "condition_to_not", "operator_version": "m3"},
        "level_results": [],
        "selection": {"levels": []},
    }
    runner._record_campaign_result(result)
    with patch("test_intelligence_unified_v1.runner.summarize_test_health", return_value={"tests": 1}) as health:
        runner._metrics()
        health.assert_not_called()
        runner._metrics(refresh_health=True)
        health.assert_called_once()
