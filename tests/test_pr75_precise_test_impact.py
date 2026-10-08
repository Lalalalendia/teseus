from __future__ import annotations

import sys
from pathlib import Path

from test_intelligence_unified_v1.index import build_index, build_nodeid_validation_context, plan_selection
from test_intelligence_unified_v1.models import CampaignAccumulator, Mutant
from test_intelligence_unified_v1.runner import LevelSpec, MutationConfig, MutationRunner


def _write_dependency_project(root: Path) -> None:
    (root / "core.py").write_text(
        "def transform(value):\n"
        "    if value > 0:\n"
        "        return 1\n"
        "    return 0\n",
        encoding="utf-8",
    )
    (root / "test_behavior.py").write_text(
        "from core import transform\n\n"
        "def test_behavior():\n"
        "    assert transform(1) == 1\n",
        encoding="utf-8",
    )
    (root / "test_unrelated.py").write_text(
        "def test_unrelated():\n"
        "    assert True\n",
        encoding="utf-8",
    )


def _runner_for_selection(root: Path, *, no_escalation: bool = False) -> MutationRunner:
    index = build_index(root, root / "index.sqlite")
    runner = MutationRunner(
        MutationConfig(
            project_root=root,
            source="core.py",
            function="transform",
            no_escalation=no_escalation,
        )
    )
    runner._campaign_index = index
    runner._campaign_source_rel = "core.py"
    runner._campaign_function_id = "core.py::transform"
    runner._campaign_function_info = next(
        item for item in index["functions"] if item["function_id"] == "core.py::transform"
    )
    runner._nodeid_validation_context = build_nodeid_validation_context(index, root)
    runner._context_map = None
    runner._impact_adapter = None
    return runner


def _mutant() -> Mutant:
    return Mutant(
        "m1",
        "condition_to_not",
        2,
        4,
        "if value > 0:",
        "if not value > 0:",
        1,
        14,
    )


def test_pr75_planner_uses_minimal_static_dependency_set(tmp_path: Path) -> None:
    # A non-name-matching test importing the target file is still selected.
    _write_dependency_project(tmp_path)
    index = build_index(tmp_path, tmp_path / "index.sqlite")
    selection = plan_selection(tmp_path, index, "core.py", "transform")

    assert selection.levels[0].nodeids == ("test_behavior.py::test_behavior",)
    assert selection.levels[0].reason == "static-dependency"
    assert {item.source for item in selection.evidence["test_behavior.py::test_behavior"]} == {
        "static_dependency"
    }


def test_pr75_per_mutant_selection_publishes_candidate_proof(tmp_path: Path) -> None:
    # Static dependency narrows execution but marks survival as needing escalation.
    _write_dependency_project(tmp_path)
    runner = _runner_for_selection(tmp_path)
    selection = runner._build_mutant_selection(_mutant(), LevelSpec("L1", "impact", (), (), ()))

    assert selection.source == "static-dependency"
    assert selection.nodeids == ("test_behavior.py::test_behavior",)
    assert selection.proof_level == "candidate"
    assert selection.requires_escalation is True
    assert selection.candidate_count == 1
    assert "static_dependency" in selection.proof_sources


def test_pr75_no_escalation_does_not_claim_unsafe_survival(tmp_path: Path) -> None:
    # Insufficient proof never becomes an unsafe narrow pass when escalation is disabled.
    _write_dependency_project(tmp_path)
    runner = _runner_for_selection(tmp_path, no_escalation=True)
    runner._campaign_selection = plan_selection(tmp_path, runner._campaign_index, "core.py", "transform")

    selected_level, detail = runner._level_for_mutant(_mutant(), LevelSpec("L1", "impact", (), (), ()))

    assert selected_level is None
    assert detail["proof_level"] == "candidate"
    assert detail["fallback_reason"] == "safe escalation disabled by configuration"


def test_pr75_metrics_report_test_reduction_and_escalation() -> None:
    # Keep the optimization multiplier visible in the bounded accumulator.
    accumulator = CampaignAccumulator(configured_test_count=10)
    for level_results in (
        [{"level": "L1", "result": {"passed": False}}],
        [
            {"level": "L1", "result": {"passed": True}},
            {"level": "L2", "result": {"passed": True}},
        ],
    ):
        accumulator.add_result(
            {
                "status": "killed" if not level_results[0]["result"]["passed"] else "survived",
                "mutant": {"mutation": "condition_to_not", "operator_version": "m2"},
                "selection": {
                    "levels": [
                        {
                            "source": "static-dependency",
                            "nodeids": ["test_behavior.py::test_behavior"],
                            "candidate_count": 2,
                            "dropped_nodeids": [],
                        }
                    ]
                },
                "level_results": level_results,
            }
        )

    metrics = accumulator.metric_payload()
    assert metrics["selected_tests_total"] == 2
    assert metrics["candidate_tests_total"] == 4
    assert metrics["tests_executed_per_mutant"] == 1.0
    assert metrics["selected_tests_ratio"] == 0.1
    assert metrics["escalation_rate"] == 0.5


def test_pr75_real_mutant_is_killed_by_selected_dependency_test(tmp_path: Path) -> None:
    # The narrow set must preserve the full-test semantic result on the representative mutation.
    _write_dependency_project(tmp_path)
    report = MutationRunner(
        MutationConfig(
            project_root=tmp_path,
            source="core.py",
            function="transform",
            operators=("condition_to_not",),
            max_mutants=1,
            common_command_argv=(sys.executable, "-m", "pytest", "-q"),
            use_baseline_cache=False,
            reports_dir=tmp_path / "reports",
        )
    ).run()

    assert report["status"] == "complete"
    result = report["results"][0]
    assert result["status"] == "killed"
    assert result["selection"]["levels"][0]["source"] == "static-dependency"
    assert result["selection"]["levels"][0]["proof_level"] == "candidate"
    assert result["level_results"][0]["nodeids"] == ["test_behavior.py::test_behavior"]
