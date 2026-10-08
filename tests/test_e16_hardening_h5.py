from __future__ import annotations
import json
import sys
from pathlib import Path
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    MutationScope,
    ProjectDescriptor,
    ProjectId,
    TestCommandDescriptor,
)
from theseus_knowledge import KnowledgePlaneStore
from theseus_local import LocalCampaignCoordinator
from theseus_local.workspace import WorkspaceProvider
def _configuration(root: Path, campaign_id: str) -> CampaignConfiguration:
    # Build two campaigns over one project so the second run can exercise partial reuse.
    return CampaignConfiguration(
        campaign_id=CampaignId(campaign_id),
        project=ProjectDescriptor(
            project_id=ProjectId("project_e16_partial"),
            display_name="E16 partial reuse fixture",
            root_path=str(root),
            test_command=TestCommandDescriptor((sys.executable, "-m", "pytest", "-q")),
            pytest_plugin_autoload=False,
        ),
        scope=MutationScope(source_path="app.py", function="choose", operators=("condition_to_not",)),
        budget=CampaignBudget(max_mutants=1, max_workers=1),
        no_escalation=True,
        reports_dir=str(root / "reports"),
        reuse_mode="experimental",
    )
def test_partial_reuse_executes_and_merges_only_missing_tests(tmp_path: Path) -> None:
    # Prove a changed test is rerun while an unchanged test is carried forward with an audit marker.
    (tmp_path / "app.py").write_text(
        "def choose(value):\n    if value > 0:\n        return 1\n    return 0\n",
        encoding="utf-8",
    )
    test_file = tmp_path / "test_app.py"
    test_file.write_text(
        "from app import choose\n\n"
        "def test_positive():\n    assert choose(1) == 1\n\n"
        "def test_negative():\n    assert choose(-1) == 0\n",
        encoding="utf-8",
    )
    coordinator = LocalCampaignCoordinator()
    first_configuration = _configuration(tmp_path, "cmp_e16_partial_first")
    first = coordinator.run(first_configuration)
    assert first.succeeded, first.error
    test_file.write_text(
        "from app import choose\n\n"
        "def test_positive():\n    assert choose(1) == 1\n\n"
        "def test_negative():\n    assert choose(-1) == int(0)\n",
        encoding="utf-8",
    )
    second_configuration = _configuration(tmp_path, "cmp_e16_partial_second")
    second = coordinator.run(second_configuration)
    assert second.succeeded, second.error
    reports_root = WorkspaceProvider(second_configuration).reports_root
    plan = json.loads(
        (reports_root / "cmp_e16_partial_second" / "reuse.plan.json").read_text(encoding="utf-8")
    )
    assert [item["kind"] for item in plan["decisions"]] == ["partial"], plan["decisions"]
    decision = plan["decisions"][0]
    assert decision["eligible"] is True
    assert decision["matched_test_ids"] == ["test_app.py::test_positive"]
    assert decision["missing_test_ids"] == ["test_app.py::test_negative"]
    report = json.loads(
        (
            reports_root
            / "engine"
            / "cmp_e16_partial_second"
            / "engine-shard-cmp_e16_partial_second-shard-000-attempt-0.json"
        ).read_text(encoding="utf-8")
    )
    result = report["results"][0]
    assert {
        nodeid
        for level in result["level_results"]
        for nodeid in level.get("nodeids", [])
    } == {"test_app.py::test_negative"}
    knowledge = KnowledgePlaneStore(
        reports_root.parent / "knowledge" / "project_e16_partial.sqlite3"
    )
    try:
        rows = knowledge.query_executions(
            campaign_id="cmp_e16_partial_second",
            limit=10,
            include_payload=True,
        ).rows
        observations = rows[0]["payload"]["test_observations"]
    finally:
        knowledge.close()
    assert {item["test_id"] for item in observations} == {
        "test_app.py::test_positive",
        "test_app.py::test_negative",
    }