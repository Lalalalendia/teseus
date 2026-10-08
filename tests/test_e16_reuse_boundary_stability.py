from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from theseus_knowledge import KnowledgePlaneStore, ReuseKind
from theseus_local import LocalCampaignCoordinator


def _runtime_dependencies(nodeids: tuple[str, ...]) -> dict[str, dict[str, object]]:
    # Build complete runtime manifests containing imported code and one independent Python data file.
    return {
        nodeid: {
            "complete": True,
            "blockers": [],
            "dependencies": [
                {"path": "test_app.py", "outside_workspace": False},
                {"path": "app.py", "outside_workspace": False},
                {"path": "data_fixture.py", "outside_workspace": False},
                {"path": "__pycache__/app.cpython-test.pyc", "outside_workspace": False},
            ],
            "environment_dependencies": [],
            "environment_blockers": [],
        }
        for nodeid in nodeids
    }


def _test_fingerprints(root: Path, nodeids: tuple[str, ...]) -> dict[str, str]:
    # Build production node fingerprints with the same runtime manifest shape used after baseline.
    fingerprints, blockers = LocalCampaignCoordinator._test_fingerprint_bundle(
        root,
        {
            "pytest_configuration_fingerprint": "configuration",
            "plugin_fingerprint": "plugins",
            "nodeids": list(nodeids),
        },
        nodeids,
        runtime_dependencies=_runtime_dependencies(nodeids),
    )
    assert blockers == ()
    return fingerprints


def test_runtime_imported_python_files_do_not_destroy_node_granularity(tmp_path: Path) -> None:
    # Keep sibling test edits local while preserving runtime-observed Python data dependencies.
    (tmp_path / "app.py").write_text(
        "def choose(value):\n    return value > 0\n",
        encoding="utf-8",
    )
    test_path = tmp_path / "test_app.py"
    test_path.write_text(
        "from app import choose\n\n"
        "def test_positive():\n    assert choose(1) is True\n\n"
        "def test_negative():\n    assert choose(-1) is False\n",
        encoding="utf-8",
    )
    data_path = tmp_path / "data_fixture.py"
    data_path.write_text("PAYLOAD = 1\n", encoding="utf-8")
    nodeids = (
        "test_app.py::test_positive",
        "test_app.py::test_negative",
    )

    before = _test_fingerprints(tmp_path, nodeids)
    test_path.write_text(
        "from app import choose\n\n"
        "def test_positive():\n    assert choose(1) is True\n\n"
        "def test_negative():\n    assert bool(choose(-1)) is False\n",
        encoding="utf-8",
    )
    after_sibling_edit = _test_fingerprints(tmp_path, nodeids)

    assert after_sibling_edit[nodeids[0]] == before[nodeids[0]]
    assert after_sibling_edit[nodeids[1]] != before[nodeids[1]]

    data_path.write_text("PAYLOAD = 2\n", encoding="utf-8")
    after_data_edit = _test_fingerprints(tmp_path, nodeids)
    assert after_data_edit[nodeids[0]] != after_sibling_edit[nodeids[0]]
    assert after_data_edit[nodeids[1]] != after_sibling_edit[nodeids[1]]


def test_selection_reuse_fingerprint_ignores_explanatory_reason(tmp_path: Path) -> None:
    # Keep historical explanation changes out of reuse identity while retaining execution membership.
    configuration = SimpleNamespace(
        scope=SimpleNamespace(source_path="app.py"),
        no_escalation=True,
    )
    first_level = SimpleNamespace(
        name="L1",
        reason="static-direct",
        nodeids=("test_app.py::test_choose",),
        files=("test_app.py",),
    )
    second_level = SimpleNamespace(
        name="L1",
        reason="static-direct;historical-kill",
        nodeids=("test_app.py::test_choose",),
        files=("test_app.py",),
    )
    first_prepared = SimpleNamespace(
        source_sha256="source",
        selection=SimpleNamespace(
            algorithm_version="selection-v5",
            source_path="app.py",
            source_sha256="source",
            selected_tests=("test_app.py::test_choose",),
            dropped_nodeids=(),
            levels=(first_level,),
        ),
    )
    second_prepared = SimpleNamespace(
        source_sha256="source",
        selection=SimpleNamespace(
            algorithm_version="selection-v5",
            source_path="app.py",
            source_sha256="source",
            selected_tests=("test_app.py::test_choose",),
            dropped_nodeids=(),
            levels=(second_level,),
        ),
    )

    first = LocalCampaignCoordinator._selection_reuse_fingerprint(
        configuration,
        first_prepared,
        tmp_path,
        ("test_app.py::test_choose",),
    )
    second = LocalCampaignCoordinator._selection_reuse_fingerprint(
        configuration,
        second_prepared,
        tmp_path,
        ("test_app.py::test_choose",),
    )
    changed_membership = LocalCampaignCoordinator._selection_reuse_fingerprint(
        configuration,
        second_prepared,
        tmp_path,
        ("test_app.py::test_choose", "test_app.py::test_other"),
    )

    assert second == first
    assert changed_membership != first


def test_historical_hint_reports_the_changed_reuse_boundary(tmp_path: Path) -> None:
    # Expose exact mismatch fields without weakening any executable reuse requirement.
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    try:
        store.ingest_effect(
            effect_id="effect-source",
            campaign_id="campaign-source",
            effect_type="mutation.execute_shard",
            payload={
                "executions": [
                    {
                        "execution_id": "exec-source",
                        "mutant_id": "m1",
                        "attempt": 0,
                        "status": "complete",
                        "semantic_result": "killed",
                        "restore_verified": True,
                        "evidence_schema_version": 2,
                        "source_kind": "observed",
                        "evidence_origin": "pytest_test_stats",
                        "function_id": "choose",
                        "function_fingerprint": "function",
                        "mutant_fingerprint": "mutant",
                        "test_fingerprint": "tests-old",
                        "conftest_fingerprint": "conftest",
                        "environment_fingerprint": "environment",
                        "selection_fingerprint": "selection",
                        "result_fingerprint": "result-old",
                        "test_observations": [
                            {
                                "test_id": "test_app.py::test_choose",
                                "test_fingerprint": "node-old",
                                "outcome": "failed",
                                "evidence_kind": "pytest_test_event",
                                "observation_schema_version": 1,
                            }
                        ],
                    }
                ]
            },
        )
        decision = store.decide_reuse(
            mutant_id="m1",
            mutant_fingerprint="mutant",
            function_id="choose",
            function_fingerprint="function",
            environment_fingerprint="environment",
            selection_fingerprint="selection",
            test_fingerprint="tests-new",
            test_fingerprints={"test_app.py::test_choose": "node-new"},
            conftest_fingerprint="conftest",
            result_fingerprint="result-new",
        )
    finally:
        store.close()

    assert decision.kind is ReuseKind.HISTORICAL_HINT
    assert "test_fingerprint" in decision.reason
    assert "result_fingerprint" in decision.reason
    assert "test_nodes=test_app.py::test_choose" in decision.reason
