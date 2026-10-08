from __future__ import annotations

from pathlib import Path

from theseus_knowledge import KnowledgePlaneStore, ReuseKind
from theseus_local import LocalCampaignCoordinator
from test_intelligence_unified_v1.pytest_plugin import _TestStatsPlugin, _is_runtime_support_path, _open_access_mode


def _bundle(root: Path, nodeid: str, *, runtime_dependencies=None):
    # Build the production fingerprint bundle used by the dependency invariants.
    return LocalCampaignCoordinator._test_fingerprint_bundle(
        root,
        None,
        (nodeid,),
        runtime_dependencies=runtime_dependencies,
    )


def test_local_pytest_plugin_transitive_helper_changes_invalidate_node(tmp_path: Path) -> None:
    # Hash a local pytest plugin and its imported helper as one node-level dependency closure.
    (tmp_path / "conftest.py").write_text(
        'pytest_plugins = ["local_plugin"]\n',
        encoding="utf-8",
    )
    (tmp_path / "local_plugin.py").write_text(
        "from plugin_helper import marker\n\n"
        "def pytest_configure(config):\n"
        "    config._theseus_marker = marker()\n",
        encoding="utf-8",
    )
    helper = tmp_path / "plugin_helper.py"
    helper.write_text("def marker():\n    return 'one'\n", encoding="utf-8")
    (tmp_path / "test_app.py").write_text(
        "def test_value():\n    assert True\n",
        encoding="utf-8",
    )
    first, first_blockers = _bundle(tmp_path, "test_app.py::test_value")
    helper.write_text("def marker():\n    return 'two'\n", encoding="utf-8")
    second, second_blockers = _bundle(tmp_path, "test_app.py::test_value")
    assert not first_blockers
    assert not second_blockers
    assert first["test_app.py::test_value"] != second["test_app.py::test_value"]


def test_pytest_plugin_constant_concatenation_is_resolved(tmp_path: Path) -> None:
    # Accept only static plugin list concatenation and include every resolved local plugin in the fingerprint.
    (tmp_path / "conftest.py").write_text(
        'plugin_names = ["first_plugin"]\npytest_plugins = plugin_names + ("second_plugin",)\n',
        encoding="utf-8",
    )
    (tmp_path / "first_plugin.py").write_text("VALUE = 'first'\n", encoding="utf-8")
    second = tmp_path / "second_plugin.py"
    second.write_text("VALUE = 'one'\n", encoding="utf-8")
    (tmp_path / "test_app.py").write_text(
        "def test_value():\n    assert True\n",
        encoding="utf-8",
    )
    first, blockers = _bundle(tmp_path, "test_app.py::test_value")
    second.write_text("VALUE = 'two'\n", encoding="utf-8")
    changed, changed_blockers = _bundle(tmp_path, "test_app.py::test_value")
    assert not blockers
    assert not changed_blockers
    assert first["test_app.py::test_value"] != changed["test_app.py::test_value"]


def test_dynamic_pytest_plugin_declaration_blocks_reuse(tmp_path: Path) -> None:
    # Refuse exact or partial reuse when the plugin set cannot be determined without executing code.
    (tmp_path / "conftest.py").write_text(
        "import os\npytest_plugins = os.environ['THESEUS_PLUGIN']\n",
        encoding="utf-8",
    )
    (tmp_path / "test_app.py").write_text(
        "def test_value():\n    assert True\n",
        encoding="utf-8",
    )
    _, blockers = _bundle(tmp_path, "test_app.py::test_value")
    assert any("dynamic-pytest-plugin-declaration" in item for item in blockers)


def test_static_data_file_changes_invalidate_test_fingerprint(tmp_path: Path) -> None:
    # Include literal data paths in the fingerprint so content changes cannot reuse old observations.
    data = tmp_path / "fixture.txt"
    data.write_text("one\n", encoding="utf-8")
    (tmp_path / "test_data.py").write_text(
        "from pathlib import Path\n\n"
        "def test_data():\n    assert Path('fixture.txt').read_text()\n",
        encoding="utf-8",
    )
    first, first_blockers = _bundle(tmp_path, "test_data.py::test_data")
    data.write_text("two\n", encoding="utf-8")
    second, second_blockers = _bundle(tmp_path, "test_data.py::test_data")
    assert not first_blockers
    assert not second_blockers
    assert first["test_data.py::test_data"] != second["test_data.py::test_data"]


def test_unknown_reuse_dependency_is_not_eligible(tmp_path: Path) -> None:
    # Persist the unresolved dependency as a decision blocker rather than silently producing a reusable hint.
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    try:
        decision = store.decide_reuse(
            mutant_id="m-unknown",
            function_id="choose",
            function_fingerprint="function",
            mutant_fingerprint="mutant",
            test_fingerprint="test",
            conftest_fingerprint="conftest",
            environment_fingerprint="environment",
            selection_fingerprint="selection",
            result_fingerprint="result",
            reuse_blockers=("dynamic-pytest-plugin-declaration",),
        )
        assert decision.kind is ReuseKind.NONE
        assert decision.eligible is False
        assert decision.blockers == ("dynamic-pytest-plugin-declaration",)
    finally:
        store.close()


def test_project_resource_under_tmpdir_is_not_filtered_as_runtime_support(tmp_path: Path, monkeypatch) -> None:
    # Preserve a project dependency even when the complete project root lives below TMPDIR.
    project_root = tmp_path / "project"
    project_root.mkdir()
    resource = project_root / "strict.txt"
    resource.write_text("one\n", encoding="utf-8")
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    assert _is_runtime_support_path(resource, project_root) is False
    assert _open_access_mode("open", (str(resource), "r", 0)) == "read"
    assert _open_access_mode("open", (str(resource), "w", 0)) == "write"
    assert _open_access_mode("open", (str(resource), "r+", 0)) == "read_write"


def test_runtime_dependency_records_dynamic_project_file_under_tmpdir(tmp_path: Path, monkeypatch) -> None:
    # Exercise the same audit boundary used by nested pytest rather than relying on static path discovery.
    project_root = tmp_path / "project"
    project_root.mkdir()
    resource = project_root / "strict.txt"
    resource.write_text("one\n", encoding="utf-8")
    events = project_root / "events"
    monkeypatch.setenv("TI_TEST_STATS_OUT_DIR", str(events))
    monkeypatch.setenv("TI_TEST_STATS_EXPECTED_PROJECT_ROOT", str(project_root))
    plugin = _TestStatsPlugin()
    nodeid = "test_app.py::test_dynamic_resource"
    plugin._active_nodeid = nodeid
    plugin._active_owner = nodeid
    try:
        plugin._audit_event("open", (str(resource), "r", 0))
        rows, blockers = plugin._runtime_dependencies(nodeid)
    finally:
        plugin._close_output()
    assert not blockers
    assert any(row["relative_path"] == "strict.txt" for row in rows)
