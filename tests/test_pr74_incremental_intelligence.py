from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

from test_intelligence_unified_v1 import index as index_module
from test_intelligence_unified_v1.index import build_index, load_index


def _write_project(root: Path) -> None:
    (root / "helper.py").write_text(
        "def normalize(value):\n"
        "    return value + 1\n",
        encoding="utf-8",
    )
    (root / "app.py").write_text(
        "from helper import normalize\n\n"
        "def choose(value):\n"
        "    return normalize(value)\n",
        encoding="utf-8",
    )
    (root / "test_app.py").write_text(
        "from app import choose\n\n"
        "def test_choose():\n"
        "    assert choose(1) == 2\n",
        encoding="utf-8",
    )
    (root / "unrelated.py").write_text(
        "def untouched():\n"
        "    return True\n",
        encoding="utf-8",
    )


def _sidecar_path(database: Path) -> Path:
    return database.with_name(f"{database.name}.intelligence.json")


def test_incremental_intelligence_reuses_ast_and_persists_graph(tmp_path: Path) -> None:
    # The second SQLite build should reuse every unchanged file, including dependency facts.
    project = tmp_path / "project"
    project.mkdir()
    _write_project(project)
    database = tmp_path / "index.sqlite"

    cold = build_index(project, database)
    with patch.object(index_module, "parse_python_file", wraps=index_module.parse_python_file) as parser:
        warm = build_index(project, database)

    assert parser.call_count == 0
    assert warm["summary"]["reused_files"] == 4
    assert warm["intelligence"]["parsed_files"] == 0
    assert warm["intelligence"]["changed_files"] == []
    assert _sidecar_path(database).is_file()
    assert cold["dependency_graph"] == warm["dependency_graph"]
    assert warm["test_to_code"] == {
        "test_app.py::test_choose": ["app.py", "helper.py"],
    }

    loaded = load_index(database)
    assert loaded["dependency_graph"] == warm["dependency_graph"]
    assert loaded["test_to_code"] == warm["test_to_code"]
    assert loaded["source_to_tests"] == {
        "app.py": ["test_app.py::test_choose"],
        "helper.py": ["test_app.py::test_choose"],
        "unrelated.py": [],
    }


def test_incremental_intelligence_invalidates_transitive_dependents(tmp_path: Path) -> None:
    # A helper change invalidates its importer and the test that reaches the importer.
    project = tmp_path / "project"
    project.mkdir()
    _write_project(project)
    database = tmp_path / "index.sqlite"
    build_index(project, database)
    (project / "helper.py").write_text(
        "def normalize(value):\n"
        "    return value + 2\n",
        encoding="utf-8",
    )

    with patch.object(index_module, "parse_python_file", wraps=index_module.parse_python_file) as parser:
        changed = build_index(project, database)

    assert [call.args[0].name for call in parser.call_args_list] == ["helper.py"]
    intelligence = changed["intelligence"]
    assert intelligence["changed_source_files"] == ["helper.py"]
    assert intelligence["changed_test_files"] == []
    assert intelligence["invalidated_files"] == ["app.py", "helper.py", "test_app.py"]
    assert intelligence["invalidated_tests"] == ["test_app.py::test_choose"]
    assert changed["source_to_tests"]["app.py"] == ["test_app.py::test_choose"]
    assert changed["source_to_tests"]["helper.py"] == ["test_app.py::test_choose"]


def test_incremental_intelligence_separates_test_changes(tmp_path: Path) -> None:
    # Editing a test must not invalidate source files or their source fingerprint.
    project = tmp_path / "project"
    project.mkdir()
    _write_project(project)
    database = tmp_path / "index.sqlite"
    first = build_index(project, database)
    (project / "test_app.py").write_text(
        "from app import choose\n\n"
        "def test_choose():\n"
        "    assert choose(2) == 3\n",
        encoding="utf-8",
    )

    with patch.object(index_module, "parse_python_file", wraps=index_module.parse_python_file) as parser:
        changed = build_index(project, database)

    assert [call.args[0].name for call in parser.call_args_list] == ["test_app.py"]
    intelligence = changed["intelligence"]
    assert intelligence["changed_source_files"] == []
    assert intelligence["changed_test_files"] == ["test_app.py"]
    assert intelligence["invalidated_files"] == ["test_app.py"]
    assert intelligence["invalidated_tests"] == ["test_app.py::test_choose"]
    assert changed["source_fingerprint"] == first["source_fingerprint"]
    assert changed["test_fingerprint"] != first["test_fingerprint"]


def test_incremental_intelligence_uses_content_hash_when_stat_is_unchanged(tmp_path: Path) -> None:
    # Same-size edits with restored timestamps must still force a parse.
    project = tmp_path / "project"
    project.mkdir()
    _write_project(project)
    database = tmp_path / "index.sqlite"
    build_index(project, database)
    helper = project / "helper.py"
    original_stat = helper.stat()
    helper.write_text(
        "def normalize(value):\n"
        "    return value + 2\n",
        encoding="utf-8",
    )
    os.utime(helper, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
    assert helper.stat().st_size == original_stat.st_size
    assert helper.stat().st_mtime_ns == original_stat.st_mtime_ns

    with patch.object(index_module, "parse_python_file", wraps=index_module.parse_python_file) as parser:
        changed = build_index(project, database)

    assert [call.args[0].name for call in parser.call_args_list] == ["helper.py"]
    assert changed["intelligence"]["changed_files"] == ["helper.py"]


def test_incremental_and_clean_intelligence_are_semantically_equivalent(tmp_path: Path) -> None:
    # Rename detection and the final semantic projection must agree with a clean rebuild.
    project = tmp_path / "project"
    project.mkdir()
    _write_project(project)
    incremental_database = tmp_path / "incremental.sqlite"
    build_index(project, incremental_database)
    helper = project / "helper.py"
    renamed = project / "helper_renamed.py"
    renamed.write_text(helper.read_text(encoding="utf-8"), encoding="utf-8")
    helper.unlink()
    (project / "app.py").write_text(
        "from helper_renamed import normalize\n\n"
        "def choose(value):\n"
        "    return normalize(value)\n",
        encoding="utf-8",
    )
