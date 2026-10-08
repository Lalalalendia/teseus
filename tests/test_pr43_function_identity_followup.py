from __future__ import annotations
import inspect
import sqlite3
from pathlib import Path
from test_intelligence_unified_v1 import index as index_module
from test_intelligence_unified_v1.index import build_index, load_index, parse_python_file

def _duplicate_local_definitions() -> str:
    # Build the collision pattern that failed the first real-project benchmark.
    return (
        "def test_first():\n"
        "    def record_batch():\n"
        "        return 1\n"
        "    class FakeCoordinator:\n"
        "        def run(self):\n"
        "            return record_batch()\n"
        "    return FakeCoordinator().run()\n\n"
        "def test_second():\n"
        "    def record_batch():\n"
        "        return 2\n"
        "    class FakeCoordinator:\n"
        "        def run(self):\n"
        "            return record_batch()\n"
        "    return FakeCoordinator().run()\n"
    )

def test_local_definitions_receive_complete_lexical_identities(tmp_path: Path) -> None:
    # Distinguish equal helper and local-class names owned by different enclosing functions.
    source = tmp_path / "test_helpers.py"
    source.write_text(_duplicate_local_definitions(), encoding="utf-8")
    functions, tests, _ = parse_python_file(source, tmp_path)
    identities = {item.function_id for item in functions}
    assert len(identities) == len(functions)
    assert "test_helpers.py::test_first.record_batch" in identities
    assert "test_helpers.py::test_first.FakeCoordinator.run" in identities
    assert "test_helpers.py::test_second.record_batch" in identities
    assert "test_helpers.py::test_second.FakeCoordinator.run" in identities
    assert {item.nodeid for item in tests} == {
        "test_helpers.py::test_first",
        "test_helpers.py::test_second",
    }

def test_repeated_definition_in_one_scope_remains_explicit_and_unique(tmp_path: Path) -> None:
    # Retain shadowed source definitions without violating the function identity primary key.
    source = tmp_path / "app.py"
    source.write_text(
        "def choose():\n    return 1\n\n"
        "def choose():\n    return 2\n",
        encoding="utf-8",
    )
    functions, _, _ = parse_python_file(source, tmp_path)
    assert [item.function_id for item in functions] == [
        "app.py::choose",
        "app.py::choose#2",
    ]

def test_sqlite_index_persists_and_reuses_duplicate_local_helper_fixture(tmp_path: Path) -> None:
    # Persist the real benchmark collision pattern and reuse the resulting normalized index.
    project = tmp_path / "project"
    project.mkdir()
    (project / "test_helpers.py").write_text(_duplicate_local_definitions(), encoding="utf-8")
    database = tmp_path / "project-index.sqlite"
    cold = build_index(project, database)
    warm = build_index(project, database)
    loaded = load_index(database)
    cold_ids = [item["function_id"] for item in cold["functions"]]
    assert len(cold_ids) == len(set(cold_ids))
    assert warm["summary"]["reused_files"] == 1
    assert [item["function_id"] for item in loaded["functions"]] == cold_ids
    connection = sqlite3.connect(database)
    try:
        count, distinct_count = connection.execute(
            "SELECT COUNT(*), COUNT(DISTINCT function_id) FROM functions"
        ).fetchone()
    finally:
        connection.close()
    assert count == distinct_count == len(cold_ids)

def test_identity_fix_does_not_mask_conflicts_in_sqlite_writes() -> None:
    # Keep collision prevention in identity generation instead of weakening SQLite integrity.
    visitor_source = inspect.getsource(index_module._DefinitionVisitor)
    writer_source = inspect.getsource(index_module._write_sqlite_index)
    assert "self._scopes" in visitor_source
    assert "scope_path" in visitor_source
    assert "INSERT OR REPLACE INTO functions" not in writer_source
    assert "INSERT OR IGNORE INTO functions" not in writer_source
