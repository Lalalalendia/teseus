import json
import sqlite3
from pathlib import Path
from unittest.mock import patch
from test_intelligence_unified_v1 import index as index_module
from test_intelligence_unified_v1.index import build_index, load_index
def test_sqlite_index_v3_uses_normalized_rows_without_duplicate_payloads(tmp_path: Path) -> None:
    # Keep the public index contract while making normalized SQLite rows the persisted source of truth.
    (tmp_path / "app.py").write_text("def choose(value):\n    return value\n", encoding="utf-8")
    (tmp_path / "test_app.py").write_text("def test_choose():\n    pass\n", encoding="utf-8")
    database = tmp_path / "index.sqlite"
    built = build_index(tmp_path, database)
    loaded = load_index(database)
    assert built["schema_version"] == 3
    assert loaded["schema_version"] == 3
    assert loaded["index_version"] == built["index_version"]
    assert loaded["files"] == built["files"]
    assert loaded["functions"] == built["functions"]
    assert loaded["tests"] == built["tests"]
    with sqlite3.connect(database) as connection:
        metadata = {row[0]: row[1] for row in connection.execute("SELECT key, value FROM metadata")}
        file_columns = {row[1] for row in connection.execute("PRAGMA table_info(files)")}
    assert metadata["schema_version"] == "3"
    assert "payload" not in metadata
    assert "functions_json" not in file_columns
    assert "tests_json" not in file_columns
def test_sqlite_index_v3_reparses_changed_files_and_deletes_removed_rows(tmp_path: Path) -> None:
    # Reuse unchanged rows and update only the changed or deleted file partitions.
    (tmp_path / "app.py").write_text("def choose(value):\n    return value\n", encoding="utf-8")
    (tmp_path / "test_app.py").write_text("def test_choose():\n    pass\n", encoding="utf-8")
    (tmp_path / "helper.py").write_text("def helper():\n    return True\n", encoding="utf-8")
    database = tmp_path / "index.sqlite"
    build_index(tmp_path, database)
    with sqlite3.connect(database) as connection:
        unchanged_rowid = connection.execute(
            "SELECT rowid FROM functions WHERE function_id = ?",
            ("test_app.py::test_choose",),
        ).fetchone()[0]
    (tmp_path / "app.py").write_text(
        "def choose(value):\n    if value:\n        return value\n    return None\n",
        encoding="utf-8",
    )
    with patch.object(index_module, "parse_python_file", wraps=index_module.parse_python_file) as parser:
        second = build_index(tmp_path, database)
    assert [call.args[0].name for call in parser.call_args_list] == ["app.py"]
    assert second["summary"]["reused_files"] == 2
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT rowid FROM functions WHERE function_id = ?",
            ("test_app.py::test_choose",),
        ).fetchone()[0] == unchanged_rowid
    (tmp_path / "helper.py").unlink()
    third = build_index(tmp_path, database)
    assert "helper.py" not in third["files"]
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT 1 FROM files WHERE rel_path = ?", ("helper.py",)
        ).fetchone() is None
def test_build_migrates_legacy_v2_payload_to_normalized_v3(tmp_path: Path) -> None:
    # Read the old payload format once, then replace it with the incremental v3 schema.
    (tmp_path / "app.py").write_text("def choose(value):\n    return value\n", encoding="utf-8")
    database = tmp_path / "index.sqlite"
    legacy = {
        "schema_version": 2,
        "index_version": "legacy",
        "created_at": "2026-01-01T00:00:00+00:00",
        "project_root": str(tmp_path.resolve()),
        "files": {},
        "functions": [],
        "tests": [],
        "parse_errors": [],
        "summary": {"files": 0, "functions": 0, "tests": 0, "parse_errors": 0, "reused_files": 0},
    }
    connection = sqlite3.connect(database)
    try:
        connection.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        connection.executemany(
            "INSERT INTO metadata(key, value) VALUES (?, ?)",
            [("schema_version", "2"), ("payload", json.dumps(legacy))],
        )
        connection.commit()
    finally:
        connection.close()
    migrated = build_index(tmp_path, database)
    assert migrated["schema_version"] == 3
    with sqlite3.connect(database) as connection:
        metadata = {row[0]: row[1] for row in connection.execute("SELECT key, value FROM metadata")}
    assert metadata["schema_version"] == "3"
    assert "payload" not in metadata