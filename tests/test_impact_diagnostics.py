import sqlite3

from test_intelligence_unified_v1.impact import V2_SCHEMA_SQL, SQLiteImpactAdapter


def test_normalized_impact_reports_indexed_diagnostics(tmp_path) -> None:
    # Verify line selection returns ranking fields and normalized query metadata.
    database = tmp_path / "impact.sqlite"
    connection = sqlite3.connect(database)
    connection.executescript(V2_SCHEMA_SQL)
    connection.execute(
        "INSERT INTO impact_links(source_path,function_id,line_no,nodeid,executions,kills,median_ms) VALUES (?,?,?,?,?,?,?)",
        ("app.py", "app.py::choose", 3, "tests/test_app.py::test_choose", 10, 2, 4.0),
    )
    connection.commit()
    connection.close()
    adapter = SQLiteImpactAdapter(database)
    rows = adapter.select_tests("app.py", "app.py::choose", 3)
    assert rows[0]["nodeid"] == "tests/test_app.py::test_choose"
    assert adapter.diagnostics["impact_mode"] == "normalized"
    assert adapter.diagnostics["rows_returned"] == 1
