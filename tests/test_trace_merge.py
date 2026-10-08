from pathlib import Path

from test_intelligence_unified_v1.trace import merge_trace_files


def test_trace_merge_unions_worker_lines(tmp_path: Path) -> None:
    # Merge overlapping worker payloads into stable line and function sets.
    first = tmp_path / "trace.gw0.json"
    second = tmp_path / "trace.gw1.json"
    first.write_text(
        '{"schema_version": 1, "files": {"app.py": [1, 3]}, "functions": {"app.py::run": {"rel_path": "app.py", "qualname": "run", "lines": [3]}}}\n',
        encoding="utf-8",
    )
    second.write_text(
        '{"schema_version": 1, "files": {"app.py": [3, 5]}, "functions": {"app.py::run": {"rel_path": "app.py", "qualname": "run", "lines": [5]}}}\n',
        encoding="utf-8",
    )
    merged = merge_trace_files([second, first], tmp_path / "merged.json")
    assert merged["files"] == {"app.py": [1, 3, 5]}
    assert merged["functions"]["app.py::run"]["lines"] == [3, 5]
    assert merged["input_count"] == 2
    assert not merged["invalid_inputs"]

