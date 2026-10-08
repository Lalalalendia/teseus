from pathlib import Path

from test_intelligence_unified_v1.index import validate_test_nodeids


def test_validate_test_nodeids_drops_stale_values(tmp_path: Path) -> None:
    # Keep an indexed test and reject a deleted or unknown nodeid.
    test_file = tmp_path / "test_sample.py"
    test_file.write_text("def test_sample():\n    pass\n", encoding="utf-8")
    index = {"tests": [{"nodeid": "test_sample.py::test_sample"}]}
    valid, dropped = validate_test_nodeids(
        index,
        tmp_path,
        ("test_sample.py::test_sample", "test_old.py::test_old"),
    )
    assert valid == ("test_sample.py::test_sample",)
    assert dropped == ("test_old.py::test_old",)
