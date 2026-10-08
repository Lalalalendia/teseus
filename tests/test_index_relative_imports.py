from pathlib import Path

from test_intelligence_unified_v1.index import build_index


def test_index_handles_relative_import_without_module_name(tmp_path: Path) -> None:
    # ``from . import name`` must reach alias fallback without constructing Path('.') with a suffix.
    package = tmp_path / "pkg"
    package.mkdir()
    (package / "__init__.py").write_text("from . import helper\n", encoding="utf-8")
    (package / "helper.py").write_text("VALUE = 1\n", encoding="utf-8")
    index = build_index(tmp_path, tmp_path / "index.sqlite")

    entry = index["files"]["pkg/__init__.py"]
    assert entry["dependencies"] == ["pkg/helper.py"]
