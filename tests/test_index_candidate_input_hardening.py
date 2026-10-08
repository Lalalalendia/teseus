from __future__ import annotations

from pathlib import Path

from test_intelligence_unified_v1.index import build_index


def test_index_ignores_non_path_string_constants_with_embedded_nul(tmp_path: Path) -> None:
    # Keep arbitrary string literals from reaching pathlib as candidate filesystem paths.
    (tmp_path / "app.py").write_text('SENTINEL = "\\x00"\nVALUE = 1\n', encoding="utf-8")
    index = build_index(tmp_path, tmp_path / "index.sqlite")
    assert "app.py" in index["files"]


def test_index_ignores_candidate_string_that_exceeds_filesystem_name_limits(tmp_path: Path) -> None:
    # Treat oversized literals as ordinary program data rather than fatal dependency paths.
    (tmp_path / "app.py").write_text(f'PAYLOAD = {"x" * 10000!r}\n', encoding="utf-8")
    index = build_index(tmp_path, tmp_path / "index.sqlite")
    assert "app.py" in index["files"]
