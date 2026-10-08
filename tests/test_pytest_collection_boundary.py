from __future__ import annotations

import configparser
import shlex
from pathlib import Path


def test_embedded_fixture_projects_are_excluded_from_main_collection() -> None:
    # Keep subprocess fixture projects available on disk without collecting them in the main suite.
    root = Path(__file__).parents[1]
    parser = configparser.ConfigParser()
    parser.read(root / "pytest.ini", encoding="utf-8")
    options = shlex.split(parser["pytest"].get("addopts", ""))
    assert "--ignore=tests/fixtures" in options


def test_roadmap_marker_is_registered() -> None:
    # Prevent the roadmap governance suite from emitting unknown-marker warnings.
    root = Path(__file__).parents[1]
    text = (root / "pytest.ini").read_text(encoding="utf-8")
    assert "roadmap: roadmap and milestone contract tests" in text
