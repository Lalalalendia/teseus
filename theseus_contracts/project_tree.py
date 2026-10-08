"""Canonical project-tree policy shared by benchmark identity and copy workspaces."""
from __future__ import annotations
from pathlib import PurePath

PROJECT_TREE_EXCLUDED_DIRS = frozenset(
    {
        ".git",
        ".hg",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".theseus",
        ".venv",
        "venv",
        "__pycache__",
        "coverage",
        "htmlcov",
        "node_modules",
        "reports",
        "state",
    }
)
PROJECT_TREE_EXCLUDED_DIR_PREFIXES = (".pytest-",)
PROJECT_TREE_EXCLUDED_SUFFIXES = frozenset({".pyc"})


def project_tree_path_is_excluded(relative_path: PurePath) -> bool:
    # Decide whether one project-relative path is generated state excluded from identity and materialization.
    parts = tuple(str(part).casefold() for part in relative_path.parts)
    if any(part in PROJECT_TREE_EXCLUDED_DIRS for part in parts):
        return True
    if any(
        any(part.startswith(prefix) for prefix in PROJECT_TREE_EXCLUDED_DIR_PREFIXES)
        for part in parts
    ):
        return True
    return relative_path.suffix.casefold() in PROJECT_TREE_EXCLUDED_SUFFIXES
