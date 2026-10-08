from __future__ import annotations

import sys
from pathlib import Path


def _replace_once(text: str, old: str, new: str) -> str:
    # Replace exactly one known source block and fail safely on an unexpected file version.
    occurrences = text.count(old)
    if occurrences != 1:
        raise RuntimeError(
            "expected exactly one original E00 fixture block, "
            f"found {occurrences}; file was not changed"
        )
    return text.replace(old, new, 1)


def main() -> int:
    # Update only the E00 fixture enumeration while preserving the rest of the current source file.
    project_root = Path(__file__).resolve().parent
    target = project_root / "tests" / "test_e00_baseline_snapshots.py"
    if not target.is_file():
        print(f"ERROR: file not found: {target}", file=sys.stderr)
        return 2

    raw = target.read_bytes()
    newline = "\r\n" if b"\r\n" in raw else "\n"
    text = raw.decode("utf-8")
    normalized = text.replace("\r\n", "\n")

    old = '''def test_e00_fixture_matrix_is_complete_and_dynamic_collection_is_visible(tmp_path: Path) -> None:
    # Keep all planned v1.26 provenance and collection fixtures present and deterministic.
    snapshot = _load_snapshot()
    fixture_names = sorted(path.name for path in FIXTURES_ROOT.iterdir() if path.is_dir())
'''
    new = '''def test_e00_fixture_matrix_is_complete_and_dynamic_collection_is_visible(tmp_path: Path) -> None:
    # Keep all planned v1.26 executable fixture projects present and deterministic.
    snapshot = _load_snapshot()
    fixture_names = sorted(
        path.name
        for path in FIXTURES_ROOT.iterdir()
        if path.is_dir()
        and (
            (path / "pyproject.toml").is_file()
            or any(path.rglob("*.py"))
        )
    )
'''

    try:
        updated = _replace_once(normalized, old, new)
    except RuntimeError as exc:
        if new in normalized:
            print("OK: E00 fixture scope fix is already installed")
            return 0
        print(f"ERROR: {exc}", file=sys.stderr)
        return 3

    target.write_bytes(updated.replace("\n", newline).encode("utf-8"))
    print(f"UPDATED: {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
