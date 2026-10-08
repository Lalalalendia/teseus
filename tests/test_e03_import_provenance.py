from __future__ import annotations

import sys
from pathlib import Path

from test_intelligence_unified_v1.runner import MutationConfig, MutationRunner


def test_e03_worker_guard_rejects_production_module_outside_workspace(tmp_path: Path) -> None:
    # Turn a wrong editable-style import into a baseline failure instead of a false mutation result.
    root = tmp_path / "project"
    root.mkdir()
    (root / "app.py").write_text(
        "def choose(value):\n    return value + 1\n",
        encoding="utf-8",
    )
    foreign = tmp_path / "foreign-package"
    foreign.mkdir()
    (foreign / "app.py").write_text(
        "def choose(value):\n    return value + 1\n",
        encoding="utf-8",
    )
    tests_dir = root / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_app.py").write_text(
        "import sys\n"
        f"sys.path.insert(0, {str(foreign)!r})\n"
        "from app import choose\n\n"
        "def test_choose():\n"
        "    assert choose(1) == 2\n",
        encoding="utf-8",
    )

    report = MutationRunner(
        MutationConfig(
            project_root=root,
            source="app.py",
            function="choose",
            test_command_argv=(sys.executable, "-m", "pytest", "tests", "-q"),
            operators=("return_value_to_none",),
            max_mutants=1,
            no_escalation=True,
            use_baseline_cache=False,
            reports_dir=root / "reports",
        )
    ).run()

    assert report["status"] == "baseline_failed"
    assert report["baseline"][0]["passed"] is False
    artifact = root / "reports" / report["baseline"][0]["output_artifact"]
    assert "IMPORT_PROVENANCE_ERROR" in artifact.read_text(encoding="utf-8")


def test_e03_correct_workspace_import_is_not_rejected(tmp_path: Path) -> None:
    # Preserve the normal in-workspace import path while keeping provenance enabled.
    root = tmp_path / "project"
    root.mkdir()
    (root / "app.py").write_text(
        "def choose(value):\n    return value + 1\n",
        encoding="utf-8",
    )
    tests_dir = root / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_app.py").write_text(
        "from app import choose\n\n"
        "def test_choose():\n"
        "    assert choose(1) == 2\n",
        encoding="utf-8",
    )

    report = MutationRunner(
        MutationConfig(
            project_root=root,
            source="app.py",
            function="choose",
            test_command_argv=(sys.executable, "-m", "pytest", "tests", "-q"),
            operators=("return_value_to_none",),
            max_mutants=1,
            no_escalation=True,
            use_baseline_cache=False,
            reports_dir=root / "reports",
        )
    ).run()

    assert report["status"] == "complete"
    assert report["baseline"][0]["passed"] is True
