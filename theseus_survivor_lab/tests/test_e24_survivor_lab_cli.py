from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from conftest import FIXTURE_DIR


def _run_cli(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    # Run the package CLI as a real standalone module.
    return subprocess.run(
        [sys.executable, "-m", "theseus_survivor_lab.cli", *args],
        cwd=cwd,
        text=True,
        capture_output=True,
        check=False,
    )


def test_validate_input_cli(tmp_path: Path) -> None:
    # Validate a fixture without touching any Theseus runtime.
    root = Path(__file__).parents[1]
    completed = _run_cli("validate-input", str(FIXTURE_DIR / "real_gap.json"), cwd=root)
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout)["valid"] is True


def test_analyze_and_render_markdown_cli(tmp_path: Path) -> None:
    # Produce JSON and Markdown artifacts through the documented commands.
    root = Path(__file__).parents[1]
    result_path = tmp_path / "result.json"
    markdown_path = tmp_path / "result.md"
    analyzed = _run_cli(
        "analyze",
        str(FIXTURE_DIR / "real_gap.json"),
        "--out",
        str(result_path),
        cwd=root,
    )
    assert analyzed.returncode == 0, analyzed.stderr
    assert result_path.exists()
    rendered = _run_cli(
        "render-markdown",
        str(result_path),
        "--out",
        str(markdown_path),
        cwd=root,
    )
    assert rendered.returncode == 0, rendered.stderr
    assert "Classification" in markdown_path.read_text(encoding="utf-8")


def test_classify_cli_prints_only_summary() -> None:
    # Keep the classify command useful for shell automation.
    root = Path(__file__).parents[1]
    completed = _run_cli("classify", str(FIXTURE_DIR / "timeout.json"), cwd=root)
    assert completed.returncode == 0, completed.stderr
    output = json.loads(completed.stdout)
    assert output["category"] == "timeout_ambiguity"
    assert "classification" not in output
