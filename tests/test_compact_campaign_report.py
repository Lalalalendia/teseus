import json
import sys
from pathlib import Path
from types import SimpleNamespace

from test_intelligence_unified_v1.cli import _load_rerun
from test_intelligence_unified_v1.runner import MutationConfig, MutationRunner


def _write_project(root: Path) -> str:
    # Create a tiny deterministic target with one compile-valid mutation.
    source = "def calculate(value):\n    return value + 1\n"
    (root / "app.py").write_text(source, encoding="utf-8")
    return source


def test_compact_report_keeps_rows_only_in_append_only_journal(tmp_path: Path) -> None:
    # Keep the final JSON bounded while preserving metrics, recovery and every journal row.
    source = _write_project(tmp_path)
    reports = tmp_path / "reports"
    report = MutationRunner(
        MutationConfig(
            project_root=tmp_path,
            source="app.py",
            function="calculate",
            test_command_argv=(sys.executable, "-B", "-c", "from app import calculate; assert calculate(1) == 2"),
            operators=("plus_to_minus",),
            max_mutants=1,
            no_escalation=True,
            use_baseline_cache=False,
            reports_dir=reports,
            compact_report=True,
        )
    ).run()

    report_path = Path(report["report_path"])
    persisted = json.loads(report_path.read_text(encoding="utf-8"))
    journal_path = Path(report["results_journal"])
    journal_rows = [json.loads(line) for line in journal_path.read_text(encoding="utf-8").splitlines()]

    assert report["status"] == "complete"
    assert source == (tmp_path / "app.py").read_text(encoding="utf-8")
    assert persisted["compact_report"] is True
    assert persisted["results"] == []
    assert len(journal_rows) == 1
    assert report["metrics"]["total_mutants"] == 1
    assert report["metrics"]["counts"] == {"killed": 1}
    assert "| Mutant | Status |" not in report_path.with_suffix(".md").read_text(encoding="utf-8")


def test_compact_report_state_keeps_progress_counters(tmp_path: Path) -> None:
    # Keep resume-facing state accurate even though the report does not embed result rows.
    _write_project(tmp_path)
    reports = tmp_path / "reports"
    report = MutationRunner(
        MutationConfig(
            project_root=tmp_path,
            source="app.py",
            function="calculate",
            test_command_argv=(sys.executable, "-c", "from app import calculate; calculate(1)"),
            operators=("plus_to_minus",),
            max_mutants=1,
            no_escalation=True,
            use_baseline_cache=False,
            reports_dir=reports,
            compact_report=True,
        )
    ).run()

    state = json.loads(Path(report["state_path"]).read_text(encoding="utf-8"))

    assert report["status"] == "complete"
    assert report["results"] == []
    assert state["completed_mutants"] == 1
    assert state["counts"] == {"survived": 1}
    _, _, survivor_ids = _load_rerun(SimpleNamespace(rerun_survivors=Path(report["report_path"])))
    assert len(survivor_ids) == 1
