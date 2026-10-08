from __future__ import annotations
import inspect
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from theseus_local import cli

def _result(root: Path, *, counts: dict[str, int] | None = None) -> SimpleNamespace:
    # Build one completed coordinator result for direct CLI contract tests.
    summary = SimpleNamespace(
        counts=counts or {"killed": 2, "survived": 1},
        total_mutants=3,
        completed_mutants=3,
        report_path=str(root / "canonical.report.json"),
    )
    return SimpleNamespace(
        campaign=SimpleNamespace(
            status=SimpleNamespace(value="completed"),
            total_mutants=3,
            completed_mutants=3,
            to_dict=lambda: {"status": "completed"},
        ),
        engine_result=SimpleNamespace(summary=summary, to_dict=lambda: {"summary": {"status": "complete"}}),
        database_path=root / "campaign.sqlite3",
        events_path=root / "events.jsonl",
        protocol_path=root / "responses.jsonl",
        succeeded=True,
        error=None,
    )

def test_direct_run_builds_public_configuration_and_prints_json(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    # Run directly from project arguments without creating an intermediate configuration file.
    source = tmp_path / "app.py"
    source.write_text("def choose(value):\n    return value\n", encoding="utf-8")
    captured = []
    result = _result(tmp_path)
    class FakeCoordinator:
        def run(self, configuration):
            # Capture the exact public contract passed to the existing coordinator.
            captured.append(configuration)
            return result
    monkeypatch.setattr(cli, "LocalCampaignCoordinator", FakeCoordinator)
    code = cli.main(
        [
            "run",
            str(tmp_path),
            "app.py",
            "--function",
            "choose",
            "--operator",
            "condition_to_not",
            "--max-mutants",
            "3",
            "--workers",
            "2",
            "--campaign-id",
            "campaign-preview",
            "--project-id",
            "project-preview",
            "--json",
            "--test-command",
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "test_app.py",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert len(captured) == 1
    configuration = captured[0]
    assert configuration.campaign_id.value == "campaign-preview"
    assert configuration.project.project_id.value == "project-preview"
    assert configuration.project.root_path == str(tmp_path.resolve())
    assert configuration.project.test_command.argv == (
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "test_app.py",
    )
    assert configuration.project.test_command.cwd == str(tmp_path.resolve())
    assert configuration.scope.source_path == "app.py"
    assert configuration.scope.function == "choose"
    assert configuration.scope.operators == ("condition_to_not",)
    assert configuration.budget.max_mutants == 3
    assert configuration.budget.max_workers == 2
    assert payload["campaign_id"] == "campaign-preview"
    assert payload["mutants_discovered"] == 3
    assert payload["mutants_executed"] == 3
    assert payload["killed"] == 2
    assert payload["survived"] == 1
    assert payload["mutation_score"] == 0.666667
    assert payload["report_path"] == str(tmp_path / "canonical.report.json")

def test_direct_run_uses_current_python_pytest_default(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    # Provide a useful default test command for an activated project virtual environment.
    (tmp_path / "app.py").write_text("def choose(value):\n    return value\n", encoding="utf-8")
    captured = []
    class FakeCoordinator:
        def run(self, configuration):
            # Capture the default command without starting a real child process.
            captured.append(configuration)
            return _result(tmp_path, counts={"invalid": 1, "timeout": 1, "infrastructure_error": 1})
    monkeypatch.setattr(cli, "LocalCampaignCoordinator", FakeCoordinator)
    code = cli.main(["run", str(tmp_path), "app.py"])
    output = capsys.readouterr().out
    assert code == 0
    assert captured[0].project.test_command.argv == (sys.executable, "-m", "pytest", "-q")
    assert "Mutants discovered: 3" in output
    assert "Invalid: 1" in output
    assert "Timeouts: 1" in output
    assert "Infrastructure failures: 1" in output
    assert f"Report: {tmp_path / 'canonical.report.json'}" in output

def test_direct_run_rejects_source_outside_project(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    # Reject traversal before workspace preparation or coordinator execution.
    project = tmp_path / "project"
    project.mkdir()
    outside = tmp_path / "outside.py"
    outside.write_text("value = 1\n", encoding="utf-8")
    class ForbiddenCoordinator:
        def run(self, configuration):
            # Fail if invalid input reaches the mutation coordinator.
            raise AssertionError(f"unexpected configuration: {configuration}")
    monkeypatch.setattr(cli, "LocalCampaignCoordinator", ForbiddenCoordinator)
    code = cli.main(["run", str(project), str(outside)])
    assert code == 2
    assert "source path must remain inside the project root" in capsys.readouterr().out

def test_direct_run_reuses_existing_coordinator_and_canonical_report() -> None:
    # Keep the preview command as a thin adapter instead of creating another mutation engine.
    source = inspect.getsource(cli)
    direct = inspect.getsource(cli._direct_run)
    payload = inspect.getsource(cli._direct_payload)
    assert "MutationRunner" not in source
    assert "LocalCampaignCoordinator().run(configuration)" in direct
    assert "summary.report_path" in payload
    assert "canonical.report" not in direct

def test_direct_run_executes_real_campaign_and_preserves_checkout(
    tmp_path: Path,
    capsys,
) -> None:
    # Cross the real CLI, coordinator, worker, mutation, pytest and canonical report boundaries once.
    source = tmp_path / "app.py"
    source_text = "def choose(value):\n    if value > 0:\n        return 1\n    return 0\n"
    source.write_text(source_text, encoding="utf-8")
    (tmp_path / "test_app.py").write_text(
        "from app import choose\n\n"
        "def test_positive():\n"
        "    assert choose(1) == 1\n",
        encoding="utf-8",
    )
    code = cli.main(
        [
            "run",
            str(tmp_path),
            "app.py",
            "--function",
            "choose",
            "--operator",
            "condition_to_not",
            "--max-mutants",
            "1",
            "--workers",
            "1",
            "--campaign-id",
            "campaign-preview-e2e",
            "--project-id",
            "project-preview-e2e",
            "--no-escalation",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload["succeeded"] is True
    assert payload["mutants_discovered"] == 1
    assert payload["mutants_executed"] == 1
    assert payload["killed"] + payload["survived"] + payload["invalid"] == 1
    report_path = Path(payload["report_path"])
    assert report_path.is_file()
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["campaign"]["durable_state"] == "completed"
    assert report["results"][0]["duration_seconds"] > 0.0
    assert report["performance"]["execution_seconds"] > 0.0
    assert report["performance"]["average_execution_seconds"] > 0.0
    assert source.read_text(encoding="utf-8") == source_text
