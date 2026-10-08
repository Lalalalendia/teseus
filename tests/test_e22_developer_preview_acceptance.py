from __future__ import annotations
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from theseus_local import cli

def _completed_result(root: Path, configuration: object) -> SimpleNamespace:
    # Build one completed durable result for recovery and interruption boundary tests.
    summary = SimpleNamespace(
        counts={"killed": 1},
        total_mutants=1,
        completed_mutants=1,
        report_path=str(root / "canonical.report.json"),
    )
    return SimpleNamespace(
        campaign=SimpleNamespace(
            configuration=configuration,
            status=SimpleNamespace(value="completed"),
            total_mutants=1,
            completed_mutants=1,
        ),
        engine_result=SimpleNamespace(summary=summary),
        database_path=root / "campaign.sqlite3",
        events_path=root / "events.jsonl",
        protocol_path=root / "responses.jsonl",
        succeeded=True,
        error=None,
    )

def test_preview_runs_async_src_layout_in_unicode_path_with_parallel_workers(
    tmp_path: Path,
    capsys,
) -> None:
    # Exercise the first supported product path across src layout, async code, Unicode and parallel workers.
    project = tmp_path / "проект с пробелом"
    source = project / "src" / "app.py"
    source.parent.mkdir(parents=True)
    source_text = (
        "async def choose(value):\n"
        "    if value > 0:\n"
        "        return 1\n"
        "    if value == 0:\n"
        "        return 2\n"
        "    return 0\n"
    )
    source.write_text(source_text, encoding="utf-8")
    tests = project / "tests"
    tests.mkdir()
    (tests / "test_app.py").write_text(
        "import asyncio\n\n"
        "from app import choose\n\n"
        "def test_positive():\n"
        "    assert asyncio.run(choose(1)) == 1\n\n"
        "def test_zero():\n"
        "    assert asyncio.run(choose(0)) == 2\n",
        encoding="utf-8",
    )
    arguments = [
        "run",
        str(project),
        "src/app.py",
        "--function",
        "choose",
        "--operator",
        "condition_to_not",
        "--max-mutants",
        "2",
        "--workers",
        "2",
        "--campaign-id",
        "campaign-preview-src-async",
        "--project-id",
        "project-preview-src-async",
        "--no-escalation",
        "--json",
        "--test-command",
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "-o",
        "pythonpath=src",
        "tests",
    ]
    first_code = cli.main(arguments)
    first = json.loads(capsys.readouterr().out)
    second_code = cli.main(arguments)
    second = json.loads(capsys.readouterr().out)
    assert first_code == second_code == 0
    assert first["succeeded"] is second["succeeded"] is True
    assert first["mutants_discovered"] == first["mutants_executed"] == 2
    assert first["killed"] + first["survived"] + first["invalid"] == 2
    assert second["report_path"] == first["report_path"]
    assert Path(first["report_path"]).is_file()
    assert Path(first["report_path"]).with_suffix(".md").is_file()
    assert source.read_text(encoding="utf-8") == source_text

def test_preview_completes_zero_mutant_campaign(tmp_path: Path, capsys) -> None:
    # Treat an explicitly empty applicable catalog as a successful reportable campaign.
    source = tmp_path / "app.py"
    source_text = "def identity(value):\n    return value\n"
    source.write_text(source_text, encoding="utf-8")
    (tmp_path / "test_app.py").write_text(
        "from app import identity\n\n"
        "def test_identity():\n"
        "    assert identity(3) == 3\n",
        encoding="utf-8",
    )
    code = cli.main(
        [
            "run",
            str(tmp_path),
            "app.py",
            "--function",
            "identity",
            "--operator",
            "eq_to_ne",
            "--campaign-id",
            "campaign-preview-no-mutants",
            "--project-id",
            "project-preview-no-mutants",
            "--no-escalation",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload["succeeded"] is True
    assert payload["mutants_discovered"] == 0
    assert payload["mutants_executed"] == 0
    assert payload["mutation_score"] is None
    assert Path(payload["report_path"]).is_file()
    assert source.read_text(encoding="utf-8") == source_text

def test_preview_reports_baseline_failure_and_preserves_checkout(
    tmp_path: Path,
    capsys,
) -> None:
    # Return one actionable non-zero result when the original project tests already fail.
    source = tmp_path / "app.py"
    source_text = "def choose(value):\n    return value\n"
    source.write_text(source_text, encoding="utf-8")
    (tmp_path / "test_app.py").write_text(
        "from app import choose\n\n"
        "def test_broken_baseline():\n"
        "    assert choose(1) == 2\n",
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
            "return_value_to_none",
            "--max-mutants",
            "1",
            "--campaign-id",
            "campaign-preview-baseline-failure",
            "--project-id",
            "project-preview-baseline-failure",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert code == 1
    assert payload["succeeded"] is False
    assert payload["error"]
    assert payload["report_path"] is None
    assert Path(payload["database_path"]).is_file()
    assert source.read_text(encoding="utf-8") == source_text

def test_preview_ctrl_c_returns_recovery_command(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    # Convert Ctrl+C into exit 130 while preserving the exact durable recovery identity.
    (tmp_path / "app.py").write_text("def choose(value):\n    return value\n", encoding="utf-8")
    class InterruptedCoordinator:
        def run(self, configuration):
            # Interrupt only after the public configuration and durable path are known.
            raise KeyboardInterrupt
        campaign_database_path = staticmethod(cli.LocalCampaignCoordinator.campaign_database_path)
    monkeypatch.setattr(cli, "LocalCampaignCoordinator", InterruptedCoordinator)
    code = cli.main(
        [
            "run",
            str(tmp_path),
            "app.py",
            "--campaign-id",
            "campaign-preview-interrupted",
            "--project-id",
            "project-preview-interrupted",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert code == 130
    assert payload["error_code"] == "campaign_interrupted"
    assert payload["campaign_id"] == "campaign-preview-interrupted"
    assert payload["database_path"].endswith("campaign.sqlite3")
    assert "campaign recover" in payload["recovery_command"]
    assert "campaign-preview-interrupted" in payload["recovery_command"]

def test_campaign_recover_uses_existing_public_authority(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    # Expose restart reconciliation without creating another recovery state machine.
    project = tmp_path / "project"
    project.mkdir()
    (project / "app.py").write_text("def choose(value):\n    return value\n", encoding="utf-8")
    configuration = cli._direct_configuration(
        cli.build_parser().parse_args(
            [
                "run",
                str(project),
                "app.py",
                "--campaign-id",
                "campaign-preview-recover",
                "--project-id",
                "project-preview-recover",
            ]
        )
    )
    result = _completed_result(tmp_path, configuration)
    calls: list[tuple[Path, str]] = []
    class RecoveringCoordinator:
        @classmethod
        def recover_campaign(cls, database_path: Path, campaign_id: str):
            # Capture the exact public recovery call and return one completed replay.
            calls.append((Path(database_path), campaign_id))
            return result
    monkeypatch.setattr(cli, "LocalCampaignCoordinator", RecoveringCoordinator)
    database = tmp_path / "campaign.sqlite3"
    code = cli.main(
        [
            "campaign",
            "recover",
            str(database),
            "--campaign-id",
            "campaign-preview-recover",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert calls == [(database, "campaign-preview-recover")]
    assert payload["campaign_id"] == "campaign-preview-recover"
    assert payload["succeeded"] is True
    assert payload["report_path"] == str(tmp_path / "canonical.report.json")

def test_root_readme_documents_the_preview_and_keeps_ai_optional() -> None:
    # Keep the root documentation aligned with the executable offline product path.
    readme = Path(__file__).resolve().parents[1] / "README.md"
    content = readme.read_text(encoding="utf-8")
    assert "Teseus Developer Preview 0.1" in content
    assert "python.exe -m theseus_local run" in content
    assert "campaign recover" in content
    assert "canonical.report.json" in content
    assert "Ctrl+C" in content
    assert "theseus_survivor_lab" in content
    assert "optional" in content.lower()
