from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from theseus_local import cli


def _configuration(tmp_path: Path, campaign_id: str = "campaign-cli-contract") -> object:
    # Build a direct CLI configuration without starting a campaign process.
    (tmp_path / "app.py").write_text("def choose(value):\n    return value\n", encoding="utf-8")
    return cli._direct_configuration(
        cli.build_parser().parse_args(
            [
                "run",
                str(tmp_path),
                "app.py",
                "--campaign-id",
                campaign_id,
                "--project-id",
                "project-cli-contract",
            ]
        )
    )


def _result(tmp_path: Path, counts: dict[str, int], *, succeeded: bool = True) -> SimpleNamespace:
    # Create one coordinator-shaped result carrying only the public summary fields the CLI consumes.
    report = tmp_path / "canonical.report.json"
    if succeeded:
        report.write_text("{}\n", encoding="utf-8")
    total = sum(counts.values())
    return SimpleNamespace(
        campaign=SimpleNamespace(
            status=SimpleNamespace(value="completed" if succeeded else "failed"),
            total_mutants=total,
            completed_mutants=total,
        ),
        engine_result=SimpleNamespace(
            summary=SimpleNamespace(
                counts=counts,
                total_mutants=total,
                completed_mutants=total,
                report_path=str(report),
            )
        )
        if succeeded
        else None,
        database_path=tmp_path / "campaign.sqlite3",
        events_path=tmp_path / "events.jsonl",
        protocol_path=tmp_path / "responses.jsonl",
        error=None if succeeded else "worker failure",
        succeeded=succeeded,
    )


@pytest.mark.parametrize(
    "status",
    ("killed", "survived", "invalid", "timeout", "infrastructure_error", "cancelled"),
)
def test_cli_payload_preserves_each_canonical_status(tmp_path: Path, status: str) -> None:
    # Keep every terminal result label visible in the compact CLI projection.
    configuration = _configuration(tmp_path, f"campaign-cli-{status}")
    payload = cli._direct_payload(
        configuration,
        _result(tmp_path, {status: 1}),
        elapsed_seconds=1.25,
    )
    assert payload[status if status not in {"timeout", "infrastructure_error"} else {
        "timeout": "timeouts",
        "infrastructure_error": "infrastructure_failures",
    }[status]] == 1
    assert payload["report_path"].endswith("canonical.report.json")


def test_cli_payload_reports_worker_failure_without_claiming_a_mutation_result(tmp_path: Path) -> None:
    # Never convert a coordinator or worker failure into a survived or killed mutation count.
    configuration = _configuration(tmp_path)
    payload = cli._direct_payload(configuration, _result(tmp_path, {}, succeeded=False), elapsed_seconds=0.2)
    assert payload["succeeded"] is False
    assert payload["infrastructure_failures"] == 0
    assert payload["killed"] == payload["survived"] == payload["invalid"] == 0
    assert payload["error"] == "worker failure"
    assert payload["report_path"] is None


def test_cli_human_summary_includes_cancelled_and_report_path(tmp_path: Path, capsys) -> None:
    # Make the non-JSON output compact but complete enough for a local operator.
    configuration = _configuration(tmp_path)
    payload = cli._direct_payload(
        configuration,
        _result(tmp_path, {"killed": 1, "cancelled": 1}),
        elapsed_seconds=1.0,
    )
    cli._print_direct_summary(payload)
    output = capsys.readouterr().out
    assert "Killed: 1" in output
    assert "Cancelled: 1" in output
    assert "Report:" in output and "canonical.report.json" in output


def test_cli_run_returns_success_and_json_summary_for_no_mutants(tmp_path: Path, monkeypatch, capsys) -> None:
    # Expose the no-mutant success contract without coupling the CLI test to worker timing.
    (tmp_path / "app.py").write_text("def choose(value):\n    return value\n", encoding="utf-8")
    result = _result(tmp_path, {}, succeeded=True)
    monkeypatch.setattr(cli.LocalCampaignCoordinator, "run", lambda self, configuration: result)
    code = cli.main(
        [
            "run",
            str(tmp_path),
            "app.py",
            "--campaign-id",
            "campaign-cli-empty",
            "--project-id",
            "project-cli-empty",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload["succeeded"] is True
    assert payload["mutants_discovered"] == payload["mutants_executed"] == 0
    assert payload["mutation_score"] is None


def test_cli_report_command_does_not_recompute_state(tmp_path: Path, capsys) -> None:
    # Keep report viewing a read-only publication lookup rather than a second execution path.
    report = tmp_path / "canonical.report.json"
    report.write_text('{"campaign_id":"campaign-cli-read"}\n', encoding="utf-8")
    assert cli.main(["campaign", "report", str(report)]) == 0
    assert capsys.readouterr().out == '{"campaign_id":"campaign-cli-read"}\n'
