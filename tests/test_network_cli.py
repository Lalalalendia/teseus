from __future__ import annotations

import json
from pathlib import Path

from theseus_local import cli
from theseus_local.network_security import EnrollmentAuthority


def test_distributed_cli_commands_expose_worker_status_and_remote_run_contract(tmp_path: Path, capsys) -> None:
    registry = tmp_path / "enrollment.json"
    authority = EnrollmentAuthority(registry, "cli-network-secret-0123456789")
    authority.authorize("worker-cli", slots=2)
    assert cli.main(["workers", "--registry", str(registry), "--secret", "cli-network-secret-0123456789", "--json"]) == 0
    workers = json.loads(capsys.readouterr().out)
    assert workers[0]["worker_id"] == "worker-cli"
    assert workers[0]["slots"] == 2

    parsed = cli.build_parser().parse_args(
        [
            "run",
            "--remote",
            "--requests",
            str(tmp_path / "requests.json"),
            "--secret",
            "cli-network-secret-0123456789",
            "--enrollment",
            str(registry),
            "--cache",
            str(tmp_path / "cache"),
            "--state",
            str(tmp_path / "scheduler.json"),
        ]
    )
    assert parsed.remote is True
    assert parsed.requests.endswith("requests.json")


def test_network_status_is_projection_only(tmp_path: Path, capsys) -> None:
    state = tmp_path / "scheduler.json"
    state.write_text(
        '{"schema_version":1,"lease_seconds":60,"runtime_identity":null,"workers":{},"pending":{},"leases":{},"attempts":{},"authoritative":{},"stale":[]}',
        encoding="utf-8",
    )
    assert cli.main(["network", "status", "--state", str(state), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["workers"] == {}
    assert payload["authoritative"] == () or payload["authoritative"] == []
