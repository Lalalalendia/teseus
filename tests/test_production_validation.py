import json
import sys
from pathlib import Path

from test_intelligence_unified_v1 import maintenance
from test_intelligence_unified_v1.models import ProcessResult


def _passed_result(argv: list[str], cwd: Path, artifact: Path) -> ProcessResult:
    # Build a successful bounded subprocess result for the validation lane contract.
    return ProcessResult(
        argv=tuple(argv),
        cwd=str(cwd),
        exit_code=0,
        elapsed_seconds=0.01,
        timed_out=False,
        output="",
        output_artifact=str(artifact),
    )


def test_validation_smoke_reports_cardinality_and_timings(tmp_path: Path) -> None:
    # Exercise the real ingestion and summary pipeline with the deterministic smoke profile.
    result = maintenance.run_production_validation(tmp_path, tmp_path / "reports", profile="smoke")

    assert result["passed"] is True
    assert result["observed"]["events_ingested"] == 2_000
    assert result["observed"]["tests"] == 400
    assert result["observed"]["runs"] == 8
    assert result["checks"] == {
        "events_ingested": True,
        "invalid_events_zero": True,
        "test_cardinality": True,
        "run_cardinality": True,
    }
    assert Path(result["result_file"]).exists()
    assert result["timings"]["total_seconds"] >= result["timings"]["ingestion_seconds"]


def test_validation_custom_workload_preserves_worker_sharding(tmp_path: Path) -> None:
    # Validate explicit event/node/run dimensions without relying on machine-specific timing limits.
    result = maintenance.run_production_validation(
        tmp_path,
        tmp_path / "reports",
        profile="smoke",
        event_count=1_200,
        nodeid_count=300,
        run_count=6,
        worker_count=3,
    )

    assert result["passed"] is True
    assert result["workload"] == {"events": 1_200, "nodeids": 300, "runs": 6, "workers": 3}
    assert result["observed"] == {"events_ingested": 1_200, "invalid_events": 0, "tests": 300, "runs": 6}


def test_validation_cli_writes_json_artifact(tmp_path: Path, monkeypatch, capsys) -> None:
    # Keep the public validate command machine-readable and independently addressable.
    from test_intelligence_unified_v1 import cli

    result_file = tmp_path / "reports" / "validation.json"
    code = cli.main(
        [
            "validate",
            str(tmp_path),
            "--profile",
            "smoke",
            "--events",
            "120",
            "--nodeids",
            "20",
            "--runs",
            "3",
            "--workers",
            "1",
            "--result-file",
            str(result_file),
        ]
    )
    value = json.loads(capsys.readouterr().out)

    assert code == 0
    assert value["passed"] is True
    assert value["observed"]["events_ingested"] == 120
    assert json.loads(result_file.read_text(encoding="utf-8"))["validation_version"] == "v1.26"


def test_ci_validation_lane_uses_safe_argv_and_external_artifacts(tmp_path: Path, monkeypatch) -> None:
    # Verify CI injects only explicit argv paths for validation artifacts and never a shell command.
    calls: list[tuple[str, ...]] = []

    def fake_run(argv, *, cwd, timeout_seconds, env, output_artifact, **kwargs):
        # Capture the translated validation subprocess without launching a child process.
        del timeout_seconds, env, kwargs
        calls.append(tuple(argv))
        return _passed_result(list(argv), cwd, output_artifact)

    monkeypatch.setattr(maintenance, "run_argv", fake_run)
    result = maintenance.run_ci_lanes(tmp_path, ("validation",), tmp_path / "reports")

    assert result["passed"] is True
    assert calls[0][0] == sys.executable
    assert calls[0][1:5] == ("-m", "test_intelligence_unified_v1", "validate", ".")
    assert "--profile" in calls[0]
    assert "--reports-dir" in calls[0]
    assert "--result-file" in calls[0]
    assert all(token not in {"cmd", "powershell", "pwsh", "sh", "bash"} for token in calls[0])
