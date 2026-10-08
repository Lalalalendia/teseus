from __future__ import annotations

import json
from pathlib import Path

from theseus_local.coordinator import LocalCampaignCoordinator, _CoordinatorTimeline


def _runner_timeline(prepared: dict[str, object]) -> dict[str, object]:
    # Build one reconciled runner timeline with prepared-invocation sidecar evidence.
    return {
        "exclusive": True,
        "total_wall_seconds": 5.0,
        "phases": [
            {"phase": "pytest_process", "wall_seconds": 3.0},
            {"phase": "prepared_invocation", "wall_seconds": 0.05},
            {"phase": "runner_unattributed_residual", "wall_seconds": 1.95},
        ],
        "prepared_invocation": prepared,
    }


def test_worker_breakdown_carries_prepared_invocation_sidecar() -> None:
    # Preserve prepared-invocation evidence beside exclusive worker accounting.
    prepared = {
        "status": "observed",
        "total_seconds": 0.05,
        "command_builds": 8,
        "process_preparations": 8,
        "seconds_per_process_preparation": 0.00625,
    }
    payload = LocalCampaignCoordinator._worker_execution_breakdown(
        worker_id="worker-pr49",
        total_seconds=6.0,
        assignment_dispatch_seconds=0.1,
        process_timeline={
            "exclusive": True,
            "total_wall_seconds": 5.5,
            "phases": [
                {"phase": "engine_request_execute_shard", "wall_seconds": 5.0},
                {"phase": "engine_session_startup", "wall_seconds": 0.5},
            ],
        },
        runner_timeline=_runner_timeline(prepared),
    )
    assert payload["valid_nested_evidence"] is True
    assert payload["prepared_invocation"] == prepared
    phases = {item["phase"]: item["wall_seconds"] for item in payload["phases"]}
    assert phases["prepared_invocation"] == 0.05


def test_worker_report_publishes_prepared_invocation_measurement_status(tmp_path: Path) -> None:
    # Publish measured cost and reconciliation status so project benchmark can decide the PR49.2 gate.
    observed = {
        "worker_execution_breakdown": {
            "total_wall_seconds": 5.0,
            "accounted_seconds": 5.0,
            "residual_seconds": 0.0,
            "phases": (),
            "prepared_invocation": {
                "status": "observed",
                "total_seconds": 0.04,
                "command_builds": 8,
                "process_preparations": 8,
                "seconds_per_process_preparation": 0.005,
            },
        }
    }
    inconsistent = {
        "worker_execution_breakdown": {
            "total_wall_seconds": 5.0,
            "accounted_seconds": 5.0,
            "residual_seconds": 0.0,
            "phases": (),
            "prepared_invocation": {
                "status": "inconsistent_total",
                "reason": "prepared_invocation_exceeds_runner_residual",
                "total_seconds": 0.06,
                "command_builds": 8,
                "process_preparations": 8,
                "seconds_per_process_preparation": 0.0075,
            },
        }
    }
    timeline = _CoordinatorTimeline("campaign-pr49", clock=lambda: 0.0)
    report_path = tmp_path / "worker-execution.performance.json"
    LocalCampaignCoordinator._publish_worker_execution_report(
        report_path,
        ((observed, None, None), (inconsistent, None, None)),
        timeline,
    )
    diagnostics = timeline.finish(tmp_path / "coordinator.performance.json", status="completed")["diagnostics"]
    persisted = json.loads(report_path.read_text(encoding="utf-8"))
    assert len(persisted["workers"]) == 2
    assert diagnostics["worker_prepared_invocation_measured_sum_seconds"] == 0.1
    assert diagnostics["worker_prepared_invocation_measured_max_seconds"] == 0.06
    assert diagnostics["worker_prepared_invocation_process_preparations_sum"] == 16.0
    assert diagnostics["worker_prepared_invocation_command_builds_sum"] == 16.0
    assert diagnostics["worker_prepared_invocation_observed_workers"] == 1.0
    assert diagnostics["worker_prepared_invocation_inconsistent_workers"] == 1.0
