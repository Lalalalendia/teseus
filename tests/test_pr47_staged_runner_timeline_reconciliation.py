from __future__ import annotations

from theseus_local.coordinator import LocalCampaignCoordinator


def test_staged_runner_cumulative_preparation_does_not_invalidate_worker_breakdown() -> None:
    # Reconcile execute-shard deltas when the staged runner report carries cumulative preparation counters.
    payload = LocalCampaignCoordinator._worker_execution_breakdown(
        worker_id="worker-001",
        total_seconds=11.0,
        assignment_dispatch_seconds=0.25,
        process_timeline={
            "exclusive": True,
            "total_wall_seconds": 10.5,
            "phases": [
                {"phase": "engine_session_startup", "wall_seconds": 0.5},
                {"phase": "engine_request_prepare", "wall_seconds": 3.0},
                {"phase": "engine_request_execute_shard", "wall_seconds": 6.0},
                {"phase": "engine_process_unattributed_residual", "wall_seconds": 1.0},
            ],
        },
        runner_timeline={
            "exclusive": True,
            "total_wall_seconds": 0.0,
            "phases": [
                {"phase": "runner_index", "wall_seconds": 1.0},
                {"phase": "runner_selection", "wall_seconds": 1.0},
                {"phase": "runner_snapshot", "wall_seconds": 0.5},
                {"phase": "runner_mutant_generation", "wall_seconds": 0.5},
                {"phase": "mutation_preparation", "wall_seconds": 0.4},
                {"phase": "mutation_application", "wall_seconds": 0.3},
                {"phase": "pytest_process", "wall_seconds": 4.0},
                {"phase": "pytest_wrapper", "wall_seconds": 0.1},
                {"phase": "test_stats_ingestion", "wall_seconds": 0.3},
                {"phase": "source_restoration", "wall_seconds": 0.2},
                {"phase": "report_publication", "wall_seconds": 0.2},
                {"phase": "runner_unattributed_residual", "wall_seconds": 0.0},
            ],
        },
    )

    phases = {item["phase"]: item["wall_seconds"] for item in payload["phases"]}
    assert payload["valid_nested_evidence"] is True
    assert payload["accounted_seconds"] == 11.0
    assert payload["accounting_error_seconds"] == 0.0
    assert phases["engine_request_prepare"] == 3.0
    assert phases["pytest_process"] == 4.0
    assert phases["engine_execute_shard_protocol"] == 0.5
    assert phases["durable_delivery_spool_and_runtime"] == 1.25
    assert "runner_index" not in phases
    assert "runner_selection" not in phases
    assert "runner_snapshot" not in phases
    assert "runner_mutant_generation" not in phases
    assert "runner_unattributed_residual" not in phases
