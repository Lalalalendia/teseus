from __future__ import annotations
import inspect
from theseus_local.coordinator import LocalCampaignCoordinator


def test_exact_reuse_delivery_does_not_require_a_runner_report() -> None:
    # Keep measurement compatible with worker deliveries that intentionally carry no fresh shard result.
    source = inspect.getsource(LocalCampaignCoordinator._execute_parallel_shards)
    assert "observed_shard_result = delivery_payload.shard_result" in source
    assert "observed_shard_result.report_path" in source
    assert "if observed_shard_result is not None" in source
    assert "runner_timeline=self._load_worker_runner_timeline(" in source
    assert "runner_report_path," in source
    assert "fallback_root=canonical_engine_root" in source
    assert "return spec, observed_shard_result, None" in source


def test_exact_reuse_worker_breakdown_reconciles_without_runner_timeline() -> None:
    # Attribute engine setup and leave the remainder explicit when exact reuse runs no mutation runner.
    payload = LocalCampaignCoordinator._worker_execution_breakdown(
        worker_id="worker-reuse",
        total_seconds=2.0,
        assignment_dispatch_seconds=0.1,
        process_timeline={
            "exclusive": True,
            "total_wall_seconds": 1.5,
            "phases": [
                {"phase": "engine_session_startup", "wall_seconds": 0.4},
                {"phase": "engine_request_prepare", "wall_seconds": 0.8},
                {"phase": "engine_process_unattributed_residual", "wall_seconds": 0.3},
            ],
        },
        runner_timeline=None,
    )
    phases = {item["phase"]: item["wall_seconds"] for item in payload["phases"]}
    assert payload["valid_nested_evidence"] is True
    assert payload["accounted_seconds"] == 2.0
    assert payload["accounting_error_seconds"] == 0.0
    assert phases["engine_session_startup"] == 0.4
    assert phases["engine_request_prepare"] == 0.8
    assert phases["durable_delivery_spool_and_runtime"] == 0.7
