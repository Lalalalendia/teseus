from __future__ import annotations

import json
from pathlib import Path

from theseus_local.coordinator import LocalCampaignCoordinator


def _runner_timeline() -> dict[str, object]:
    # Build one complete nested runner timeline independent of engine-process evidence.
    return {
        "exclusive": True,
        "total_wall_seconds": 5.0,
        "phases": [
            {"phase": "mutation_preparation", "wall_seconds": 0.5},
            {"phase": "mutation_application", "wall_seconds": 0.5},
            {"phase": "pytest_process", "wall_seconds": 3.0},
            {"phase": "source_restoration", "wall_seconds": 0.5},
            {"phase": "runner_unattributed_residual", "wall_seconds": 0.5},
        ],
    }


def test_worker_breakdown_uses_runner_timeline_without_engine_process_artifact() -> None:
    # Preserve actionable runner attribution when the optional engine-process timeline is unavailable.
    payload = LocalCampaignCoordinator._worker_execution_breakdown(
        worker_id="worker-001",
        total_seconds=7.0,
        assignment_dispatch_seconds=0.25,
        process_timeline=None,
        runner_timeline=_runner_timeline(),
    )
    phases = {item["phase"]: item["wall_seconds"] for item in payload["phases"]}
    assert payload["valid_nested_evidence"] is True
    assert payload["accounted_seconds"] == 7.0
    assert payload["accounting_error_seconds"] == 0.0
    assert phases["pytest_process"] == 3.0
    assert phases["durable_delivery_spool_and_runtime"] == 1.75


def test_runner_timeline_loader_falls_back_to_published_canonical_report(tmp_path: Path) -> None:
    # Recover nested runner evidence from the canonical engine copy when the worker-local report path is absent.
    worker_report = tmp_path / "missing-worker" / "engine-run.json"
    canonical_root = tmp_path / "canonical-engine"
    canonical_root.mkdir()
    canonical_report = canonical_root / worker_report.name
    canonical_report.write_text(
        json.dumps({"metrics": {"performance": {"worker_execution_timeline": _runner_timeline()}}}),
        encoding="utf-8",
    )
    timeline = LocalCampaignCoordinator._load_worker_runner_timeline(
        str(worker_report),
        fallback_root=canonical_root,
    )
    assert timeline == _runner_timeline()
