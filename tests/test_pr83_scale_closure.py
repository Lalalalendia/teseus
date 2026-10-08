from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

from theseus_performance.project_benchmark import ProjectBenchmarkRequest, run_project_benchmark


class ScaleMatrixCoordinator:
    """Fast deterministic stand-in for the full coordinator matrix acceptance test."""

    def run(self, configuration):
        # Publish the same authoritative semantic mapping for every lane and vary only physical cost.
        report_root = Path(configuration.reports_dir) / configuration.campaign_id.value
        report_root.mkdir(parents=True, exist_ok=True)
        count = int(configuration.budget.max_mutants or 1)
        workers = int(configuration.budget.max_workers)
        wall = max(1.0, count / max(1, workers))
        raw_path = report_root / "raw-engine.report.json"
        raw_path.write_text(
            json.dumps(
                {
                    "status": "complete",
                    "metrics": {
                        "performance": {
                            "campaign_wall_seconds": wall,
                            "index_seconds": 0.1,
                            "selection_seconds": 0.1,
                            "mutant_generation_seconds": 0.1,
                            "snapshot_seconds": 0.1,
                            "pytest_seconds": wall,
                            "test_stats_ingestion_seconds": 0.1,
                            "report_materialization_seconds": 0.1,
                            "processes_started": workers + 1,
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        canonical_path = report_root / "canonical.report.json"
        canonical_path.write_text(
            json.dumps(
                {
                    "source": "gallifrey_authoritative",
                    "results": [
                        {
                            "mutant": {"mutant_id": f"m{index}"},
                            "status": "killed" if index % 2 else "survived",
                        }
                        for index in range(count)
                    ],
                    "integrity": {
                        "one_terminal_evidence_per_mutant": True,
                        "terminal_evidence_count": count,
                    },
                }
            ),
            encoding="utf-8",
        )
        campaign = SimpleNamespace(
            campaign_id=configuration.campaign_id,
            status=SimpleNamespace(value="completed"),
            total_mutants=count,
            completed_mutants=count,
        )
        summary = SimpleNamespace(
            report_path=str(canonical_path),
            total_mutants=count,
            completed_mutants=count,
        )
        engine_result = SimpleNamespace(
            report={"raw_engine_report_path": str(raw_path)},
            summary=summary,
        )
        return SimpleNamespace(
            campaign=campaign,
            engine_result=engine_result,
            database_path=report_root / "campaign.sqlite3",
            succeeded=True,
            error=None,
        )


def test_pr83_scale_matrix_covers_campaign_growth_and_worker_width(tmp_path: Path) -> None:
    # Exercise the full matrix shape cheaply while retaining canonical semantic and integrity evidence.
    project = tmp_path / "project"
    project.mkdir()
    (project / "app.py").write_text("def choose(value):\n    return value\n", encoding="utf-8")
    request = ProjectBenchmarkRequest(
        project_root=project,
        source_path="app.py",
        test_command=(sys.executable, "-m", "pytest", "-q"),
        output_root=tmp_path / "benchmarks",
        mutant_counts=(500, 24, 100),
        worker_counts=(8, 1, 4, 2),
        max_mutants=24,
        repetitions=1,
    )
    report = run_project_benchmark(request, coordinator_factory=ScaleMatrixCoordinator)
    assert report["completed"] is True
    assert report["configuration"]["effective_mutant_counts"] == [24, 100, 500]
    assert report["configuration"]["worker_counts"] == [1, 2, 4, 8]
    assert len(report["scenarios"]) == 3 * 4 * 2
    assert report["acceptance"]["status"] == "passed"
    assert report["acceptance"]["semantic_results_equal"] is True
    assert report["acceptance"]["integrity_safe"] is True
    assert report["scale_health"]["status"] == "observed"
    assert report["scale_health"]["superlinear_observation_count"] == 0
    assert all(
        item["metrics"]["cost_per_authoritative_mutation_result"]["status"] == "observed"
        for item in report["scenarios"]
    )
    assert len(report["comparisons"]["worker_scaling"]) == 3 * 2 * 3
    assert len(report["comparisons"]["campaign_scaling"]) == 2 * 4 * 2
