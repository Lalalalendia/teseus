from __future__ import annotations
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from theseus_local import cli
from theseus_performance.project_benchmark import ProjectBenchmarkRequest, run_project_benchmark


class ScaleFakeCoordinator:
    """Deterministic benchmark executor that exposes campaign and shard scale evidence."""
    configurations: list[object] = []

    def run(self, configuration):
        # Record one exact scale lane and publish realistic runner and campaign-plan artifacts.
        self.configurations.append(configuration)
        mode = "warm" if configuration.campaign_id.value.endswith("-warm") else "cold"
        workers = int(configuration.budget.max_workers)
        selected = int(configuration.budget.max_mutants or 6)
        wall = (8.0 if mode == "cold" else 4.0) * selected / max(1, workers)
        report_root = Path(configuration.reports_dir) / configuration.campaign_id.value
        report_root.mkdir(parents=True, exist_ok=True)
        raw_path = report_root / "raw-engine.report.json"
        raw_path.write_text(
            json.dumps(
                {
                    "status": "complete",
                    "metrics": {
                        "performance": {
                            "campaign_wall_seconds": wall,
                            "index_seconds": 1.0 if mode == "cold" else 0.25,
                            "selection_seconds": 0.1,
                            "mutant_generation_seconds": 0.1,
                            "snapshot_seconds": 0.1,
                            "pytest_seconds": wall * 0.8,
                            "test_stats_ingestion_seconds": 0.05,
                            "report_materialization_seconds": 0.05,
                            "processes_started": workers + 1,
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        shard_count = min(workers, selected)
        buckets = [[] for _ in range(shard_count)]
        for index in range(selected):
            buckets[index % shard_count].append(f"m{index:03d}")
        (report_root / "campaign.plan.json").write_text(
            json.dumps(
                {
                    "candidate_count": selected + 2,
                    "eligible_count": selected + 1,
                    "selected_count": selected,
                    "shards": [
                        {
                            "shard_id": f"shard-{index:03d}",
                            "mutant_ids": mutant_ids,
                            "estimated_cost": float(len(mutant_ids)),
                        }
                        for index, mutant_ids in enumerate(buckets)
                    ],
                }
            ),
            encoding="utf-8",
        )
        canonical_path = report_root / "canonical.report.json"
        canonical_path.write_text("{}\n", encoding="utf-8")
        campaign = SimpleNamespace(
            campaign_id=configuration.campaign_id,
            status=SimpleNamespace(value="completed"),
            total_mutants=selected,
            completed_mutants=selected,
        )
        engine_result = SimpleNamespace(
            report={"raw_engine_report_path": str(raw_path)},
            summary=SimpleNamespace(
                report_path=str(canonical_path),
                total_mutants=selected,
                completed_mutants=selected,
            ),
        )
        return SimpleNamespace(
            campaign=campaign,
            engine_result=engine_result,
            database_path=report_root / "campaign.sqlite3",
            succeeded=True,
            error=None,
        )


def _request(
    tmp_path: Path,
    *,
    mutant_counts: tuple[int, ...] = (2, 4),
    worker_counts: tuple[int, ...] = (1, 2),
) -> ProjectBenchmarkRequest:
    # Build one scale benchmark request with deterministic source and test inputs.
    project = tmp_path / "project"
    project.mkdir(exist_ok=True)
    (project / "app.py").write_text(
        "def choose(value):\n    return 1 if value > 0 else 0\n",
        encoding="utf-8",
    )
    (project / "test_app.py").write_text(
        "from app import choose\n\ndef test_choose():\n    assert choose(1) == 1\n",
        encoding="utf-8",
    )
    return ProjectBenchmarkRequest(
        project_root=project,
        source_path="app.py",
        function="choose",
        test_command=(sys.executable, "-m", "pytest", "-q"),
        output_root=tmp_path / "benchmarks",
        operators=("condition_to_not",),
        max_mutants=10,
        mutant_counts=mutant_counts,
        worker_counts=worker_counts,
    )


def test_scale_matrix_isolates_campaign_size_and_worker_lanes(tmp_path: Path) -> None:
    # Execute every size/worker/cold-warm cell without leaking warm state across another scale lane.
    ScaleFakeCoordinator.configurations = []
    report = run_project_benchmark(_request(tmp_path), coordinator_factory=ScaleFakeCoordinator)
    assert report["completed"] is True
    assert report["configuration"]["mutant_counts"] == [2, 4]
    assert report["configuration"]["effective_mutant_counts"] == [2, 4]
    assert [
        (item["max_mutants"], item["workers"], item["mode"])
        for item in report["scenarios"]
    ] == [
        (2, 1, "cold"),
        (2, 1, "warm"),
        (2, 2, "cold"),
        (2, 2, "warm"),
        (4, 1, "cold"),
        (4, 1, "warm"),
        (4, 2, "cold"),
        (4, 2, "warm"),
    ]
    lanes: dict[tuple[int, int], list[object]] = {}
    for configuration in ScaleFakeCoordinator.configurations:
        key = (int(configuration.budget.max_mutants), int(configuration.budget.max_workers))
        lanes.setdefault(key, []).append(configuration)
    assert set(lanes) == {(2, 1), (2, 2), (4, 1), (4, 2)}
    assert all(len(items) == 2 for items in lanes.values())
    assert all(items[0].project.project_id == items[1].project.project_id for items in lanes.values())
    assert len({items[0].project.project_id.value for items in lanes.values()}) == 4


def test_scale_report_publishes_throughput_and_shard_distribution(tmp_path: Path) -> None:
    # Persist measured throughput and immutable planner load diagnostics for every scale scenario.
    ScaleFakeCoordinator.configurations = []
    report = run_project_benchmark(_request(tmp_path), coordinator_factory=ScaleFakeCoordinator)
    for scenario in report["scenarios"]:
        metrics = scenario["metrics"]
        assert metrics["mutants_selected"]["value"] == float(scenario["max_mutants"])
        assert metrics["mutants_completed"]["value"] == float(scenario["max_mutants"])
        assert metrics["mutants_per_second"]["status"] == "observed"
        assert metrics["mutants_per_second"]["value"] > 0.0
        scale = scenario["scale_summary"]
        assert scale["status"] == "observed"
        assert scale["selected_mutants"] == scenario["max_mutants"]
        assert scale["shard_count"] == min(scenario["max_mutants"], scenario["workers"])
        assert sum(scale["shard_mutant_counts"]) == scenario["max_mutants"]
        assert scale["shard_mutant_imbalance_ratio"] == 1.0
        assert scale["shard_cost_imbalance_ratio"] == 1.0
        assert scale["planned_worker_coverage_ratio"] == 1.0


def test_scale_comparisons_expose_parallel_efficiency_and_campaign_growth(tmp_path: Path) -> None:
    # Compare worker and workload growth independently so large-campaign regressions remain attributable.
    ScaleFakeCoordinator.configurations = []
    report = run_project_benchmark(_request(tmp_path), coordinator_factory=ScaleFakeCoordinator)
    comparisons = report["comparisons"]
    assert len(comparisons["cold_to_warm"]) == 4
    assert len(comparisons["worker_scaling"]) == 4
    assert len(comparisons["campaign_scaling"]) == 4
    assert all(item["speedup"] is not None and item["speedup"] > 0.0 for item in comparisons["worker_scaling"])
    assert all(
        item["parallel_efficiency"] is not None and item["parallel_efficiency"] > 0.0
        for item in comparisons["worker_scaling"]
    )
    assert all(item["requested_work_growth"] == 2.0 for item in comparisons["campaign_scaling"])
    assert all(item["actual_work_growth"] == 2.0 for item in comparisons["campaign_scaling"])
    assert all(item["wall_growth"] is not None and item["wall_growth"] > 0.0 for item in comparisons["campaign_scaling"])
    assert all(
        item["throughput_ratio"] is not None and item["throughput_ratio"] > 0.0
        for item in comparisons["campaign_scaling"]
    )


def test_legacy_max_mutants_remains_one_scale_lane(tmp_path: Path) -> None:
    # Preserve the pre-PR48 benchmark shape when no explicit mutant-count matrix is requested.
    ScaleFakeCoordinator.configurations = []
    request = _request(tmp_path, mutant_counts=(), worker_counts=(1,))
    report = run_project_benchmark(request, coordinator_factory=ScaleFakeCoordinator)
    assert report["configuration"]["max_mutants"] == 10
    assert report["configuration"]["mutant_counts"] == []
    assert report["configuration"]["effective_mutant_counts"] == [10]
    assert [(item["max_mutants"], item["mode"]) for item in report["scenarios"]] == [
        (10, "cold"),
        (10, "warm"),
    ]


def test_cli_accepts_repeatable_mutant_count_scale_matrix(tmp_path: Path, monkeypatch, capsys) -> None:
    # Map repeatable campaign-size and worker-count flags into one public benchmark request.
    project = tmp_path / "project"
    project.mkdir()
    (project / "app.py").write_text("def choose(value):\n    return value\n", encoding="utf-8")
    captured: list[ProjectBenchmarkRequest] = []

    def fake_benchmark(request: ProjectBenchmarkRequest) -> dict[str, object]:
        # Capture the scale request without launching worker processes.
        captured.append(request)
        return {
            "session_id": "benchmark-pr48-fixture",
            "completed": True,
            "scenarios": [],
            "comparisons": {},
            "report_path": str(tmp_path / "report.json"),
            "history_database": str(tmp_path / "performance.sqlite3"),
        }

    monkeypatch.setattr(cli, "run_project_benchmark", fake_benchmark)
    code = cli.main(
        [
            "benchmark",
            str(project),
            "app.py",
            "--function",
            "choose",
            "--mutant-count",
            "2",
            "--mutant-count",
            "8",
            "--worker-count",
            "1",
            "--worker-count",
            "4",
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
    assert payload["completed"] is True
    assert len(captured) == 1
    request = captured[0]
    assert request.mutant_counts == (2, 8)
    assert request.worker_counts == (1, 4)
    assert request.max_mutants == 10
