from __future__ import annotations
import json
import sys
from pathlib import Path
from types import SimpleNamespace
import pytest
from theseus_local import cli
from theseus_performance.authority import PerformanceStore
from theseus_performance.project_benchmark import ProjectBenchmarkRequest, run_project_benchmark
class FakeCoordinator:
    """Deterministic campaign executor that publishes realistic runner performance evidence."""
    configurations: list[object] = []
    def run(self, configuration):
        # Record lane identity and publish one raw report for the benchmark adapter.
        self.configurations.append(configuration)
        mode = "warm" if configuration.campaign_id.value.endswith("-warm") else "cold"
        workers = configuration.budget.max_workers
        base = 8.0 if mode == "cold" else 4.0
        wall = base / max(1, workers)
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
                            "index_seconds": 1.5 if mode == "cold" else 0.5,
                            "selection_seconds": 0.25,
                            "mutant_generation_seconds": 0.5,
                            "snapshot_seconds": 0.2,
                            "pytest_seconds": wall * 0.6,
                            "test_stats_ingestion_seconds": 0.1,
                            "report_materialization_seconds": 0.05,
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
                        {"mutant": {"mutant_id": "m1"}, "status": "killed"},
                        {"mutant": {"mutant_id": "m2"}, "status": "survived"},
                    ],
                    "integrity": {
                        "one_terminal_evidence_per_mutant": True,
                        "terminal_evidence_count": 2,
                    },
                }
            )
            + "\n",
            encoding="utf-8",
        )
        campaign = SimpleNamespace(
            campaign_id=configuration.campaign_id,
            status=SimpleNamespace(value="completed"),
            total_mutants=2,
            completed_mutants=2,
        )
        engine_result = SimpleNamespace(
            report={"raw_engine_report_path": str(raw_path)},
            summary=SimpleNamespace(report_path=str(canonical_path), total_mutants=2, completed_mutants=2),
        )
        return SimpleNamespace(
            campaign=campaign,
            engine_result=engine_result,
            database_path=report_root / "campaign.sqlite3",
            succeeded=True,
            error=None,
        )
def _request(tmp_path: Path, *, worker_counts: tuple[int, ...] = (1, 2)) -> ProjectBenchmarkRequest:
    # Build one project benchmark request whose state root is outside the checkout.
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
        max_mutants=2,
        worker_counts=worker_counts,
        pin_baselines=True,
    )
def test_real_project_benchmark_persists_cold_warm_worker_matrix(tmp_path: Path) -> None:
    # Persist four comparable runs and summarize warm-state and worker scaling without timing budgets.
    FakeCoordinator.configurations = []
    report = run_project_benchmark(_request(tmp_path), coordinator_factory=FakeCoordinator)
    assert report["completed"] is True
    assert len(report["scenarios"]) == 4
    assert [(item["mode"], item["workers"]) for item in report["scenarios"]] == [
        ("cold", 1),
        ("warm", 1),
        ("cold", 2),
        ("warm", 2),
    ]
    lanes: dict[int, list[object]] = {}
    for configuration in FakeCoordinator.configurations:
        lanes.setdefault(configuration.budget.max_workers, []).append(configuration)
    assert set(lanes) == {1, 2}
    assert all(len(items) == 2 for items in lanes.values())
    assert all(items[0].project.project_id == items[1].project.project_id for items in lanes.values())
    assert lanes[1][0].project.project_id != lanes[2][0].project.project_id
    assert Path(report["report_path"]).is_file()
    history = Path(report["history_database"])
    assert history == _request(tmp_path).output_root / "performance.sqlite3"
    assert history.is_file()
    store = PerformanceStore(history)
    for scenario in report["scenarios"]:
        run = store.get(scenario["run_id"])
        assert run is not None
        assert run.metric_map()["wall_seconds"].status == "observed"
        assert run.metric_map()["cpu_seconds"].status == "unavailable"
        assert run.metric_map()["coordinator_cpu_seconds"].status == "observed"
        assert run.metric_map()["coordinator_peak_memory_bytes"].status == "unavailable"
        cost_metric = run.metric_map()["cost_per_authoritative_mutation_result"]
        assert cost_metric.status == "observed"
        assert cost_metric.value is not None and cost_metric.value > 0.0
        assert scenario["bottleneck_summary"]["dominant_observed_phase"] == "execution"
        assert scenario["metadata"]["authoritative_acceptance"]["status"] == "observed"
        assert store.evaluate(run).status == "observe_only"
    warm = {item["workers"]: item["warm_speedup"] for item in report["comparisons"]["cold_to_warm"]}
    assert set(warm) == {1, 2}
    assert all(value is not None and value > 0.0 for value in warm.values())
    scaling = {(item["mode"], item["workers"]): item["speedup"] for item in report["comparisons"]["worker_scaling"]}
    assert set(scaling) == {("cold", 2), ("warm", 2)}
    assert all(value is not None and value > 0.0 for value in scaling.values())
    assert report["acceptance"]["status"] == "passed"
    assert report["acceptance"]["semantic_results_equal"] is True
    assert report["acceptance"]["integrity_safe"] is True
def test_project_fingerprint_fences_changed_source_across_sessions(tmp_path: Path) -> None:
    # Produce a different comparison identity after one benchmark input file changes.
    FakeCoordinator.configurations = []
    request = _request(tmp_path, worker_counts=(1,))
    first = run_project_benchmark(request, coordinator_factory=FakeCoordinator)
    first_keys = {item["comparison_key"] for item in first["scenarios"]}
    (request.project_root / "app.py").write_text(
        "def choose(value):\n    return 2 if value > 0 else 0\n",
        encoding="utf-8",
    )
    second = run_project_benchmark(request, coordinator_factory=FakeCoordinator)
    second_keys = {item["comparison_key"] for item in second["scenarios"]}
    assert first_keys.isdisjoint(second_keys)
    assert first["history_database"] == second["history_database"]
def test_benchmark_rejects_output_state_inside_checkout(tmp_path: Path) -> None:
    # Keep benchmark artifacts from changing the project fingerprint they are measuring.
    project = tmp_path / "project"
    project.mkdir()
    (project / "app.py").write_text("value = 1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="outside the project checkout"):
        ProjectBenchmarkRequest(
            project_root=project,
            source_path="app.py",
            test_command=(sys.executable, "-m", "pytest", "-q"),
            output_root=project / "benchmarks",
        )
def test_cli_benchmark_builds_request_and_prints_json(tmp_path: Path, monkeypatch, capsys) -> None:
    # Expose one reproducible CLI command while keeping the custom test argv shell-free.
    project = tmp_path / "project"
    project.mkdir()
    (project / "app.py").write_text("def choose(value):\n    return value\n", encoding="utf-8")
    captured: list[ProjectBenchmarkRequest] = []
    def fake_benchmark(request: ProjectBenchmarkRequest) -> dict[str, object]:
        # Capture the public request without starting real worker processes.
        captured.append(request)
        return {
            "session_id": "benchmark-fixture",
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
            "--worker-count",
            "3",
            "--max-mutants",
            "5",
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
    assert request.worker_counts == (3,)
    assert request.max_mutants == 5
    assert request.function == "choose"
    assert request.test_command[-1] == "test_app.py"
    assert request.output_root.parent.name == ".theseus-benchmarks"
