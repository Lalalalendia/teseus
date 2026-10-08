import json
from pathlib import Path

from test_intelligence_unified_v1.maintenance import benchmark


def test_benchmark_emits_all_production_like_workloads(tmp_path: Path) -> None:
    # Verify workload coverage and semantic outputs without imposing machine-specific timing limits.
    project_root = tmp_path / "project"
    project_root.mkdir()
    report = benchmark(project_root, tmp_path / "reports")

    expected = [
        "nodeid_validation",
        "mutation_generation_full",
        "mutation_generation_limited",
        "mutant_prepare_and_compile",
        "line_selection",
        "stats_ingestion",
        "stats_health_summary",
        "index_cold_build",
        "index_warm_build",
        "worker_stats_merge",
        "small_e2e_campaign",
    ]
    assert report["schema_version"] == 2
    assert report["benchmark_version"] == "v1.26"
    assert report["workload_order"] == expected
    assert list(report["workloads"]) == expected
    for name in expected:
        workload = report["workloads"][name]
        assert workload["elapsed_seconds"] >= 0.0
        assert workload["elapsed_ms"] >= 0.0
        assert isinstance(workload["details"], dict)

    assert report["workloads"]["mutation_generation_full"]["details"]["mutants"] == 3
    assert report["workloads"]["mutation_generation_limited"]["details"]["mutants"] == 1
    assert report["workloads"]["stats_ingestion"]["details"] == {
        "event_files": 1,
        "events_ingested": 128,
        "invalid_events": 0,
    }
    assert report["workloads"]["worker_stats_merge"]["details"] == {
        "source_databases": 2,
        "events_ingested": 64,
    }
    assert report["workloads"]["small_e2e_campaign"]["details"] == {
        "status": "complete",
        "mutants": 1,
        "results": 1,
        "mutation_score": 1.0,
    }
    assert "ti-benchmark-" not in json.dumps(report["workloads"], sort_keys=True)


def test_benchmark_fixture_semantics_are_repeatable(tmp_path: Path) -> None:
    # Compare deterministic fixture and result metadata while deliberately ignoring elapsed values.
    project_root = tmp_path / "project"
    project_root.mkdir()
    first = benchmark(project_root, tmp_path / "reports-one")
    second = benchmark(project_root, tmp_path / "reports-two")

    assert first["fixture"] == second["fixture"]
    first_semantics = {name: value["details"] for name, value in first["workloads"].items()}
    second_semantics = {name: value["details"] for name, value in second["workloads"].items()}
    assert first_semantics == second_semantics
