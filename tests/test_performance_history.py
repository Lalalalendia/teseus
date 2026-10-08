from pathlib import Path

from test_intelligence_unified_v1.maintenance import (
    compare_benchmark_runs,
    load_benchmark_history,
    record_benchmark_history,
)


def _benchmark_result(version: str, values: dict[str, float]) -> dict[str, object]:
    # Build a compact synthetic benchmark result with deterministic workload metadata.
    order = list(values)
    return {
        "benchmark_version": version,
        "project_root": "/fixture/project",
        "workload_order": order,
        "fixture": {"source": "app.py", "function": "classify"},
        "workloads": {
            name: {"elapsed_seconds": value, "elapsed_ms": value * 1000.0, "details": {"count": 1}}
            for name, value in values.items()
        },
    }


def test_compare_benchmark_runs_reports_regressions_improvements_and_new_workloads() -> None:
    # Keep comparison semantics independent from machine-specific benchmark execution.
    previous = _benchmark_result("v1.15", {"fast": 1.0, "slow": 0.10})
    current = _benchmark_result("v1.16", {"fast": 1.12, "slow": 0.08, "new": 0.01})

    comparison = compare_benchmark_runs(current, previous)

    assert comparison["previous_available"] is True
    assert round(comparison["workloads"]["fast"]["delta_seconds"], 6) == 0.12
    assert comparison["workloads"]["fast"]["potential_regression"] is True
    assert comparison["workloads"]["slow"]["improved"] is True
    assert comparison["workloads"]["new"]["previous_seconds"] is None
    assert comparison["summary"] == {
        "compared": 2,
        "new_workloads": 1,
        "potential_regressions": 1,
        "improvements": 1,
    }


def test_benchmark_history_is_append_only_and_compares_with_last_record(tmp_path: Path) -> None:
    # Persist successive snapshots without rewriting the earlier JSONL records.
    history_path = tmp_path / "reports" / "benchmark_history.jsonl"
    first = record_benchmark_history(history_path, _benchmark_result("v1.16", {"fast": 1.0}))
    second = record_benchmark_history(history_path, _benchmark_result("v1.16", {"fast": 1.2}))

    rows = load_benchmark_history(history_path, limit=10)

    assert first["recorded"] is True
    assert first["previous_available"] is False
    assert second["previous_available"] is True
    assert second["comparison"]["summary"]["potential_regressions"] == 1
    assert len(rows) == 2
    assert rows[0]["workloads"]["fast"]["elapsed_seconds"] == 1.0
    assert rows[1]["workloads"]["fast"]["elapsed_seconds"] == 1.2
    assert len(history_path.read_text(encoding="utf-8").splitlines()) == 2


def test_benchmark_history_loader_keeps_only_the_requested_tail(tmp_path: Path) -> None:
    # Bound history inspection memory even when many benchmark runs accumulate.
    history_path = tmp_path / "benchmark_history.jsonl"
    for value in range(5):
        record_benchmark_history(history_path, _benchmark_result("v1.16", {"run": float(value)}))

    rows = load_benchmark_history(history_path, limit=2)

    assert len(rows) == 2
    assert [row["workloads"]["run"]["elapsed_seconds"] for row in rows] == [3.0, 4.0]
