from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from theseus_performance import (
    InvariantFinding,
    PerformanceCollector,
    PerformanceStore,
    WorkloadIdentity,
)


def _identity(*, input_fingerprint: str = "input-a") -> WorkloadIdentity:
    # Bind governance tests to the same exact project/environment/runtime epoch dimensions as production.
    return WorkloadIdentity(
        workload_name="optimization-campaign",
        workload_version="optimization-v1",
        project_key="project-a",
        input_fingerprint=input_fingerprint,
        environment_fingerprint="environment-a",
        runtime_fingerprint="runtime-a",
    )


def _run(run_id: str, wall_seconds: float, *, input_fingerprint: str = "input-a"):
    # Produce one measured run with a single authoritative wall-clock metric.
    collector = PerformanceCollector(_identity(input_fingerprint=input_fingerprint), run_id=run_id)
    collector.observe("wall_seconds", wall_seconds, unit="seconds", source="governance-fixture")
    return collector.finish()


def test_pr84_exact_identity_creates_separate_fingerprint_epochs() -> None:
    # A changed input/test-command fingerprint must never reuse an older performance baseline.
    first = _run("epoch-a", 1.0)
    changed_input = _run("epoch-b", 1.0, input_fingerprint="test-command-b")
    changed_runtime = replace(first, run_id="epoch-c", identity=replace(first.identity, runtime_fingerprint="runtime-b"))
    assert first.identity.comparison_key != changed_input.identity.comparison_key
    assert first.identity.comparison_key != changed_runtime.identity.comparison_key


def test_pr84_stable_regression_blocks_but_small_drift_warns(tmp_path: Path) -> None:
    # A configured budget may be broad, but a stable regression above the explicit 10% governance line still blocks.
    store = PerformanceStore(tmp_path / "performance.sqlite3")
    baseline = _run("baseline", 1.0)
    stable_regression = _run("stable-regression", 1.15)
    store.append(baseline)
    store.append(stable_regression)
    store.pin_baseline(baseline.run_id)
    from theseus_performance import RegressionBudget

    store.set_budget(
        baseline.identity.comparison_key,
        RegressionBudget("wall_seconds", max_relative_regression=0.50),
    )
    blocked = store.evaluate_gate(stable_regression)
    assert blocked.performance.status == "passed"
    assert blocked.status == "blocked"
    assert blocked.blocking is True

    small_drift = _run("small-drift", 1.05)
    store.append(small_drift)
    warning = store.evaluate_gate(small_drift)
    assert warning.performance.status == "passed"
    assert warning.status == "warning"
    assert warning.blocking is False


def test_pr84_hard_invariant_blocks_without_performance_baseline(tmp_path: Path) -> None:
    # Correctness/integrity violations are immediate blockers even before a performance epoch is pinned.
    store = PerformanceStore(tmp_path / "performance.sqlite3")
    current = _run("current", 1.0)
    store.append(current)
    gate = store.evaluate_gate(
        current,
        invariants=(
            InvariantFinding(
                "canonical_integrity",
                "failed",
                reason="terminal evidence count changed",
                evidence={"expected": 2, "actual": 1},
            ),
        ),
    )
    assert gate.performance.status == "no_baseline"
    assert gate.status == "blocked"
    assert gate.to_dict()["invariants"][0]["blocking"] is True


def test_pr84_missing_invariant_evidence_is_visible_as_warning(tmp_path: Path) -> None:
    # Unavailable optional dimensions warn explicitly and never masquerade as a successful hard invariant.
    store = PerformanceStore(tmp_path / "performance.sqlite3")
    current = _run("current", 1.0)
    store.append(current)
    gate = store.evaluate_gate(
        current,
        invariants=(InvariantFinding("network_transfer", "insufficient_evidence"),),
    )
    assert gate.status == "warning"
    assert gate.blocking is False
