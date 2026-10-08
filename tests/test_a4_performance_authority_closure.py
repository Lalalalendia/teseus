from __future__ import annotations

from theseus_performance import reconcile_exclusive_timeline, reconcile_nested_timeline


def test_a4_exclusive_timeline_derives_residual_and_reconciles() -> None:
    # Prove that missing residual evidence is derived from the independent wall-clock authority.
    result = reconcile_exclusive_timeline(
        10.0,
        (
            {"phase": "bootstrap", "wall_seconds": 2.0},
            {"phase": "pytest_process", "wall_seconds": 7.5},
        ),
    )
    assert result.status == "passed"
    assert result.known_phase_seconds == 9.5
    assert result.residual_seconds == 0.5
    assert result.accounted_seconds == 10.0
    assert result.accounting_error_seconds == 0.0


def test_a4_exclusive_timeline_fails_closed_when_phases_exceed_total() -> None:
    # Reject impossible phase accounting instead of masking it with a negative residual.
    result = reconcile_exclusive_timeline(
        3.0,
        (
            {"phase": "collection", "wall_seconds": 2.0},
            {"phase": "tests", "wall_seconds": 2.0},
        ),
    )
    assert result.status == "failed"
    assert result.reason == "known phases exceed wall-clock total"


def test_a4_nested_measurement_becomes_diagnostic_when_it_exceeds_parent() -> None:
    # Keep useful child timing evidence without allowing it to corrupt exclusive accounting.
    result = reconcile_nested_timeline(0.05, 0.06)
    assert result["status"] == "diagnostic_only"
    assert result["overflow_seconds"] == 0.01
