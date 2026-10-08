"""Exclusive timeline reconciliation shared by performance authorities."""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Mapping


@dataclass(frozen=True, slots=True)
class TimelineAccounting:
    """Canonical reconciliation result for one exclusive wall-clock timeline."""

    total_wall_seconds: float
    known_phase_seconds: float
    residual_seconds: float
    accounted_seconds: float
    accounting_error_seconds: float
    status: str
    reason: str | None = None

    def to_dict(self) -> dict[str, object]:
        # Serialize one reconciliation result without rounding away authority evidence.
        return {
            "total_wall_seconds": self.total_wall_seconds,
            "known_phase_seconds": self.known_phase_seconds,
            "residual_seconds": self.residual_seconds,
            "accounted_seconds": self.accounted_seconds,
            "accounting_error_seconds": self.accounting_error_seconds,
            "status": self.status,
            "reason": self.reason,
        }


def _seconds(value: object, field_name: str) -> float:
    # Normalize one finite non-negative duration used by timeline accounting.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result < 0.0:
        raise ValueError(f"{field_name} must be finite and non-negative")
    return result


def reconcile_exclusive_timeline(
    total_wall_seconds: float,
    phases: Iterable[Mapping[str, object]],
    *,
    residual_phase_names: tuple[str, ...] = ("unattributed", "residual"),
    tolerance_seconds: float = 0.001,
) -> TimelineAccounting:
    # Reconcile one exclusive phase list against its independently measured wall-clock total.
    total = _seconds(total_wall_seconds, "total_wall_seconds")
    tolerance = _seconds(tolerance_seconds, "tolerance_seconds")
    known = 0.0
    explicit_residual = 0.0
    for index, phase in enumerate(phases):
        raw = phase.get("wall_seconds", 0.0)
        seconds = _seconds(raw, f"phases[{index}].wall_seconds")
        name = str(phase.get("phase", "")).casefold()
        if any(token in name for token in residual_phase_names):
            explicit_residual += seconds
        else:
            known += seconds
    if explicit_residual:
        residual = explicit_residual
    else:
        residual = max(0.0, total - known)
    accounted = known + residual
    error = abs(total - accounted)
    if known > total + tolerance:
        return TimelineAccounting(total, known, residual, accounted, error, "failed", "known phases exceed wall-clock total")
    if error > tolerance:
        return TimelineAccounting(total, known, residual, accounted, error, "failed", "exclusive phases do not reconcile")
    return TimelineAccounting(total, known, residual, accounted, error, "passed")


def reconcile_nested_timeline(
    parent_phase_seconds: float,
    child_total_seconds: float,
    *,
    tolerance_seconds: float = 0.001,
) -> dict[str, object]:
    # Classify nested evidence as authoritative only when it fits inside its parent exclusive phase.
    parent = _seconds(parent_phase_seconds, "parent_phase_seconds")
    child = _seconds(child_total_seconds, "child_total_seconds")
    tolerance = _seconds(tolerance_seconds, "tolerance_seconds")
    overflow = round(max(0.0, child - parent), 12)
    authoritative = overflow <= tolerance
    return {
        "status": "authoritative" if authoritative else "diagnostic_only",
        "parent_phase_seconds": parent,
        "child_total_seconds": child,
        "overflow_seconds": overflow,
        "accounting_error_seconds": 0.0 if authoritative else overflow,
        "reason": None if authoritative else "nested timing exceeds parent exclusive phase",
    }


__all__ = ["TimelineAccounting", "reconcile_exclusive_timeline", "reconcile_nested_timeline"]
