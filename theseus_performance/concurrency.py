"""Deterministic AUTO-concurrency policy for local mutation campaigns."""
from __future__ import annotations

import os
from dataclasses import dataclass
from statistics import median
from typing import Iterable


@dataclass(frozen=True, slots=True)
class ConcurrencySample:
    """One observed project throughput sample for a concrete worker count."""

    workers: int
    mutants_per_second: float
    wall_seconds: float
    completed_mutants: int

    def __post_init__(self) -> None:
        # Reject malformed performance history before it can influence scheduling.
        if isinstance(self.workers, bool) or self.workers < 1:
            raise ValueError("workers must be a positive integer")
        if self.mutants_per_second < 0.0 or self.wall_seconds <= 0.0 or self.completed_mutants < 0:
            raise ValueError("concurrency sample values are invalid")


@dataclass(frozen=True, slots=True)
class AutoConcurrencyDecision:
    """Explain one AUTO worker-count choice."""

    workers: int
    source: str
    cpu_count: int
    candidate_ceiling: int
    historical_samples: int

    def to_dict(self) -> dict[str, object]:
        # Serialize one worker decision for diagnostics and public summaries.
        return {
            "workers": self.workers,
            "source": self.source,
            "cpu_count": self.cpu_count,
            "candidate_ceiling": self.candidate_ceiling,
            "historical_samples": self.historical_samples,
        }


def choose_auto_workers(
    *,
    cpu_count: int | None = None,
    memory_available_bytes: int | None = None,
    preferred_workers: int | None = None,
    workload_size: int | None = None,
    history: Iterable[ConcurrencySample] = (),
) -> AutoConcurrencyDecision:
    # Choose throughput-oriented local concurrency while preferring fewer workers on near-ties.
    detected_cpu = max(1, int(cpu_count or os.cpu_count() or 1))
    ceiling = min(detected_cpu, 16)
    if memory_available_bytes is not None:
        memory = max(0, int(memory_available_bytes))
        if memory < 2 * 1024**3:
            ceiling = min(ceiling, 2)
        elif memory < 4 * 1024**3:
            ceiling = min(ceiling, 4)
        elif memory < 8 * 1024**3:
            ceiling = min(ceiling, 8)
    if workload_size is not None:
        ceiling = min(ceiling, max(1, int(workload_size)))
    ceiling = max(1, ceiling)
    samples = tuple(item for item in history if item.workers <= ceiling and item.completed_mutants > 0)
    if samples:
        grouped: dict[int, list[float]] = {}
        for item in samples:
            grouped.setdefault(item.workers, []).append(item.mutants_per_second)
        median_throughput = {workers: float(median(values)) for workers, values in grouped.items()}
        best_throughput = max(median_throughput.values())
        threshold = best_throughput * 0.95
        near_best = tuple(workers for workers, throughput in median_throughput.items() if throughput >= threshold)
        chosen_workers = min(near_best)
        return AutoConcurrencyDecision(chosen_workers, "history", detected_cpu, ceiling, len(samples))
    if preferred_workers is not None:
        preferred = max(1, min(int(preferred_workers), ceiling))
        return AutoConcurrencyDecision(preferred, "profile", detected_cpu, ceiling, 0)
    initial = max(1, min(ceiling, max(1, detected_cpu // 2)))
    return AutoConcurrencyDecision(initial, "cpu_half", detected_cpu, ceiling, 0)


__all__ = ["AutoConcurrencyDecision", "ConcurrencySample", "choose_auto_workers"]
