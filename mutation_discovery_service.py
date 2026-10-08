"""Mutation discovery service extracted from campaign orchestration."""

from __future__ import annotations

import time
from typing import Iterable, Sequence

from .models import Mutant, PerformanceMetrics
from .mutations import generate_mutants


class MutationDiscoveryService:
    """Generate deterministic first-order mutants while owning discovery timing."""

    def __init__(self, metrics: PerformanceMetrics) -> None:
        # Record discovery cost in the caller's existing performance object.
        self.metrics = metrics

    def discover(
        self,
        source_text: str,
        *,
        function_range: tuple[int, int] | None = None,
        from_line: int | None = None,
        to_line: int | None = None,
        mutant_ids: Iterable[str] | None = None,
        max_mutants: int | None = None,
        operators: Sequence[str] | None = None,
    ) -> list[Mutant]:
        # Delegate operator semantics while keeping discovery as a replaceable public service.
        started = time.perf_counter()
        result = generate_mutants(
            source_text,
            function_range=function_range,
            from_line=from_line,
            to_line=to_line,
            mutant_ids=set(mutant_ids) if mutant_ids is not None else None,
            max_mutants=max_mutants,
            operators=operators,
        )
        self.metrics.mutant_generation_seconds += time.perf_counter() - started
        return result
