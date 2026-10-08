"""One-mutant execution port for the next runner decomposition step."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence


class MutantExecutionService:
    """Compatibility service that owns the public one-mutant execution call shape."""

    def __init__(self, runner: Any) -> None:
        # Keep the runner as the temporary backend until mutation policy is moved here fully.
        self.runner = runner

    def execute(
        self,
        mutant: Any,
        snapshot: Any,
        levels: Sequence[Any],
        report_id: str,
        manifest_path: Path,
    ) -> Any:
        # Delegate through one explicit service port while preserving exact result and recovery semantics.
        return self.runner._run_mutant_impl(mutant, snapshot, levels, report_id, manifest_path)
