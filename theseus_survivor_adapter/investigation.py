"""Runtime adapter connecting E24 survivor artifacts to A10/A11 active investigation."""
from __future__ import annotations

from dataclasses import dataclass

from theseus_knowledge import InvestigationTrajectoryStore
from theseus_survivor_lab import (
    ActiveInvestigator,
    ExperimentExecutor,
    InvestigationResult,
    LearningActiveInvestigator,
)

from .contracts import SurvivorAdapterResult


@dataclass(frozen=True, slots=True)
class SurvivorInvestigationResult:
    """Active-investigation result fenced to the exact source analysis artifact."""

    source_analysis_id: str
    source_result_id: str
    investigation: InvestigationResult
    learned_from_samples: int
    trajectory_id: str | None


class SurvivorInvestigationService:
    """Run Active Investigator over already-authenticated Survivor Adapter evidence."""

    def __init__(
        self,
        *,
        trajectory_store: InvestigationTrajectoryStore | None = None,
        investigator: ActiveInvestigator | None = None,
    ) -> None:
        # Bind active investigation to optional durable learning memory without weakening source evidence fencing.
        self.trajectory_store = trajectory_store
        self.investigator = investigator or ActiveInvestigator()

    def run(self, source: SurvivorAdapterResult, executor: ExperimentExecutor) -> SurvivorInvestigationResult:
        # Investigate the exact E24 request/classification and optionally persist its verified trajectory for later learning.
        if self.trajectory_store is None:
            result = self.investigator.run(source.request, source.analysis.classification, executor)
            return SurvivorInvestigationResult(source.analysis.analysis_id, source.analysis.result_id, result, 0, None)
        learned = LearningActiveInvestigator(self.trajectory_store, self.investigator).run(
            source.request,
            source.analysis.classification,
            executor,
        )
        return SurvivorInvestigationResult(
            source.analysis.analysis_id,
            source.analysis.result_id,
            learned.result,
            learned.policy_samples,
            learned.trajectory.trajectory_id,
        )


__all__ = ["SurvivorInvestigationResult", "SurvivorInvestigationService"]
