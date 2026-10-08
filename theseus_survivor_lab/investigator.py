"""Active multi-step survivor investigation driven by verified experiments."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Protocol

from .beliefs import BeliefState, initial_belief_state
from .contracts import SurvivorAnalysisRequest, SurvivorCategory, SurvivorClassification
from .experiments import ExperimentDefinition, ExperimentKind, ExperimentScore, default_experiment_catalog, rank_experiments


@dataclass(frozen=True, slots=True)
class ExperimentObservation:
    """Verified or rejected evidence returned by an external experiment executor."""

    experiment: ExperimentKind
    outcome: str
    evidence_id: str
    verified: bool
    actual_cost: float = 0.0
    details: Mapping[str, object] | None = None


class ExperimentExecutor(Protocol):
    """Runtime port that performs one diagnostic action outside the reasoning layer."""

    def execute(self, experiment: ExperimentDefinition, request: SurvivorAnalysisRequest) -> ExperimentObservation:
        # Define the only side-effecting boundary used by Active Investigator.
        ...


@dataclass(frozen=True, slots=True)
class InvestigationStep:
    """One selected experiment and its evidence-driven belief transition."""

    ordinal: int
    experiment: ExperimentKind
    expected_information_gain_bits: float
    expected_utility: float
    observation: ExperimentObservation
    belief_before: BeliefState
    belief_after: BeliefState


@dataclass(frozen=True, slots=True)
class InvestigationResult:
    """Bounded autonomous investigation result suitable for persistence and learning."""

    final_belief: BeliefState
    diagnosis: SurvivorCategory
    resolved: bool
    stop_reason: str
    steps: tuple[InvestigationStep, ...]
    total_cost: float

    def to_dict(self) -> dict[str, object]:
        # Serialize one investigation while keeping every conclusion linked to verified experiment evidence.
        return {
            "diagnosis": self.diagnosis.value,
            "resolved": self.resolved,
            "stop_reason": self.stop_reason,
            "total_cost": self.total_cost,
            "final_belief": self.final_belief.to_dict(),
            "steps": [
                {
                    "ordinal": item.ordinal,
                    "experiment": item.experiment.value,
                    "expected_information_gain_bits": item.expected_information_gain_bits,
                    "expected_utility": item.expected_utility,
                    "observation": {
                        "outcome": item.observation.outcome,
                        "evidence_id": item.observation.evidence_id,
                        "verified": item.observation.verified,
                        "actual_cost": item.observation.actual_cost,
                    },
                    "belief_before": item.belief_before.to_dict(),
                    "belief_after": item.belief_after.to_dict(),
                }
                for item in self.steps
            ],
        }


class ActiveInvestigator:
    """Choose and execute the next best evidence-gathering action until diagnosis or low value of information."""

    def __init__(
        self,
        *,
        catalog: tuple[ExperimentDefinition, ...] | None = None,
        confidence_threshold: float = 0.90,
        min_utility: float = 0.02,
        max_steps: int = 5,
    ) -> None:
        # Freeze investigation policy limits so one run remains reproducible from its inputs.
        self.catalog = catalog or default_experiment_catalog()
        self.confidence_threshold = max(0.5, min(0.999999, float(confidence_threshold)))
        self.min_utility = float(min_utility)
        self.max_steps = max(1, int(max_steps))

    def run(
        self,
        request: SurvivorAnalysisRequest,
        classification: SurvivorClassification,
        executor: ExperimentExecutor,
        *,
        learned_bonuses: Mapping[ExperimentKind, float] | None = None,
    ) -> InvestigationResult:
        # Repeatedly acquire verified evidence and stop only on confidence, budget, or exhausted information value.
        belief = initial_belief_state(classification, request)
        steps: list[InvestigationStep] = []
        used: set[ExperimentKind] = set()
        total_cost = 0.0
        stop_reason = "max_steps"
        for ordinal in range(1, self.max_steps + 1):
            if belief.top.probability >= self.confidence_threshold:
                stop_reason = "confidence_threshold"
                break
            candidates = tuple(item for item in self.catalog if item.kind not in used)
            if not candidates:
                stop_reason = "experiments_exhausted"
                break
            ranked = rank_experiments(belief, candidates, learned_bonuses=learned_bonuses)
            selected: ExperimentScore = ranked[0]
            if selected.utility < self.min_utility:
                stop_reason = "value_below_cost"
                break
            observation = executor.execute(selected.experiment, request)
            if observation.experiment != selected.experiment.kind:
                raise ValueError("experiment executor returned a mismatched experiment identity")
            used.add(selected.experiment.kind)
            before = belief
            if observation.verified and observation.outcome in selected.experiment.likelihoods:
                belief = belief.update(selected.experiment.likelihoods[observation.outcome], observation.evidence_id)
            total_cost += max(0.0, float(observation.actual_cost))
            steps.append(
                InvestigationStep(
                    ordinal,
                    selected.experiment.kind,
                    selected.information_gain_bits,
                    selected.utility,
                    observation,
                    before,
                    belief,
                )
            )
        else:
            stop_reason = "max_steps"
        resolved = belief.top.probability >= self.confidence_threshold
        diagnosis = belief.top.category if resolved else SurvivorCategory.INSUFFICIENT_EVIDENCE
        return InvestigationResult(belief, diagnosis, resolved, stop_reason, tuple(steps), total_cost)


__all__ = [
    "ActiveInvestigator",
    "ExperimentExecutor",
    "ExperimentObservation",
    "InvestigationResult",
    "InvestigationStep",
]
