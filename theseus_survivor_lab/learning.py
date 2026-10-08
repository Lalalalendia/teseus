"""Interpretable trajectory learning for Active Investigator experiment selection."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

from theseus_knowledge import InvestigationTrajectory, InvestigationTrajectoryStore, TrajectoryStepEvidence

from .contracts import SurvivorAnalysisRequest, SurvivorClassification
from .experiments import ExperimentKind
from .investigator import ActiveInvestigator, ExperimentExecutor, InvestigationResult


def investigation_signature(request: SurvivorAnalysisRequest, classification: SurvivorClassification) -> str:
    # Group comparable investigations by stable mutation family and starting classification only.
    return f"{request.mutant.operator}|{classification.category.value}"


def trajectory_from_result(
    request: SurvivorAnalysisRequest,
    classification: SurvivorClassification,
    result: InvestigationResult,
) -> InvestigationTrajectory:
    # Convert one verified investigation result to a content-addressed learning trajectory.
    signature = investigation_signature(request, classification)
    identity_payload = "|".join(
        (
            request.project_id,
            request.request_id,
            request.mutant.mutant_id,
            signature,
            result.diagnosis.value,
            ",".join(f"{item.experiment.value}:{item.observation.evidence_id}" for item in result.steps),
        )
    )
    trajectory_id = "investigation-" + hashlib.sha256(identity_payload.encode("utf-8")).hexdigest()[:24]
    return InvestigationTrajectory(
        trajectory_id=trajectory_id,
        project_id=request.project_id,
        signature=signature,
        resolved_category=result.diagnosis.value,
        success=result.resolved,
        total_cost=result.total_cost,
        steps=tuple(
            TrajectoryStepEvidence(
                experiment=item.experiment.value,
                outcome=item.observation.outcome,
                verified=item.observation.verified,
                expected_information_gain_bits=item.expected_information_gain_bits,
                expected_utility=item.expected_utility,
                actual_cost=item.observation.actual_cost,
            )
            for item in result.steps
        ),
    )


@dataclass(frozen=True, slots=True)
class LearnedExperimentPolicy:
    """Laplace-smoothed experiment preference learned from verified historical trajectories."""

    bonuses: dict[ExperimentKind, float]
    sample_count: int

    @classmethod
    def train(
        cls,
        store: InvestigationTrajectoryStore,
        *,
        project_id: str,
        signature: str,
    ) -> "LearnedExperimentPolicy":
        # Learn bounded interpretable bonuses from success frequency and cost of matching project trajectories.
        trajectories = store.list(project_id=project_id, signature=signature)
        attempts: dict[ExperimentKind, int] = {}
        credits: dict[ExperimentKind, float] = {}
        costs: dict[ExperimentKind, float] = {}
        for trajectory in trajectories:
            verified_steps = tuple(item for item in trajectory.steps if item.verified)
            for step in verified_steps:
                try:
                    kind = ExperimentKind(step.experiment)
                except ValueError:
                    continue
                attempts[kind] = attempts.get(kind, 0) + 1
                costs[kind] = costs.get(kind, 0.0) + max(0.0, step.actual_cost)
                if trajectory.success:
                    credits[kind] = credits.get(kind, 0.0) + 1.0 / max(1, len(verified_steps))
        bonuses: dict[ExperimentKind, float] = {}
        for kind, count in attempts.items():
            success_rate = (credits.get(kind, 0.0) + 1.0) / (count + 2.0)
            average_cost = costs.get(kind, 0.0) / count
            bonuses[kind] = max(-0.25, min(0.25, (success_rate - 0.5) * 0.5 - min(0.1, average_cost * 0.01)))
        return cls(bonuses, len(trajectories))


@dataclass(frozen=True, slots=True)
class LearningInvestigationRun:
    """One active investigation plus the policy sample count used for its decision ranking."""

    result: InvestigationResult
    policy_samples: int
    trajectory: InvestigationTrajectory


class LearningActiveInvestigator:
    """Close the A11 loop by training bonuses, running investigation, and persisting verified experience."""

    def __init__(self, store: InvestigationTrajectoryStore, investigator: ActiveInvestigator | None = None) -> None:
        # Bind one active investigator to its durable trajectory memory without changing verifier authority.
        self.store = store
        self.investigator = investigator or ActiveInvestigator()

    def run(
        self,
        request: SurvivorAnalysisRequest,
        classification: SurvivorClassification,
        executor: ExperimentExecutor,
    ) -> LearningInvestigationRun:
        # Apply learned bonuses only as bounded ranking hints and record the verified resulting trajectory idempotently.
        signature = investigation_signature(request, classification)
        policy = LearnedExperimentPolicy.train(self.store, project_id=request.project_id, signature=signature)
        result = self.investigator.run(request, classification, executor, learned_bonuses=policy.bonuses)
        trajectory = trajectory_from_result(request, classification, result)
        self.store.record(trajectory)
        return LearningInvestigationRun(result, policy.sample_count, trajectory)


__all__ = ["LearnedExperimentPolicy", "LearningActiveInvestigator", "LearningInvestigationRun", "investigation_signature", "trajectory_from_result"]
