"""Information-gain experiment catalog for active survivor investigation."""
from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum
from typing import Mapping

from .beliefs import BeliefState
from .contracts import SurvivorCategory


class ExperimentKind(StrEnum):
    """Bounded diagnostic actions that may acquire new survivor evidence."""

    FRESH_RERUN = "fresh_rerun"
    RELATED_TEST = "related_test"
    BROADEN_SELECTION = "broaden_selection"
    BOUNDARY_PROBE = "boundary_probe"
    REACHABILITY_PROBE = "reachability_probe"
    ENVIRONMENT_COMPARISON = "environment_comparison"
    FOCUSED_GENERATED_TEST = "focused_generated_test"
    FULL_SUITE = "full_suite"


@dataclass(frozen=True, slots=True)
class ExperimentDefinition:
    """One deterministic diagnostic experiment with outcome likelihood model and costs."""

    kind: ExperimentKind
    estimated_cost: float
    risk: float
    diagnostic_value: float
    likelihoods: Mapping[str, Mapping[SurvivorCategory, float]]

    def __post_init__(self) -> None:
        # Reject invalid utility inputs before they can bias active investigation.
        if self.estimated_cost < 0.0 or self.risk < 0.0 or self.diagnostic_value < 0.0:
            raise ValueError("experiment cost, risk, and diagnostic value must be non-negative")
        if not self.likelihoods:
            raise ValueError("experiment likelihoods must not be empty")


@dataclass(frozen=True, slots=True)
class ExperimentScore:
    """Expected information and utility for one candidate experiment."""

    experiment: ExperimentDefinition
    information_gain_bits: float
    expected_entropy_bits: float
    utility: float


def _binary_likelihoods(targets: tuple[SurvivorCategory, ...], *, target_support: float = 0.85, other_support: float = 0.15) -> dict[str, dict[SurvivorCategory, float]]:
    # Build a two-outcome likelihood table that strongly separates target categories from alternatives.
    support: dict[SurvivorCategory, float] = {}
    refute: dict[SurvivorCategory, float] = {}
    target_set = set(targets)
    for category in SurvivorCategory:
        if category == SurvivorCategory.INSUFFICIENT_EVIDENCE:
            continue
        probability = target_support if category in target_set else other_support
        support[category] = probability
        refute[category] = 1.0 - probability
    return {"supports": support, "refutes": refute}


def default_experiment_catalog() -> tuple[ExperimentDefinition, ...]:
    # Return the stable A9 diagnostic action catalog ordered by increasing operational cost.
    return (
        ExperimentDefinition(ExperimentKind.RELATED_TEST, 0.08, 0.01, 0.85, _binary_likelihoods((SurvivorCategory.SELECTION_ESCAPE,), target_support=0.92)),
        ExperimentDefinition(ExperimentKind.BOUNDARY_PROBE, 0.10, 0.01, 0.90, _binary_likelihoods((SurvivorCategory.TEST_DATA_GAP, SurvivorCategory.REAL_TEST_GAP, SurvivorCategory.WEAK_ORACLE), target_support=0.82)),
        ExperimentDefinition(ExperimentKind.REACHABILITY_PROBE, 0.14, 0.02, 0.90, _binary_likelihoods((SurvivorCategory.UNREACHABLE_CODE,), target_support=0.93)),
        ExperimentDefinition(ExperimentKind.FRESH_RERUN, 0.18, 0.01, 0.70, _binary_likelihoods((SurvivorCategory.INFRASTRUCTURE_AMBIGUITY, SurvivorCategory.TIMEOUT_AMBIGUITY), target_support=0.88)),
        ExperimentDefinition(ExperimentKind.ENVIRONMENT_COMPARISON, 0.25, 0.03, 0.82, _binary_likelihoods((SurvivorCategory.ENVIRONMENT_DEPENDENT, SurvivorCategory.INFRASTRUCTURE_AMBIGUITY), target_support=0.87)),
        ExperimentDefinition(ExperimentKind.BROADEN_SELECTION, 0.32, 0.02, 0.78, _binary_likelihoods((SurvivorCategory.SELECTION_ESCAPE, SurvivorCategory.REAL_TEST_GAP), target_support=0.80)),
        ExperimentDefinition(ExperimentKind.FOCUSED_GENERATED_TEST, 0.45, 0.05, 0.95, _binary_likelihoods((SurvivorCategory.REAL_TEST_GAP, SurvivorCategory.WEAK_ORACLE, SurvivorCategory.ASSERTION_TOO_BROAD, SurvivorCategory.TEST_DATA_GAP), target_support=0.84)),
        ExperimentDefinition(ExperimentKind.FULL_SUITE, 0.90, 0.04, 0.65, _binary_likelihoods((SurvivorCategory.SELECTION_ESCAPE, SurvivorCategory.REAL_TEST_GAP), target_support=0.75)),
    )


def _posterior_entropy(belief: BeliefState, likelihoods: Mapping[SurvivorCategory, float]) -> tuple[float, float]:
    # Compute one outcome probability and posterior entropy without mutating the input belief state.
    outcome_probability = sum(item.probability * float(likelihoods.get(item.category, 0.0)) for item in belief.beliefs)
    if outcome_probability <= 0.0:
        return 0.0, belief.entropy_bits
    posterior = [item.probability * float(likelihoods.get(item.category, 0.0)) / outcome_probability for item in belief.beliefs]
    entropy = -sum(value * math.log2(value) for value in posterior if value > 0.0)
    return outcome_probability, entropy


def expected_information_gain(belief: BeliefState, experiment: ExperimentDefinition) -> tuple[float, float]:
    # Compute expected entropy reduction over every declared experiment outcome.
    expected_entropy = 0.0
    probability_sum = 0.0
    for likelihoods in experiment.likelihoods.values():
        probability, entropy = _posterior_entropy(belief, likelihoods)
        expected_entropy += probability * entropy
        probability_sum += probability
    if probability_sum > 0.0 and abs(probability_sum - 1.0) > 1e-6:
        expected_entropy /= probability_sum
    information_gain = max(0.0, belief.entropy_bits - expected_entropy)
    return information_gain, expected_entropy


def score_experiment(
    belief: BeliefState,
    experiment: ExperimentDefinition,
    *,
    learned_bonus: float = 0.0,
) -> ExperimentScore:
    # Combine epistemic value, diagnostic utility, cost, risk, and bounded learned history bonus.
    information_gain, expected_entropy = expected_information_gain(belief, experiment)
    bonus = max(-0.5, min(0.5, float(learned_bonus)))
    utility = information_gain + 0.5 * experiment.diagnostic_value + bonus - experiment.estimated_cost - experiment.risk
    return ExperimentScore(experiment, information_gain, expected_entropy, utility)


def rank_experiments(
    belief: BeliefState,
    catalog: tuple[ExperimentDefinition, ...] | None = None,
    *,
    learned_bonuses: Mapping[ExperimentKind, float] | None = None,
) -> tuple[ExperimentScore, ...]:
    # Rank candidate experiments deterministically by utility, information gain, cost, and stable kind name.
    bonuses = learned_bonuses or {}
    scores = tuple(score_experiment(belief, item, learned_bonus=float(bonuses.get(item.kind, 0.0))) for item in (catalog or default_experiment_catalog()))
    return tuple(sorted(scores, key=lambda item: (-item.utility, -item.information_gain_bits, item.experiment.estimated_cost, item.experiment.kind.value)))


__all__ = [
    "ExperimentDefinition",
    "ExperimentKind",
    "ExperimentScore",
    "default_experiment_catalog",
    "expected_information_gain",
    "rank_experiments",
    "score_experiment",
]
