"""Deterministic belief state over competing survivor explanations."""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Mapping

from .contracts import SurvivorAnalysisRequest, SurvivorCategory, SurvivorClassification

BELIEF_SCHEMA_VERSION = 1
BELIEF_EPSILON = 1e-9


@dataclass(frozen=True, slots=True)
class HypothesisBelief:
    """One normalized survivor-cause probability with bounded evidence provenance."""

    category: SurvivorCategory
    probability: float
    evidence_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class BeliefState:
    """Immutable normalized probability distribution over survivor causes."""

    beliefs: tuple[HypothesisBelief, ...]
    revision: int = 0
    schema_version: int = BELIEF_SCHEMA_VERSION

    def __post_init__(self) -> None:
        # Validate one normalized non-empty distribution before it can drive experiment selection.
        if not self.beliefs:
            raise ValueError("belief state must not be empty")
        categories = tuple(item.category for item in self.beliefs)
        if len(set(categories)) != len(categories):
            raise ValueError("belief categories must be unique")
        if any(not math.isfinite(item.probability) or item.probability < 0.0 for item in self.beliefs):
            raise ValueError("belief probabilities must be finite and non-negative")
        if abs(sum(item.probability for item in self.beliefs) - 1.0) > 1e-9:
            raise ValueError("belief probabilities must sum to one")

    @property
    def top(self) -> HypothesisBelief:
        # Return the most probable category with deterministic category-name tie breaking.
        return max(self.beliefs, key=lambda item: (item.probability, item.category.value))

    @property
    def entropy_bits(self) -> float:
        # Measure current uncertainty in bits without treating zero-probability categories as evidence.
        return -sum(item.probability * math.log2(item.probability) for item in self.beliefs if item.probability > 0.0)

    def probability(self, category: SurvivorCategory) -> float:
        # Read one category probability without exposing the tuple representation to callers.
        return next((item.probability for item in self.beliefs if item.category == category), 0.0)

    def update(self, likelihoods: Mapping[SurvivorCategory, float], evidence_id: str) -> "BeliefState":
        # Apply one verified likelihood observation using a normalized Bayesian update.
        weighted: list[tuple[HypothesisBelief, float]] = []
        for item in self.beliefs:
            likelihood = float(likelihoods.get(item.category, BELIEF_EPSILON))
            if not math.isfinite(likelihood) or likelihood < 0.0:
                raise ValueError("likelihoods must be finite and non-negative")
            weighted.append((item, item.probability * max(BELIEF_EPSILON, likelihood)))
        total = sum(value for _item, value in weighted)
        if total <= 0.0:
            raise ValueError("belief update produced zero probability mass")
        return BeliefState(
            tuple(
                HypothesisBelief(
                    item.category,
                    value / total,
                    tuple(dict.fromkeys((*item.evidence_ids, evidence_id))) if evidence_id else item.evidence_ids,
                )
                for item, value in weighted
            ),
            revision=self.revision + 1,
        )

    def to_dict(self) -> dict[str, object]:
        # Serialize one belief state in deterministic category order for artifacts and trajectories.
        return {
            "schema_version": self.schema_version,
            "revision": self.revision,
            "entropy_bits": self.entropy_bits,
            "top_category": self.top.category.value,
            "top_probability": self.top.probability,
            "beliefs": [
                {"category": item.category.value, "probability": item.probability, "evidence_ids": list(item.evidence_ids)}
                for item in sorted(self.beliefs, key=lambda value: value.category.value)
            ],
        }


def _normalized(weights: Mapping[SurvivorCategory, float], evidence_ids: tuple[str, ...]) -> BeliefState:
    # Normalize deterministic category weights while retaining authoritative classification evidence ids.
    positive = {category: max(BELIEF_EPSILON, float(value)) for category, value in weights.items()}
    total = sum(positive.values())
    return BeliefState(tuple(HypothesisBelief(category, value / total, evidence_ids) for category, value in sorted(positive.items(), key=lambda item: item[0].value)))


def initial_belief_state(
    classification: SurvivorClassification,
    request: SurvivorAnalysisRequest | None = None,
) -> BeliefState:
    # Seed competing hypotheses from deterministic classification plus explicit runtime evidence signals.
    categories = tuple(category for category in SurvivorCategory if category != SurvivorCategory.INSUFFICIENT_EVIDENCE)
    weights = {category: 1.0 for category in categories}
    primary = classification.category
    if primary != SurvivorCategory.INSUFFICIENT_EVIDENCE:
        weights[primary] = 4.0 + 8.0 * max(0.0, min(1.0, float(classification.confidence)))
    for secondary in classification.secondary_categories:
        if secondary in weights:
            weights[secondary] += 3.0
    if request is not None:
        selection = request.selection
        if selection.equivalent_observation is True:
            weights[SurvivorCategory.EQUIVALENT_SUSPECTED] += 8.0
        if selection.runtime_location_reached is False or selection.mutated_branch_reached is False:
            weights[SurvivorCategory.UNREACHABLE_CODE] += 7.0
        if selection.boundary_values_observed is False:
            weights[SurvivorCategory.TEST_DATA_GAP] += 5.0
        if selection.oracle_observed is False or selection.assertions_observed is False:
            weights[SurvivorCategory.WEAK_ORACLE] += 5.0
        selected = set(selection.selected_tests)
        related = set(selection.related_test_nodeids)
        if related - selected:
            weights[SurvivorCategory.SELECTION_ESCAPE] += 6.0
        if request.environment is not None and (request.environment.stable is False or request.environment.differences):
            weights[SurvivorCategory.ENVIRONMENT_DEPENDENT] += 6.0
        if any(item.timed_out for item in request.executions):
            weights[SurvivorCategory.TIMEOUT_AMBIGUITY] += 8.0
        if any(item.infrastructure_failure for item in request.executions):
            weights[SurvivorCategory.INFRASTRUCTURE_AMBIGUITY] += 8.0
    return _normalized(weights, classification.evidence_ids)


__all__ = ["BELIEF_SCHEMA_VERSION", "BeliefState", "HypothesisBelief", "initial_belief_state"]
