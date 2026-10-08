from __future__ import annotations

from dataclasses import replace

from theseus_survivor_lab import SurvivorCategory, initial_belief_state
from theseus_survivor_lab.contracts import SurvivorClassification


def test_a8_belief_state_keeps_competing_hypotheses_and_normalizes_probability() -> None:
    # Preserve uncertainty instead of collapsing the survivor to one classification label.
    classification = SurvivorClassification(SurvivorCategory.SELECTION_ESCAPE, "related test omitted", 0.7, ("e1",), (SurvivorCategory.WEAK_ORACLE,))
    belief = initial_belief_state(classification)
    assert abs(sum(item.probability for item in belief.beliefs) - 1.0) < 1e-9
    assert belief.top.category == SurvivorCategory.SELECTION_ESCAPE
    assert belief.probability(SurvivorCategory.WEAK_ORACLE) > belief.probability(SurvivorCategory.ENVIRONMENT_DEPENDENT)
    assert belief.entropy_bits > 0.0


def test_a8_verified_likelihood_updates_belief_and_preserves_evidence_id() -> None:
    # Move probability toward the explanation supported by new evidence while retaining provenance.
    classification = SurvivorClassification(SurvivorCategory.INSUFFICIENT_EVIDENCE, "unknown", 0.2, (), ())
    belief = initial_belief_state(classification)
    updated = belief.update(
        {category: (0.95 if category == SurvivorCategory.UNREACHABLE_CODE else 0.05) for category in SurvivorCategory if category != SurvivorCategory.INSUFFICIENT_EVIDENCE},
        "trace-1",
    )
    assert updated.top.category == SurvivorCategory.UNREACHABLE_CODE
    assert updated.revision == 1
    assert "trace-1" in updated.top.evidence_ids
