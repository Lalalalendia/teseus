from __future__ import annotations

from theseus_survivor_lab import ExperimentKind, SurvivorCategory, default_experiment_catalog, initial_belief_state, rank_experiments
from theseus_survivor_lab.contracts import SurvivorClassification


def test_a9_experiment_ranking_prefers_information_that_splits_current_hypotheses() -> None:
    # Select a targeted diagnostic action instead of blindly paying for the full suite.
    classification = SurvivorClassification(SurvivorCategory.SELECTION_ESCAPE, "possible selection miss", 0.55, (), (SurvivorCategory.REAL_TEST_GAP,))
    belief = initial_belief_state(classification)
    ranked = rank_experiments(belief, default_experiment_catalog())
    assert ranked[0].information_gain_bits > 0.0
    assert ranked[0].experiment.kind != ExperimentKind.FULL_SUITE
    full_suite = next(item for item in ranked if item.experiment.kind == ExperimentKind.FULL_SUITE)
    assert ranked[0].utility > full_suite.utility
