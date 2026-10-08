from __future__ import annotations

from theseus_knowledge import InvestigationTrajectory, InvestigationTrajectoryConflict, InvestigationTrajectoryStore, TrajectoryStepEvidence
from theseus_survivor_lab import ExperimentKind, LearnedExperimentPolicy


def _trajectory(identity: str, experiment: ExperimentKind, *, success: bool, cost: float = 0.1) -> InvestigationTrajectory:
    # Build one compact verified trajectory for deterministic learning tests.
    return InvestigationTrajectory(
        trajectory_id=identity,
        project_id="project-1",
        signature="condition_to_not|selection_escape",
        resolved_category="selection_escape" if success else "insufficient_evidence",
        success=success,
        total_cost=cost,
        steps=(TrajectoryStepEvidence(experiment.value, "supports", True, 0.5, 0.8, cost),),
    )


def test_a11_trajectory_store_is_idempotent_and_rejects_identity_conflict(tmp_path) -> None:
    # Keep learning data replay-safe exactly like other Theseus evidence stores.
    store = InvestigationTrajectoryStore(tmp_path / "investigations.sqlite3")
    first = _trajectory("trajectory-1", ExperimentKind.RELATED_TEST, success=True)
    assert store.record(first) is True
    assert store.record(first) is False
    conflicting = _trajectory("trajectory-1", ExperimentKind.FULL_SUITE, success=True)
    try:
        store.record(conflicting)
    except InvestigationTrajectoryConflict as exc:
        assert "identity conflict" in str(exc)
    else:
        raise AssertionError("trajectory identity conflict must fail closed")
    store.close()


def test_a11_learned_policy_rewards_successful_low_cost_experiment_over_failed_alternative(tmp_path) -> None:
    # Learn an interpretable preference without replacing the deterministic information-gain baseline.
    store = InvestigationTrajectoryStore(tmp_path / "investigations.sqlite3")
    for index in range(5):
        store.record(_trajectory(f"related-{index}", ExperimentKind.RELATED_TEST, success=True, cost=0.05))
    for index in range(5):
        store.record(_trajectory(f"full-{index}", ExperimentKind.FULL_SUITE, success=False, cost=1.0))
    policy = LearnedExperimentPolicy.train(store, project_id="project-1", signature="condition_to_not|selection_escape")
    assert policy.sample_count == 10
    assert policy.bonuses[ExperimentKind.RELATED_TEST] > policy.bonuses[ExperimentKind.FULL_SUITE]
    store.close()
