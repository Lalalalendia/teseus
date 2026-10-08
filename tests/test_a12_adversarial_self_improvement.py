from __future__ import annotations

from theseus_survivor_lab import (
    InvestigationAttempt,
    MutationChallenge,
    PolicyBenchmarkSummary,
    PromotionCriteria,
    SelfPlayLeague,
    TestCandidate,
    VerificationOutcome,
    evaluate_policy_promotion,
    run_self_play_episode,
)


class _Mutator:
    def create_challenge(self, seed):
        # Produce one meaningful challenge whose truth is still decided by the verifier.
        return MutationChallenge(f"challenge-{seed}", f"mutant-{seed}", True, {})


class _Investigator:
    def investigate(self, challenge):
        # Resolve the synthetic challenge at a bounded diagnostic cost.
        return InvestigationAttempt("test_data_gap", True, 0.2, ("e1",))


class _Tester:
    def propose_test(self, challenge, investigation):
        # Propose one stable candidate identity without claiming that it kills the mutant.
        return TestCandidate(f"test-{challenge.challenge_id}", "boundary regression")


class _Verifier:
    def verify(self, challenge, candidate):
        # Supply the only authoritative success evidence in the self-play episode.
        return VerificationOutcome(True, True, True, True, False)


def test_a12_self_play_episode_is_successful_only_after_verifier_evidence() -> None:
    # Exercise all four roles while keeping success tied to objective verification rather than role opinions.
    episode = run_self_play_episode("1", _Mutator(), _Investigator(), _Tester(), _Verifier())
    assert episode.success is True
    assert episode.verification.mutant_killed is True


def _summary(policy_id: str, *, episodes: int, resolved: int, success: int, cost: float, false: int = 0) -> PolicyBenchmarkSummary:
    # Build one aggregate policy benchmark for fail-closed promotion tests.
    return PolicyBenchmarkSummary(policy_id, episodes, resolved, success, episodes, false, cost)


def test_a12_candidate_policy_must_improve_and_have_zero_false_authoritative_conclusions() -> None:
    # Promote only a genuinely better candidate and reject an otherwise strong policy with one false authority event.
    baseline = _summary("v1", episodes=100, resolved=70, success=60, cost=100.0)
    candidate = _summary("v2", episodes=100, resolved=80, success=70, cost=95.0)
    decision = evaluate_policy_promotion(candidate, baseline, PromotionCriteria(min_episodes=50))
    assert decision.promote is True
    unsafe = _summary("v3", episodes=100, resolved=90, success=80, cost=90.0, false=1)
    rejected = evaluate_policy_promotion(unsafe, baseline, PromotionCriteria(min_episodes=50))
    assert rejected.promote is False
    assert "false_authoritative_conclusions" in rejected.reasons


def test_a12_league_opponent_selection_is_replayable_and_excludes_candidate() -> None:
    # Keep adversarial benchmark composition deterministic across retries and machines.
    league = SelfPlayLeague(("baseline", "v1", "v2", "security-specialist"))
    first = league.opponent("episode-42", exclude="v2")
    second = league.opponent("episode-42", exclude="v2")
    assert first == second
    assert first != "v2"
