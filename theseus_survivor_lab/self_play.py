"""Adversarial self-play league and fail-closed policy promotion for Theseus."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Protocol, Sequence


@dataclass(frozen=True, slots=True)
class MutationChallenge:
    """One adversarial mutant challenge created without granting it execution authority."""

    challenge_id: str
    mutant_id: str
    meaningful: bool
    metadata: dict[str, object]


@dataclass(frozen=True, slots=True)
class InvestigationAttempt:
    """One investigator response to a hidden mutation challenge."""

    diagnosis: str
    resolved: bool
    experiment_cost: float
    evidence_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TestCandidate:
    """One tester proposal whose value is determined only by external verification."""

    candidate_id: str
    description: str


TestCandidate.__test__ = False


@dataclass(frozen=True, slots=True)
class VerificationOutcome:
    """Objective self-play outcome emitted by the verifier authority."""

    meaningful_mutant: bool
    original_passes: bool
    mutant_killed: bool
    deterministic: bool
    false_authoritative_conclusion: bool = False


class MutatorRole(Protocol):
    """Adversarial role that proposes a mutation challenge."""

    def create_challenge(self, seed: str) -> MutationChallenge:
        # Define the challenge-generation boundary without making the mutator authoritative.
        ...


class InvestigatorRole(Protocol):
    """Role that gathers evidence and diagnoses a mutation challenge."""

    def investigate(self, challenge: MutationChallenge) -> InvestigationAttempt:
        # Define the investigation boundary consumed by self-play evaluation.
        ...


class TesterRole(Protocol):
    """Role that proposes a minimal diagnostic test after investigation."""

    def propose_test(self, challenge: MutationChallenge, investigation: InvestigationAttempt) -> TestCandidate:
        # Define the test-proposal boundary without applying project changes.
        ...


class VerifierRole(Protocol):
    """Objective role that validates the mutation and proposed test."""

    def verify(self, challenge: MutationChallenge, candidate: TestCandidate) -> VerificationOutcome:
        # Define the sole authority that determines whether the self-play episode succeeded.
        ...


@dataclass(frozen=True, slots=True)
class SelfPlayEpisode:
    """One complete Mutator → Investigator → Tester → Verifier episode."""

    episode_id: str
    challenge: MutationChallenge
    investigation: InvestigationAttempt
    candidate: TestCandidate
    verification: VerificationOutcome

    @property
    def success(self) -> bool:
        # Count success only when the mutant is meaningful and a stable regression test kills it without breaking original behavior.
        result = self.verification
        return result.meaningful_mutant and result.original_passes and result.mutant_killed and result.deterministic and not result.false_authoritative_conclusion


def run_self_play_episode(
    seed: str,
    mutator: MutatorRole,
    investigator: InvestigatorRole,
    tester: TesterRole,
    verifier: VerifierRole,
) -> SelfPlayEpisode:
    # Execute one bounded adversarial episode while keeping verification as the only result authority.
    challenge = mutator.create_challenge(seed)
    investigation = investigator.investigate(challenge)
    candidate = tester.propose_test(challenge, investigation)
    verification = verifier.verify(challenge, candidate)
    identity = "|".join((seed, challenge.challenge_id, candidate.candidate_id, investigation.diagnosis))
    episode_id = "self-play-" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]
    return SelfPlayEpisode(episode_id, challenge, investigation, candidate, verification)


@dataclass(frozen=True, slots=True)
class PolicyBenchmarkSummary:
    """Aggregate objective metrics used to compare candidate and incumbent policies."""

    policy_id: str
    episodes: int
    resolved: int
    successful: int
    meaningful_mutants: int
    false_authoritative: int
    total_experiment_cost: float

    @property
    def resolution_rate(self) -> float:
        # Derive resolved-investigation rate from objective episode counts.
        return self.resolved / self.episodes if self.episodes else 0.0

    @property
    def success_rate(self) -> float:
        # Derive fully verified episode success rate without subjective critic scores.
        return self.successful / self.episodes if self.episodes else 0.0

    @property
    def meaningful_rate(self) -> float:
        # Derive useful-mutant rate so a policy cannot win by generating equivalent noise.
        return self.meaningful_mutants / self.episodes if self.episodes else 0.0

    @property
    def average_cost(self) -> float:
        # Derive mean investigation cost for promotion cost guards.
        return self.total_experiment_cost / self.episodes if self.episodes else 0.0

    @classmethod
    def from_episodes(cls, policy_id: str, episodes: Sequence[SelfPlayEpisode]) -> "PolicyBenchmarkSummary":
        # Aggregate one policy's episodes without accepting empty benchmark evidence as a promotion signal.
        values = tuple(episodes)
        return cls(
            policy_id=policy_id,
            episodes=len(values),
            resolved=sum(int(item.investigation.resolved) for item in values),
            successful=sum(int(item.success) for item in values),
            meaningful_mutants=sum(int(item.verification.meaningful_mutant) for item in values),
            false_authoritative=sum(int(item.verification.false_authoritative_conclusion) for item in values),
            total_experiment_cost=sum(max(0.0, item.investigation.experiment_cost) for item in values),
        )


@dataclass(frozen=True, slots=True)
class PromotionCriteria:
    """Hard candidate-policy gates that prevent silent self-degradation."""

    min_episodes: int = 20
    min_resolution_delta: float = 0.02
    min_success_delta: float = 0.02
    max_cost_ratio: float = 1.05


@dataclass(frozen=True, slots=True)
class PromotionDecision:
    """Explain whether one candidate policy may replace the incumbent."""

    promote: bool
    reasons: tuple[str, ...]
    candidate: PolicyBenchmarkSummary
    baseline: PolicyBenchmarkSummary


def evaluate_policy_promotion(
    candidate: PolicyBenchmarkSummary,
    baseline: PolicyBenchmarkSummary,
    criteria: PromotionCriteria = PromotionCriteria(),
) -> PromotionDecision:
    # Promote only a sufficiently sampled candidate that improves verified outcomes without new false authority or excessive cost.
    reasons: list[str] = []
    if candidate.episodes < criteria.min_episodes or baseline.episodes < criteria.min_episodes:
        reasons.append("insufficient_samples")
    if candidate.false_authoritative:
        reasons.append("false_authoritative_conclusions")
    if candidate.resolution_rate + 1e-12 < baseline.resolution_rate + criteria.min_resolution_delta:
        reasons.append("resolution_rate_not_improved")
    if candidate.success_rate + 1e-12 < baseline.success_rate + criteria.min_success_delta:
        reasons.append("verified_success_rate_not_improved")
    if candidate.meaningful_rate + 1e-12 < baseline.meaningful_rate:
        reasons.append("meaningful_mutant_rate_regressed")
    if baseline.average_cost > 0.0 and candidate.average_cost > baseline.average_cost * criteria.max_cost_ratio:
        reasons.append("investigation_cost_regressed")
    return PromotionDecision(not reasons, tuple(reasons), candidate, baseline)


@dataclass(frozen=True, slots=True)
class SelfPlayLeague:
    """Deterministic pool of current, historical, and specialized adversarial policy identities."""

    policy_ids: tuple[str, ...]

    def opponent(self, seed: str, *, exclude: str | None = None) -> str:
        # Select one league opponent deterministically so benchmark composition is replayable.
        candidates = tuple(item for item in self.policy_ids if item != exclude)
        if not candidates:
            raise ValueError("self-play league has no eligible opponent")
        digest = hashlib.sha256(f"{seed}|self-play-league-v1".encode("utf-8")).digest()
        return candidates[int.from_bytes(digest[:8], "big") % len(candidates)]


__all__ = [
    "InvestigationAttempt",
    "InvestigatorRole",
    "MutationChallenge",
    "MutatorRole",
    "PolicyBenchmarkSummary",
    "PromotionCriteria",
    "PromotionDecision",
    "SelfPlayEpisode",
    "SelfPlayLeague",
    "TestCandidate",
    "TesterRole",
    "VerificationOutcome",
    "VerifierRole",
    "evaluate_policy_promotion",
    "run_self_play_episode",
]
