"""Optional proposal-provider boundary with no concrete SDK dependency."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .contracts import RepairHypothesis, SurvivorCategory


@dataclass(frozen=True)
class ProviderRequest:
    """Sanitized minimum context that may cross an external provider boundary."""

    request_id: str
    category: SurvivorCategory
    mutant_operator: str
    mutation_line: str
    source_excerpt: str
    findings: tuple[str, ...]
    hypotheses: tuple[RepairHypothesis, ...]


@dataclass(frozen=True)
class ProviderSuggestion:
    """Non-authoritative textual enhancement for a local proposal."""

    hypothesis_kind: str
    title: str
    arrangement: tuple[str, ...]
    action: tuple[str, ...]
    assertions: tuple[str, ...]
    rationale: str
    generated_code: str | None


@dataclass(frozen=True)
class ProviderResponse:
    """Provider output that can only enrich proposals, never classification."""

    suggestions: tuple[ProviderSuggestion, ...]
    warnings: tuple[str, ...]


class RepairProposalProvider(Protocol):
    """Protocol for optional deterministic or external proposal generators."""

    def propose(self, request: ProviderRequest) -> ProviderResponse:
        """Return non-authoritative proposal suggestions for sanitized context."""
        ...


class NullRepairProposalProvider:
    """Provider that proves the core package does not require an external service."""

    def propose(self, request: ProviderRequest) -> ProviderResponse:
        # Return no external suggestions while preserving local analysis behavior.
        del request
        return ProviderResponse((), ())


class DeterministicTemplateProvider:
    """Render stable suggestions from local hypotheses without an LLM."""

    def propose(self, request: ProviderRequest) -> ProviderResponse:
        # Convert each local hypothesis into a bounded deterministic suggestion.
        suggestions = tuple(
            ProviderSuggestion(
                hypothesis_kind=hypothesis.kind,
                title=hypothesis.title,
                arrangement=hypothesis.suggested_inputs,
                action=(hypothesis.target_behavior,),
                assertions=hypothesis.suggested_assertions,
                rationale=hypothesis.explanation,
                generated_code=None,
            )
            for hypothesis in request.hypotheses
        )
        return ProviderResponse(suggestions, ())
