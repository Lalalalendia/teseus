"""Deterministic structured test-proposal generation."""
from __future__ import annotations
import hashlib
import re
from .contracts import (
    GenerationSource,
    RepairHypothesis,
    SurvivorAnalysisRequest,
    TestProposal,
)
from .providers import ProviderSuggestion
from .validation import canonical_relative_path
def _slug(value: str) -> str:
    # Create a compact identifier safe for a test function name.
    text = re.sub(r"[^a-zA-Z0-9]+", "_", value).strip("_").lower()
    return text[:48] or "mutant"
def proposal_identity(request: SurvivorAnalysisRequest, hypothesis: RepairHypothesis) -> str:
    # Generate a deterministic proposal identity independent of physical paths.
    payload = f"{request.mutant.mutant_id}|{hypothesis.hypothesis_id}|{hypothesis.kind}"
    return f"proposal-{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:16]}"
def _target(request: SurvivorAnalysisRequest) -> tuple[str | None, str | None]:
    # Choose a canonical related-test target without inventing a repository path.
    for test in request.related_tests:
        if test.source_path:
            return canonical_relative_path(test.source_path), test.nodeid
    if request.related_tests:
        return None, request.related_tests[0].nodeid
    return None, None
def _default_code(name: str, hypothesis: RepairHypothesis) -> str:
    # Render a deliberately non-executable skeleton for human review.
    return "\n".join(
        (
            f"def {name}():",
            "    # Candidate only; fill project-specific fixtures and imports.",
            f"    # Arrange: {hypothesis.suggested_inputs[0]}",
            f"    # Assert: {hypothesis.suggested_assertions[0]}",
            "    raise NotImplementedError",
        )
    )
def build_proposals(
    request: SurvivorAnalysisRequest,
    hypotheses: tuple[RepairHypothesis, ...],
    provider_suggestions: tuple[ProviderSuggestion, ...] = (),
    generation_source: GenerationSource = GenerationSource.DETERMINISTIC_TEMPLATE,
    provider_is_external: bool = False,
) -> tuple[TestProposal, ...]:
    # Build stable proposals and optionally apply non-authoritative provider text.
    target_file, target_nodeid = _target(request)
    suggestion_by_kind = {item.hypothesis_kind: item for item in provider_suggestions}
    proposals: list[TestProposal] = []
    for hypothesis in hypotheses:
        name = f"test_survivor_{_slug(hypothesis.kind)}_{_slug(request.mutant.mutant_id)}"
        suggestion = suggestion_by_kind.get(hypothesis.kind)
        arrangement = suggestion.arrangement or hypothesis.suggested_inputs if suggestion else hypothesis.suggested_inputs
        action = suggestion.action or (hypothesis.target_behavior,) if suggestion else (hypothesis.target_behavior,)
        assertions = suggestion.assertions or hypothesis.suggested_assertions if suggestion else hypothesis.suggested_assertions
        rationale = suggestion.rationale or hypothesis.explanation if suggestion else hypothesis.explanation
        generated_code = (suggestion.generated_code or _default_code(name, hypothesis)) if suggestion else _default_code(name, hypothesis)
        proposal_source = (
            GenerationSource.EXTERNAL_PROVIDER
            if suggestion and provider_is_external
            else (generation_source if suggestion else GenerationSource.DETERMINISTIC_TEMPLATE)
        )
        proposals.append(
            TestProposal(
                proposal_id=proposal_identity(request, hypothesis),
                target_test_file=target_file,
                target_test_nodeid=target_nodeid,
                proposed_test_name=name,
                arrangement=arrangement,
                action=action,
                assertions=assertions,
                rationale=rationale,
                expected_original_outcome="Candidate test passes on the original source.",
                expected_mutant_outcome="Candidate test fails on the target mutant.",
                imports_needed=("pytest",) if hypothesis.kind == "exception_contract" else (),
                fixtures_needed=("project-specific fixture for the mutated function",),
                generated_code=generated_code,
                generation_source=proposal_source,
            )
        )
    return tuple(proposals)

__all__ = ["build_proposals", "proposal_identity"]
