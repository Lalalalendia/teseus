"""Offline Survivor Lab service orchestration."""

from __future__ import annotations

import hashlib
from dataclasses import replace

from .classification import ClassificationDecision, classify_survivor
from .context import extract_causal_context
from .contracts import (
    RepairHypothesis,
    SurvivorAnalysisRequest,
    SurvivorAnalysisResult,
    ValidationPlan,
    ValidationStep,
)
from .hypotheses import generate_hypotheses
from .proposals import build_proposals
from .providers import (
    DeterministicTemplateProvider,
    ProviderRequest,
    ProviderResponse,
    ProviderSuggestion,
    RepairProposalProvider,
)
from .serialization import canonical_json, request_to_dict
from .validation import normalize_request, sanitize_text, validate_result


def _stable_unique(values: tuple[str, ...]) -> tuple[str, ...]:
    # Remove duplicate warnings while preserving deterministic lexical order.
    return tuple(sorted(set(values)))


def _identity_payload(request: SurvivorAnalysisRequest) -> dict[str, object]:
    # Build result identity from semantic evidence, excluding volatile text fields.
    payload = request_to_dict(request)
    source = payload["source"]
    if isinstance(source, dict):
        source["source_text"] = "<inline-source-excluded>"
        source["function_source"] = "<function-source-excluded>"
    for item in payload.get("related_tests", []):
        if isinstance(item, dict):
            item["source_text"] = "<inline-test-source-excluded>"
            item["source_sha256"] = item.get("source_sha256") or "<no-test-source-hash>"
    for item in payload.get("executions", []):
        if isinstance(item, dict):
            item["output_excerpt"] = "<execution-output-excluded>"
    return payload


def _result_id(
    request: SurvivorAnalysisRequest,
    decision: ClassificationDecision,
    hypotheses: tuple[RepairHypothesis, ...],
    proposal_ids: tuple[str, ...],
) -> str:
    # Generate a deterministic result identity independent of time, PID, and absolute paths.
    payload = {
        "request": _identity_payload(request),
        "category": decision.classification.category.value,
        "hypothesis_kinds": [item.kind for item in hypotheses],
        "proposal_ids": list(proposal_ids),
    }
    digest = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    return f"result-{digest[:24]}"


def _sanitize_hypothesis(hypothesis: RepairHypothesis) -> RepairHypothesis:
    # Bound and sanitize hypothesis text before it crosses a provider boundary.
    return replace(
        hypothesis,
        title=sanitize_text(hypothesis.title, 600) or "",
        explanation=sanitize_text(hypothesis.explanation, 1200) or "",
        target_behavior=sanitize_text(hypothesis.target_behavior, 1000) or "",
        suggested_inputs=tuple(sanitize_text(item, 400) or "" for item in hypothesis.suggested_inputs),
        suggested_assertions=tuple(sanitize_text(item, 500) or "" for item in hypothesis.suggested_assertions),
        related_tests=tuple(sanitize_text(item, 300) or "" for item in hypothesis.related_tests),
    )


def _provider_request(
    request: SurvivorAnalysisRequest,
    decision: ClassificationDecision,
    context,
    hypotheses: tuple[RepairHypothesis, ...],
) -> ProviderRequest:
    # Construct the minimal sanitized context allowed for an optional provider.
    source_excerpt = "\n".join(
        (*context.lines_before, context.mutation_line, *context.lines_after)
    )
    finding_text = tuple(
        sanitize_text(finding.description, 800) or ""
        for finding in decision.findings
    )
    return ProviderRequest(
        request_id=sanitize_text(request.request_id, 200) or "",
        category=decision.classification.category,
        mutant_operator=sanitize_text(request.mutant.operator, 200) or "",
        mutation_line=sanitize_text(context.mutation_line, 500) or "",
        source_excerpt=sanitize_text(source_excerpt, 2400) or "",
        findings=finding_text,
        hypotheses=tuple(_sanitize_hypothesis(item) for item in hypotheses),
    )


def _sanitize_suggestion(suggestion: ProviderSuggestion) -> ProviderSuggestion:
    # Bound untrusted provider text before it becomes a result artifact.
    return ProviderSuggestion(
        hypothesis_kind=sanitize_text(suggestion.hypothesis_kind, 200) or "",
        title=sanitize_text(suggestion.title, 500) or "",
        arrangement=tuple(sanitize_text(item, 500) or "" for item in suggestion.arrangement[:12]),
        action=tuple(sanitize_text(item, 800) or "" for item in suggestion.action[:12]),
        assertions=tuple(sanitize_text(item, 800) or "" for item in suggestion.assertions[:12]),
        rationale=sanitize_text(suggestion.rationale, 1200) or "",
        generated_code=sanitize_text(suggestion.generated_code, 3000),
    )


def _provider_response(
    provider: RepairProposalProvider,
    request: ProviderRequest,
) -> tuple[tuple[ProviderSuggestion, ...], tuple[str, ...]]:
    # Call an optional provider defensively and keep local analysis authoritative.
    try:
        response = provider.propose(request)
    except Exception as exc:  # pragma: no cover - provider failure is tested through a fake.
        return (), (f"provider_failed:{type(exc).__name__}",)
    if not isinstance(response, ProviderResponse):
        return (), ("provider_returned_invalid_response",)
    return tuple(_sanitize_suggestion(item) for item in response.suggestions), tuple(response.warnings)


def _validation_plan() -> ValidationPlan:
    # Build the fixed offline validation sequence required by the E24 contract.
    return ValidationPlan(
        original_checks=(
            ValidationStep("original-pass", "Candidate passes on original", "pytest <candidate-nodeid>", "Confirm candidate correctness on original source."),
        ),
        mutant_checks=(
            ValidationStep("mutant-kill", "Candidate fails on target mutant", "pytest <candidate-nodeid>", "Confirm the candidate distinguishes the target mutant."),
        ),
        regression_checks=(
            ValidationStep("related-green", "Related tests remain green", "pytest <related-tests>", "Prevent an unrelated regression in connected behavior."),
        ),
        stability_checks=(
            ValidationStep("repeat-original", "Repeat original candidate", "pytest <candidate-nodeid> --count=<n>", "Detect flaky or timing-only assertions."),
            ValidationStep("workspace-independent", "Check workspace independence", "review candidate fixture paths", "Ensure the candidate does not depend on a temporary absolute path."),
            ValidationStep("no-timing-only", "Check assertion stability", "review candidate assertions", "Ensure success is based on behavior rather than timing alone."),
        ),
    )


class SurvivorAnalysisService:
    """Analyze a survivor entirely offline with a replaceable provider boundary."""

    def __init__(self, provider: RepairProposalProvider | None = None) -> None:
        # Configure an optional provider while keeping the default dependency-free.
        self._provider = provider if provider is not None else DeterministicTemplateProvider()

    def analyze(self, request: SurvivorAnalysisRequest) -> SurvivorAnalysisResult:
        # Run the complete local evidence-to-proposal pipeline.
        normalized = normalize_request(request)
        context = extract_causal_context(
            normalized.source,
            normalized.mutant.line_no,
            normalized.related_tests,
        )
        decision = classify_survivor(normalized, context)
        hypotheses = generate_hypotheses(normalized, decision.classification, context)
        provider_request = _provider_request(normalized, decision, context, hypotheses)
        provider_suggestions, provider_warnings = _provider_response(self._provider, provider_request)
        proposals = build_proposals(
            normalized,
            hypotheses,
            provider_suggestions,
        )
        warnings = _stable_unique(decision.warnings + provider_warnings)
        result = SurvivorAnalysisResult(
            schema_version=1,
            request_id=normalized.request_id,
            result_id=_result_id(
                normalized,
                decision,
                hypotheses,
                tuple(item.proposal_id for item in proposals),
            ),
            classification=decision.classification,
            confidence=decision.classification.confidence,
            findings=decision.findings,
            hypotheses=hypotheses,
            proposals=proposals,
            validation_plan=_validation_plan(),
            blockers=decision.blockers,
            warnings=warnings,
        )
        validate_result(result)
        return result


def analyze_request(
    request: SurvivorAnalysisRequest,
    provider: RepairProposalProvider | None = None,
) -> SurvivorAnalysisResult:
    # Analyze one request through the standalone service facade.
    return SurvivorAnalysisService(provider=provider).analyze(request)
