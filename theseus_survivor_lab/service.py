"""Offline Survivor Lab orchestration with content-addressed result artifacts."""
from __future__ import annotations
import hashlib
from dataclasses import dataclass, replace
from .classification import ClassificationDecision, classify_survivor
from .context import build_survivor_causal_context, causal_context_to_dict, extract_causal_context, relevant_tests
from .contracts import (
    ProviderMetadata,
    RequestedMode,
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
    provider_response_from_dict,
    provider_response_to_dict,
)
from .serialization import canonical_json, content_addressed_result_id, request_to_dict
from .validation import (
    MAX_PROVIDER_SUGGESTIONS,
    MAX_PROVIDER_WARNINGS,
    normalize_request,
    sanitize_text,
    sha256_text,
    validate_result,
)
@dataclass(frozen=True)
class _ProviderBoundaryResult:
    """Sanitized provider output kept private to the orchestration boundary."""
    suggestions: tuple[ProviderSuggestion, ...]
    warnings: tuple[str, ...]
    response_sha256: str
def _stable_unique(values: tuple[str, ...]) -> tuple[str, ...]:
    # Remove duplicate warnings while preserving deterministic lexical order.
    return tuple(sorted(set(item for item in values if item)))
def _analysis_identity_payload(request: SurvivorAnalysisRequest) -> dict[str, object]:
    # Build local analysis identity from content hashes, not volatile inline text or paths.
    payload = request_to_dict(request)
    payload.pop("request_id", None)
    source = payload.get("source")
    if isinstance(source, dict):
        source["source_text"] = "<source-content-addressed>"
        source["function_source"] = "<function-source-derived>"
    for item in payload.get("related_tests", []):
        if isinstance(item, dict):
            item["source_text"] = "<test-content-addressed>"
    for item in payload.get("executions", []):
        if isinstance(item, dict):
            item["output_excerpt"] = "<execution-output-excluded>"
    return payload
def _content_id(prefix: str, payload: object) -> str:
    # Create a stable short content address from canonical UTF-8 JSON.
    digest = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    return f"{prefix}-{digest[:24]}"
def _analysis_id(request: SurvivorAnalysisRequest) -> str:
    # Identify only the deterministic local evidence analysis.
    return _content_id("analysis", _analysis_identity_payload(request))
def _provider_identity(provider: RepairProposalProvider) -> tuple[str, str, bool]:
    # Resolve explicit provider identity and fail closed for unknown implementations.
    provider_type = type(provider)
    default_name = f"{provider_type.__module__}.{provider_type.__qualname__}"
    name = sanitize_text(getattr(provider, "provider_name", default_name), 200) or default_name
    version = sanitize_text(getattr(provider, "provider_version", "unknown"), 100) or "unknown"
    deterministic = getattr(provider, "deterministic", False) is True
    return name, version, deterministic
def _provider_request_payload(request: ProviderRequest) -> dict[str, object]:
    # Serialize only the sanitized provider request for request identity.
    return {
        "request_id": request.request_id,
        "category": request.category.value,
        "mutant_operator": request.mutant_operator,
        "mutation_line": request.mutation_line,
        "source_excerpt": request.source_excerpt,
        "findings": list(request.findings),
        "context": causal_context_to_dict(request.context) if request.context is not None else None,
        "hypotheses": [
            {
                "hypothesis_id": item.hypothesis_id,
                "kind": item.kind,
                "title": item.title,
                "explanation": item.explanation,
                "target_behavior": item.target_behavior,
                "suggested_inputs": list(item.suggested_inputs),
                "suggested_assertions": list(item.suggested_assertions),
                "related_tests": list(item.related_tests),
                "confidence": item.confidence,
                "evidence_ids": list(item.evidence_ids),
            }
            for item in request.hypotheses
        ],
    }
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
    syntax_context,
    causal_context,
    hypotheses: tuple[RepairHypothesis, ...],
) -> ProviderRequest:
    # Construct the minimal sanitized context allowed for an optional provider.
    source_excerpt = "\n".join((*syntax_context.lines_before, syntax_context.mutation_line, *syntax_context.lines_after))
    finding_text = tuple(sanitize_text(finding.description, 800) or "" for finding in decision.findings)
    return ProviderRequest(
        request_id=sanitize_text(request.request_id, 200) or "",
        category=decision.classification.category,
        mutant_operator=sanitize_text(request.mutant.operator, 200) or "",
        mutation_line=sanitize_text(syntax_context.mutation_line, 500) or "",
        source_excerpt=sanitize_text(source_excerpt, 2400) or "",
        findings=finding_text,
        hypotheses=tuple(_sanitize_hypothesis(item) for item in hypotheses),
        context=causal_context,
    )
def _validate_provider_suggestion(value: object) -> ProviderSuggestion:
    # Validate one untrusted suggestion before sanitizing any of its fields.
    if not isinstance(value, ProviderSuggestion):
        raise TypeError("provider suggestion is not a ProviderSuggestion")
    if not isinstance(value.hypothesis_kind, str) or not value.hypothesis_kind.strip():
        raise TypeError("provider suggestion hypothesis_kind must be non-empty text")
    for field in ("title", "rationale"):
        if not isinstance(getattr(value, field), str):
            raise TypeError(f"provider suggestion {field} must be text")
    for field in ("arrangement", "action", "assertions"):
        items = getattr(value, field)
        if not isinstance(items, tuple) or len(items) > 12 or any(not isinstance(item, str) for item in items):
            raise TypeError(f"provider suggestion {field} must be a bounded tuple of strings")
    if value.generated_code is not None and not isinstance(value.generated_code, str):
        raise TypeError("provider suggestion generated_code must be text or null")
    return value
def _sanitize_suggestion(suggestion: ProviderSuggestion) -> ProviderSuggestion:
    # Bound and redact every provider-controlled proposal field.
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
    allowed_kinds: set[str],
) -> _ProviderBoundaryResult:
    # Call, validate, sanitize, and hash provider output inside one fail-safe boundary.
    try:
        response = provider.propose(request)
        if not isinstance(response, ProviderResponse):
            raise TypeError("provider returned a non-ProviderResponse value")
        response = provider_response_from_dict(provider_response_to_dict(response))
        if not isinstance(response.suggestions, tuple) or len(response.suggestions) > MAX_PROVIDER_SUGGESTIONS:
            raise TypeError("provider suggestions are not a bounded tuple")
        if not isinstance(response.warnings, tuple) or len(response.warnings) > MAX_PROVIDER_WARNINGS:
            raise TypeError("provider warnings are not a bounded tuple")
        warnings: list[str] = []
        for warning in response.warnings:
            if not isinstance(warning, str):
                raise TypeError("provider warnings must be strings")
            sanitized_warning = sanitize_text(warning, 800)
            if sanitized_warning:
                warnings.append(sanitized_warning)
        suggestions: list[ProviderSuggestion] = []
        seen_kinds: set[str] = set()
        for raw_suggestion in response.suggestions:
            suggestion = _sanitize_suggestion(_validate_provider_suggestion(raw_suggestion))
            if suggestion.hypothesis_kind not in allowed_kinds:
                warnings.append("provider_suggestion_has_unknown_hypothesis_kind")
                continue
            if suggestion.hypothesis_kind in seen_kinds:
                warnings.append("provider_duplicate_hypothesis_kind")
                continue
            seen_kinds.add(suggestion.hypothesis_kind)
            suggestions.append(suggestion)
        sanitized_payload = {
            "schema_version": response.schema_version,
            "suggestions": [
                {
                    "hypothesis_kind": item.hypothesis_kind,
                    "title": item.title,
                    "arrangement": list(item.arrangement),
                    "action": list(item.action),
                    "assertions": list(item.assertions),
                    "rationale": item.rationale,
                    "generated_code": item.generated_code,
                }
                for item in suggestions
            ],
            "warnings": list(_stable_unique(tuple(warnings))),
            "extensions": dict(response.extensions),
        }
        response_sha256 = sha256_text(canonical_json(sanitized_payload))
        return _ProviderBoundaryResult(tuple(suggestions), tuple(sorted(set(warnings))), response_sha256)
    except Exception as exc:
        fallback = {"status": "invalid", "error_type": type(exc).__name__}
        return _ProviderBoundaryResult(
            (),
            ("provider_returned_invalid_response",),
            sha256_text(canonical_json(fallback)),
        )
def _empty_validation_plan() -> ValidationPlan:
    # Return an explicit empty plan when the caller did not request validation planning.
    return ValidationPlan((), (), (), ())
def _validation_plan() -> ValidationPlan:
    # Build the fixed offline validation sequence required by the E24 contract.
    return ValidationPlan(
        original_checks=(
            ValidationStep(
                "original-pass",
                "Candidate passes on original",
                "pytest <candidate-nodeid>",
                "Confirm candidate correctness on original source.",
            ),
        ),
        mutant_checks=(
            ValidationStep(
                "mutant-kill",
                "Candidate fails on target mutant",
                "pytest <candidate-nodeid>",
                "Confirm the candidate distinguishes the target mutant.",
            ),
        ),
        regression_checks=(
            ValidationStep(
                "related-green",
                "Related tests remain green",
                "pytest <related-tests>",
                "Prevent an unrelated regression in connected behavior.",
            ),
        ),
        stability_checks=(
            ValidationStep(
                "repeat-original",
                "Repeat original candidate",
                "pytest <candidate-nodeid> --count=<n>",
                "Detect flaky or timing-only assertions.",
            ),
            ValidationStep(
                "workspace-independent",
                "Check workspace independence",
                "review candidate fixture paths",
                "Ensure the candidate does not depend on a temporary absolute path.",
            ),
            ValidationStep(
                "no-timing-only",
                "Check assertion stability",
                "review candidate assertions",
                "Ensure success is based on behavior rather than timing alone.",
            ),
        ),
    )
def _has_mode(request: SurvivorAnalysisRequest, mode: RequestedMode) -> bool:
    # Check one explicit pipeline mode without accepting arbitrary strings.
    return mode in request.requested_modes
def _proposal_set_payload(
    analysis_id: str,
    proposals,
    provider: ProviderMetadata,
) -> dict[str, object]:
    # Build proposal-set identity from complete proposal content and provider identity.
    return {
        "analysis_id": analysis_id,
        "provider": {
            "name": provider.name,
            "version": provider.version,
            "deterministic": provider.deterministic,
            "invoked": provider.invoked,
            "request_sha256": provider.request_sha256,
            "response_sha256": provider.response_sha256,
        },
        "proposals": [
            {
                "proposal_id": item.proposal_id,
                "target_test_file": item.target_test_file,
                "target_test_nodeid": item.target_test_nodeid,
                "proposed_test_name": item.proposed_test_name,
                "arrangement": list(item.arrangement),
                "action": list(item.action),
                "assertions": list(item.assertions),
                "rationale": item.rationale,
                "expected_original_outcome": item.expected_original_outcome,
                "expected_mutant_outcome": item.expected_mutant_outcome,
                "imports_needed": list(item.imports_needed),
                "fixtures_needed": list(item.fixtures_needed),
                "generated_code": item.generated_code,
                "generation_source": item.generation_source.value,
            }
            for item in proposals
        ],
    }
class SurvivorAnalysisService:
    """Analyze a survivor entirely offline with an explicit provider boundary."""
    def __init__(self, provider: RepairProposalProvider | None = None) -> None:
        # Configure an optional provider while keeping the default dependency-free.
        self._provider = provider if provider is not None else DeterministicTemplateProvider()
    def analyze(self, request: SurvivorAnalysisRequest) -> SurvivorAnalysisResult:
        # Run only the pipeline stages explicitly requested by the caller.
        normalized = normalize_request(request)
        filtered_tests = relevant_tests(normalized)
        context = extract_causal_context(
            normalized.source,
            normalized.mutant.line_no,
            filtered_tests,
        )
        decision = classify_survivor(normalized, context)
        causal_context = build_survivor_causal_context(
            normalized,
            context,
            decision.classification,
            decision.blockers,
            decision.warnings,
        )
        wants_hypotheses = _has_mode(normalized, RequestedMode.HYPOTHESES) or _has_mode(normalized, RequestedMode.PROPOSE)
        wants_proposals = _has_mode(normalized, RequestedMode.PROPOSE)
        hypotheses = generate_hypotheses(normalized, decision.classification, context) if wants_hypotheses else ()
        provider_name, provider_version, provider_deterministic = _provider_identity(self._provider)
        provider_warnings: tuple[str, ...] = ()
        provider_suggestions: tuple[ProviderSuggestion, ...] = ()
        if wants_proposals and causal_context.complete:
            provider_request = _provider_request(normalized, decision, context, causal_context, hypotheses)
            provider_request_sha256 = sha256_text(canonical_json(_provider_request_payload(provider_request)))
            boundary = _provider_response(
                self._provider,
                provider_request,
                {item.kind for item in hypotheses},
            )
            provider_suggestions = boundary.suggestions
            provider_warnings = boundary.warnings
            provider_metadata = ProviderMetadata(
                name=provider_name,
                version=provider_version,
                deterministic=provider_deterministic,
                invoked=True,
                request_sha256=provider_request_sha256,
                response_sha256=boundary.response_sha256,
            )
        else:
            provider_metadata = ProviderMetadata(
                name="not-invoked",
                version="0",
                deterministic=True,
                invoked=False,
                request_sha256=None,
                response_sha256=None,
            )
            if wants_proposals and not causal_context.complete:
                provider_warnings = ("provider_blocked_by_incomplete_causal_context",)
        proposals = (
            build_proposals(
                normalized,
                hypotheses,
                provider_suggestions,
                provider_is_external=not provider_deterministic,
            )
            if wants_proposals
            else ()
        )
        plan = _validation_plan() if wants_proposals or _has_mode(normalized, RequestedMode.VALIDATION_PLAN) else _empty_validation_plan()
        warnings = _stable_unique(decision.warnings + context.warnings + causal_context.uncertainty + provider_warnings)
        analysis_id = _analysis_id(normalized)
        proposal_set_id = _content_id(
            "proposal-set",
            _proposal_set_payload(analysis_id, proposals, provider_metadata),
        )
        pending = SurvivorAnalysisResult(
            schema_version=1,
            request_id=normalized.request_id,
            analysis_id=analysis_id,
            proposal_set_id=proposal_set_id,
            result_id="result-pending",
            classification=decision.classification,
            confidence=decision.classification.confidence,
            findings=decision.findings,
            hypotheses=hypotheses,
            proposals=proposals,
            validation_plan=plan,
            provider=provider_metadata,
            blockers=decision.blockers,
            warnings=warnings,
        )
        result_id = content_addressed_result_id(pending)
        result = replace(pending, result_id=result_id)
        validate_result(result)
        return result
def analyze_request(
    request: SurvivorAnalysisRequest,
    provider: RepairProposalProvider | None = None,
) -> SurvivorAnalysisResult:
    # Analyze one standalone request through the public synchronous facade.
    return SurvivorAnalysisService(provider=provider).analyze(request)
