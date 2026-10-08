"""Optional proposal-provider boundary with no concrete SDK dependency."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol
from .contracts import RepairHypothesis, SurvivorCausalContext, SurvivorCategory
from .errors import ContractError, UnsupportedSchemaVersion
PROVIDER_RESPONSE_SCHEMA_VERSION = 1
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
    context: SurvivorCausalContext | None = None
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
    """Versioned provider output that can enrich proposals but never verify them."""
    suggestions: tuple[ProviderSuggestion, ...]
    warnings: tuple[str, ...]
    schema_version: int = PROVIDER_RESPONSE_SCHEMA_VERSION
    extensions: Mapping[str, Any] = field(default_factory=dict)
def _suggestion_to_dict(value: ProviderSuggestion) -> dict[str, Any]:
    # Serialize one provider suggestion without interpreting its untrusted content.
    return {
        "hypothesis_kind": value.hypothesis_kind,
        "title": value.title,
        "arrangement": list(value.arrangement),
        "action": list(value.action),
        "assertions": list(value.assertions),
        "rationale": value.rationale,
        "generated_code": value.generated_code,
    }
def provider_response_to_dict(value: ProviderResponse) -> dict[str, Any]:
    # Serialize one provider response with explicit version and extension policy.
    if not isinstance(value, ProviderResponse):
        raise ContractError("provider response must be a ProviderResponse")
    extensions = dict(value.extensions)
    invalid = tuple(sorted(key for key in extensions if not isinstance(key, str) or not key.startswith("x-")))
    if invalid:
        raise ContractError(f"provider response extensions must use x- keys: {invalid}")
    return {
        "schema_version": value.schema_version,
        "suggestions": [_suggestion_to_dict(item) for item in value.suggestions],
        "warnings": list(value.warnings),
        "extensions": extensions,
    }
def _tuple_strings(value: object, field_name: str) -> tuple[str, ...]:
    # Decode one provider string array and reject malformed values fail closed.
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ContractError(f"{field_name} must be an array of strings")
    return tuple(value)
def _suggestion_from_dict(value: object) -> ProviderSuggestion:
    # Decode one strict provider suggestion while rejecting unknown nested fields.
    if not isinstance(value, dict):
        raise ContractError("provider suggestion must be a JSON object")
    allowed = {"hypothesis_kind", "title", "arrangement", "action", "assertions", "rationale", "generated_code"}
    unknown = tuple(sorted(set(value) - allowed))
    if unknown:
        raise ContractError(f"provider suggestion contains unknown fields: {unknown}")
    for field_name in ("hypothesis_kind", "title", "rationale"):
        if not isinstance(value.get(field_name), str):
            raise ContractError(f"provider suggestion {field_name} must be text")
    generated_code = value.get("generated_code")
    if generated_code is not None and not isinstance(generated_code, str):
        raise ContractError("provider suggestion generated_code must be text or null")
    return ProviderSuggestion(
        hypothesis_kind=value["hypothesis_kind"],
        title=value["title"],
        arrangement=_tuple_strings(value.get("arrangement"), "provider suggestion arrangement"),
        action=_tuple_strings(value.get("action"), "provider suggestion action"),
        assertions=_tuple_strings(value.get("assertions"), "provider suggestion assertions"),
        rationale=value["rationale"],
        generated_code=generated_code,
    )
def provider_response_from_dict(value: object) -> ProviderResponse:
    # Decode versioned provider output and preserve only explicit x- forward extensions.
    if not isinstance(value, dict):
        raise ContractError("provider response must be a JSON object")
    schema_version = value.get("schema_version")
    if schema_version != PROVIDER_RESPONSE_SCHEMA_VERSION:
        raise UnsupportedSchemaVersion(f"unsupported provider response schema_version: {schema_version!r}")
    known = {"schema_version", "suggestions", "warnings", "extensions"}
    unknown = {key: item for key, item in value.items() if key not in known}
    invalid_unknown = tuple(sorted(key for key in unknown if not isinstance(key, str) or not key.startswith("x-")))
    if invalid_unknown:
        raise ContractError(f"provider response contains unsupported fields: {invalid_unknown}")
    extensions = value.get("extensions", {})
    if not isinstance(extensions, dict):
        raise ContractError("provider response extensions must be a JSON object")
    merged_extensions = {**extensions, **unknown}
    invalid_extensions = tuple(sorted(key for key in merged_extensions if not isinstance(key, str) or not key.startswith("x-")))
    if invalid_extensions:
        raise ContractError(f"provider response extensions must use x- keys: {invalid_extensions}")
    suggestions = value.get("suggestions")
    warnings = value.get("warnings")
    if not isinstance(suggestions, list):
        raise ContractError("provider response suggestions must be an array")
    return ProviderResponse(
        suggestions=tuple(_suggestion_from_dict(item) for item in suggestions),
        warnings=_tuple_strings(warnings, "provider response warnings"),
        schema_version=schema_version,
        extensions=merged_extensions,
    )
class RepairProposalProvider(Protocol):
    """Protocol for optional deterministic or external proposal generators."""
    def propose(self, request: ProviderRequest) -> ProviderResponse:
        # Return non-authoritative proposal suggestions for sanitized context.
        ...
class NullRepairProposalProvider:
    """Provider that proves the core package does not require an external service."""
    provider_name = "null"
    provider_version = "1"
    deterministic = True
    def propose(self, request: ProviderRequest) -> ProviderResponse:
        # Return no external suggestions while preserving local analysis behavior.
        del request
        return ProviderResponse((), ())
class DeterministicTemplateProvider:
    """Render stable suggestions from local hypotheses without an LLM."""
    provider_name = "deterministic-template"
    provider_version = "1"
    deterministic = True
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
