from __future__ import annotations

from dataclasses import replace

from theseus_survivor_lab.providers import ProviderResponse, ProviderSuggestion
from theseus_survivor_lab.service import SurvivorAnalysisService
from theseus_survivor_lab.validation import sanitize_text


class _MalformedProvider:
    """Return an invalid provider response shape."""

    def propose(self, request):
        # Exercise the response type guard without raising inside the provider.
        del request
        return ProviderResponse(("not-a-suggestion",), ())


class _WarningProvider:
    """Return hostile warning text."""

    def propose(self, request):
        # Include a quoted secret and an oversized warning in the response.
        del request
        return ProviderResponse((), ('{"api_key": "TOPSECRET"}', "x" * 10_000))


class _DuplicateProvider:
    """Return unknown and duplicate suggestion kinds."""

    def propose(self, request):
        # Only one valid kind may enrich the local proposal set.
        del request
        suggestion = ProviderSuggestion(
            hypothesis_kind="boundary_input",
            title="valid",
            arrangement=("input=3",),
            action=("call decide",),
            assertions=("assert exact result",),
            rationale="valid rationale",
            generated_code=None,
        )
        return ProviderResponse(
            (
                replace(suggestion, hypothesis_kind="invented_kind"),
                suggestion,
                replace(suggestion, title="duplicate"),
            ),
            (),
        )


class _EmptyFieldsProvider:
    """Return a structurally valid but empty suggestion."""

    def propose(self, request):
        # Local deterministic fields must survive empty provider fields.
        del request
        return ProviderResponse(
            (
                ProviderSuggestion(
                    hypothesis_kind="boundary_input",
                    title="",
                    arrangement=(),
                    action=(),
                    assertions=(),
                    rationale="",
                    generated_code=None,
                ),
            ),
            (),
        )


def test_malformed_provider_response_falls_back_without_crashing(real_gap_request) -> None:
    # A malformed response must become a bounded warning, not an AttributeError.
    result = SurvivorAnalysisService(_MalformedProvider()).analyze(real_gap_request)
    assert result.proposals
    assert "provider_returned_invalid_response" in result.warnings


def test_provider_warnings_are_redacted_and_bounded(real_gap_request) -> None:
    # Provider warnings cannot leak secrets or inflate the result artifact.
    result = SurvivorAnalysisService(_WarningProvider()).analyze(real_gap_request)
    rendered = "\n".join(result.warnings)
    assert "TOPSECRET" not in rendered
    assert "api_key" in rendered
    assert all(len(item) <= 820 for item in result.warnings)


def test_classify_only_does_not_invoke_provider(real_gap_request) -> None:
    # Explicit classify mode must not cross the optional provider boundary.
    class NeverCalled:
        def propose(self, request):
            # Fail loudly if mode gating regresses.
            raise AssertionError("provider was called for classify-only request")

    request = replace(real_gap_request, requested_modes=("classify",))
    result = SurvivorAnalysisService(NeverCalled()).analyze(request)
    assert result.provider.invoked is False
    assert result.hypotheses == ()
    assert result.proposals == ()
    assert result.validation_plan.original_checks == ()


def test_unknown_and_duplicate_provider_kinds_are_reported(real_gap_request) -> None:
    # Provider output cannot silently overwrite local hypotheses.
    result = SurvivorAnalysisService(_DuplicateProvider()).analyze(real_gap_request)
    assert "provider_suggestion_has_unknown_hypothesis_kind" in result.warnings
    assert "provider_duplicate_hypothesis_kind" in result.warnings
    assert len(result.proposals) == 1


def test_empty_provider_fields_fall_back_to_local_proposal(real_gap_request) -> None:
    # Empty provider fields must not erase deterministic proposal content.
    result = SurvivorAnalysisService(_EmptyFieldsProvider()).analyze(real_gap_request)
    proposal = result.proposals[0]
    assert proposal.arrangement
    assert proposal.action
    assert proposal.assertions
    assert proposal.rationale
    assert proposal.generated_code


def test_secret_sanitizer_covers_json_uri_and_private_key_forms() -> None:
    # Common structured credential encodings must be redacted consistently.
    text = (
        '{"api_key": "TOPSECRET", "token": "VALUE"} '
        "https://alice:password@example.com/path "
        "-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----"
    )
    sanitized = sanitize_text(text)
    assert sanitized is not None
    assert "TOPSECRET" not in sanitized
    assert "VALUE" not in sanitized
    assert "alice:password@" not in sanitized
    assert "BEGIN PRIVATE KEY" not in sanitized
