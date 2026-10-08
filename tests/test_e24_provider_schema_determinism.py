from __future__ import annotations
from dataclasses import replace
import pytest
from theseus_survivor_adapter import ProposalEvidenceStatus, SurvivorProposalPipeline, SurvivorProviderSubmission, SurvivorRuntimeAdapter
from theseus_survivor_lab.errors import ContractError
from theseus_survivor_lab.providers import (
    ProviderResponse,
    ProviderSuggestion,
    provider_response_from_dict,
    provider_response_to_dict,
)
from theseus_survivor_lab.serialization import canonical_json
from theseus_survivor_lab.service import SurvivorAnalysisService
from test_e24_survivor_runtime_adapter import _evidence
def _response() -> ProviderResponse:
    # Build one versioned deterministic provider response for round-trip checks.
    return ProviderResponse(
        suggestions=(
            ProviderSuggestion(
                hypothesis_kind="boundary_input",
                title="boundary candidate",
                arrangement=("input=3",),
                action=("call decide",),
                assertions=("assert exact result",),
                rationale="exercise the exact boundary",
                generated_code=None,
            ),
        ),
        warnings=("provider warning",),
        extensions={"x-trace-format": {"version": 1}},
    )
def test_provider_response_round_trip_is_byte_and_identity_stable(real_gap_request) -> None:
    # Typed and serialized provider responses must produce identical result identities.
    response = _response()
    payload = provider_response_to_dict(response)
    restored = provider_response_from_dict(payload)
    assert restored == response
    assert canonical_json(provider_response_to_dict(restored)) == canonical_json(payload)
    class OriginalProvider:
        provider_name = "fixture"
        provider_version = "1"
        deterministic = True
        def propose(self, request):
            # Return the original typed response without external I/O.
            del request
            return response
    class RestoredProvider:
        provider_name = "fixture"
        provider_version = "1"
        deterministic = True
        def propose(self, request):
            # Return the deserialized response without external I/O.
            del request
            return restored
    first = SurvivorAnalysisService(OriginalProvider()).analyze(real_gap_request)
    second = SurvivorAnalysisService(RestoredProvider()).analyze(real_gap_request)
    assert first.result_id == second.result_id
    assert first.provider.response_sha256 == second.provider.response_sha256
def test_forward_extensions_are_preserved_but_unknown_core_fields_fail_closed() -> None:
    # Only explicitly namespaced x- fields participate in forward-compatible decoding.
    payload = provider_response_to_dict(_response())
    payload["x-future-capability"] = {"enabled": True}
    restored = provider_response_from_dict(payload)
    assert restored.extensions["x-future-capability"] == {"enabled": True}
    malformed = {**payload, "future_capability": True}
    with pytest.raises(ContractError, match="unsupported fields"):
        provider_response_from_dict(malformed)
def test_malformed_provider_wire_output_is_rejected_evidence() -> None:
    # A malformed serialized response must remain rejected evidence and never become verified.
    source = SurvivorRuntimeAdapter().analyze(_evidence())
    result = SurvivorProposalPipeline().generate(
        source,
        SurvivorProviderSubmission(
            provider_name="wire-provider",
            provider_version="1",
            deterministic=False,
            response={"schema_version": 1, "suggestions": "not-an-array", "warnings": []},
        ),
    )
    assert result.rejected
    assert any("provider_response_schema_invalid" in item.reasons for item in result.rejected)
    assert all(item.status is ProposalEvidenceStatus.REJECTED for item in result.rejected)
def test_serialized_provider_response_can_enter_pr39_without_verification() -> None:
    # A valid wire response may create accepted evidence but cannot create a fresh verification receipt.
    source = SurvivorRuntimeAdapter().analyze(_evidence())
    hypothesis = source.analysis.hypotheses[0]
    response = ProviderResponse((ProviderSuggestion(
        hypothesis_kind=hypothesis.kind,
        title=hypothesis.title,
        arrangement=hypothesis.suggested_inputs,
        action=(hypothesis.target_behavior,),
        assertions=hypothesis.suggested_assertions,
        rationale=hypothesis.explanation,
        generated_code=None,
    ),), ())
    result = SurvivorProposalPipeline().generate(
        source,
        SurvivorProviderSubmission(
            provider_name="wire-provider",
            provider_version="1",
            deterministic=False,
            response=provider_response_to_dict(response),
        ),
    )
    assert result.accepted
    assert all(item.status is ProposalEvidenceStatus.ACCEPTED for item in result.accepted)
    assert not hasattr(result.accepted[0], "verified")
def test_provider_secrets_and_absolute_paths_do_not_enter_result(real_gap_request) -> None:
    # Untrusted provider text must be redacted before result serialization and identity hashing.
    response = _response()
    hostile = replace(
        response,
        suggestions=(replace(response.suggestions[0], rationale="token=TOPSECRET C:/Temp/private.txt"),),
    )
    class HostileProvider:
        provider_name = "hostile"
        provider_version = "1"
        deterministic = False
        def propose(self, request):
            # Return hostile text to exercise deterministic redaction.
            del request
            return hostile
    result = SurvivorAnalysisService(HostileProvider()).analyze(real_gap_request)
    rendered = canonical_json({"proposals": [item.rationale for item in result.proposals]})
    assert "TOPSECRET" not in rendered
    assert "C:/Temp" not in rendered
    assert "<redacted>" in rendered and "<absolute-path>" in rendered
