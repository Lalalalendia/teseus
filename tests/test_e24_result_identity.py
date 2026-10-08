from __future__ import annotations

from dataclasses import replace

from theseus_survivor_lab.contracts import TestEvidence as Evidence
from theseus_survivor_lab.providers import ProviderResponse, ProviderSuggestion
from theseus_survivor_lab.serialization import result_to_dict
from theseus_survivor_lab.service import SurvivorAnalysisService, analyze_request
from theseus_survivor_lab.validation import sha256_text


class _ProviderOne:
    """Return one external proposal payload."""

    provider_name = "fake-provider"
    provider_version = "7"
    deterministic = False

    def propose(self, request):
        # Produce bounded external content for identity assertions.
        del request
        return ProviderResponse(
            (
                ProviderSuggestion(
                    hypothesis_kind="boundary_input",
                    title="first provider title",
                    arrangement=("input=3",),
                    action=("call decide",),
                    assertions=("assert result is False",),
                    rationale="first rationale",
                    generated_code="assert decide(3) is False",
                ),
            ),
            (),
        )


class _ProviderTwo(_ProviderOne):
    """Return a different external proposal payload with the same identity."""

    def propose(self, request):
        # Change content while keeping provider name/version stable.
        del request
        return ProviderResponse(
            (
                ProviderSuggestion(
                    hypothesis_kind="boundary_input",
                    title="second provider title",
                    arrangement=("input=4",),
                    action=("call decide",),
                    assertions=("assert result is True",),
                    rationale="second rationale",
                    generated_code="assert decide(4) is True",
                ),
            ),
            (),
        )


def test_provider_content_changes_proposal_and_result_identity(real_gap_request) -> None:
    # Result identity must include complete provider-enriched artifact content.
    first = SurvivorAnalysisService(_ProviderOne()).analyze(real_gap_request)
    second = SurvivorAnalysisService(_ProviderTwo()).analyze(real_gap_request)
    assert first.analysis_id == second.analysis_id
    assert first.proposal_set_id != second.proposal_set_id
    assert first.result_id != second.result_id
    assert first.provider.request_sha256 == second.provider.request_sha256
    assert first.provider.response_sha256 != second.provider.response_sha256
    assert first.proposals[0].generation_source.value == "external_provider"
    assert result_to_dict(first)["provider"]["deterministic"] is False


def test_changed_inline_related_test_source_changes_analysis_identity(real_gap_request) -> None:
    # Test evidence IDs must be content-addressed rather than nodeid-only.
    nodeid = "tests/test_decision.py::test_target"
    source_one = "def test_target():\n    assert decide(3) is False\n"
    source_two = "def test_target():\n    assert decide(3) is True\n"

    def make_request(source_text: str):
        # Build one otherwise identical request around one test source version.
        related = Evidence(
            nodeid=nodeid,
            source_path="tests/test_decision.py",
            source_sha256=sha256_text(source_text),
            source_text=source_text,
            selection_reasons=("coverage",),
            executions=1,
            failures=0,
            median_duration_ms=1.0,
            killed_related_mutants=(),
        )
        return replace(real_gap_request, related_tests=(related,))

    first = analyze_request(make_request(source_one))
    second = analyze_request(make_request(source_two))
    assert first.analysis_id != second.analysis_id
    assert first.result_id != second.result_id


def test_semantically_same_source_in_different_checkouts_keeps_analysis_identity(real_gap_request) -> None:
    # Canonical project-relative paths remove machine-specific checkout roots.
    first_payload = replace(
        real_gap_request,
        mutant=replace(real_gap_request.mutant, source_path="C:/Users/A/project/src/decision.py"),
        source=replace(real_gap_request.source, source_path="C:/Users/A/project/src/decision.py"),
    )
    second_payload = replace(
        real_gap_request,
        mutant=replace(real_gap_request.mutant, source_path="/home/b/project/src/decision.py"),
        source=replace(real_gap_request.source, source_path="/home/b/project/src/decision.py"),
    )
    first = analyze_request(first_payload)
    second = analyze_request(second_payload)
    assert first.analysis_id == second.analysis_id
    assert first.result_id == second.result_id
