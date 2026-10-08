from __future__ import annotations


import pytest

from theseus_survivor_lab.serialization import request_from_dict
from theseus_survivor_lab.service import analyze_request

from conftest import FIXTURE_DIR, load_fixture_payload


FIXTURE_NAMES = sorted(
    path.name
    for path in FIXTURE_DIR.glob("*.json")
    if path.name != "real_gap.json"
)


@pytest.mark.parametrize("fixture_name", FIXTURE_NAMES)
def test_fixture_primary_category_matches_expected(fixture_name: str) -> None:
    # Lock the primary rule outcome for every mandatory survivor scenario.
    payload, expected = load_fixture_payload(fixture_name)
    result = analyze_request(request_from_dict(payload))
    assert result.classification.category.value == expected["primary_category"], fixture_name
    if "finding_kinds" in expected:
        observed = {finding.kind for finding in result.findings}
        assert set(expected["finding_kinds"]).issubset(observed), fixture_name
    if "blockers" in expected:
        assert result.blockers == tuple(expected["blockers"]), fixture_name


def test_classifier_is_provider_independent(real_gap_request) -> None:
    # Classification must stay identical when the optional provider is absent.
    from theseus_survivor_lab.providers import NullRepairProposalProvider
    from theseus_survivor_lab.service import SurvivorAnalysisService

    default_result = SurvivorAnalysisService().analyze(real_gap_request)
    null_result = SurvivorAnalysisService(NullRepairProposalProvider()).analyze(real_gap_request)
    assert default_result.classification == null_result.classification
    assert default_result.validation_plan == null_result.validation_plan


def test_selection_escape_also_produces_scope_repair_hypothesis() -> None:
    # Connect selection evidence to a concrete non-authoritative repair idea.
    payload, _ = load_fixture_payload("selection_escape.json")
    result = analyze_request(request_from_dict(payload))
    kinds = {item.kind for item in result.hypotheses}
    assert "selection_scope_repair" in kinds


def test_insufficient_evidence_has_explicit_blocker() -> None:
    # Prevent a missing execution record from being presented as a confident gap.
    payload, _ = load_fixture_payload("insufficient_evidence.json")
    result = analyze_request(request_from_dict(payload))
    assert result.classification.category.value == "insufficient_evidence"
    assert "semantic_evidence_incomplete" in result.blockers
    assert result.classification.evidence_ids
