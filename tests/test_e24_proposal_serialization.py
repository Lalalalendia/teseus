from __future__ import annotations

from theseus_survivor_lab.serialization import (
    canonical_json,
    result_from_dict,
    result_to_dict,
    result_to_markdown,
)
from theseus_survivor_lab.service import SurvivorAnalysisService, analyze_request
from theseus_survivor_lab.providers import NullRepairProposalProvider


def test_result_round_trip_is_byte_stable(real_gap_request) -> None:
    # Check nested hypotheses, proposals, and validation plan round-trip exactly.
    result = analyze_request(real_gap_request)
    restored = result_from_dict(result_to_dict(result))
    assert restored == result
    assert canonical_json(result_to_dict(restored)) == canonical_json(result_to_dict(result))


def test_markdown_is_projection_of_result(real_gap_request) -> None:
    # Markdown must expose analysis fields without introducing a second classifier.
    result = analyze_request(real_gap_request)
    markdown = result_to_markdown(result)
    assert "# Theseus Survivor Lab report" in markdown
    assert result.classification.category.value in markdown
    assert result.classification.reason in markdown
    assert "## Validation plan" in markdown
    assert "absolute-path" not in markdown


def test_null_provider_still_generates_local_proposals(real_gap_request) -> None:
    # Core proposal generation must not depend on an external provider.
    result = SurvivorAnalysisService(NullRepairProposalProvider()).analyze(real_gap_request)
    assert result.proposals
    assert all(item.generation_source.value == "deterministic_template" for item in result.proposals)


def test_proposal_target_path_is_relative(real_gap_request) -> None:
    # Proposal targets must never preserve a machine-specific absolute path.
    from dataclasses import replace

    request = replace(
        real_gap_request,
        related_tests=(
            replace(
                real_gap_request.related_tests[0]
                if real_gap_request.related_tests
        else __import__("theseus_survivor_lab").TestEvidence(
                    nodeid="tests/test.py::test_x",
                    source_path="C:/checkout/tests/test.py",
                    source_sha256=None,
                    source_text=None,
                    selection_reasons=(),
                    executions=0,
                    failures=0,
                    median_duration_ms=None,
                    killed_related_mutants=(),
                ),
                source_path="C:/checkout/tests/test.py",
            ),
        ),
    )
    result = analyze_request(request)
    assert result.proposals[0].target_test_file == "tests/test.py"
