from __future__ import annotations

import math
import copy
from dataclasses import replace

import pytest

from theseus_survivor_lab.contracts import SourceEvidence, TestEvidence as Evidence
from theseus_survivor_lab.errors import ContractError
from theseus_survivor_lab.service import analyze_request
from theseus_survivor_lab.serialization import result_from_dict, result_to_dict
from theseus_survivor_lab.validation import (
    MAX_SOURCE_TEXT_BYTES,
    sha256_text,
    validate_request,
)


def test_invalid_execution_cannot_be_presented_as_real_gap(real_gap_request) -> None:
    # An error execution is ambiguous even when restoration was reported true.
    execution = replace(
        real_gap_request.executions[0],
        status="error",
        exit_code=1,
        infrastructure_failure=False,
        restore_verified=True,
        observed_tests=(),
    )
    result = analyze_request(replace(real_gap_request, executions=(execution,)))
    assert result.classification.category.value == "infrastructure_ambiguity"
    assert "execution_integrity_unproven" in result.blockers


def test_mutant_and_source_paths_must_match(real_gap_request) -> None:
    # A bundle that joins evidence from different production files is rejected.
    source = replace(real_gap_request.source, source_path="src/other.py")
    with pytest.raises(ContractError, match="source_path"):
        validate_request(replace(real_gap_request, source=source))


def test_inline_test_source_requires_its_content_hash(real_gap_request) -> None:
    # Inline test content without a digest cannot participate in identity or causality.
    related = Evidence(
        nodeid="tests/test_decision.py::test_target",
        source_path="tests/test_decision.py",
        source_sha256=None,
        source_text="def test_target():\n    assert True\n",
        selection_reasons=("coverage",),
        executions=1,
        failures=0,
        median_duration_ms=1.0,
        killed_related_mutants=(),
    )
    with pytest.raises(ContractError, match="source_sha256 is required"):
        validate_request(replace(real_gap_request, related_tests=(related,)))


def test_function_source_must_match_authoritative_slice(real_gap_request) -> None:
    # A free-form function projection cannot override the source text.
    source = replace(real_gap_request.source, function_source="def forged():\n    return False")
    with pytest.raises(ContractError, match="function_source"):
        validate_request(replace(real_gap_request, source=source))


def test_unknown_requested_mode_is_rejected(real_gap_request) -> None:
    # The mode boundary is closed so future behavior cannot be invoked accidentally.
    with pytest.raises(ContractError, match="requested_modes"):
        validate_request(replace(real_gap_request, requested_modes=("classify", "send_to_llm")))


def test_non_finite_test_duration_is_rejected(real_gap_request) -> None:
    # NaN and infinity must not enter identity, JSON, or selection decisions.
    related = Evidence(
        nodeid="tests/test_decision.py::test_target",
        source_path=None,
        source_sha256=None,
        source_text=None,
        selection_reasons=("coverage",),
        executions=1,
        failures=0,
        median_duration_ms=math.nan,
        killed_related_mutants=(),
    )
    with pytest.raises(ContractError, match="finite"):
        validate_request(replace(real_gap_request, related_tests=(related,)))


def test_oversized_source_is_rejected_before_ast_authentication(real_gap_request) -> None:
    # Resource limits must stop oversized evidence before parsing it.
    oversized = "x" * (MAX_SOURCE_TEXT_BYTES + 1)
    source = SourceEvidence(
        source_path="src/decision.py",
        source_sha256=sha256_text(oversized),
        source_text=oversized,
        function_source=None,
        function_start_line=None,
        function_end_line=None,
    )
    with pytest.raises(ContractError, match="maximum size"):
        validate_request(replace(real_gap_request, source=source))


def test_malformed_nested_result_is_rejected(real_gap_request) -> None:
    # Published result validation must recurse into every DTO rather than trust shape alone.
    result = analyze_request(real_gap_request)
    payload = result_to_dict(result)
    malformed = copy.deepcopy(payload)
    malformed["findings"] = ["not-a-finding"]
    with pytest.raises(ContractError, match="JSON object|malformed"):
        result_from_dict(malformed)


def test_result_and_classification_confidence_must_match(real_gap_request) -> None:
    # A projection cannot publish two contradictory confidence values.
    result = analyze_request(real_gap_request)
    payload = result_to_dict(result)
    payload["confidence"] = 0.1
    with pytest.raises(ContractError, match="must equal"):
        result_from_dict(payload)


def test_result_id_is_verified_against_full_artifact(real_gap_request) -> None:
    # A result identifier cannot be changed without changing the serialized artifact.
    result = analyze_request(real_gap_request)
    payload = result_to_dict(result)
    payload["result_id"] = "result-000000000000000000000000"
    with pytest.raises(ContractError, match="result_id"):
        result_from_dict(payload)
