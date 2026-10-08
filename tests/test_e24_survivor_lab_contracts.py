from __future__ import annotations

import copy

import pytest

from theseus_survivor_lab.errors import ContractError, UnsupportedSchemaVersion
from theseus_survivor_lab.serialization import (
    canonical_json,
    request_from_dict,
    request_to_dict,
    result_from_dict,
    result_to_dict,
)
from theseus_survivor_lab.service import analyze_request
from theseus_survivor_lab.validation import normalize_request, sha256_text, validate_request

from conftest import load_fixture_payload


def test_request_round_trip_preserves_contract(real_gap_request) -> None:
    # Verify that the versioned input contract survives JSON serialization.
    encoded = request_to_dict(real_gap_request)
    decoded = request_from_dict(encoded)
    assert decoded == real_gap_request


def test_unknown_fields_are_rejected() -> None:
    # Keep the hand-written decoder aligned with the strict published schemas.
    payload, _ = load_fixture_payload("real_gap.json")
    payload["future_optional"] = {"producer": "later"}
    payload["selection"]["future_selection_note"] = "ignored"
    payload["source"]["future_artifact_ref"] = {"id": "ignored"}
    with pytest.raises(ContractError, match="unknown fields"):
        request_from_dict(payload)


def test_missing_required_field_is_rejected() -> None:
    # Reject incomplete evidence rather than silently fabricating required data.
    payload, _ = load_fixture_payload("real_gap.json")
    del payload["mutant"]["operator"]
    with pytest.raises(ContractError, match="missing required field"):
        request_from_dict(payload)


def test_unsupported_schema_version_is_rejected() -> None:
    # Refuse a future schema that this package cannot interpret safely.
    payload, _ = load_fixture_payload("real_gap.json")
    payload["schema_version"] = 99
    with pytest.raises(UnsupportedSchemaVersion):
        request_from_dict(payload)


def test_source_hash_is_verified() -> None:
    # Ensure inline source evidence cannot be silently substituted.
    payload, _ = load_fixture_payload("real_gap.json")
    payload["source"]["source_sha256"] = sha256_text("different source")
    with pytest.raises(ContractError, match="does not match"):
        validate_request(request_from_dict(payload))


def test_deterministic_result_and_byte_identical_json(real_gap_request) -> None:
    # Repeated analysis must produce the same identity and canonical bytes.
    first = analyze_request(real_gap_request)
    second = analyze_request(real_gap_request)
    assert first.result_id == second.result_id
    assert canonical_json(result_to_dict(first)) == canonical_json(result_to_dict(second))
    assert result_from_dict(result_to_dict(first)) == first


def test_absolute_paths_do_not_enter_result_identity() -> None:
    # Physical checkout paths must normalize to the same semantic result.
    first_payload, _ = load_fixture_payload("real_gap.json")
    second_payload = copy.deepcopy(first_payload)
    for payload in (first_payload, second_payload):
        payload["mutant"]["source_path"] = "C:/Users/Alex/project/src/decision.py"
        payload["source"]["source_path"] = "C:/Users/Alex/project/src/decision.py"
    second_payload["mutant"]["source_path"] = "/home/alex/project/src/decision.py"
    second_payload["source"]["source_path"] = "/home/alex/project/src/decision.py"
    first = analyze_request(request_from_dict(first_payload))
    second = analyze_request(request_from_dict(second_payload))
    assert first.result_id == second.result_id
    result_json = canonical_json(result_to_dict(first))
    assert "C:/" not in result_json
    assert "/home/" not in result_json


def test_normalized_paths_are_relative(real_gap_request) -> None:
    # Normalize Windows and POSIX paths before any semantic projection.
    payload = request_to_dict(real_gap_request)
    payload["source"]["source_path"] = "D:/checkout/src/decision.py"
    normalized = normalize_request(request_from_dict(payload))
    assert normalized.source.source_path == "src/decision.py"


def test_invalid_enum_is_rejected(real_gap_request) -> None:
    # Reject unknown classification enum values on result input.
    result = analyze_request(real_gap_request)
    payload = result_to_dict(result)
    payload["classification"]["category"] = "made_up_category"
    with pytest.raises(ContractError, match="unsupported value"):
        result_from_dict(payload)
