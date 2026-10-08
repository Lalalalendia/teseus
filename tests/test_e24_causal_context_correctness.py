from __future__ import annotations

from dataclasses import replace

from theseus_survivor_lab.context import extract_causal_context
from theseus_survivor_lab.contracts import SourceEvidence, TestEvidence as Evidence
from theseus_survivor_lab.service import analyze_request
from theseus_survivor_lab.validation import sha256_text


def _source() -> SourceEvidence:
    # Provide a small source file with two similarly shaped test functions.
    source_text = "def decide(x):\n    if x > 3:\n        return True\n    return False\n"
    return SourceEvidence("src/decision.py", sha256_text(source_text), source_text, None, 1, 4)


def test_nodeid_selects_target_function_instead_of_first_definition() -> None:
    # AST context must follow the pytest nodeid, including parameter suffixes.
    test_text = (
        "def test_first():\n"
        "    first_only = 1\n"
        "    assert first_only\n\n"
        "def test_target(value):\n"
        "    target_only = value + 1\n"
        "    assert target_only\n"
    )
    test = Evidence(
        nodeid="tests/test_app.py::test_target[value]",
        source_path="tests/test_app.py",
        source_sha256=sha256_text(test_text),
        source_text=test_text,
        selection_reasons=("coverage",),
        executions=1,
        failures=0,
        median_duration_ms=1.0,
        killed_related_mutants=(),
    )
    context = extract_causal_context(_source(), 2, (test,))
    fragment = context.related_test_fragments[0]
    assert "target_only" in fragment.excerpt
    assert "first_only" not in fragment.excerpt
    assert "target_only" in fragment.referenced_names


def test_missing_nodeid_scope_is_explicitly_warned() -> None:
    # Fallback extraction remains bounded but cannot be mistaken for an exact match.
    test_text = "def test_actual():\n    assert True\n"
    test = Evidence(
        nodeid="tests/test_app.py::test_missing",
        source_path="tests/test_app.py",
        source_sha256=sha256_text(test_text),
        source_text=test_text,
        selection_reasons=("coverage",),
        executions=1,
        failures=0,
        median_duration_ms=1.0,
        killed_related_mutants=(),
    )
    request = replace(
        _request(),
        related_tests=(test,),
        requested_modes=("classify",),
    )
    result = analyze_request(request)
    assert "test_nodeid_scope_not_found" in result.warnings


def test_untrusted_related_test_does_not_create_selection_escape() -> None:
    # Mere presence in related_test_nodeids is not causal evidence.
    selection = replace(
        _request().selection,
        related_test_nodeids=("tests/test_decision.py::test_other",),
        selection_reasons=(),
    )
    result = analyze_request(replace(_request(), selection=selection))
    assert result.classification.category.value == "real_test_gap"
    assert not any(item.kind == "selection_escape" for item in result.findings)


def test_similar_mutant_history_can_prove_selection_escape() -> None:
    # Historical killing of a declared similar mutant is trusted evidence.
    base = _request()
    selection = replace(base.selection, similar_mutant_ids=("m-similar",))
    related = Evidence(
        nodeid="tests/test_decision.py::test_boundary",
        source_path=None,
        source_sha256=None,
        source_text=None,
        selection_reasons=(),
        executions=1,
        failures=0,
        median_duration_ms=1.0,
        killed_related_mutants=("m-similar",),
    )
    result = analyze_request(replace(base, selection=selection, related_tests=(related,)))
    assert result.classification.category.value == "selection_escape"


def test_causal_ast_condition_can_supply_boundary_signal() -> None:
    # Local context participates when the adapter did not classify the operator family.
    base = _request()
    mutant = replace(base.mutant, operator="unknown_operator", original="x", replacement="y")
    selection = replace(base.selection, boundary_values_observed=False)
    result = analyze_request(replace(base, mutant=mutant, selection=selection))
    assert result.classification.category.value == "test_data_gap"
    assert any("context:" in item for item in result.classification.evidence_ids)


def _request():
    # Load the repository fixture without duplicating its evidence fields.
    from conftest import load_fixture_payload
    from theseus_survivor_lab.serialization import request_from_dict

    payload, _ = load_fixture_payload("real_gap.json")
    return request_from_dict(payload)
