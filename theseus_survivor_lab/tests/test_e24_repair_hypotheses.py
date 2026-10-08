from __future__ import annotations

from theseus_survivor_lab.serialization import request_from_dict
from theseus_survivor_lab.service import analyze_request

from conftest import load_fixture_payload


def _hypotheses(fixture_name: str):
    # Analyze one fixture and return the immutable generated hypotheses.
    payload, _ = load_fixture_payload(fixture_name)
    request = request_from_dict(payload)
    result = analyze_request(request)
    return result.hypotheses


def test_boundary_hypothesis_contains_neighboring_values() -> None:
    # Comparison repairs must include below, equal, and above cases.
    hypotheses = _hypotheses("condition_boundary_gap.json")
    boundary = next(item for item in hypotheses if item.kind == "boundary_input")
    assert boundary.suggested_inputs == ("2", "3", "4")
    assert "exact comparison boundary" in boundary.target_behavior


def test_boolean_hypothesis_contains_truth_table_rows() -> None:
    # Boolean repairs must propose rows that distinguish and/or behavior.
    hypotheses = _hypotheses("and_or_truth_table_gap.json")
    truth_table = next(item for item in hypotheses if item.kind == "boolean_truth_table")
    assert "left=True, right=False" in truth_table.suggested_inputs
    assert len(truth_table.suggested_inputs) == 4


def test_exception_and_side_effect_hypotheses_are_specific() -> None:
    # Exception and removed-call mutations must produce different contracts.
    exception = _hypotheses("raise_removed.json")
    side_effect = _hypotheses("side_effect_call_removed.json")
    assert any(item.kind == "exception_contract" for item in exception)
    assert any(item.kind == "side_effect_observation" for item in side_effect)


def test_hypothesis_ids_are_stable_and_do_not_include_paths() -> None:
    # Hypothesis identity must survive repeated offline analysis.
    first = _hypotheses("condition_boundary_gap.json")
    second = _hypotheses("condition_boundary_gap.json")
    assert [item.hypothesis_id for item in first] == [item.hypothesis_id for item in second]
    assert all("/" not in item.hypothesis_id for item in first)
