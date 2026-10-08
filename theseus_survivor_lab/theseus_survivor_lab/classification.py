"""Deterministic, explainable survivor classification rules."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from .contracts import (
    AnalysisFinding,
    CausalContext,
    SurvivorAnalysisRequest,
    SurvivorCategory,
    SurvivorClassification,
)


@dataclass(frozen=True)
class ClassificationDecision:
    """Classification plus findings and lifecycle blockers."""

    classification: SurvivorClassification
    findings: tuple[AnalysisFinding, ...]
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]


def _evidence_id(kind: str, value: str) -> str:
    # Generate a stable compact evidence identifier.
    digest = hashlib.sha256(f"{kind}:{value}".encode("utf-8")).hexdigest()[:12]
    return f"{kind}:{digest}"


def _finding(
    kind: str,
    title: str,
    description: str,
    evidence_ids: tuple[str, ...],
    severity: str,
) -> AnalysisFinding:
    # Build a deterministic explainability finding.
    finding_id = _evidence_id("finding", f"{kind}|{title}|{'|'.join(evidence_ids)}")
    return AnalysisFinding(finding_id, kind, title, description, evidence_ids, severity)


def _execution_ids(request: SurvivorAnalysisRequest) -> tuple[str, ...]:
    # Return stable evidence identifiers for all recorded executions.
    return tuple(_evidence_id("execution", item.execution_id) for item in request.executions)


def _related_test_ids(request: SurvivorAnalysisRequest) -> tuple[str, ...]:
    # Return stable evidence identifiers for related tests.
    return tuple(_evidence_id("test", item.nodeid) for item in request.related_tests)


def _selected_tests(request: SurvivorAnalysisRequest) -> set[str]:
    # Combine planner selection and execution-level selected tests.
    selected = set(request.selection.selected_tests)
    for execution in request.executions:
        selected.update(execution.selected_tests)
    return selected


def _is_boundary_mutation(request: SurvivorAnalysisRequest) -> bool:
    # Detect comparison and boundary-oriented mutation operators.
    operator = request.mutant.operator.lower()
    text = f"{request.mutant.original} {request.mutant.replacement}".lower()
    return bool(re.search(r"(?:>=|<=|>|<|==|!=|boundary|comparison)", f"{operator} {text}"))


def _is_boolean_mutation(request: SurvivorAnalysisRequest) -> bool:
    # Detect boolean operator and condition-negation mutations.
    text = f"{request.mutant.operator} {request.mutant.original} {request.mutant.replacement}".lower()
    return any(token in text for token in ("and", "or", "not", "condition", "boolean", "truth"))


def _valid_execution(request: SurvivorAnalysisRequest) -> bool:
    # Check whether execution evidence is usable for semantic conclusions.
    return bool(request.executions) and all(
        not item.infrastructure_failure and item.restore_verified and not item.timed_out
        for item in request.executions
    )


def _add_signal(
    findings: list[AnalysisFinding],
    kind: str,
    title: str,
    description: str,
    evidence_ids: tuple[str, ...],
    severity: str = "warning",
) -> None:
    # Append a signal only once while preserving deterministic order.
    if any(item.kind == kind for item in findings):
        return
    findings.append(_finding(kind, title, description, evidence_ids, severity))


def classify_survivor(
    request: SurvivorAnalysisRequest,
    context: CausalContext | None = None,
) -> ClassificationDecision:
    # Apply ordered local rules and never delegate classification to a provider.
    del context
    findings: list[AnalysisFinding] = []
    execution_ids = _execution_ids(request)
    test_ids = _related_test_ids(request)
    mutant_id = _evidence_id("mutant", request.mutant.mutant_id)
    selection_id = _evidence_id("selection", request.request_id)
    environment_id = _evidence_id("environment", request.request_id)
    selected = _selected_tests(request)
    related = set(request.selection.related_test_nodeids)
    related.update(item.nodeid for item in request.related_tests)
    related.update(
        item.nodeid
        for item in request.related_tests
        if request.mutant.mutant_id in item.killed_related_mutants
    )
    missing_related = tuple(sorted(related - selected))
    infra_evidence = tuple(
        _evidence_id("execution", item.execution_id)
        for item in request.executions
        if item.infrastructure_failure or not item.restore_verified
    )
    if infra_evidence:
        _add_signal(
            findings,
            "infrastructure_ambiguity",
            "Execution integrity is not proven",
            "At least one execution reported infrastructure failure or did not verify source restoration.",
            infra_evidence,
            "blocker",
        )
        classification = SurvivorClassification(
            SurvivorCategory.INFRASTRUCTURE_AMBIGUITY,
            "The evidence cannot distinguish a surviving mutant from an invalid execution because infrastructure or restoration integrity is unresolved.",
            0.99,
            infra_evidence,
            (),
        )
        return ClassificationDecision(classification, tuple(findings), ("execution_integrity_unproven",), ())

    timeout_evidence = tuple(
        _evidence_id("execution", item.execution_id) for item in request.executions if item.timed_out
    )
    if request.executions and timeout_evidence and len(timeout_evidence) == len(request.executions):
        _add_signal(
            findings,
            "timeout_ambiguity",
            "All executions timed out",
            "No completed semantic observation is available because every recorded execution timed out.",
            timeout_evidence,
            "blocker",
        )
        classification = SurvivorClassification(
            SurvivorCategory.TIMEOUT_AMBIGUITY,
            "The survivor status is ambiguous because every execution timed out before a reliable result was observed.",
            0.98,
            timeout_evidence,
            (),
        )
        return ClassificationDecision(classification, tuple(findings), ("timeout_prevented_semantic_observation",), ())

    if request.environment and (request.environment.differences or request.environment.stable is False):
        _add_signal(
            findings,
            "environment_dependent",
            "Environment varies across observations",
            "The bundle records environment differences or an unstable environment that may change the outcome.",
            (environment_id,),
        )

    if missing_related:
        evidence = (selection_id, *test_ids)
        _add_signal(
            findings,
            "selection_escape",
            "Relevant tests escaped selection",
            f"Related tests were identified but absent from selected execution: {', '.join(missing_related[:6])}.",
            evidence,
        )

    static_unreachable = (request.selection.static_reachability or "").lower() in {
        "unreachable",
        "dead",
        "unreachable_code",
    }
    if request.selection.runtime_location_reached is False and static_unreachable:
        _add_signal(
            findings,
            "unreachable_code",
            "Mutation location appears unreachable",
            "Runtime evidence did not reach the mutation and static reachability evidence marks the location as unreachable.",
            (mutant_id, selection_id),
        )

    if request.selection.equivalent_observation is True and request.selection.reachable_paths_proven:
        _add_signal(
            findings,
            "equivalent_suspected",
            "Equivalent behavior is suspected",
            "All currently proven reachable paths produced the same observed effect for original and replacement.",
            (mutant_id, selection_id),
        )

    oracle_missing = request.selection.oracle_observed is False or request.selection.assertions_observed is False
    if request.selection.runtime_location_reached is True and oracle_missing:
        _add_signal(
            findings,
            "weak_oracle",
            "Relevant execution lacks an observing oracle",
            "The mutation was reached, but the available assertions did not observe the changed value or branch.",
            (mutant_id, selection_id, *test_ids),
        )
        if request.selection.assertions_observed is False:
            _add_signal(
                findings,
                "assertion_too_broad",
                "Assertions are broader than the mutated behavior",
                "The bundle explicitly reports that assertions execute without checking the mutated behavior precisely.",
                (selection_id, *test_ids),
            )

    if (
        _is_boundary_mutation(request)
        and request.selection.boundary_values_observed is False
        and request.selection.runtime_location_reached is not False
    ):
        _add_signal(
            findings,
            "test_data_gap",
            "Boundary-discriminating data is missing",
            "The mutation changes a comparison, but no evidence shows values at or around the distinguishing boundary.",
            (mutant_id, selection_id, *test_ids),
        )

    if (
        _is_boolean_mutation(request)
        and request.selection.truth_table_observed is False
        and request.selection.runtime_location_reached is not False
    ):
        _add_signal(
            findings,
            "test_data_gap",
            "Distinguishing truth-table rows are missing",
            "The mutation changes boolean composition or condition polarity, but no evidence covers the rows that distinguish original and mutant.",
            (mutant_id, selection_id, *test_ids),
        )

    valid = _valid_execution(request)
    if not valid and request.executions:
        _add_signal(
            findings,
            "insufficient_evidence",
            "Execution evidence is incomplete",
            "The bundle contains executions, but they do not establish a complete semantic observation for this rule set.",
            execution_ids,
            "warning",
        )

    signal_kinds = {item.kind for item in findings}
    if "selection_escape" in signal_kinds:
        category = SurvivorCategory.SELECTION_ESCAPE
        reason = "Relevant tests or tests that kill similar mutants were not present in the selected execution scope."
        confidence = 0.94
    elif "environment_dependent" in signal_kinds:
        category = SurvivorCategory.ENVIRONMENT_DEPENDENT
        reason = "Recorded environment differences make the survivor outcome dependent on an unstable or varying environment."
        confidence = 0.86
    elif "unreachable_code" in signal_kinds:
        category = SurvivorCategory.UNREACHABLE_CODE
        reason = "The mutation has no runtime reachability evidence and static context marks its branch as unreachable."
        confidence = 0.88
    elif "equivalent_suspected" in signal_kinds:
        category = SurvivorCategory.EQUIVALENT_SUSPECTED
        reason = "Original and replacement have the same observed effect on all currently proven reachable paths."
        confidence = 0.78
    elif "assertion_too_broad" in signal_kinds:
        category = SurvivorCategory.ASSERTION_TOO_BROAD
        reason = "The available assertions are too broad to distinguish the original behavior from the mutant."
        confidence = 0.9
    elif "weak_oracle" in signal_kinds:
        category = SurvivorCategory.WEAK_ORACLE
        reason = "The selected tests reach the mutation but do not assert the behavior it changes."
        confidence = 0.91
    elif "test_data_gap" in signal_kinds:
        category = SurvivorCategory.TEST_DATA_GAP
        reason = "The execution is valid, but test data does not exercise the values that distinguish the mutation."
        confidence = 0.9
    elif valid and request.selection.runtime_location_reached is True and request.selection.observed_effects_changed is True:
        _add_signal(
            findings,
            "real_test_gap",
            "Reachable behavior changed without a kill",
            "The mutant changes an observed behavior, yet no selected test failed on it.",
            (mutant_id, selection_id, *test_ids),
            "info",
        )
        category = SurvivorCategory.REAL_TEST_GAP
        reason = "Execution is valid and the mutant changes reachable behavior, but the current tests do not detect it."
        confidence = 0.93
    else:
        category = SurvivorCategory.INSUFFICIENT_EVIDENCE
        reason = "The bundle does not provide enough reliable evidence to distinguish a test gap from another survivor cause."
        confidence = 0.52
        if not request.executions:
            _add_signal(
                findings,
                "insufficient_evidence",
                "No execution evidence",
                "No execution record was supplied, so semantic survivor conclusions are provisional.",
                (mutant_id,),
                "blocker",
            )

    secondary = tuple(
        category_value
        for category_value in SurvivorCategory
        if category_value.value in signal_kinds and category_value != category
    )
    evidence_ids = tuple(
        dict.fromkeys(
            item
            for finding in findings
            for item in finding.evidence_ids
        )
    )
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    if category == SurvivorCategory.INSUFFICIENT_EVIDENCE:
        blockers = ("semantic_evidence_incomplete",)
    if "environment_dependent" in signal_kinds:
        warnings = ("environment_variation_requires_revalidation",)
    classification = SurvivorClassification(category, reason, confidence, evidence_ids, secondary)
    return ClassificationDecision(classification, tuple(findings), blockers, warnings)
