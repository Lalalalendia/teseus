"""Deterministic repair hypotheses derived from survivor evidence."""

from __future__ import annotations

import hashlib
import re

from .contracts import (
    CausalContext,
    RepairHypothesis,
    SurvivorAnalysisRequest,
    SurvivorCategory,
    SurvivorClassification,
)


def _hypothesis_id(kind: str, request: SurvivorAnalysisRequest) -> str:
    # Generate a deterministic hypothesis identity without physical paths.
    payload = "|".join(
        (
            request.mutant.mutant_id,
            request.mutant.operator,
            request.mutant.original,
            request.mutant.replacement,
            kind,
        )
    )
    return f"hyp-{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:16]}"


def _evidence(request: SurvivorAnalysisRequest, classification: SurvivorClassification) -> tuple[str, ...]:
    # Reuse classifier evidence references for every generated hypothesis.
    return classification.evidence_ids or (f"mutant:{request.mutant.mutant_id}",)


def _related_tests(request: SurvivorAnalysisRequest) -> tuple[str, ...]:
    # Select stable related test nodeids for proposal targeting.
    return tuple(item.nodeid for item in request.related_tests)


def _number_boundaries(text: str) -> tuple[str, ...]:
    # Turn numeric literals near a comparison into boundary candidates.
    values = [int(item) for item in re.findall(r"(?<![A-Za-z_])[-+]?\d+(?![A-Za-z_])", text)]
    if not values:
        return ("boundary - 1", "boundary", "boundary + 1")
    boundary = values[-1]
    return (str(boundary - 1), str(boundary), str(boundary + 1))


def _base_hypothesis(
    request: SurvivorAnalysisRequest,
    classification: SurvivorClassification,
    kind: str,
    title: str,
    explanation: str,
    target_behavior: str,
    suggested_inputs: tuple[str, ...],
    suggested_assertions: tuple[str, ...],
    confidence: float,
) -> RepairHypothesis:
    # Construct one immutable hypothesis with stable identity and ordering.
    return RepairHypothesis(
        hypothesis_id=_hypothesis_id(kind, request),
        kind=kind,
        title=title,
        explanation=explanation,
        target_behavior=target_behavior,
        suggested_inputs=suggested_inputs,
        suggested_assertions=suggested_assertions,
        related_tests=_related_tests(request),
        confidence=confidence,
        evidence_ids=_evidence(request, classification),
    )


def generate_hypotheses(
    request: SurvivorAnalysisRequest,
    classification: SurvivorClassification,
    context: CausalContext,
) -> tuple[RepairHypothesis, ...]:
    # Generate bounded, deterministic repair hypotheses without executing code.
    context_text = " ".join(
        (
            context.mutation_line,
            *context.nearby_conditions,
            *context.return_expressions,
            *context.referenced_names,
        )
    )
    operator_text = f"{request.mutant.operator} {request.mutant.original} {request.mutant.replacement} {context_text}".lower()
    category = classification.category
    hypotheses: list[RepairHypothesis] = []
    if any(token in operator_text for token in (">", "<", ">=", "<=", "boundary", "comparison")):
        hypotheses.append(
            _base_hypothesis(
                request,
                classification,
                "boundary_input",
                "Exercise the comparison boundary",
                "The mutation changes a comparison outcome. Use values immediately below, at, and immediately above the boundary.",
                "The branch and returned behavior must change at the exact comparison boundary.",
                _number_boundaries(f"{operator_text} {' '.join(context.nearby_conditions)}"),
                ("assert the exact branch or returned value", "assert the boundary and neighboring cases separately"),
                0.9,
            )
        )
    if any(token in operator_text for token in ("and", "or", "not", "boolean", "condition")):
        hypotheses.append(
            _base_hypothesis(
                request,
                classification,
                "boolean_truth_table",
                "Cover distinguishing boolean combinations",
                "The mutation changes boolean composition or condition polarity. Include combinations where exactly one operand is true.",
                "The original and mutant must disagree for every truth-table row that the contract distinguishes.",
                ("left=True, right=False", "left=False, right=True", "left=False, right=False", "left=True, right=True"),
                ("assert the expected result for each distinguishing row", "assert that the short-circuit branch is or is not reached"),
                0.88,
            )
        )
    if "return" in operator_text or request.mutant.operator.lower() in {"return", "literal", "constant"}:
        hypotheses.append(
            _base_hypothesis(
                request,
                classification,
                "return_observation",
                "Assert the returned value precisely",
                "The mutation appears to replace a return expression or returned literal. A broad truthiness assertion may miss it.",
                "The exact return value, type, and relevant side effects must satisfy the function contract.",
                ("the normal input", "the input that reaches the mutated return"),
                ("assert exact equality", "assert the expected type", "assert relevant observable side effects"),
                0.9,
            )
        )
    if "raise" in operator_text or "exception" in operator_text:
        hypotheses.append(
            _base_hypothesis(
                request,
                classification,
                "exception_contract",
                "Assert the required exception contract",
                "The mutation appears to remove or alter an exception path.",
                "Invalid input must raise the documented exception type and preserve its observable message contract.",
                ("a valid input", "an invalid input that enters the exception branch"),
                ("assert the exception type", "assert the exception is raised before the forbidden side effect"),
                0.92,
            )
        )
    if "pass" in operator_text or "call" in operator_text or "removed" in operator_text:
        hypotheses.append(
            _base_hypothesis(
                request,
                classification,
                "side_effect_observation",
                "Observe the removed interaction or side effect",
                "The mutation appears to remove a standalone call or side effect.",
                "The required interaction must occur exactly when the contract requires it and must not occur otherwise.",
                ("an input that should trigger the interaction", "an input that should not trigger the interaction"),
                ("assert the call or state transition", "assert call arguments", "assert no duplicate interaction"),
                0.84,
            )
        )
    if not hypotheses:
        hypotheses.append(
            _base_hypothesis(
                request,
                classification,
                "behavior_observation",
                "Add an observation for the mutated behavior",
                "The available evidence identifies a survivor but not a more specific mutation family.",
                "Add a focused assertion that distinguishes original and replacement behavior.",
                ("the smallest input reaching the mutation",),
                ("assert the changed observable result",),
                0.62,
            )
        )
    if category == SurvivorCategory.SELECTION_ESCAPE:
        hypotheses.append(
            _base_hypothesis(
                request,
                classification,
                "selection_scope_repair",
                "Include the related test in selection",
                "The current selection omitted a test already connected to the mutated behavior.",
                "The selected test set must contain the related test or an equivalent coverage-backed replacement.",
                ("the existing related test nodeid",),
                ("assert that the test is selected for this location", "assert that it kills the mutant"),
                0.95,
            )
        )
    return tuple(hypotheses)
