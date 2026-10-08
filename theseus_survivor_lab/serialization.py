"""JSON codecs and deterministic projections for Survivor Lab contracts."""
from __future__ import annotations
import json
import math
import unicodedata
from importlib.resources import files
from pathlib import Path
from typing import Any, TypeVar
from .contracts import (
    AnalysisFinding,
    DependencyEvidence,
    EnvironmentEvidence,
    ExecutionEvidence,
    ExecutionStatus,
    GenerationSource,
    MutantEvidence,
    MutantStatus,
    ProviderMetadata,
    RepairHypothesis,
    SelectionEvidence,
    SourceEvidence,
    SurvivorAnalysisRequest,
    SurvivorAnalysisResult,
    SurvivorCategory,
    SurvivorClassification,
    TestEvidence,
    TestProposal,
    ValidationPlan,
    ValidationStep,
)
from .errors import ContractError, UnsupportedSchemaVersion
T = TypeVar("T")
def _mapping(value: object, field: str) -> dict[str, Any]:
    # Require a JSON object at a contract boundary.
    if not isinstance(value, dict):
        raise ContractError(f"{field} must be a JSON object")
    return value
def _reject_unknown(data: dict[str, Any], allowed: set[str], field: str) -> None:
    # Keep the hand-written decoder and published JSON schemas equally strict.
    unknown = sorted(set(data) - allowed)
    if unknown:
        raise ContractError(f"{field} contains unknown fields: {', '.join(unknown)}")
def _required(data: dict[str, Any], key: str, field: str) -> Any:
    # Read a required field and provide a stable contract error.
    if key not in data:
        raise ContractError(f"missing required field: {field}.{key}")
    return data[key]
def _enum_value(value: object) -> object:
    # Serialize StrEnum values and preserve already-serialized compatibility strings.
    return getattr(value, "value", value)
def _text(data: dict[str, Any], key: str, field: str) -> str:
    # Read a required string field.
    value = _required(data, key, field)
    if not isinstance(value, str):
        raise ContractError(f"{field}.{key} must be a string")
    return value
def _optional_text(data: dict[str, Any], key: str, field: str) -> str | None:
    # Read an optional string field while ignoring absent optional metadata.
    value = data.get(key)
    if value is not None and not isinstance(value, str):
        raise ContractError(f"{field}.{key} must be a string or null")
    return value
def _int(data: dict[str, Any], key: str, field: str) -> int:
    # Read a required integer without accepting booleans.
    value = _required(data, key, field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractError(f"{field}.{key} must be an integer")
    return value
def _optional_int(data: dict[str, Any], key: str, field: str) -> int | None:
    # Read an optional integer without accepting booleans.
    value = data.get(key)
    if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
        raise ContractError(f"{field}.{key} must be an integer or null")
    return value
def _optional_float(data: dict[str, Any], key: str, field: str) -> float | None:
    # Read an optional finite numeric field.
    value = data.get(key)
    if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float))):
        raise ContractError(f"{field}.{key} must be a number or null")
    if value is not None and not math.isfinite(float(value)):
        raise ContractError(f"{field}.{key} must be finite")
    return float(value) if value is not None else None
def _bool(data: dict[str, Any], key: str, field: str) -> bool:
    # Read a required boolean field.
    value = _required(data, key, field)
    if not isinstance(value, bool):
        raise ContractError(f"{field}.{key} must be a boolean")
    return value
def _optional_bool(data: dict[str, Any], key: str, field: str) -> bool | None:
    # Read an optional boolean field.
    value = data.get(key)
    if value is not None and not isinstance(value, bool):
        raise ContractError(f"{field}.{key} must be a boolean or null")
    return value
def _tuple_text(data: dict[str, Any], key: str, field: str, required: bool = False) -> tuple[str, ...]:
    # Read a tuple-shaped JSON array containing strings.
    value = _required(data, key, field) if required else data.get(key, [])
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ContractError(f"{field}.{key} must be an array of strings")
    return tuple(value)
def _enum(value: object, enum_type: type[T], field: str) -> T:
    # Convert a serialized enum value and reject unknown values.
    try:
        return enum_type(value)  # type: ignore[call-arg]
    except (TypeError, ValueError) as exc:
        raise ContractError(f"{field} contains an unsupported value: {value!r}") from exc
def _mutant_to_dict(value: MutantEvidence) -> dict[str, Any]:
    # Serialize mutant evidence into JSON-compatible primitives.
    return {
        "mutant_id": value.mutant_id,
        "operator": value.operator,
        "operator_version": value.operator_version,
        "source_path": value.source_path,
        "function_id": value.function_id,
        "class_name": value.class_name,
        "line_no": value.line_no,
        "column_no": value.column_no,
        "original": value.original,
        "replacement": value.replacement,
        "diff": value.diff,
        "status": _enum_value(value.status),
    }
def _mutant_from_dict(value: object) -> MutantEvidence:
    # Deserialize mutant evidence against the exact versioned contract.
    data = _mapping(value, "mutant")
    _reject_unknown(
        data,
        {
            "mutant_id", "operator", "operator_version", "source_path", "function_id",
            "class_name", "line_no", "column_no", "original", "replacement", "diff", "status",
        },
        "mutant",
    )
    return MutantEvidence(
        mutant_id=_text(data, "mutant_id", "mutant"),
        operator=_text(data, "operator", "mutant"),
        operator_version=_text(data, "operator_version", "mutant"),
        source_path=_text(data, "source_path", "mutant"),
        function_id=_optional_text(data, "function_id", "mutant"),
        class_name=_optional_text(data, "class_name", "mutant"),
        line_no=_int(data, "line_no", "mutant"),
        column_no=_int(data, "column_no", "mutant"),
        original=_text(data, "original", "mutant"),
        replacement=_text(data, "replacement", "mutant"),
        diff=_optional_text(data, "diff", "mutant"),
        status=_enum(_text(data, "status", "mutant"), MutantStatus, "mutant.status"),
    )
def _source_to_dict(value: SourceEvidence) -> dict[str, Any]:
    # Serialize source evidence into JSON-compatible primitives.
    return {
        "source_path": value.source_path,
        "source_sha256": value.source_sha256,
        "source_text": value.source_text,
        "function_source": value.function_source,
        "function_start_line": value.function_start_line,
        "function_end_line": value.function_end_line,
    }
def _source_from_dict(value: object) -> SourceEvidence:
    # Deserialize source evidence and preserve inline source text exactly.
    data = _mapping(value, "source")
    _reject_unknown(
        data,
        {"source_path", "source_sha256", "source_text", "function_source", "function_start_line", "function_end_line"},
        "source",
    )
    return SourceEvidence(
        source_path=_text(data, "source_path", "source"),
        source_sha256=_text(data, "source_sha256", "source"),
        source_text=_text(data, "source_text", "source"),
        function_source=_optional_text(data, "function_source", "source"),
        function_start_line=_optional_int(data, "function_start_line", "source"),
        function_end_line=_optional_int(data, "function_end_line", "source"),
    )
def _selection_to_dict(value: SelectionEvidence) -> dict[str, Any]:
    # Serialize selection observations with stable field names.
    return {
        "selected_tests": list(value.selected_tests),
        "related_test_nodeids": list(value.related_test_nodeids),
        "similar_mutant_ids": list(value.similar_mutant_ids),
        "runtime_location_reached": value.runtime_location_reached,
        "mutated_branch_reached": value.mutated_branch_reached,
        "boundary_values_observed": value.boundary_values_observed,
        "truth_table_observed": value.truth_table_observed,
        "oracle_observed": value.oracle_observed,
        "assertions_observed": value.assertions_observed,
        "equivalent_observation": value.equivalent_observation,
        "reachable_paths_proven": value.reachable_paths_proven,
        "static_reachability": value.static_reachability,
        "observed_effects_changed": value.observed_effects_changed,
        "selection_reasons": list(value.selection_reasons),
    }
def _selection_from_dict(value: object) -> SelectionEvidence:
    # Deserialize selection observations with safe defaults and no hidden fields.
    data = _mapping(value, "selection")
    _reject_unknown(
        data,
        {
            "selected_tests", "related_test_nodeids", "similar_mutant_ids", "runtime_location_reached",
            "mutated_branch_reached", "boundary_values_observed", "truth_table_observed", "oracle_observed",
            "assertions_observed", "equivalent_observation", "reachable_paths_proven", "static_reachability",
            "observed_effects_changed", "selection_reasons",
        },
        "selection",
    )
    return SelectionEvidence(
        selected_tests=_tuple_text(data, "selected_tests", "selection"),
        related_test_nodeids=_tuple_text(data, "related_test_nodeids", "selection"),
        similar_mutant_ids=_tuple_text(data, "similar_mutant_ids", "selection"),
        runtime_location_reached=_optional_bool(data, "runtime_location_reached", "selection"),
        mutated_branch_reached=_optional_bool(data, "mutated_branch_reached", "selection"),
        boundary_values_observed=_optional_bool(data, "boundary_values_observed", "selection"),
        truth_table_observed=_optional_bool(data, "truth_table_observed", "selection"),
        oracle_observed=_optional_bool(data, "oracle_observed", "selection"),
        assertions_observed=_optional_bool(data, "assertions_observed", "selection"),
        equivalent_observation=_optional_bool(data, "equivalent_observation", "selection"),
        reachable_paths_proven=_bool(data, "reachable_paths_proven", "selection") if "reachable_paths_proven" in data else False,
        static_reachability=_optional_text(data, "static_reachability", "selection"),
        observed_effects_changed=_optional_bool(data, "observed_effects_changed", "selection"),
        selection_reasons=_tuple_text(data, "selection_reasons", "selection"),
    )
def _execution_to_dict(value: ExecutionEvidence) -> dict[str, Any]:
    # Serialize one execution evidence record.
    return {
        "execution_id": value.execution_id,
        "level": value.level,
        "status": _enum_value(value.status),
        "exit_code": value.exit_code,
        "timed_out": value.timed_out,
        "infrastructure_failure": value.infrastructure_failure,
        "restore_verified": value.restore_verified,
        "selected_tests": list(value.selected_tests),
        "observed_tests": list(value.observed_tests),
        "output_excerpt": value.output_excerpt,
    }
def _execution_from_dict(value: object) -> ExecutionEvidence:
    # Deserialize one execution evidence record.
    data = _mapping(value, "execution")
    _reject_unknown(
        data,
        {
            "execution_id", "level", "status", "exit_code", "timed_out", "infrastructure_failure",
            "restore_verified", "selected_tests", "observed_tests", "output_excerpt",
        },
        "execution",
    )
    return ExecutionEvidence(
        execution_id=_text(data, "execution_id", "execution"),
        level=_text(data, "level", "execution"),
        status=_enum(_text(data, "status", "execution"), ExecutionStatus, "execution.status"),
        exit_code=_optional_int(data, "exit_code", "execution"),
        timed_out=_bool(data, "timed_out", "execution"),
        infrastructure_failure=_bool(data, "infrastructure_failure", "execution"),
        restore_verified=_bool(data, "restore_verified", "execution"),
        selected_tests=_tuple_text(data, "selected_tests", "execution", required=True),
        observed_tests=_tuple_text(data, "observed_tests", "execution", required=True),
        output_excerpt=_optional_text(data, "output_excerpt", "execution"),
    )
def _test_to_dict(value: TestEvidence) -> dict[str, Any]:
    # Serialize related test evidence.
    return {
        "nodeid": value.nodeid,
        "source_path": value.source_path,
        "source_sha256": value.source_sha256,
        "source_text": value.source_text,
        "selection_reasons": list(value.selection_reasons),
        "executions": value.executions,
        "failures": value.failures,
        "median_duration_ms": value.median_duration_ms,
        "killed_related_mutants": list(value.killed_related_mutants),
    }
def _test_from_dict(value: object) -> TestEvidence:
    # Deserialize related test evidence.
    data = _mapping(value, "test")
    _reject_unknown(
        data,
        {
            "nodeid", "source_path", "source_sha256", "source_text", "selection_reasons", "executions",
            "failures", "median_duration_ms", "killed_related_mutants",
        },
        "test",
    )
    return TestEvidence(
        nodeid=_text(data, "nodeid", "test"),
        source_path=_optional_text(data, "source_path", "test"),
        source_sha256=_optional_text(data, "source_sha256", "test"),
        source_text=_optional_text(data, "source_text", "test"),
        selection_reasons=_tuple_text(data, "selection_reasons", "test", required=True),
        executions=_int(data, "executions", "test"),
        failures=_int(data, "failures", "test"),
        median_duration_ms=_optional_float(data, "median_duration_ms", "test"),
        killed_related_mutants=_tuple_text(data, "killed_related_mutants", "test", required=True),
    )
def _dependency_to_dict(value: DependencyEvidence) -> dict[str, Any]:
    # Serialize one dependency descriptor.
    return {
        "name": value.name,
        "kind": value.kind,
        "source_path": value.source_path,
        "version": value.version,
        "relationship": value.relationship,
    }
def _dependency_from_dict(value: object) -> DependencyEvidence:
    # Deserialize one dependency descriptor.
    data = _mapping(value, "dependency")
    _reject_unknown(data, {"name", "kind", "source_path", "version", "relationship"}, "dependency")
    return DependencyEvidence(
        name=_text(data, "name", "dependency"),
        kind=_text(data, "kind", "dependency"),
        source_path=_optional_text(data, "source_path", "dependency"),
        version=_optional_text(data, "version", "dependency"),
        relationship=_text(data, "relationship", "dependency"),
    )
def _environment_to_dict(value: EnvironmentEvidence | None) -> dict[str, Any] | None:
    # Serialize optional environment evidence without secret values.
    if value is None:
        return None
    return {
        "platform": value.platform,
        "python_version": value.python_version,
        "markers": list(value.markers),
        "differences": list(value.differences),
        "stable": value.stable,
    }
def _environment_from_dict(value: object) -> EnvironmentEvidence | None:
    # Deserialize optional environment descriptors.
    if value is None:
        return None
    data = _mapping(value, "environment")
    _reject_unknown(data, {"platform", "python_version", "markers", "differences", "stable"}, "environment")
    return EnvironmentEvidence(
        platform=_optional_text(data, "platform", "environment"),
        python_version=_optional_text(data, "python_version", "environment"),
        markers=_tuple_text(data, "markers", "environment"),
        differences=_tuple_text(data, "differences", "environment"),
        stable=_optional_bool(data, "stable", "environment"),
    )
def request_to_dict(request: SurvivorAnalysisRequest) -> dict[str, Any]:
    # Serialize a validated request into a JSON-compatible dictionary.
    return {
        "schema_version": request.schema_version,
        "request_id": request.request_id,
        "project_id": request.project_id,
        "revision": request.revision,
        "mutant": _mutant_to_dict(request.mutant),
        "source": _source_to_dict(request.source),
        "selection": _selection_to_dict(request.selection),
        "executions": [_execution_to_dict(item) for item in request.executions],
        "related_tests": [_test_to_dict(item) for item in request.related_tests],
        "related_dependencies": [_dependency_to_dict(item) for item in request.related_dependencies],
        "environment": _environment_to_dict(request.environment),
        "requested_modes": [_enum_value(item) for item in request.requested_modes],
    }
def request_from_dict(value: object) -> SurvivorAnalysisRequest:
    # Deserialize a request and reject unsupported schema versions.
    data = _mapping(value, "request")
    _reject_unknown(
        data,
        {
            "schema_version", "request_id", "project_id", "revision", "mutant", "source", "selection",
            "executions", "related_tests", "related_dependencies", "environment", "requested_modes",
        },
        "request",
    )
    schema_version = _int(data, "schema_version", "request")
    if schema_version != 1:
        raise UnsupportedSchemaVersion(f"unsupported request schema_version: {schema_version}")
    executions_value = _required(data, "executions", "request")
    related_tests_value = _required(data, "related_tests", "request")
    dependencies_value = _required(data, "related_dependencies", "request")
    if not isinstance(executions_value, list):
        raise ContractError("request.executions must be an array")
    if not isinstance(related_tests_value, list):
        raise ContractError("request.related_tests must be an array")
    if not isinstance(dependencies_value, list):
        raise ContractError("request.related_dependencies must be an array")
    return SurvivorAnalysisRequest(
        schema_version=schema_version,
        request_id=_text(data, "request_id", "request"),
        project_id=_text(data, "project_id", "request"),
        revision=_optional_text(data, "revision", "request"),
        mutant=_mutant_from_dict(_required(data, "mutant", "request")),
        source=_source_from_dict(_required(data, "source", "request")),
        selection=_selection_from_dict(_required(data, "selection", "request")),
        executions=tuple(_execution_from_dict(item) for item in executions_value),
        related_tests=tuple(_test_from_dict(item) for item in related_tests_value),
        related_dependencies=tuple(_dependency_from_dict(item) for item in dependencies_value),
        environment=_environment_from_dict(data.get("environment")),
        requested_modes=_tuple_text(data, "requested_modes", "request", required=True),
    )
def _classification_to_dict(value: SurvivorClassification) -> dict[str, Any]:
    # Serialize the authoritative local classification.
    return {
        "category": value.category.value,
        "reason": value.reason,
        "confidence": value.confidence,
        "evidence_ids": list(value.evidence_ids),
        "secondary_categories": [item.value for item in value.secondary_categories],
    }
def _classification_from_dict(value: object) -> SurvivorClassification:
    # Deserialize the authoritative local classification.
    data = _mapping(value, "classification")
    _reject_unknown(data, {"category", "reason", "confidence", "evidence_ids", "secondary_categories"}, "classification")
    secondary = _required(data, "secondary_categories", "classification")
    if not isinstance(secondary, list):
        raise ContractError("classification.secondary_categories must be an array")
    confidence = _required(data, "confidence", "classification")
    if isinstance(confidence, bool) or not isinstance(confidence, (int, float)):
        raise ContractError("classification.confidence must be a number")
    return SurvivorClassification(
        category=_enum(_required(data, "category", "classification"), SurvivorCategory, "classification.category"),
        reason=_text(data, "reason", "classification"),
        confidence=float(confidence),
        evidence_ids=_tuple_text(data, "evidence_ids", "classification", required=True),
        secondary_categories=tuple(
            _enum(item, SurvivorCategory, "classification.secondary_categories") for item in secondary
        ),
    )
def _finding_to_dict(value: AnalysisFinding) -> dict[str, Any]:
    # Serialize one explainability finding.
    return {
        "finding_id": value.finding_id,
        "kind": value.kind,
        "title": value.title,
        "description": value.description,
        "evidence_ids": list(value.evidence_ids),
        "severity": value.severity,
    }
def _finding_from_dict(value: object) -> AnalysisFinding:
    # Deserialize one explainability finding.
    data = _mapping(value, "finding")
    _reject_unknown(data, {"finding_id", "kind", "title", "description", "evidence_ids", "severity"}, "finding")
    return AnalysisFinding(
        finding_id=_text(data, "finding_id", "finding"),
        kind=_text(data, "kind", "finding"),
        title=_text(data, "title", "finding"),
        description=_text(data, "description", "finding"),
        evidence_ids=_tuple_text(data, "evidence_ids", "finding", required=True),
        severity=_text(data, "severity", "finding"),
    )
def _hypothesis_to_dict(value: RepairHypothesis) -> dict[str, Any]:
    # Serialize one repair hypothesis.
    return {
        "hypothesis_id": value.hypothesis_id,
        "kind": value.kind,
        "title": value.title,
        "explanation": value.explanation,
        "target_behavior": value.target_behavior,
        "suggested_inputs": list(value.suggested_inputs),
        "suggested_assertions": list(value.suggested_assertions),
        "related_tests": list(value.related_tests),
        "confidence": value.confidence,
        "evidence_ids": list(value.evidence_ids),
    }
def _hypothesis_from_dict(value: object) -> RepairHypothesis:
    # Deserialize one repair hypothesis.
    data = _mapping(value, "hypothesis")
    _reject_unknown(
        data,
        {
            "hypothesis_id", "kind", "title", "explanation", "target_behavior", "suggested_inputs",
            "suggested_assertions", "related_tests", "confidence", "evidence_ids",
        },
        "hypothesis",
    )
    confidence = _required(data, "confidence", "hypothesis")
    if isinstance(confidence, bool) or not isinstance(confidence, (int, float)):
        raise ContractError("hypothesis.confidence must be a number")
    return RepairHypothesis(
        hypothesis_id=_text(data, "hypothesis_id", "hypothesis"),
        kind=_text(data, "kind", "hypothesis"),
        title=_text(data, "title", "hypothesis"),
        explanation=_text(data, "explanation", "hypothesis"),
        target_behavior=_text(data, "target_behavior", "hypothesis"),
        suggested_inputs=_tuple_text(data, "suggested_inputs", "hypothesis", required=True),
        suggested_assertions=_tuple_text(data, "suggested_assertions", "hypothesis", required=True),
        related_tests=_tuple_text(data, "related_tests", "hypothesis", required=True),
        confidence=float(confidence),
        evidence_ids=_tuple_text(data, "evidence_ids", "hypothesis", required=True),
    )
def _proposal_to_dict(value: TestProposal) -> dict[str, Any]:
    # Serialize one structured test proposal.
    return {
        "proposal_id": value.proposal_id,
        "target_test_file": value.target_test_file,
        "target_test_nodeid": value.target_test_nodeid,
        "proposed_test_name": value.proposed_test_name,
        "arrangement": list(value.arrangement),
        "action": list(value.action),
        "assertions": list(value.assertions),
        "rationale": value.rationale,
        "expected_original_outcome": value.expected_original_outcome,
        "expected_mutant_outcome": value.expected_mutant_outcome,
        "imports_needed": list(value.imports_needed),
        "fixtures_needed": list(value.fixtures_needed),
        "generated_code": value.generated_code,
        "generation_source": value.generation_source.value,
    }
def _proposal_from_dict(value: object) -> TestProposal:
    # Deserialize one structured test proposal.
    data = _mapping(value, "proposal")
    _reject_unknown(
        data,
        {
            "proposal_id", "target_test_file", "target_test_nodeid", "proposed_test_name", "arrangement",
            "action", "assertions", "rationale", "expected_original_outcome", "expected_mutant_outcome",
            "imports_needed", "fixtures_needed", "generated_code", "generation_source",
        },
        "proposal",
    )
    return TestProposal(
        proposal_id=_text(data, "proposal_id", "proposal"),
        target_test_file=_optional_text(data, "target_test_file", "proposal"),
        target_test_nodeid=_optional_text(data, "target_test_nodeid", "proposal"),
        proposed_test_name=_text(data, "proposed_test_name", "proposal"),
        arrangement=_tuple_text(data, "arrangement", "proposal", required=True),
        action=_tuple_text(data, "action", "proposal", required=True),
        assertions=_tuple_text(data, "assertions", "proposal", required=True),
        rationale=_text(data, "rationale", "proposal"),
        expected_original_outcome=_text(data, "expected_original_outcome", "proposal"),
        expected_mutant_outcome=_text(data, "expected_mutant_outcome", "proposal"),
        imports_needed=_tuple_text(data, "imports_needed", "proposal", required=True),
        fixtures_needed=_tuple_text(data, "fixtures_needed", "proposal", required=True),
        generated_code=_optional_text(data, "generated_code", "proposal"),
        generation_source=_enum(_required(data, "generation_source", "proposal"), GenerationSource, "proposal.generation_source"),
    )
def _step_to_dict(value: ValidationStep) -> dict[str, Any]:
    # Serialize one validation step.
    return {
        "step_id": value.step_id,
        "title": value.title,
        "command_hint": value.command_hint,
        "purpose": value.purpose,
    }
def _step_from_dict(value: object) -> ValidationStep:
    # Deserialize one validation step.
    data = _mapping(value, "validation_step")
    _reject_unknown(data, {"step_id", "title", "command_hint", "purpose"}, "validation_step")
    return ValidationStep(
        step_id=_text(data, "step_id", "validation_step"),
        title=_text(data, "title", "validation_step"),
        command_hint=_text(data, "command_hint", "validation_step"),
        purpose=_text(data, "purpose", "validation_step"),
    )
def _plan_to_dict(value: ValidationPlan) -> dict[str, Any]:
    # Serialize the offline validation plan.
    return {
        "original_checks": [_step_to_dict(item) for item in value.original_checks],
        "mutant_checks": [_step_to_dict(item) for item in value.mutant_checks],
        "regression_checks": [_step_to_dict(item) for item in value.regression_checks],
        "stability_checks": [_step_to_dict(item) for item in value.stability_checks],
    }
def _plan_from_dict(value: object) -> ValidationPlan:
    # Deserialize the offline validation plan.
    data = _mapping(value, "validation_plan")
    _reject_unknown(data, {"original_checks", "mutant_checks", "regression_checks", "stability_checks"}, "validation_plan")
    result: dict[str, tuple[ValidationStep, ...]] = {}
    for key in ("original_checks", "mutant_checks", "regression_checks", "stability_checks"):
        items = _required(data, key, "validation_plan")
        if not isinstance(items, list):
            raise ContractError(f"validation_plan.{key} must be an array")
        result[key] = tuple(_step_from_dict(item) for item in items)
    return ValidationPlan(**result)
def result_to_dict(result: SurvivorAnalysisResult) -> dict[str, Any]:
    # Serialize a validated analysis result into JSON-compatible primitives.
    return {
        "schema_version": result.schema_version,
        "request_id": result.request_id,
        "analysis_id": result.analysis_id,
        "proposal_set_id": result.proposal_set_id,
        "result_id": result.result_id,
        "classification": _classification_to_dict(result.classification),
        "confidence": result.confidence,
        "findings": [_finding_to_dict(item) for item in result.findings],
        "hypotheses": [_hypothesis_to_dict(item) for item in result.hypotheses],
        "proposals": [_proposal_to_dict(item) for item in result.proposals],
        "validation_plan": _plan_to_dict(result.validation_plan),
        "provider": {
            "name": result.provider.name,
            "version": result.provider.version,
            "deterministic": result.provider.deterministic,
            "invoked": result.provider.invoked,
            "request_sha256": result.provider.request_sha256,
            "response_sha256": result.provider.response_sha256,
        },
        "blockers": list(result.blockers),
        "warnings": list(result.warnings),
    }
def content_addressed_result_id(result: SurvivorAnalysisResult) -> str:
    # Recompute the full result artifact identity without recursively hashing its own ID.
    payload = result_to_dict(result)
    payload["result_id"] = "<content-addressed>"
    import hashlib
    digest = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    return f"result-{digest[:24]}"
def result_from_dict(value: object) -> SurvivorAnalysisResult:
    # Deserialize an analysis result and reject unsupported schema versions.
    data = _mapping(value, "result")
    _reject_unknown(
        data,
        {
            "schema_version", "request_id", "analysis_id", "proposal_set_id", "result_id", "classification",
            "confidence", "findings", "hypotheses", "proposals", "validation_plan", "provider", "blockers", "warnings",
        },
        "result",
    )
    schema_version = _int(data, "schema_version", "result")
    if schema_version != 1:
        raise UnsupportedSchemaVersion(f"unsupported result schema_version: {schema_version}")
    findings = _required(data, "findings", "result")
    hypotheses = _required(data, "hypotheses", "result")
    proposals = _required(data, "proposals", "result")
    if not all(isinstance(items, list) for items in (findings, hypotheses, proposals)):
        raise ContractError("result findings, hypotheses, and proposals must be arrays")
    confidence = _required(data, "confidence", "result")
    if isinstance(confidence, bool) or not isinstance(confidence, (int, float)):
        raise ContractError("result.confidence must be a number")
    provider_data = _mapping(_required(data, "provider", "result"), "provider")
    _reject_unknown(
        provider_data,
        {"name", "version", "deterministic", "invoked", "request_sha256", "response_sha256"},
        "provider",
    )
    result = SurvivorAnalysisResult(
        schema_version=schema_version,
        request_id=_text(data, "request_id", "result"),
        analysis_id=_text(data, "analysis_id", "result"),
        proposal_set_id=_text(data, "proposal_set_id", "result"),
        result_id=_text(data, "result_id", "result"),
        classification=_classification_from_dict(_required(data, "classification", "result")),
        confidence=float(confidence),
        findings=tuple(_finding_from_dict(item) for item in findings),
        hypotheses=tuple(_hypothesis_from_dict(item) for item in hypotheses),
        proposals=tuple(_proposal_from_dict(item) for item in proposals),
        validation_plan=_plan_from_dict(_required(data, "validation_plan", "result")),
        provider=ProviderMetadata(
            name=_text(provider_data, "name", "provider"),
            version=_text(provider_data, "version", "provider"),
            deterministic=_bool(provider_data, "deterministic", "provider"),
            invoked=_bool(provider_data, "invoked", "provider"),
            request_sha256=_optional_text(provider_data, "request_sha256", "provider"),
            response_sha256=_optional_text(provider_data, "response_sha256", "provider"),
        ),
        blockers=_tuple_text(data, "blockers", "result", required=True),
        warnings=_tuple_text(data, "warnings", "result", required=True),
    )
    from .validation import validate_result
    validate_result(result)
    return result
def _canonical_json_value(value: object) -> object:
    # Normalize every JSON string and mapping key before deterministic serialization.
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if isinstance(value, list):
        return [_canonical_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [_canonical_json_value(item) for item in value]
    if isinstance(value, dict):
        normalized: dict[str, object] = {}
        for raw_key, raw_value in value.items():
            if not isinstance(raw_key, str):
                raise TypeError("canonical JSON object keys must be strings")
            key = unicodedata.normalize("NFC", raw_key)
            if key in normalized:
                raise ValueError(f"canonical JSON contains colliding normalized key: {key!r}")
            normalized[key] = _canonical_json_value(raw_value)
        return normalized
    return value
def canonical_json(value: object) -> str:
    # Produce locale-independent NFC UTF-8 JSON for identity and artifact comparisons.
    return json.dumps(_canonical_json_value(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)

_RESOURCE_ROOTS = frozenset({"schemas", "examples"})
_RESOURCE_FILES = frozenset({"py.typed", "README.md", "ARCHITECTURE_ISOLATION.md"})
def package_resource_bytes(relative_path: str) -> bytes:
    # Read one declared package resource independently of the current working directory.
    if not isinstance(relative_path, str) or not relative_path.strip():
        raise ContractError("package resource path must be non-empty text")
    normalized = relative_path.replace("\\", "/").strip("/")
    parts = tuple(item for item in normalized.split("/") if item)
    if not parts or any(item in {".", ".."} for item in parts):
        raise ContractError(f"invalid package resource path: {relative_path!r}")
    if not (normalized in _RESOURCE_FILES or parts[0] in _RESOURCE_ROOTS):
        raise ContractError(f"package resource is outside the published resource boundary: {relative_path!r}")
    try:
        target = files("theseus_survivor_lab").joinpath(*parts)
        if not target.is_file():
            raise FileNotFoundError(normalized)
        return target.read_bytes()
    except (FileNotFoundError, OSError) as exc:
        raise ContractError(f"cannot read packaged resource {normalized}: {exc}") from exc
def package_resource_text(relative_path: str) -> str:
    # Decode one packaged UTF-8 resource while accepting an optional BOM.
    try:
        return package_resource_bytes(relative_path).decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ContractError(f"packaged resource is not UTF-8: {relative_path}") from exc
def package_resource_json(relative_path: str) -> object:
    # Decode one packaged JSON resource without consulting the process CWD.
    try:
        return json.loads(package_resource_text(relative_path))
    except json.JSONDecodeError as exc:
        raise ContractError(f"packaged resource is not valid JSON: {relative_path}: {exc}") from exc
def load_json(path: str | Path) -> object:
    # Read one bounded UTF-8 JSON document from a local path.
    from .validation import MAX_JSON_DOCUMENT_BYTES
    try:
        raw = Path(path).read_bytes()
        if len(raw) > MAX_JSON_DOCUMENT_BYTES:
            raise ContractError(
                f"JSON document exceeds the maximum size of {MAX_JSON_DOCUMENT_BYTES} bytes"
            )
        return json.loads(raw.decode("utf-8-sig"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ContractError) as exc:
        raise ContractError(f"cannot read JSON document {path}: {exc}") from exc
def write_json(path: str | Path, value: object) -> None:
    # Write deterministic UTF-8 JSON with explicit LF bytes on every platform.
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(f"{canonical_json(value)}\n".encode("utf-8"))
def result_to_markdown(result: SurvivorAnalysisResult) -> str:
    # Render a human-readable projection without adding analysis logic.
    lines = [
        "# Theseus Survivor Lab report",
        "",
        f"- Request: `{result.request_id}`",
        f"- Result: `{result.result_id}`",
        f"- Classification: **{result.classification.category.value}**",
        f"- Confidence: `{result.confidence:.2f}`",
        "",
        "## Why it survived",
        "",
        result.classification.reason,
        "",
        "## Evidence",
        "",
    ]
    if result.classification.evidence_ids:
        lines.extend(f"- `{item}`" for item in result.classification.evidence_ids)
    else:
        lines.append("- No evidence references")
    lines.extend(("", "## Findings", ""))
    if result.findings:
        for finding in result.findings:
            lines.extend(
                (
                    f"### {finding.title}",
                    "",
                    f"{finding.description}",
                    "",
                    f"Severity: `{finding.severity}`; evidence: {', '.join(f'`{item}`' for item in finding.evidence_ids)}",
                    "",
                )
            )
    else:
        lines.append("No findings.")
    lines.extend(("## Possible equivalent behavior", "", "- " + ("yes, suspected" if result.classification.category == SurvivorCategory.EQUIVALENT_SUSPECTED else "not established"), ""))
    lines.extend(("## Missing observations", ""))
    if result.blockers:
        lines.extend(f"- `{item}`" for item in result.blockers)
    else:
        lines.append("- None recorded")
    lines.extend(("", "## Suggested inputs and assertions", ""))
    for hypothesis in result.hypotheses:
        lines.extend((f"### {hypothesis.title}", "", f"{hypothesis.explanation}", ""))
        lines.append("Inputs:")
        lines.extend(f"- {item}" for item in hypothesis.suggested_inputs)
        lines.append("Assertions:")
        lines.extend(f"- {item}" for item in hypothesis.suggested_assertions)
        lines.append("")
    lines.extend(("## Candidate test proposal", ""))
    for proposal in result.proposals:
        lines.extend(
            (
                f"### `{proposal.proposed_test_name}`",
                "",
                f"{proposal.rationale}",
                "",
                "Arrangement:",
                *(f"- {item}" for item in proposal.arrangement),
                "Action:",
                *(f"- {item}" for item in proposal.action),
                "Assertions:",
                *(f"- {item}" for item in proposal.assertions),
                "",
            )
        )
    lines.extend(("## Validation plan", ""))
    for title, steps in (
        ("Original", result.validation_plan.original_checks),
        ("Mutant", result.validation_plan.mutant_checks),
        ("Regression", result.validation_plan.regression_checks),
        ("Stability", result.validation_plan.stability_checks),
    ):
        lines.append(f"### {title}")
        lines.extend(f"- {step.title}: `{step.command_hint}`" for step in steps)
        lines.append("")
    lines.extend(("## Warnings", ""))
    if result.warnings:
        lines.extend(f"- `{item}`" for item in result.warnings)
    else:
        lines.append("- None")
    return "\n".join(lines).rstrip() + "\n"
def write_markdown(path: str | Path, result: SurvivorAnalysisResult) -> None:
    # Write the Markdown projection as explicit UTF-8 LF bytes on every platform.
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(result_to_markdown(result).encode("utf-8"))
