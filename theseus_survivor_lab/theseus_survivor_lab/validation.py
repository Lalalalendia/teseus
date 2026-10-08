"""Contract validation and path normalization for the standalone lab."""

from __future__ import annotations

import hashlib
import posixpath
import re
from dataclasses import replace

from .contracts import (
    DependencyEvidence,
    EnvironmentEvidence,
    ExecutionEvidence,
    MutantEvidence,
    SelectionEvidence,
    SourceEvidence,
    SurvivorAnalysisRequest,
    SurvivorAnalysisResult,
    SurvivorCategory,
    TestEvidence,
)
from .errors import ContractError, UnsupportedSchemaVersion


_DRIVE_PREFIX = re.compile(r"^[A-Za-z]:")
_SECRET_PATTERN = re.compile(
    r"(?i)(?:password|passwd|secret|token|api[_-]?key|authorization)\s*"
    r"(?:[:=]|is)\s*([^\s,;]+)"
)
_ABSOLUTE_PATH_PATTERN = re.compile(
    r"(?:(?:[A-Za-z]:[\\/])|(?:/(?:home|Users|workspace|tmp|var|mnt|opt)/))[^^\s,;\"']+"
)


def canonical_relative_path(value: str | None) -> str | None:
    # Convert a local or Windows path into a stable relative representation.
    if value is None:
        return None
    if not isinstance(value, str):
        raise ContractError("path must be a string or null")
    normalized = value.replace("\\", "/")
    was_absolute = bool(_DRIVE_PREFIX.match(normalized) or normalized.startswith("/"))
    normalized = _DRIVE_PREFIX.sub("", normalized).lstrip("/")
    normalized = posixpath.normpath(normalized)
    if normalized in {"", "."}:
        return ""
    while normalized.startswith("../"):
        normalized = normalized[3:]
        normalized = f"__external__/{normalized}"
    if normalized == "..":
        return "__external__"
    if was_absolute:
        parts = normalized.split("/")
        for marker in ("src", "tests"):
            if marker in parts:
                return "/".join(parts[parts.index(marker) :])
        if "project" in parts:
            project_index = parts.index("project")
            if project_index + 1 < len(parts):
                return "/".join(parts[project_index + 1 :])
        return f"__absolute__/{parts[-1]}"
    return normalized


def sanitize_text(value: str | None, limit: int = 4000) -> str | None:
    # Remove secrets and bound untrusted evidence before provider handoff.
    if value is None:
        return None
    sanitized = _SECRET_PATTERN.sub(lambda match: match.group(0)[: match.start(1) - match.start(0)] + "<redacted>", value)
    sanitized = re.sub(r"(?i)bearer\s+[A-Za-z0-9._~+/=-]+", "Bearer <redacted>", sanitized)
    sanitized = _ABSOLUTE_PATH_PATTERN.sub("<absolute-path>", sanitized)
    if len(sanitized) > limit:
        sanitized = f"{sanitized[:limit]}\n<excerpt-truncated>"
    return sanitized


def sha256_text(value: str) -> str:
    # Calculate the canonical digest used by source and test evidence.
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _require_text(value: object, field: str) -> str:
    # Check that a required contract field contains non-empty text.
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{field} must be a non-empty string")
    return value


def _validate_sha(value: str, field: str) -> None:
    # Check that a digest has the expected SHA-256 representation.
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", value):
        raise ContractError(f"{field} must be a 64-character SHA-256 hex digest")


def _validate_tuple_text(values: tuple[str, ...], field: str) -> None:
    # Check that a tuple field contains only strings.
    if not isinstance(values, tuple) or any(not isinstance(item, str) for item in values):
        raise ContractError(f"{field} must be a tuple of strings")


def _validate_mutant(mutant: MutantEvidence) -> None:
    # Validate mutation identity and source location fields.
    _require_text(mutant.mutant_id, "mutant.mutant_id")
    _require_text(mutant.operator, "mutant.operator")
    _require_text(mutant.operator_version, "mutant.operator_version")
    _require_text(mutant.original, "mutant.original")
    _require_text(mutant.replacement, "mutant.replacement")
    _require_text(mutant.status, "mutant.status")
    _require_text(mutant.source_path, "mutant.source_path")
    if mutant.line_no < 1 or mutant.column_no < 0:
        raise ContractError("mutant source location is invalid")
    if mutant.function_id is not None and not isinstance(mutant.function_id, str):
        raise ContractError("mutant.function_id must be a string or null")
    if mutant.class_name is not None and not isinstance(mutant.class_name, str):
        raise ContractError("mutant.class_name must be a string or null")
    if mutant.diff is not None and not isinstance(mutant.diff, str):
        raise ContractError("mutant.diff must be a string or null")


def _validate_source(source: SourceEvidence) -> None:
    # Validate source identity and ensure inline source evidence is authentic.
    _require_text(source.source_path, "source.source_path")
    if not isinstance(source.source_text, str):
        raise ContractError("source.source_text must be a string")
    _validate_sha(source.source_sha256, "source.source_sha256")
    if sha256_text(source.source_text) != source.source_sha256.lower():
        raise ContractError("source.source_sha256 does not match source.source_text")
    if source.function_source is not None and not isinstance(source.function_source, str):
        raise ContractError("source.function_source must be a string or null")
    if source.function_start_line is not None and source.function_start_line < 1:
        raise ContractError("source.function_start_line must be positive")
    if source.function_end_line is not None and source.function_end_line < 1:
        raise ContractError("source.function_end_line must be positive")
    if (
        source.function_start_line is not None
        and source.function_end_line is not None
        and source.function_end_line < source.function_start_line
    ):
        raise ContractError("source function line range is inverted")


def _validate_selection(selection: SelectionEvidence) -> None:
    # Validate selection and reachability observations.
    for field in (
        "selected_tests",
        "related_test_nodeids",
        "similar_mutant_ids",
        "selection_reasons",
    ):
        _validate_tuple_text(getattr(selection, field), f"selection.{field}")
    for field in (
        "runtime_location_reached",
        "mutated_branch_reached",
        "boundary_values_observed",
        "truth_table_observed",
        "oracle_observed",
        "assertions_observed",
        "equivalent_observation",
        "reachable_paths_proven",
        "observed_effects_changed",
    ):
        value = getattr(selection, field)
        if not isinstance(value, bool) and value is not None:
            raise ContractError(f"selection.{field} must be boolean or null")
    if selection.static_reachability is not None and not isinstance(selection.static_reachability, str):
        raise ContractError("selection.static_reachability must be a string or null")


def _validate_executions(executions: tuple[ExecutionEvidence, ...]) -> None:
    # Validate execution evidence and reject duplicate execution identities.
    if not isinstance(executions, tuple):
        raise ContractError("executions must be a tuple")
    seen: set[str] = set()
    for execution in executions:
        _require_text(execution.execution_id, "execution.execution_id")
        _require_text(execution.level, "execution.level")
        _require_text(execution.status, "execution.status")
        if execution.execution_id in seen:
            raise ContractError(f"duplicate execution_id: {execution.execution_id}")
        seen.add(execution.execution_id)
        if not isinstance(execution.exit_code, int) and execution.exit_code is not None:
            raise ContractError("execution.exit_code must be an integer or null")
        for field in ("timed_out", "infrastructure_failure", "restore_verified"):
            if not isinstance(getattr(execution, field), bool):
                raise ContractError(f"execution.{field} must be boolean")
        _validate_tuple_text(execution.selected_tests, "execution.selected_tests")
        _validate_tuple_text(execution.observed_tests, "execution.observed_tests")
        if execution.output_excerpt is not None and not isinstance(execution.output_excerpt, str):
            raise ContractError("execution.output_excerpt must be a string or null")


def _validate_related_tests(tests: tuple[TestEvidence, ...]) -> None:
    # Validate related test evidence and its bounded source snippets.
    if not isinstance(tests, tuple):
        raise ContractError("related_tests must be a tuple")
    seen: set[str] = set()
    for test in tests:
        _require_text(test.nodeid, "test.nodeid")
        if test.nodeid in seen:
            raise ContractError(f"duplicate related test nodeid: {test.nodeid}")
        seen.add(test.nodeid)
        if test.source_path is not None and not isinstance(test.source_path, str):
            raise ContractError("test.source_path must be a string or null")
        if test.source_sha256 is not None:
            _validate_sha(test.source_sha256, "test.source_sha256")
        if test.source_text is not None:
            if not isinstance(test.source_text, str):
                raise ContractError("test.source_text must be a string or null")
            if test.source_sha256 is not None and sha256_text(test.source_text) != test.source_sha256.lower():
                raise ContractError(f"test.source_sha256 does not match source_text for {test.nodeid}")
        _validate_tuple_text(test.selection_reasons, "test.selection_reasons")
        _validate_tuple_text(test.killed_related_mutants, "test.killed_related_mutants")
        if not isinstance(test.executions, int) or test.executions < 0:
            raise ContractError("test.executions must be a non-negative integer")
        if not isinstance(test.failures, int) or test.failures < 0:
            raise ContractError("test.failures must be a non-negative integer")
        if test.median_duration_ms is not None and test.median_duration_ms < 0:
            raise ContractError("test.median_duration_ms cannot be negative")


def _validate_dependencies(dependencies: tuple[DependencyEvidence, ...]) -> None:
    # Validate dependency descriptors without resolving or importing them.
    if not isinstance(dependencies, tuple):
        raise ContractError("related_dependencies must be a tuple")
    for dependency in dependencies:
        _require_text(dependency.name, "dependency.name")
        _require_text(dependency.kind, "dependency.kind")
        _require_text(dependency.relationship, "dependency.relationship")
        if dependency.source_path is not None and not isinstance(dependency.source_path, str):
            raise ContractError("dependency.source_path must be a string or null")
        if dependency.version is not None and not isinstance(dependency.version, str):
            raise ContractError("dependency.version must be a string or null")


def _validate_environment(environment: EnvironmentEvidence | None) -> None:
    # Validate non-secret environment descriptors.
    if environment is None:
        return
    for field in ("platform", "python_version", "stable"):
        value = getattr(environment, field)
        if field == "stable":
            if value is not None and not isinstance(value, bool):
                raise ContractError("environment.stable must be boolean or null")
        elif value is not None and not isinstance(value, str):
            raise ContractError(f"environment.{field} must be string or null")
    _validate_tuple_text(environment.markers, "environment.markers")
    _validate_tuple_text(environment.differences, "environment.differences")
    if any(_SECRET_PATTERN.search(item) for item in (*environment.markers, *environment.differences)):
        raise ContractError("environment descriptors must not contain secret-like values")


def validate_request(request: SurvivorAnalysisRequest) -> None:
    # Validate every required input invariant before analysis begins.
    if request.schema_version != 1:
        raise UnsupportedSchemaVersion(f"unsupported request schema_version: {request.schema_version}")
    _require_text(request.request_id, "request_id")
    _require_text(request.project_id, "project_id")
    if request.revision is not None and not isinstance(request.revision, str):
        raise ContractError("revision must be a string or null")
    _validate_mutant(request.mutant)
    _validate_source(request.source)
    _validate_selection(request.selection)
    _validate_executions(request.executions)
    _validate_related_tests(request.related_tests)
    _validate_dependencies(request.related_dependencies)
    _validate_environment(request.environment)
    _validate_tuple_text(request.requested_modes, "requested_modes")


def normalize_request(request: SurvivorAnalysisRequest) -> SurvivorAnalysisRequest:
    # Return a semantically equivalent request with canonical relative paths.
    validate_request(request)
    mutant = replace(request.mutant, source_path=canonical_relative_path(request.mutant.source_path) or "")
    source = replace(request.source, source_path=canonical_relative_path(request.source.source_path) or "")
    related_tests = tuple(
        replace(test, source_path=canonical_relative_path(test.source_path))
        for test in request.related_tests
    )
    dependencies = tuple(
        replace(dependency, source_path=canonical_relative_path(dependency.source_path))
        for dependency in request.related_dependencies
    )
    executions = tuple(
        replace(
            execution,
            output_excerpt=sanitize_text(execution.output_excerpt, limit=1600),
        )
        for execution in request.executions
    )
    return replace(
        request,
        mutant=mutant,
        source=source,
        related_tests=related_tests,
        related_dependencies=dependencies,
        executions=executions,
    )


def validate_result(result: SurvivorAnalysisResult) -> None:
    # Validate result-level invariants before serialization or projection.
    if result.schema_version != 1:
        raise UnsupportedSchemaVersion(f"unsupported result schema_version: {result.schema_version}")
    _require_text(result.request_id, "result.request_id")
    _require_text(result.result_id, "result.result_id")
    if not 0.0 <= result.confidence <= 1.0:
        raise ContractError("result.confidence must be between 0 and 1")
    if not isinstance(result.classification.category, SurvivorCategory):
        raise ContractError("invalid survivor classification category")
    if not 0.0 <= result.classification.confidence <= 1.0:
        raise ContractError("classification confidence must be between 0 and 1")
    _validate_tuple_text(result.blockers, "result.blockers")
    _validate_tuple_text(result.warnings, "result.warnings")
