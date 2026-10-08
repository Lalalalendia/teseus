"""Strict contracts, bounded inputs, and safe normalization for E24."""
from __future__ import annotations
import ast
import hashlib
import math
import posixpath
import re
import unicodedata
from datetime import datetime, timezone
from dataclasses import replace
from .contracts import (
    AnalysisFinding,
    DependencyEvidence,
    ExecutionEvidence,
    ExecutionStatus,
    GenerationSource,
    MutantEvidence,
    MutantStatus,
    ProviderMetadata,
    RequestedMode,
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
MAX_JSON_DOCUMENT_BYTES = 10 * 1024 * 1024
MAX_SOURCE_TEXT_BYTES = 2 * 1024 * 1024
MAX_FUNCTION_SOURCE_BYTES = 256 * 1024
MAX_TEST_SOURCE_BYTES = 512 * 1024
MAX_RELATED_TESTS = 100
MAX_EXECUTIONS = 100
MAX_DEPENDENCIES = 1000
MAX_ENVIRONMENT_ITEMS = 200
MAX_AST_NODES = 100_000
MAX_NODEID_LENGTH = 1_000
MAX_TEXT_LENGTH = 16_000
MAX_PROVIDER_SUGGESTIONS = 20
MAX_PROVIDER_WARNINGS = 20
_DRIVE_PREFIX = re.compile(r"^[A-Za-z]:")
_SHA256_PATTERN = re.compile(r"^[0-9a-fA-F]{64}$")
_SECRET_KEY_PATTERN = (
    r"password|passwd|secret|token|api[_-]?key|authorization|"
    r"openai[_-]?api[_-]?key|access[_-]?token|refresh[_-]?token|"
    r"client[_-]?secret|aws[_-]?(?:access[_-]?key[_-]?id|secret[_-]?access[_-]?key)|"
    r"cookie|set-cookie"
)
_JSON_SECRET_PATTERN = re.compile(
    rf"(?i)(?P<prefix>[\"']?(?:{_SECRET_KEY_PATTERN})[\"']?\s*:\s*)"
    r"(?P<value>\"(?:\\.|[^\"])*\"|'(?:\\.|[^'])*'|[^,}\n]+)"
)
_ASSIGNMENT_SECRET_PATTERN = re.compile(
    rf"(?i)(?P<prefix>\b(?:{_SECRET_KEY_PATTERN})\b\s*(?:=|:|\bis\b)\s*)"
    r"(?P<value>\"(?:\\.|[^\"])*\"|'(?:\\.|[^'])*'|[^\s,;}]+)"
)
_BEARER_PATTERN = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+")
_URI_CREDENTIAL_PATTERN = re.compile(
    r"(?i)\b(?P<scheme>https?|sftp|ssh|postgres(?:ql)?|mysql|redis)://"
    r"[^\s/@:]+:[^\s/@]+@"
)
_PRIVATE_KEY_PATTERN = re.compile(
    r"-----BEGIN [^-\n]*PRIVATE KEY-----.*?-----END [^-\n]*PRIVATE KEY-----",
    re.IGNORECASE | re.DOTALL,
)
_AWS_ACCESS_KEY_PATTERN = re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")
_ABSOLUTE_PATH_PATTERN = re.compile(
    r"(?:(?:[A-Za-z]:[\\/])|(?:/(?:home|Users|workspace|tmp|var|mnt|opt)/))"
    r"[^\s,;\"']+"
)
def canonical_text(value: str, *, final_newline: bool = True) -> str:
    # Normalize BOM, Unicode, line endings, and final newline for semantic identity.
    if not isinstance(value, str):
        raise ContractError("canonical text must be a string")
    normalized = unicodedata.normalize("NFC", value.removeprefix("\ufeff").replace("\r\n", "\n").replace("\r", "\n"))
    if final_newline:
        return normalized.rstrip("\n") + "\n" if normalized else ""
    return normalized.rstrip("\n")
def semantic_text_sha256(value: str, *, final_newline: bool = True) -> str:
    # Hash canonical semantic text independently of host newline and Unicode form.
    return hashlib.sha256(canonical_text(value, final_newline=final_newline).encode("utf-8")).hexdigest()
def canonical_utc_timestamp(value: str | datetime) -> str:
    # Convert an aware timestamp to a deterministic UTC RFC3339 representation.
    try:
        timestamp = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ContractError(f"timestamp must be ISO-8601 text or datetime: {value!r}") from exc
    if timestamp.tzinfo is None:
        raise ContractError("timestamp must include a timezone")
    return timestamp.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
def canonical_relative_path(value: str | None) -> str | None:
    # Convert relative, drive, POSIX, or UNC paths into one traversal-safe POSIX identity.
    if value is None:
        return None
    if not isinstance(value, str):
        raise ContractError("path must be a string or null")
    if "\x00" in value:
        raise ContractError("path must not contain NUL")
    normalized = unicodedata.normalize("NFC", value.strip()).replace("\\", "/")
    is_unc = normalized.startswith("//")
    is_drive = bool(_DRIVE_PREFIX.match(normalized))
    is_posix = normalized.startswith("/") and not is_unc
    absolute = is_unc or is_drive or is_posix
    without_root = _DRIVE_PREFIX.sub("", normalized).lstrip("/")
    raw_parts = tuple(item for item in without_root.split("/") if item not in {"", "."})
    if any(item == ".." for item in raw_parts):
        raise ContractError("path must not contain traversal segments")
    if not raw_parts:
        return ""
    if absolute:
        lower = tuple(item.casefold() for item in raw_parts)
        boundary = None
        for marker in ("src", "tests"):
            if marker in lower:
                boundary = lower.index(marker)
                break
        if boundary is None and "project" in lower:
            project_index = lower.index("project")
            if project_index + 1 < len(raw_parts):
                boundary = project_index + 1
        if boundary is None:
            raise ContractError("absolute paths must include a project-relative boundary (src, tests, or project)")
        raw_parts = raw_parts[boundary:]
    result = posixpath.normpath("/".join(raw_parts))
    if result in {"", "."}:
        return ""
    if result == ".." or result.startswith("../") or "/../" in f"/{result}/":
        raise ContractError("path must not escape the project root")
    return result
def _redact_secret_values(value: str) -> str:
    # Remove structured and common credential forms before any provider handoff.
    sanitized = _PRIVATE_KEY_PATTERN.sub("<private-key-redacted>", value)
    sanitized = _URI_CREDENTIAL_PATTERN.sub(
        lambda match: f"{match.group('scheme')}://<credentials-redacted>@", sanitized
    )
    sanitized = _JSON_SECRET_PATTERN.sub(lambda match: f"{match.group('prefix')}\"<redacted>\"", sanitized)
    sanitized = _ASSIGNMENT_SECRET_PATTERN.sub(
        lambda match: f"{match.group('prefix')}<redacted>", sanitized
    )
    sanitized = _BEARER_PATTERN.sub("Bearer <redacted>", sanitized)
    sanitized = _AWS_ACCESS_KEY_PATTERN.sub("<aws-key-redacted>", sanitized)
    return sanitized
def sanitize_text(value: str | None, limit: int = 4000) -> str | None:
    # Redact untrusted text, physical paths, and oversized content deterministically.
    if value is None:
        return None
    if not isinstance(value, str):
        raise ContractError("sanitized value must be a string or null")
    if limit < 1:
        raise ContractError("sanitization limit must be positive")
    sanitized = _redact_secret_values(value)
    sanitized = _ABSOLUTE_PATH_PATTERN.sub("<absolute-path>", sanitized)
    if len(sanitized) > limit:
        sanitized = f"{sanitized[:limit]}\n<excerpt-truncated>"
    return sanitized
def sha256_text(value: str) -> str:
    # Calculate the raw UTF-8 digest retained for non-source compatibility hashes.
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
def _require_text(value: object, field: str, *, max_length: int = MAX_TEXT_LENGTH) -> str:
    # Check that a required field is non-empty text within its resource budget.
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{field} must be a non-empty string")
    if len(value) > max_length:
        raise ContractError(f"{field} exceeds the maximum length of {max_length}")
    return value
def _validate_size(value: str, field: str, limit: int) -> None:
    # Reject oversized UTF-8 evidence before parsing or AST construction.
    if len(value.encode("utf-8")) > limit:
        raise ContractError(f"{field} exceeds the maximum size of {limit} bytes")
def _validate_sha(value: str, field: str) -> None:
    # Check that a digest has the expected SHA-256 representation.
    if not isinstance(value, str) or not _SHA256_PATTERN.fullmatch(value):
        raise ContractError(f"{field} must be a 64-character SHA-256 hex digest")
def _validate_tuple_text(
    values: tuple[str, ...],
    field: str,
    *,
    max_items: int | None = None,
    allow_empty_items: bool = False,
) -> None:
    # Check tuple shape, item types, item content, and collection bounds.
    if not isinstance(values, tuple):
        raise ContractError(f"{field} must be a tuple of strings")
    if max_items is not None and len(values) > max_items:
        raise ContractError(f"{field} exceeds the maximum item count of {max_items}")
    for index, item in enumerate(values):
        if not isinstance(item, str):
            raise ContractError(f"{field}[{index}] must be a string")
        if not allow_empty_items and not item.strip():
            raise ContractError(f"{field}[{index}] must not be empty")
        if len(item) > MAX_TEXT_LENGTH:
            raise ContractError(f"{field}[{index}] exceeds the maximum length of {MAX_TEXT_LENGTH}")
def _enum(value: object, enum_type: type, field: str):
    # Convert a known string or enum value and reject unknown contract values.
    try:
        return enum_type(value)
    except (TypeError, ValueError) as exc:
        allowed = ", ".join(item.value for item in enum_type)
        raise ContractError(f"{field} contains an unsupported value {value!r}; expected one of {allowed}") from exc
def _validate_mutant(mutant: MutantEvidence) -> None:
    # Validate mutation identity, survivor status, and source location fields.
    _require_text(mutant.mutant_id, "mutant.mutant_id")
    _require_text(mutant.operator, "mutant.operator")
    _require_text(mutant.operator_version, "mutant.operator_version")
    _require_text(mutant.original, "mutant.original", max_length=MAX_TEXT_LENGTH)
    _require_text(mutant.replacement, "mutant.replacement", max_length=MAX_TEXT_LENGTH)
    _require_text(mutant.status, "mutant.status")
    _enum(mutant.status, MutantStatus, "mutant.status")
    _require_text(mutant.source_path, "mutant.source_path")
    if isinstance(mutant.line_no, bool) or not isinstance(mutant.line_no, int) or mutant.line_no < 1:
        raise ContractError("mutant.line_no must be a positive integer")
    if isinstance(mutant.column_no, bool) or not isinstance(mutant.column_no, int) or mutant.column_no < 0:
        raise ContractError("mutant.column_no must be a non-negative integer")
    if mutant.function_id is not None:
        _require_text(mutant.function_id, "mutant.function_id")
    if mutant.class_name is not None:
        _require_text(mutant.class_name, "mutant.class_name")
    if mutant.diff is not None:
        if not isinstance(mutant.diff, str):
            raise ContractError("mutant.diff must be a string or null")
        _validate_size(mutant.diff, "mutant.diff", MAX_FUNCTION_SOURCE_BYTES)
def _source_lines(source_text: str) -> list[str]:
    # Split canonical authoritative source without depending on host newline or BOM form.
    normalized = canonical_text(source_text)
    return normalized.split("\n")[:-1] if normalized.endswith("\n") else normalized.split("\n")
def _scope_candidates(tree: ast.AST, line_no: int) -> list[ast.AST]:
    # Return function/class scopes that contain a source line, smallest first.
    candidates: list[ast.AST] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            continue
        start = int(getattr(node, "lineno", 0))
        end = int(getattr(node, "end_lineno", start))
        if start <= line_no <= end:
            candidates.append(node)
    return sorted(candidates, key=lambda node: (int(getattr(node, "end_lineno", 0)) - int(getattr(node, "lineno", 0)), int(getattr(node, "lineno", 0))))
def _validate_function_identity(source: SourceEvidence, mutant: MutantEvidence) -> None:
    # Prove that the supplied function range/source and function id match the source text.
    lines = _source_lines(source.source_text)
    if mutant.line_no > len(lines):
        raise ContractError("mutant.line_no is outside source.source_text")
    line = lines[mutant.line_no - 1]
    original = canonical_text(mutant.original, final_newline=False)
    if "\n" in original or original not in line:
        raise ContractError("mutant.original is not present on mutant.line_no")
    if source.function_start_line is not None or source.function_end_line is not None:
        if source.function_start_line is None or source.function_end_line is None:
            raise ContractError("source function line range must contain both endpoints")
        if source.function_end_line > len(lines) or not source.function_start_line <= mutant.line_no <= source.function_end_line:
            raise ContractError("mutant.line_no is outside the declared function range")
    try:
        tree = ast.parse(canonical_text(source.source_text))
    except SyntaxError as exc:
        if source.function_source is not None or mutant.function_id is not None or mutant.class_name is not None:
            raise ContractError("source syntax is required to authenticate function evidence") from exc
        return
    candidates = _scope_candidates(tree, mutant.line_no)
    if source.function_source is not None:
        if source.function_start_line is not None and source.function_end_line is not None:
            expected = "\n".join(lines[source.function_start_line - 1 : source.function_end_line])
        elif candidates:
            node = candidates[0]
            expected = "\n".join(lines[int(node.lineno) - 1 : int(getattr(node, "end_lineno", node.lineno))])
        else:
            raise ContractError("function_source cannot be authenticated without a function scope")
        supplied = canonical_text(source.function_source, final_newline=False)
        if supplied != expected.rstrip("\n"):
            raise ContractError("source.function_source does not match authoritative source_text")
    if mutant.function_id is not None:
        function_name = mutant.function_id.rsplit(".", 1)[-1]
        matching = [node for node in candidates if getattr(node, "name", None) == function_name]
        if not matching:
            raise ContractError("mutant.function_id does not match the source scope")
    if mutant.class_name is not None:
        class_names = {getattr(node, "name", None) for node in candidates if isinstance(node, ast.ClassDef)}
        if mutant.class_name not in class_names:
            raise ContractError("mutant.class_name does not match the source scope")
def _validate_source(source: SourceEvidence) -> None:
    # Validate source identity and size before any AST operation.
    _require_text(source.source_path, "source.source_path")
    if not isinstance(source.source_text, str) or not source.source_text:
        raise ContractError("source.source_text must be a non-empty string")
    _validate_size(source.source_text, "source.source_text", MAX_SOURCE_TEXT_BYTES)
    _validate_sha(source.source_sha256, "source.source_sha256")
    if source.source_sha256.lower() not in {sha256_text(source.source_text), semantic_text_sha256(source.source_text)}:
        raise ContractError("source.source_sha256 does not match source.source_text")
    if source.function_source is not None:
        if not isinstance(source.function_source, str):
            raise ContractError("source.function_source must be a string or null")
        _validate_size(source.function_source, "source.function_source", MAX_FUNCTION_SOURCE_BYTES)
    for field in ("function_start_line", "function_end_line"):
        value = getattr(source, field)
        if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 1):
            raise ContractError(f"source.{field} must be a positive integer or null")
    if source.function_start_line is not None and source.function_end_line is not None and source.function_end_line < source.function_start_line:
        raise ContractError("source function line range is inverted")
def _validate_selection(selection: SelectionEvidence) -> None:
    # Validate selection observations and bound every collection supplied by an adapter.
    for field in ("selected_tests", "related_test_nodeids", "similar_mutant_ids", "selection_reasons"):
        _validate_tuple_text(getattr(selection, field), f"selection.{field}", max_items=MAX_RELATED_TESTS if field != "similar_mutant_ids" else MAX_DEPENDENCIES)
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
    if selection.static_reachability is not None:
        _require_text(selection.static_reachability, "selection.static_reachability")
def _validate_executions(executions: tuple[ExecutionEvidence, ...]) -> None:
    # Validate execution status fields and reject duplicate or oversized records.
    if not isinstance(executions, tuple):
        raise ContractError("executions must be a tuple")
    if len(executions) > MAX_EXECUTIONS:
        raise ContractError(f"executions exceeds the maximum item count of {MAX_EXECUTIONS}")
    seen: set[str] = set()
    for execution in executions:
        _require_text(execution.execution_id, "execution.execution_id")
        _require_text(execution.level, "execution.level")
        _require_text(execution.status, "execution.status")
        _enum(execution.status, ExecutionStatus, "execution.status")
        if execution.execution_id in seen:
            raise ContractError(f"duplicate execution_id: {execution.execution_id}")
        seen.add(execution.execution_id)
        if execution.exit_code is not None and (isinstance(execution.exit_code, bool) or not isinstance(execution.exit_code, int)):
            raise ContractError("execution.exit_code must be an integer or null")
        for field in ("timed_out", "infrastructure_failure", "restore_verified"):
            if not isinstance(getattr(execution, field), bool):
                raise ContractError(f"execution.{field} must be boolean")
        if execution.status == ExecutionStatus.TIMEOUT and not execution.timed_out:
            raise ContractError("timeout execution must set timed_out=true")
        _validate_tuple_text(execution.selected_tests, "execution.selected_tests", max_items=MAX_RELATED_TESTS)
        _validate_tuple_text(execution.observed_tests, "execution.observed_tests", max_items=MAX_RELATED_TESTS)
        if execution.output_excerpt is not None:
            if not isinstance(execution.output_excerpt, str):
                raise ContractError("execution.output_excerpt must be a string or null")
            _validate_size(execution.output_excerpt, "execution.output_excerpt", 64 * 1024)
def _validate_related_tests(tests: tuple[TestEvidence, ...]) -> None:
    # Validate related-test identity, source hashes, metrics, and collection bounds.
    if not isinstance(tests, tuple):
        raise ContractError("related_tests must be a tuple")
    if len(tests) > MAX_RELATED_TESTS:
        raise ContractError(f"related_tests exceeds the maximum item count of {MAX_RELATED_TESTS}")
    seen: set[str] = set()
    for test in tests:
        _require_text(test.nodeid, "test.nodeid", max_length=MAX_NODEID_LENGTH)
        if test.nodeid in seen:
            raise ContractError(f"duplicate related test nodeid: {test.nodeid}")
        seen.add(test.nodeid)
        if test.source_path is not None:
            _require_text(test.source_path, "test.source_path")
        if test.source_sha256 is not None:
            _validate_sha(test.source_sha256, "test.source_sha256")
        if test.source_text is not None:
            if not isinstance(test.source_text, str):
                raise ContractError("test.source_text must be a string or null")
            _validate_size(test.source_text, f"test.source_text[{test.nodeid}]", MAX_TEST_SOURCE_BYTES)
            if test.source_sha256 is None:
                raise ContractError(f"test.source_sha256 is required when source_text is present for {test.nodeid}")
            if test.source_sha256.lower() not in {sha256_text(test.source_text), semantic_text_sha256(test.source_text)}:
                raise ContractError(f"test.source_sha256 does not match source_text for {test.nodeid}")
        _validate_tuple_text(test.selection_reasons, "test.selection_reasons", max_items=20)
        _validate_tuple_text(test.killed_related_mutants, "test.killed_related_mutants", max_items=MAX_DEPENDENCIES)
        if not isinstance(test.executions, int) or isinstance(test.executions, bool) or test.executions < 0:
            raise ContractError("test.executions must be a non-negative integer")
        if not isinstance(test.failures, int) or isinstance(test.failures, bool) or test.failures < 0:
            raise ContractError("test.failures must be a non-negative integer")
        if test.failures > test.executions:
            raise ContractError(f"test.failures cannot exceed executions for {test.nodeid}")
        if test.median_duration_ms is not None:
            if isinstance(test.median_duration_ms, bool) or not isinstance(test.median_duration_ms, (int, float)):
                raise ContractError("test.median_duration_ms must be a finite number or null")
            if not math.isfinite(float(test.median_duration_ms)) or test.median_duration_ms < 0:
                raise ContractError("test.median_duration_ms must be finite and non-negative")
def _validate_dependencies(dependencies: tuple[DependencyEvidence, ...]) -> None:
    # Validate dependency descriptors without resolving or importing them.
    if not isinstance(dependencies, tuple):
        raise ContractError("related_dependencies must be a tuple")
    if len(dependencies) > MAX_DEPENDENCIES:
        raise ContractError(f"related_dependencies exceeds the maximum item count of {MAX_DEPENDENCIES}")
    for dependency in dependencies:
        _require_text(dependency.name, "dependency.name")
        _require_text(dependency.kind, "dependency.kind")
        _require_text(dependency.relationship, "dependency.relationship")
        if dependency.source_path is not None:
            _require_text(dependency.source_path, "dependency.source_path")
        if dependency.version is not None:
            _require_text(dependency.version, "dependency.version")
def _validate_environment(environment) -> None:
    # Validate bounded non-secret environment descriptors.
    if environment is None:
        return
    for field in ("platform", "python_version"):
        value = getattr(environment, field)
        if value is not None:
            _require_text(value, f"environment.{field}")
    if environment.stable is not None and not isinstance(environment.stable, bool):
        raise ContractError("environment.stable must be boolean or null")
    _validate_tuple_text(environment.markers, "environment.markers", max_items=MAX_ENVIRONMENT_ITEMS)
    _validate_tuple_text(environment.differences, "environment.differences", max_items=MAX_ENVIRONMENT_ITEMS)
    for field in ("markers", "differences"):
        for item in getattr(environment, field):
            _validate_size(item, f"environment.{field}", 4 * 1024)
            if _redact_secret_values(item) != item:
                raise ContractError("environment descriptors must not contain secret-like values")
def _validate_cross_evidence(request: SurvivorAnalysisRequest) -> None:
    # Prove that mutant, source, function, and test evidence refer to one bundle.
    mutant_path = canonical_relative_path(request.mutant.source_path)
    source_path = canonical_relative_path(request.source.source_path)
    if mutant_path != source_path:
        raise ContractError("mutant.source_path must match source.source_path")
    _validate_function_identity(request.source, request.mutant)
    for test in request.related_tests:
        if test.source_path is not None:
            canonical_relative_path(test.source_path)
def validate_request(request: SurvivorAnalysisRequest) -> None:
    # Validate every request invariant before normalization, AST parsing, or provider use.
    if not isinstance(request, SurvivorAnalysisRequest):
        raise ContractError("request must be a SurvivorAnalysisRequest")
    if request.schema_version != 1:
        raise UnsupportedSchemaVersion(f"unsupported request schema_version: {request.schema_version}")
    _require_text(request.request_id, "request_id")
    _require_text(request.project_id, "project_id")
    if request.revision is not None:
        _require_text(request.revision, "revision")
    _validate_mutant(request.mutant)
    _validate_source(request.source)
    _validate_selection(request.selection)
    _validate_executions(request.executions)
    _validate_related_tests(request.related_tests)
    _validate_dependencies(request.related_dependencies)
    _validate_environment(request.environment)
    _validate_tuple_text(request.requested_modes, "requested_modes", max_items=5)
    if not request.requested_modes:
        raise ContractError("requested_modes must not be empty")
    normalized_modes = [_enum(item, RequestedMode, "requested_modes") for item in request.requested_modes]
    if len(set(normalized_modes)) != len(normalized_modes):
        raise ContractError("requested_modes must not contain duplicates")
    _validate_cross_evidence(request)
def normalize_request(request: SurvivorAnalysisRequest) -> SurvivorAnalysisRequest:
    # Return a validated request with canonical paths, enums, and redacted excerpts.
    validate_request(request)
    mutant = replace(
        request.mutant,
        mutant_id=canonical_text(request.mutant.mutant_id, final_newline=False),
        operator=canonical_text(request.mutant.operator, final_newline=False),
        operator_version=canonical_text(request.mutant.operator_version, final_newline=False),
        source_path=canonical_relative_path(request.mutant.source_path) or "",
        function_id=canonical_text(request.mutant.function_id, final_newline=False) if request.mutant.function_id is not None else None,
        class_name=canonical_text(request.mutant.class_name, final_newline=False) if request.mutant.class_name is not None else None,
        original=canonical_text(request.mutant.original, final_newline=False),
        replacement=canonical_text(request.mutant.replacement, final_newline=False),
        diff=canonical_text(request.mutant.diff, final_newline=True) if request.mutant.diff is not None else None,
        status=_enum(request.mutant.status, MutantStatus, "mutant.status"),
    )
    source_text = canonical_text(request.source.source_text)
    source = replace(
        request.source,
        source_path=canonical_relative_path(request.source.source_path) or "",
        source_sha256=semantic_text_sha256(source_text),
        source_text=source_text,
        function_source=canonical_text(request.source.function_source, final_newline=False) if request.source.function_source is not None else None,
    )
    related_tests = tuple(sorted((
        replace(
            test,
            nodeid=canonical_text(test.nodeid, final_newline=False),
            source_path=canonical_relative_path(test.source_path),
            source_sha256=semantic_text_sha256(test.source_text) if test.source_text is not None else test.source_sha256,
            source_text=canonical_text(test.source_text) if test.source_text is not None else None,
            selection_reasons=tuple(sorted({canonical_text(item, final_newline=False) for item in test.selection_reasons})),
            killed_related_mutants=tuple(sorted({canonical_text(item, final_newline=False) for item in test.killed_related_mutants})),
        )
        for test in request.related_tests
    ), key=lambda item: (item.nodeid, item.source_path or "", item.source_sha256 or "")))
    dependencies = tuple(sorted((
        replace(
            dependency,
            name=canonical_text(dependency.name, final_newline=False),
            kind=canonical_text(dependency.kind, final_newline=False),
            source_path=canonical_relative_path(dependency.source_path),
            version=canonical_text(dependency.version, final_newline=False) if dependency.version is not None else None,
            relationship=canonical_text(dependency.relationship, final_newline=False),
        )
        for dependency in request.related_dependencies
    ), key=lambda item: (item.name, item.kind, item.relationship, item.source_path or "", item.version or "")))
    executions = tuple(sorted((
        replace(
            execution,
            execution_id=canonical_text(execution.execution_id, final_newline=False),
            level=canonical_text(execution.level, final_newline=False),
            status=_enum(execution.status, ExecutionStatus, "execution.status"),
            selected_tests=tuple(sorted({canonical_text(item, final_newline=False) for item in execution.selected_tests})),
            observed_tests=tuple(sorted({canonical_text(item, final_newline=False) for item in execution.observed_tests})),
            output_excerpt=sanitize_text(execution.output_excerpt, limit=1600),
        )
        for execution in request.executions
    ), key=lambda item: (item.execution_id, item.level, item.status.value)))
    selection = replace(
        request.selection,
        selected_tests=tuple(sorted({canonical_text(item, final_newline=False) for item in request.selection.selected_tests})),
        related_test_nodeids=tuple(sorted({canonical_text(item, final_newline=False) for item in request.selection.related_test_nodeids})),
        similar_mutant_ids=tuple(sorted({canonical_text(item, final_newline=False) for item in request.selection.similar_mutant_ids})),
        selection_reasons=tuple(sorted({canonical_text(item, final_newline=False) for item in request.selection.selection_reasons})),
    )
    environment = request.environment
    if environment is not None:
        environment = replace(
            environment,
            platform=canonical_text(environment.platform, final_newline=False) if environment.platform else environment.platform,
            python_version=canonical_text(environment.python_version, final_newline=False) if environment.python_version else environment.python_version,
            markers=tuple(sorted({canonical_text(item, final_newline=False) for item in environment.markers})),
            differences=tuple(sorted({canonical_text(item, final_newline=False) for item in environment.differences})),
        )
    normalized = replace(
        request,
        request_id=canonical_text(request.request_id, final_newline=False),
        project_id=canonical_text(request.project_id, final_newline=False),
        revision=canonical_text(request.revision, final_newline=False) if request.revision is not None else None,
        mutant=mutant,
        source=source,
        selection=selection,
        related_tests=related_tests,
        related_dependencies=dependencies,
        executions=executions,
        environment=environment,
        requested_modes=tuple(sorted((_enum(item, RequestedMode, "requested_modes") for item in request.requested_modes), key=lambda item: item.value)),
    )
    _validate_cross_evidence(normalized)
    return normalized
def _validate_finite(value: object, field: str) -> float:
    # Validate a finite confidence or metric value.
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        raise ContractError(f"{field} must be a finite number")
    return float(value)
def _validate_step(step: ValidationStep, field: str) -> None:
    # Validate one validation-plan step.
    _require_text(step.step_id, f"{field}.step_id")
    _require_text(step.title, f"{field}.title")
    _require_text(step.command_hint, f"{field}.command_hint")
    _require_text(step.purpose, f"{field}.purpose")
def _validate_result_nested(result: SurvivorAnalysisResult) -> None:
    # Validate every nested result DTO and enforce unique stable identities.
    classification: SurvivorClassification = result.classification
    if not isinstance(classification, SurvivorClassification):
        raise ContractError("result.classification must be a SurvivorClassification")
    if not isinstance(classification.category, SurvivorCategory):
        raise ContractError("invalid survivor classification category")
    _require_text(classification.reason, "classification.reason")
    _validate_finite(classification.confidence, "classification.confidence")
    _validate_tuple_text(classification.evidence_ids, "classification.evidence_ids")
    if len(set(classification.evidence_ids)) != len(classification.evidence_ids):
        raise ContractError("classification.evidence_ids must be unique")
    if len(set(classification.secondary_categories)) != len(classification.secondary_categories):
        raise ContractError("classification.secondary_categories must be unique")
    if classification.category in classification.secondary_categories:
        raise ContractError("classification.category cannot be secondary")
    if not isinstance(result.findings, tuple):
        raise ContractError("result.findings must be a tuple")
    finding_ids: set[str] = set()
    for index, finding in enumerate(result.findings):
        if not isinstance(finding, AnalysisFinding):
            raise ContractError(f"result.findings[{index}] is malformed")
        _require_text(finding.finding_id, f"findings[{index}].finding_id")
        _require_text(finding.kind, f"findings[{index}].kind")
        _require_text(finding.title, f"findings[{index}].title")
        _require_text(finding.description, f"findings[{index}].description")
        _validate_tuple_text(finding.evidence_ids, f"findings[{index}].evidence_ids")
        _require_text(finding.severity, f"findings[{index}].severity")
        if finding.severity not in {"info", "warning", "blocker"}:
            raise ContractError(f"findings[{index}].severity is unsupported")
        if finding.finding_id in finding_ids:
            raise ContractError(f"duplicate finding_id: {finding.finding_id}")
        finding_ids.add(finding.finding_id)
    if not isinstance(result.hypotheses, tuple):
        raise ContractError("result.hypotheses must be a tuple")
    hypothesis_ids: set[str] = set()
    for index, hypothesis in enumerate(result.hypotheses):
        if not isinstance(hypothesis, RepairHypothesis):
            raise ContractError(f"result.hypotheses[{index}] is malformed")
        _require_text(hypothesis.hypothesis_id, f"hypotheses[{index}].hypothesis_id")
        _require_text(hypothesis.kind, f"hypotheses[{index}].kind")
        _require_text(hypothesis.title, f"hypotheses[{index}].title")
        _require_text(hypothesis.explanation, f"hypotheses[{index}].explanation")
        _require_text(hypothesis.target_behavior, f"hypotheses[{index}].target_behavior")
        _validate_tuple_text(hypothesis.suggested_inputs, f"hypotheses[{index}].suggested_inputs")
        _validate_tuple_text(hypothesis.suggested_assertions, f"hypotheses[{index}].suggested_assertions")
        _validate_tuple_text(hypothesis.related_tests, f"hypotheses[{index}].related_tests", max_items=MAX_RELATED_TESTS)
        _validate_finite(hypothesis.confidence, f"hypotheses[{index}].confidence")
        _validate_tuple_text(hypothesis.evidence_ids, f"hypotheses[{index}].evidence_ids")
        if hypothesis.hypothesis_id in hypothesis_ids:
            raise ContractError(f"duplicate hypothesis_id: {hypothesis.hypothesis_id}")
        hypothesis_ids.add(hypothesis.hypothesis_id)
    if not isinstance(result.proposals, tuple):
        raise ContractError("result.proposals must be a tuple")
    proposal_ids: set[str] = set()
    for index, proposal in enumerate(result.proposals):
        if not isinstance(proposal, TestProposal):
            raise ContractError(f"result.proposals[{index}] is malformed")
        _require_text(proposal.proposal_id, f"proposals[{index}].proposal_id")
        if proposal.target_test_file is not None:
            _require_text(proposal.target_test_file, f"proposals[{index}].target_test_file")
        if proposal.target_test_nodeid is not None:
            _require_text(proposal.target_test_nodeid, f"proposals[{index}].target_test_nodeid", max_length=MAX_NODEID_LENGTH)
        for field in ("proposed_test_name", "rationale", "expected_original_outcome", "expected_mutant_outcome"):
            _require_text(getattr(proposal, field), f"proposals[{index}].{field}")
        for field in ("arrangement", "action", "assertions", "imports_needed", "fixtures_needed"):
            _validate_tuple_text(getattr(proposal, field), f"proposals[{index}].{field}")
        if proposal.generated_code is not None:
            _require_text(proposal.generated_code, f"proposals[{index}].generated_code", max_length=64 * 1024)
        if not isinstance(proposal.generation_source, GenerationSource):
            raise ContractError(f"proposals[{index}].generation_source is unsupported")
        if proposal.proposal_id in proposal_ids:
            raise ContractError(f"duplicate proposal_id: {proposal.proposal_id}")
        proposal_ids.add(proposal.proposal_id)
    if not isinstance(result.validation_plan, ValidationPlan):
        raise ContractError("result.validation_plan must be a ValidationPlan")
    step_ids: set[str] = set()
    for field in ("original_checks", "mutant_checks", "regression_checks", "stability_checks"):
        steps = getattr(result.validation_plan, field)
        if not isinstance(steps, tuple):
            raise ContractError(f"validation_plan.{field} must be a tuple")
        for index, step in enumerate(steps):
            _validate_step(step, f"validation_plan.{field}[{index}]")
            if step.step_id in step_ids:
                raise ContractError(f"duplicate validation step_id: {step.step_id}")
            step_ids.add(step.step_id)
def validate_result(result: SurvivorAnalysisResult) -> None:
    # Validate result identity fields and all nested DTOs before publication.
    if not isinstance(result, SurvivorAnalysisResult):
        raise ContractError("result must be a SurvivorAnalysisResult")
    if result.schema_version != 1:
        raise UnsupportedSchemaVersion(f"unsupported result schema_version: {result.schema_version}")
    _require_text(result.request_id, "result.request_id")
    _require_text(result.analysis_id, "result.analysis_id")
    _require_text(result.proposal_set_id, "result.proposal_set_id")
    _require_text(result.result_id, "result.result_id")
    result_confidence = _validate_finite(result.confidence, "result.confidence")
    classification_confidence = _validate_finite(result.classification.confidence, "classification.confidence")
    if result_confidence != classification_confidence:
        raise ContractError("result.confidence must equal classification.confidence")
    _validate_result_nested(result)
    if not isinstance(result.provider, ProviderMetadata):
        raise ContractError("result.provider must be ProviderMetadata")
    _require_text(result.provider.name, "provider.name")
    _require_text(result.provider.version, "provider.version")
    if not isinstance(result.provider.deterministic, bool) or not isinstance(result.provider.invoked, bool):
        raise ContractError("provider.deterministic and provider.invoked must be boolean")
    for field in ("request_sha256", "response_sha256"):
        value = getattr(result.provider, field)
        if value is not None:
            _validate_sha(value, f"provider.{field}")
    if result.provider.invoked and (result.provider.request_sha256 is None or result.provider.response_sha256 is None):
        raise ContractError("invoked provider must have request and response digests")
    _validate_tuple_text(result.blockers, "result.blockers", max_items=MAX_PROVIDER_WARNINGS)
    _validate_tuple_text(result.warnings, "result.warnings", max_items=MAX_PROVIDER_WARNINGS)
    from .serialization import content_addressed_result_id
    if result.result_id != content_addressed_result_id(result):
        raise ContractError("result.result_id does not match the serialized result content")
