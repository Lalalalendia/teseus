"""Bounded AST context extraction without importing or executing user code."""
from __future__ import annotations
import ast
import hashlib
import json
from .contracts import (
    CausalContext,
    CausalDependencyEvidence,
    CausalExecutionEvidence,
    SourceEvidence,
    SurvivorAnalysisRequest,
    SurvivorCausalContext,
    SurvivorClassification,
    TestEvidence,
    TestFragment,
)
from .errors import ContractError
from .validation import MAX_AST_NODES, canonical_relative_path, sanitize_text
def _node_end(node: ast.AST) -> int:
    # Return a safe one-based end line for an AST node.
    return int(getattr(node, "end_lineno", getattr(node, "lineno", 0)))
def _bounded(value: str, limit: int = 2400) -> str:
    # Bound source snippets so one analysis cannot absorb a repository.
    return sanitize_text(value, limit=limit) or ""
def _unique_sorted(values: list[str], limit: int = 40) -> tuple[str, ...]:
    # Return stable unique names with a deterministic size limit.
    return tuple(sorted(set(values))[:limit])
def _parse_source(source_text: str) -> ast.Module | None:
    # Parse source defensively and enforce an AST-node budget.
    try:
        tree = ast.parse(source_text)
    except (SyntaxError, ValueError, MemoryError):
        return None
    if sum(1 for _ in ast.walk(tree)) > MAX_AST_NODES:
        return None
    return tree
def _find_scope(tree: ast.Module, line_no: int) -> ast.AST:
    # Find the smallest function/class/module scope containing the mutation.
    candidates: list[ast.AST] = [tree]
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            continue
        start = int(getattr(node, "lineno", 0))
        end = _node_end(node)
        if start <= line_no <= end:
            candidates.append(node)
    return min(
        candidates,
        key=lambda node: (
            _node_end(node) - int(getattr(node, "lineno", 0)),
            int(getattr(node, "lineno", 0)),
        ),
    )
def _source_segment(source_lines: list[str], node: ast.AST) -> str:
    # Extract an AST node segment using only already-loaded source lines.
    start = int(getattr(node, "lineno", 1)) - 1
    end = _node_end(node)
    if start < 0 or start >= len(source_lines):
        return ""
    return "\n".join(source_lines[start:end])
def _condition_text(node: ast.AST) -> str | None:
    # Render only a condition expression, never evaluate it.
    expression: ast.AST | None = None
    if isinstance(node, (ast.If, ast.While)):
        expression = node.test
    elif isinstance(node, ast.IfExp):
        expression = node.test
    elif isinstance(node, ast.Assert):
        expression = node.test
    elif isinstance(node, ast.comprehension):
        expression = node.ifs[0] if node.ifs else None
    if expression is None:
        return None
    try:
        return _bounded(ast.unparse(expression), 500)
    except (AttributeError, ValueError):
        return None
def _return_text(node: ast.Return) -> str:
    # Render a return expression without executing or importing its symbols.
    if node.value is None:
        return "return"
    try:
        return _bounded(ast.unparse(node.value), 500)
    except (AttributeError, ValueError):
        return "return <unrenderable>"
def _nodeid_target(nodeid: str) -> tuple[tuple[str, ...], str] | None:
    # Extract class path and function name from a pytest nodeid.
    segments = nodeid.split("::")
    if len(segments) < 2:
        return None
    function_name = segments[-1].split("[", 1)[0]
    if not function_name:
        return None
    return tuple(segments[1:-1]), function_name
def _scoped_definitions(tree: ast.AST) -> list[tuple[ast.AST, tuple[str, ...]]]:
    # Collect definitions together with their enclosing class path.
    definitions: list[tuple[ast.AST, tuple[str, ...]]] = []
    def visit(node: ast.AST, class_path: tuple[str, ...]) -> None:
        # Walk syntax nodes while retaining class ownership for nodeid matching.
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.ClassDef):
                definitions.append((child, class_path))
                visit(child, class_path + (child.name,))
            elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                definitions.append((child, class_path))
                visit(child, class_path)
            else:
                visit(child, class_path)
    visit(tree, ())
    return definitions
def _select_test_scope(tree: ast.Module, nodeid: str) -> tuple[ast.AST | None, bool]:
    # Resolve the exact pytest function/class and report whether fallback was needed.
    target = _nodeid_target(nodeid)
    definitions = _scoped_definitions(tree)
    if target is not None:
        class_path, function_name = target
        exact = [
            node
            for node, parent_classes in definitions
            if getattr(node, "name", None) == function_name
            and (not class_path or parent_classes[-len(class_path) :] == class_path)
        ]
        if exact:
            return min(exact, key=lambda node: (int(getattr(node, "lineno", 0)), _node_end(node))), True
        class_matches = [
            node
            for node, parent_classes in definitions
            if getattr(node, "name", None) == function_name
            and not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        ]
        if class_matches:
            return min(class_matches, key=lambda node: int(getattr(node, "lineno", 0))), True
    if definitions:
        return min(definitions, key=lambda item: (int(getattr(item[0], "lineno", 0)), _node_end(item[0])))[0], False
    return tree, False
def _test_fragment_details(test: TestEvidence) -> tuple[TestFragment, str | None]:
    # Build a bounded syntax fragment and a warning when nodeid resolution is incomplete.
    source_path = canonical_relative_path(test.source_path)
    if not test.source_text:
        return TestFragment(test.nodeid, source_path, "", ()), None
    source_lines = test.source_text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    tree = _parse_source(test.source_text)
    if tree is None:
        return TestFragment(test.nodeid, source_path, _bounded("\n".join(source_lines[:12]), 1600), ()), "test_source_ast_unavailable"
    selected, exact = _select_test_scope(tree, test.nodeid)
    names = [node.id for node in ast.walk(selected) if isinstance(node, ast.Name)]
    warning = None if exact else "test_nodeid_scope_not_found"
    return (
        TestFragment(
            nodeid=test.nodeid,
            source_path=source_path,
            excerpt=_bounded(_source_segment(source_lines, selected), 1600),
            referenced_names=_unique_sorted(names),
        ),
        warning,
    )
def _test_fragment(test: TestEvidence) -> TestFragment:
    # Keep the small public helper compatible while using nodeid-aware extraction.
    return _test_fragment_details(test)[0]
def extract_causal_context(
    source: SourceEvidence,
    mutation_line_no: int,
    related_tests: tuple[TestEvidence, ...] = (),
    radius: int = 3,
) -> CausalContext:
    # Extract bounded AST and line context around a mutation.
    source_lines = source.source_text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    if source_lines and source_lines[-1] == "":
        source_lines.pop()
    line_index = max(0, mutation_line_no - 1)
    mutation_line = source_lines[line_index] if line_index < len(source_lines) else ""
    before_start = max(0, line_index - max(0, radius))
    after_end = min(len(source_lines), line_index + max(0, radius) + 1)
    tree = _parse_source(source.source_text)
    fragments: list[TestFragment] = []
    warnings: list[str] = []
    for test in related_tests[:12]:
        fragment, warning = _test_fragment_details(test)
        fragments.append(fragment)
        if warning:
            warnings.append(warning)
    if tree is None:
        # Degrade to bounded text evidence without executing invalid source.
        return CausalContext(
            function_source=None,
            mutation_line=_bounded(mutation_line, 500),
            lines_before=tuple(_bounded(item, 500) for item in source_lines[before_start:line_index]),
            lines_after=tuple(_bounded(item, 500) for item in source_lines[line_index + 1 : after_end]),
            referenced_names=(),
            nearby_conditions=(),
            return_expressions=(),
            related_test_fragments=tuple(fragments),
            warnings=tuple(sorted(set((*warnings, "source_ast_unavailable")))),
        )
    scope = _find_scope(tree, mutation_line_no)
    # Always derive the scope from authoritative source_text; never trust a free-form projection.
    scope_source = _source_segment(source_lines, scope)
    names = [node.id for node in ast.walk(scope) if isinstance(node, ast.Name)]
    conditions: list[str] = []
    returns: list[str] = []
    for node in ast.walk(scope):
        start = int(getattr(node, "lineno", 0))
        end = _node_end(node)
        if isinstance(node, ast.Return):
            returns.append(_return_text(node))
        if not (start <= mutation_line_no <= end):
            continue
        condition = _condition_text(node)
        if condition:
            conditions.append(condition)
    return CausalContext(
        function_source=_bounded(scope_source, 2400) if scope_source else None,
        mutation_line=_bounded(mutation_line, 500),
        lines_before=tuple(_bounded(item, 500) for item in source_lines[before_start:line_index]),
        lines_after=tuple(_bounded(item, 500) for item in source_lines[line_index + 1 : after_end]),
        referenced_names=_unique_sorted(names),
        nearby_conditions=tuple(sorted(set(conditions))[:12]),
        return_expressions=tuple(sorted(set(returns))[:12]),
        related_test_fragments=tuple(fragments),
        warnings=tuple(sorted(set(warnings))),
    )

MAX_CAUSAL_TESTS = 8
MAX_CAUSAL_DEPENDENCIES = 32
MAX_CAUSAL_CONTEXT_BYTES = 32 * 1024
_TRUSTED_SELECTION_REASONS = frozenset({
    "coverage", "runtime", "dynamic", "trace", "static", "historical", "impact", "explicit", "manual", "trusted",
    "static-direct", "runtime-observed",
})
_UNRESOLVED_DEPENDENCY_TOKENS = frozenset({"dynamic", "missing", "unresolved", "unknown", "reflective", "wildcard"})

def relevant_tests(request: SurvivorAnalysisRequest) -> tuple[TestEvidence, ...]:
    # Select only tests backed by execution, trusted selection, or similar-mutant evidence.
    selected = set(request.selection.selected_tests)
    for execution in request.executions:
        selected.update(execution.selected_tests)
        selected.update(execution.observed_tests)
    trusted_selection = any(item.strip().lower() in _TRUSTED_SELECTION_REASONS for item in request.selection.selection_reasons)
    if trusted_selection:
        selected.update(request.selection.related_test_nodeids)
    similar = set(request.selection.similar_mutant_ids)
    rows: list[TestEvidence] = []
    for test in request.related_tests:
        trusted_test = any(item.strip().lower() in _TRUSTED_SELECTION_REASONS for item in test.selection_reasons)
        related_history = bool(similar.intersection(test.killed_related_mutants))
        if test.nodeid in selected or trusted_test or related_history:
            rows.append(test)
    return tuple(sorted(rows, key=lambda item: (item.nodeid, canonical_relative_path(item.source_path) or "")))[:MAX_CAUSAL_TESTS]

def causal_context_to_dict(context: SurvivorCausalContext, *, include_identity: bool = True) -> dict[str, object]:
    # Serialize one bounded context with stable field order and no environment values or raw output.
    payload: dict[str, object] = {
        "complete": context.complete,
        "project_id": context.project_id,
        "revision": context.revision,
        "mutant_id": context.mutant_id,
        "execution_ids": list(context.execution_ids),
        "source_path": context.source_path,
        "source_sha256": context.source_sha256,
        "mutation_diff": context.mutation_diff,
        "function_source": context.function_source,
        "mutation_line": context.mutation_line,
        "related_tests": [
            {
                "nodeid": item.nodeid,
                "source_path": item.source_path,
                "excerpt": item.excerpt,
                "referenced_names": list(item.referenced_names),
            }
            for item in context.related_tests
        ],
        "dependencies": [
            {
                "name": item.name,
                "kind": item.kind,
                "source_path": item.source_path,
                "version": item.version,
                "relationship": item.relationship,
            }
            for item in context.dependencies
        ],
        "executions": [
            {
                "execution_id": item.execution_id,
                "level": item.level,
                "status": item.status.value,
                "restore_verified": item.restore_verified,
                "selected_tests": list(item.selected_tests),
                "observed_tests": list(item.observed_tests),
            }
            for item in context.executions
        ],
        "classification": context.classification.value,
        "classification_reason": context.classification_reason,
        "authoritative_contracts": list(context.authoritative_contracts),
        "blockers": list(context.blockers),
        "uncertainty": list(context.uncertainty),
        "total_bytes": context.total_bytes,
    }
    if include_identity:
        payload["context_id"] = context.context_id
    return payload

def _dependency_rows(request: SurvivorAnalysisRequest) -> tuple[tuple[CausalDependencyEvidence, ...], tuple[str, ...]]:
    # Keep only confirmed bounded dependency rows and expose unresolved closure as blockers.
    confirmed: list[CausalDependencyEvidence] = []
    blockers: list[str] = []
    ordered = sorted(
        request.related_dependencies,
        key=lambda item: (item.name, item.kind, item.relationship, item.source_path or "", item.version or ""),
    )
    for dependency in ordered:
        marker = f"{dependency.kind} {dependency.relationship}".lower()
        if any(token in marker for token in _UNRESOLVED_DEPENDENCY_TOKENS):
            blockers.append(f"dependency_closure_incomplete:{dependency.name}")
            continue
        if any(token in marker for token in ("unrelated", "dev-only", "dev_only", "history-only", "historical-only")):
            continue
        try:
            source_path = canonical_relative_path(dependency.source_path)
        except ContractError:
            blockers.append(f"dependency_path_unresolved:{dependency.name}")
            continue
        confirmed.append(
            CausalDependencyEvidence(
                name=_bounded(dependency.name, 300),
                kind=_bounded(dependency.kind, 100),
                source_path=source_path,
                version=_bounded(dependency.version, 100) if dependency.version else None,
                relationship=_bounded(dependency.relationship, 200),
            )
        )
        if len(confirmed) >= MAX_CAUSAL_DEPENDENCIES:
            if len(ordered) > len(confirmed):
                blockers.append("dependency_closure_truncated")
            break
    return tuple(confirmed), tuple(sorted(set(blockers)))

def _context_payload(
    request: SurvivorAnalysisRequest,
    syntax: CausalContext,
    classification: SurvivorClassification,
    tests: tuple[TestFragment, ...],
    dependencies: tuple[CausalDependencyEvidence, ...],
    executions: tuple[CausalExecutionEvidence, ...],
    blockers: tuple[str, ...],
    uncertainty: tuple[str, ...],
) -> dict[str, object]:
    # Build identity input without context_id, host paths, volatile request IDs, or total byte count.
    mutation_diff = request.mutant.diff or f"- {request.mutant.original}\n+ {request.mutant.replacement}"
    return {
        "project_id": request.project_id,
        "revision": request.revision,
        "mutant_id": request.mutant.mutant_id,
        "execution_ids": [item.execution_id for item in executions],
        "source_path": canonical_relative_path(request.source.source_path),
        "source_sha256": request.source.source_sha256.lower(),
        "mutation_diff": _bounded(mutation_diff, 2000),
        "function_source": _bounded(syntax.function_source, 4000) if syntax.function_source else None,
        "mutation_line": _bounded(syntax.mutation_line, 500),
        "related_tests": [
            {
                "nodeid": item.nodeid,
                "source_path": item.source_path,
                "excerpt": item.excerpt,
                "referenced_names": list(item.referenced_names),
            }
            for item in tests
        ],
        "dependencies": [
            {
                "name": item.name,
                "kind": item.kind,
                "source_path": item.source_path,
                "version": item.version,
                "relationship": item.relationship,
            }
            for item in dependencies
        ],
        "executions": [
            {
                "execution_id": item.execution_id,
                "level": item.level,
                "status": item.status.value,
                "restore_verified": item.restore_verified,
                "selected_tests": list(item.selected_tests),
                "observed_tests": list(item.observed_tests),
            }
            for item in executions
        ],
        "classification": classification.category.value,
        "classification_reason": _bounded(classification.reason, 1200),
        "authoritative_contracts": [
            "survivor-analysis-request:v1",
            "survivor-analysis-result:v1",
            "survivor-proposal-evidence:v1",
            "fresh-proposal-validation:v1",
        ],
        "blockers": list(blockers),
        "uncertainty": list(uncertainty),
    }

def build_survivor_causal_context(
    request: SurvivorAnalysisRequest,
    syntax: CausalContext,
    classification: SurvivorClassification,
    blockers: tuple[str, ...] = (),
    uncertainty: tuple[str, ...] = (),
) -> SurvivorCausalContext:
    # Build a minimal deterministic context and fail closed through explicit completeness blockers.
    dependencies, dependency_blockers = _dependency_rows(request)
    fragments = tuple(sorted(syntax.related_test_fragments, key=lambda item: (item.nodeid, item.source_path or "")))[:MAX_CAUSAL_TESTS]
    execution_rows = tuple(
        CausalExecutionEvidence(
            execution_id=item.execution_id,
            level=item.level,
            status=item.status,
            restore_verified=item.restore_verified,
            selected_tests=tuple(sorted(set(item.selected_tests))),
            observed_tests=tuple(sorted(set(item.observed_tests))),
        )
        for item in sorted(request.executions, key=lambda item: (item.execution_id, item.level, item.status.value))
    )
    all_blockers = list(blockers) + list(dependency_blockers)
    all_uncertainty = sorted(set((*uncertainty, *syntax.warnings)))
    if not execution_rows:
        all_blockers.append("execution_evidence_missing")
    if any(not item.restore_verified for item in execution_rows):
        all_blockers.append("execution_restore_unverified")
    while True:
        normalized_blockers = tuple(sorted(set(all_blockers)))
        payload = _context_payload(
            request,
            syntax,
            classification,
            fragments,
            dependencies,
            execution_rows,
            normalized_blockers,
            tuple(all_uncertainty),
        )
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        if len(encoded) <= MAX_CAUSAL_CONTEXT_BYTES:
            break
        all_blockers.append("causal_context_truncated")
        if dependencies:
            dependencies = dependencies[:-1]
            continue
        if fragments:
            fragments = fragments[:-1]
            continue
        break
    digest = hashlib.sha256(encoded).hexdigest()
    mutation_diff = request.mutant.diff or f"- {request.mutant.original}\n+ {request.mutant.replacement}"
    normalized_blockers = tuple(sorted(set(all_blockers)))
    return SurvivorCausalContext(
        context_id=f"causal-{digest[:24]}",
        complete=not any(
            item.startswith((
                "dependency_closure_",
                "dependency_path_",
                "execution_evidence_",
                "execution_restore_",
                "stale_identity:",
                "causal_context_truncated",
            ))
            for item in normalized_blockers
        ),
        project_id=request.project_id,
        revision=request.revision,
        mutant_id=request.mutant.mutant_id,
        execution_ids=tuple(item.execution_id for item in execution_rows),
        source_path=canonical_relative_path(request.source.source_path) or "",
        source_sha256=request.source.source_sha256.lower(),
        mutation_diff=_bounded(mutation_diff, 2000),
        function_source=_bounded(syntax.function_source, 4000) if syntax.function_source else None,
        mutation_line=_bounded(syntax.mutation_line, 500),
        related_tests=fragments,
        dependencies=dependencies,
        executions=execution_rows,
        classification=classification.category,
        classification_reason=_bounded(classification.reason, 1200),
        authoritative_contracts=(
            "survivor-analysis-request:v1",
            "survivor-analysis-result:v1",
            "survivor-proposal-evidence:v1",
            "fresh-proposal-validation:v1",
        ),
        blockers=normalized_blockers,
        uncertainty=tuple(all_uncertainty),
        total_bytes=len(encoded),
    )
