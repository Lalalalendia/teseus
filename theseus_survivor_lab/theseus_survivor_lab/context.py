"""Bounded AST context extraction without importing or executing user code."""

from __future__ import annotations

import ast

from .contracts import CausalContext, SourceEvidence, TestEvidence, TestFragment
from .validation import canonical_relative_path, sanitize_text


def _node_end(node: ast.AST) -> int:
    # Return a safe one-based end line for an AST node.
    return int(getattr(node, "end_lineno", getattr(node, "lineno", 0)))


def _bounded(value: str, limit: int = 2400) -> str:
    # Bound source snippets so one analysis cannot absorb a repository.
    sanitized = sanitize_text(value, limit=limit) or ""
    return sanitized


def _unique_sorted(values: list[str], limit: int = 40) -> tuple[str, ...]:
    # Return stable unique names with a deterministic size limit.
    return tuple(sorted(set(values))[:limit])


def _parse_source(source_text: str) -> ast.Module | None:
    # Parse source defensively and treat syntax failure as missing AST evidence.
    try:
        return ast.parse(source_text)
    except SyntaxError:
        return None


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
    return min(candidates, key=lambda node: (_node_end(node) - int(getattr(node, "lineno", 0)), int(getattr(node, "lineno", 0))))


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


def _test_fragment(test: TestEvidence) -> TestFragment:
    # Build a bounded, syntax-only fragment from related test evidence.
    source_path = canonical_relative_path(test.source_path)
    if not test.source_text:
        return TestFragment(test.nodeid, source_path, "", ())
    source_lines = test.source_text.splitlines()
    tree = _parse_source(test.source_text)
    if tree is None:
        return TestFragment(test.nodeid, source_path, _bounded("\n".join(source_lines[:12]), 1600), ())
    definitions = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    ]
    definitions.sort(key=lambda node: (int(getattr(node, "lineno", 0)), _node_end(node)))
    selected = definitions[0] if definitions else tree
    names = [node.id for node in ast.walk(selected) if isinstance(node, ast.Name)]
    return TestFragment(
        nodeid=test.nodeid,
        source_path=source_path,
        excerpt=_bounded(_source_segment(source_lines, selected), 1600),
        referenced_names=_unique_sorted(names),
    )


def extract_causal_context(
    source: SourceEvidence,
    mutation_line_no: int,
    related_tests: tuple[TestEvidence, ...] = (),
    radius: int = 3,
) -> CausalContext:
    # Extract a bounded AST and line context around a mutation.
    source_lines = source.source_text.splitlines()
    line_index = max(0, mutation_line_no - 1)
    mutation_line = source_lines[line_index] if line_index < len(source_lines) else ""
    before_start = max(0, line_index - max(0, radius))
    after_end = min(len(source_lines), line_index + max(0, radius) + 1)
    tree = _parse_source(source.source_text)
    if tree is None:
        return CausalContext(
            function_source=_bounded(source.function_source, 2400) if source.function_source else None,
            mutation_line=_bounded(mutation_line, 500),
            lines_before=tuple(_bounded(item, 500) for item in source_lines[before_start:line_index]),
            lines_after=tuple(_bounded(item, 500) for item in source_lines[line_index + 1 : after_end]),
            referenced_names=(),
            nearby_conditions=(),
            return_expressions=(),
            related_test_fragments=tuple(_test_fragment(test) for test in related_tests[:12]),
        )
    scope = _find_scope(tree, mutation_line_no)
    scope_source = source.function_source or _source_segment(source_lines, scope)
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
        related_test_fragments=tuple(_test_fragment(test) for test in related_tests[:12]),
    )
