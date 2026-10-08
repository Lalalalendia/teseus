from __future__ import annotations

from theseus_survivor_lab.context import extract_causal_context
from theseus_survivor_lab.contracts import SourceEvidence, TestEvidence as Evidence
from theseus_survivor_lab.validation import sha256_text


def test_ast_context_is_bounded_and_finds_condition_and_return() -> None:
    # Extract useful local syntax without importing the analyzed project.
    source_text = "def decide(x):\n    if x > 3:\n        return True\n    return False\n"
    source = SourceEvidence(
        source_path="C:/checkout/src/decision.py",
        source_sha256=sha256_text(source_text),
        source_text=source_text,
        function_source=None,
        function_start_line=1,
        function_end_line=4,
    )
    context = extract_causal_context(source, 2, radius=1)
    assert context.mutation_line.strip() == "if x > 3:"
    assert context.lines_before == ("def decide(x):",)
    assert context.lines_after == ("        return True",)
    assert "x" in context.referenced_names
    assert any("x > 3" in item for item in context.nearby_conditions)
    assert "True" in context.return_expressions
    assert all("C:/" not in item for item in (context.function_source or "",))


def test_invalid_syntax_returns_bounded_line_context() -> None:
    # Syntax failure must degrade to bounded evidence instead of executing anything.
    source_text = "def broken(:\n    return password = secret-token\n"
    source = SourceEvidence(
        source_path="src/broken.py",
        source_sha256=sha256_text(source_text),
        source_text=source_text,
        function_source=None,
        function_start_line=None,
        function_end_line=None,
    )
    context = extract_causal_context(source, 2, radius=1)
    assert context.mutation_line.startswith("    return")
    assert context.referenced_names == ()
    assert len(context.lines_before) <= 1
    assert len(context.lines_after) == 0


def test_related_test_fragment_is_canonical_and_bounded() -> None:
    # Include only a small syntax fragment from a related test source.
    source_text = "def decide(x):\n    return x\n"
    test_text = "\n".join(["def test_value():", "    value = decide(1)", "    assert value == 1"] + ["    # filler"] * 50)
    source = SourceEvidence("src/app.py", sha256_text(source_text), source_text, None, 1, 2)
    test = Evidence(
        nodeid="tests/test_app.py::test_value",
        source_path="D:/checkout/tests/test_app.py",
        source_sha256=sha256_text(test_text),
        source_text=test_text,
        selection_reasons=("coverage",),
        executions=1,
        failures=0,
        median_duration_ms=1.0,
        killed_related_mutants=(),
    )
    context = extract_causal_context(source, 2, (test,), radius=2)
    fragment = context.related_test_fragments[0]
    assert fragment.source_path == "tests/test_app.py"
    assert len(fragment.excerpt) <= 1600
    assert "decide" in fragment.referenced_names
