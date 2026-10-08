from __future__ import annotations

import ast
from pathlib import Path

import theseus_performance.authority as authority


def test_performance_authority_has_no_ai_provider_or_runtime_engine_imports() -> None:
    # Keep the measurement layer usable by the offline core without loading optional AI or worker runtimes.
    path = Path(authority.__file__)
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.add(node.module or "")
    forbidden = (
        "theseus_survivor",
        "openai",
        "anthropic",
        "theseus_local",
        "gallifrey_mutation",
    )
    assert not any(name.startswith(forbidden) for name in imports)


def test_new_python_functions_keep_first_body_comment() -> None:
    # Enforce the repository comment rule for every PR40 Python function and method.
    root = Path(__file__).resolve().parents[1]
    for relative in (
        "theseus_performance/authority.py",
        "tests/test_e22_performance_measurement_authority.py",
        "tests/test_e22_performance_measurement_source_contract.py",
    ):
        path = root / relative
        source = path.read_text(encoding="utf-8")
        lines = source.splitlines()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or not node.body:
                continue
            first = node.body[0]
            assert first.lineno >= 2 and lines[first.lineno - 2].strip().startswith("#"), f"{relative}:{node.name}"


def test_performance_tests_do_not_encode_machine_specific_wall_clock_budgets() -> None:
    # Keep correctness tests focused on contract semantics rather than host-dependent execution speed.
    root = Path(__file__).resolve().parents[1]
    source = (root / "tests/test_e22_performance_measurement_authority.py").read_text(encoding="utf-8")
    assert "perf_counter() <" not in source
    assert "elapsed_seconds <" not in source
    assert "duration <" not in source
