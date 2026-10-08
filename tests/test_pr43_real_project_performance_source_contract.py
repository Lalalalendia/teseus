from __future__ import annotations
import ast
from pathlib import Path
import theseus_performance.project_benchmark as project_benchmark
def test_project_benchmark_keeps_ai_and_provider_sdks_out_of_the_measurement_path() -> None:
    # Keep real-project measurement offline and independent from optional repair providers.
    path = Path(project_benchmark.__file__)
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.add(node.module or "")
    forbidden = ("theseus_survivor", "openai", "anthropic")
    assert not any(name.startswith(forbidden) for name in imports)
def test_pr43_functions_keep_first_body_comment() -> None:
    # Enforce the repository comment rule for every new or modified PR43 function and method.
    root = Path(__file__).resolve().parents[1]
    for relative in (
        "theseus_performance/project_benchmark.py",
        "theseus_local/cli.py",
        "tests/test_pr43_real_project_performance_baseline.py",
        "tests/test_pr43_real_project_performance_source_contract.py",
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
def test_pr43_tests_do_not_encode_machine_specific_performance_thresholds() -> None:
    # Keep benchmark assertions structural so different Windows hosts remain comparable rather than flaky.
    root = Path(__file__).resolve().parents[1]
    source = (root / "tests/test_pr43_real_project_performance_baseline.py").read_text(encoding="utf-8")
    assert "perf_counter() <" not in source
    assert "wall_seconds <" not in source
    assert "elapsed_seconds <" not in source
