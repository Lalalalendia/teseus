from pathlib import Path

from test_intelligence_unified_v1.mutations import generate_mutants
from test_intelligence_unified_v1.runner import MutationConfig, MutationRunner


def test_await_operator_uses_ast_span_and_version() -> None:
    # Replace the complete Await node and keep the operand as a valid expression.
    source = (
        "async def load(client):\n"
        "    value = await client.fetch()\n"
        "    return value\n"
    )
    mutants = generate_mutants(source, operators=("await_to_expression",))
    assert len(mutants) == 1
    mutant = mutants[0]
    mutated = source[:mutant.start] + mutant.replacement + source[mutant.end:]
    compile(mutated, "async_sample.py", "exec")
    assert mutant.original == "await client.fetch()"
    assert mutant.replacement == "client.fetch()"
    assert mutant.operator_version == "m4"
    assert mutant.mutant_id.startswith("m4:await_to_expression:")


def test_operator_metrics_are_grouped_by_version() -> None:
    # Report per-operator counts without requiring a subprocess campaign.
    runner = MutationRunner(MutationConfig(project_root=Path("."), source="app.py"))
    metrics = runner._metrics(
        [
            {
                "status": "killed",
                "mutant": {"mutation": "await_to_expression", "operator_version": "m4"},
            },
            {
                "status": "survived",
                "mutant": {"mutation": "await_to_expression", "operator_version": "m4"},
            },
        ]
    )
    assert metrics["operator_stats"]["await_to_expression"] == {
        "operator_version": "m4",
        "total_mutants": 2,
        "counts": {"killed": 1, "survived": 1},
        "mutation_score": 0.5,
    }
