from test_intelligence_unified_v1.mutations import generate_mutants


def test_ast_operator_has_stable_patch_and_compiles() -> None:
    # Generate an AST condition mutation without materializing a mutant source copy.
    source = "def choose(value):\n    if value > 0:\n        return 'yes'\n    return 'no'\n"
    mutants = generate_mutants(source, operators=("condition_to_not",))
    assert len(mutants) == 1
    mutant = mutants[0]
    mutated = source[:mutant.start] + mutant.replacement + source[mutant.end:]
    compile(mutated, "sample.py", "exec")
    assert mutant.mutant_id.startswith("m3:condition_to_not:")
