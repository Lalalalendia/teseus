from unittest.mock import patch

from test_intelligence_unified_v1 import mutations
from test_intelligence_unified_v1.mutations import (
    apply_prepared_mutant,
    create_snapshot,
    generate_mutants,
    prepare_mutant,
    restore_snapshot,
)
from test_intelligence_unified_v1.models import PerformanceMetrics


def test_max_mutants_is_a_deterministic_source_order_prefix() -> None:
    # Bound the returned stream without changing the established mutant ordering.
    source = (
        "def calculate(value):\n"
        "    if value > 0 and value != 3:\n"
        "        return value + 1\n"
        "    return None\n"
    )
    complete = generate_mutants(source)
    limited = generate_mutants(source, max_mutants=3)
    assert [item.mutant_id for item in limited] == [item.mutant_id for item in complete[:3]]


def test_mutation_generation_uses_one_tokenizer_pass() -> None:
    # Reuse the token stream for protection checks and token mutation generation.
    source = "def calculate(value):\n    return value + 1\n"
    original = mutations.tokenize.generate_tokens
    with patch.object(mutations.tokenize, "generate_tokens", wraps=original) as tokenizer:
        generate_mutants(source)
    assert tokenizer.call_count == 1


def test_prepared_mutant_installs_without_a_second_render(tmp_path) -> None:
    # Compile once during preparation and install the retained bytes during application.
    target = tmp_path / "sample.py"
    target.write_text("def calculate(value):\n    return value + 1\n", encoding="utf-8")
    snapshot = create_snapshot(target, tmp_path / "recovery")
    mutant = generate_mutants(snapshot.text, operators=("plus_to_minus",))[0]
    with patch.object(mutations, "render_mutant", wraps=mutations.render_mutant) as renderer:
        prepared = prepare_mutant(snapshot, mutant)
        applied_hash = apply_prepared_mutant(
            snapshot,
            prepared,
            current_sha256_value=snapshot.original_sha256,
        )
        assert renderer.call_count == 1
    assert applied_hash == prepared.sha256
    assert target.read_bytes() == prepared.data
    restore_snapshot(snapshot, expected_sha256=applied_hash)
    assert target.read_bytes() == snapshot.original_bytes


def test_intermediate_install_is_normal_and_final_restore_is_critical(tmp_path) -> None:
    # Keep fast campaign writes non-durable while retaining a critical final restore boundary.
    target = tmp_path / "sample.py"
    target.write_text("def calculate(value):\n    return value + 1\n", encoding="utf-8")
    metrics = PerformanceMetrics()
    snapshot = create_snapshot(target, tmp_path / "recovery", metrics=metrics)
    mutant = generate_mutants(snapshot.text, operators=("plus_to_minus",))[0]
    prepared = prepare_mutant(snapshot, mutant)
    apply_prepared_mutant(snapshot, prepared, durability="normal", metrics=metrics)
    assert metrics.normal_writes >= 1
    restore_snapshot(snapshot, expected_sha256=prepared.sha256, durability="critical", metrics=metrics)
    assert metrics.critical_writes >= 2
