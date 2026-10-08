from __future__ import annotations

from dataclasses import replace

import pytest

from test_intelligence_unified_v1.mutations import (
    InvalidMutantError,
    apply_prepared_mutant,
    create_snapshot,
    generate_mutants,
    prepare_mutant,
    restore_snapshot,
)


def _condition_mutant(source: str):
    # Return the single condition mutation used by line-ending regression tests.
    mutants = generate_mutants(source, operators=("condition_to_not",))
    assert len(mutants) == 1
    return mutants[0]


def test_prepare_mutant_maps_canonical_offsets_to_crlf_snapshot(tmp_path) -> None:
    # Apply offsets generated from LF text to the exact CRLF bytes stored in the source snapshot.
    physical = (
        "def choose(value):\r\n"
        "    if value > 0:\r\n"
        "        return 1\r\n"
        "    return 0\r\n"
    )
    canonical = physical.replace("\r\n", "\n")
    target = tmp_path / "app.py"
    target.write_bytes(physical.encode("utf-8"))
    snapshot = create_snapshot(target, tmp_path / "recovery")
    mutant = _condition_mutant(canonical)

    prepared = prepare_mutant(snapshot, mutant)

    expected = physical.replace("if value > 0:", "if not (value > 0):")
    assert prepared.data == expected.encode("utf-8")
    namespace: dict[str, object] = {}
    exec(prepared.data, namespace)
    assert namespace["choose"](1) == 0
    assert namespace["choose"](-1) == 1
    applied_hash = apply_prepared_mutant(snapshot, prepared)
    assert target.read_bytes() == prepared.data
    restore_snapshot(snapshot, expected_sha256=applied_hash)
    assert target.read_bytes() == physical.encode("utf-8")


def test_prepare_mutant_preserves_mixed_newlines_inside_multiline_condition(tmp_path) -> None:
    # Preserve each physical newline sequence when a canonical multiline condition is wrapped by a mutation.
    physical = (
        "def choose(value):\r\n"
        "    if (\n"
        "        value > 0\r\n"
        "        and value < 10\r"
        "    ):\n"
        "        return 1\r\n"
        "    return 0\n"
    )
    canonical = physical.replace("\r\n", "\n").replace("\r", "\n")
    target = tmp_path / "mixed.py"
    target.write_bytes(physical.encode("utf-8"))
    snapshot = create_snapshot(target, tmp_path / "recovery")
    mutant = _condition_mutant(canonical)

    prepared = prepare_mutant(snapshot, mutant)
    rendered = prepared.data.decode("utf-8")

    assert "not (value > 0\r\n        and value < 10)" in rendered
    assert rendered.startswith("def choose(value):\r\n    if (\n")
    assert rendered.endswith("        return 1\r\n    return 0\n")
    compile(rendered, str(target), "exec")


def test_mutant_identity_is_stable_across_lf_crlf_and_mixed_storage() -> None:
    # Keep logical mutant identities identical while physical source offsets vary by newline encoding.
    lf = (
        "def choose(value):\n"
        "    if value > 0:\n"
        "        return 1\n"
        "    return 0\n"
    )
    crlf = lf.replace("\n", "\r\n")
    mixed = (
        "def choose(value):\r\n"
        "    if value > 0:\n"
        "        return 1\r"
        "    return 0\r\n"
    )

    lf_mutant = _condition_mutant(lf)
    crlf_mutant = _condition_mutant(crlf)
    mixed_mutant = _condition_mutant(mixed)

    assert lf_mutant.mutant_id == crlf_mutant.mutant_id == mixed_mutant.mutant_id
    assert lf_mutant.original == crlf_mutant.original == mixed_mutant.original == "value > 0"
    assert len({lf_mutant.start, crlf_mutant.start, mixed_mutant.start}) > 1


def test_prepare_mutant_rejects_a_nonmatching_source_span(tmp_path) -> None:
    # Fail closed before compilation when stored offsets no longer identify the recorded original expression.
    source = (
        "def choose(value):\n"
        "    if value > 0:\n"
        "        return 1\n"
        "    return 0\n"
    )
    target = tmp_path / "app.py"
    target.write_text(source, encoding="utf-8", newline="")
    snapshot = create_snapshot(target, tmp_path / "recovery")
    mutant = replace(_condition_mutant(source), original="value >= 0")

    with pytest.raises(InvalidMutantError, match="source span mismatch"):
        prepare_mutant(snapshot, mutant)
