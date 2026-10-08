from __future__ import annotations

from pathlib import Path

from test_intelligence_unified_v1.mutations import (
    create_snapshot,
    generate_mutants,
    prepare_mutant,
)


LF_SOURCE = (
    "def classify(value):\n"
    "    if value > 0:\n"
    "        return value + 1\n"
    "    return 0\n"
)

CRLF_SOURCE = LF_SOURCE.replace(
    "\n",
    "\r\n",
)

EXPECTED_MUTANT_ID = (
    "m3:condition_to_not:L2:C8:"
    "ccaadac969af"
)


def _condition_mutant(source: str):
    # Generate the first condition mutant from one controlled source representation.
    return generate_mutants(
        source,
        function_range=(1, 4),
        operators=("condition_to_not",),
        max_mutants=1,
    )[0]


def test_mutant_identity_is_independent_from_line_endings() -> None:
    # Keep the logical mutant ID stable while retaining representation-specific offsets.
    lf_mutant = _condition_mutant(LF_SOURCE)
    crlf_mutant = _condition_mutant(CRLF_SOURCE)

    assert lf_mutant.mutant_id == EXPECTED_MUTANT_ID
    assert crlf_mutant.mutant_id == EXPECTED_MUTANT_ID

    assert lf_mutant.start == 28
    assert crlf_mutant.start == 29

    assert lf_mutant.end == 37
    assert crlf_mutant.end == 38


def test_crlf_snapshot_uses_its_own_patch_offsets(
    tmp_path: Path,
) -> None:
    # Compile a CRLF mutant without borrowing offsets from an LF source string.
    target = tmp_path / "app.py"
    target.write_bytes(
        CRLF_SOURCE.encode("utf-8")
    )

    snapshot = create_snapshot(
        target,
        tmp_path / "recovery",
    )
    mutant = _condition_mutant(
        snapshot.text
    )
    prepared = prepare_mutant(
        snapshot,
        mutant,
    )

    mutated_text = prepared.data.decode(
        "utf-8"
    )

    assert prepared.mutant_id == EXPECTED_MUTANT_ID
    assert "if not (value > 0):" in mutated_text
    assert "\r\n" in mutated_text

    compile(
        mutated_text,
        str(target),
        "exec",
    )