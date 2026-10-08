from __future__ import annotations

import json
from pathlib import Path

import pytest

from theseus_survivor_lab.serialization import canonical_json, write_json, write_markdown
from theseus_survivor_lab.service import analyze_request
from theseus_survivor_lab.validation import MAX_JSON_DOCUMENT_BYTES


def test_json_and_markdown_writers_emit_lf_bytes(tmp_path: Path, real_gap_request) -> None:
    # Artifact bytes must not depend on the host text-mode newline convention.
    result = analyze_request(real_gap_request)
    json_path = tmp_path / "result.json"
    markdown_path = tmp_path / "result.md"
    write_json(json_path, {"value": "line1\nline2"})
    write_markdown(markdown_path, result)
    assert b"\r\n" not in json_path.read_bytes()
    assert b"\r\n" not in markdown_path.read_bytes()


def test_canonical_json_rejects_non_finite_numbers() -> None:
    # Canonical identity and artifacts must never contain NaN or Infinity.
    with pytest.raises(ValueError):
        canonical_json({"value": float("nan")})


def test_oversized_json_document_is_rejected(tmp_path: Path) -> None:
    # File-level limits must run before JSON parsing.
    path = tmp_path / "oversized.json"
    path.write_bytes(b"{" + b"x" * (MAX_JSON_DOCUMENT_BYTES + 1))
    from theseus_survivor_lab.serialization import load_json

    with pytest.raises(Exception, match="maximum size"):
        load_json(path)


def test_published_package_resources_are_present() -> None:
    # Schemas and installed documentation must travel with the wheel.
    package_dir = Path(__file__).parents[1] / "theseus_survivor_lab"
    assert (package_dir / "schemas" / "survivor_analysis_request.v1.schema.json").is_file()
    assert (package_dir / "schemas" / "survivor_analysis_result.v1.schema.json").is_file()
    assert (package_dir / "README.md").is_file()
    assert (package_dir / "ARCHITECTURE_ISOLATION.md").is_file()
    assert (package_dir / "examples" / "real_gap.json").is_file()


@pytest.mark.parametrize(
    "schema_name",
    ("survivor_analysis_request.v1.schema.json", "survivor_analysis_result.v1.schema.json"),
)
def test_published_schemas_are_not_open_object_placeholders(schema_name: str) -> None:
    # Every object contract in the published schema has an explicit property policy.
    package_dir = Path(__file__).parents[1] / "theseus_survivor_lab"
    schema = json.loads((package_dir / "schemas" / schema_name).read_text(encoding="utf-8"))

    def walk(value):
        # Inspect nested schema nodes without requiring a third-party validator.
        if isinstance(value, dict):
            if value.get("type") == "object":
                assert value.get("additionalProperties") is False
                assert value.get("properties")
            for child in value.values():
                yield from walk(child)
        elif isinstance(value, list):
            for child in value:
                yield from walk(child)

    list(walk(schema))
