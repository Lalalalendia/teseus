from __future__ import annotations
import os
from pathlib import Path
import pytest
from theseus_survivor_lab.errors import ContractError
from theseus_survivor_lab.serialization import package_resource_bytes, package_resource_json, package_resource_text
def test_package_resources_load_without_current_working_directory(tmp_path: Path) -> None:
    # Schemas, examples, documentation, and py.typed must resolve through package resources only.
    previous = Path.cwd()
    os.chdir(tmp_path)
    try:
        request_schema = package_resource_json("schemas/survivor_analysis_request.v1.schema.json")
        result_schema = package_resource_json("schemas/survivor_analysis_result.v1.schema.json")
        example = package_resource_json("examples/real_gap.json")
        readme = package_resource_text("README.md")
        architecture = package_resource_text("ARCHITECTURE_ISOLATION.md")
        report = package_resource_text("examples/real_gap_result.md")
        marker = package_resource_bytes("py.typed")
    finally:
        os.chdir(previous)
    assert request_schema["$schema"]
    assert result_schema["$schema"]
    assert example["schema_version"] == 1
    assert "Survivor" in readme
    assert "offline" in architecture.lower()
    assert "Theseus Survivor Lab" in report
    assert marker == b""
def test_resource_boundary_rejects_traversal_and_undeclared_files() -> None:
    # Package loading must not become an arbitrary filesystem reader.
    with pytest.raises(ContractError, match="invalid package resource path"):
        package_resource_bytes("schemas/../README.md")
    with pytest.raises(ContractError, match="outside the published resource boundary"):
        package_resource_bytes("service.py")
def test_package_manifest_declares_all_required_resources() -> None:
    # Editable and wheel installs must use the same explicit package-data manifest.
    manifest = (Path(__file__).parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    for entry in (
        '"py.typed"',
        '"schemas/*.json"',
        '"examples/*.json"',
        '"examples/*.md"',
        '"README.md"',
        '"ARCHITECTURE_ISOLATION.md"',
    ):
        assert entry in manifest, f"missing package-data entry: {entry}"
