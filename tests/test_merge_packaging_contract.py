from __future__ import annotations

import tomllib
from pathlib import Path


def _root_pyproject() -> dict[str, object]:
    # Load the authoritative root packaging manifest without importing the project.
    path = Path(__file__).parents[1] / "pyproject.toml"
    with path.open("rb") as handle:
        return tomllib.load(handle)


def test_root_manifest_remains_the_theseus_platform_after_survivor_lab_merge() -> None:
    # Prevent a separately developed module from replacing the root distribution metadata again.
    document = _root_pyproject()
    project = document["project"]
    assert isinstance(project, dict)
    assert project["name"] == "theseus-mutation-platform"
    assert project["version"] == "1.29.6"


def test_root_manifest_exposes_all_runtime_entrypoints() -> None:
    # Keep every packaged runtime entrypoint installable from one root manifest.
    document = _root_pyproject()
    project = document["project"]
    assert isinstance(project, dict)
    scripts = project["scripts"]
    assert isinstance(scripts, dict)
    assert scripts == {
        "theseus": "theseus_local.cli:main",
        "theseus-worker": "theseus_local.worker_runtime.entrypoint:main",
        "test-intelligence-unified": "test_intelligence_unified_v1.cli:main",
        "theseus-survivor-lab": "theseus_survivor_lab.cli:main",
        "theseus-ui": "theseus_ui.__main__:main",
    }


def test_merged_survivor_lab_uses_one_explicit_outer_package_root() -> None:
    # Exclude the stale nested standalone copy from the main distribution package graph.
    document = _root_pyproject()
    tool = document["tool"]
    assert isinstance(tool, dict)
    setuptools = tool["setuptools"]
    assert isinstance(setuptools, dict)
    packages = setuptools["packages"]
    package_dir = setuptools["package-dir"]
    assert isinstance(packages, list)
    assert isinstance(package_dir, dict)
    assert packages.count("theseus_survivor_lab") == 1
    assert "theseus_survivor_lab.theseus_survivor_lab" not in packages
    assert package_dir["theseus_survivor_lab"] == "theseus_survivor_lab"
    assert "packages.find" not in setuptools
