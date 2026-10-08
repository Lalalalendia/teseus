from __future__ import annotations
from collections import Counter
import ast
from pathlib import Path
import pytest
from theseus_local import coordinator as coordinator_module
from theseus_local.coordinator import LocalCampaignCoordinator, _FingerprintSnapshot


def _write_project(root: Path, *, dynamic_conftest: bool = False, test_count: int = 2) -> tuple[str, ...]:
    # Build one local dependency graph with shared code, data and optional dynamic uncertainty.
    (root / "helper.py").write_text(
        "def normalize(value):\n    return value + 1\n",
        encoding="utf-8",
    )
    (root / "app.py").write_text(
        "from helper import normalize\n\ndef choose(value):\n    return normalize(value)\n",
        encoding="utf-8",
    )
    conftest_source = "VALUE = 1\n"
    if dynamic_conftest:
        conftest_source += "plugin_name = 'missing_plugin'\n__import__(plugin_name)\n"
    (root / "conftest.py").write_text(conftest_source, encoding="utf-8")
    (root / "fixture.json").write_text('{"value": 1}\n', encoding="utf-8")
    tests = []
    nodeids = []
    for index in range(test_count):
        name = f"test_choose_{index}"
        tests.append(
            f"def {name}():\n"
            f"    assert choose({index}) == {index + 1}\n"
        )
        nodeids.append(f"test_app.py::{name}")
    (root / "test_app.py").write_text(
        "import os\nfrom app import choose\n\nFIXTURE = 'fixture.json'\n\n"
        + "\n".join(tests),
        encoding="utf-8",
    )
    return tuple(nodeids)


def test_campaign_snapshot_matches_independent_fingerprint_evaluation(tmp_path: Path) -> None:
    # Preserve fingerprints and blockers while sharing work across selected nodeids.
    nodeids = _write_project(tmp_path)
    collection = {
        "pytest_configuration_fingerprint": "pytest-config-v1",
        "plugin_fingerprint": "plugins-v1",
    }
    shared, shared_blockers = LocalCampaignCoordinator._test_fingerprint_bundle(
        tmp_path,
        collection,
        nodeids,
        declared_globs=("fixture.json",),
    )
    independent: dict[str, str] = {}
    independent_blockers: set[str] = set()
    for nodeid in nodeids:
        current, blockers = LocalCampaignCoordinator._test_fingerprint_bundle(
            tmp_path,
            collection,
            (nodeid,),
            declared_globs=("fixture.json",),
        )
        independent.update(current)
        independent_blockers.update(blockers)
    assert shared == independent
    assert shared_blockers == tuple(sorted(independent_blockers))
    assert shared[nodeids[0]] != shared[nodeids[1]]
    (tmp_path / "helper.py").write_text(
        "def normalize(value):\n    return value + 2\n",
        encoding="utf-8",
    )
    changed, _ = LocalCampaignCoordinator._test_fingerprint_bundle(
        tmp_path,
        collection,
        nodeids,
        declared_globs=("fixture.json",),
    )
    assert all(changed[nodeid] != shared[nodeid] for nodeid in nodeids)


def test_campaign_snapshot_bounds_reads_parses_hashes_and_dynamic_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Scale planning work with unique files instead of selected tests multiplied by their closure.
    nodeids = _write_project(tmp_path, dynamic_conftest=True, test_count=12)
    (tmp_path / "unrelated.py").write_text("VALUE = 1\n", encoding="utf-8")
    root = tmp_path.resolve()
    read_counts: Counter[Path] = Counter()
    parse_counts: Counter[str] = Counter()
    hash_counts: Counter[Path] = Counter()
    dynamic_scans = 0
    original_read_text = Path.read_text
    original_rglob = Path.rglob
    original_parse = _FingerprintSnapshot._parse_source
    original_hash = coordinator_module._file_sha256

    def counted_read_text(path: Path, *args: object, **kwargs: object) -> str:
        # Count only project reads performed after the fixture has been materialized.
        resolved = path.resolve()
        if resolved == root or root in resolved.parents:
            read_counts[resolved] += 1
        return original_read_text(path, *args, **kwargs)

    def counted_parse(source: str):
        # Count snapshot-owned AST parsing without intercepting fingerprint library internals.
        parse_counts[source] += 1
        return original_parse(source)

    def counted_hash(path: Path) -> str:
        # Count binary dependency hashing behind the snapshot cache.
        hash_counts[path.resolve()] += 1
        return original_hash(path)

    def counted_rglob(path: Path, pattern: str):
        # Count only the fail-closed full Python fallback traversal.
        nonlocal dynamic_scans
        if path.resolve() == root and pattern == "*.py":
            dynamic_scans += 1
        return original_rglob(path, pattern)

    monkeypatch.setattr(Path, "read_text", counted_read_text)
    monkeypatch.setattr(Path, "rglob", counted_rglob)
    monkeypatch.setattr(_FingerprintSnapshot, "_parse_source", staticmethod(counted_parse))
    monkeypatch.setattr(coordinator_module, "_file_sha256", counted_hash)
    snapshot = _FingerprintSnapshot(root)
    fingerprints, blockers = LocalCampaignCoordinator._test_fingerprint_bundle(
        root,
        None,
        nodeids,
        declared_globs=("fixture.json",),
        fingerprint_snapshot=snapshot,
    )
    python_paths = tuple(sorted(root.glob("*.py")))
    assert len(fingerprints) == len(nodeids)
    assert len(set(fingerprints.values())) == len(nodeids)
    assert all(read_counts[path.resolve()] == 1 for path in python_paths)
    assert parse_counts and max(parse_counts.values()) == 1
    assert hash_counts[root / "fixture.json"] == 1
    assert dynamic_scans == 1
    assert "<dynamic-imports>=uncertain" in blockers


def test_pr46_snapshot_is_campaign_local_and_functions_keep_first_body_comment() -> None:
    # Keep the optimization scoped to one planning pass and preserve the repository function-comment rule.
    root = Path(__file__).resolve().parents[1]
    path = root / "theseus_local" / "coordinator.py"
    source = path.read_text(encoding="utf-8")
    assert "fingerprint_snapshot = _FingerprintSnapshot(root)" in source
    assert "_FINGERPRINT_SNAPSHOT" not in source
    lines = source.splitlines()
    tree = ast.parse(source)
    names = {
        "_parse_source",
        "read_text",
        "tree",
        "tree_for_source",
        "file_sha256",
        "python_fallback_rows",
        "environment_descriptor_key",
        "_conftest_closure",
        "_local_dependency_closure",
        "_pytest_plugin_declarations",
        "_external_pytest_plugin_identity",
        "_conftest_paths",
        "_test_dependency_closure",
        "_data_dependency_manifest",
        "_environment_reads",
        "_environment_reads_from_tree",
        "_environment_dependency_contract",
        "_test_fingerprint_bundle",
        "_resolve_local_module",
        "_node_source",
        "_conftest_fingerprint",
        "_reuse_requests",
    }
    found = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or node.name not in names:
            continue
        found.add(node.name)
        first = node.body[0]
        assert first.lineno >= 2 and lines[first.lineno - 2].strip().startswith("#"), node.name
    assert found == names
