import sqlite3
from pathlib import Path

import test_intelligence_unified_v1.runner as runner_module
from test_intelligence_unified_v1.impact import V2_SCHEMA_SQL, SQLiteImpactAdapter
from test_intelligence_unified_v1.index import (
    build_collection_snapshot,
    build_index,
    build_nodeid_validation_context,
    parse_pytest_collection_output,
    plan_selection,
    validate_test_nodeids,
)
from test_intelligence_unified_v1.models import Mutant
from test_intelligence_unified_v1.runner import LevelSpec, MutationConfig, MutationRunner


def test_nodeid_context_accepts_authoritative_collected_bases(tmp_path: Path) -> None:
    # Prefer a supplied pytest collection inventory over the static AST inventory.
    test_file = tmp_path / "test_dynamic.py"
    test_file.write_text("def test_static():\n    pass\n", encoding="utf-8")
    index = {"tests": [{"nodeid": "test_dynamic.py::test_static"}]}
    snapshot = build_collection_snapshot("test_dynamic.py::test_generated[one]\n")
    context = build_nodeid_validation_context(
        index,
        tmp_path,
        collection_snapshot=snapshot,
    )
    valid, dropped = validate_test_nodeids(
        index,
        tmp_path,
        (
            "test_dynamic.py::test_generated[one]",
            "test_dynamic.py::test_generated[two]",
            "test_dynamic.py::test_static",
        ),
        context=context,
    )
    assert context.authoritative is True
    assert valid == ("test_dynamic.py::test_generated[one]",)
    assert dropped == (
        "test_dynamic.py::test_generated[two]",
        "test_dynamic.py::test_static",
    )


def test_plan_selection_builds_one_validation_context(tmp_path: Path, monkeypatch) -> None:
    # Reuse one prepared inventory across all candidate-source validation passes.
    root = tmp_path / "project"
    root.mkdir()
    (root / "app.py").write_text("def choose(value):\n    return value\n", encoding="utf-8")
    tests_dir = root / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_app.py").write_text(
        "def test_one():\n    pass\n\ndef test_two():\n    pass\n",
        encoding="utf-8",
    )
    index = build_index(root, root / "index.sqlite")
    selected = root / "selected.txt"
    selected.write_text(
        "tests/test_app.py::test_missing\n"
        "tests/test_app.py::test_one\n",
        encoding="utf-8",
    )
    calls = 0
    original = runner_module.build_nodeid_validation_context

    def counted(index_value, root_value, **kwargs):
        # Count only campaign-boundary context construction, not candidate validation.
        nonlocal calls
        calls += 1
        return original(index_value, root_value, **kwargs)

    monkeypatch.setattr(
        "test_intelligence_unified_v1.index.build_nodeid_validation_context",
        counted,
    )
    plan_selection(root, index, "app.py", "choose", selected_tests_file=selected)
    assert calls == 1


def test_collection_snapshot_is_authoritative_only_without_collection_errors() -> None:
    # Keep dynamic pytest nodeids exact while making failed collection fall back safely.
    output = (
        "tests/test_dynamic.py::test_generated[one]\n"
        "tests/test_dynamic.py::test_generated[two]\n"
        "2 tests collected\n"
    )
    assert parse_pytest_collection_output(output) == (
        "tests/test_dynamic.py::test_generated[one]",
        "tests/test_dynamic.py::test_generated[two]",
    )
    snapshot = build_collection_snapshot(output, revision="rev-1", pytest_version="8.0")
    assert snapshot.collection_id
    assert snapshot.collection_errors == ()
    failed = build_collection_snapshot(
        output,
        revision="rev-1",
        collection_errors=("ERROR collecting tests/test_dynamic.py",),
    )
    assert failed.collection_errors == ("ERROR collecting tests/test_dynamic.py",)


def test_normalized_impact_deduplicates_before_limit(tmp_path: Path) -> None:
    # Apply pagination after nodeid deduplication so a duplicate cannot consume the limit.
    database = tmp_path / "impact.sqlite"
    connection = sqlite3.connect(database)
    connection.executescript(V2_SCHEMA_SQL)
    connection.executemany(
        "INSERT INTO impact_links(source_path,function_id,line_no,nodeid,executions,kills,median_ms) "
        "VALUES (?,?,?,?,?,?,?)",
        (
            ("app.py", "app.py::choose", 3, "tests/test_app.py::test_one", 2, 0, 10.0),
            ("./app.py", "app.py::choose", 3, "tests/test_app.py::test_one", 9, 1, 4.0),
            ("app.py", "app.py::choose", 3, "tests/test_app.py::test_two", 8, 0, 6.0),
        ),
    )
    connection.commit()
    connection.close()

    adapter = SQLiteImpactAdapter(database)
    first = adapter.select_tests("app.py", "app.py::choose", limit=1)
    second = adapter.select_tests("app.py", "app.py::choose", limit=1, offset=1)
    assert [row["nodeid"] for row in first] == ["tests/test_app.py::test_one"]
    assert first[0]["executions"] == 9
    assert [row["nodeid"] for row in second] == ["tests/test_app.py::test_two"]
    assert adapter.diagnostics["truncated"] is False


def test_frozen_selection_skips_selection_fallbacks(tmp_path: Path, monkeypatch) -> None:
    # Keep a frozen snapshot independent from impact, context-map and static fallback preparation.
    root = tmp_path / "project"
    root.mkdir()
    (root / "app.py").write_text("def choose(value):\n    return value\n", encoding="utf-8")
    tests_dir = root / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_app.py").write_text("def test_one():\n    pass\n", encoding="utf-8")
    index = build_index(root, root / "index.sqlite")
    runner = MutationRunner(
        MutationConfig(
            project_root=root,
            source="app.py",
            selection_snapshot={"snapshot_id": "frozen"},
        )
    )
    runner._campaign_index = index
    runner._campaign_source_rel = "app.py"
    runner._campaign_function_id = "app.py::choose"
    runner._nodeid_validation_context = build_nodeid_validation_context(index, root)

    class ExplodingImpact:
        def select_tests(self, *args, **kwargs):
            # Fail if the frozen path touches SQLite impact selection.
            raise AssertionError("impact selection must not run for frozen input")

    runner._impact_adapter = ExplodingImpact()
    monkeypatch.setattr(
        runner_module,
        "_context_tests",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("context fallback must not run")),
    )
    monkeypatch.setattr(
        runner_module,
        "_static_related_tests",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("static fallback must not run")),
    )
    mutant = Mutant("m1", "condition_to_not", 1, 0, "return", "return", 0, 6)
    level = LevelSpec(
        "L1",
        "frozen",
        (),
        ("tests/test_app.py::test_one",),
        ("tests/test_app.py",),
    )
    selection = runner._build_mutant_selection(mutant, level)
    assert selection.source == "frozen-selection"
    assert selection.nodeids == ("tests/test_app.py::test_one",)
