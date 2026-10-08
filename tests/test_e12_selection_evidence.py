from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

from test_intelligence_unified_v1.impact import V2_SCHEMA_SQL
from test_intelligence_unified_v1.index import add_domain_levels, build_index, plan_selection
from test_intelligence_unified_v1.runner import MutationConfig, MutationRunner
from theseus_contracts import SelectionEvidence as PublicSelectionEvidence
from theseus_contracts import SelectionSnapshot as PublicSelectionSnapshot


def _selection_project(root: Path) -> None:
    # Create one source function and one real pytest nodeid for selection fixtures.
    (root / "app.py").write_text(
        "def choose(value):\n"
        "    if value > 0:\n"
        "        return 1\n"
        "    return 0\n",
        encoding="utf-8",
    )
    tests = root / "tests"
    tests.mkdir()
    (tests / "test_app.py").write_text(
        "from app import choose\n\n"
        "def test_choose():\n"
        "    assert choose(1) == 1\n",
        encoding="utf-8",
    )


def _impact_database(path: Path, *, rows: tuple[tuple[object, ...], ...] = ()) -> None:
    # Build the normalized v2 impact schema with deterministic duplicate rows.
    with sqlite3.connect(path) as connection:
        connection.executescript(V2_SCHEMA_SQL)
        connection.executemany(
            "INSERT INTO impact_links(source_path,function_id,line_no,nodeid,executions,kills,median_ms) "
            "VALUES (?,?,?,?,?,?,?)",
            rows,
        )


def test_e12_plan_publishes_normalized_evidence_metadata(tmp_path: Path) -> None:
    # Attach runtime and historical-kill evidence to one deduplicated nodeid.
    _selection_project(tmp_path)
    index = build_index(tmp_path, tmp_path / "index.sqlite")
    impact = tmp_path / "impact.sqlite"
    _impact_database(
        impact,
        rows=(
            ("app.py", "app.py::choose", 2, "tests/test_app.py::test_choose", 2, 0, 4.0),
            ("./app.py", "app.py::choose", 2, "tests/test_app.py::test_choose", 9, 3, 2.0),
        ),
    )
    selection = plan_selection(
        tmp_path,
        index,
        "app.py",
        "choose",
        impact_db=impact,
    )

    rows = selection.evidence["tests/test_app.py::test_choose"]
    assert selection.levels[0].nodeids == ("tests/test_app.py::test_choose",)
    assert {item.source for item in rows} == {"runtime_function", "historical_kill"}
    assert all(item.source_snapshot == selection.snapshot_id for item in rows)
    assert all(item.revision and item.environment and item.confidence_class for item in rows)


def test_e12_incomplete_dynamic_graph_uses_domain_fallback(tmp_path: Path) -> None:
    # Never turn an empty dynamic graph into a precise static L1 claim.
    _selection_project(tmp_path)
    index = build_index(tmp_path, tmp_path / "index.sqlite")
    impact = tmp_path / "empty-impact.sqlite"
    _impact_database(impact)
    selection = plan_selection(tmp_path, index, "app.py", "choose", impact_db=impact)
    expanded = add_domain_levels(selection, None)

    assert selection.levels[0].nodeids == ()
    assert selection.evidence["__selection__"][0].source == "domain_fallback"
    assert expanded.evidence["__level__:L2"][0].source == "domain_fallback"
    assert expanded.evidence["__level__:L3"][0].source == "domain_fallback"


def test_e12_runner_marks_unobserved_l1_and_executes_l2(tmp_path: Path) -> None:
    # Use the broad level safely when no dynamic evidence can justify an exact L1 run.
    _selection_project(tmp_path)
    impact = tmp_path / "empty-impact.sqlite"
    _impact_database(impact)
    report = MutationRunner(
        MutationConfig(
            project_root=tmp_path,
            source="app.py",
            function="choose",
            impact_db=impact,
            operators=("condition_to_not",),
            max_mutants=1,
            common_command_argv=(sys.executable, "-m", "pytest", "tests", "-q"),
            use_baseline_cache=False,
            reports_dir=tmp_path / "reports",
        )
    ).run()

    assert report["status"] == "complete"
    assert report["results"][0]["selection"]["levels"][0]["source"] == "domain-fallback"
    assert [item["level"] for item in report["results"][0]["level_results"]] == ["L2"]
    assert report["results"][0]["status"] == "selection_escape"


def test_e12_selection_audit_rechecks_l1_kill_with_l3(tmp_path: Path) -> None:
    # Keep the bounded audit visible when a narrow explicit selection kills a mutant.
    _selection_project(tmp_path)
    command = (sys.executable, "-m", "pytest", "tests/test_app.py", "-q")
    report = MutationRunner(
        MutationConfig(
            project_root=tmp_path,
            source="app.py",
            function="choose",
            test_command_argv=command,
            common_command_argv=command,
            operators=("condition_to_not",),
            max_mutants=1,
            audit_percent=100.0,
            use_baseline_cache=False,
            reports_dir=tmp_path / "reports",
        )
    ).run()

    assert report["status"] == "complete"
    assert report["results"][0]["status"] == "killed"
    assert len(report["selection_audit"]) == 1
    assert report["selection_audit"][0]["level"] == "L3"
    assert "disagreement" in report["selection_audit"][0]


def test_e12_public_evidence_roundtrip() -> None:
    # Keep the public Theseus DTO able to carry the internal evidence contract.
    evidence = PublicSelectionEvidence(
        source="runtime_line",
        source_snapshot="snap-1",
        revision="index-1",
        environment="env-1",
        confidence_class="exact",
        detail="line=2",
    )
    snapshot = PublicSelectionSnapshot(
        snapshot_id="snap-1",
        source_path="app.py",
        source_sha256="sha",
        algorithm_version="selection-v5",
        levels=(),
        evidence={"tests/test_app.py::test_choose": (evidence,)},
    )
    restored = PublicSelectionSnapshot.from_dict(snapshot.to_dict())

    assert restored.evidence["tests/test_app.py::test_choose"][0].source == "runtime_line"
    assert restored.evidence["tests/test_app.py::test_choose"][0].confidence_class == "exact"
