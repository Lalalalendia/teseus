from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

from test_intelligence_unified_v1 import __version__, cli, maintenance
from test_intelligence_unified_v1.index import build_index, plan_selection
from test_intelligence_unified_v1.mutations import available_mutation_operators, generate_mutants
from test_intelligence_unified_v1.runner import MutationConfig, MutationRunner


FIXTURES_ROOT = Path(__file__).parent / "fixtures"
SNAPSHOT_PATH = Path(__file__).parent / "snapshots" / "e00_v126_baseline.json"


def _load_snapshot() -> dict[str, object]:
    # Load the committed E-00 contract without including machine-specific paths.
    return json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))


def _copy_fixture(name: str, destination: Path) -> Path:
    # Copy a fixture into an isolated checkout so reports and SQLite files never pollute it.
    root = destination / name
    shutil.copytree(FIXTURES_ROOT / name, root)
    return root


def _run_golden_campaign(root: Path) -> dict[str, object]:
    # Run the smallest deterministic campaign used as the v1.26 semantic anchor.
    return MutationRunner(
        MutationConfig(
            project_root=root,
            source="app.py",
            function="classify",
            test_command_argv=(
                sys.executable,
                "-B",
                "-c",
                "from app import classify; assert classify(1) == 2",
            ),
            operators=("condition_to_not",),
            max_mutants=1,
            no_escalation=True,
            use_baseline_cache=False,
            reports_dir=root / "reports",
        )
    ).run()


def _normalize_campaign(report: dict[str, object]) -> dict[str, object]:
    # Remove timestamps, absolute paths and measured timings while preserving semantic output.
    results = report["results"]
    result = results[0]
    mutant = result["mutant"]
    level_result = result["level_results"][0]
    return {
        "function_id": report["target"]["function_id"],
        "metrics": {
            key: report["metrics"][key]
            for key in ("counts", "mutation_score", "selection_escapes", "total_mutants", "operator_stats")
        },
        "mutant_ids": [item["mutant_id"] for item in report["mutants"]],
        "result_contract": {
            "level": level_result["level"],
            "level_exit_code": level_result["result"]["exit_code"],
            "mutant": {
                key: mutant[key]
                for key in ("mutant_id", "mutation", "operator_version", "line_no", "column_no", "original", "replacement")
            },
            "restore_verified": result["restore_verified"],
            "selection_source": result["selection"]["levels"][0]["source"],
        },
        "result_statuses": [item["status"] for item in results],
        "runner_version": report["runner_version"],
        "schema_version": report["schema_version"],
        "source_sha256": report["source_snapshot"]["original_sha256"],
        "source_path": report["target"]["source_path"],
        "status": report["status"],
    }


def _sqlite_schema(path: Path) -> dict[str, object]:
    # Capture the normalized SQLite schema while excluding data and volatile metadata values.
    tables: dict[str, object] = {}
    with sqlite3.connect(path) as connection:
        metadata_keys = sorted(row[0] for row in connection.execute("SELECT key FROM metadata"))
        for table in ("metadata", "files", "functions", "tests"):
            columns = [row[1] for row in connection.execute(f"PRAGMA table_info({table})")]
            indexes = sorted(row[1] for row in connection.execute(f"PRAGMA index_list({table})"))
            tables[table] = {"columns": columns, "indexes": indexes}
    return {"metadata_keys": metadata_keys, "tables": tables}


def _normalize_index(index: dict[str, object], database: Path) -> dict[str, object]:
    # Preserve source entities and schema shape without freezing mtimes or index fingerprints.
    return {
        "files": sorted(index["files"]),
        "functions": [item["function_id"] for item in index["functions"]],
        "sqlite": _sqlite_schema(database),
        "summary": {key: index["summary"][key] for key in ("files", "functions", "parse_errors", "tests")},
        "tests": [item["nodeid"] for item in index["tests"]],
        "schema_version": index["schema_version"],
    }


def _normalize_selection(selection: object) -> dict[str, object]:
    # Freeze selected tests and reasons while intentionally omitting volatile index fingerprints.
    level = selection.levels[0]
    return {
        "algorithm_version": selection.algorithm_version,
        "files": list(level.files),
        "function_id": selection.function_id,
        "level_name": level.name,
        "level_reason": level.reason,
        "nodeids": list(level.nodeids),
        "schema_version": selection.schema_version,
        "selected_tests": list(selection.selected_tests),
        "source_path": selection.source_path,
        "source_sha256": selection.source_sha256,
    }


def _cli_commands() -> list[str]:
    # Extract the public command names from the same parser used by the CLI entry point.
    parser = cli._build_parser()
    subparsers = next(action for action in parser._actions if getattr(action, "choices", None) is not None)
    return sorted(subparsers.choices)

def _cli_help_contract() -> dict[str, object]:
    # Freeze semantic top-level help content without depending on argparse line wrapping.
    parser = cli._build_parser()
    subparsers = next(
        action
        for action in parser._actions
        if getattr(action, "choices", None) is not None
    )
    command_help = {
        str(action.dest): str(action.help)
        for action in subparsers._choices_actions
    }
    options = tuple(
        tuple(str(option) for option in action.option_strings)
        for action in parser._actions
        if action.option_strings
    )
    return {
        "prog": parser.prog,
        "description": parser.description,
        "options": options,
        "commands": command_help,
    }

def test_e00_fixture_matrix_is_complete_and_dynamic_collection_is_visible(tmp_path: Path) -> None:
    # Keep all planned v1.26 executable fixture projects present and deterministic.
    snapshot = _load_snapshot()
    fixture_names = sorted(
        path.name
        for path in FIXTURES_ROOT.iterdir()
        if path.is_dir()
        and (
            (path / "pyproject.toml").is_file()
            or any(path.rglob("*.py"))
        )
    )
    assert fixture_names == snapshot["fixtures"]
    assert (FIXTURES_ROOT / "golden_project" / "app.py").is_file()
    assert (FIXTURES_ROOT / "editable_install_project" / "src" / "editable_fixture" / "core.py").is_file()
    assert (FIXTURES_ROOT / "src_layout_project" / "src" / "src_fixture" / "core.py").is_file()
    assert not (FIXTURES_ROOT / "namespace_project" / "src" / "namespace_fixture" / "__init__.py").exists()
    assert (FIXTURES_ROOT / "dynamic_pytest_project" / "conftest.py").is_file()
    assert "THESEUS_E00_FLAKY_PASS" in (FIXTURES_ROOT / "flaky_project" / "test_flaky.py").read_text(encoding="utf-8")

    environment = os.environ.copy()
    package_root = Path(__file__).resolve().parents[1]
    inherited = []
    for item in environment.get("PYTHONPATH", "").split(os.pathsep):
        if not item:
            continue
        candidate = Path(item)
        inherited.append(str((Path.cwd() / candidate).resolve() if not candidate.is_absolute() else candidate))
    environment["PYTHONPATH"] = os.pathsep.join(
        [*inherited, str(package_root), str(package_root.parent)]
    )
    dynamic_root = FIXTURES_ROOT / "dynamic_pytest_project"
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-m",
            "pytest",
            "--collect-only",
            "-q",
            "-p",
            "no:cacheprovider",
            "--confcutdir",
            str(dynamic_root),
        ],
        cwd=dynamic_root,
        capture_output=True,
        env=environment,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    collected = sorted(
        "test_dynamic.py::" + line.strip().split("::", 1)[1]
        for line in result.stdout.splitlines()
        if "test_dynamic.py::test_generated[" in line
    )
    assert collected == [
        "test_dynamic.py::test_generated[one]",
        "test_dynamic.py::test_generated[two]",
    ]


def test_e00_golden_campaign_matches_report_and_mutant_snapshots(tmp_path: Path) -> None:
    # Freeze semantic report fields, mutant IDs and restoration behavior for v1.26.
    snapshot = _load_snapshot()
    root = _copy_fixture("golden_project", tmp_path)
    report = _run_golden_campaign(root)
    assert _normalize_campaign(report) == snapshot["golden_campaign"]
    source = (root / "app.py").read_bytes()
    assert hashlib.sha256(source).hexdigest() == snapshot["golden_campaign"]["source_sha256"]
    assert source == (root / "app.py").read_bytes()


def test_e00_index_selection_and_sqlite_schema_match_snapshots(tmp_path: Path) -> None:
    # Freeze the v3 index entities, selection reasons and normalized SQLite tables.
    snapshot = _load_snapshot()
    root = _copy_fixture("golden_project", tmp_path)
    database = tmp_path / "index.sqlite"
    index = build_index(root, database)
    selection = plan_selection(root, index, "app.py", "classify")
    assert _normalize_index(index, database) == snapshot["index"]
    assert _normalize_selection(selection) == snapshot["selection"]


def test_e00_cli_version_and_operator_catalog_match_snapshots() -> None:
    # Keep public version, semantic help content and operator order stable across Python versions.
    snapshot = _load_snapshot()

    assert __version__ == snapshot["cli"]["version"]
    assert _cli_commands() == snapshot["cli"]["commands"]

    assert _cli_help_contract() == {
        "prog": "test-intelligence-unified",
        "description": (
            "Fast, safe test selection, impact context and mutation testing for Codex."
        ),
        "options": (
            ("-h", "--help"),
            ("--version",),
        ),
        "commands": {
            "index": "build a compact AST/function/test index",
            "select": "freeze selected tests and their reasons",
            "mutate": "run baseline, safe mutants and escalation reports",
            "run": "run baseline, safe mutants and escalation reports",
            "recover": "restore a target from a recovery manifest",
            "resume": "recover and resume an interrupted worker campaign",
            "inspect": "print a compact report summary",
            "trace": "trace a selected command into compact line/function JSON",
            "trace-merge": "merge line/function trace artifacts",
            "test": "run one safe test command and save its result",
            "doctor": "check runtime, cache, index and recovery health",
            "gc": "plan or remove completed old reports",
            "benchmark": "run local performance workloads and probes",
            "benchmark-history": "inspect benchmark regression history",
            "ci-matrix": "print the deterministic CI lane contract",
            "ci": "run selected CI lanes with bounded JSON reports",
            "validate": "run deterministic production-scale stats validation",
            "stats": "show historical per-test execution statistics",
            "impact-migrate": "create or migrate an indexed SQLite impact database",
        },
    }

    assert list(available_mutation_operators()) == snapshot["mutation_operator_catalog"]
    assert [
        mutant.mutant_id
        for mutant in generate_mutants(
            "def classify(value):\n"
            "    if value > 0:\n"
            "        return value + 1\n"
            "    return 0\n",
            function_range=(1, 4),
            operators=("condition_to_not",),
            max_mutants=1,
        )
    ] == snapshot["golden_campaign"]["mutant_ids"]


def test_e00_benchmark_contract_matches_snapshot(tmp_path: Path) -> None:
    # Freeze benchmark workload identity and semantic E2E output, not machine-specific timings.
    snapshot = _load_snapshot()
    result = maintenance.benchmark(tmp_path, tmp_path / "reports")
    small_e2e = result["workloads"]["small_e2e_campaign"]["details"]
    normalized = {
        "benchmark_version": result["benchmark_version"],
        "schema_version": result["schema_version"],
        "subprocess_passed": result["subprocess_passed"],
        "workload_count": len(result["workloads"]),
        "workload_order": result["workload_order"],
        "small_e2e_campaign": {
            key: small_e2e[key] for key in ("status", "mutants", "results", "mutation_score")
        },
    }
    assert normalized == snapshot["benchmark"]
