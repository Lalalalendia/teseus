"""Read-only diagnostics, bounded cleanup planning and local benchmarks."""

from __future__ import annotations

import json
import os
import sqlite3
import shutil
import sys
import tempfile
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Sequence

from .commands import resolve_python, run_argv
from .impact import SQLiteImpactAdapter
from .index import build_index, load_index, plan_selection, validate_test_nodeids
from .io_utils import append_json_line, atomic_write_json, iter_json_lines, read_json, utc_now_iso
from .mutations import create_snapshot, generate_mutants, prepare_mutant
from .runner import MutationConfig, MutationRunner
from .test_stats import (
    ingest_test_stats,
    merge_test_stats_databases,
    stats_db_path,
    summarize_test_health_rows,
    summarize_test_runs,
    summarize_test_stats,
)


STORAGE_MANIFEST_NAME = "storage.manifest.json"
STORAGE_BUCKETS = ("artifacts", "recovery", "reports", "stats", "worker_workspaces", "other")
BENCHMARK_HISTORY_NAME = "benchmark_history.jsonl"
VALIDATION_PROFILES = {
    "smoke": {"events": 2_000, "nodeids": 400, "runs": 8, "workers": 2},
    "production": {"events": 100_000, "nodeids": 20_000, "runs": 25, "workers": 4},
}
VALIDATION_PROFILE_NAMES = tuple(VALIDATION_PROFILES)
CI_LANE_NAMES = ("fast", "full", "performance", "slow", "coverage", "validation", "release", "scale", "soak", "mutation")


def ci_matrix() -> dict[str, Any]:
    # Return the deterministic CI lane contract without executing project code.
    python = "python"
    return {
        "schema_version": 1,
        "lanes": [
            {
                "name": "fast",
                "purpose": "required unit and regression tests",
                "markers": "not performance and not slow",
                "commands": [[python, "-m", "pytest", "-q", "-m", "not performance and not slow"]],
                "required": True,
                "machine_dependent": False,
            },
            {
                "name": "full",
                "purpose": "complete functional suite",
                "markers": "all",
                "commands": [[python, "-m", "pytest", "-q"]],
                "required": True,
                "machine_dependent": False,
            },
            {
                "name": "performance",
                "purpose": "wall-clock and hot-path workloads",
                "markers": "performance",
                "commands": [[python, "-m", "pytest", "-q", "-m", "performance"]],
                "required": False,
                "machine_dependent": True,
            },
            {
                "name": "slow",
                "purpose": "subprocess, timeout and worker recovery tests",
                "markers": "slow",
                "commands": [[python, "-m", "pytest", "-q", "-m", "slow"]],
                "required": False,
                "machine_dependent": True,
            },
            {
                "name": "coverage",
                "purpose": "branch coverage gate for the complete functional suite",
                "markers": "all",
                "commands": [
                    ["coverage", "run", "--branch", "-m", "pytest", "-q"],
                    ["coverage", "report", "--fail-under=78"],
                ],
                "required": False,
                "machine_dependent": False,
                "optional_dependency": "coverage",
                "fail_under": 78,
            },
            {
                "name": "validation",
                "purpose": "production-scale synthetic stats ingestion and summary validation",
                "markers": "synthetic",
                "commands": [
                    [
                        python,
                        "-m",
                        "test_intelligence_unified_v1",
                        "validate",
                        ".",
                        "--profile",
                        "production",
                    ]
                ],
                "required": False,
                "machine_dependent": True,
            },
            {
                "name": "release",
                "purpose": "clean-wheel installed acceptance and incomplete-wheel rejection",
                "markers": "release",
                "commands": [[python, "-m", "pytest", "-q", "tests/test_distribution_acceptance.py", "-m", "release"]],
                "required": True,
                "machine_dependent": True,
                "required_environment": "THESEUS_RELEASE_WHEEL",
            },
            {
                "name": "scale",
                "purpose": "real 24/100 request distributed campaigns and worker churn",
                "markers": "scale",
                "commands": [[python, "-m", "pytest", "-q", "tests/test_distributed_scale_acceptance.py", "tests/test_network_scale_acceptance.py", "-m", "scale"]],
                "required": False,
                "machine_dependent": True,
            },
            {
                "name": "soak",
                "purpose": "repeated remote execution with workspace and resource leak checks",
                "markers": "soak",
                "commands": [[python, "-m", "pytest", "-q", "tests/test_distributed_scale_acceptance.py", "-m", "soak"]],
                "required": False,
                "machine_dependent": True,
                "environment": {"THESEUS_RUN_SOAK": "1"},
            },
            {
                "name": "mutation",
                "purpose": "golden behavior plus control-plane schema/hash/lease/identity mutation gate",
                "markers": "mutation",
                "commands": [[
                    python,
                    "-m",
                    "pytest",
                    "-q",
                    "tests/test_campaign_golden_regression.py",
                    "tests/test_control_plane_mutation_gate.py",
                ]],
                "required": True,
                "machine_dependent": False,
            },
        ],
    }


def run_production_validation(
    project_root: Path,
    reports_dir: Path,
    *,
    profile: str = "smoke",
    event_count: int | None = None,
    nodeid_count: int | None = None,
    run_count: int | None = None,
    worker_count: int | None = None,
    result_file: Path | None = None,
) -> dict[str, Any]:
    # Run a deterministic synthetic stats workload and persist its validation timings.
    if profile not in VALIDATION_PROFILES:
        raise ValueError(f"unknown validation profile: {profile}")
    defaults = VALIDATION_PROFILES[profile]
    workload = {
        "events": int(event_count if event_count is not None else defaults["events"]),
        "nodeids": int(nodeid_count if nodeid_count is not None else defaults["nodeids"]),
        "runs": int(run_count if run_count is not None else defaults["runs"]),
        "workers": int(worker_count if worker_count is not None else defaults["workers"]),
    }
    if any(value <= 0 for value in workload.values()):
        raise ValueError("validation workload values must be positive")
    root = project_root.resolve()
    reports_root = reports_dir.resolve()
    result_path = (result_file or reports_root / "validation" / f"{profile}.json").resolve()
    started = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="test-intelligence-validation-") as temporary_name:
        temporary_root = Path(temporary_name)
        event_dir = temporary_root / "events"
        event_dir.mkdir(parents=True, exist_ok=True)
        handles = [
            (event_dir / f"worker-{worker_index:02d}.jsonl").open("w", encoding="utf-8")
            for worker_index in range(workload["workers"])
        ]
        generation_started = time.perf_counter()
        try:
            for index in range(workload["events"]):
                phase = "baseline" if index % 10 == 0 else "mutant"
                failed = index % 997 == 0
                event = {
                    "event_id": f"validation-event-{index:07d}",
                    "run_id": f"validation-run-{index % workload['runs']:04d}",
                    "source_path": "validation_fixture.py",
                    "target_sha256": "validation-target-sha256",
                    "phase": phase,
                    "level": "L1",
                    "mutant_id": f"validation-mutant-{index:07d}" if phase == "mutant" else None,
                    "nodeid": f"tests/generated.py::test_{index % workload['nodeids']:05d}",
                    "outcome": "failed" if failed else "passed",
                    "duration_ms": float(1 + index % 41),
                    "first_failure": failed and phase == "mutant",
                    "worker_id": f"gw{index % workload['workers']}",
                    "retry": False,
                    "recorded_at": "2026-01-01T00:00:00+00:00",
                }
                handles[index % workload["workers"]].write(json.dumps(event, separators=(",", ":")) + "\n")
        finally:
            for handle in handles:
                handle.close()
        generation_seconds = time.perf_counter() - generation_started
        database_path = temporary_root / "test_stats.sqlite"
        ingestion_started = time.perf_counter()
        ingestion = ingest_test_stats(
            event_dir,
            database_path,
            project_root=root,
            run_id="validation-ingestion",
            source_path="validation_fixture.py",
        )
        ingestion_seconds = time.perf_counter() - ingestion_started
        summary_started = time.perf_counter()
        test_rows = summarize_test_stats(
            database_path,
            project_root=root,
            limit=max(workload["nodeids"], workload["events"]),
            order_by="nodeid",
        )
        test_summary_seconds = time.perf_counter() - summary_started
        run_summary_started = time.perf_counter()
        run_rows = summarize_test_runs(
            database_path,
            project_root=root,
            limit=workload["runs"],
        )
        run_summary_seconds = time.perf_counter() - run_summary_started
    expected_tests = min(workload["events"], workload["nodeids"])
    expected_runs = min(workload["events"], workload["runs"])
    checks = {
        "events_ingested": ingestion["events_ingested"] == workload["events"],
        "invalid_events_zero": ingestion["invalid_events"] == 0,
        "test_cardinality": len(test_rows) == expected_tests,
        "run_cardinality": len(run_rows) == expected_runs,
    }
    result = {
        "schema_version": 1,
        "validation_version": "v1.26",
        "profile": profile,
        "project_root": str(root),
        "workload": workload,
        "expected": {"tests": expected_tests, "runs": expected_runs},
        "observed": {
            "events_ingested": ingestion["events_ingested"],
            "invalid_events": ingestion["invalid_events"],
            "tests": len(test_rows),
            "runs": len(run_rows),
        },
        "checks": checks,
        "timings": {
            "generation_seconds": generation_seconds,
            "ingestion_seconds": ingestion_seconds,
            "test_summary_seconds": test_summary_seconds,
            "run_summary_seconds": run_summary_seconds,
            "total_seconds": time.perf_counter() - started,
        },
        "passed": all(checks.values()),
        "result_file": str(result_path),
    }
    atomic_write_json(result_path, result, durability="normal", category="report")
    return result


def run_ci_lanes(
    project_root: Path,
    lane_names: Sequence[str],
    reports_dir: Path,
    *,
    python_executable: str | None = None,
    timeout_seconds: float = 900.0,
    fail_fast: bool = False,
    require_optional: bool = False,
    result_file: Path | None = None,
) -> dict[str, Any]:
    # Execute selected CI lanes through safe argv and retain only bounded subprocess summaries.
    root = project_root.resolve()
    reports_root = reports_dir.resolve()
    output_root = reports_root / "ci"
    output_root.mkdir(parents=True, exist_ok=True)
    contract = ci_matrix()
    definitions = {str(item["name"]): item for item in contract["lanes"]}
    selected = tuple(str(name) for name in lane_names)
    unknown = sorted(set(selected) - set(definitions))
    if unknown:
        raise ValueError(f"unknown CI lane(s): {', '.join(unknown)}")
    python = resolve_python(root, python_executable)
    coverage_executable = shutil.which("coverage")
    records: list[dict[str, Any]] = []
    stopped = False
    for lane_name in selected:
        definition = definitions[lane_name]
        if stopped:
            records.append(
                {
                    "name": lane_name,
                    "status": "not_run",
                    "passed": False,
                    "reason": "fail_fast",
                    "required": bool(definition.get("required", False)),
                }
            )
            continue
        commands = definition.get("commands", [])
        required_environment = str(definition.get("required_environment", "")).strip()
        if required_environment and not os.environ.get(required_environment):
            records.append(
                {
                    "name": lane_name,
                    "status": "failed" if bool(definition.get("required")) else "skipped",
                    "passed": False if bool(definition.get("required")) else True,
                    "reason": f"required environment variable is missing: {required_environment}",
                    "required": bool(definition.get("required", False)),
                    "required_environment": required_environment,
                }
            )
            stopped = fail_fast and bool(definition.get("required"))
            continue
        needs_coverage = any(command and command[0] == "coverage" for command in commands)
        if needs_coverage and not coverage_executable:
            status = "failed" if require_optional else "skipped"
            records.append(
                {
                    "name": lane_name,
                    "status": status,
                    "passed": status == "skipped",
                    "reason": "coverage executable is not installed",
                    "required": bool(definition.get("required", False)),
                    "optional_dependency": definition.get("optional_dependency"),
                }
            )
            stopped = fail_fast and status == "failed"
            continue
        started = time.perf_counter()
        command_results: list[dict[str, Any]] = []
        lane_failed = False
        lane_env: dict[str, str] | None = None
        if needs_coverage or definition.get("environment"):
            lane_env = dict(os.environ)
            if needs_coverage:
                lane_env["COVERAGE_FILE"] = str(output_root / "coverage.data")
            lane_env.update({str(key): str(value) for key, value in dict(definition.get("environment", {})).items()})
        for command_index, raw_command in enumerate(commands):
            command = [str(item) for item in raw_command]
            if command and command[0] == "python":
                command[0] = python
            elif command and command[0] == "coverage":
                command[0] = coverage_executable or "coverage"
            if lane_name == "validation":
                command.extend(
                    [
                        "--reports-dir",
                        str(output_root),
                        "--result-file",
                        str(output_root / "validation-result.json"),
                    ]
                )
            if any(command[index : index + 2] == ["-m", "pytest"] for index in range(len(command) - 1)) and "--basetemp" not in command:
                command.extend(["--basetemp", str(output_root / f"{lane_name}-pytest-tmp")])
            artifact = output_root / f"{lane_name}-{command_index:02d}.txt"
            try:
                process = run_argv(
                    command,
                    cwd=root,
                    timeout_seconds=timeout_seconds,
                    env=lane_env,
                    output_artifact=artifact,
                )
                command_result = process.to_dict()
                command_results.append(command_result)
                if not process.passed:
                    lane_failed = True
                    break
            except (OSError, ValueError) as exc:
                command_results.append(
                    {
                        "argv": command,
                        "exit_code": None,
                        "passed": False,
                        "error": str(exc),
                        "output_artifact": str(artifact),
                    }
                )
                lane_failed = True
                break
        status = "failed" if lane_failed else "passed"
        records.append(
            {
                "name": lane_name,
                "status": status,
                "passed": not lane_failed,
                "required": bool(definition.get("required", False)),
                "elapsed_seconds": time.perf_counter() - started,
                "commands": command_results,
            }
        )
        stopped = fail_fast and lane_failed
    result = {
        "schema_version": 1,
        "project_root": str(root),
        "reports_dir": str(reports_root),
        "selected_lanes": list(selected),
        "lanes": records,
        "passed": all(item.get("passed") is True for item in records),
        "failed_lanes": [item["name"] for item in records if item.get("status") == "failed"],
        "skipped_lanes": [item["name"] for item in records if item.get("status") == "skipped"],
        "not_run_lanes": [item["name"] for item in records if item.get("status") == "not_run"],
    }
    if result_file is not None:
        result_path = result_file.resolve()
        result["result_file"] = str(result_path)
        atomic_write_json(result_path, result, durability="normal", category="report")
    return result


def storage_manifest_path(reports_dir: Path) -> Path:
    # Return the durable accounting manifest kept beside campaign reports.
    return reports_dir.resolve() / STORAGE_MANIFEST_NAME


def _storage_bucket(relative_path: Path) -> str:
    # Classify one report-tree path without opening or rereading its contents.
    parts = set(relative_path.parts)
    name = relative_path.name
    if "workers" in parts:
        return "worker_workspaces"
    if "artifacts" in parts:
        return "artifacts"
    if "recovery" in parts:
        return "recovery"
    if "test_stats_events" in parts or "test_stats_workers" in parts or name.startswith("test_stats.sqlite"):
        return "stats"
    if relative_path.parent == Path("."):
        return "reports"
    return "other"


def build_storage_manifest(reports_dir: Path) -> dict[str, Any]:
    # Scan the reports tree once and produce bounded file/byte accounting by category.
    root = reports_dir.resolve()
    manifest_path = storage_manifest_path(root)
    categories = {name: {"files": 0, "bytes": 0} for name in STORAGE_BUCKETS}
    total_files = 0
    total_bytes = 0
    for path in root.rglob("*"):
        if not path.is_file() or path.resolve() == manifest_path:
            continue
        try:
            size = int(path.stat().st_size)
            relative = path.relative_to(root)
        except OSError:
            continue
        bucket = categories[_storage_bucket(relative)]
        bucket["files"] += 1
        bucket["bytes"] += size
        total_files += 1
        total_bytes += size
    return {
        "schema_version": 1,
        "root": str(root),
        "generated_at": utc_now_iso(),
        "files": total_files,
        "bytes": total_bytes,
        "categories": categories,
    }


def load_storage_manifest(reports_dir: Path, *, deep: bool = False) -> dict[str, Any]:
    # Reuse cached accounting unless an explicit deep refresh or invalid cache requires a scan.
    root = reports_dir.resolve()
    path = storage_manifest_path(root)
    if not deep and path.exists():
        try:
            value = read_json(path)
            if isinstance(value, dict) and value.get("root") == str(root) and value.get("schema_version") == 1:
                return value | {"source": "cached", "manifest_path": str(path)}
        except (OSError, ValueError):
            pass
    value = build_storage_manifest(root)
    atomic_write_json(path, value, durability="normal", category="report")
    return value | {"source": "fresh", "manifest_path": str(path)}


def benchmark_history_path(reports_dir: Path) -> Path:
    # Return the append-only benchmark history location beside campaign reports.
    return reports_dir.resolve() / BENCHMARK_HISTORY_NAME


def load_benchmark_history(path: Path, *, limit: int = 50) -> list[dict[str, Any]]:
    # Stream only the latest benchmark records so history inspection stays bounded.
    if limit <= 0 or not path.exists():
        return []
    records: deque[dict[str, Any]] = deque(maxlen=limit)
    for item in iter_json_lines(path):
        records.append(item)
    return list(records)


def compare_benchmark_runs(
    current: dict[str, Any],
    previous: dict[str, Any] | None,
    *,
    relative_threshold: float = 0.10,
    absolute_threshold_seconds: float = 0.005,
) -> dict[str, Any]:
    # Compare workload wall-clock values without making ordinary CI timing a hard gate.
    current_workloads = current.get("workloads", {}) if isinstance(current, dict) else {}
    previous_workloads = previous.get("workloads", {}) if isinstance(previous, dict) else {}
    order = current.get("workload_order", []) if isinstance(current, dict) else []
    if not isinstance(order, list):
        order = list(current_workloads) if isinstance(current_workloads, dict) else []
    rows: dict[str, dict[str, Any]] = {}
    potential_regressions = 0
    improvements = 0
    compared = 0
    new_workloads = 0
    for name in order:
        current_row = current_workloads.get(name, {}) if isinstance(current_workloads, dict) else {}
        previous_row = previous_workloads.get(name, {}) if isinstance(previous_workloads, dict) else {}
        current_seconds = float(current_row.get("elapsed_seconds", 0.0) or 0.0)
        previous_seconds = (
            float(previous_row.get("elapsed_seconds", 0.0) or 0.0)
            if previous_row
            else None
        )
        if previous_seconds is None:
            new_workloads += 1
            rows[name] = {
                "current_seconds": current_seconds,
                "previous_seconds": None,
                "delta_seconds": None,
                "delta_percent": None,
                "potential_regression": False,
                "improved": False,
            }
            continue
        compared += 1
        delta = current_seconds - previous_seconds
        delta_percent = (delta / previous_seconds) if previous_seconds else None
        potential_regression = bool(
            delta > absolute_threshold_seconds
            and (delta_percent is None or delta_percent > relative_threshold)
        )
        improved = delta < -absolute_threshold_seconds
        potential_regressions += int(potential_regression)
        improvements += int(improved)
        rows[name] = {
            "current_seconds": current_seconds,
            "previous_seconds": previous_seconds,
            "delta_seconds": delta,
            "delta_percent": delta_percent,
            "potential_regression": potential_regression,
            "improved": improved,
        }
    return {
        "schema_version": 1,
        "previous_available": previous is not None,
        "thresholds": {
            "relative_percent": relative_threshold * 100.0,
            "absolute_seconds": absolute_threshold_seconds,
        },
        "workloads": rows,
        "summary": {
            "compared": compared,
            "new_workloads": new_workloads,
            "potential_regressions": potential_regressions,
            "improvements": improvements,
        },
    }


def record_benchmark_history(
    history_path: Path,
    benchmark_result: dict[str, Any],
) -> dict[str, Any]:
    # Append one semantic benchmark snapshot and compare it with the latest snapshot for this project.
    path = history_path.resolve()
    previous: dict[str, Any] | None = None
    project_root = benchmark_result.get("project_root")
    if path.exists():
        for item in iter_json_lines(path):
            if item.get("project_root") == project_root:
                previous = item
    comparison = compare_benchmark_runs(benchmark_result, previous)
    record = {
        "schema_version": 1,
        "recorded_at": utc_now_iso(),
        "benchmark_version": benchmark_result.get("benchmark_version"),
        "project_root": benchmark_result.get("project_root"),
        "workload_order": benchmark_result.get("workload_order", []),
        "workloads": benchmark_result.get("workloads", {}),
        "fixture": benchmark_result.get("fixture", {}),
    }
    append_json_line(path, record, durability="normal", category="report")
    return {
        "history_path": str(path),
        "recorded": True,
        "previous_available": previous is not None,
        "comparison": comparison,
    }


def inspect_benchmark_history(path: Path, *, limit: int = 20) -> dict[str, Any]:
    # Inspect a bounded history tail and compare the two latest records for quick CI diagnosis.
    records = load_benchmark_history(path.resolve(), limit=max(0, limit))
    latest = records[-1] if records else None
    previous = None
    if latest is not None:
        for item in reversed(records[:-1]):
            if item.get("project_root") == latest.get("project_root"):
                previous = item
                break
    comparison = (
        compare_benchmark_runs(latest, previous)
        if latest is not None
        else compare_benchmark_runs({"workload_order": [], "workloads": {}}, None)
    )
    return {
        "history_path": str(path.resolve()),
        "records_loaded": len(records),
        "latest": latest,
        "comparison": comparison,
    }


def doctor(
    project_root: Path,
    reports_dir: Path,
    index_path: Path | None = None,
    impact_db: Path | None = None,
    *,
    deep: bool = False,
) -> dict[str, Any]:
    # Inspect runtime dependencies, reports, recovery manifests and worker projects.
    reports_dir = reports_dir.resolve()
    reports_dir.mkdir(parents=True, exist_ok=True)
    writable = True
    writable_error: str | None = None
    probe = reports_dir / ".doctor-write-probe"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
    except OSError as exc:
        writable = False
        writable_error = str(exc)

    pytest = run_argv(
        [sys.executable, "-m", "pytest", "--version"],
        cwd=project_root.resolve(),
        timeout_seconds=10,
    )
    xdist = run_argv(
        [sys.executable, "-m", "pytest", "--help"],
        cwd=project_root.resolve(),
        timeout_seconds=10,
    )
    locks = [str(path) for path in project_root.resolve().rglob("*.test_intelligence.lock")]
    active_manifests: list[str] = []
    for path in reports_dir.glob("*.manifest.json"):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if value.get("status") == "active":
            active_manifests.append(str(path))

    index_status: dict[str, Any] = {"status": "missing"}
    if index_path and index_path.exists():
        try:
            value = load_index(index_path)
            index_status = {"status": "ok", "schema_version": value.get("schema_version"), "summary": value.get("summary", {})}
        except (OSError, ValueError) as exc:
            index_status = {"status": "incompatible", "error": str(exc)}

    impact_status: dict[str, Any] = {"status": "missing"}
    if impact_db and impact_db.exists():
        adapter = SQLiteImpactAdapter(impact_db)
        try:
            adapter.select_tests("", None)
            impact_status = dict(adapter.diagnostics)
        finally:
            adapter.close()

    usage = shutil.disk_usage(reports_dir)
    try:
        storage = load_storage_manifest(reports_dir, deep=deep)
    except (OSError, ValueError) as exc:
        storage = {
            "status": "unavailable",
            "error": str(exc),
            "manifest_path": str(storage_manifest_path(reports_dir)),
            "files": 0,
            "bytes": 0,
            "categories": {},
        }
    cache_bytes = int(storage.get("bytes", 0) or 0)
    test_stats_path = stats_db_path(reports_dir)
    test_stats_status: dict[str, Any] = {"status": "missing", "path": str(test_stats_path)}
    if test_stats_path.exists():
        try:
            stats_rows = summarize_test_stats(test_stats_path, project_root=project_root, limit=1_000_000, order_by="nodeid")
            test_stats_status = {
                "status": "ok",
                "path": str(test_stats_path),
                "bytes": test_stats_path.stat().st_size,
                "known_tests": len(stats_rows),
                "health": summarize_test_health_rows(stats_rows),
            }
        except (OSError, sqlite3.Error, ValueError) as exc:
            test_stats_status = {"status": "incompatible", "path": str(test_stats_path), "error": str(exc)}
    return {
        "project_root": str(project_root.resolve()),
        "python": {"executable": sys.executable, "version": sys.version.split()[0]},
        "pytest": {"passed": pytest.passed, "output_tail": pytest.output_tail or pytest.output},
        "xdist": {"available": "xdist" in (xdist.output_tail or xdist.output)},
        "reports": {"path": str(reports_dir), "writable": writable, "error": writable_error, "bytes": cache_bytes},
        "storage": storage,
        "stale_locks": locks,
        "active_manifests": active_manifests,
        "worker_campaigns": _worker_campaign_diagnostics(reports_dir),
        "test_stats": test_stats_status,
        "index": index_status,
        "impact": impact_status,
        "free_bytes": usage.free,
    }


def gc_reports(reports_dir: Path, *, keep_runs: int = 50, keep_days: int = 30, dry_run: bool = True) -> dict[str, Any]:
    # Plan bounded cleanup for reports and old completed worker projects.
    reports_dir = reports_dir.resolve()
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(0, keep_days))
    reports = sorted(reports_dir.glob("*.json"), key=lambda path: path.stat().st_mtime, reverse=True)
    planned: list[str] = []
    kept = 0
    for index, report_path in enumerate(reports):
        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if report.get("status") in {"running", "starting"}:
            kept += 1
            continue
        manifest_path = report_path.with_suffix(".manifest.json")
        if manifest_path.exists():
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                manifest = {}
            if manifest.get("status") == "active":
                kept += 1
                continue
        created = report.get("created_at")
        old_by_date = True
        if isinstance(created, str):
            try:
                old_by_date = datetime.fromisoformat(created).astimezone(timezone.utc) < cutoff
            except ValueError:
                pass
        if index < max(0, keep_runs) or not old_by_date:
            kept += 1
            continue
        companions = [
            report_path,
            report_path.with_suffix(".md"),
            report_path.with_suffix(".results.jsonl"),
            report_path.with_suffix(".state.json"),
            manifest_path,
        ]
        artifacts = reports_dir / "artifacts"
        companions.extend(artifacts.glob(f"{report_path.stem}__*.txt"))
        stats_events = reports_dir / "test_stats_events" / report_path.stem
        if stats_events.exists():
            planned.append(str(stats_events))
            if not dry_run:
                shutil.rmtree(stats_events)
        companions.extend((reports_dir / "test_stats_workers").glob(f"{report_path.stem}.*.sqlite"))
        for path in companions:
            if path.exists():
                planned.append(str(path))
                if not dry_run:
                    path.unlink()
    worker_cleanup = _worker_cleanup(reports_dir, keep_days=keep_days, dry_run=dry_run)
    planned.extend(worker_cleanup.get("planned", []))
    return {
        "reports_dir": str(reports_dir),
        "dry_run": dry_run,
        "planned": planned,
        "kept_reports": kept,
        "worker_cleanup": worker_cleanup,
    }


def _worker_campaign_diagnostics(reports_dir: Path) -> dict[str, Any]:
    # Import worker diagnostics lazily so maintenance remains usable standalone.
    from .workers import inspect_worker_workspaces

    return inspect_worker_workspaces(reports_dir)


def _worker_cleanup(reports_dir: Path, *, keep_days: int, dry_run: bool) -> dict[str, Any]:
    # Import bounded worker cleanup lazily to avoid a module import cycle.
    from .workers import cleanup_worker_workspaces

    return cleanup_worker_workspaces(reports_dir, keep_days=keep_days, dry_run=dry_run)


def benchmark(project_root: Path, reports_dir: Path) -> dict[str, Any]:
    # Run deterministic production-like workloads in temporary fixtures and emit comparable JSON.
    reports_dir = reports_dir.resolve()
    reports_dir.mkdir(parents=True, exist_ok=True)
    workload_order = (
        "nodeid_validation",
        "mutation_generation_full",
        "mutation_generation_limited",
        "mutant_prepare_and_compile",
        "line_selection",
        "stats_ingestion",
        "stats_health_summary",
        "index_cold_build",
        "index_warm_build",
        "worker_stats_merge",
        "small_e2e_campaign",
    )
    workloads: dict[str, dict[str, Any]] = {}

    def measure(name: str, callback: Any) -> None:
        # Measure one named workload and retain bounded JSON-friendly details.
        started = time.perf_counter()
        details = callback()
        elapsed = time.perf_counter() - started
        workloads[name] = {
            "elapsed_seconds": elapsed,
            "elapsed_ms": elapsed * 1000.0,
            "details": details if isinstance(details, dict) else {"value": details},
        }

    with tempfile.TemporaryDirectory(prefix="ti-benchmark-", dir=reports_dir) as temporary:
        # Keep generated source and tests isolated from the project being measured.
        temporary_root = Path(temporary)
        fixture_root = temporary_root / "fixture"
        tests_root = fixture_root / "tests"
        tests_root.mkdir(parents=True, exist_ok=True)
        source = "\n".join(
            [
                "def classify(value):",
                "    if value > 0:",
                "        return value + 1",
                "    return 0",
                "",
            ]
        )
        tests = "\n".join(
            [
                "from app import classify",
                "",
                "",
                "def test_positive_value():",
                "    assert classify(1) == 2",
                "",
                "",
                "def test_non_positive_value():",
                "    assert classify(0) == 0",
                "",
            ]
        )
        (fixture_root / "app.py").write_text(source, encoding="utf-8")
        (tests_root / "test_app.py").write_text(tests, encoding="utf-8")
        fixture = {
            "source": "app.py",
            "function": "classify",
            "test_file": "tests/test_app.py",
            "production_lines": len(source.splitlines()),
            "test_lines": len(tests.splitlines()),
        }
        fixture_index_path = temporary_root / "fixture-index.sqlite"
        fixture_index = build_index(fixture_root, fixture_index_path)
        known_nodeids = tuple(
            str(item["nodeid"])
            for item in fixture_index.get("tests", [])
            if isinstance(item, dict) and item.get("nodeid")
        )
        selected_file = temporary_root / "selected-tests.txt"
        selected_file.write_text("\n".join(known_nodeids[:1]) + "\n", encoding="utf-8")
        recovery_dir = temporary_root / "recovery"

        def write_events(directory: Path, run_id: str, count: int) -> None:
            # Write repeatable JSONL events for ingestion and worker-merge workloads.
            directory.mkdir(parents=True, exist_ok=True)
            journal = directory / "worker-0.jsonl"
            with journal.open("w", encoding="utf-8") as handle:
                for index in range(count):
                    event = {
                        "event_id": f"{run_id}-{index}",
                        "run_id": run_id,
                        "source_path": "app.py",
                        "target_sha256": "fixture-sha",
                        "phase": "standalone",
                        "level": "L1",
                        "mutant_id": f"m-{index % 8}",
                        "nodeid": known_nodeids[index % len(known_nodeids)],
                        "outcome": "passed" if index % 5 else "failed",
                        "duration_ms": float(index % 17) + 1.0,
                        "first_failure": bool(index % 5 == 0),
                        "worker_id": "worker-0",
                        "retry": False,
                        "recorded_at": "2026-01-01T00:00:00+00:00",
                    }
                    handle.write(json.dumps(event, sort_keys=True) + "\n")

        stats_event_dir = temporary_root / "stats-events"
        stats_db = temporary_root / "stats.sqlite"
        write_events(stats_event_dir, "benchmark-main", 128)
        worker_databases: list[Path] = []
        for worker_index in range(2):
            worker_events = temporary_root / f"worker-{worker_index}-events"
            write_events(worker_events, f"benchmark-worker-{worker_index}", 32)
            worker_database = temporary_root / f"worker-{worker_index}.sqlite"
            ingest_test_stats(
                worker_events,
                worker_database,
                project_root=fixture_root,
                run_id=f"benchmark-worker-{worker_index}",
                source_path="app.py",
            )
            worker_databases.append(worker_database)
        project_index_path = temporary_root / "project-index.sqlite"
        function_range = (1, len(source.splitlines()))

        measure(
            "nodeid_validation",
            lambda: {
                "valid": len(
                    validate_test_nodeids(
                        fixture_index,
                        fixture_root,
                        [*known_nodeids, "tests/missing.py::test_missing"] * 32,
                    )[0]
                ),
                "known_tests": len(known_nodeids),
            },
        )

        def generate_full() -> dict[str, Any]:
            # Generate the full deterministic candidate stream for the fixture function.
            mutants = generate_mutants(
                source,
                function_range=function_range,
                operators=("condition_to_not", "gt_to_ge", "plus_to_minus"),
            )
            return {"mutants": len(mutants), "first_id": mutants[0].mutant_id if mutants else None}

        measure("mutation_generation_full", generate_full)

        def generate_limited() -> dict[str, Any]:
            # Exercise the early-stop path instead of slicing a complete candidate list.
            mutants = generate_mutants(
                source,
                function_range=function_range,
                operators=("condition_to_not", "gt_to_ge", "plus_to_minus"),
                max_mutants=1,
            )
            return {"mutants": len(mutants), "first_id": mutants[0].mutant_id if mutants else None}

        measure("mutation_generation_limited", generate_limited)

        def prepare_one() -> dict[str, Any]:
            # Generate the benchmark mutant from the exact snapshot text it will patch.
            snapshot = create_snapshot(
                fixture_root / fixture["source"],
                recovery_dir,
            )
            mutant = generate_mutants(
                snapshot.text,
                function_range=function_range,
                operators=("condition_to_not",),
                max_mutants=1,
            )[0]
            prepared = prepare_mutant(
                snapshot,
                mutant,
            )
            return {
                "mutant_id": prepared.mutant_id,
                "bytes": len(prepared.data),
            }

        measure("mutant_prepare_and_compile", prepare_one)

        def select_lines() -> dict[str, Any]:
            # Build one frozen selection snapshot from the indexed fixture.
            selection = plan_selection(
                fixture_root,
                fixture_index,
                fixture["source"],
                fixture["function"],
                selected_tests_file=selected_file,
            )
            return {"levels": len(selection.levels), "selected": len(selection.selected_tests)}

        measure("line_selection", select_lines)

        def ingest_stats() -> dict[str, Any]:
            # Ingest the fixture journal while stripping temporary paths from benchmark JSON.
            result = ingest_test_stats(
                stats_event_dir,
                stats_db,
                project_root=fixture_root,
                run_id="benchmark-main",
                source_path="app.py",
            )
            return {
                "event_files": int(result.get("event_files", 0)),
                "events_ingested": int(result.get("events_ingested", 0)),
                "invalid_events": int(result.get("invalid_events", 0)),
            }

        measure(
            "stats_ingestion",
            ingest_stats,
        )
        measure(
            "stats_health_summary",
            lambda: summarize_test_health_rows(
                summarize_test_stats(stats_db, project_root=fixture_root, limit=1_000_000, order_by="nodeid")
            ),
        )
        measure(
            "index_cold_build",
            lambda: {"summary": build_index(fixture_root, project_index_path).get("summary", {})},
        )
        measure(
            "index_warm_build",
            lambda: {"summary": build_index(fixture_root, project_index_path).get("summary", {})},
        )

        def merge_workers() -> dict[str, Any]:
            # Merge isolated worker databases while keeping temporary paths out of the result.
            result = merge_test_stats_databases(
                worker_databases,
                temporary_root / "merged.sqlite",
                project_root=fixture_root,
            )
            return {
                "source_databases": int(result.get("source_databases", 0)),
                "events_ingested": int(result.get("events_ingested", 0)),
            }

        measure(
            "worker_stats_merge",
            merge_workers,
        )

        def run_small_campaign() -> dict[str, Any]:
            # Execute one isolated mutation campaign as the semantic end-to-end guardrail.
            report = MutationRunner(
                MutationConfig(
                    project_root=fixture_root,
                    source=fixture["source"],
                    function=fixture["function"],
                    index_path=fixture_index_path,
                    test_command_argv=(
                        sys.executable,
                        "-B",
                        "-c",
                        "from app import classify; assert classify(1) == 2",
                    ),
                    reports_dir=temporary_root / "e2e-reports",
                    max_mutants=1,
                    no_escalation=True,
                    use_baseline_cache=False,
                    operators=("condition_to_not",),
                )
            ).run()
            return {
                "status": report.get("status"),
                "mutants": len(report.get("mutants", [])),
                "results": len(report.get("results", [])),
                "mutation_score": report.get("metrics", {}).get("mutation_score"),
            }

        measure("small_e2e_campaign", run_small_campaign)
    process = run_argv(
        [sys.executable, "-c", "print('test-intelligence-benchmark')"],
        cwd=project_root,
        timeout_seconds=10,
    )
    return {
        "schema_version": 2,
        "benchmark_version": "v1.26",
        "project_root": str(project_root),
        "workload_order": list(workload_order),
        "workloads": workloads,
        "fixture": fixture,
        "cold_index_seconds": workloads["index_cold_build"]["elapsed_seconds"],
        "warm_index_seconds": workloads["index_warm_build"]["elapsed_seconds"],
        "cold_summary": workloads["index_cold_build"]["details"].get("summary", {}),
        "warm_summary": workloads["index_warm_build"]["details"].get("summary", {}),
        "subprocess_seconds": process.elapsed_seconds,
        "subprocess_output_bytes": process.output_bytes,
        "subprocess_passed": process.passed,
    }
