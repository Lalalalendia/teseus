"""Command-line entry point for the unified Codex test intelligence tool."""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Sequence

from . import __version__
from .commands import CommandError, normalize_argv, parse_argv_json, parse_command_text, run_argv
from .impact import migrate_impact_database
from .index import add_domain_levels, build_index, load_context_map, load_index, plan_selection
from .io_utils import atomic_write_json, atomic_write_text, iter_json_lines, read_json, iter_lines, utc_now_iso
from .maintenance import CI_LANE_NAMES, VALIDATION_PROFILE_NAMES, benchmark, ci_matrix, doctor, gc_reports, inspect_benchmark_history, record_benchmark_history, run_ci_lanes, run_production_validation
from .engine import RunnerMutationEngine
from .runner import MutationConfig, recover_from_manifest
from .test_stats import COMPARE_PHASES, HEALTH_PHASES, build_test_stats_env, compare_test_runs, ingest_test_stats, instrument_pytest_command, is_pytest_command, stats_db_path, summarize_test_runs, summarize_test_stats
from .trace import TraceConfig, merge_trace_files, run_trace
from .mutations import available_mutation_operators


_STATS_COLUMNS = (
    "nodeid",
    "executions",
    "passed",
    "failed",
    "skipped",
    "xfailed",
    "xpassed",
    "cancelled",
    "timeout",
    "unknown",
    "errors",
    "baseline_failures",
    "regression_failures",
    "standalone_failures",
    "health_executions",
    "health_failures",
    "mutant_attempts",
    "mutant_kills",
    "kill_rate",
    "health_failure_rate",
    "flaky_rate",
    "health_status",
    "total_duration_ms",
    "avg_duration_ms",
    "median_ms",
    "p95_ms",
    "last_seen",
)
_STATS_MARKDOWN_COLUMNS = (
    ("nodeid", "nodeid"),
    ("executions", "exec"),
    ("passed", "passed"),
    ("failed", "failed"),
    ("skipped", "skipped"),
    ("xfailed", "xfailed"),
    ("xpassed", "xpassed"),
    ("cancelled", "cancelled"),
    ("timeout", "timeout"),
    ("unknown", "unknown"),
    ("errors", "errors"),
    ("baseline_failures", "baseline"),
    ("regression_failures", "regression"),
    ("mutant_attempts", "mutants"),
    ("mutant_kills", "kills"),
    ("kill_rate", "kill rate"),
    ("flaky_rate", "flaky rate"),
    ("health_status", "health"),
    ("median_ms", "median ms"),
    ("p95_ms", "p95 ms"),
    ("last_seen", "last seen"),
)
_RUN_STATS_COLUMNS = (
    "run_id",
    "executions",
    "tests",
    "passed",
    "failed",
    "skipped",
    "xfailed",
    "xpassed",
    "cancelled",
    "timeout",
    "unknown",
    "errors",
    "failure_rate",
    "total_duration_ms",
    "avg_duration_ms",
    "mutant_attempts",
    "phases",
    "first_seen",
    "last_seen",
)
_RUN_STATS_MARKDOWN_COLUMNS = (
    ("run_id", "run id"),
    ("executions", "exec"),
    ("tests", "tests"),
    ("passed", "passed"),
    ("failed", "failed"),
    ("skipped", "skipped"),
    ("xfailed", "xfailed"),
    ("xpassed", "xpassed"),
    ("cancelled", "cancelled"),
    ("timeout", "timeout"),
    ("unknown", "unknown"),
    ("errors", "errors"),
    ("failure_rate", "failure rate"),
    ("total_duration_ms", "duration ms"),
    ("avg_duration_ms", "avg ms"),
    ("mutant_attempts", "mutants"),
    ("phases", "phases"),
    ("last_seen", "last seen"),
)
_COMPARE_STATS_COLUMNS = (
    "before_run_id",
    "after_run_id",
    "nodeid",
    "status",
    "before_executions",
    "after_executions",
    "delta_executions",
    "before_passed",
    "after_passed",
    "before_failed",
    "after_failed",
    "before_skipped",
    "after_skipped",
    "before_errors",
    "after_errors",
    "delta_failures",
    "before_failure_rate",
    "after_failure_rate",
    "delta_failure_rate",
    "before_avg_duration_ms",
    "after_avg_duration_ms",
    "delta_avg_duration_ms",
)
_COMPARE_STATS_MARKDOWN_COLUMNS = (
    ("nodeid", "nodeid"),
    ("status", "status"),
    ("before_executions", "before exec"),
    ("after_executions", "after exec"),
    ("delta_executions", "delta exec"),
    ("before_failed", "before failed"),
    ("after_failed", "after failed"),
    ("before_errors", "before errors"),
    ("after_errors", "after errors"),
    ("delta_failures", "delta failures"),
    ("before_failure_rate", "before rate"),
    ("after_failure_rate", "after rate"),
    ("delta_failure_rate", "delta rate"),
    ("delta_avg_duration_ms", "delta avg ms"),
)


def _path(value: str | None) -> Path | None:
    return Path(value).expanduser().resolve() if value else None


def _add_project_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("project_root", type=Path, help="project checkout used as subprocess cwd")


def _add_selection_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--source", required=False, help="source path relative to project root")
    parser.add_argument("--function", help="function or method name; combined with line and mutant filters")
    parser.add_argument("--index", dest="index_path", type=Path, help="prebuilt index.json")
    parser.add_argument("--context-map", type=Path, help="coverage/context map JSON")
    parser.add_argument("--impact-db", type=Path, help="normalized test-impact SQLite database")
    parser.add_argument("--selected-tests-file", type=Path, help="nodeids text file or selection snapshot JSON")
    parser.add_argument("--domain", help="pytest path for L2 escalation, e.g. tests/commerce")
    parser.add_argument(
        "--test-command-argv",
        help='JSON argv array for L1, e.g. ["python", "-m", "pytest", "tests/test_x.py", "-q"]',
    )
    parser.add_argument(
        "--test-command",
        help="safe shell-like L1 command string; it is tokenized, never passed through a shell",
    )
    parser.add_argument("--domain-command-argv", help="JSON argv array overriding the L2 command")
    parser.add_argument("--common-command-argv", help="JSON argv array overriding the L3 command")
    parser.add_argument("--python", dest="python_executable", help="explicit Python executable")
    parser.add_argument("--reports-dir", type=Path, help="directory for JSON, Markdown and output artifacts")
    parser.add_argument("--timeout", type=float, default=120.0, help="per-command timeout in seconds")
    parser.add_argument("--timeout-retry-factor", type=float, default=2.0)
    parser.add_argument("--max-mutants", type=int)
    parser.add_argument("--from-line", type=int)
    parser.add_argument("--to-line", type=int)
    parser.add_argument("--mutant-id", action="append", default=[], help="exact mutant ID; may be repeated")
    parser.add_argument("--mutant-ids-file", type=Path, help="text file with exact mutant IDs")
    parser.add_argument("--no-escalation", action="store_true", help="run only L1")
    parser.add_argument("--no-baseline-cache", action="store_true")
    parser.add_argument("--audit-percent", type=float, default=0.0, help="L3 audit percentage of L1 kills")
    parser.add_argument("--operators", help="comma-separated mutation operator allowlist")
    parser.add_argument("--disable-operator", action="append", default=[], help="disable one mutation operator; may be repeated")
    parser.add_argument("--rerun-survivors", type=Path, help="reuse a previous report and rerun survivor IDs")
    parser.add_argument("--workers", type=int, help="isolated mutation workers; default: 1")
    parser.add_argument("--compact-report", action="store_true", help="keep mutant rows in results JSONL instead of the final JSON")


def _build_parser() -> argparse.ArgumentParser:
    # Build the CLI contract, including the historical test statistics command.
    parser = argparse.ArgumentParser(
        prog="test-intelligence-unified",
        description="Fast, safe test selection, impact context and mutation testing for Codex.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    index_parser = subparsers.add_parser("index", help="build a compact AST/function/test index")
    _add_project_argument(index_parser)
    index_parser.add_argument("--out", type=Path, help="index output path")

    select_parser = subparsers.add_parser("select", help="freeze selected tests and their reasons")
    _add_project_argument(select_parser)
    select_parser.add_argument("--source", required=True)
    select_parser.add_argument("--function")
    select_parser.add_argument("--index", dest="index_path", type=Path)
    select_parser.add_argument("--context-map", type=Path)
    select_parser.add_argument("--impact-db", type=Path)
    select_parser.add_argument("--selected-tests-file", type=Path)
    select_parser.add_argument("--test-stats-db", type=Path)
    select_parser.add_argument("--domain")
    select_parser.add_argument("--no-escalation", action="store_true")
    select_parser.add_argument("--out", type=Path, required=True)

    for name in ("mutate", "run"):
        mutate_parser = subparsers.add_parser(name, help="run baseline, safe mutants and escalation reports")
        _add_project_argument(mutate_parser)
        _add_selection_arguments(mutate_parser)

    recover_parser = subparsers.add_parser("recover", help="restore a target from a recovery manifest")
    recover_parser.add_argument("--manifest", type=Path, required=True)
    recover_parser.add_argument("--force", action="store_true", help="override an unlisted current hash after inspection")

    resume_parser = subparsers.add_parser("resume", help="recover and resume an interrupted worker campaign")
    resume_parser.add_argument("--manifest", type=Path, required=True)
    resume_parser.add_argument("--force", action="store_true", help="resume only after verifying no runner is active")
    resume_parser.add_argument("--takeover", action="store_true", help="take over only after the recorded owner is proven dead")

    inspect_parser = subparsers.add_parser("inspect", help="print a compact report summary")
    inspect_parser.add_argument("report", type=Path)
    inspect_parser.add_argument("--json", action="store_true")

    trace_parser = subparsers.add_parser("trace", help="trace a selected command into compact line/function JSON")
    _add_project_argument(trace_parser)
    trace_parser.add_argument("--command-argv", help="JSON argv array to trace")
    trace_parser.add_argument("--command", dest="command_text", help="safe command string to trace")
    trace_parser.add_argument("--out", type=Path, required=True)
    trace_parser.add_argument("--timeout", type=float, default=120.0)
    trace_parser.add_argument("--keep-runtime", action="store_true")
    trace_parser.add_argument("--merge-workers", action="store_true", help="retain xdist worker traces and merge them")

    trace_merge_parser = subparsers.add_parser("trace-merge", help="merge line/function trace artifacts")
    trace_merge_parser.add_argument("traces", nargs="+", type=Path)
    trace_merge_parser.add_argument("--out", type=Path, required=True)
    trace_merge_parser.add_argument("--project-root", type=Path)

    test_parser = subparsers.add_parser("test", help="run one safe test command and save its result")
    _add_project_argument(test_parser)
    test_parser.add_argument("--command-argv", help="JSON argv array to run")
    test_parser.add_argument("--command", dest="command_text", help="safe command string to run")
    test_parser.add_argument("--out", type=Path, required=True)
    test_parser.add_argument("--timeout", type=float, default=120.0)

    doctor_parser = subparsers.add_parser("doctor", help="check runtime, cache, index and recovery health")
    _add_project_argument(doctor_parser)
    doctor_parser.add_argument("--reports-dir", type=Path)
    doctor_parser.add_argument("--index", dest="index_path", type=Path)
    doctor_parser.add_argument("--impact-db", type=Path)
    doctor_parser.add_argument("--deep", action="store_true", help="refresh the reports storage accounting manifest")

    gc_parser = subparsers.add_parser("gc", help="plan or remove completed old reports")
    _add_project_argument(gc_parser)
    gc_parser.add_argument("--reports-dir", type=Path)
    gc_parser.add_argument("--keep-runs", type=int, default=50)
    gc_parser.add_argument("--keep-days", type=int, default=30)
    gc_parser.add_argument("--dry-run", action="store_true", default=True)
    gc_parser.add_argument("--apply", action="store_true", help="apply the bounded cleanup plan")

    benchmark_parser = subparsers.add_parser("benchmark", help="run local performance workloads and probes")
    _add_project_argument(benchmark_parser)
    benchmark_parser.add_argument("--reports-dir", type=Path)
    benchmark_parser.add_argument("--record-history", action="store_true", help="append this run to benchmark history")
    benchmark_parser.add_argument("--history-file", type=Path, help="benchmark history JSONL path; implies --record-history")
    benchmark_parser.add_argument(
        "--fail-on-regression",
        action="store_true",
        help="return non-zero when the recorded history flags a workload regression",
    )

    history_parser = subparsers.add_parser("benchmark-history", help="inspect benchmark regression history")
    history_parser.add_argument("--history-file", type=Path, required=True)
    history_parser.add_argument("--limit", type=int, default=20)

    subparsers.add_parser("ci-matrix", help="print the deterministic CI lane contract")

    ci_parser = subparsers.add_parser("ci", help="run selected CI lanes with bounded JSON reports")
    _add_project_argument(ci_parser)
    ci_parser.add_argument("--lane", action="append", choices=CI_LANE_NAMES, help="lane to run; may be repeated")
    ci_parser.add_argument("--all-lanes", action="store_true", help="run every declared lane")
    ci_parser.add_argument("--reports-dir", type=Path)
    ci_parser.add_argument("--python", dest="python_executable")
    ci_parser.add_argument("--timeout", type=float, default=900.0)
    ci_parser.add_argument("--fail-fast", action="store_true")
    ci_parser.add_argument("--require-coverage", action="store_true", help="fail when optional coverage is unavailable")
    ci_parser.add_argument("--result-file", type=Path, help="atomic JSON summary path; defaults under reports/ci")

    validation_parser = subparsers.add_parser("validate", help="run deterministic production-scale stats validation")
    _add_project_argument(validation_parser)
    validation_parser.add_argument("--profile", choices=VALIDATION_PROFILE_NAMES, default="smoke")
    validation_parser.add_argument("--reports-dir", type=Path)
    validation_parser.add_argument("--result-file", type=Path)
    validation_parser.add_argument("--events", type=int)
    validation_parser.add_argument("--nodeids", type=int)
    validation_parser.add_argument("--runs", type=int)
    validation_parser.add_argument("--workers", type=int)

    stats_parser = subparsers.add_parser("stats", help="show historical per-test execution statistics")
    _add_project_argument(stats_parser)
    stats_parser.add_argument("--reports-dir", type=Path)
    stats_parser.add_argument("--nodeid")
    stats_parser.add_argument("--limit", type=int, default=50)
    stats_parser.add_argument(
        "--order-by",
        choices=("kill_rate", "duration", "executions", "failure_rate", "flaky_rate", "health", "nodeid"),
        default="kill_rate",
    )
    stats_parser.add_argument("--json", action="store_true")
    stats_parser.add_argument("--format", choices=("text", "json", "csv", "markdown"))
    stats_parser.add_argument("--out", type=Path, help="write the rendered statistics atomically to this file")
    stats_parser.add_argument("--runs", action="store_true", help="show one row per historical run_id")
    stats_parser.add_argument(
        "--compare",
        nargs=2,
        metavar=("BEFORE_RUN", "AFTER_RUN"),
        help="compare two run_ids and classify per-test changes",
    )
    stats_parser.add_argument(
        "--compare-phase",
        action="append",
        choices=COMPARE_PHASES,
        help="phase included by --compare; repeat to add phases (default: baseline and standalone)",
    )
    stats_parser.add_argument(
        "--fail-on-regression",
        action="store_true",
        help="return non-zero for regressed or newly failing tests in --compare mode",
    )

    impact_parser = subparsers.add_parser("impact-migrate", help="create or migrate an indexed SQLite impact database")
    impact_parser.add_argument("--source", type=Path, required=True)
    impact_parser.add_argument("--out", type=Path, required=True)
    return parser


def _parse_command(value: str | None, json_value: str | None, label: str, tail: Sequence[str] = ()) -> tuple[str, ...] | None:
    if tail and (value or json_value):
        raise CommandError(f"use either --{label}-argv/--{label} or the '--' argv delimiter")
    if tail:
        return normalize_argv(tail)
    if value and json_value:
        raise CommandError(f"use either --{label}-argv or --{label}, not both")
    if json_value:
        return parse_argv_json(json_value)
    if value:
        return parse_command_text(value)
    return None


def _read_mutant_ids(args: argparse.Namespace) -> frozenset[str]:
    values = {str(item) for item in args.mutant_id}
    if args.mutant_ids_file:
        values.update(iter_lines(args.mutant_ids_file.resolve()))
    return frozenset(values)


def _default_reports_dir() -> Path:
    return (Path(__file__).resolve().parent / "reports").resolve()


def _default_index_path() -> Path:
    return _default_reports_dir() / "index.sqlite"


def _load_rerun(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any], frozenset[str]]:
    # Load survivor candidates from the compact report journal when results are not embedded.
    previous = read_json(args.rerun_survivors.resolve())
    target = previous.get("target", {})
    config = previous.get("config", {})
    previous_results = previous.get("results", [])
    if not isinstance(previous_results, list) or not previous_results:
        journal_value = previous.get("results_journal")
        journal_path = Path(str(journal_value)) if journal_value else None
        if journal_path and not journal_path.is_absolute():
            journal_path = args.rerun_survivors.resolve().parent / journal_path
        if journal_path and journal_path.exists():
            previous_results = list(iter_json_lines(journal_path))
    ids = {
        str(item.get("mutant", {}).get("mutant_id"))
        for item in previous_results
        if isinstance(item, dict)
        if item.get("status") in {"survived", "selection_escape", "mutant_induced_timeout", "infra_timeout"}
    }
    return previous, {"target": target, "config": config}, frozenset(ids)


def _mutate(args: argparse.Namespace) -> int:
    # Resolve operator allowlists and inherited campaign settings.
    root = args.project_root.resolve()
    previous: dict[str, Any] | None = None
    previous_info: dict[str, Any] = {"target": {}, "config": {}}
    rerun_ids = frozenset()
    if args.rerun_survivors:
        previous, previous_info, rerun_ids = _load_rerun(args)
    target = previous_info["target"]
    previous_config = previous_info["config"]
    source = args.source or target.get("source_path")
    if not source:
        raise CommandError("--source is required unless --rerun-survivors points to a report")
    function = args.function if args.function is not None else target.get("function")

    def inherited_path(option: Path | None, key: str) -> Path | None:
        return option.resolve() if option else (_path(previous_config.get(key)) if previous_config.get(key) else None)

    def inherited_argv(value: str | None, key: str, text_key: str | None = None) -> tuple[str, ...] | None:
        if value:
            return parse_argv_json(value)
        old = previous_config.get(key)
        if isinstance(old, list):
            return tuple(str(item) for item in old)
        old_text = previous_config.get(text_key) if text_key else None
        return parse_command_text(str(old_text)) if old_text else None

    test_argv = _parse_command(
        getattr(args, "test_command", None),
        getattr(args, "test_command_argv", None),
        "test-command",
        getattr(args, "test_command_tail", ()),
    )
    if test_argv is None:
        test_argv = inherited_argv(None, "test_command_argv")
    domain_argv = inherited_argv(args.domain_command_argv, "domain_command_argv")
    common_argv = inherited_argv(args.common_command_argv, "common_command_argv")
    context_map = inherited_path(args.context_map, "context_map")
    impact_db = inherited_path(args.impact_db, "impact_db")
    selected_tests = inherited_path(args.selected_tests_file, "selected_tests_file")
    index_path = args.index_path.resolve() if args.index_path else None
    reports_dir = args.reports_dir.resolve() if args.reports_dir else _default_reports_dir()
    ids = _read_mutant_ids(args) or rerun_ids
    requested_operators = args.operators or previous_config.get("operators")
    if isinstance(requested_operators, list):
        requested = {str(item) for item in requested_operators}
    elif requested_operators:
        requested = {item.strip() for item in str(requested_operators).split(",") if item.strip()}
    else:
        requested = None
    disabled_operators = {str(item) for item in args.disable_operator}
    catalog = available_mutation_operators()
    unknown_operators = (requested or set()) | disabled_operators
    unknown_operators -= set(catalog)
    if unknown_operators:
        raise CommandError(f"unknown mutation operator(s): {', '.join(sorted(unknown_operators))}")
    operators = None if requested is None and not disabled_operators else tuple(
        item for item in catalog if (requested is None or item in requested) and item not in disabled_operators
    )
    selection_snapshot = previous.get("selection_snapshot") if previous else None
    config = MutationConfig(
        project_root=root,
        source=str(source),
        function=function,
        index_path=index_path,
        context_map_path=context_map,
        impact_db=impact_db,
        selected_tests_file=selected_tests,
        test_command_argv=test_argv,
        domain=args.domain or previous_config.get("domain"),
        domain_command_argv=domain_argv,
        common_command_argv=common_argv,
        python_executable=args.python_executable or previous_config.get("python_executable"),
        reports_dir=reports_dir,
        timeout_seconds=args.timeout,
        timeout_retry_factor=args.timeout_retry_factor,
        max_mutants=args.max_mutants,
        from_line=args.from_line,
        to_line=args.to_line,
        mutant_ids=ids,
        no_escalation=args.no_escalation,
        use_baseline_cache=not args.no_baseline_cache,
        audit_percent=args.audit_percent,
        selection_snapshot=selection_snapshot,
        operators=operators,
        workers=max(1, int(args.workers if args.workers is not None else previous_config.get("workers", 1))),
        compact_report=bool(args.compact_report or previous_config.get("compact_report", False)),
    )
    report = asyncio.run(RunnerMutationEngine().run_config_async(config))
    print(json.dumps({"status": report.get("status"), "report": report.get("report_path"), "metrics": report.get("metrics", {} )}, ensure_ascii=False, indent=2))
    return 0 if report.get("status") in {"complete", "no_mutants"} else 1


def _select(args: argparse.Namespace) -> int:
    # Freeze validated selection while optionally using historical test metrics.
    root = args.project_root.resolve()
    index_path = args.index_path.resolve() if args.index_path else _default_index_path()
    index = load_index(index_path) if index_path.exists() else build_index(root, index_path)
    snapshot = plan_selection(
        root,
        index,
        args.source,
        args.function,
        context_map=load_context_map(args.context_map.resolve() if args.context_map else None),
        impact_db=args.impact_db.resolve() if args.impact_db else None,
        selected_tests_file=args.selected_tests_file.resolve() if args.selected_tests_file else None,
        test_stats_db=args.test_stats_db.resolve() if args.test_stats_db else None,
    )
    if not args.no_escalation:
        snapshot = add_domain_levels(snapshot, args.domain)
    atomic_write_json(args.out.resolve(), snapshot.to_dict())
    print(json.dumps({"snapshot": str(args.out.resolve()), "selected_tests": len(snapshot.selected_tests), "snapshot_id": snapshot.snapshot_id}, ensure_ascii=False, indent=2))
    return 0


def _inspect(args: argparse.Namespace) -> int:
    # Read the large result journal only for an explicit JSON inspection request.
    report = read_json(args.report.resolve())
    def related_path(key: str) -> Path | None:
        value = report.get(key)
        if not value:
            return None
        path = Path(str(value))
        return path if path.is_absolute() else args.report.resolve().parent / path

    journal_path = related_path("results_journal")
    state_path = related_path("state_path")
    journal_results = list(iter_json_lines(journal_path)) if args.json and journal_path and journal_path.exists() else []
    state: dict[str, Any] = {}
    if state_path and state_path.exists():
        value = read_json(state_path)
        if isinstance(value, dict):
            state = value
    if journal_results and not report.get("results"):
        report["results"] = journal_results
    if state:
        report["progress_state"] = state
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    metrics = report.get("metrics", {})
    if not metrics.get("counts") and state.get("counts"):
        metrics = dict(metrics)
        metrics["counts"] = state["counts"]
        metrics["total_mutants"] = state.get("completed_mutants", 0)
    print(f"status: {report.get('status')}")
    print(f"target: {report.get('target', {}).get('source_path')}::{report.get('target', {}).get('function_id')}")
    print(f"report: {args.report.resolve()}")
    print(f"mutants: {metrics.get('total_mutants', 0)}")
    print(f"mutation_score: {metrics.get('mutation_score')}")
    print(f"selection_escapes: {metrics.get('selection_escapes', 0)}")
    print(f"selection_precision: {metrics.get('selection_precision')}")
    print(f"counts: {json.dumps(metrics.get('counts', {}), ensure_ascii=False, sort_keys=True)}")
    if state:
        print(f"checkpoint: {state.get('completed_mutants', 0)}/{state.get('total_mutants', 0)}")
    if journal_path:
        print(f"results_journal: {journal_path}")
    return 0


def _trace(args: argparse.Namespace) -> int:
    command = _parse_command(
        args.command_text,
        args.command_argv,
        "command",
        getattr(args, "test_command_tail", ()),
    )
    if command is None:
        raise CommandError("trace requires --command-argv, --command, or the '--' argv delimiter")
    result = run_trace(
        TraceConfig(
            project_root=args.project_root.resolve(),
            command_argv=command,
            output=args.out.resolve(),
            timeout_seconds=args.timeout,
            keep_runtime=args.keep_runtime,
            merge_workers=args.merge_workers,
        )
    )
    print(json.dumps({"trace": str(args.out.resolve()), "exit_code": result["process"].get("exit_code"), "files": len(result["files"]), "functions": len(result["functions"])}, ensure_ascii=False, indent=2))
    return 0 if result["process"].get("passed") else 1


def _trace_merge(args: argparse.Namespace) -> int:
    # Merge explicit trace artifacts without executing project code.
    result = merge_trace_files(
        [path.resolve() for path in args.traces],
        args.out.resolve(),
        project_root=args.project_root.resolve() if args.project_root else None,
    )
    print(
        json.dumps(
            {
                "trace": str(args.out.resolve()),
                "inputs": result.get("input_count", 0),
                "files": len(result.get("files", {})),
                "functions": len(result.get("functions", {})),
                "invalid_inputs": len(result.get("invalid_inputs", [])),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if not result.get("invalid_inputs") else 1


def _resume(args: argparse.Namespace) -> int:
    # Resume only mutants absent from a verified serial or worker journal.
    from .workers import ResumeError, resume_campaign

    try:
        report = resume_campaign(args.manifest.resolve(), force=args.force, takeover=args.takeover)
    except ResumeError as exc:
        raise CommandError(str(exc)) from exc
    print(
        json.dumps(
            {
                "status": report.get("status"),
                "report": report.get("report_path"),
                "resumed_from": report.get("resumed_from"),
                "metrics": report.get("metrics", {}),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if report.get("status") in {"complete", "no_mutants"} else 1


def _render_stats_output(rows: Sequence[dict[str, Any]], output_format: str, reports_dir: Path) -> str:
    # Render one bounded stats snapshot for terminals, spreadsheets and JSON consumers.
    if output_format == "json":
        return json.dumps({"reports_dir": str(reports_dir), "tests": list(rows)}, ensure_ascii=False, indent=2) + "\n"
    if output_format == "csv":
        buffer = io.StringIO(newline="")
        writer = csv.DictWriter(buffer, fieldnames=_STATS_COLUMNS, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in _STATS_COLUMNS})
        return buffer.getvalue()
    if output_format == "markdown":
        headers = [label for _, label in _STATS_MARKDOWN_COLUMNS]
        lines = [
            "| " + " | ".join(headers) + " |",
            "| " + " | ".join("---" for _ in headers) + " |",
        ]
        for row in rows:
            values: list[str] = []
            for column, _ in _STATS_MARKDOWN_COLUMNS:
                value = row.get(column, "")
                if column in {"kill_rate", "flaky_rate"}:
                    value = f"{float(value or 0.0):.3f}"
                values.append(str(value).replace("|", "\\|").replace("\n", " "))
            lines.append("| " + " | ".join(values) + " |")
        return "\n".join(lines) + "\n"
    if output_format == "text":
        lines = [f"reports_dir: {reports_dir}", f"tests: {len(rows)}"]
        for row in rows:
            lines.append(
                f"{row['nodeid']} executions={row['executions']} failed={row['failed']} "
                f"passed={row['passed']} skipped={row['skipped']} xfailed={row.get('xfailed', 0)} "
                f"xpassed={row.get('xpassed', 0)} errors={row['errors']} "
                f"timeout={row.get('timeout', 0)} cancelled={row.get('cancelled', 0)} "
                f"unknown={row.get('unknown', 0)} "
                f"baseline_failures={row['baseline_failures']} "
                f"regression_failures={row['regression_failures']} health={row['health_status']} "
                f"flaky_rate={row['flaky_rate']:.3f} median_ms={row['median_ms']} "
                f"kill_rate={row['kill_rate']:.3f}"
            )
        return "\n".join(lines) + "\n"
    raise CommandError(f"unsupported stats format: {output_format}")


def _render_run_stats_output(rows: Sequence[dict[str, Any]], output_format: str, reports_dir: Path) -> str:
    # Render one row per historical run while preserving the stats export formats.
    if output_format == "json":
        return json.dumps({"reports_dir": str(reports_dir), "runs": list(rows)}, ensure_ascii=False, indent=2) + "\n"
    if output_format == "csv":
        buffer = io.StringIO(newline="")
        writer = csv.DictWriter(buffer, fieldnames=_RUN_STATS_COLUMNS, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            values = dict(row)
            values["phases"] = ",".join(str(item) for item in row.get("phases", ()))
            writer.writerow({column: values.get(column, "") for column in _RUN_STATS_COLUMNS})
        return buffer.getvalue()
    if output_format == "markdown":
        headers = [label for _, label in _RUN_STATS_MARKDOWN_COLUMNS]
        lines = [
            "| " + " | ".join(headers) + " |",
            "| " + " | ".join("---" for _ in headers) + " |",
        ]
        for row in rows:
            values: list[str] = []
            for column, _ in _RUN_STATS_MARKDOWN_COLUMNS:
                value = row.get(column, "")
                if column == "failure_rate":
                    value = f"{float(value or 0.0):.3f}"
                if column == "phases":
                    value = ",".join(str(item) for item in value or ())
                values.append(str(value).replace("|", "\\|").replace("\n", " "))
            lines.append("| " + " | ".join(values) + " |")
        return "\n".join(lines) + "\n"
    if output_format == "text":
        lines = [f"reports_dir: {reports_dir}", f"runs: {len(rows)}"]
        for row in rows:
            phases = ",".join(str(item) for item in row.get("phases", ()))
            lines.append(
                f"{row['run_id']} executions={row['executions']} tests={row['tests']} "
                f"passed={row['passed']} failed={row['failed']} skipped={row['skipped']} "
                f"xfailed={row.get('xfailed', 0)} xpassed={row.get('xpassed', 0)} "
                f"errors={row['errors']} timeout={row.get('timeout', 0)} "
                f"failure_rate={row['failure_rate']:.3f} "
                f"duration_ms={row['total_duration_ms']} phases={phases} last_seen={row['last_seen']}"
            )
        return "\n".join(lines) + "\n"
    raise CommandError(f"unsupported run stats format: {output_format}")


def _render_compare_stats_output(
    rows: Sequence[dict[str, Any]],
    output_format: str,
    reports_dir: Path,
    before_run_id: str,
    after_run_id: str,
    summary: dict[str, Any],
) -> str:
    # Render a bounded before/after diff with explicit regression statuses.
    if output_format == "json":
        return (
            json.dumps(
                {
                    "reports_dir": str(reports_dir),
                    "before_run_id": before_run_id,
                    "after_run_id": after_run_id,
                    "summary": summary,
                    "comparisons": list(rows),
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n"
        )
    if output_format == "csv":
        buffer = io.StringIO(newline="")
        writer = csv.DictWriter(buffer, fieldnames=_COMPARE_STATS_COLUMNS, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in _COMPARE_STATS_COLUMNS})
        return buffer.getvalue()
    if output_format == "markdown":
        headers = [label for _, label in _COMPARE_STATS_MARKDOWN_COLUMNS]
        lines = [
            f"comparison: {before_run_id} -> {after_run_id}",
            "summary: "
            + ", ".join(
                f"{key}={value}"
                for key, value in summary.items()
                if key not in {"gate_passed"}
            )
            + f", gate_passed={summary['gate_passed']}",
            "",
            "| " + " | ".join(headers) + " |",
            "| " + " | ".join("---" for _ in headers) + " |",
        ]
        for row in rows:
            values: list[str] = []
            for column, _ in _COMPARE_STATS_MARKDOWN_COLUMNS:
                value = row.get(column, "")
                if "rate" in column:
                    value = f"{float(value or 0.0):.3f}"
                values.append(str(value).replace("|", "\\|").replace("\n", " "))
            lines.append("| " + " | ".join(values) + " |")
        return "\n".join(lines) + "\n"
    if output_format == "text":
        phase_scope = ",".join(str(item) for item in summary.get("phases", ()))
        lines = [
            f"reports_dir: {reports_dir}",
            f"comparison: {before_run_id} -> {after_run_id}",
            f"phases: {phase_scope}",
            f"tests: {len(rows)}",
            f"gate_passed: {summary['gate_passed']}",
            f"blocking_changes: {summary['blocking_changes']}",
        ]
        for row in rows:
            lines.append(
                f"{row['nodeid']} status={row['status']} "
                f"before_failures={row['before_failed'] + row['before_errors']} "
                f"after_failures={row['after_failed'] + row['after_errors']} "
                f"delta_failures={row['delta_failures']} "
                f"delta_rate={row['delta_failure_rate']:.3f}"
            )
        return "\n".join(lines) + "\n"
    raise CommandError(f"unsupported compare stats format: {output_format}")


def _summarize_compare_rows(rows: Sequence[dict[str, Any]], phases: Sequence[str] = HEALTH_PHASES) -> dict[str, Any]:
    # Count comparison statuses and identify the rows that should block an opt-in gate.
    statuses = ("regressed", "new", "recovered", "removed", "changed", "stable")
    counts = {status: 0 for status in statuses}
    for row in rows:
        status = str(row.get("status", "changed"))
        counts[status] = counts.get(status, 0) + 1
    blocking = counts.get("regressed", 0) + counts.get("new", 0)
    return {
        "total": len(rows),
        **counts,
        "blocking_changes": blocking,
        "gate_passed": blocking == 0,
        "phases": list(phases),
    }


def _stats(args: argparse.Namespace) -> int:
    # Print historical counters plus baseline/regression health from the campaign database.
    reports_dir = (args.reports_dir or _default_reports_dir()).resolve()
    compare_phases = tuple(dict.fromkeys(args.compare_phase or HEALTH_PHASES))
    if args.compare and args.runs:
        raise CommandError("--compare cannot be combined with --runs")
    if args.fail_on_regression and not args.compare:
        raise CommandError("--fail-on-regression requires --compare")
    if args.compare and args.nodeid:
        raise CommandError("--nodeid cannot be combined with --compare")
    if args.compare_phase and not args.compare:
        raise CommandError("--compare-phase requires --compare")
    if args.fail_on_regression and "mutant" in compare_phases:
        raise CommandError("--fail-on-regression cannot include the mutant phase")
    if args.compare:
        rows = compare_test_runs(
            stats_db_path(reports_dir),
            args.compare[0],
            args.compare[1],
            project_root=args.project_root.resolve(),
            limit=args.limit,
            phases=compare_phases,
        )
    elif args.runs:
        if args.nodeid:
            raise CommandError("--nodeid cannot be combined with --runs")
        rows = summarize_test_runs(
            stats_db_path(reports_dir),
            project_root=args.project_root.resolve(),
            limit=args.limit,
        )
    else:
        rows = summarize_test_stats(
            stats_db_path(reports_dir),
            project_root=args.project_root.resolve(),
            nodeid=args.nodeid,
            limit=args.limit,
            order_by=args.order_by,
        )
    if args.json and args.format and args.format != "json":
        raise CommandError("--json cannot be combined with a non-json --format")
    output_format = args.format or ("json" if args.json else "text")
    compare_summary = _summarize_compare_rows(rows, compare_phases) if args.compare else {}
    rendered = (
        _render_compare_stats_output(
            rows,
            output_format,
            reports_dir,
            args.compare[0],
            args.compare[1],
            compare_summary,
        )
        if args.compare
        else _render_run_stats_output(rows, output_format, reports_dir)
        if args.runs
        else _render_stats_output(rows, output_format, reports_dir)
    )
    if args.out:
        output_path = args.out.resolve()
        atomic_write_text(output_path, rendered, durability="normal", category="report")
        print(
            json.dumps(
                {
                    "result": str(output_path),
                    "format": output_format,
                    "comparisons" if args.compare else "runs" if args.runs else "tests": len(rows),
                    **({"summary": compare_summary} if args.compare else {}),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0 if not args.fail_on_regression or compare_summary["gate_passed"] else 1
    print(rendered, end="")
    return 0 if not args.fail_on_regression or compare_summary["gate_passed"] else 1


def _test(args: argparse.Namespace) -> int:
    # Run a standalone command and collect per-test statistics when it is pytest.
    command = _parse_command(
        args.command_text,
        args.command_argv,
        "command",
        getattr(args, "test_command_tail", ()),
    )
    if command is None:
        raise CommandError("test requires --command-argv, --command, or the '--' argv delimiter")
    run_id = args.out.resolve().stem
    event_dir = args.out.resolve().parent / "test_stats_events" / run_id
    pytest_command = is_pytest_command(command)
    process = run_argv(
        instrument_pytest_command(command),
        cwd=args.project_root.resolve(),
        timeout_seconds=args.timeout,
        env=(
            build_test_stats_env(
                event_dir,
                run_id=run_id,
                phase="standalone",
                level="test",
                mutant_id=None,
                source_path="",
                target_sha256=None,
            )
            if pytest_command
            else None
        ),
    )
    stats = (
        ingest_test_stats(
            event_dir,
            stats_db_path(args.out.resolve().parent),
            project_root=args.project_root.resolve(),
            run_id=run_id,
            source_path="",
        )
        if pytest_command
        else {"events_ingested": 0, "invalid_events": 0, "event_files": 0}
    )
    result = {
        "schema_version": 1,
        "created_at": utc_now_iso(),
        "project_root": str(args.project_root.resolve()),
        "process": process.to_dict(),
        "test_stats": stats,
    }
    atomic_write_json(args.out.resolve(), result)
    print(json.dumps({"result": str(args.out.resolve()), "passed": process.passed, "exit_code": process.exit_code}, ensure_ascii=False, indent=2))
    return 0 if process.passed else 1


def main(argv: Sequence[str] | None = None) -> int:
    # Dispatch one safe CLI command and normalize user-facing failures.
    parser = _build_parser()
    raw_args = list(sys.argv[1:] if argv is None else argv)
    command_tail: list[str] = []
    if "--" in raw_args:
        delimiter = raw_args.index("--")
        command_tail = raw_args[delimiter + 1 :]
        raw_args = raw_args[:delimiter]
    args = parser.parse_args(raw_args)
    args.test_command_tail = command_tail
    try:
        if args.command == "index":
            root = args.project_root.resolve()
            out = args.out.resolve() if args.out else _default_index_path()
            index = build_index(root, out)
            print(json.dumps({"index": str(out), "summary": index["summary"]}, ensure_ascii=False, indent=2))
            return 0
        if args.command == "select":
            return _select(args)
        if args.command in {"mutate", "run"}:
            return _mutate(args)
        if args.command == "recover":
            restored = recover_from_manifest(args.manifest.resolve(), force=args.force)
            print(json.dumps({"status": "restored", "sha256": restored}, ensure_ascii=False, indent=2))
            return 0
        if args.command == "resume":
            return _resume(args)
        if args.command == "stats":
            return _stats(args)
        if args.command == "inspect":
            return _inspect(args)
        if args.command == "trace":
            return _trace(args)
        if args.command == "trace-merge":
            return _trace_merge(args)
        if args.command == "test":
            return _test(args)
        if args.command == "doctor":
            value = doctor(
                args.project_root.resolve(),
                (args.reports_dir or _default_reports_dir()).resolve(),
                args.index_path.resolve() if args.index_path else None,
                args.impact_db.resolve() if args.impact_db else None,
                deep=args.deep,
            )
            print(json.dumps(value, ensure_ascii=False, indent=2))
            return 0 if value["reports"]["writable"] and not value["stale_locks"] else 1
        if args.command == "gc":
            value = gc_reports(
                (args.reports_dir or _default_reports_dir()).resolve(),
                keep_runs=args.keep_runs,
                keep_days=args.keep_days,
                dry_run=not args.apply,
            )
            print(json.dumps(value, ensure_ascii=False, indent=2))
            return 0
        if args.command == "benchmark":
            reports_dir = (args.reports_dir or _default_reports_dir()).resolve()
            value = benchmark(args.project_root.resolve(), reports_dir)
            if args.record_history or args.history_file or args.fail_on_regression:
                history_path = (args.history_file or (reports_dir / "benchmark_history.jsonl")).resolve()
                value["history"] = record_benchmark_history(history_path, value)
            print(json.dumps(value, ensure_ascii=False, indent=2))
            e2e = value.get("workloads", {}).get("small_e2e_campaign", {}).get("details", {})
            regressions = (
                value.get("history", {})
                .get("comparison", {})
                .get("summary", {})
                .get("potential_regressions", 0)
            )
            return 0 if value["subprocess_passed"] and e2e.get("status") == "complete" and not (args.fail_on_regression and regressions) else 1
        if args.command == "benchmark-history":
            value = inspect_benchmark_history(args.history_file.resolve(), limit=args.limit)
            print(json.dumps(value, ensure_ascii=False, indent=2))
            return 0
        if args.command == "ci-matrix":
            print(json.dumps(ci_matrix(), ensure_ascii=False, indent=2))
            return 0
        if args.command == "validate":
            reports_dir = (args.reports_dir or _default_reports_dir()).resolve()
            value = run_production_validation(
                args.project_root.resolve(),
                reports_dir,
                profile=args.profile,
                event_count=args.events,
                nodeid_count=args.nodeids,
                run_count=args.runs,
                worker_count=args.workers,
                result_file=(args.result_file or (reports_dir / "validation" / f"{args.profile}.json")).resolve(),
            )
            print(json.dumps(value, ensure_ascii=False, indent=2))
            return 0 if value["passed"] else 1
        if args.command == "ci":
            if args.all_lanes and args.lane:
                raise CommandError("use either --all-lanes or repeated --lane")
            reports_dir = (args.reports_dir or _default_reports_dir()).resolve()
            lane_names = tuple(args.lane or (CI_LANE_NAMES if args.all_lanes else ("fast",)))
            value = run_ci_lanes(
                args.project_root.resolve(),
                lane_names,
                reports_dir,
                python_executable=args.python_executable,
                timeout_seconds=args.timeout,
                fail_fast=args.fail_fast,
                require_optional=args.require_coverage,
                result_file=(args.result_file or (reports_dir / "ci" / "result.json")).resolve(),
            )
            print(json.dumps(value, ensure_ascii=False, indent=2))
            return 0 if value["passed"] else 1
        if args.command == "impact-migrate":
            value = migrate_impact_database(args.source.resolve(), args.out.resolve())
            print(json.dumps(value, ensure_ascii=False, indent=2))
            return 0
    except (CommandError, OSError, ValueError, LookupError, RuntimeError, sqlite3.Error) as exc:
        parser.error(str(exc))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
