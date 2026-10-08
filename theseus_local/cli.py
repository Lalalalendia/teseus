"""Minimal headless Theseus CLI for local project and campaign control."""
from __future__ import annotations
import argparse
import hashlib
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    MutationScope,
    ProjectDescriptor,
    ProjectId,
    TestCommandDescriptor,
)
from theseus_contracts.serialization import loads_object
from gallifrey_mutation import MutationCampaignService, SQLiteMutationStore, Success
from test_intelligence_unified_v1 import __version__
from theseus_performance.project_benchmark import ProjectBenchmarkRequest, run_project_benchmark
from .coordinator import LocalCampaignCoordinator
from .runtime_identity import current_runtime_identity
from .ai_analysis import DeterministicAnalysisProvider, analyze_report, write_advisory_analysis
from .ai_mutation import MutationCandidate, prepare_candidate
from .operations import list_campaigns
def _read_configuration(path: Path) -> CampaignConfiguration:
    # Load a JSON campaign contract while keeping CLI parsing separate from domain state.
    return CampaignConfiguration.from_dict(loads_object(path.read_text(encoding="utf-8")))
def _positive_int(value: str) -> int:
    # Parse one strictly positive integer CLI limit.
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed
def _positive_float(value: str) -> float:
    # Parse one strictly positive floating-point CLI limit.
    parsed = float(value)
    if parsed <= 0.0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return parsed
def _default_project_id(root: Path) -> str:
    # Derive one stable non-secret project identity from the resolved checkout location.
    digest = hashlib.sha256(str(root).encode("utf-8")).hexdigest()[:20]
    return f"project-{digest}"
def _default_campaign_id() -> str:
    # Create one collision-resistant local campaign identity suitable for filesystem paths.
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"campaign-{timestamp}-{uuid4().hex[:8]}"
def _direct_test_command(args: argparse.Namespace, root: Path) -> TestCommandDescriptor:
    # Build one shell-free test command and require custom argv to be the final CLI option.
    raw = tuple(str(item) for item in (args.test_command or ()))
    if raw[:1] == ("--",):
        raw = raw[1:]
    argv = raw or (sys.executable, "-m", "pytest", "-q")
    if not argv:
        raise ValueError("test command must not be empty")
    return TestCommandDescriptor(argv=argv, cwd=str(root), shell=False)
def _direct_source(root: Path, value: str) -> str:
    # Resolve one Python source under the selected project without permitting traversal.
    raw = Path(value).expanduser()
    target = raw.resolve() if raw.is_absolute() else (root / raw).resolve()
    if not target.is_relative_to(root):
        raise ValueError("source path must remain inside the project root")
    if not target.is_file():
        raise ValueError(f"source file does not exist: {target}")
    if target.suffix.lower() != ".py":
        raise ValueError("source file must be a Python .py file")
    return target.relative_to(root).as_posix()
def _direct_configuration(args: argparse.Namespace) -> CampaignConfiguration:
    # Convert direct CLI arguments into the existing public campaign contract.
    root = Path(args.project_root).expanduser().resolve()
    if not root.is_dir():
        raise ValueError(f"project root is not a directory: {root}")
    source = _direct_source(root, args.source)
    function = str(args.function).strip() if args.function else None
    operators = tuple(dict.fromkeys(str(item).strip() for item in args.operator if str(item).strip()))
    reports_dir = str(Path(args.reports_dir).expanduser().resolve()) if args.reports_dir else None
    return CampaignConfiguration(
        campaign_id=CampaignId(args.campaign_id or _default_campaign_id()),
        project=ProjectDescriptor(
            project_id=ProjectId(args.project_id or _default_project_id(root)),
            display_name=args.name or root.name or "Theseus project",
            root_path=str(root),
            test_command=_direct_test_command(args, root),
        ),
        scope=MutationScope(
            source_path=source,
            function=function,
            operators=operators,
        ),
        budget=CampaignBudget(
            max_mutants=args.max_mutants,
            max_workers=args.workers,
            max_test_seconds=args.test_timeout,
            lease_seconds=args.lease_seconds,
        ),
        no_escalation=bool(args.no_escalation),
        reports_dir=reports_dir,
    )
def _normalized_counts(value: Mapping[str, Any]) -> dict[str, int]:
    # Normalize public result counters without changing the canonical report artifact.
    return {str(key).strip().lower(): max(0, int(item)) for key, item in value.items()}
def _count_status(counts: Mapping[str, int], *names: str) -> int:
    # Sum equivalent terminal labels used by older and current engine reports.
    return sum(max(0, int(counts.get(name, 0))) for name in names)
def _direct_payload(
    configuration: CampaignConfiguration,
    result: Any,
    *,
    elapsed_seconds: float,
) -> dict[str, Any]:
    # Build one stable CLI result projection around the authoritative canonical report.
    summary = result.engine_result.summary if result.engine_result is not None else None
    counts = _normalized_counts(summary.counts if summary is not None else {})
    killed = _count_status(counts, "killed", "kill")
    survived = _count_status(counts, "survived", "survive")
    invalid = _count_status(counts, "invalid", "invalid_mutant")
    timeouts = _count_status(counts, "timeout", "timed_out")
    cancelled = _count_status(counts, "cancelled", "canceled", "cancel_requested")
    infrastructure_failures = _count_status(
        counts,
        "infrastructure_error",
        "infrastructure_failed",
        "error",
        "failed",
    )
    score_denominator = killed + survived
    return {
        "schema_version": 1,
        "campaign_id": configuration.campaign_id.value,
        "project_id": configuration.project.project_id.value,
        "source_path": configuration.scope.source_path,
        "function": configuration.scope.function,
        "status": getattr(result.campaign.status, "value", result.campaign.status),
        "succeeded": bool(result.succeeded),
        "mutants_discovered": int(summary.total_mutants if summary is not None else result.campaign.total_mutants),
        "mutants_executed": int(summary.completed_mutants if summary is not None else result.campaign.completed_mutants),
        "killed": killed,
        "survived": survived,
        "invalid": invalid,
        "timeouts": timeouts,
        "cancelled": cancelled,
        "infrastructure_failures": infrastructure_failures,
        "mutation_score": round(killed / score_denominator, 6) if score_denominator else None,
        "elapsed_seconds": round(max(0.0, float(elapsed_seconds)), 6),
        "report_path": (
            summary.report_path
            if summary is not None and summary.report_path
            else str(result.database_path.parent / "canonical.report.json")
            if (result.database_path.parent / "canonical.report.json").is_file()
            else None
        ),
        "database_path": str(result.database_path),
        "events_path": str(result.events_path),
        "protocol_path": str(result.protocol_path),
        "error": result.error,
    }
def _print_direct_summary(payload: Mapping[str, Any]) -> None:
    # Print one compact human-readable summary while leaving the JSON report authoritative.
    score = payload.get("mutation_score")
    score_text = "n/a" if score is None else f"{float(score) * 100.0:.2f}%"
    rows = (
        ("Campaign", payload.get("campaign_id")),
        ("Status", payload.get("status")),
        ("Source", payload.get("source_path")),
        ("Function", payload.get("function") or "<file scope>"),
        ("Mutants discovered", payload.get("mutants_discovered")),
        ("Mutants executed", payload.get("mutants_executed")),
        ("Killed", payload.get("killed")),
        ("Survived", payload.get("survived")),
        ("Invalid", payload.get("invalid")),
        ("Timeouts", payload.get("timeouts")),
        ("Cancelled", payload.get("cancelled")),
        ("Infrastructure failures", payload.get("infrastructure_failures")),
        ("Mutation score", score_text),
        ("Elapsed", f"{float(payload.get('elapsed_seconds', 0.0)):.3f}s"),
        ("Report", payload.get("report_path") or "not materialized"),
    )
    for label, value in rows:
        print(f"{label}: {value}")
    if payload.get("error"):
        print(f"Error: {payload['error']}")
def _print_cli_error(
    message: str,
    *,
    as_json: bool,
    error_code: str,
    context: Mapping[str, Any] | None = None,
) -> None:
    # Print one stable machine-readable or human-readable command failure.
    payload: dict[str, Any] = {
        "schema_version": 1,
        "succeeded": False,
        "error_code": str(error_code),
        "error": str(message),
    }
    payload.update(dict(context or {}))
    if as_json:
        print(json.dumps(payload, ensure_ascii=False))
        return
    print(f"Error [{error_code}]: {message}")
    if payload.get("recovery_command"):
        print(f"Recovery: {payload['recovery_command']}")
def _direct_run(args: argparse.Namespace) -> int:
    # Run one mutation campaign directly from user arguments without an intermediate JSON file.
    configuration: CampaignConfiguration | None = None
    started = time.perf_counter()
    try:
        configuration = _direct_configuration(args)
        result = LocalCampaignCoordinator().run(configuration)
        payload = _direct_payload(configuration, result, elapsed_seconds=time.perf_counter() - started)
    except KeyboardInterrupt:
        context: dict[str, Any] = {}
        if configuration is not None:
            database_path = LocalCampaignCoordinator.campaign_database_path(configuration)
            context = {
                "campaign_id": configuration.campaign_id.value,
                "database_path": str(database_path),
                "recovery_command": (
                    f'{sys.executable} -m theseus_local campaign recover "{database_path}" '
                    f"--campaign-id {configuration.campaign_id.value}"
                ),
            }
        _print_cli_error(
            "campaign interrupted; durable state can be recovered",
            as_json=bool(args.json),
            error_code="campaign_interrupted",
            context=context,
        )
        return 130
    except (OSError, RuntimeError, ValueError) as exc:
        _print_cli_error(
            str(exc),
            as_json=bool(args.json),
            error_code="campaign_start_failed",
        )
        return 2
    if args.json:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        _print_direct_summary(payload)
    return 0 if result.succeeded else 1
def _benchmark_output_root(root: Path, value: str | None) -> Path:
    # Resolve benchmark state outside the checkout so measurement never changes project inputs.
    if value:
        return Path(value).expanduser().resolve()
    return (root.parent / ".theseus-benchmarks" / _default_project_id(root)).resolve()
def _print_benchmark_summary(report: Mapping[str, Any]) -> None:
    # Print the measured matrix and report location without imposing host-specific pass thresholds.
    print(f"Benchmark: {report.get('session_id')}")
    print(f"Completed: {report.get('completed')}")
    for item in report.get("scenarios", []):
        if not isinstance(item, Mapping):
            continue
        metrics = item.get("metrics", {})
        wall = metrics.get("wall_seconds", {}) if isinstance(metrics, Mapping) else {}
        seconds = wall.get("value") if isinstance(wall, Mapping) else None
        dominant = item.get("bottleneck_summary", {})
        dominant_phase = dominant.get("dominant_observed_phase") if isinstance(dominant, Mapping) else None
        print(
            f"{item.get('mode')} mutants={item.get('max_mutants')} workers={item.get('workers')}: "
            f"status={item.get('status')} wall={seconds}s dominant={dominant_phase or 'unmeasured'}"
        )
    comparisons = report.get("comparisons", {})
    if isinstance(comparisons, Mapping):
        for item in comparisons.get("cold_to_warm", []):
            if isinstance(item, Mapping):
                print(
                    f"Warm speedup mutants={item.get('max_mutants')} workers={item.get('workers')}: "
                    f"{item.get('warm_speedup')}"
                )
        for item in comparisons.get("worker_scaling", []):
            if isinstance(item, Mapping):
                print(
                    f"Worker speedup {item.get('mode')} mutants={item.get('max_mutants')} "
                    f"{item.get('baseline_workers')}->{item.get('workers')}: "
                    f"speedup={item.get('speedup')} efficiency={item.get('parallel_efficiency')}"
                )
        for item in comparisons.get("campaign_scaling", []):
            if isinstance(item, Mapping):
                print(
                    f"Campaign scale {item.get('mode')} workers={item.get('workers')} "
                    f"{item.get('baseline_max_mutants')}->{item.get('max_mutants')}: "
                    f"wall_growth={item.get('wall_growth')} throughput_ratio={item.get('throughput_ratio')}"
                )
    print(f"Report: {report.get('report_path')}")
    print(f"History: {report.get('history_database')}")
def _project_benchmark(args: argparse.Namespace) -> int:
    # Measure fresh cold/warm local campaigns through the existing coordinator and durable authority store.
    root = Path(args.project_root).expanduser().resolve()
    try:
        source = _direct_source(root, args.source)
        command = _direct_test_command(args, root).argv
        request = ProjectBenchmarkRequest(
            project_root=root,
            source_path=source,
            function=str(args.function).strip() if args.function else None,
            test_command=command,
            output_root=_benchmark_output_root(root, args.output_dir),
            operators=tuple(args.operator),
            max_mutants=args.max_mutants,
            mutant_counts=tuple(args.mutant_count or ()),
            worker_counts=tuple(args.worker_count or (1, 2)),
            test_timeout_seconds=args.test_timeout,
            lease_seconds=args.lease_seconds,
            no_escalation=bool(args.no_escalation),
            project_id=args.project_id or _default_project_id(root),
            display_name=args.name or root.name or "Theseus project",
            pin_baselines=bool(args.pin_baselines),
            repetitions=args.repetitions,
        )
        report = run_project_benchmark(request)
    except KeyboardInterrupt:
        _print_cli_error(
            "project benchmark interrupted; completed scenario records remain durable",
            as_json=bool(args.json),
            error_code="benchmark_interrupted",
        )
        return 130
    except (OSError, RuntimeError, ValueError) as exc:
        _print_cli_error(
            str(exc),
            as_json=bool(args.json),
            error_code="benchmark_failed",
        )
        return 2
    if args.json:
        print(json.dumps(report, ensure_ascii=False))
    else:
        _print_benchmark_summary(report)
    return 0 if bool(report.get("completed")) else 1
def _project_add(args: argparse.Namespace) -> int:
    # Materialize one project descriptor for later campaign configuration creation.
    descriptor = ProjectDescriptor(
        project_id=ProjectId(args.project_id),
        display_name=args.name,
        root_path=str(Path(args.root).expanduser().resolve()),
    )
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(descriptor.to_dict(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(descriptor.to_dict(), ensure_ascii=False))
    return 0
def _campaign_create(args: argparse.Namespace) -> int:
    # Validate and copy a public configuration into an explicit campaign registry path.
    configuration = _read_configuration(Path(args.configuration))
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(configuration.to_dict(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"campaign_id": configuration.campaign_id.value, "configuration": str(target)}, ensure_ascii=False))
    return 0
def _campaign_run(args: argparse.Namespace) -> int:
    # Run one configured campaign through the process-backed local coordinator.
    result = LocalCampaignCoordinator().run(_read_configuration(Path(args.configuration)))
    payload: dict[str, Any] = {
        "campaign": result.campaign.to_dict(),
        "succeeded": result.succeeded,
        "database_path": str(result.database_path),
        "events_path": str(result.events_path),
        "protocol_path": str(result.protocol_path),
        "report": result.engine_result.to_dict() if result.engine_result else None,
        "error": result.error,
    }
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if result.succeeded else 1
def _campaign_status(args: argparse.Namespace) -> int:
    # Read campaign state directly from the durable SQLite projection.
    store = SQLiteMutationStore(Path(args.database))
    try:
        result = store.get_campaign(CampaignId(args.campaign_id))
        if not isinstance(result, Success) or result.value is None:
            print(json.dumps({"error": getattr(result, "message", "campaign not found")}, ensure_ascii=False))
            return 1
        print(json.dumps(result.value.to_dict(), ensure_ascii=False))
        return 0
    finally:
        store.close()
def _campaign_cancel(args: argparse.Namespace) -> int:
    # Publish a durable cancellation effect using the current optimistic revision.
    store = SQLiteMutationStore(Path(args.database))
    try:
        service = MutationCampaignService(store)
        current = service.get_campaign(CampaignId(args.campaign_id))
        if not isinstance(current, Success):
            print(json.dumps({"error": getattr(current, "message", "campaign not found")}, ensure_ascii=False))
            return 1
        status = getattr(current.value.status, "value", current.value.status)
        if str(status) not in {"completed", "failed", "cancelled"}:
            LocalCampaignCoordinator.request_cancel(current.value.configuration)
        result = service.cancel(
            args.effect_id,
            current.value.campaign_id,
            expected_revision=current.value.revision_number,
        )
        print(json.dumps(result.value.to_dict() if isinstance(result, Success) else {"error": result.message}, ensure_ascii=False))
        return 0 if isinstance(result, Success) else 1
    finally:
        store.close()
def _campaign_recover(args: argparse.Namespace) -> int:
    # Reconcile and resume one exact durable campaign through the public coordinator authority.
    started = time.perf_counter()
    try:
        result = LocalCampaignCoordinator.recover_campaign(
            Path(args.database),
            str(args.campaign_id),
        )
        payload = _direct_payload(
            result.campaign.configuration,
            result,
            elapsed_seconds=time.perf_counter() - started,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        _print_cli_error(
            str(exc),
            as_json=bool(args.json),
            error_code="campaign_recovery_failed",
        )
        return 2
    if args.json:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        _print_direct_summary(payload)
    return 0 if result.succeeded else 1
def _campaign_report(args: argparse.Namespace) -> int:
    # Print an existing materialized JSON report without recomputing campaign state.
    sys.stdout.write(Path(args.report).read_text(encoding="utf-8"))
    return 0


def _runtime_info(args: argparse.Namespace) -> int:
    # Print the installed runtime identity used for coordinator/worker compatibility.
    payload = current_runtime_identity().to_dict()
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    else:
        print(f"Theseus: {payload['theseus_version']}")
        print(f"Python: {payload['python_version']} ({payload['python_implementation']})")
        print(f"Platform: {payload['platform']}/{payload['architecture']}")
        print(f"Runtime fingerprint: {payload['runtime_fingerprint']}")
        print(f"Protocols: {json.dumps(payload['protocol_versions'], sort_keys=True)}")
        print(f"Schemas: {json.dumps(payload['schema_versions'], sort_keys=True)}")
    return 0


def _analysis_report(args: argparse.Namespace) -> int:
    # Create an advisory projection without modifying the canonical report.
    raw = json.loads(Path(args.report).read_text(encoding="utf-8"))
    provider = DeterministicAnalysisProvider() if args.provider == "deterministic" else None
    analysis = analyze_report(raw, provider=provider)
    if args.output:
        write_advisory_analysis(Path(args.output), analysis)
    payload = analysis.to_dict()
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True) if args.json else json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def _operations_list(args: argparse.Namespace) -> int:
    # List operational views derived from canonical report projections.
    payload = [
        item.to_dict()
        for item in list_campaigns(
            Path(args.report_root),
            offset=args.offset,
            limit=args.limit,
        )
    ]
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True) if args.json else json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def _validate_mutation(args: argparse.Namespace) -> int:
    # Validate one advisory candidate through the ordinary immutable preparation boundary.
    value = json.loads(Path(args.candidate).read_text(encoding="utf-8"))
    candidate = MutationCandidate(
        source_path=str(value["source_path"]),
        line_no=int(value["line_no"]),
        column_no=int(value.get("column_no", 0)),
        original=str(value["original"]),
        replacement=str(value["replacement"]),
        operator_version=str(value.get("operator_version", "ai-v1")),
        rationale=str(value.get("rationale", "")),
    )
    prepared = prepare_candidate(candidate, Path(args.project_root))
    payload = prepared.to_dict()
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True) if args.json else json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def _network_worker_start(args: argparse.Namespace) -> int:
    # Start one enrolled worker host while keeping execution semantics in RemoteWorkerRuntime.
    from .network_control import NetworkWorkerAgent

    agent = NetworkWorkerAgent(
        host=args.host,
        port=args.port,
        shared_secret=args.secret,
        worker_id=args.worker_id,
        root=Path(args.root),
        slots=args.slots,
        heartbeat_seconds=args.heartbeat,
    )
    if args.json:
        print(json.dumps({"worker_id": args.worker_id, "host": args.host, "port": args.port}, sort_keys=True), flush=True)
    try:
        agent.run()
    except KeyboardInterrupt:
        return 0
    return 0


def _network_workers(args: argparse.Namespace) -> int:
    # Show the durable enrollment projection; live session state remains coordinator-owned.
    from .network_security import EnrollmentAuthority

    authority = EnrollmentAuthority(Path(args.registry), args.secret)
    payload = list(authority.workers())
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True) if args.json else json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def _network_run(args: argparse.Namespace) -> int:
    # Run already-bound RemoteExecutionRequest JSON through the real socket control plane.
    from .network_control import NetworkCoordinator
    from .network_security import EnrollmentAuthority
    from .artifact_store import ContentAddressedArtifactStore

    raw = json.loads(Path(args.requests).read_text(encoding="utf-8"))
    rows = raw.get("requests") if isinstance(raw, dict) else raw
    if not isinstance(rows, list):
        raise ValueError("network request file must contain an array or a requests array")
    from theseus_contracts import RemoteExecutionRequest

    requests = tuple(RemoteExecutionRequest.from_dict(item) for item in rows if isinstance(item, dict))
    authority = EnrollmentAuthority(Path(args.enrollment), args.secret)
    coordinator = NetworkCoordinator(
        host=args.host,
        port=args.port,
        state_path=Path(args.state),
        artifact_store=ContentAddressedArtifactStore(Path(args.cache)),
        enrollment=authority,
        ha_state_path=Path(args.ha_state) if args.ha_state else None,
    )
    coordinator.start()
    try:
        workers = coordinator.wait_for_workers(args.worker_count, timeout_seconds=args.worker_wait)
        if len(workers) < args.worker_count:
            raise ValueError(f"expected {args.worker_count} enrolled workers, got {len(workers)}")
        report = coordinator.run(requests, timeout_seconds=args.timeout, campaign_id=args.campaign_id)
        payload = report.to_dict()
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True) if args.json else json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    finally:
        coordinator.stop()


# Project the durable network scheduler state without becoming campaign authority.
def _network_status(args: argparse.Namespace) -> int:
    # Print the durable scheduler projection without becoming campaign authority.
    from .distributed import DistributedScheduler

    payload = DistributedScheduler(Path(args.state)).snapshot()
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True) if args.json else json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


# Cancel one durable network evidence item and let late workers fence their result.
def _network_cancel(args: argparse.Namespace) -> int:
    # Cancel one durable evidence item; workers will fence any late result.
    from .distributed import DistributedScheduler

    scheduler = DistributedScheduler(Path(args.state))
    removed = scheduler.cancel(args.evidence, reason=args.reason)
    payload = {"evidence_identity": args.evidence, "cancelled": bool(removed), "reason": args.reason}
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True) if args.json else json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if removed else 1


# Route the public run command to either the local or real-socket execution path.
def _run_dispatch(args: argparse.Namespace) -> int:
    # Route the public run command to either the local or real-socket execution path.
    if bool(getattr(args, "remote", False)):
        return _network_run(args)
    return _direct_run(args)


def build_parser() -> argparse.ArgumentParser:
    # Build the small command tree without adding a third-party CLI dependency.
    parser = argparse.ArgumentParser(prog="theseus")
    parser.add_argument("--version", action="version", version=__version__)
    commands = parser.add_subparsers(dest="area", required=True)
    runtime = commands.add_parser("runtime", help="show installed runtime identity and compatibility versions")
    runtime.add_argument("--json", action="store_true")
    runtime.set_defaults(handler=_runtime_info)
    analysis = commands.add_parser("analysis", help="create optional advisory analysis over a canonical report")
    analysis_commands = analysis.add_subparsers(dest="command", required=True)
    analysis_report = analysis_commands.add_parser("report")
    analysis_report.add_argument("report")
    analysis_report.add_argument("--provider", choices=("unavailable", "deterministic"), default="unavailable")
    analysis_report.add_argument("--output")
    analysis_report.add_argument("--json", action="store_true")
    analysis_report.set_defaults(handler=_analysis_report)
    operations = commands.add_parser("operations", help="show projection-only campaign operations")
    operations_commands = operations.add_subparsers(dest="command", required=True)
    operations_list = operations_commands.add_parser("list")
    operations_list.add_argument("report_root")
    operations_list.add_argument("--offset", type=int, default=0)
    operations_list.add_argument("--limit", type=int)
    operations_list.add_argument("--json", action="store_true")
    operations_list.set_defaults(handler=_operations_list)
    mutation = commands.add_parser("mutation", help="validate advisory mutations through normal preparation")
    mutation_commands = mutation.add_subparsers(dest="command", required=True)
    mutation_validate = mutation_commands.add_parser("validate")
    mutation_validate.add_argument("project_root")
    mutation_validate.add_argument("candidate")
    mutation_validate.add_argument("--json", action="store_true")
    mutation_validate.set_defaults(handler=_validate_mutation)
    worker = commands.add_parser("worker", help="run a real network worker host")
    worker_commands = worker.add_subparsers(dest="command", required=True)
    worker_start = worker_commands.add_parser("start")
    worker_start.add_argument("--host", default="127.0.0.1")
    worker_start.add_argument("--port", type=int, required=True)
    worker_start.add_argument("--secret", required=True)
    worker_start.add_argument("--worker-id", required=True)
    worker_start.add_argument("--root", required=True)
    worker_start.add_argument("--slots", type=_positive_int, default=1)
    worker_start.add_argument("--heartbeat", type=_positive_float, default=0.5)
    worker_start.add_argument("--json", action="store_true")
    worker_start.set_defaults(handler=_network_worker_start)
    network = commands.add_parser("network", help="operate the real socket distributed control plane")
    network_commands = network.add_subparsers(dest="command", required=True)
    network_workers = network_commands.add_parser("workers", help="show durable worker enrollment records")
    network_workers.add_argument("--registry", required=True)
    network_workers.add_argument("--secret", required=True)
    network_workers.add_argument("--json", action="store_true")
    network_workers.set_defaults(handler=_network_workers)
    network_run = network_commands.add_parser("run", help="run bound remote requests through enrolled workers")
    network_run.add_argument("requests")
    network_run.add_argument("--host", default="127.0.0.1")
    network_run.add_argument("--port", type=int, default=8765)
    network_run.add_argument("--secret", required=True)
    network_run.add_argument("--enrollment", required=True)
    network_run.add_argument("--cache", required=True)
    network_run.add_argument("--state", required=True)
    network_run.add_argument("--ha-state")
    network_run.add_argument("--campaign-id")
    network_run.add_argument("--worker-count", type=_positive_int, default=1)
    network_run.add_argument("--worker-wait", type=_positive_float, default=30.0)
    network_run.add_argument("--timeout", type=_positive_float, default=120.0)
    network_run.add_argument("--json", action="store_true")
    network_run.set_defaults(handler=_network_run)
    network_status = network_commands.add_parser("status", help="show the durable distributed scheduler projection")
    network_status.add_argument("--state", required=True)
    network_status.add_argument("--json", action="store_true")
    network_status.set_defaults(handler=_network_status)
    network_cancel = network_commands.add_parser("cancel", help="fence one distributed evidence item")
    network_cancel.add_argument("--state", required=True)
    network_cancel.add_argument("--evidence", required=True)
    network_cancel.add_argument("--reason", default="cancelled")
    network_cancel.add_argument("--json", action="store_true")
    network_cancel.set_defaults(handler=_network_cancel)
    workers_alias = commands.add_parser("workers", help="show durable worker enrollment records")
    workers_alias.add_argument("--registry", required=True)
    workers_alias.add_argument("--secret", required=True)
    workers_alias.add_argument("--json", action="store_true")
    workers_alias.set_defaults(handler=_network_workers)
    direct = commands.add_parser("run", help="run a local mutation campaign directly")
    direct.add_argument("project_root", nargs="?")
    direct.add_argument("source", nargs="?")
    direct.add_argument("--remote", action="store_true", help="run bound requests through the network worker plane")
    direct.add_argument("--requests", help="JSON request file for --remote")
    direct.add_argument("--host", default="127.0.0.1")
    direct.add_argument("--port", type=int, default=8765)
    direct.add_argument("--secret")
    direct.add_argument("--enrollment")
    direct.add_argument("--cache")
    direct.add_argument("--state")
    direct.add_argument("--ha-state")
    direct.add_argument("--worker-count", type=_positive_int, default=1)
    direct.add_argument("--worker-wait", type=_positive_float, default=30.0)
    direct.add_argument("--timeout", type=_positive_float, default=120.0)
    direct.add_argument("--campaign-id")
    direct.add_argument("--function")
    direct.add_argument("--operator", action="append", default=[])
    direct.add_argument("--max-mutants", type=_positive_int)
    direct.add_argument("--workers", type=_positive_int, default=1)
    direct.add_argument("--test-timeout", type=_positive_float, default=120.0)
    direct.add_argument("--lease-seconds", type=_positive_float, default=60.0)
    direct.add_argument("--reports-dir")
    direct.add_argument("--project-id")
    direct.add_argument("--name")
    direct.add_argument("--no-escalation", action="store_true")
    direct.add_argument("--json", action="store_true")
    direct.add_argument(
        "--test-command",
        nargs=argparse.REMAINDER,
        help="shell-free test argv; this option must be last",
    )
    direct.set_defaults(handler=_run_dispatch)
    benchmark = commands.add_parser("benchmark", help="measure cold/warm real-project mutation campaigns")
    benchmark.add_argument("project_root")
    benchmark.add_argument("source")
    benchmark.add_argument("--function")
    benchmark.add_argument("--operator", action="append", default=[])
    benchmark.add_argument("--max-mutants", type=_positive_int, default=10)
    benchmark.add_argument(
        "--mutant-count",
        type=_positive_int,
        action="append",
        help="repeat to benchmark an explicit campaign-size matrix; overrides the single --max-mutants lane",
    )
    benchmark.add_argument("--worker-count", type=_positive_int, action="append")
    benchmark.add_argument("--test-timeout", type=_positive_float, default=120.0)
    benchmark.add_argument("--lease-seconds", type=_positive_float, default=60.0)
    benchmark.add_argument("--output-dir")
    benchmark.add_argument("--project-id")
    benchmark.add_argument("--name")
    benchmark.add_argument("--no-escalation", action="store_true")
    benchmark.add_argument("--repetitions", type=_positive_int, default=1, help="physical repetitions per benchmark lane")
    benchmark.add_argument("--pin-baselines", action="store_true")
    benchmark.add_argument("--json", action="store_true")
    benchmark.add_argument(
        "--test-command",
        nargs=argparse.REMAINDER,
        help="shell-free test argv; this option must be last",
    )
    benchmark.set_defaults(handler=_project_benchmark)
    project = commands.add_parser("project")
    project_commands = project.add_subparsers(dest="command", required=True)
    project_add = project_commands.add_parser("add")
    project_add.add_argument("root")
    project_add.add_argument("--project-id", required=True)
    project_add.add_argument("--name", required=True)
    project_add.add_argument("--output", required=True)
    project_add.set_defaults(handler=_project_add)
    campaign = commands.add_parser("campaign")
    campaign_commands = campaign.add_subparsers(dest="command", required=True)
    create = campaign_commands.add_parser("create")
    create.add_argument("configuration")
    create.add_argument("--output", required=True)
    create.set_defaults(handler=_campaign_create)
    run = campaign_commands.add_parser("run")
    run.add_argument("configuration")
    run.set_defaults(handler=_campaign_run)
    status = campaign_commands.add_parser("status")
    status.add_argument("database")
    status.add_argument("--campaign-id", required=True)
    status.set_defaults(handler=_campaign_status)
    cancel = campaign_commands.add_parser("cancel")
    cancel.add_argument("database")
    cancel.add_argument("--campaign-id", required=True)
    cancel.add_argument("--effect-id", default="cli-cancel")
    cancel.set_defaults(handler=_campaign_cancel)
    recover = campaign_commands.add_parser("recover")
    recover.add_argument("database")
    recover.add_argument("--campaign-id", required=True)
    recover.add_argument("--json", action="store_true")
    recover.set_defaults(handler=_campaign_recover)
    report = campaign_commands.add_parser("report")
    report.add_argument("report")
    report.set_defaults(handler=_campaign_report)
    return parser
def main(argv: list[str] | None = None) -> int:
    # Parse one command and return a conventional CLI exit code.
    args = build_parser().parse_args(argv)
    return int(args.handler(args))
if __name__ == "__main__":
    raise SystemExit(main())
