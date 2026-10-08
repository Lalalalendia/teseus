"""Real-project performance baseline orchestration for the local Theseus runtime."""
from __future__ import annotations
import hashlib
import json
import math
import os
import platform
import shutil
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping, Protocol, Sequence
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    MutationScope,
    ProjectDescriptor,
    ProjectId,
    TestCommandDescriptor,
)
from theseus_contracts.project_tree import project_tree_path_is_excluded
from .authority import (
    PERFORMANCE_MIN_REPETITIONS,
    PERFORMANCE_AGGREGATION_VERSION,
    STANDARD_METRICS,
    STANDARD_PHASES,
    PerformanceMetric,
    PerformanceRepetitionSummary,
    PerformanceRun,
    PerformanceStore,
    PhaseMeasurement,
    evaluate_phase_accounting,
    WorkloadIdentity,
    performance_run_from_runner_report,
    summarize_performance_runs,
)
PROJECT_BENCHMARK_SCHEMA_VERSION = 1
PROJECT_BENCHMARK_VERSION = "real-project-baseline-v3"
class CampaignExecutor(Protocol):
    """Minimal coordinator boundary consumed by the benchmark matrix."""
    def run(self, configuration: CampaignConfiguration) -> object:
        # Execute one campaign and return its public local result projection.
        ...
def _default_coordinator_factory() -> CampaignExecutor:
    # Load the local runtime only when the benchmark command actually executes a scenario.
    from theseus_local.coordinator import LocalCampaignCoordinator
    return LocalCampaignCoordinator()
@dataclass(frozen=True, slots=True)
class ProjectBenchmarkRequest:
    """Validated user inputs for one cold/warm worker-count benchmark session."""
    project_root: Path
    source_path: str
    test_command: tuple[str, ...]
    output_root: Path
    function: str | None = None
    operators: tuple[str, ...] = ()
    max_mutants: int | None = 10
    worker_counts: tuple[int, ...] = (1, 2)
    test_timeout_seconds: float = 120.0
    lease_seconds: float = 60.0
    no_escalation: bool = False
    project_id: str | None = None
    display_name: str | None = None
    pin_baselines: bool = False
    mutant_counts: tuple[int, ...] = ()
    repetitions: int = 1
    def __post_init__(self) -> None:
        # Normalize paths and reject benchmark state or source identities that escape their ownership roots.
        project_root = Path(self.project_root).expanduser().resolve()
        output_root = Path(self.output_root).expanduser().resolve()
        if not project_root.is_dir():
            raise ValueError(f"project root is not a directory: {project_root}")
        if output_root == project_root or project_root in output_root.parents:
            raise ValueError("benchmark output root must remain outside the project checkout")
        raw_source = Path(str(self.source_path).strip()).expanduser()
        source_file = raw_source.resolve() if raw_source.is_absolute() else (project_root / raw_source).resolve()
        if not str(self.source_path).strip() or not source_file.is_relative_to(project_root) or not source_file.is_file():
            raise ValueError("benchmark source must be an existing file inside the project root")
        if source_file.suffix.lower() != ".py":
            raise ValueError("benchmark source must be a Python .py file")
        command = tuple(str(item) for item in self.test_command if str(item))
        if not command:
            raise ValueError("benchmark test command must not be empty")
        workers = tuple(sorted(set(int(item) for item in self.worker_counts)))
        if not workers or any(item < 1 for item in workers):
            raise ValueError("benchmark worker counts must be positive integers")
        mutant_counts = tuple(sorted(set(int(item) for item in self.mutant_counts)))
        if any(item < 1 for item in mutant_counts):
            raise ValueError("benchmark mutant counts must be positive integers")
        if isinstance(self.repetitions, bool) or not isinstance(self.repetitions, int) or self.repetitions < 1:
            raise ValueError("benchmark repetitions must be a positive integer")
        if self.max_mutants is not None and int(self.max_mutants) < 1:
            raise ValueError("benchmark max_mutants must be positive when provided")
        if float(self.test_timeout_seconds) <= 0.0 or float(self.lease_seconds) <= 0.0:
            raise ValueError("benchmark timeouts must be greater than zero")
        object.__setattr__(self, "project_root", project_root)
        object.__setattr__(self, "output_root", output_root)
        object.__setattr__(self, "source_path", source_file.relative_to(project_root).as_posix())
        object.__setattr__(self, "test_command", command)
        object.__setattr__(self, "mutant_counts", mutant_counts)
        object.__setattr__(self, "repetitions", int(self.repetitions))
        object.__setattr__(self, "worker_counts", workers)
        object.__setattr__(self, "operators", tuple(dict.fromkeys(str(item) for item in self.operators if str(item))))
        object.__setattr__(self, "max_mutants", int(self.max_mutants) if self.max_mutants is not None else None)
        object.__setattr__(self, "test_timeout_seconds", float(self.test_timeout_seconds))
        object.__setattr__(self, "lease_seconds", float(self.lease_seconds))
@dataclass(frozen=True, slots=True)
class ProjectFingerprint:
    """Content identity and bounded scan statistics for one benchmark input tree."""
    sha256: str
    file_count: int
    byte_count: int
    scan_seconds: float
    def to_dict(self) -> dict[str, object]:
        # Serialize project input identity without exposing individual source contents.
        return {
            "sha256": self.sha256,
            "file_count": self.file_count,
            "byte_count": self.byte_count,
            "scan_seconds": self.scan_seconds,
        }
def _utc_now() -> str:
    # Return one timezone-aware timestamp for durable benchmark records.
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")
def _stable_hash(value: object) -> str:
    # Hash one JSON-compatible identity with deterministic key ordering.
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
def _hash_file(path: Path) -> tuple[str, int]:
    # Hash one project input through bounded reads while retaining its exact size.
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size
def fingerprint_project(root: Path) -> ProjectFingerprint:
    # Scan exactly the same project inputs that copy workspaces are allowed to materialize.
    started = time.perf_counter()
    resolved_root = Path(root).resolve()
    rows: list[tuple[str, str, int]] = []
    pending = [resolved_root]
    while pending:
        directory = pending.pop()
        try:
            entries = list(os.scandir(directory))
        except OSError:
            continue
        for entry in sorted(entries, key=lambda item: item.name.lower(), reverse=True):
            path = Path(entry.path)
            try:
                relative_path = path.relative_to(resolved_root)
            except ValueError:
                continue
            if project_tree_path_is_excluded(relative_path):
                continue
            if entry.is_dir(follow_symlinks=False):
                pending.append(path)
                continue
            if not entry.is_file(follow_symlinks=False):
                continue
            try:
                digest, size = _hash_file(path)
                relative = relative_path.as_posix()
            except OSError:
                continue
            rows.append((relative, digest, size))
    rows.sort()
    return ProjectFingerprint(
        sha256=_stable_hash(rows),
        file_count=len(rows),
        byte_count=sum(item[2] for item in rows),
        scan_seconds=max(0.0, time.perf_counter() - started),
    )
def _environment_fingerprint() -> str:
    # Fence comparisons by non-secret host and interpreter capabilities that materially affect timings.
    return _stable_hash(
        {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "python_implementation": platform.python_implementation(),
            "python_version": platform.python_version(),
            "cpu_count": os.cpu_count(),
        }
    )
def _runtime_fingerprint() -> str:
    # Identify the exact interpreter and benchmark implementation without absolute executable paths.
    return _stable_hash(
        {
            "benchmark_version": PROJECT_BENCHMARK_VERSION,
            "python_version": sys.version,
            "python_implementation": platform.python_implementation(),
        }
    )
def _project_key(request: ProjectBenchmarkRequest) -> str:
    # Derive a stable project identity while keeping the absolute checkout path out of comparison payloads.
    return request.project_id or f"project-{hashlib.sha256(str(request.project_root).encode('utf-8')).hexdigest()[:20]}"
def _scenario_mutant_counts(request: ProjectBenchmarkRequest) -> tuple[int | None, ...]:
    # Preserve the legacy single-size benchmark unless an explicit scale matrix was requested.
    return tuple(request.mutant_counts) if request.mutant_counts else (request.max_mutants,)
def _mutant_count_token(max_mutants: int | None) -> str:
    # Serialize one mutant limit into a stable filesystem and workload identity token.
    return "all" if max_mutants is None else str(int(max_mutants))
def _input_fingerprint(
    request: ProjectBenchmarkRequest,
    project: ProjectFingerprint,
    *,
    max_mutants: int | None,
) -> str:
    # Bind every campaign-affecting input except cold/warm mode and worker count into one stable identity.
    return _stable_hash(
        {
            "project_sha256": project.sha256,
            "source_path": request.source_path,
            "function": request.function,
            "operators": request.operators,
            "max_mutants": max_mutants,
            "test_command": request.test_command,
            "test_timeout_seconds": request.test_timeout_seconds,
            "lease_seconds": request.lease_seconds,
            "no_escalation": request.no_escalation,
        }
    )
def _scenario_identity(
    request: ProjectBenchmarkRequest,
    project: ProjectFingerprint,
    *,
    mode: str,
    workers: int,
    max_mutants: int | None,
) -> WorkloadIdentity:
    # Build one exact cold/warm, campaign-size and worker-count comparison boundary.
    mutant_token = _mutant_count_token(max_mutants)
    return WorkloadIdentity(
        workload_name=f"offline-mutation-campaign.{mode}.mutants-{mutant_token}.workers-{workers}",
        workload_version=PROJECT_BENCHMARK_VERSION,
        project_key=_project_key(request),
        input_fingerprint=_input_fingerprint(request, project, max_mutants=max_mutants),
        environment_fingerprint=_environment_fingerprint(),
        runtime_fingerprint=_runtime_fingerprint(),
    )
def _scenario_configuration(
    request: ProjectBenchmarkRequest,
    *,
    session_id: str,
    mode: str,
    workers: int,
    max_mutants: int | None,
    repetition: int = 1,
    repetitions: int = 1,
) -> CampaignConfiguration:
    # Create isolated campaign state while preserving one shared cold/warm history per exact scale lane.
    mutant_token = _mutant_count_token(max_mutants)
    repetition_token = "" if repetitions == 1 else f"-r{repetition}"
    lane_id = hashlib.sha256(f"{session_id}:{mutant_token}:{workers}{repetition_token}".encode("utf-8")).hexdigest()[:16]
    project_token = hashlib.sha256(_project_key(request).encode("utf-8")).hexdigest()[:12]
    campaign_id = CampaignId(f"benchmark-{session_id[-12:]}-m{mutant_token}-w{workers}-{mode}{repetition_token}")
    project_id = ProjectId(f"benchmark-{project_token}-{lane_id}")
    reports_dir = request.output_root / session_id / f"mutants-{mutant_token}" / f"workers-{workers}" / "reports"
    return CampaignConfiguration(
        campaign_id=campaign_id,
        project=ProjectDescriptor(
            project_id=project_id,
            display_name=request.display_name or request.project_root.name or "Theseus benchmark project",
            root_path=str(request.project_root),
            test_command=TestCommandDescriptor(
                argv=request.test_command,
                cwd=str(request.project_root),
                shell=False,
            ),
        ),
        scope=MutationScope(
            source_path=request.source_path,
            function=request.function,
            operators=request.operators,
        ),
        budget=CampaignBudget(
            max_mutants=max_mutants,
            max_workers=workers,
            max_test_seconds=request.test_timeout_seconds,
            lease_seconds=request.lease_seconds,
        ),
        no_escalation=request.no_escalation,
        reports_dir=str(reports_dir),
    )
def _raw_runner_report(result: object) -> tuple[Mapping[str, object] | None, str | None]:
    # Load the engine report referenced by the canonical result without guessing a missing path.
    engine_result = getattr(result, "engine_result", None)
    report = getattr(engine_result, "report", None)
    if not isinstance(report, Mapping):
        return None, None
    raw_path = report.get("raw_engine_report_path")
    if not raw_path:
        return None, None
    path = Path(str(raw_path))
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return None, str(path)
    return (dict(value), str(path)) if isinstance(value, Mapping) else (None, str(path))


def _physical_process_spawn_evidence(raw_report_path: str | None) -> tuple[int | None, str | None]:
    # Prefer one durable per-pytest evidence file per physical launch, including launches from retried attempts.
    if not raw_report_path:
        return None, None
    report_path = Path(raw_report_path).resolve()
    if not report_path.is_file():
        return None, None
    campaign_root = next(
        (
            parent
            for parent in report_path.parents
            if parent.parent is not None and parent.parent.name == "workers"
        ),
        None,
    )
    if campaign_root is None:
        return None, None
    process_evidence = tuple(
        candidate
        for candidate in campaign_root.rglob("*.performance.json")
        if "test_stats_events" in candidate.parts
    )
    if process_evidence:
        return len(process_evidence), "project-benchmark.aggregate-test-stats.processes_started"
    # Fall back to shard-level counters for older reports without per-process evidence.
    counts: list[int] = []
    for candidate in campaign_root.rglob("engine-shard-*.json"):
        engine_parent = candidate.parent.name == "engine" or (
            candidate.parent.parent is not None and candidate.parent.parent.name == "engine"
        )
        if not engine_parent or candidate.name.endswith(".performance.json"):
            continue
        try:
            value = json.loads(candidate.read_text(encoding="utf-8"))
        except (OSError, TypeError, ValueError):
            continue
        if not isinstance(value, Mapping):
            continue
        metrics_root = value.get("metrics", {})
        metrics = metrics_root if isinstance(metrics_root, Mapping) else {}
        performance_root = metrics.get("performance", metrics)
        performance = performance_root if isinstance(performance_root, Mapping) else {}
        raw_count = performance.get("processes_started")
        if isinstance(raw_count, int) and not isinstance(raw_count, bool) and raw_count >= 0:
            counts.append(raw_count)
    return (
        (sum(counts), "project-benchmark.aggregate-shard-runner.processes_started")
        if counts
        else (None, None)
    )


def _physical_process_spawn_count(raw_report_path: str | None) -> int | None:
    # Keep the compact count helper for callers while preserving source provenance in benchmark reports.
    count, _source = _physical_process_spawn_evidence(raw_report_path)
    return count


def _non_negative_number(value: object) -> float | None:
    # Normalize one finite non-negative timeline measurement without accepting booleans.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    normalized = float(value)
    return normalized if normalized >= 0.0 and math.isfinite(normalized) else None
def _diagnostic_metric_unit(name: object) -> str:
    # Keep scheduler ratios/counts typed correctly when timeline diagnostics enter the public benchmark report.
    normalized = str(name).strip().lower()
    if normalized.endswith("_per_mutant"):
        return "ratio"
    if normalized == "worker_utilization" or normalized.endswith("_ratio"):
        return "ratio"
    if "bytes" in normalized:
        return "bytes"
    if (
        normalized.endswith("_count")
        or normalized.endswith("_units")
        or normalized.endswith("_files")
        or normalized.endswith("_hits")
        or normalized.endswith("_misses")
        or normalized == "scheduler_worker_slots"
    ):
        return "count"
    return "seconds"

def _coordinator_timeline(result: object) -> tuple[Mapping[str, object] | None, str | None]:
    # Load the exclusive coordinator timeline located beside the authoritative campaign database.
    raw_database = getattr(result, "database_path", None)
    if not raw_database:
        return None, None
    path = Path(str(raw_database)).resolve().parent / "coordinator.performance.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return None, str(path)
    if not isinstance(value, Mapping) or value.get("exclusive") is not True:
        return None, str(path)
    return dict(value), str(path)
def _campaign_counts(result: object) -> tuple[int | None, int | None]:
    # Read selected and completed mutant counts from the public result without inventing missing evidence.
    engine_result = getattr(result, "engine_result", None)
    summary = getattr(engine_result, "summary", None)
    campaign = getattr(result, "campaign", None)
    raw_selected = getattr(summary, "total_mutants", None)
    if raw_selected is None:
        raw_selected = getattr(campaign, "total_mutants", None)
    raw_completed = getattr(summary, "completed_mutants", None)
    if raw_completed is None:
        raw_completed = getattr(campaign, "completed_mutants", None)
    selected = int(raw_selected) if isinstance(raw_selected, int) and not isinstance(raw_selected, bool) and raw_selected >= 0 else None
    completed = int(raw_completed) if isinstance(raw_completed, int) and not isinstance(raw_completed, bool) and raw_completed >= 0 else None
    return selected, completed


def _authoritative_result_acceptance(result: object) -> dict[str, object]:
    # Read only the published canonical report when deriving semantic and integrity acceptance evidence.
    raw_engine_result = getattr(result, "engine_result", None)
    raw_summary = getattr(raw_engine_result, "summary", None)
    candidates: list[Path] = []
    summary_path = getattr(raw_summary, "report_path", None)
    if summary_path:
        candidates.append(Path(str(summary_path)).resolve())
    database_path = getattr(result, "database_path", None)
    if database_path:
        candidates.append(Path(str(database_path)).resolve().parent / "canonical.report.json")
    report_path = next((item for item in candidates if item.is_file()), None)
    if report_path is None:
        return {
            "status": "unavailable",
            "reason": "authoritative canonical report was not available",
        }
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return {
            "status": "unavailable",
            "reason": "authoritative canonical report could not be decoded",
            "report_path": str(report_path),
        }
    if not isinstance(report, Mapping):
        return {
            "status": "unavailable",
            "reason": "authoritative canonical report was not an object",
            "report_path": str(report_path),
        }
    raw_results = report.get("results", ())
    if not isinstance(raw_results, Sequence) or isinstance(raw_results, (str, bytes)):
        return {
            "status": "unavailable",
            "reason": "authoritative canonical results were not an array",
            "report_path": str(report_path),
        }
    semantic_rows: list[tuple[str, str]] = []
    for raw_row in raw_results:
        if not isinstance(raw_row, Mapping):
            return {
                "status": "unavailable",
                "reason": "authoritative canonical result row was not an object",
                "report_path": str(report_path),
            }
        raw_mutant = raw_row.get("mutant", {})
        mutant_id = raw_mutant.get("mutant_id") if isinstance(raw_mutant, Mapping) else raw_row.get("mutant_id")
        status = str(raw_row.get("status", "")).strip()
        if not str(mutant_id).strip() or not status:
            return {
                "status": "unavailable",
                "reason": "authoritative canonical result lacked mutant or semantic status",
                "report_path": str(report_path),
            }
        semantic_rows.append((str(mutant_id), status))
    semantic_rows.sort()
    integrity = report.get("integrity", {})
    integrity_ok = (
        report.get("source") == "gallifrey_authoritative"
        and isinstance(integrity, Mapping)
        and integrity.get("one_terminal_evidence_per_mutant") is True
        and integrity.get("terminal_evidence_count") == len(semantic_rows)
        and len({item[0] for item in semantic_rows}) == len(semantic_rows)
    )
    return {
        "status": "observed",
        "report_path": str(report_path),
        "semantic_digest": _stable_hash(semantic_rows),
        "result_count": len(semantic_rows),
        "integrity_ok": integrity_ok,
    }


def _load_campaign_plan_diagnostics(result: object, *, workers: int) -> dict[str, object]:
    # Summarize immutable shard topology so scale reports expose planner load distribution without changing planning.
    raw_database = getattr(result, "database_path", None)
    if not raw_database:
        return {"status": "unavailable", "reason": "campaign database path was not available"}
    path = Path(str(raw_database)).resolve().parent / "campaign.plan.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return {"status": "unavailable", "reason": "campaign plan was not available", "plan_path": str(path)}
    if not isinstance(value, Mapping):
        return {"status": "unavailable", "reason": "campaign plan was not an object", "plan_path": str(path)}
    raw_shards = value.get("shards", ())
    if not isinstance(raw_shards, Sequence) or isinstance(raw_shards, (str, bytes)):
        return {"status": "unavailable", "reason": "campaign plan shards were not a sequence", "plan_path": str(path)}
    shard_mutant_counts: list[int] = []
    shard_estimated_seconds: list[float] = []
    for raw in raw_shards:
        if not isinstance(raw, Mapping):
            continue
        mutant_ids = raw.get("mutant_ids", ())
        if isinstance(mutant_ids, Sequence) and not isinstance(mutant_ids, (str, bytes)):
            shard_mutant_counts.append(len(mutant_ids))
        estimated = _non_negative_number(raw.get("estimated_cost"))
        if estimated is not None:
            shard_estimated_seconds.append(estimated)
    shard_count = len(shard_mutant_counts)
    average_mutants = sum(shard_mutant_counts) / shard_count if shard_count else 0.0
    average_seconds = sum(shard_estimated_seconds) / len(shard_estimated_seconds) if shard_estimated_seconds else 0.0
    candidate_count = _non_negative_number(value.get("candidate_count"))
    eligible_count = _non_negative_number(value.get("eligible_count"))
    selected_count = _non_negative_number(value.get("selected_count"))
    return {
        "status": "observed",
        "plan_path": str(path),
        "candidate_mutants": int(candidate_count) if candidate_count is not None else None,
        "eligible_mutants": int(eligible_count) if eligible_count is not None else None,
        "selected_mutants": int(selected_count) if selected_count is not None else sum(shard_mutant_counts),
        "shard_count": shard_count,
        "shard_mutant_counts": shard_mutant_counts,
        "shard_estimated_seconds": shard_estimated_seconds,
        "shard_mutant_imbalance_ratio": round(max(shard_mutant_counts) / average_mutants, 6) if average_mutants > 0.0 else None,
        "shard_cost_imbalance_ratio": round(max(shard_estimated_seconds) / average_seconds, 6) if average_seconds > 0.0 else None,
        "planned_worker_coverage_ratio": round(min(shard_count, workers) / workers, 6) if workers > 0 else None,
    }
def _timeline_phases(value: Mapping[str, object]) -> tuple[PhaseMeasurement, ...]:
    # Convert reconciled exclusive intervals into authority phases without inventing CPU or memory evidence.
    raw_phases = value.get("phases", ())
    if not isinstance(raw_phases, Sequence) or isinstance(raw_phases, (str, bytes)):
        return ()
    phases: list[PhaseMeasurement] = []
    seen: set[str] = set()
    for raw in raw_phases:
        if not isinstance(raw, Mapping):
            continue
        phase = str(raw.get("phase", "")).strip()
        seconds = raw.get("wall_seconds")
        source = str(raw.get("source", "coordinator.perf_counter")).strip()
        if (
            not phase
            or phase in seen
            or _non_negative_number(seconds) is None
            or not source
        ):
            continue
        seen.add(phase)
        phases.append(
            PhaseMeasurement(
                phase=phase,
                wall_seconds=PerformanceMetric.observed(
                    "wall_seconds",
                    _non_negative_number(seconds) or 0.0,
                    "seconds",
                    source,
                ),
                cpu_seconds=PerformanceMetric.unavailable(
                    "cpu_seconds",
                    "seconds",
                    "coordinator timeline has no per-phase CPU evidence",
                ),
                peak_memory_bytes=PerformanceMetric.unavailable(
                    "peak_memory_bytes",
                    "bytes",
                    "coordinator timeline has no per-phase memory evidence",
                ),
            )
        )
    return tuple(phases)
def _replace_metric(
    metrics: Sequence[PerformanceMetric],
    replacement: PerformanceMetric,
) -> tuple[PerformanceMetric, ...]:
    # Replace one metric by name while retaining deterministic ordering and custom runner evidence.
    values = {item.name: item for item in metrics}
    values[replacement.name] = replacement
    return tuple(values[name] for name in sorted(values))
def _scenario_run(
    result: object,
    *,
    identity: WorkloadIdentity,
    run_id: str,
    started_at: str,
    completed_at: str,
    wall_seconds: float,
    coordinator_cpu_seconds: float,
    mode: str,
    workers: int,
    max_mutants: int | None = None,
) -> PerformanceRun:
    # Merge authoritative runner phase evidence with externally measured end-to-end wall time.
    raw_report, raw_report_path = _raw_runner_report(result)
    if raw_report is not None:
        adapted = performance_run_from_runner_report(raw_report, identity=identity, run_id=run_id)
        phases = adapted.phases
        metrics = adapted.metrics
    else:
        phases = tuple(
            PhaseMeasurement.unavailable(phase, "runner performance report was not available")
            for phase in STANDARD_PHASES
        )
        metrics = tuple(
            PerformanceMetric.unavailable(name, unit, "runner performance report was not available")
            for name, unit in sorted(STANDARD_METRICS.items())
        )
    physical_process_count, physical_process_source = _physical_process_spawn_evidence(raw_report_path)
    if physical_process_count is not None:
        metrics = _replace_metric(
            metrics,
            PerformanceMetric.observed(
                "process_spawn_count",
                physical_process_count,
                "count",
                physical_process_source or "project-benchmark.physical-process-evidence",
            ),
        )
    timeline, timeline_path = _coordinator_timeline(result)
    timeline_phases = _timeline_phases(timeline) if timeline is not None else ()
    if timeline_phases:
        phases = timeline_phases
        total = _non_negative_number(timeline.get("total_wall_seconds"))
        accounted = _non_negative_number(timeline.get("accounted_seconds"))
        residual = _non_negative_number(timeline.get("residual_seconds"))
        total = total if total is not None else sum(
            float(item.wall_seconds.value or 0.0) for item in timeline_phases
        )
        residual = residual if residual is not None else next(
            (
                float(item.wall_seconds.value or 0.0)
                for item in timeline_phases
                if item.phase == "unattributed_residual"
            ),
            0.0,
        )
        accounted = accounted if accounted is not None else total
        metrics = _replace_metric(
            metrics,
            PerformanceMetric.observed(
                "coordinator_timeline_wall_seconds",
                total,
                "seconds",
                "coordinator.performance.total_wall_seconds",
            ),
        )
        metrics = _replace_metric(
            metrics,
            PerformanceMetric.observed(
                "phase_accounted_seconds",
                accounted,
                "seconds",
                "coordinator.performance.accounted_seconds",
            ),
        )
        metrics = _replace_metric(
            metrics,
            PerformanceMetric.observed(
                "phase_residual_seconds",
                residual,
                "seconds",
                "coordinator.performance.residual_seconds",
            ),
        )
        metrics = _replace_metric(
            metrics,
            PerformanceMetric.observed(
                "phase_accounting_ratio",
                min(1.0, accounted / total) if total > 0.0 else 1.0,
                "ratio",
                "coordinator.performance.reconciliation",
            ),
        )
        metrics = _replace_metric(
            metrics,
            PerformanceMetric.observed(
                "benchmark_wrapper_overhead_seconds",
                max(0.0, wall_seconds - total),
                "seconds",
                "project-benchmark.minus-coordinator-timeline",
            ),
        )
        raw_diagnostics = timeline.get("diagnostics", {})
        if isinstance(raw_diagnostics, Mapping):
            for name, value in raw_diagnostics.items():
                normalized_value = _non_negative_number(value)
                if normalized_value is not None:
                    metrics = _replace_metric(
                        metrics,
                        PerformanceMetric.observed(
                            str(name),
                            normalized_value,
                            _diagnostic_metric_unit(name),
                            "coordinator.performance.diagnostics",
                        ),
                    )
    metrics = _replace_metric(
        metrics,
        PerformanceMetric.observed(
            "wall_seconds",
            wall_seconds,
            "seconds",
            "project-benchmark.perf_counter",
        ),
    )
    metrics = _replace_metric(
        metrics,
        PerformanceMetric.observed(
            "coordinator_cpu_seconds",
            coordinator_cpu_seconds,
            "seconds",
            "project-benchmark.process_time",
        ),
    )
    metrics = _replace_metric(
        metrics,
        PerformanceMetric.unavailable(
            "coordinator_peak_memory_bytes",
            "bytes",
            "child-process-aware peak memory sampling is not connected",
        ),
    )
    authoritative_acceptance = _authoritative_result_acceptance(result)
    raw_authoritative_count = authoritative_acceptance.get("result_count")
    authoritative_result_count = (
        int(raw_authoritative_count)
        if isinstance(raw_authoritative_count, int) and not isinstance(raw_authoritative_count, bool) and raw_authoritative_count >= 0
        else None
    )
    selected_mutants, completed_mutants = _campaign_counts(result)
    metrics = _replace_metric(
        metrics,
        PerformanceMetric.observed(
            "mutants_selected",
            selected_mutants,
            "count",
            "campaign.summary.total_mutants",
        )
        if selected_mutants is not None
        else PerformanceMetric.unavailable(
            "mutants_selected",
            "count",
            "campaign result did not expose selected mutant count",
        ),
    )
    metrics = _replace_metric(
        metrics,
        PerformanceMetric.observed(
            "mutants_completed",
            completed_mutants,
            "count",
            "campaign.summary.completed_mutants",
        )
        if completed_mutants is not None
        else PerformanceMetric.unavailable(
            "mutants_completed",
            "count",
            "campaign result did not expose completed mutant count",
        ),
    )
    metrics = _replace_metric(
        metrics,
        PerformanceMetric.observed(
            "mutants_per_second",
            completed_mutants / wall_seconds,
            "mutants/second",
            "project-benchmark.completed-mutants-per-wall-second",
        )
        if completed_mutants is not None and wall_seconds > 0.0
        else PerformanceMetric.unavailable(
            "mutants_per_second",
            "mutants/second",
            "completed mutant count or positive wall time was not available",
        ),
    )
    metrics = _replace_metric(
        metrics,
        PerformanceMetric.observed(
            "cost_per_authoritative_mutation_result",
            wall_seconds / authoritative_result_count,
            "seconds/authoritative-result",
            "project-benchmark.wall-per-authoritative-result",
        )
        if (
            authoritative_acceptance.get("status") == "observed"
            and authoritative_acceptance.get("integrity_ok") is True
            and authoritative_result_count is not None
            and authoritative_result_count > 0
            and wall_seconds > 0.0
        )
        else PerformanceMetric.unavailable(
            "cost_per_authoritative_mutation_result",
            "seconds/authoritative-result",
            "authoritative canonical result count/integrity or positive wall time was not available",
        ),
    )
    scale_diagnostics = _load_campaign_plan_diagnostics(result, workers=workers)
    succeeded = bool(getattr(result, "succeeded", False))
    campaign = getattr(result, "campaign", None)
    campaign_status = str(getattr(getattr(campaign, "status", "unknown"), "value", getattr(campaign, "status", "unknown")))
    return PerformanceRun(
        run_id=run_id,
        identity=identity,
        status="completed" if succeeded else "failed",
        started_at=started_at,
        completed_at=completed_at,
        phases=phases,
        metrics=metrics,
        metadata={
            "benchmark_version": PROJECT_BENCHMARK_VERSION,
            "mode": mode,
            "workers": workers,
            "max_mutants": max_mutants,
            "scale_diagnostics": scale_diagnostics,
            "authoritative_acceptance": authoritative_acceptance,
            "campaign_id": str(getattr(getattr(campaign, "campaign_id", ""), "value", getattr(campaign, "campaign_id", ""))),
            "campaign_status": campaign_status,
            "database_path": str(getattr(result, "database_path", "")),
            "canonical_report_path": str(
                getattr(getattr(getattr(result, "engine_result", None), "summary", None), "report_path", "")
            ),
            "raw_engine_report_path": raw_report_path,
            "coordinator_timeline_path": timeline_path,
            "coordinator_timeline_version": (
                str(timeline.get("timeline_version", "")) if timeline is not None else None
            ),
            "coordinator_diagnostics": (
                dict(timeline.get("diagnostics", {}))
                if timeline is not None and isinstance(timeline.get("diagnostics"), Mapping)
                else {}
            ),
            "error": getattr(result, "error", None),
        },
    )
def _execute_scenario(
    configuration: CampaignConfiguration,
    identity: WorkloadIdentity,
    *,
    run_id: str,
    mode: str,
    workers: int,
    max_mutants: int | None,
    coordinator_factory: Callable[[], CampaignExecutor],
) -> tuple[object, PerformanceRun]:
    # Execute one real campaign while measuring only evidence available without a new runtime dependency.
    started_at = _utc_now()
    started_wall = time.perf_counter()
    started_cpu = time.process_time()
    try:
        result = coordinator_factory().run(configuration)
    finally:
        wall_seconds = max(0.0, time.perf_counter() - started_wall)
        cpu_seconds = max(0.0, time.process_time() - started_cpu)
    completed_at = _utc_now()
    return result, _scenario_run(
        result,
        identity=identity,
        run_id=run_id,
        started_at=started_at,
        completed_at=completed_at,
        wall_seconds=wall_seconds,
        coordinator_cpu_seconds=cpu_seconds,
        mode=mode,
        workers=workers,
        max_mutants=max_mutants,
    )
def _phase_summary(run: PerformanceRun) -> dict[str, object]:
    # Rank only observed phase wall times and list every instrumentation gap explicitly.
    observed = [
        {
            "phase": item.phase,
            "wall_seconds": item.wall_seconds.value,
            "share_of_total": (
                round(float(item.wall_seconds.value) / float(run.metric_map()["wall_seconds"].value), 6)
                if item.wall_seconds.value is not None
                and run.metric_map().get("wall_seconds") is not None
                and run.metric_map()["wall_seconds"].value not in {None, 0.0}
                else None
            ),
        }
        for item in run.phases
        if item.wall_seconds.status == "observed" and item.wall_seconds.value is not None
    ]
    observed.sort(key=lambda item: (-float(item["wall_seconds"]), str(item["phase"])))
    unmeasured = [item.phase for item in run.phases if item.wall_seconds.status != "observed"]
    metric_map = run.metric_map()
    total_metric = metric_map.get("wall_seconds")
    total_seconds = float(total_metric.value) if total_metric is not None and total_metric.value is not None else None
    accounting_metric = metric_map.get("phase_accounting_ratio")
    accounting_ratio = (
        float(accounting_metric.value)
        if accounting_metric is not None and accounting_metric.value is not None
        else None
    )
    observed_seconds = sum(float(item["wall_seconds"]) for item in observed)
    residual_seconds = next(
        (float(item["wall_seconds"]) for item in observed if item["phase"] == "unattributed_residual"),
        None,
    )
    return {
        "dominant_observed_phase": observed[0]["phase"] if observed else None,
        "observed_phase_ranking": observed,
        "unmeasured_phases": unmeasured,
        "phase_coverage_ratio": round(len(observed) / len(run.phases), 6) if run.phases else 0.0,
        "observed_phase_seconds": observed_seconds,
        "unattributed_residual_seconds": residual_seconds,
        "external_wall_seconds": total_seconds,
        "phase_accounting_ratio": round(accounting_ratio, 6) if accounting_ratio is not None else None,
        "external_wall_coverage_ratio": (
            round(observed_seconds / total_seconds, 6)
            if total_seconds not in {None, 0.0}
            else None
        ),
        "phase_accounting": evaluate_phase_accounting(run),
    }
def _metric_value(run: PerformanceRun | PerformanceRepetitionSummary, name: str) -> float | None:
    # Read one observed metric value without treating unavailable evidence as zero.
    if isinstance(run, PerformanceRepetitionSummary):
        return run.metric_value(name)
    metric = run.metric_map().get(name)
    return float(metric.value) if metric is not None and metric.value is not None else None
def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    # Calculate one finite non-negative ratio only when both measurements are usable.
    if numerator is None or denominator is None or denominator <= 0.0:
        return None
    value = numerator / denominator
    return round(value, 6) if math.isfinite(value) and value >= 0.0 else None
def _speedup(baseline: PerformanceRun | PerformanceRepetitionSummary, current: PerformanceRun | PerformanceRepetitionSummary) -> float | None:
    # Calculate one wall-clock speedup only when both measured totals are positive.
    return _ratio(_metric_value(baseline, "wall_seconds"), _metric_value(current, "wall_seconds"))
def _matrix_comparisons(
    runs: Mapping[tuple[int | None, str, int], PerformanceRun | PerformanceRepetitionSummary],
    *,
    mutant_counts: Sequence[int | None],
    worker_counts: Sequence[int],
) -> dict[str, object]:
    # Summarize warm-state, worker scaling and campaign-size effects without host-specific pass thresholds.
    cold_warm: list[dict[str, object]] = []
    worker_scaling: list[dict[str, object]] = []
    campaign_scaling: list[dict[str, object]] = []
    baseline_workers = min(worker_counts) if worker_counts else None
    for max_mutants in mutant_counts:
        for worker_count in worker_counts:
            cold = runs.get((max_mutants, "cold", worker_count))
            warm = runs.get((max_mutants, "warm", worker_count))
            if cold is not None and warm is not None:
                cold_warm.append(
                    {
                        "max_mutants": max_mutants,
                        "workers": worker_count,
                        "cold_run_id": cold.run_id,
                        "warm_run_id": warm.run_id,
                        "warm_speedup": _speedup(cold, warm),
                        "throughput_ratio": _ratio(
                            _metric_value(warm, "mutants_per_second"),
                            _metric_value(cold, "mutants_per_second"),
                        ),
                    }
                )
        if baseline_workers is None:
            continue
        for mode in ("cold", "warm"):
            baseline = runs.get((max_mutants, mode, baseline_workers))
            if baseline is None:
                continue
            for worker_count in worker_counts:
                if worker_count == baseline_workers:
                    continue
                current = runs.get((max_mutants, mode, worker_count))
                if current is None:
                    continue
                speedup = _speedup(baseline, current)
                theoretical = worker_count / baseline_workers
                worker_scaling.append(
                    {
                        "max_mutants": max_mutants,
                        "mode": mode,
                        "baseline_workers": baseline_workers,
                        "workers": worker_count,
                        "baseline_run_id": baseline.run_id,
                        "run_id": current.run_id,
                        "speedup": speedup,
                        "parallel_efficiency": _ratio(speedup, theoretical),
                        "throughput_ratio": _ratio(
                            _metric_value(current, "mutants_per_second"),
                            _metric_value(baseline, "mutants_per_second"),
                        ),
                    }
                )
    finite_counts = [int(item) for item in mutant_counts if item is not None]
    baseline_mutants = min(finite_counts) if finite_counts else None
    if baseline_mutants is not None:
        for mode in ("cold", "warm"):
            for worker_count in worker_counts:
                baseline = runs.get((baseline_mutants, mode, worker_count))
                if baseline is None:
                    continue
                for max_mutants in finite_counts:
                    if max_mutants == baseline_mutants:
                        continue
                    current = runs.get((max_mutants, mode, worker_count))
                    if current is None:
                        continue
                    campaign_scaling.append(
                        {
                            "mode": mode,
                            "workers": worker_count,
                            "baseline_max_mutants": baseline_mutants,
                            "max_mutants": max_mutants,
                            "baseline_run_id": baseline.run_id,
                            "run_id": current.run_id,
                            "requested_work_growth": round(max_mutants / baseline_mutants, 6),
                            "actual_work_growth": _ratio(
                                _metric_value(current, "mutants_completed"),
                                _metric_value(baseline, "mutants_completed"),
                            ),
                            "wall_growth": _ratio(
                                _metric_value(current, "wall_seconds"),
                                _metric_value(baseline, "wall_seconds"),
                            ),
                            "throughput_ratio": _ratio(
                                _metric_value(current, "mutants_per_second"),
                                _metric_value(baseline, "mutants_per_second"),
                            ),
                        }
                    )
    return {
        "cold_to_warm": cold_warm,
        "worker_scaling": worker_scaling,
        "campaign_scaling": campaign_scaling,
    }


def _matrix_acceptance(scenarios: Sequence[Mapping[str, object]]) -> dict[str, object]:
    # Compare canonical semantic outcomes within each campaign-size lane and surface missing evidence explicitly.
    groups: dict[str, list[Mapping[str, object]]] = {}
    for scenario in scenarios:
        key = str(scenario.get("max_mutants"))
        groups.setdefault(key, []).append(scenario)
    group_reports: list[dict[str, object]] = []
    for key, items in sorted(groups.items()):
        acceptances: list[Mapping[str, object]] = []
        for item in items:
            metadata = item.get("metadata", {})
            acceptance = metadata.get("authoritative_acceptance") if isinstance(metadata, Mapping) else None
            if isinstance(acceptance, Mapping):
                acceptances.append(acceptance)
        observed = [item for item in acceptances if item.get("status") == "observed"]
        digests = {str(item.get("semantic_digest")) for item in observed if item.get("semantic_digest")}
        semantic_status = (
            "insufficient_evidence"
            if len(observed) != len(items)
            else "passed"
            if len(digests) == 1
            else "failed"
        )
        integrity_status = (
            "insufficient_evidence"
            if len(observed) != len(items)
            else "passed"
            if all(item.get("integrity_ok") is True for item in observed)
            else "failed"
        )
        group_reports.append(
            {
                "max_mutants": None if key == "None" else int(key),
                "scenario_count": len(items),
                "semantic_status": semantic_status,
                "semantic_results_equal": True if semantic_status == "passed" else False if semantic_status == "failed" else None,
                "integrity_status": integrity_status,
                "integrity_safe": True if integrity_status == "passed" else False if integrity_status == "failed" else None,
                "semantic_digests": sorted(digests),
            }
        )
    semantic_statuses = {str(item["semantic_status"]) for item in group_reports}
    integrity_statuses = {str(item["integrity_status"]) for item in group_reports}
    semantic_status = "failed" if "failed" in semantic_statuses else "insufficient_evidence" if "insufficient_evidence" in semantic_statuses else "passed"
    integrity_status = "failed" if "failed" in integrity_statuses else "insufficient_evidence" if "insufficient_evidence" in integrity_statuses else "passed"
    overall_status = "failed" if "failed" in {semantic_status, integrity_status} else "insufficient_evidence" if "insufficient_evidence" in {semantic_status, integrity_status} else "passed"
    return {
        "status": overall_status,
        "semantic_status": semantic_status,
        "semantic_results_equal": True if semantic_status == "passed" else False if semantic_status == "failed" else None,
        "integrity_status": integrity_status,
        "integrity_safe": True if integrity_status == "passed" else False if integrity_status == "failed" else None,
        "groups": group_reports,
    }


def _scale_health(comparisons: Mapping[str, object]) -> dict[str, object]:
    # Report scale pathologies as observations; do not turn host-specific efficiency into an invented gate.
    raw_worker = comparisons.get("worker_scaling", ())
    worker_scaling = raw_worker if isinstance(raw_worker, Sequence) and not isinstance(raw_worker, (str, bytes)) else ()
    superlinear = [
        {
            "max_mutants": item.get("max_mutants"),
            "mode": item.get("mode"),
            "workers": item.get("workers"),
            "parallel_efficiency": item.get("parallel_efficiency"),
        }
        for item in worker_scaling
        if isinstance(item, Mapping)
        and isinstance(item.get("parallel_efficiency"), (int, float))
        and float(item["parallel_efficiency"]) > 1.0
    ]
    return {
        "status": "observed" if worker_scaling else "insufficient_evidence",
        "worker_scaling_observations": len(worker_scaling),
        "superlinear_observations": superlinear,
        "superlinear_observation_count": len(superlinear),
    }
def _atomic_write_json(path: Path, value: Mapping[str, object]) -> None:
    # Publish one benchmark report atomically without adding a dependency on engine IO helpers.
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    payload = json.dumps(value, ensure_ascii=False, indent=2) + "\n"
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
def _remove_benchmark_owned_path(path: Path, state_root: Path, *, directory: bool) -> None:
    # Remove one benchmark-owned disposable path without permitting symlink or path-escape deletion.
    state_root = state_root.resolve()
    candidate = Path(path)
    if candidate.is_symlink():
        raise RuntimeError(f"benchmark cleanup refuses symlink: {candidate}")
    resolved = candidate.resolve()
    if resolved == state_root or not resolved.is_relative_to(state_root):
        raise RuntimeError(f"benchmark cleanup path escapes state root: {candidate}")
    if directory:
        if resolved.exists():
            if not resolved.is_dir():
                raise RuntimeError(f"benchmark cleanup expected directory: {resolved}")
            shutil.rmtree(resolved)
        return
    if resolved.exists():
        if not resolved.is_file():
            raise RuntimeError(f"benchmark cleanup expected file: {resolved}")
        resolved.unlink()


def _cleanup_benchmark_lane_state(configurations: Sequence[CampaignConfiguration]) -> None:
    # Reclaim only copied project trees after both successful cold and warm scenarios have durable evidence.
    for configuration in configurations:
        raw_reports = configuration.reports_dir
        if not raw_reports:
            raise RuntimeError("benchmark cleanup requires an explicit reports directory")
        state_root = Path(raw_reports).expanduser().resolve().parent / "state"
        campaign_id = configuration.campaign_id.value
        workspaces_root = state_root / "workspaces"
        _remove_benchmark_owned_path(
            workspaces_root / campaign_id,
            state_root,
            directory=True,
        )
        _remove_benchmark_owned_path(
            workspaces_root / f"{campaign_id}.ownership.json",
            state_root,
            directory=False,
        )
        workers_root = state_root / "workers" / campaign_id
        if workers_root.is_symlink():
            raise RuntimeError(f"benchmark cleanup refuses symlink: {workers_root}")
        if not workers_root.is_dir():
            continue
        for worker_root in workers_root.iterdir():
            if worker_root.is_symlink() or not worker_root.is_dir():
                continue
            for attempt_root in worker_root.iterdir():
                if (
                    attempt_root.is_symlink()
                    or not attempt_root.is_dir()
                    or not attempt_root.name.startswith("attempt-")
                ):
                    continue
                _remove_benchmark_owned_path(
                    attempt_root / "workspace",
                    state_root,
                    directory=True,
                )
                _remove_benchmark_owned_path(
                    attempt_root / "workspace.ownership.json",
                    state_root,
                    directory=False,
                )


def run_project_benchmark(
    request: ProjectBenchmarkRequest,
    *,
    coordinator_factory: Callable[[], CampaignExecutor] | None = None,
) -> dict[str, object]:
    # Execute isolated campaign-size, worker-count and cold/warm lanes and persist comparable authority records.
    coordinator_factory = coordinator_factory or _default_coordinator_factory
    session_id = f"benchmark-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}-{uuid.uuid4().hex[:8]}"
    session_root = request.output_root / session_id
    session_root.mkdir(parents=True, exist_ok=False)
    project = fingerprint_project(request.project_root)
    history_database = request.output_root / "performance.sqlite3"
    store = PerformanceStore(history_database)
    mutant_counts = _scenario_mutant_counts(request)
    runs: dict[tuple[int | None, str, int], PerformanceRepetitionSummary] = {}
    repetition_runs: dict[tuple[int | None, str, int], list[PerformanceRun]] = {}
    scenarios: list[dict[str, object]] = []
    for max_mutants in mutant_counts:
        mutant_token = _mutant_count_token(max_mutants)
        for workers in request.worker_counts:
            completed_lane_configurations: list[CampaignConfiguration] = []
            for mode in ("cold", "warm"):
                identity = _scenario_identity(
                    request,
                    project,
                    mode=mode,
                    workers=workers,
                    max_mutants=max_mutants,
                )
                physical_runs: list[PerformanceRun] = []
                mode_configurations: list[CampaignConfiguration] = []
                for repetition in range(1, request.repetitions + 1):
                    configuration = _scenario_configuration(
                        request,
                        session_id=session_id,
                        mode=mode,
                        workers=workers,
                        max_mutants=max_mutants,
                        repetition=repetition,
                        repetitions=request.repetitions,
                    )
                    repetition_token = "" if request.repetitions == 1 else f"-r{repetition}"
                    run_id = f"{session_id}-m{mutant_token}-w{workers}-{mode}{repetition_token}"
                    result, run = _execute_scenario(
                        configuration,
                        identity,
                        run_id=run_id,
                        mode=mode,
                        workers=workers,
                        max_mutants=max_mutants,
                        coordinator_factory=coordinator_factory,
                    )
                    store.append(run)
                    if request.pin_baselines and repetition == 1:
                        store.pin_baseline(run.run_id)
                    physical_runs.append(run)
                    scenario = {
                        "mode": mode,
                        "workers": workers,
                        "max_mutants": max_mutants,
                        "repetition": repetition,
                        "repetition_count": request.repetitions,
                        "run_id": run.run_id,
                        "comparison_key": run.identity.comparison_key,
                        "status": run.status,
                        "succeeded": bool(getattr(result, "succeeded", False)),
                        "metrics": {name: item.to_dict() for name, item in run.metric_map().items()},
                        "phases": [item.to_dict() for item in run.phases],
                        "bottleneck_summary": _phase_summary(run),
                        "scale_summary": dict(run.metadata.get("scale_diagnostics", {})),
                        "metadata": dict(run.metadata),
                    }
                    scenarios.append(scenario)
                    artifact_name = (
                        f"{mode}.performance.json"
                        if request.repetitions == 1
                        else f"{mode}.r{repetition}.performance.json"
                    )
                    _atomic_write_json(
                        session_root / f"mutants-{mutant_token}" / f"workers-{workers}" / artifact_name,
                        run.to_dict(),
                    )
                    if not bool(getattr(result, "succeeded", False)):
                        break
                    mode_configurations.append(configuration)
                if physical_runs:
                    lane_key = (max_mutants, mode, workers)
                    repetition_runs[lane_key] = physical_runs
                    runs[lane_key] = summarize_performance_runs(physical_runs)
                if len(mode_configurations) == request.repetitions:
                    completed_lane_configurations.extend(mode_configurations)
            if len(completed_lane_configurations) == 2 * request.repetitions:
                _cleanup_benchmark_lane_state(completed_lane_configurations)
    expected_scenarios = len(mutant_counts) * len(request.worker_counts) * 2 * request.repetitions
    authority_lanes: list[dict[str, object]] = []
    for max_mutants in mutant_counts:
        for workers in request.worker_counts:
            for mode in ("cold", "warm"):
                lane_key = (max_mutants, mode, workers)
                summary = runs.get(lane_key)
                if summary is None:
                    continue
                authority_lanes.append(
                    {
                        "mode": mode,
                        "workers": workers,
                        "max_mutants": max_mutants,
                        "physical_run_count": len(repetition_runs.get(lane_key, ())),
                        "summary": summary.to_dict(),
                    }
                )
    comparisons = _matrix_comparisons(
        runs,
        mutant_counts=mutant_counts,
        worker_counts=request.worker_counts,
    )
    acceptance = _matrix_acceptance(scenarios)
    report: dict[str, object] = {
        "schema_version": PROJECT_BENCHMARK_SCHEMA_VERSION,
        "benchmark_version": PROJECT_BENCHMARK_VERSION,
        "session_id": session_id,
        "created_at": _utc_now(),
        "project": {
            "project_key": _project_key(request),
            "root": str(request.project_root),
            "source_path": request.source_path,
            "function": request.function,
            "fingerprint": project.to_dict(),
        },
        "configuration": {
            "test_command": list(request.test_command),
            "operators": list(request.operators),
            "max_mutants": request.max_mutants,
            "mutant_counts": list(request.mutant_counts),
            "effective_mutant_counts": list(mutant_counts),
            "worker_counts": list(request.worker_counts),
            "test_timeout_seconds": request.test_timeout_seconds,
            "lease_seconds": request.lease_seconds,
            "no_escalation": request.no_escalation,
            "pin_baselines": request.pin_baselines,
            "repetitions": request.repetitions,
        },
        "environment_fingerprint": _environment_fingerprint(),
        "runtime_fingerprint": _runtime_fingerprint(),
        "scenarios": scenarios,
        "performance_authority": {
            "aggregation_version": PERFORMANCE_AGGREGATION_VERSION,
            "minimum_repetitions": PERFORMANCE_MIN_REPETITIONS,
            "requested_repetitions": request.repetitions,
            "sufficient_repetitions": request.repetitions >= PERFORMANCE_MIN_REPETITIONS,
            "lane_count": len(authority_lanes),
            "sufficient_lane_count": sum(
                item["summary"]["repetition_status"] == "sufficient" for item in authority_lanes
            ),
            "lanes": authority_lanes,
        },
        "comparisons": comparisons,
        "scale_health": _scale_health(comparisons),
        "acceptance": acceptance,
        "history_database": str(history_database),
        "report_path": str(session_root / "project-benchmark.report.json"),
        "completed": len(scenarios) == expected_scenarios and all(
            bool(item.get("succeeded")) for item in scenarios
        ),
    }
    _atomic_write_json(Path(str(report["report_path"])), report)
    return report
__all__ = [
    "PROJECT_BENCHMARK_SCHEMA_VERSION",
    "PROJECT_BENCHMARK_VERSION",
    "ProjectBenchmarkRequest",
    "ProjectFingerprint",
    "fingerprint_project",
    "run_project_benchmark",
]
