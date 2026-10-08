"""Mutation campaign orchestration: baseline cache, escalation and reports."""
from __future__ import annotations
import re
import hashlib
import json
import os
import sqlite3
import time
from collections import Counter
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Mapping, Sequence
from .commands import (
    command_fingerprint,
    classify_pytest_exit,
    env_fingerprint,
    looks_like_infrastructure_failure,
    resolve_python,
    run_argv,
    prepare_pytest_command,
    without_parallelism,
)
from .index import (
    SELECTION_ALGORITHM_VERSION,
    NodeidValidationContext,
    _context_tests,
    _rank_pairs,
    _selection_score,
    _static_dependency_tests,
    _static_related_tests,
    add_domain_levels,
    build_index,
    build_nodeid_validation_context,
    find_function,
    load_context_map,
    load_index,
    plan_selection,
    validate_test_nodeids,
)
from .impact import SQLiteImpactAdapter
from .io_utils import (
    FileLock,
    append_json_line,
    atomic_write_json,
    atomic_write_text,
    ensure_dir,
    read_json,
    sha256_file,
    sha256_text,
    stable_hash,
    utc_now_iso,
)
from .models import (
    CampaignAccumulator,
    MutantSelection,
    PerformanceMetrics,
    Mutant,
    MutantResult,
    ProcessResult,
    SelectionEvidence,
    SelectionLevel,
    SelectionSnapshot,
    make_selection_evidence,
)
from .mutations import (
    InvalidMutantError,
    PreparedMutant,
    RestoreError,
    SourceSnapshot,
    apply_prepared_mutant,
    create_snapshot,
    current_sha256,
    prepare_mutant,
    recover_manifest,
    restore_snapshot,
    write_manifest,
)
from .recovery import (
    campaign_input_fingerprint,
    current_process_birth_token,
    journal_metadata,
    replay_result_journal,
    result_identity_digests,
    result_event_id,
)
from .test_stats import (
    build_test_stats_env,
    ingest_test_stats,
    is_pytest_command,
    instrument_pytest_command,
    load_pytest_attempt_performance,
    open_test_stats_connection,
    stats_attempt_id,
    stats_db_path,
    summarize_test_health,
    summarize_test_health_rows,
)
from .mutation_discovery_service import MutationDiscoveryService
from .workspace_cow import privatize_hardlinked_tree
RUNNER_VERSION = "unified-v1.26"
STATE_CHECKPOINT_MUTANTS = 25
STATE_CHECKPOINT_SECONDS = 5.0


def _with_project_pythonpath(
    argv: Sequence[str],
    root: Path,
    environment: dict[str, str],
) -> dict[str, str]:
    # Resolve pytest pythonpath overrides against the project being executed.
    override_values: list[str] = []
    tokens = [str(item) for item in argv]
    index = 0
    while index < len(tokens):
        token = tokens[index]
        candidate: str | None = None
        if token in {"-o", "--override-ini"} and index + 1 < len(tokens):
            candidate = tokens[index + 1]
            index += 1
        elif token.startswith("--override-ini="):
            candidate = token.split("=", 1)[1]
        elif token.startswith("-o="):
            candidate = token.split("=", 1)[1]
        if candidate is not None:
            name, separator, value = candidate.partition("=")
            if separator and name.strip().lower() == "pythonpath":
                override_values.extend(part.strip() for part in value.split(os.pathsep) if part.strip())
        index += 1
    if not override_values:
        return dict(environment)
    resolved_paths: list[str] = []
    seen: set[str] = set()
    for raw_path in override_values:
        path = Path(raw_path).expanduser()
        if not path.is_absolute():
            path = root / path
        resolved = str(path.resolve())
        key = os.path.normcase(resolved)
        if key not in seen:
            seen.add(key)
            resolved_paths.append(resolved)
    existing = str(environment.get("PYTHONPATH", ""))
    if existing:
        resolved_paths.append(existing)
    updated = dict(environment)
    updated["PYTHONPATH"] = os.pathsep.join(resolved_paths)
    return updated


class StaleInputError(ValueError):
    """Raised before mutation when an index or frozen snapshot is obsolete."""
    def __init__(self, status: str, message: str) -> None:
        super().__init__(message)
        self.status = status
@dataclass(frozen=True)
class MutationConfig:
    project_root: Path
    source: str
    main_root_path: Path | None = None
    function: str | None = None
    index_path: Path | None = None
    context_map_path: Path | None = None
    impact_db: Path | None = None
    selected_tests_file: Path | None = None
    test_command_argv: tuple[str, ...] | None = None
    test_command_cwd: Path | None = None
    domain: str | None = None
    domain_command_argv: tuple[str, ...] | None = None
    common_command_argv: tuple[str, ...] | None = None
    python_executable: str | None = None
    reports_dir: Path | None = None
    timeout_seconds: float = 120.0
    timeout_retry_factor: float = 2.0
    max_mutants: int | None = None
    from_line: int | None = None
    to_line: int | None = None
    mutant_ids: frozenset[str] = frozenset()
    no_escalation: bool = False
    use_baseline_cache: bool = True
    audit_percent: float = 0.0
    selection_snapshot: dict[str, Any] | None = None
    operators: tuple[str, ...] | None = None
    workers: int = 1
    workspace_backend: str = "copy"
    index_snapshot: dict[str, Any] | None = None
    shared_baseline_provider: Any = None
    compact_report: bool = False
    environment_mode: str = "strict"
    environment_keys: tuple[str, ...] = ()
    environment_prefixes: tuple[str, ...] = ()
    environment_ignored: tuple[str, ...] = ()
    environment_secrets: tuple[str, ...] = ()
    environment_secret_key: str = "theseus-environment-v1"
    pytest_plugin_autoload: bool = True
@dataclass(frozen=True)
class LevelSpec:
    name: str
    reason: str
    command_argv: tuple[str, ...]
    nodeids: tuple[str, ...] = ()
    files: tuple[str, ...] = ()
@dataclass(frozen=True, slots=True)
class _PreparedTestLaunch:
    """Immutable launch facts shared by fresh pytest processes in one worker."""

    command_argv: tuple[str, ...]
    pytest_command: bool
    environment_template: tuple[tuple[str, str], ...] = ()

    def environment_for_attempt(
        self,
        *,
        run_id: str,
        phase: str,
        level: str,
        mutant_id: str | None,
        target_sha256: str | None,
        retry: bool,
    ) -> dict[str, str] | None:
        # Copy only the mutable per-attempt map while retaining frozen command/plugin setup.
        if not self.pytest_command:
            return None
        environment = dict(self.environment_template)
        environment.update(
            {
                "TI_TEST_STATS_RUN_ID": str(run_id),
                "TI_TEST_STATS_PHASE": str(phase),
                "TI_TEST_STATS_LEVEL": str(level),
                "TI_TEST_STATS_MUTANT_ID": mutant_id or "",
                "TI_TEST_STATS_TARGET_SHA256": target_sha256 or "",
                "TI_TEST_STATS_RETRY": "1" if retry else "0",
                "TI_TEST_STATS_ATTEMPT": stats_attempt_id(run_id, phase, level, mutant_id, retry),
            }
        )
        return environment
def _safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)[:100]
def _snapshot_from_dict(value: dict[str, Any]) -> SelectionSnapshot:
    # Convert a JSON selection snapshot while preserving freshness metadata.
    levels = tuple(
        SelectionLevel(
            name=str(item.get("name", "L1")),
            reason=str(item.get("reason", "snapshot")),
            nodeids=tuple(str(entry) for entry in item.get("nodeids", [])),
            files=tuple(str(entry) for entry in item.get("files", [])),
            command_argv=tuple(str(entry) for entry in item.get("command_argv", [])),
        )
        for item in value.get("levels", [])
        if isinstance(item, dict)
    )
    map_version = str(value.get("map_version", "external"))
    algorithm_version = str(
        value.get(
            "algorithm_version",
            "selection-v2" if map_version.startswith("selection-v2:") else SELECTION_ALGORITHM_VERSION,
        )
    )
    selected_tests = tuple(str(item) for item in value.get("selected_tests", []))
    if not selected_tests and levels:
        selected_tests = levels[0].nodeids
    raw_evidence = value.get("evidence", {})
    evidence: dict[str, tuple[SelectionEvidence, ...]] = {}
    if isinstance(raw_evidence, dict):
        for nodeid, rows in raw_evidence.items():
            if not isinstance(rows, (list, tuple)):
                continue
            evidence[str(nodeid)] = tuple(
                SelectionEvidence.from_dict(item)
                for item in rows
                if isinstance(item, dict)
            )
    return SelectionSnapshot(
        schema_version=int(value.get("schema_version", 1)),
        snapshot_id=str(value.get("snapshot_id", "external")),
        created_at=str(value.get("created_at", utc_now_iso())),
        project_root=str(value.get("project_root", "")),
        source_path=str(value.get("source_path", "")),
        function_id=value.get("function_id"),
        source_sha256=str(value.get("source_sha256", "")),
        map_version=map_version,
        levels=levels,
        selected_tests=selected_tests,
        reasons={
            str(key): ([str(entry) for entry in item] if isinstance(item, list) else [str(item)])
            for key, item in value.get("reasons", {}).items()
        },
        impact_status=str(value.get("impact_status", "missing")),
        impact_schema_version=(
            int(value["impact_schema_version"])
            if value.get("impact_schema_version") is not None
            else None
        ),
        impact_warning=value.get("impact_warning"),
        impact_error=value.get("impact_error"),
        algorithm_version=algorithm_version,
        index_version=value.get("index_version"),
        test_config_fingerprint=value.get("test_config_fingerprint"),
        dropped_nodeids=tuple(str(item) for item in value.get("dropped_nodeids", [])),
        evidence=evidence,
    )
class MutationRunner:
    def __init__(self, config: MutationConfig, *, execution_backend: Any | None = None) -> None:
        # Initialize campaign state and the bounded performance collector.
        self.config = config
        self.root = config.project_root.resolve()
        self.test_cwd = (config.test_command_cwd or self.root).resolve()
        if execution_backend is None:
            from theseus_local.execution_backend import LocalProcessBackend

            execution_backend = LocalProcessBackend(executor=self._legacy_process_executor)
        self.execution_backend = execution_backend
        self.reports_dir = (config.reports_dir or Path(__file__).resolve().parent / "reports").resolve()
        self.artifacts_dir = self.reports_dir / "artifacts"
        self.cache_path = self.reports_dir / "baseline_cache.json"
        self._baseline_cache: dict[str, Any] = {}
        self.performance = PerformanceMetrics()
        self._mutation_discovery_service = MutationDiscoveryService(self.performance)
        self._campaign_accumulator = CampaignAccumulator()
        self._health_summary_cache: dict[str, Any] = summarize_test_health_rows(())
        self._baseline_results: dict[str, dict[str, Any]] = {}
        self._mutant_by_id: dict[str, Mutant] = {}
        self._prepared_mutant_by_id: dict[str, PreparedMutant] = {}
        self._campaign_selection: SelectionSnapshot | None = None
        self._nodeid_validation_context: NodeidValidationContext | None = None
        self._campaign_function_info: dict[str, Any] | None = None
        self._campaign_source_rel = ""
        self._campaign_function_id: str | None = None
        self._line_selection_cache: dict[int, MutantSelection] = {}
        self._line_impact_cache: dict[int, tuple[dict[str, Any], ...]] = {}
        self._function_impact_cache: tuple[dict[str, Any], ...] | None = None
        self._context_selection_cache: list[tuple[str, str]] | None = None
        self._static_selection_cache: list[tuple[str, str]] | None = None
        self._static_dependency_cache: list[tuple[str, str]] | None = None
        self._impact_adapter: SQLiteImpactAdapter | None = None
        self._campaign_index: dict[str, Any] | None = None
        self._context_map: dict[str, Any] | None = None
        self._results_path: Path | None = None
        self._state_path: Path | None = None
        self._test_stats_run_id = ""
        self._test_stats_event_dir: Path | None = None
        self._test_stats_connection: sqlite3.Connection | None = None
        self._test_stats_summary: dict[str, Any] = {"events_ingested": 0, "invalid_events": 0, "event_files": 0}
        self._campaign_snapshot: SourceSnapshot | None = None
        self._last_state_checkpoint_mutants = 0
        self._last_state_checkpoint_at = time.monotonic()
        self._results_journal_size = 0
        self._results_journal_digest = hashlib.sha256()
        self._completed_identity_digests: dict[str, str] = {}
        self._completed_mutant_ids: set[str] = set()
        self._pytest_process_attempts = 0
        self._pytest_process_observed_attempts = 0
        self._pytest_process_observed_elapsed_seconds = 0.0
        self._pytest_process_phase_seconds: dict[str, float] = {}
        self._pytest_process_unavailable_reasons: Counter[str] = Counter()
        self._prepared_invocation_seconds = 0.0
        self._prepared_invocation_command_builds = 0
        self._prepared_invocation_process_preparations = 0
        self._prepared_test_launches: dict[tuple[Any, ...], _PreparedTestLaunch] = {}
        self._prepared_selection_commands: dict[tuple[str, ...], tuple[str, ...]] = {}
        self._prepared_launch_descriptor_builds = 0
        self._prepared_launch_descriptor_hits = 0
        self._prepared_launch_descriptor_seconds = 0.0
        self._hardlink_workspace_private = config.workspace_backend != "hardlink-cow"

    def _legacy_process_executor(self, argv: Sequence[str], **kwargs: Any) -> ProcessResult:
        # Preserve the existing test hook while routing production launches through the backend port.
        return run_argv(argv, **kwargs)
    def run(self) -> dict[str, Any]:
        # Keep the mutation runner serial-only so production parallelism has one coordinator runtime.
        campaign_started = time.perf_counter()
        if self.config.workers > 1:
            raise ValueError(
                "MutationRunner is serial-only; use LocalCampaignCoordinator for multi-worker execution"
            )
        try:
            report = self._run()
        except StaleInputError as exc:
            ensure_dir(self.reports_dir)
            report_id = f"{utc_now_iso().replace(':', '').replace('+00:00', 'Z')}_{_safe_name(self.config.source)}"
            report_path = self.reports_dir / f"{report_id}.json"
            self._results_path = report_path.with_suffix(".results.jsonl")
            self._state_path = report_path.with_suffix(".state.json")
            report = {
                "schema_version": 2,
                "runner_version": RUNNER_VERSION,
                "run_id": report_id,
                "status": exc.status,
                "created_at": utc_now_iso(),
                "project_root": str(self.root),
                "target": {"source_path": self.config.source, "function": self.config.function},
                "error": str(exc),
                "results": [],
                "results_journal": str(self._results_path),
                "state_path": str(self._state_path),
                "metrics": self._metrics([]),
                "report_path": str(report_path),
                "markdown_path": str(report_path.with_suffix(".md")),
            }
            self._write_report(report_path, report)
            atomic_write_text(
                report_path.with_suffix(".md"),
                f"# {exc.status}\n\n{exc}\n",
                durability="normal",
                category="report",
                metrics=self.performance,
            )
            self._write_state(exc.status, [])
        self.performance.campaign_wall_seconds = time.perf_counter() - campaign_started
        if isinstance(report, dict):
            report.setdefault("metrics", {})["performance"] = self._performance_payload()
            report_path_value = report.get("report_path")
            if report_path_value:
                self._write_report(Path(str(report_path_value)), report)
        return report
    def _run(self) -> dict[str, Any]:
        # Execute one serialized mutation campaign with lazy escalation.
        ensure_dir(self.reports_dir)
        ensure_dir(self.artifacts_dir)
        self._campaign_accumulator = CampaignAccumulator()
        self._health_summary_cache = summarize_test_health_rows(())
        self._campaign_snapshot = None
        self._last_state_checkpoint_mutants = 0
        self._last_state_checkpoint_at = time.monotonic()
        self._results_journal_size = 0
        self._results_journal_digest = hashlib.sha256()
        self._completed_identity_digests = {}
        self._completed_mutant_ids = set()
        self._pytest_process_attempts = 0
        self._pytest_process_observed_attempts = 0
        self._pytest_process_observed_elapsed_seconds = 0.0
        self._pytest_process_phase_seconds = {}
        self._pytest_process_unavailable_reasons = Counter()
        self._prepared_invocation_seconds = 0.0
        self._prepared_invocation_command_builds = 0
        self._prepared_invocation_process_preparations = 0
        self._prepared_test_launches.clear()
        self._prepared_selection_commands.clear()
        self._prepared_launch_descriptor_builds = 0
        self._prepared_launch_descriptor_hits = 0
        self._prepared_launch_descriptor_seconds = 0.0
        phase_started = time.perf_counter()
        index = self._load_or_build_index()
        self.performance.index_seconds += time.perf_counter() - phase_started
        source_path = (self.root / self.config.source).resolve()
        source_rel = source_path.relative_to(self.root).as_posix()
        current_source_sha256 = sha256_file(source_path)
        try:
            self.performance.source_bytes_read += source_path.stat().st_size
        except OSError:
            pass
        indexed_file = index.get("files", {}).get(source_rel)
        if not isinstance(indexed_file, dict):
            raise StaleInputError("stale_index", f"index does not contain target source: {source_rel}")
        if indexed_file.get("sha256") != current_source_sha256:
            raise StaleInputError(
                "stale_index",
                f"stale index for {source_rel}: index has {indexed_file.get('sha256')}, current source is {current_source_sha256}",
            )
        function_info = find_function(index, source_rel, self.config.function) if self.config.function else None
        function_range = None
        function_id = None
        if function_info:
            function_range = (int(function_info["start_line"]), int(function_info["end_line"]))
            function_id = str(function_info["function_id"])
        phase_started = time.perf_counter()
        context_map = load_context_map(self.config.context_map_path)
        self._nodeid_validation_context = build_nodeid_validation_context(index, self.root)
        self._campaign_accumulator.configured_test_count = len(self._nodeid_validation_context.known_collected_nodeids)
        if self.config.selection_snapshot:
            selection = _snapshot_from_dict(self.config.selection_snapshot)
            if selection.source_path.replace("\\", "/").lstrip("./") != source_rel:
                raise StaleInputError("stale_snapshot", "selection snapshot target does not match --source")
            if selection.source_sha256 and selection.source_sha256 != current_source_sha256:
                raise StaleInputError(
                    "stale_snapshot",
                    f"stale selection snapshot for {source_rel}: snapshot has {selection.source_sha256}, current source is {current_source_sha256}",
                )
            if function_id and selection.function_id and selection.function_id != function_id:
                raise StaleInputError("stale_snapshot", "selection snapshot function does not match current index")
            if selection.algorithm_version != SELECTION_ALGORITHM_VERSION:
                raise StaleInputError("stale_snapshot", "selection snapshot algorithm version is obsolete")
            if selection.index_version and selection.index_version != str(index.get("index_version", "")):
                raise StaleInputError(
                    "stale_snapshot",
                    f"selection snapshot index version differs: {selection.index_version} != {index.get('index_version')}",
                )
            selection = self._validate_selection_snapshot(selection, index)
        else:
            selection = plan_selection(
                self.root,
                index,
                source_rel,
                function_id or self.config.function,
                context_map=context_map,
                impact_db=self.config.impact_db,
                selected_tests_file=self.config.selected_tests_file,
                test_stats_db=stats_db_path(self.reports_dir),
                validation_context=self._nodeid_validation_context,
            )
        if not self.config.no_escalation and len(selection.levels) < 3:
            selection = add_domain_levels(selection, self.config.domain)
        self.performance.selection_seconds += time.perf_counter() - phase_started
        self._campaign_index = index
        self._context_map = context_map
        target_lock = source_path.with_name(source_path.name + ".test_intelligence.lock")
        report_id = f"{utc_now_iso().replace(':', '').replace('+00:00', 'Z')}_{_safe_name(source_rel)}"
        report_path = self.reports_dir / f"{report_id}.json"
        manifest_path = self.reports_dir / f"{report_id}.manifest.json"
        snapshot: SourceSnapshot | None = None
        self._results_path = report_path.with_suffix(".results.jsonl")
        self._state_path = report_path.with_suffix(".state.json")
        self._test_stats_run_id = report_id
        self._test_stats_event_dir = self.reports_dir / "test_stats_events" / report_id
        self._baseline_results = {}
        report: dict[str, Any] = {
            "schema_version": 2,
            "runner_version": RUNNER_VERSION,
            "run_id": report_id,
            "status": "starting",
            "created_at": utc_now_iso(),
            "project_root": str(self.root),
            "target": {"source_path": source_rel, "function_id": function_id, "function": self.config.function},
            "config": self._config_dict(),
            "selection_snapshot": selection.to_dict(),
            "input_fingerprint": campaign_input_fingerprint(
                self.root,
                source_rel,
                current_source_sha256,
                selection.to_dict(),
                self._config_dict(),
            ),
            "baseline": [],
            "mutants": [],
            "results": [],
            "results_journal": str(self._results_path),
            "state_path": str(self._state_path),
            "test_stats": {
                "db_path": str(stats_db_path(self.reports_dir)),
                "event_dir": str(self._test_stats_event_dir),
            },
            "selection_audit": [],
            "compact_report": self.config.compact_report,
            "metrics": {},
            "recovery_manifest": str(manifest_path),
            "report_path": str(report_path),
            "markdown_path": str(report_path.with_suffix(".md")),
        }
        try:
            with FileLock(target_lock, {"target": str(source_path), "run_id": report_id}):
                snapshot_started = time.perf_counter()
                snapshot = create_snapshot(
                    source_path,
                    self.reports_dir / "recovery" / report_id,
                    metrics=self.performance,
                )
                self._campaign_snapshot = snapshot
                self.performance.snapshot_seconds += time.perf_counter() - snapshot_started
                write_manifest(
                    manifest_path,
                    snapshot,
                    extra={
                        "run_id": report_id,
                        "coordinator_pid": os.getpid(),
                        "process_birth_token": current_process_birth_token(),
                        "report_path": str(report_path),
                        "state_path": str(self._state_path),
                        "results_journal": str(self._results_path),
                        "input_fingerprint": report["input_fingerprint"],
                        "lock_path": str(target_lock),
                        "active_mutant": None,
                        "phase": "running",
                    },
                    metrics=self.performance,
                )
                report["source_snapshot"] = snapshot.to_dict()
                self._load_cache()
                levels = self._level_specs(selection)
                self._campaign_selection = selection
                self._campaign_function_info = function_info
                self._campaign_source_rel = source_rel
                self._campaign_function_id = function_id
                self._line_selection_cache = {}
                self._line_impact_cache = {}
                self._function_impact_cache = None
                self._context_selection_cache = None
                self._static_selection_cache = None
                self._static_dependency_cache = None
                self._impact_adapter = SQLiteImpactAdapter(self.config.impact_db) if self.config.impact_db else None
                report["levels"] = [self._level_dict(level) for level in levels]
                mutants = self._mutation_discovery_service.discover(
                    snapshot.text,
                    function_range=function_range,
                    from_line=self.config.from_line,
                    to_line=self.config.to_line,
                    mutant_ids=self.config.mutant_ids or None,
                    max_mutants=self.config.max_mutants,
                    operators=self.config.operators,
                )
                self._mutant_by_id = {mutant.mutant_id: mutant for mutant in mutants}
                report["mutants"] = [mutant.to_dict() for mutant in mutants]
                if not mutants:
                    report["status"] = "no_mutants"
                    report["metrics"] = self._metrics([])
                    self._finish_report(report_path, report, manifest_path, status="complete")
                    return report
                first_level = levels[0] if levels else None
                conservative_fallback = bool(
                    first_level is not None
                    and not first_level.nodeids
                    and not first_level.files
                    and any(
                        row.source == "domain_fallback"
                        for row in selection.evidence.get("__selection__", ())
                    )
                )
                if (
                    not self.config.test_command_argv
                    and (first_level is None or not first_level.nodeids and not first_level.files)
                    and not conservative_fallback
                ):
                    report["status"] = "no_l1_selection"
                    report["selection_error"] = "no valid L1 test nodeids remain after validation"
                    report["metrics"] = self._metrics([])
                    self._finish_report(report_path, report, manifest_path, status="no_l1_selection")
                    return report
                self._write_report(report_path, report)
                baseline_levels = levels[1:] if conservative_fallback else levels[:1]
                baselines = self._run_baselines(baseline_levels, snapshot, selection, function_info, report_id)
                report["baseline"] = baselines
                failed_baselines = [item for item in baselines if not item.get("passed", False)]
                if failed_baselines:
                    report["status"] = "baseline_failed"
                    report["metrics"] = self._metrics([])
                    self._finish_report(report_path, report, manifest_path, status="baseline_failed")
                    return report
                report["status"] = "running"
                for mutant in mutants:
                    result = self._run_mutant(mutant, snapshot, levels, report_id, manifest_path)
                    result_dict = result.to_dict()
                    if not self.config.compact_report:
                        report["results"].append(result_dict)
                    self._record_campaign_result(result_dict)
                    report["baseline"] = list(self._baseline_results.values())
                    report["metrics"] = self._metrics()
                    self._append_result(result)
                    self._write_state(report["status"], report["results"])
                    if result.status == "restore_error":
                        report["status"] = "restore_error"
                        self._finish_report(report_path, report, manifest_path, status="restore_error")
                        return report
                    if result.status == "baseline_failed":
                        report["status"] = "baseline_failed"
                        self._finish_report(report_path, report, manifest_path, status="baseline_failed")
                        return report
                if self.config.audit_percent > 0 and len(levels) >= 3:
                    audit_started = time.perf_counter()
                    audit_results = report["results"] if not self.config.compact_report else self._read_results_journal()
                    report["selection_audit"] = self._selection_audit(
                        audit_results, snapshot, levels[-1], report_id, manifest_path
                    )
                    self.performance.selection_audit_seconds += time.perf_counter() - audit_started
                report["metrics"] = self._metrics()
                self._finish_report(report_path, report, manifest_path, status="complete")
                return report
        except (OSError, UnicodeError, SyntaxError, LookupError, RestoreError, RuntimeError) as exc:
            self._close_test_stats_connection()
            report["status"] = "error"
            report["error"] = str(exc)
            report["metrics"] = self._metrics(refresh_health=True)
            self._write_report(report_path, report)
            self._write_state(report["status"], report.get("results", []))
            return report
    def _validate_selection_snapshot(
        self,
        selection: SelectionSnapshot,
        index: dict[str, Any],
    ) -> SelectionSnapshot:
        # Remove deleted or unindexed nodeids before any subprocess is created.
        levels: list[SelectionLevel] = []
        dropped = list(selection.dropped_nodeids)
        for level in selection.levels:
            if not level.nodeids:
                levels.append(level)
                continue
            valid, invalid = validate_test_nodeids(
                index,
                self.root,
                level.nodeids,
                context=self._nodeid_validation_context,
            )
            dropped.extend(invalid)
            files = tuple(dict.fromkeys(nodeid.split("::", 1)[0] for nodeid in valid))
            levels.append(replace(level, nodeids=valid, files=files or level.files))
        selected = levels[0].nodeids if levels else ()
        return replace(
            selection,
            levels=tuple(levels),
            selected_tests=selected,
            dropped_nodeids=tuple(dict.fromkeys(dropped)),
        )
    def _load_or_build_index(self) -> dict[str, Any]:
        # Prefer an immutable coordinator snapshot before reading or rebuilding local storage.
        if self.config.index_path and self.config.index_path.exists():
            index = load_index(self.config.index_path)
            indexed_root = index.get("project_root") if isinstance(index, dict) else None
            if indexed_root and Path(str(indexed_root)).resolve() != self.root:
                raise StaleInputError(
                    "stale_index",
                    f"index project root does not match current root: {indexed_root} != {self.root}",
                )
            return index
        if self.config.index_snapshot is not None:
            index = self.config.index_snapshot
            indexed_root = index.get("project_root") if isinstance(index, dict) else None
            if indexed_root and Path(str(indexed_root)).resolve() != self.root:
                raise StaleInputError(
                    "stale_index",
                    f"index snapshot project root does not match current root: {indexed_root} != {self.root}",
                )
            if not isinstance(index, dict):
                raise StaleInputError("stale_index", "index snapshot is not an object")
            return index
        index = build_index(self.root, self.reports_dir / "index.sqlite")
        self.performance.index_build_count += 1
        return index
    def _config_dict(self) -> dict[str, Any]:
        # Serialize the effective campaign configuration for reproducibility.
        config = self.config
        return {
            "source": config.source,
            "function": config.function,
            "index": str(config.index_path) if config.index_path else None,
            "context_map": str(config.context_map_path) if config.context_map_path else None,
            "impact_db": str(config.impact_db) if config.impact_db else None,
            "selected_tests_file": str(config.selected_tests_file) if config.selected_tests_file else None,
            "test_command_argv": list(config.test_command_argv) if config.test_command_argv else None,
            "domain": config.domain,
            "domain_command_argv": list(config.domain_command_argv) if config.domain_command_argv else None,
            "common_command_argv": list(config.common_command_argv) if config.common_command_argv else None,
            "pytest_plugin_autoload": config.pytest_plugin_autoload,
            "python_executable": config.python_executable,
            "timeout_seconds": config.timeout_seconds,
            "timeout_retry_factor": config.timeout_retry_factor,
            "max_mutants": config.max_mutants,
            "from_line": config.from_line,
            "to_line": config.to_line,
            "mutant_ids": sorted(config.mutant_ids),
            "no_escalation": config.no_escalation,
            "use_baseline_cache": config.use_baseline_cache,
            "audit_percent": config.audit_percent,
            "operators": list(config.operators) if config.operators is not None else None,
            "workers": config.workers,
            "compact_report": config.compact_report,
            "index_snapshot_version": (
                str(config.index_snapshot.get("index_version"))
                if isinstance(config.index_snapshot, dict) and config.index_snapshot.get("index_version")
                else None
            ),
            "shared_baseline": config.shared_baseline_provider is not None,
        }
    def _level_specs(self, selection: SelectionSnapshot) -> list[LevelSpec]:
        # Resolve configured commands while keeping an empty L1 explicit.
        python = resolve_python(self.root, self.config.python_executable)
        specs: list[LevelSpec] = []
        for item in selection.levels:
            if item.name == "L1":
                if self.config.test_command_argv:
                    command = self.config.test_command_argv
                elif item.command_argv:
                    command = item.command_argv
                elif item.nodeids or item.files:
                    command = self._pytest_selection_command(python, item)
                else:
                    command = ()
            elif item.name == "L2":
                command = self.config.domain_command_argv or self._pytest_domain_command(python, self.config.domain)
            else:
                command = self.config.common_command_argv or (python, "-m", "pytest", "tests", "-q", "-n", "4")
            prepared = prepare_pytest_command(command, mode="baseline") if command else ()
            specs.append(
                LevelSpec(
                    item.name,
                    item.reason,
                    prepared,
                    item.nodeids,
                    item.files,
                )
            )
        return specs
    @staticmethod
    def _pytest_selection_command(python: str, level: SelectionLevel) -> tuple[str, ...]:
        values = level.nodeids or level.files or ("tests",)
        return (python, "-m", "pytest", *values, "-q")
    @staticmethod
    def _pytest_domain_command(python: str, domain: str | None) -> tuple[str, ...]:
        return (python, "-m", "pytest", domain or "tests", "-q")
    @staticmethod
    def _level_dict(level: LevelSpec) -> dict[str, Any]:
        return {
            "name": level.name,
            "reason": level.reason,
            "command_argv": list(level.command_argv),
            "nodeids": list(level.nodeids),
            "files": list(level.files),
        }
    def _load_cache(self) -> None:
        if not self.config.use_baseline_cache or not self.cache_path.exists():
            self._baseline_cache = {}
            return
        try:
            value = read_json(self.cache_path)
            self._baseline_cache = value if isinstance(value, dict) else {}
        except (OSError, ValueError):
            self._baseline_cache = {}
    def _save_cache(self) -> None:
        if self.config.use_baseline_cache:
            atomic_write_json(
                self.cache_path,
                self._baseline_cache,
                durability="normal",
                category="baseline_artifact",
                metrics=self.performance,
            )
    def _baseline_key(
        self,
        level: LevelSpec,
        snapshot: SourceSnapshot,
        selection: SelectionSnapshot,
        function_info: dict[str, Any] | None,
    ) -> str:
        # Reuse an explicit project test-command baseline across file campaigns while keeping selected-test baselines source-scoped.
        if level.name == "L1" and self.config.test_command_argv:
            try:
                test_cwd = self.test_cwd.relative_to(self.root).as_posix() or "."
            except ValueError:
                test_cwd = str(self.test_cwd)
            environment = env_fingerprint(
                self.test_cwd,
                include_cwd=False,
                declared_keys=self.config.environment_keys,
                declared_patterns=self.config.environment_prefixes,
                ignored_variables=self.config.environment_ignored,
                secret_variables=self.config.environment_secrets,
                inherit_policy=self.config.environment_mode,
                secret_key=self.config.environment_secret_key,
            )
            return stable_hash(
                {
                    "runner_version": RUNNER_VERSION,
                    "baseline_scope": "project-test-command-v1",
                    "level": level.name,
                    "command": list(level.command_argv),
                    "test_cwd": test_cwd,
                    "environment": environment,
                    "index_version": selection.index_version,
                    "timeout_policy": {"seconds": self.config.timeout_seconds, "retry_factor": self.config.timeout_retry_factor},
                }
            )
        function_fingerprint = sha256_text(
            snapshot.text[0:] if not function_info else "\n".join(
                snapshot.text.splitlines()[int(function_info["start_line"]) - 1 : int(function_info["end_line"])]
            )
        )
        return stable_hash(
            {
                "runner_version": RUNNER_VERSION,
                "level": level.name,
                "source_sha256": snapshot.original_sha256,
                "function_fingerprint": function_fingerprint,
                "command": list(level.command_argv),
                "command_fingerprint": command_fingerprint(level.command_argv, self.root),
                "env_fingerprint": env_fingerprint(self.root),
                "test_set": {"nodeids": list(level.nodeids), "files": list(level.files)},
                "selection_snapshot_id": selection.snapshot_id,
                "selection_map_version": selection.map_version,
                "timeout_policy": {"seconds": self.config.timeout_seconds, "retry_factor": self.config.timeout_retry_factor},
            }
        )
    def _ensure_private_worker_workspace(self) -> None:
        # Detach one hardlink-backed worker before any external test process can mutate project files.
        if self._hardlink_workspace_private or self.config.workspace_backend != "hardlink-cow":
            return
        privatize_hardlinked_tree(self.root)
        self._hardlink_workspace_private = True

    def _get_prepared_test_launch(
        self,
        argv: Sequence[str],
        *,
        report_id: str,
    ) -> _PreparedTestLaunch:
        # Cache only immutable launch facts; per-attempt identity is injected below.
        raw_argv = tuple(str(item) for item in argv)
        event_dir = self._test_stats_event_dir or (self.reports_dir / "test_stats_events" / report_id)
        cache_key = (str(report_id), str(event_dir), raw_argv)
        cached = self._prepared_test_launches.get(cache_key)
        if cached is not None:
            self._prepared_launch_descriptor_hits += 1
            return cached

        started_ns = time.perf_counter_ns()
        command = tuple(str(item) for item in instrument_pytest_command(raw_argv))
        pytest_command = is_pytest_command(raw_argv)
        environment_template: tuple[tuple[str, str], ...] = ()
        if pytest_command:
            environment = build_test_stats_env(
                event_dir,
                run_id=report_id,
                phase="__prepared__",
                level="__prepared__",
                mutant_id=None,
                source_path=self._campaign_source_rel or self.config.source,
                target_sha256=None,
                retry=False,
                environment_mode=self.config.environment_mode,
                environment_keys=self.config.environment_keys,
                environment_prefixes=self.config.environment_prefixes,
                environment_ignored=self.config.environment_ignored,
                environment_secrets=self.config.environment_secrets,
                environment_secret_key=self.config.environment_secret_key,
                pytest_plugin_autoload=self.config.pytest_plugin_autoload,
            )
            environment["TI_TEST_STATS_EXPECTED_PROJECT_ROOT"] = str(self.root)
            environment = _with_project_pythonpath(raw_argv, self.test_cwd, environment)
            for key in (
                "TI_TEST_STATS_RUN_ID",
                "TI_TEST_STATS_PHASE",
                "TI_TEST_STATS_LEVEL",
                "TI_TEST_STATS_MUTANT_ID",
                "TI_TEST_STATS_TARGET_SHA256",
                "TI_TEST_STATS_RETRY",
                "TI_TEST_STATS_ATTEMPT",
            ):
                environment.pop(key, None)
            environment_template = tuple(sorted((str(key), str(value)) for key, value in environment.items()))

        prepared = _PreparedTestLaunch(command, pytest_command, environment_template)
        self._prepared_test_launches[cache_key] = prepared
        self._prepared_launch_descriptor_builds += 1
        self._prepared_launch_descriptor_seconds += max(
            0.0, (time.perf_counter_ns() - started_ns) / 1_000_000_000
        )
        return prepared

    def _run_test_command(
        self,
        argv: Sequence[str],
        *,
        phase: str,
        level: str,
        mutant_id: str | None,
        target_sha256: str | None,
        report_id: str,
        output_artifact: Path,
        timeout_seconds: float,
        retry: bool = False,
    ) -> ProcessResult:
        # Run pytest with per-test attribution and ingest its journal immediately.
        invocation_started = time.perf_counter()
        event_dir = self._test_stats_event_dir or (self.reports_dir / "test_stats_events" / report_id)
        launch = self._get_prepared_test_launch(argv, report_id=report_id)
        command = launch.command_argv
        pytest_command = launch.pytest_command
        self._ensure_private_worker_workspace()
        test_env = None
        if pytest_command:
            test_env = launch.environment_for_attempt(
                run_id=report_id,
                phase=phase,
                level=level,
                mutant_id=mutant_id,
                target_sha256=target_sha256,
                retry=retry,
            )
        self._record_prepared_invocation(invocation_started, process_preparation=True)
        process_started = time.perf_counter()
        from theseus_local.execution_backend import ExecutionRequest

        process = self.execution_backend.execute(
            ExecutionRequest(
                execution_id=(
                    f"{report_id}:{phase}:{level}:"
                    f"{mutant_id or 'baseline'}:{'retry' if retry else 'first'}"
                ),
                argv=tuple(command),
                cwd=self.test_cwd,
                environment=test_env,
                timeout_seconds=timeout_seconds,
                output_artifact=output_artifact,
                retry=retry,
            ),
            metrics=self.performance,
        )
        process_wall_seconds = max(0.0, time.perf_counter() - process_started)
        self.performance.pytest_wrapper_seconds += max(0.0, process_wall_seconds - process.elapsed_seconds)
        if pytest_command:
            lowered_command = tuple(str(item).lower() for item in command)
            parallel_pytest = any(
                item in {"-n", "-d", "--numprocesses", "--dist", "--tx"}
                or item.startswith("--numprocesses=")
                or item.startswith("--dist=")
                or item.startswith("--tx=")
                or item.startswith("-n=")
                or item.startswith("-n") and len(item) > 2
                for item in lowered_command
            )
            pytest_performance = (
                {"status": "unavailable", "reason": "parallel_pytest_command"}
                if parallel_pytest
                else load_pytest_attempt_performance(
                    event_dir,
                    run_id=report_id,
                    phase=phase,
                    level=level,
                    mutant_id=mutant_id,
                    retry=retry,
                )
            )
            self._record_pytest_process_performance(process, pytest_performance)
            ingestion_started = time.perf_counter()
            attempt_id = stats_attempt_id(report_id, phase, level, mutant_id, retry)
            stats_connection = self._get_test_stats_connection()
            ingestion = ingest_test_stats(
                event_dir,
                stats_db_path(self.reports_dir),
                project_root=self.root,
                run_id=report_id,
                source_path=self._campaign_source_rel or self.config.source,
                event_files=event_dir.glob(f"{report_id}.{attempt_id}.*.jsonl"),
                connection=stats_connection,
                metrics=self.performance,
            )
            self.performance.test_stats_ingestion_seconds += time.perf_counter() - ingestion_started
        else:
            ingestion = {"events_ingested": 0, "invalid_events": 0, "event_files": 0}
        for key in ("events_ingested", "invalid_events"):
            self._test_stats_summary[key] = self._test_stats_summary.get(key, 0) + int(ingestion.get(key, 0))
        self._test_stats_summary["event_files"] = int(ingestion.get("event_files", 0))
        return process
    def _record_prepared_invocation(
        self,
        started: float,
        *,
        command_build: bool = False,
        process_preparation: bool = False,
    ) -> None:
        # Accumulate only pre-subprocess invocation work that previously lived in runner residual.
        self._prepared_invocation_seconds += max(0.0, time.perf_counter() - started)
        if command_build:
            self._prepared_invocation_command_builds += 1
        if process_preparation:
            self._prepared_invocation_process_preparations += 1
    def _record_pytest_process_performance(self, process: ProcessResult, evidence: dict[str, Any]) -> None:
        # Accumulate one exclusive pytest-process breakdown while retaining subprocess elapsed time as authority.
        self._pytest_process_attempts += 1
        if evidence.get("status") != "observed":
            self._pytest_process_unavailable_reasons[str(evidence.get("reason") or "unknown")] += 1
            return
        metrics = evidence.get("metrics")
        if not isinstance(metrics, dict):
            self._pytest_process_unavailable_reasons["performance_metrics_missing"] += 1
            return
        try:
            lifecycle_seconds = float(metrics.get("plugin_lifecycle_seconds", 0.0) or 0.0)
            process_seconds = max(0.0, float(process.elapsed_seconds))
        except (TypeError, ValueError):
            self._pytest_process_unavailable_reasons["performance_metrics_invalid"] += 1
            return
        tolerance = max(0.05, process_seconds * 0.01)
        if lifecycle_seconds > process_seconds + tolerance:
            self._pytest_process_unavailable_reasons["plugin_lifecycle_exceeds_process"] += 1
            return
        phases = {
            "pytest_bootstrap_shutdown_residual": max(0.0, process_seconds - lifecycle_seconds),
            "pytest_config_initialization": float(metrics.get("config_initialization_seconds", 0.0) or 0.0),
            "pytest_collection_import": float(metrics.get("collection_import_seconds", 0.0) or 0.0),
            "pytest_test_execution": float(metrics.get("test_execution_seconds", 0.0) or 0.0),
            "pytest_session_finalize": float(metrics.get("session_finalize_seconds", 0.0) or 0.0),
            "pytest_framework_residual": float(metrics.get("framework_residual_seconds", 0.0) or 0.0),
        }
        phase_total = sum(phases.values())
        reconciliation_tolerance = max(1e-6, process_seconds * 1e-6)
        if abs(phase_total - process_seconds) > reconciliation_tolerance:
            self._pytest_process_unavailable_reasons["process_breakdown_unreconciled"] += 1
            return
        self._pytest_process_observed_attempts += 1
        self._pytest_process_observed_elapsed_seconds += process_seconds
        for name, seconds in phases.items():
            self._pytest_process_phase_seconds[name] = self._pytest_process_phase_seconds.get(name, 0.0) + seconds
    def _performance_payload(self) -> dict[str, Any]:
        # Attach nested pytest timing evidence without replacing the authoritative aggregate process phase.
        payload = self.performance.to_dict()
        descriptor_builds = max(0, int(self._prepared_launch_descriptor_builds))
        descriptor_hits = max(0, int(self._prepared_launch_descriptor_hits))
        descriptor_lookups = descriptor_builds + descriptor_hits
        prepared_launch_descriptor = {
            "status": "observed",
            "cache_entries": len(self._prepared_test_launches),
            "builds": descriptor_builds,
            "hits": descriptor_hits,
            "lookups": descriptor_lookups,
            "hit_rate": descriptor_hits / descriptor_lookups if descriptor_lookups else 0.0,
            "static_build_seconds": max(0.0, float(self._prepared_launch_descriptor_seconds)),
        }
        payload["prepared_launch_descriptor"] = prepared_launch_descriptor
        timeline = payload.get("worker_execution_timeline")
        phases = timeline.get("phases") if isinstance(timeline, dict) else None
        if not isinstance(phases, list):
            return payload
        prepared_total = max(0.0, float(self._prepared_invocation_seconds))
        process_attempts = max(0, int(self._prepared_invocation_process_preparations))
        prepared = {
            "status": "observed",
            "total_seconds": prepared_total,
            "command_builds": max(0, int(self._prepared_invocation_command_builds)),
            "process_preparations": process_attempts,
            "seconds_per_process_preparation": prepared_total / process_attempts if process_attempts else 0.0,
        }
        residual_row = next(
            (item for item in phases if isinstance(item, dict) and item.get("phase") == "runner_unattributed_residual"),
            None,
        )
        if isinstance(residual_row, dict):
            residual_seconds = max(0.0, float(residual_row.get("wall_seconds", 0.0) or 0.0))
            tolerance = max(1e-6, float(timeline.get("total_wall_seconds", 0.0) or 0.0) * 1e-6)
            if prepared_total <= residual_seconds + tolerance:
                accounted_prepared = min(prepared_total, residual_seconds)
                residual_row["wall_seconds"] = max(0.0, residual_seconds - accounted_prepared)
                residual_row["source"] = "runner.reconciliation.minus-prepared-invocation"
                residual_index = phases.index(residual_row)
                phases.insert(
                    residual_index,
                    {
                        "phase": "prepared_invocation",
                        "wall_seconds": accounted_prepared,
                        "source": "runner.perf_counter",
                    },
                )
                timeline["observed_phase_seconds"] = max(
                    0.0, float(timeline.get("observed_phase_seconds", 0.0) or 0.0) + accounted_prepared
                )
                timeline["residual_seconds"] = max(
                    0.0, float(timeline.get("residual_seconds", 0.0) or 0.0) - accounted_prepared
                )
            else:
                prepared["status"] = "inconsistent_total"
                prepared["reason"] = "prepared_invocation_exceeds_runner_residual"
        payload["prepared_invocation"] = prepared
        timeline["prepared_invocation"] = prepared
        timeline["prepared_launch_descriptor"] = prepared_launch_descriptor
        pytest_row = next(
            (item for item in phases if isinstance(item, dict) and item.get("phase") == "pytest_process"),
            None,
        )
        if not isinstance(pytest_row, dict):
            return payload
        pytest_total = max(0.0, float(pytest_row.get("wall_seconds", 0.0) or 0.0))
        observed_total = max(0.0, self._pytest_process_observed_elapsed_seconds)
        tolerance = max(0.05, pytest_total * 0.01)
        status = "unavailable"
        if self._pytest_process_observed_attempts:
            status = "observed" if self._pytest_process_observed_attempts == self._pytest_process_attempts else "partial"
        phase_seconds = {
            name: max(0.0, float(self._pytest_process_phase_seconds.get(source_name, 0.0)))
            for name, source_name in (
                ("bootstrap_shutdown_residual", "pytest_bootstrap_shutdown_residual"),
                ("config_initialization", "pytest_config_initialization"),
                ("collection_import", "pytest_collection_import"),
                ("test_execution", "pytest_test_execution"),
                ("session_finalize", "pytest_session_finalize"),
                ("framework_residual", "pytest_framework_residual"),
            )
        }
        observed_phase_total = sum(phase_seconds.values())
        if observed_total > pytest_total + tolerance or observed_phase_total > pytest_total + tolerance:
            status = "inconsistent_total"
        phase_seconds["unattributed"] = max(0.0, pytest_total - observed_phase_total)
        breakdown = {
            "status": status,
            "exclusive": True,
            "attempts_total": self._pytest_process_attempts,
            "attempts_observed": self._pytest_process_observed_attempts,
            "attempts_unavailable": max(0, self._pytest_process_attempts - self._pytest_process_observed_attempts),
            "unavailable_reasons": dict(sorted(self._pytest_process_unavailable_reasons.items())),
            "total_pytest_process_seconds": pytest_total,
            "observed_process_seconds": observed_total,
            "phase_seconds": phase_seconds,
        }
        payload["pytest_process_breakdown"] = breakdown
        timeline["pytest_process_breakdown"] = breakdown
        return payload
    def _get_test_stats_connection(self) -> sqlite3.Connection:
        # Reuse one initialized SQLite connection for all pytest attempts in this runner.
        if self._test_stats_connection is None:
            self._test_stats_connection = open_test_stats_connection(stats_db_path(self.reports_dir))
        return self._test_stats_connection
    def _close_test_stats_connection(self) -> None:
        # Close the campaign statistics connection at a durable report boundary.
        if self._test_stats_connection is None:
            return
        self._test_stats_connection.close()
        self._test_stats_connection = None
    def _run_baselines(
        self,
        levels: Sequence[LevelSpec],
        snapshot: SourceSnapshot,
        selection: SelectionSnapshot,
        function_info: dict[str, Any] | None,
        report_id: str,
    ) -> list[dict[str, Any]]:
        # Delegate baseline policy to the extracted service while retaining the old private call shape.
        from .baseline_service import BaselineService
        return BaselineService(self).run(levels, snapshot, selection, function_info, report_id)
    def _activate_prepared_mutant(
        self,
        manifest_path: Path,
        snapshot: SourceSnapshot,
        prepared: PreparedMutant,
    ) -> dict[str, Any] | None:
        # Arm recovery with only the currently prepared mutant before changing the target.
        manifest = read_json(manifest_path)
        if not isinstance(manifest, dict):
            raise RuntimeError(f"recovery manifest is not an object: {manifest_path}")
        manifest["expected_hashes"] = sorted({snapshot.original_sha256, prepared.sha256})
        manifest["active_mutant"] = prepared.to_manifest()
        manifest["active_started_at"] = utc_now_iso()
        atomic_write_json(
            manifest_path,
            manifest,
            durability="critical",
            category="manifest",
            metrics=self.performance,
        )
    def _run_mutant(
        self,
        mutant: Mutant,
        snapshot: SourceSnapshot,
        levels: Sequence[LevelSpec],
        report_id: str,
        manifest_path: Path,
    ) -> MutantResult:
        # Delegate one-mutant execution through the extracted service port.
        from .mutation_execution_service import MutantExecutionService
        return MutantExecutionService(self).execute(mutant, snapshot, levels, report_id, manifest_path)
    def _run_mutant_impl(
        self,
        mutant: Mutant,
        snapshot: SourceSnapshot,
        levels: Sequence[LevelSpec],
        report_id: str,
        manifest_path: Path,
    ) -> MutantResult:
        # Apply, test, escalate and restore one patch while retaining provenance.
        execution_started = time.perf_counter()
        before = current_sha256(snapshot)
        prepared: PreparedMutant | None = None
        mutated_hash = ""
        restore_hash = before
        level_results: list[dict[str, Any]] = []
        output_artifacts: list[str] = []
        status = "error"
        reason = "not-executed"
        restored = False
        not_observed = False
        selection_details: list[dict[str, Any]] = []
        try:
            preparation_started = time.perf_counter()
            try:
                prepared = self._prepared_mutant_by_id.get(mutant.mutant_id)
                if prepared is None:
                    prepared = prepare_mutant(snapshot, mutant)
                elif prepared.original_sha256 != snapshot.original_sha256:
                    raise RestoreError(
                        f"prepared mutant baseline mismatch for {mutant.mutant_id}: "
                        f"expected {snapshot.original_sha256}, got {prepared.original_sha256}"
                    )
                self._activate_prepared_mutant(manifest_path, snapshot, prepared)
            finally:
                self.performance.mutation_preparation_seconds += time.perf_counter() - preparation_started
            apply_started = time.perf_counter()
            mutated_hash = apply_prepared_mutant(
                snapshot,
                prepared,
                current_sha256_value=before,
                durability="normal",
                metrics=self.performance,
            )
            self.performance.mutant_apply_seconds += time.perf_counter() - apply_started
            for level_index, level in enumerate(levels):
                if level_index > 0:
                    restore_started = time.perf_counter()
                    restore_hash = restore_snapshot(
                        snapshot,
                        expected_sha256=mutated_hash or None,
                        durability="normal",
                        metrics=self.performance,
                    )
                    self.performance.restore_seconds += time.perf_counter() - restore_started
                    mutated_hash = ""
                    campaign_selection = self._campaign_selection
                    if campaign_selection is None:
                        raise RuntimeError("campaign selection is not initialized")
                    baseline = self._run_baselines(
                        (level,),
                        snapshot,
                        campaign_selection,
                        self._campaign_function_info,
                        report_id,
                    )[0]
                    if not baseline.get("passed", False):
                        status = "baseline_failed"
                        reason = f"{level.name} baseline failed before escalation"
                        break
                    apply_started = time.perf_counter()
                    mutated_hash = apply_prepared_mutant(
                        snapshot,
                        prepared,
                        current_sha256_value=restore_hash,
                        durability="normal",
                        metrics=self.performance,
                    )
                    self.performance.mutant_apply_seconds += time.perf_counter() - apply_started
                run_level, selection_detail = self._level_for_mutant(mutant, level)
                selection_details.append(selection_detail)
                if run_level is None:
                    conservative_fallback = selection_detail.get("source") == "domain-fallback"
                    if level.name == "L1" and conservative_fallback:
                        not_observed = True
                        status = "not_observed"
                        reason = "L1 selection unavailable; conservative domain fallback"
                        if level_index + 1 < len(levels):
                            continue
                    status = "not_observed" if conservative_fallback else "no_l1_selection"
                    reason = (
                        "test selection was not observed at this level"
                        if conservative_fallback
                        else "no valid tests remain for mutant line"
                    )
                    break
                process, issue, artifacts = self._run_mutant_level(mutant, run_level, report_id, mutated_hash)
                output_artifacts.extend(artifacts)
                level_results.append(
                    {
                        "level": run_level.name,
                        "selection_reason": run_level.reason,
                        "nodeids": list(run_level.nodeids),
                        "command_argv": list(run_level.command_argv),
                        "result": process.to_dict(),
                        "issue": issue,
                        "output_artifacts": artifacts,
                    }
                )
                if issue:
                    status = issue
                    reason = "timeout retry remained timed out" if issue == "mutant_induced_timeout" else "test infrastructure or process failure"
                    break
                if not process.passed:
                    status = "killed" if level.name == "L1" else "selection_escape"
                    reason = f"{level.name} test command exited {process.exit_code}"
                    break
            else:
                if not not_observed:
                    status = "survived"
                    reason = "mutant passed every enabled escalation level"
        except InvalidMutantError as exc:
            status = "invalid_mutant"
            reason = str(exc)
        except (OSError, RestoreError, UnicodeError) as exc:
            status = "restore_error" if isinstance(exc, RestoreError) else "error"
            reason = str(exc)
        finally:
            try:
                restore_started = time.perf_counter()
                restore_hash = restore_snapshot(
                    snapshot,
                    expected_sha256=mutated_hash or None,
                    durability="normal",
                    metrics=self.performance,
                )
                self.performance.restore_seconds += time.perf_counter() - restore_started
                restored = restore_hash == snapshot.original_sha256
            except RestoreError as exc:
                status = "restore_error"
                reason = str(exc)
                restored = False
                try:
                    restore_hash = current_sha256(snapshot)
                except OSError:
                    restore_hash = "missing"
        return MutantResult(
            mutant=mutant.to_dict(),
            status=status,
            classification_reason=reason,
            level_results=tuple(level_results),
            source_sha256_before=before,
            source_sha256_after=mutated_hash,
            restore_sha256=restore_hash,
            restore_verified=restored,
            output_artifacts=tuple(output_artifacts),
            selection={"levels": selection_details},
            duration_seconds=max(0.0, time.perf_counter() - execution_started),
        )
    def _build_mutant_selection(self, mutant: Mutant, level: LevelSpec) -> MutantSelection:
        # Resolve and rank one line selection, reusing immutable campaign-level inputs.
        cached = self._line_selection_cache.get(mutant.line_no)
        if cached is not None:
            return replace(cached, mutant_id=mutant.mutant_id)
        if self._uses_frozen_selection():
            selection = self._build_frozen_mutant_selection(mutant, level)
            self._line_selection_cache[mutant.line_no] = selection
            return selection
        index = self._campaign_index or {}
        groups: list[tuple[str, float, list[tuple[str, str]], tuple[dict[str, Any], ...]]] = []
        dropped: list[str] = []
        dynamic_impact_available = False
        if self.config.selection_snapshot is not None or self.config.selected_tests_file is not None:
            groups.append(
                (
                    "frozen-selection",
                    1.0,
                    [(nodeid, "frozen-selection") for nodeid in level.nodeids],
                    (),
                )
            )
        if self._impact_adapter is not None:
            line_rows = self._line_impact_cache.get(mutant.line_no)
            if line_rows is None:
                line_rows = self._impact_adapter.select_tests(
                    self._campaign_source_rel,
                    self._campaign_function_id,
                    mutant.line_no,
                )
                self._line_impact_cache[mutant.line_no] = line_rows
            dynamic_impact_available = bool(line_rows)
            groups.append(
                (
                    "line-impact",
                    1.0,
                    [(str(item["nodeid"]), str(item.get("reason", "line-impact"))) for item in line_rows],
                    line_rows,
                )
            )
            if self._function_impact_cache is None:
                self._function_impact_cache = self._impact_adapter.select_tests(
                    self._campaign_source_rel,
                    self._campaign_function_id,
                )
            function_rows = self._function_impact_cache
            dynamic_impact_available = dynamic_impact_available or bool(function_rows)
            groups.append(
                (
                    "function-impact",
                    0.85,
                    [(str(item["nodeid"]), str(item.get("reason", "function-impact"))) for item in function_rows],
                    function_rows,
                )
            )
        if self._context_selection_cache is None:
            self._context_selection_cache = _context_tests(
                self._context_map,
                self._campaign_function_id or self._campaign_source_rel,
                self._campaign_source_rel,
            )
        context_rows = self._context_selection_cache
        groups.append(("context-map", 0.65, context_rows, ()))
        if self._static_dependency_cache is None:
            self._static_dependency_cache = _static_dependency_tests(index, self._campaign_source_rel)
        if self._static_selection_cache is None:
            function_name = self._campaign_function_info.get("name") if self._campaign_function_info else self.config.function
            self._static_selection_cache = _static_related_tests(index, self._campaign_source_rel, function_name)
        if self._impact_adapter is None or dynamic_impact_available or context_rows:
            groups.append(("static-dependency", 0.55, self._static_dependency_cache, ()))
            groups.append(("static-fallback", 0.35, self._static_selection_cache, ()))
        groups.append(
            (
                "selection-snapshot",
                0.9,
                [(nodeid, "selection-snapshot") for nodeid in level.nodeids],
                (),
            )
        )
        for source, confidence, pairs, impact_rows in groups:
            if not pairs:
                continue
            ranked = _rank_pairs(pairs, impact_rows=impact_rows)
            valid, invalid = validate_test_nodeids(
                index,
                self.root,
                (item[0] for item in ranked),
                context=self._nodeid_validation_context,
            )
            dropped.extend(invalid)
            if not valid:
                continue
            valid_set = set(valid)
            selected_pairs = tuple(item for item in ranked if item[0] in valid_set)
            impact_stats = {
                str(row.get("nodeid")): row
                for row in impact_rows
                if isinstance(row, dict) and row.get("nodeid")
            }
            scores = {
                nodeid: round(_selection_score(nodeid, reason, impact_stats.get(nodeid)), 4)
                for nodeid, reason in selected_pairs
            }
            reasons: dict[str, list[str]] = {}
            for nodeid, reason in selected_pairs:
                reasons.setdefault(nodeid, []).append(reason)
            evidence = {
                nodeid: self._selection_evidence_for(
                    nodeid,
                    reasons[nodeid],
                    impact_rows=impact_rows,
                )
                for nodeid, _ in selected_pairs
            }
            proof_level = {
                "line-impact": "exact",
                "function-impact": "strong",
                "context-map": "strong",
                "static-dependency": "candidate",
                "static-fallback": "insufficient",
                "selection-snapshot": "exact",
                "frozen-selection": "exact",
            }.get(source, "insufficient")
            proof_sources = tuple(
                sorted({item.source for rows in evidence.values() for item in rows})
            )
            selection = MutantSelection(
                mutant_id=mutant.mutant_id,
                line_no=mutant.line_no,
                nodeids=tuple(nodeid for nodeid, _ in selected_pairs),
                source=source,
                confidence=confidence,
                reasons=reasons,
                scores=scores,
                dropped_nodeids=tuple(dict.fromkeys(dropped)),
                evidence=evidence,
                proof_level=proof_level,
                proof_sources=proof_sources,
                requires_escalation=source not in {"frozen-selection", "selection-snapshot"},
                candidate_count=len(ranked),
                fallback_reason=None,
            )
            self._line_selection_cache[mutant.line_no] = selection
            return selection
        selection = MutantSelection(
            mutant_id=mutant.mutant_id,
            line_no=mutant.line_no,
            nodeids=(),
            source="domain-fallback",
            confidence=0.0,
            reasons={"__selection__": ["domain-fallback"]},
            dropped_nodeids=tuple(dict.fromkeys(dropped)),
            evidence={
                "__selection__": self._selection_evidence_for(
                    "__selection__",
                    ["domain-fallback"],
                )
            },
            proof_level="insufficient",
            proof_sources=("domain_fallback",),
            requires_escalation=True,
            candidate_count=0,
            fallback_reason="no justified test impact evidence",
        )
        self._line_selection_cache[mutant.line_no] = selection
        return selection
    def _selection_evidence_for(
        self,
        nodeid: str,
        reasons: Sequence[str],
        *,
        impact_rows: Sequence[dict[str, Any]] = (),
    ) -> tuple[SelectionEvidence, ...]:
        # Build deterministic per-mutant provenance without re-reading campaign inputs.
        snapshot = self._campaign_selection
        inherited = snapshot.evidence.get(nodeid, ()) if snapshot is not None else ()
        source_snapshot = snapshot.snapshot_id if snapshot is not None else ""
        revision = snapshot.index_version if snapshot is not None and snapshot.index_version else ""
        environment = ""
        if inherited:
            source_snapshot = inherited[0].source_snapshot or source_snapshot
            revision = inherited[0].revision or revision
            environment = inherited[0].environment
        if not environment:
            environment = stable_hash(
                {
                    "source": self._campaign_source_rel,
                    "revision": revision,
                }
            )[:20]
        if reasons == ["frozen-selection"] and inherited:
            return inherited
        rows = [
            make_selection_evidence(
                reason,
                source_snapshot=source_snapshot,
                revision=revision,
                environment=environment,
                detail=reason,
            )
            for reason in reasons
        ]
        if any(int(row.get("kills", 0) or 0) > 0 for row in impact_rows if isinstance(row, dict)):
            rows.append(
                make_selection_evidence(
                    "historical-kill",
                    source_snapshot=source_snapshot,
                    revision=revision,
                    environment=environment,
                    detail="impact_kills>0",
                )
            )
        return tuple(dict.fromkeys(rows))
    def _uses_frozen_selection(self) -> bool:
        # Treat an explicitly supplied snapshot or successfully selected file as immutable input.
        if self.config.selection_snapshot is not None:
            return True
        if self.config.selected_tests_file is None or self._campaign_selection is None:
            return False
        levels = self._campaign_selection.levels
        return bool(levels and "selected-tests-file" in levels[0].reason)
    def _build_frozen_mutant_selection(self, mutant: Mutant, level: LevelSpec) -> MutantSelection:
        # Validate and rank frozen candidates without touching impact, context or static fallbacks.
        pairs = tuple((nodeid, "frozen-selection") for nodeid in level.nodeids)
        ranked = _rank_pairs(pairs)
        valid, invalid = validate_test_nodeids(
            self._campaign_index or {},
            self.root,
            (item[0] for item in ranked),
            context=self._nodeid_validation_context,
        )
        valid_set = set(valid)
        selected_pairs = tuple(item for item in ranked if item[0] in valid_set)
        reasons = {nodeid: [reason] for nodeid, reason in selected_pairs}
        scores = {
            nodeid: round(_selection_score(nodeid, reason), 4)
            for nodeid, reason in selected_pairs
        }
        evidence = {
            nodeid: self._selection_evidence_for(nodeid, ["frozen-selection"])
            for nodeid, _ in selected_pairs
        }
        return MutantSelection(
            mutant_id=mutant.mutant_id,
            line_no=mutant.line_no,
            nodeids=tuple(nodeid for nodeid, _ in selected_pairs),
            source="frozen-selection",
            confidence=1.0,
            reasons=reasons,
            scores=scores,
            dropped_nodeids=tuple(dict.fromkeys(invalid)),
            evidence=evidence,
            proof_level="exact",
            proof_sources=("manual",),
            requires_escalation=False,
            candidate_count=len(ranked),
            fallback_reason=None,
        )
    def _level_for_mutant(self, mutant: Mutant, level: LevelSpec) -> tuple[LevelSpec | None, dict[str, Any]]:
        # Build a per-mutant L1 command while preserving explicit user commands.
        if level.name != "L1":
            evidence = ()
            if self._campaign_selection is not None:
                evidence = self._campaign_selection.evidence.get(f"__level__:{level.name}", ())
            return level, {
                "level": level.name,
                "source": level.reason,
                "nodeids": list(level.nodeids),
                "evidence": [item.to_dict() for item in evidence],
            }
        if self.config.test_command_argv is not None:
            return level, {"level": level.name, "source": "explicit-command", "nodeids": list(level.nodeids)}
        selection = self._build_mutant_selection(mutant, level)
        detail = {"level": level.name, **selection.to_dict()}
        if selection.requires_escalation and self.config.no_escalation:
            detail["fallback_reason"] = "safe escalation disabled by configuration"
            return None, detail
        if not selection.nodeids:
            return None, detail
        invocation_started = time.perf_counter()
        python = resolve_python(self.root, self.config.python_executable)
        line_level = SelectionLevel(
            name="L1",
            reason=f"{level.reason};{selection.source}",
            nodeids=selection.nodeids,
            files=tuple(dict.fromkeys(nodeid.split("::", 1)[0] for nodeid in selection.nodeids)),
        )
        raw_command = self._pytest_selection_command(python, line_level)
        command_key = ("mutant",) + tuple(str(item) for item in raw_command)
        prepared_command = self._prepared_selection_commands.get(command_key)
        command_built = prepared_command is None
        if prepared_command is None:
            prepared_command = prepare_pytest_command(raw_command, mode="mutant")
            self._prepared_selection_commands[command_key] = prepared_command
        prepared_level = LevelSpec(
            name=level.name,
            reason=line_level.reason,
            command_argv=prepared_command,
            nodeids=selection.nodeids,
            files=line_level.files,
        )
        self._record_prepared_invocation(invocation_started, command_build=command_built)
        return prepared_level, detail
    def _run_mutant_level(
        self, mutant: Mutant, level: LevelSpec, report_id: str, target_sha256: str
    ) -> tuple[ProcessResult, str | None, list[str]]:
        # Execute one mutant level, retrying timeouts while preserving test journals.
        artifact_path = self._artifact_path(report_id, mutant.mutant_id, level.name)
        try:
            process = self._run_test_command(
                level.command_argv,
                phase="mutant",
                level=level.name,
                mutant_id=mutant.mutant_id,
                target_sha256=target_sha256,
                report_id=report_id,
                output_artifact=artifact_path,
                timeout_seconds=self.config.timeout_seconds,
            )
        except OSError as exc:
            process = ProcessResult(level.command_argv, str(self.root), 127, 0.0, False, str(exc))
            return process, "error", []
        self.performance.pytest_seconds += process.elapsed_seconds
        artifacts = [self._relative_artifact(artifact_path)] if artifact_path.exists() else []
        if process.process_tree_leak:
            return process, "process_tree_leak", artifacts
        if process.timed_out:
            retry_command = without_parallelism(level.command_argv)
            retry_artifact_path = self._artifact_path(report_id, mutant.mutant_id, f"{level.name}_retry")
            try:
                retry = self._run_test_command(
                    retry_command,
                    phase="mutant",
                    level=level.name,
                    mutant_id=mutant.mutant_id,
                    target_sha256=target_sha256,
                    report_id=report_id,
                    output_artifact=retry_artifact_path,
                    timeout_seconds=self.config.timeout_seconds * self.config.timeout_retry_factor,
                    retry=True,
                )
            except OSError as exc:
                retry = ProcessResult(retry_command, str(self.root), 127, 0.0, False, str(exc), retry=True)
            self.performance.pytest_seconds += retry.elapsed_seconds
            if retry_artifact_path.exists():
                artifacts.append(self._relative_artifact(retry_artifact_path))
            if retry.process_tree_leak:
                return retry, "process_tree_leak", artifacts
            if retry.timed_out:
                return retry, "mutant_induced_timeout", artifacts
            retry_classification = classify_pytest_exit(retry.exit_code)
            if retry_classification == "passed":
                return retry, "infra_timeout", artifacts
            if retry_classification == "tests_failed" and not looks_like_infrastructure_failure(
                retry.output,
                infrastructure_flags=retry.infrastructure_flags,
            ):
                return retry, None, artifacts
            if retry_classification == "no_tests_collected":
                return retry, "no_tests_collected", artifacts
            return retry, "error", artifacts
        classification = classify_pytest_exit(process.exit_code)
        if process.exit_code != 0 and (
            looks_like_infrastructure_failure(
                process.output,
                infrastructure_flags=process.infrastructure_flags,
            )
            or classification in {"no_tests_collected", "interrupted", "internal_error", "usage_error", "spawn_error", "unknown"}
        ):
            if classification == "no_tests_collected":
                return process, "no_tests_collected", artifacts
            return process, "error", artifacts
        return process, None, artifacts
    def _selection_audit(
        self,
        results: Sequence[dict[str, Any]],
        snapshot: SourceSnapshot,
        level: LevelSpec,
        report_id: str,
        manifest_path: Path,
    ) -> list[dict[str, Any]]:
        # Re-run a bounded sample with the same prepare-and-recover protocol as the campaign.
        candidates = [item for item in results if item.get("status") == "killed"]
        if not candidates:
            return []
        count = max(1, round(len(candidates) * self.config.audit_percent / 100.0))
        audited: list[dict[str, Any]] = []
        for item in sorted(candidates, key=lambda value: str(value.get("mutant", {}).get("mutant_id", "")))[:count]:
            mutant_id = str(item.get("mutant", {}).get("mutant_id", ""))
            mutant = self._mutant_by_id.get(mutant_id)
            if not mutant:
                continue
            row: dict[str, Any] = {"mutant_id": mutant_id, "level": level.name, "l1_status": "killed"}
            mutated_hash: str | None = None
            try:
                before = current_sha256(snapshot)
                prepared = prepare_mutant(snapshot, mutant)
                self._activate_prepared_mutant(manifest_path, snapshot, prepared)
                mutated_hash = apply_prepared_mutant(
                    snapshot,
                    prepared,
                    current_sha256_value=before,
                    durability="normal",
                    metrics=self.performance,
                )
                artifact_path = self._artifact_path(report_id, mutant_id, "selection_audit_L3")
                process = self._run_test_command(
                    level.command_argv,
                    phase="audit",
                    level=level.name,
                    mutant_id=mutant_id,
                    target_sha256=mutated_hash,
                    report_id=report_id,
                    output_artifact=artifact_path,
                    timeout_seconds=self.config.timeout_seconds,
                )
                self.performance.pytest_seconds += process.elapsed_seconds
                row.update(
                    {
                        "result": process.to_dict(),
                        "output_artifact": self._relative_artifact(artifact_path),
                        "disagreement": process.passed,
                    }
                )
            except (OSError, RestoreError) as exc:
                row.update({"error": str(exc), "disagreement": True})
            finally:
                try:
                    restore_snapshot(
                        snapshot,
                        expected_sha256=mutated_hash,
                        durability="normal",
                        metrics=self.performance,
                    )
                except RestoreError as exc:
                    row.update({"restore_error": str(exc), "disagreement": True})
            audited.append(row)
        return audited
    def _artifact_path(self, report_id: str, mutant_id: str, level: str) -> Path:
        name = f"{_safe_name(report_id)}__{_safe_name(mutant_id)}__{_safe_name(level)}.txt"
        return self.artifacts_dir / name
    def _relative_artifact(self, path: Path) -> str:
        return str(path.resolve().relative_to(self.reports_dir).as_posix())
    def _write_output(self, report_id: str, mutant_id: str, level: str, output: str) -> str:
        """Legacy helper retained for non-subprocess diagnostic paths."""
        path = self._artifact_path(report_id, mutant_id, level)
        atomic_write_text(
            path,
            output,
            durability="normal",
            category="baseline_artifact",
            metrics=self.performance,
        )
        return self._relative_artifact(path)
    def _write_report(self, report_path: Path, report: dict[str, Any]) -> None:
        # Materialize one bounded report artifact and time its serialization boundary.
        started = time.perf_counter()
        atomic_write_json(
            report_path,
            report,
            durability="normal",
            category="report",
            metrics=self.performance,
        )
        elapsed = time.perf_counter() - started
        self.performance.report_write_seconds += elapsed
        self.performance.report_materialization_seconds += elapsed
    def _append_result(
        self,
        result: MutantResult,
        *,
        attempt: int = 0,
        lease_id: str | None = None,
        worker_id: str | None = None,
        test_observations: Sequence[Mapping[str, Any]] = (),
    ) -> dict[str, Any] | None:
        # Append one completion event with its execution identity and advance the digest in O(1).
        if self._results_path is None:
            return None
        payload = result.to_dict()
        run_id = self._test_stats_run_id or self._results_path.stem
        mutant = payload.get("mutant") if isinstance(payload.get("mutant"), dict) else {}
        mutant_id = str(mutant.get("mutant_id", ""))
        normalized_attempt = max(0, int(attempt))
        payload.update(
            {
                "event_type": "mutant_completed",
                "event_schema_version": 2,
                "execution_id": stable_hash(
                    {"run_id": run_id, "mutant_id": mutant_id, "attempt": normalized_attempt}
                )[:32],
                "attempt": normalized_attempt,
                "lease_id": lease_id,
                "worker_id": worker_id,
                "event_id": result_event_id(run_id, payload),
                "test_observations": [dict(item) for item in test_observations],
            }
        )
        encoded = (json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        append_json_line(
            self._results_path,
            payload,
            durability="normal",
            category="result_journal",
            metrics=self.performance,
        )
        self._results_journal_digest.update(encoded)
        self._results_journal_size += len(encoded)
        self._completed_identity_digests.update(result_identity_digests((payload,)))
        if mutant_id:
            self._completed_mutant_ids.add(mutant_id)
        return payload
    def _read_results_journal(self) -> list[dict[str, Any]]:
        # Materialize deduplicated result rows only for explicit features such as selection audit.
        if self._results_path is None or not self._results_path.exists():
            return []
        return list(replay_result_journal(self._results_path).rows)
    def _write_state(
        self,
        status: str,
        results: Sequence[dict[str, Any]],
        *,
        force: bool = False,
    ) -> None:
        # Persist sparse checkpoints while forcing every terminal or error boundary.
        if self._state_path is None:
            return
        completed_mutants = self._campaign_accumulator.total_results
        now = time.monotonic()
        if (
            not force
            and completed_mutants
            and completed_mutants - self._last_state_checkpoint_mutants < STATE_CHECKPOINT_MUTANTS
            and now - self._last_state_checkpoint_at < STATE_CHECKPOINT_SECONDS
        ):
            return
        counts = dict(self._campaign_accumulator.counts)
        if not completed_mutants:
            completed_mutants = len(results)
            counts = dict(Counter(str(item.get("status", "error")) for item in results))
        if self._results_path is not None:
            journal_info = {
                "journal_size": self._results_journal_size,
                "journal_sha256": self._results_journal_digest.hexdigest(),
            }
        else:
            journal_info = journal_metadata(None)
        checkpoint_id = stable_hash(
            {
                "run_id": self._test_stats_run_id,
                "status": status,
                "completed_mutants": completed_mutants,
                **journal_info,
            }
        )[:32]
        state = {
            "checkpoint_schema_version": 2,
            "checkpoint_id": checkpoint_id,
            "status": status,
            "completed_mutants": completed_mutants,
            "total_mutants": len(self._mutant_by_id),
            "counts": dict(counts),
            "results_journal": str(self._results_path) if self._results_path else None,
            "journal_offset": journal_info["journal_size"],
            "last_sequence": completed_mutants,
            "completed_execution_ids": [
                identity.split(":", 1)[1]
                for identity in sorted(self._completed_identity_digests)
                if identity.startswith("execution:")
            ],
            "completed_mutant_ids": sorted(self._completed_mutant_ids),
            "completed_identity_digests": dict(self._completed_identity_digests),
            "accumulator": self._campaign_accumulator.checkpoint_dict(),
            **journal_info,
            "performance": self._performance_payload(),
        }
        atomic_write_json(
            self._state_path,
            state,
            durability="normal",
            category="state_checkpoint",
            metrics=self.performance,
        )
        self._last_state_checkpoint_mutants = completed_mutants
        self._last_state_checkpoint_at = now
    def _record_campaign_result(self, result: dict[str, Any]) -> None:
        # Feed one completed mutant into the O(1) report accumulator.
        self._campaign_accumulator.add_result(result)
        mutant = result.get("mutant")
        if isinstance(mutant, dict) and mutant.get("mutant_id"):
            self._completed_mutant_ids.add(str(mutant["mutant_id"]))
    def _metrics(
        self,
        results: Sequence[dict[str, Any]] | None = None,
        *,
        refresh_health: bool = False,
    ) -> dict[str, Any]:
        # Materialize report metrics from accumulated counters and refresh health only on demand.
        if results is None:
            accumulator = self._campaign_accumulator
        elif len(results) == self._campaign_accumulator.total_results:
            accumulator = self._campaign_accumulator
        elif self.config.compact_report and self._campaign_accumulator.total_results:
            accumulator = self._campaign_accumulator
        else:
            accumulator = CampaignAccumulator()
            for item in results:
                accumulator.add_result(item)
        if refresh_health:
            health_started = time.perf_counter()
            self._health_summary_cache = summarize_test_health(
                stats_db_path(self.reports_dir),
                project_root=self.root,
            )
            self.performance.health_aggregation_seconds += time.perf_counter() - health_started
        payload = accumulator.metric_payload()
        payload.update(
            {
            "test_stats": {
                "db_path": str(stats_db_path(self.reports_dir)),
                "event_dir": str(self._test_stats_event_dir) if self._test_stats_event_dir else None,
                "health": dict(self._health_summary_cache),
                **self._test_stats_summary,
            },
                "performance": self._performance_payload(),
            }
        )
        return payload
    def _finish_report(self, report_path: Path, report: dict[str, Any], manifest_path: Path, *, status: str) -> None:
        # Delegate report finalization through the extracted service port.
        from .campaign_finalization_service import CampaignFinalizationService
        CampaignFinalizationService(self).finalize(report_path, report, manifest_path, status=status)
    def _finish_report_impl(self, report_path: Path, report: dict[str, Any], manifest_path: Path, *, status: str) -> None:
        # Restore first, materialize artifacts second, and publish terminal manifest status last.
        final_status = status
        finalization_error: str | None = None
        manifest: dict[str, Any] | None = None
        try:
            value = read_json(manifest_path)
            if not isinstance(value, dict):
                raise ValueError(f"recovery manifest is not an object: {manifest_path}")
            manifest = value
            if self._campaign_snapshot is not None:
                restore_snapshot(
                    self._campaign_snapshot,
                    expected_sha256=None,
                    durability="critical",
                    category="recovery",
                    metrics=self.performance,
                )
            manifest["status"] = "restored" if status != "restore_error" else "restore_error"
            manifest["phase"] = "materializing" if status != "restore_error" else "restore_failed"
            if status != "restore_error":
                manifest["active_mutant"] = None
                manifest["expected_hashes"] = [str(manifest.get("original_sha256"))]
            atomic_write_json(
                manifest_path,
                manifest,
                durability="critical",
                category="manifest",
                metrics=self.performance,
            )
        except (OSError, RestoreError, ValueError, KeyError, TypeError) as exc:
            finalization_error = str(exc)
            if isinstance(exc, RestoreError):
                final_status = "restore_error"
            elif status != "restore_error":
                final_status = "finalization_error"
        report["status"] = final_status
        if finalization_error:
            report["finalization_error"] = finalization_error
            prefix = str(report.get("error") or "").strip()
            suffix = f"recovery manifest finalization failed: {finalization_error}"
            report["error"] = f"{prefix}; {suffix}" if prefix else suffix
        try:
            report["metrics"] = self._metrics(refresh_health=True)
            self._write_report(report_path, report)
            markdown = MutationRunner._markdown(report)
            atomic_write_text(
                report_path.with_suffix(".md"),
                markdown,
                durability="normal",
                category="report",
                metrics=self.performance,
            )
            self._write_state(final_status, report.get("results", []), force=True)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            finalization_error = finalization_error or str(exc)
            if status != "restore_error":
                final_status = "finalization_error"
            report["status"] = final_status
            report["finalization_error"] = finalization_error
            prefix = str(report.get("error") or "").strip()
            suffix = f"report finalization failed: {finalization_error}"
            report["error"] = f"{prefix}; {suffix}" if prefix else suffix
        if manifest is not None and finalization_error is None:
            try:
                manifest["phase"] = "ready_to_commit"
                manifest["status"] = "ready_to_commit"
                atomic_write_json(
                    manifest_path,
                    manifest,
                    durability="critical",
                    category="manifest",
                    metrics=self.performance,
                )
                manifest["status"] = final_status
                manifest["phase"] = "completed"
                manifest["finished_at"] = utc_now_iso()
                atomic_write_json(
                    manifest_path,
                    manifest,
                    durability="critical",
                    category="manifest",
                    metrics=self.performance,
                )
            except (OSError, ValueError, KeyError, TypeError) as exc:
                finalization_error = str(exc)
                if status != "restore_error":
                    final_status = "finalization_error"
                report["status"] = final_status
                report["finalization_error"] = finalization_error
                prefix = str(report.get("error") or "").strip()
                suffix = f"recovery manifest commit failed: {finalization_error}"
                report["error"] = f"{prefix}; {suffix}" if prefix else suffix
        if finalization_error:
            try:
                self._write_report(report_path, report)
            except (OSError, ValueError, KeyError, TypeError):
                pass
            try:
                self._write_state(final_status, report.get("results", []), force=True)
            except (OSError, ValueError, KeyError, TypeError):
                pass
        self._close_test_stats_connection()
        if self._impact_adapter is not None:
            self._impact_adapter.close()
    @staticmethod
    def _markdown(report: dict[str, Any]) -> str:
        # Keep compact reports readable without materializing their result journal.
        metrics = report.get("metrics", {})
        lines = [
            f"# Mutation run {report.get('run_id', '')}",
            "",
            f"- Status: `{report.get('status')}`",
            f"- Target: `{report.get('target', {}).get('source_path')}` / `{report.get('target', {}).get('function_id')}`",
            f"- Mutants: {metrics.get('total_mutants', 0)}",
            f"- Mutation score: {metrics.get('mutation_score')}",
            f"- Selection escapes: {metrics.get('selection_escapes', 0)}",
            f"- Selection precision: {metrics.get('selection_precision')}",
            "",
            "## Results",
            "",
            "| Mutant | Status | Reason | Restored |",
            "|---|---|---|---|",
        ]
        if report.get("compact_report"):
            return "\n".join(
                lines[: lines.index("## Results")]
                + [
                    "## Results",
                    "",
                    "Result rows are stored in the append-only journal:",
                    f"`{report.get('results_journal', '')}`",
                    "",
                ]
            )
        for item in report.get("results", []):
            mutant = item.get("mutant", {}).get("mutant_id", "")
            lines.append(
                f"| `{mutant}` | `{item.get('status')}` | {item.get('classification_reason', '')} | {item.get('restore_verified')} |"
            )
        return "\n".join(lines) + "\n"
def recover_from_manifest(path: Path, *, force: bool = False) -> str:
    return recover_manifest(path, force=force)
