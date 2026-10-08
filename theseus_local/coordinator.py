"""First local end-to-end coordinator for Theseus and the Gallifrey mutation domain."""
from __future__ import annotations
import asyncio
import ast
import hashlib
import importlib.metadata
import importlib.util
import json
import sqlite3
import os
import tomllib
import time
from contextlib import nullcontext
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from fnmatch import fnmatchcase
from threading import Event, RLock, Thread
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from theseus_contracts import (
    CampaignConfiguration,
    CampaignId,
    CampaignPlan,
    CampaignResult,
    CampaignStatus,
    CampaignSummary,
    ExecutionAcknowledged,
    ExecutionDelivery,
    ExecutionId,
    ExecuteShardRequest,
    FinalizeCampaignRequest,
    HeartbeatReceipt,
    MutationDiscoveryResult,
    MutantDescriptor,
    MutantExecutionResult,
    MutantId,
    PreparedCampaign,
    PreparedCampaignSnapshot,
    RegisterWorker,
    ShardDescriptor,
    ShardExecutionResult,
    ShardId,
    ShardLease,
    ShardAssignment,
    TestLevelPlan,
    WorkerId,
    WorkerHeartbeat,
    WorkerHeartbeatFrame,
    WorkerMessageType,
    WorkerProtocolFrame,
    WorkerTerminated,
    WorkerStatus,
    deterministic_id,
    normalize_reuse_mode,
)
from theseus_contracts.serialization import utc_now
from gallifrey_mutation import (
    CampaignState,
    MutationShardState,
    MutationCampaign,
    MutationExecution,
    MutationCampaignService,
    MutationShard,
    SQLiteMutationStore,
    Success,
)
from .process import EngineProcessSession
from .finalization import (
    ArtifactSource,
    build_finalization_intent,
    cleanup_finalization_staging,
    publish_finalization_intent,
    validate_registered_artifacts,
)
from .canonical_report import (
    build_canonical_report,
    canonical_json_text,
    canonical_markdown,
)
from .workspace import WorkspaceProvider
from .worker_pool import (
    WorkerRegistryProjection,
    materialize_prepared_snapshot,
    prepare_worker_workspace,
    worker_workspace_backend,
)
from .worker_runtime import (
    DurableExecutionSpool,
    PersistentWorkerProcess,
    quarantine_non_current_deliveries,
    terminate_recorded_process,
)
from .runtime_identity import (
    RuntimeCompatibilityError,
    RuntimeIdentity,
    assert_runtime_compatible,
    current_runtime_identity,
)
from .startup_recovery import (
    StartupRecoveryLock,
    replay_finalization,
    replay_pending_deliveries,
    scan_campaign_databases,
    unfinished_campaign_configurations,
    validate_campaign_plan_topology,
    write_recovery_report,
)
from theseus_planner import CampaignPlanner, PlanError
from theseus_statistics import (
    STATISTICS_PROJECTION_MAX_BATCH,
    StatisticsEventStore,
    StatisticsProjectionStore,
    project_plan_decision_outbox,
)
from test_intelligence_unified_v1.test_stats import (
    canonical_nodeid,
    load_runtime_dependency_manifest,
    stats_db_path,
)
from test_intelligence_unified_v1.io_utils import atomic_write_json, atomic_write_text, read_json, stable_hash
from test_intelligence_unified_v1.mutations import recover_manifest
from test_intelligence_unified_v1.recovery import current_process_birth_token, inspect_campaign_recovery
from theseus_knowledge import (
    KnowledgePlaneStore,
    ReuseDecision,
    ReuseKind,
    ReuseMetrics,
    ReusePlanArtifact,
    ReuseAuditPolicy,
    compare_reuse_audit,
    fingerprint_conftest,
    fingerprint_environment,
    fingerprint_evidence_identity,
    fingerprint_function,
    fingerprint_mutant,
    fingerprint_test,
)
from theseus_performance.sqlite_metrics import SQLiteMetrics
def _file_sha256(path: Path) -> str:
    # Hash one dependency file without retaining its contents in the coordinator.
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
_COORDINATOR_TIMELINE_VERSION = "coordinator-exclusive-v1"
class _CoordinatorTimeline:
    """Accumulate one non-overlapping coordinator wall-clock timeline."""
    def __init__(self, campaign_id: str, *, clock: Callable[[], float] = time.perf_counter) -> None:
        # Start one monotonic timeline without importing the benchmark runtime into the coordinator.
        self.campaign_id = str(campaign_id)
        self._clock = clock
        self._started = float(clock())
        self._last = self._started
        self._active_phase: str | None = None
        self._phase_seconds: dict[str, float] = {}
        self._diagnostics: dict[str, float] = {}
        self._finished = False
    def switch(self, phase: str) -> None:
        # Close the current exclusive interval and start the requested coordinator phase.
        if self._finished:
            raise RuntimeError("coordinator timeline is already finished")
        normalized = str(phase).strip()
        if not normalized:
            raise ValueError("coordinator timeline phase must be non-empty")
        now = float(self._clock())
        if self._active_phase is not None:
            self._phase_seconds[self._active_phase] = (
                self._phase_seconds.get(self._active_phase, 0.0)
                + max(0.0, now - self._last)
            )
        self._active_phase = normalized
        self._last = now
    def observe_diagnostic(self, name: str, value: float) -> None:
        # Store one non-exclusive diagnostic beside the reconciled coordinator timeline.
        normalized = str(name).strip()
        if not normalized:
            raise ValueError("coordinator diagnostic name must be non-empty")
        self._diagnostics[normalized] = max(0.0, float(value))
    def finish(self, path: Path, *, status: str, error: str | None = None) -> dict[str, Any]:
        # Reconcile exclusive phases with total wall time and publish one durable diagnostic artifact.
        if self._finished:
            raise RuntimeError("coordinator timeline is already finished")
        now = float(self._clock())
        if self._active_phase is not None:
            self._phase_seconds[self._active_phase] = (
                self._phase_seconds.get(self._active_phase, 0.0)
                + max(0.0, now - self._last)
            )
        total = max(0.0, now - self._started)
        observed = sum(self._phase_seconds.values())
        residual = max(0.0, total - observed)
        phases = [
            {
                "phase": phase,
                "wall_seconds": seconds,
                "source": "coordinator.perf_counter",
            }
            for phase, seconds in self._phase_seconds.items()
        ]
        phases.append(
            {
                "phase": "unattributed_residual",
                "wall_seconds": residual,
                "source": "coordinator.reconciliation",
            }
        )
        payload = {
            "schema_version": 1,
            "timeline_version": _COORDINATOR_TIMELINE_VERSION,
            "campaign_id": self.campaign_id,
            "status": str(status),
            "exclusive": True,
            "total_wall_seconds": total,
            "observed_phase_seconds": observed,
            "residual_seconds": residual,
            "accounted_seconds": observed + residual,
            "accounting_error_seconds": abs(total - observed - residual),
            "phases": phases,
            "diagnostics": dict(sorted(self._diagnostics.items())),
            "error": str(error) if error else None,
        }
        atomic_write_json(path, payload, durability="normal", category="report")
        self._finished = True
        return payload
class _FingerprintSnapshot:
    """Caches immutable fingerprint inputs for one coordinator planning boundary."""
    def __init__(self, root: Path) -> None:
        # Keep every read, parse and resolution result local to one campaign planning pass.
        self.root = root.resolve()
        self.text_cache: dict[Path, tuple[bool, str]] = {}
        self.tree_cache: dict[Path, ast.Module | None] = {}
        self.source_tree_cache: dict[str, ast.Module | None] = {}
        self.sha256_cache: dict[Path, str] = {}
        self.module_cache: dict[tuple[Path, str], Path | None] = {}
        self.external_plugin_cache: dict[str, str | None] = {}
        self.local_closure_cache: dict[Path, tuple[tuple[tuple[str, str], ...], bool]] = {}
        self.conftest_paths_cache: dict[Path, tuple[Path, ...]] = {}
        self.conftest_closure_cache: dict[Path, tuple[tuple[str, str], ...]] = {}
        self.node_source_cache: dict[str, tuple[str, Path]] = {}
        self.environment_cache: dict[tuple[str, tuple[Any, ...]], tuple[tuple[str, ...], tuple[str, ...]]] = {}
        self.raw_data_cache: dict[tuple[Path, str], tuple[str, ...]] = {}
        self.declared_data_cache: dict[
            tuple[tuple[str, ...], tuple[str, ...], int],
            tuple[tuple[tuple[str, str], ...], tuple[str, ...]],
        ] = {}
        self.dynamic_python_rows: tuple[tuple[str, str], ...] | None = None
    @staticmethod
    def _parse_source(source: str) -> ast.Module | None:
        # Parse one immutable source string and retain syntax failure as explicit uncertainty.
        try:
            return ast.parse(source)
        except SyntaxError:
            return None
    def read_text(self, path: Path) -> tuple[bool, str]:
        # Read one path at most once for the complete planning snapshot.
        resolved = path.resolve()
        cached = self.text_cache.get(resolved)
        if cached is not None:
            return cached
        try:
            value = (True, resolved.read_text(encoding="utf-8"))
        except (OSError, UnicodeError):
            value = (False, "unreadable")
        self.text_cache[resolved] = value
        return value
    def tree(self, path: Path) -> ast.Module | None:
        # Parse one readable path at most once for all test and dependency consumers.
        resolved = path.resolve()
        if resolved in self.tree_cache:
            return self.tree_cache[resolved]
        readable, source = self.read_text(resolved)
        tree = self._parse_source(source) if readable else None
        self.tree_cache[resolved] = tree
        return tree
    def tree_for_source(self, path: Path, source: str) -> ast.Module | None:
        # Reuse the path tree when source matches, otherwise parse one fallback source once.
        readable, current = self.read_text(path)
        if readable and current == source:
            return self.tree(path)
        if source not in self.source_tree_cache:
            self.source_tree_cache[source] = self._parse_source(source)
        return self.source_tree_cache[source]
    def file_sha256(self, path: Path) -> str:
        # Hash one immutable data dependency at most once during planning.
        resolved = path.resolve()
        if resolved not in self.sha256_cache:
            self.sha256_cache[resolved] = _file_sha256(resolved)
        return self.sha256_cache[resolved]
    def python_fallback_rows(self) -> tuple[tuple[str, str], ...]:
        # Scan the fail-closed dynamic Python fallback only once per campaign snapshot.
        if self.dynamic_python_rows is not None:
            return self.dynamic_python_rows
        rows: dict[str, str] = {}
        for candidate in self.root.rglob("*.py"):
            if any(
                part in {".git", ".theseus", ".pytest_cache", "__pycache__", ".venv", "venv", "reports"}
                for part in candidate.parts
            ):
                continue
            readable, source = self.read_text(candidate)
            if readable:
                rows[candidate.relative_to(self.root).as_posix()] = stable_hash(source)
            else:
                rows[str(candidate.resolve())] = "unreadable"
        self.dynamic_python_rows = tuple(sorted(rows.items()))
        return self.dynamic_python_rows
    @staticmethod
    def environment_descriptor_key(descriptor: Any) -> tuple[Any, ...]:
        # Canonicalize only the environment fields that affect dependency admission.
        if descriptor is None:
            return (None,)
        return (
            str(getattr(descriptor, "inherit_policy", "allowlisted") or "allowlisted").lower(),
            tuple(str(item) for item in getattr(descriptor, "declared_env_keys", ()) if str(item)),
            tuple(str(item) for item in getattr(descriptor, "tracked_variables", ()) if str(item)),
            tuple(str(item) for item in getattr(descriptor, "declared_env_patterns", ()) if str(item)),
            tuple(str(item) for item in getattr(descriptor, "tracked_prefixes", ()) if str(item)),
            tuple(str(item) for item in getattr(descriptor, "ignored_variables", ()) if str(item)),
        )
@dataclass(frozen=True, slots=True)
class LocalCampaignResult:
    """Durable paths and terminal projections returned by one local campaign run."""
    campaign: MutationCampaign
    engine_result: CampaignResult | None
    database_path: Path
    events_path: Path
    protocol_path: Path
    stdout_path: Path
    stderr_path: Path
    error: str | None = None
    @property
    def succeeded(self) -> bool:
        # Treat only a completed domain campaign with a materialized engine result as success.
        return self.error is None and self.campaign.status == CampaignState.COMPLETED and self.engine_result is not None
class LocalCampaignCoordinator:
    """Coordinates preparation, persistent worker agents and one SQLite-backed campaign."""
    def __init__(self, *, process_command: tuple[str, ...] | None = None) -> None:
        # Allow tests and future packaged launchers to replace only the child executable.
        self.process_command = process_command
    @staticmethod
    def cancel_path(configuration: CampaignConfiguration) -> Path:
        # Resolve the external cancellation marker shared by a running coordinator and its process session.
        return LocalCampaignCoordinator._reports_dir(configuration) / "cancel.requested"
    @staticmethod
    def request_cancel(configuration: CampaignConfiguration) -> Path:
        # Publish an atomic cancellation request without touching the main checkout.
        path = LocalCampaignCoordinator.cancel_path(configuration)
        atomic_write_json(
            path,
            {
                "schema_version": 1,
                "campaign_id": configuration.campaign_id.value,
                "requested_at": utc_now(),
            },
            durability="critical",
            category="report",
        )
        return path
    @staticmethod
    def clear_cancel(configuration: CampaignConfiguration) -> None:
        # Remove only a terminal campaign's own marker so a later resume cannot inherit stale cancellation.
        try:
            LocalCampaignCoordinator.cancel_path(configuration).unlink()
        except FileNotFoundError:
            return
    @staticmethod
    def _reports_dir(configuration: CampaignConfiguration) -> Path:
        # Resolve report artifacts in the external state root without changing campaign IDs.
        return WorkspaceProvider(configuration).reports_root / configuration.campaign_id.value
    @staticmethod
    def _knowledge_path(configuration: CampaignConfiguration) -> Path:
        # Keep project history outside both the checkout and per-campaign control state.
        provider = WorkspaceProvider(configuration)
        return provider.knowledge_root / f"{configuration.project.project_id.value}.sqlite3"
    @staticmethod
    def campaign_database_path(configuration: CampaignConfiguration) -> Path:
        # Resolve the authoritative campaign database without preparing or mutating a workspace.
        provider = WorkspaceProvider(configuration)
        return (provider.reports_root / configuration.campaign_id.value / "campaign.sqlite3").resolve()
    @staticmethod
    def _adaptive_planner_observations(
        statistics_projection: StatisticsProjectionStore,
        catalog: Sequence[MutantDescriptor],
        reuse_decisions: Mapping[str, Any] | None = None,
        *,
        knowledge: KnowledgePlaneStore | None = None,
        project_id: str | None = None,
        runtime_class: str | None = None,
    ) -> dict[str, dict[str, object]]:
        # Merge bounded statistics with the frozen Knowledge Plane decision ledger without N+1 reads.
        mutant_ids = tuple(sorted({item.mutant_id.value for item in catalog}))
        summaries = {}
        for offset in range(0, len(mutant_ids), STATISTICS_PROJECTION_MAX_BATCH):
            summaries.update(
                statistics_projection.get_many(
                    "mutant",
                    mutant_ids[offset:offset + STATISTICS_PROJECTION_MAX_BATCH],
                )
            )
        cost_history: dict[str, dict[str, object]] = {}
        if knowledge is not None:
            try:
                cost_history = knowledge.query_cost_observations(
                    mutant_ids,
                    project_id=project_id,
                    runtime_class=runtime_class,
                )
            except (OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
                cost_history = {}
        observations: dict[str, dict[str, object]] = {}
        for mutant_id in mutant_ids:
            summary = summaries.get(mutant_id)
            decision = (reuse_decisions or {}).get(mutant_id)
            result_status = str(getattr(decision, "result_status", "") or "").lower()
            killed = max(0, int(summary.killed_count)) if summary is not None else 0
            survived = max(0, int(summary.survived_count)) if summary is not None else 0
            invalid = max(0, int(summary.invalid_count)) if summary is not None else 0
            if result_status == "killed":
                killed = max(killed, 1)
            elif result_status == "survived":
                survived = max(survived, 1)
            elif result_status in {"invalid", "invalid_mutant"}:
                invalid = max(invalid, 1)
            terminal = killed + survived
            sample_count = terminal + invalid
            duration_count = int(summary.duration_count) if summary is not None else 0
            cost = cost_history.get(mutant_id, {})
            if sample_count == 0 and duration_count == 0 and not cost:
                continue
            event_count = max(1, int(summary.event_count)) if summary is not None else max(1, sample_count)
            kill_probability = killed / terminal if terminal else 0.5
            observations[mutant_id] = {
                **cost,
                "duration_seconds": (
                    max(
                        max(0.0, float(summary.duration_avg_ms) / 1000.0) if summary is not None else 0.0,
                        float(cost.get("duration_seconds", 0.0) or 0.0),
                    )
                ),
                "duration_p95_seconds": (
                    max(
                        max(0.0, float(summary.duration_p95_ms) / 1000.0) if summary is not None else 0.0,
                        float(cost.get("duration_p95_seconds", 0.0) or 0.0),
                    )
                ),
                "duration_sample_count": max(
                    float(max(0, duration_count)),
                    float(cost.get("duration_sample_count", 0.0) or 0.0),
                ),
                "kill_probability": kill_probability,
                "selection_confidence": min(1.0, sample_count / 5.0),
                "operator_effectiveness": kill_probability,
                "flaky_risk": (
                    min(1.0, max(0, int(summary.flaky_transition_count)) / event_count)
                    if summary is not None
                    else 0.0
                ),
                "infrastructure_risk": (
                    min(
                        1.0,
                        (max(0, int(summary.error_count)) + max(0, int(summary.timeout_count))) / event_count,
                    )
                    if summary is not None
                    else 0.0
                ),
                "sample_count": float(sample_count),
                "killed_count": float(killed),
                "survived_count": float(survived),
                "invalid_count": float(invalid),
            }
        return observations
    @staticmethod
    def _prepared_for_campaign_plan(
        prepared: PreparedCampaign,
        campaign_plan: CampaignPlan,
    ) -> PreparedCampaign:
        # Bind worker inputs to the immutable planner mutant set and escalation ladder.
        selection = prepared.selection
        if selection is not None:
            selection = replace(
                selection,
                levels=tuple(TestLevelPlan.from_dict(item) for item in campaign_plan.levels),
            )
        return replace(
            prepared,
            mutants=tuple(item.mutant for item in campaign_plan.selected),
            selection=selection,
        )
    @staticmethod
    def _dispatch_outbox(
        store: SQLiteMutationStore,
        knowledge: KnowledgePlaneStore,
        legacy_knowledge: KnowledgePlaneStore,
        statistics_store: StatisticsEventStore,
        statistics_projection: StatisticsProjectionStore,
    ) -> int:
        # Project canonical statistics and knowledge before acknowledging durable mutation effects.
        rows = LocalCampaignCoordinator._require(store.list_outbox(undelivered_only=True))
        if not rows:
            return 0
        project_plan_decision_outbox(
            rows,
            event_store=statistics_store,
            projection_store=statistics_projection,
        )
        knowledge.ingest_outbox(rows, namespace_effects=True)
        legacy_knowledge.ingest_outbox(rows, namespace_effects=True)
        effect_ids = tuple(str(row["effect_id"]) for row in rows)
        LocalCampaignCoordinator._require(store.acknowledge_outbox(effect_ids))
        return len(effect_ids)
    @staticmethod
    def _engine_command_timeouts(
        configuration: CampaignConfiguration,
        shard_size: int,
        *,
        estimated_shard_seconds: float | None = None,
        timeout_multiplier: float = 3.0,
    ) -> dict[str, float]:
        # Bound execute-shard from frozen estimates without dropping the conservative retry ceiling.
        test_seconds = max(1.0, float(configuration.budget.max_test_seconds or 120.0))
        mutants = max(1, int(shard_size))
        legacy_budget = (test_seconds * mutants * 3.0) + 30.0
        execute_budget = legacy_budget
        if estimated_shard_seconds is not None and float(estimated_shard_seconds) > 0.0:
            multiplier = max(1.0, float(timeout_multiplier))
            single_mutant_floor = (test_seconds * 3.0) + 30.0
            adaptive_budget = max(single_mutant_floor, (float(estimated_shard_seconds) * multiplier) + 30.0)
            execute_budget = min(legacy_budget, adaptive_budget)
        execute_budget = max(60.0, execute_budget)
        return {
            "prepare": max(120.0, test_seconds),
            "collect": max(120.0, test_seconds),
            "index": max(120.0, test_seconds),
            "baseline": max(30.0, test_seconds + 10.0),
            "discover": max(120.0, test_seconds),
            "execute-shard": execute_budget,
            "finalize": 30.0,
            "shutdown": 5.0,
        }
    @staticmethod
    def _heartbeat_interval_seconds(configuration: CampaignConfiguration) -> float:
        # Keep a larger scheduling margin so short leases survive host CPU contention and parallel test runners.
        lease_seconds = max(0.15, float(configuration.budget.lease_seconds))
        return min(0.25, max(0.01, lease_seconds / 16.0))
    @staticmethod
    def _load_prepared_snapshot(prepared: PreparedCampaign, root: Path) -> PreparedCampaignSnapshot | None:
        # Load the immutable engine snapshot when the child process exposes its artifact path.
        if not prepared.snapshot_path:
            return None
        path = Path(prepared.snapshot_path)
        if not path.is_absolute():
            path = root / path
        try:
            value = read_json(path)
            return PreparedCampaignSnapshot.from_dict(value) if isinstance(value, Mapping) else None
        except (OSError, TypeError, ValueError):
            return None
    @staticmethod
    def _prepared_snapshot_identity(
        prepared: PreparedCampaign,
        snapshot: PreparedCampaignSnapshot | None,
    ) -> str:
        # Resolve one canonical content identity even when an older prepare response omitted snapshot_id.
        explicit = str(prepared.snapshot_id or "").strip()
        if explicit:
            return explicit
        if snapshot is None:
            return ""
        return stable_hash(snapshot.to_dict())[:32]
    @staticmethod
    def _conftest_closure(
        root: Path,
        test_path: Path,
        *,
        fingerprint_snapshot: _FingerprintSnapshot | None = None,
    ) -> tuple[tuple[str, str], ...]:
        # Hash conftest files once per snapshot from the test directory up to the project root.
        snapshot = fingerprint_snapshot or _FingerprintSnapshot(root)
        root = snapshot.root
        resolved_test_path = test_path.resolve()
        cached = snapshot.conftest_closure_cache.get(resolved_test_path)
        if cached is not None:
            return cached
        rows: list[tuple[str, str]] = []
        for candidate in LocalCampaignCoordinator._conftest_paths(
            root,
            resolved_test_path,
            fingerprint_snapshot=snapshot,
        ):
            readable, source = snapshot.read_text(candidate)
            if readable:
                rows.append((candidate.relative_to(root).as_posix(), source))
            else:
                rows.append((str(candidate), "unreadable"))
        result = tuple(sorted(rows))
        snapshot.conftest_closure_cache[resolved_test_path] = result
        return result
    @staticmethod
    def _canonical_nodeid(root: Path, nodeid: str) -> str:
        # Keep every selection and evidence key independent of the external workspace directory.
        return canonical_nodeid(root, nodeid)
    @staticmethod
    def _local_dependency_closure(
        root: Path,
        entrypoint: Path,
        *,
        entrypoint_source: str | None = None,
        fingerprint_snapshot: _FingerprintSnapshot | None = None,
    ) -> tuple[tuple[str, str], ...]:
        # Resolve one transitive local closure while sharing immutable reads and parses across nodeids.
        snapshot = fingerprint_snapshot or _FingerprintSnapshot(root)
        root = snapshot.root
        resolved_entrypoint = entrypoint.resolve()
        cached = snapshot.local_closure_cache.get(resolved_entrypoint)
        if cached is None:
            pending = [resolved_entrypoint]
            visited: set[Path] = set()
            rows: dict[str, str] = {}
            dynamic_import = False
            while pending:
                current = pending.pop()
                if current in visited or not current.is_file() or root not in current.parents and current != root:
                    continue
                visited.add(current)
                readable, source = snapshot.read_text(current)
                tree = snapshot.tree(current) if readable else None
                hash_source = source if tree is not None else "unreadable"
                try:
                    relative = current.relative_to(root).as_posix()
                except ValueError:
                    relative = current.name
                rows[relative] = stable_hash(hash_source)
                if tree is None:
                    continue
                plugin_names, dynamic_plugin = LocalCampaignCoordinator._pytest_plugin_declarations(
                    source,
                    tree=tree,
                    source_parsed=True,
                )
                if dynamic_plugin:
                    dynamic_import = True
                    rows["<dynamic-pytest-plugin-declaration>"] = "uncertain"
                for plugin_name in plugin_names:
                    candidate = LocalCampaignCoordinator._resolve_local_module(
                        root,
                        current,
                        plugin_name,
                        fingerprint_snapshot=snapshot,
                    )
                    if candidate is None:
                        external_identity = LocalCampaignCoordinator._external_pytest_plugin_identity(
                            plugin_name,
                            fingerprint_snapshot=snapshot,
                        )
                        if external_identity is None:
                            dynamic_import = True
                            rows[f"<pytest-plugin:{plugin_name}>"] = "unresolved"
                        else:
                            rows[f"<external-pytest-plugin:{plugin_name}>"] = external_identity
                    elif candidate not in visited:
                        pending.append(candidate)
                for node in ast.walk(tree):
                    module_names: list[str] = []
                    if isinstance(node, ast.Import):
                        module_names.extend(alias.name for alias in node.names)
                    elif isinstance(node, ast.ImportFrom):
                        base_name = "." * int(node.level) + (node.module or "")
                        if base_name:
                            module_names.append(base_name)
                        for alias in node.names:
                            if alias.name == "*":
                                dynamic_import = True
                                continue
                            alias_name = f"{base_name}.{alias.name}" if base_name else alias.name
                            module_names.append(alias_name)
                    elif isinstance(node, ast.Call):
                        function = node.func
                        if (
                            (isinstance(function, ast.Name) and function.id == "__import__")
                            or (isinstance(function, ast.Attribute) and function.attr == "import_module")
                        ):
                            dynamic_import = True
                    for module_name in module_names:
                        candidate = LocalCampaignCoordinator._resolve_local_module(
                            root,
                            current,
                            module_name,
                            fingerprint_snapshot=snapshot,
                        )
                        if candidate is not None and candidate not in visited:
                            pending.append(candidate)
            if dynamic_import:
                rows["<dynamic-imports>"] = "uncertain"
                rows.update(snapshot.python_fallback_rows())
            cached = (tuple(sorted(rows.items())), dynamic_import)
            snapshot.local_closure_cache[resolved_entrypoint] = cached
        base_rows, dynamic_import = cached
        if entrypoint_source is None or dynamic_import:
            return base_rows
        rows = dict(base_rows)
        try:
            relative = resolved_entrypoint.relative_to(root).as_posix()
        except ValueError:
            relative = resolved_entrypoint.name
        rows[relative] = stable_hash(entrypoint_source)
        return tuple(sorted(rows.items()))
    @staticmethod
    def _pytest_plugin_declarations(
        source: str,
        *,
        tree: ast.Module | None = None,
        source_parsed: bool = False,
    ) -> tuple[tuple[str, ...], bool]:
        # Extract literal pytest_plugins modules without reparsing a cached source tree.
        if not source_parsed:
            tree = _FingerprintSnapshot._parse_source(source)
        if tree is None:
            return (), True
        names: list[str] = []
        uncertain = False
        bindings: dict[str, tuple[str, ...] | None] = {}
        def literal_sequence(node: ast.AST) -> tuple[str, ...] | None:
            # Evaluate only immutable string/list/tuple/set expressions and reject executable code.
            if isinstance(node, ast.Constant):
                return (node.value,) if isinstance(node.value, str) and node.value else () if node.value == "" else None
            if isinstance(node, ast.Name):
                return bindings.get(node.id)
            if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
                values: list[str] = []
                for item in node.elts:
                    parsed = literal_sequence(item)
                    if parsed is None or len(parsed) != 1:
                        return None
                    values.extend(parsed)
                return tuple(values)
            if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
                left = literal_sequence(node.left)
                right = literal_sequence(node.right)
                return None if left is None or right is None else (*left, *right)
            return None
        for node in tree.body:
            assignments: tuple[str, ast.AST, bool] = ()
            if isinstance(node, ast.Assign):
                assignments = tuple(
                    (target.id, node.value, False)
                    for target in node.targets
                    if isinstance(target, ast.Name)
                )
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value is not None:
                assignments = ((node.target.id, node.value, False),)
            elif isinstance(node, ast.AugAssign) and isinstance(node.target, ast.Name):
                assignments = ((node.target.id, node.value, True),)
            for target_name, value, is_augmented in assignments:
                parsed = literal_sequence(value)
                if is_augmented:
                    previous = bindings.get(target_name)
                    parsed = None if previous is None or parsed is None else (*previous, *parsed)
                bindings[target_name] = parsed
                if target_name != "pytest_plugins":
                    continue
                if parsed is None:
                    uncertain = True
                else:
                    names.extend(parsed)
        return tuple(sorted(set(names))), uncertain
    @staticmethod
    def _external_pytest_plugin_identity(
        module_name: str,
        *,
        fingerprint_snapshot: _FingerprintSnapshot | None = None,
    ) -> str | None:
        # Fingerprint one external pytest plugin once per campaign planning snapshot.
        if fingerprint_snapshot is not None and module_name in fingerprint_snapshot.external_plugin_cache:
            return fingerprint_snapshot.external_plugin_cache[module_name]
        try:
            spec = importlib.util.find_spec(module_name)
        except (ImportError, ModuleNotFoundError, ValueError):
            result = None
        else:
            if spec is None or not spec.origin or spec.origin in {"built-in", "frozen"}:
                result = None
            else:
                top_level = module_name.split(".", 1)[0]
                try:
                    distributions = importlib.metadata.packages_distributions().get(top_level, ())
                    versions = tuple(
                        sorted(
                            (
                                str(name),
                                str(importlib.metadata.version(name)),
                            )
                            for name in distributions
                        )
                    )
                except (KeyError, importlib.metadata.PackageNotFoundError, TypeError):
                    versions = ()
                result = (
                    stable_hash({"module": module_name, "origin_name": Path(spec.origin).name, "distributions": versions})
                    if versions
                    else None
                )
        if fingerprint_snapshot is not None:
            fingerprint_snapshot.external_plugin_cache[module_name] = result
        return result
    @staticmethod
    def _conftest_paths(
        root: Path,
        test_path: Path,
        *,
        fingerprint_snapshot: _FingerprintSnapshot | None = None,
    ) -> tuple[Path, ...]:
        # Resolve applicable conftest paths once per test module and planning snapshot.
        snapshot = fingerprint_snapshot or _FingerprintSnapshot(root)
        root = snapshot.root
        resolved_test_path = test_path.resolve()
        cached = snapshot.conftest_paths_cache.get(resolved_test_path)
        if cached is not None:
            return cached
        current = resolved_test_path.parent
        paths: list[Path] = []
        while current == root or root in current.parents:
            candidate = current / "conftest.py"
            if candidate.is_file():
                paths.append(candidate.resolve())
            if current == root:
                break
            current = current.parent
        result = tuple(paths)
        snapshot.conftest_paths_cache[resolved_test_path] = result
        return result
    @staticmethod
    def _test_dependency_closure(
        root: Path,
        candidate: Path,
        test_code: str,
        *,
        fingerprint_snapshot: _FingerprintSnapshot | None = None,
    ) -> tuple[tuple[str, str], ...]:
        # Combine one test module and its conftests through the shared campaign-local closure cache.
        snapshot = fingerprint_snapshot or _FingerprintSnapshot(root)
        rows: dict[str, str] = {}
        entrypoints = (
            candidate,
            *LocalCampaignCoordinator._conftest_paths(
                snapshot.root,
                candidate,
                fingerprint_snapshot=snapshot,
            ),
        )
        for entrypoint in entrypoints:
            closure = LocalCampaignCoordinator._local_dependency_closure(
                snapshot.root,
                entrypoint,
                entrypoint_source=test_code if entrypoint.resolve() == candidate.resolve() else None,
                fingerprint_snapshot=snapshot,
            )
            rows.update(closure)
        return tuple(sorted(rows.items()))
    @staticmethod
    def _runtime_dependency_is_static_code(
        path_value: str,
        code_dependency_paths: frozenset[str],
    ) -> bool:
        # Keep imported Python modules in the static closure while retaining unrelated Python data files.
        normalized = Path(str(path_value).replace("\\", "/")).as_posix().lstrip("./")
        candidate = Path(normalized)
        if candidate.suffix.lower() in {".pyc", ".pyo"} or "__pycache__" in candidate.parts:
            return True
        return normalized in code_dependency_paths
    @staticmethod
    def _data_dependency_manifest(
        root: Path,
        candidate: Path,
        source: str,
        *,
        runtime_record: Mapping[str, Any] | None,
        declared_globs: Sequence[str],
        excluded_globs: Sequence[str] = (),
        max_file_size_bytes: int = 10 * 1024 * 1024,
        code_dependency_paths: Sequence[str] = (),
        fingerprint_snapshot: _FingerprintSnapshot | None = None,
    ) -> tuple[tuple[tuple[str, str], ...], tuple[str, ...]]:
        # Hash static, declared and observed data through one campaign-local immutable snapshot.
        snapshot = fingerprint_snapshot or _FingerprintSnapshot(root)
        root = snapshot.root
        candidate = candidate.resolve()
        normalized_code_dependency_paths = frozenset(
            Path(str(item).replace("\\", "/")).as_posix().lstrip("./")
            for item in code_dependency_paths
            if str(item) and not str(item).startswith("<")
        )
        rows: dict[str, str] = {}
        blockers: set[str] = set()
        raw_key = (candidate, source)
        raw_candidates = snapshot.raw_data_cache.get(raw_key)
        if raw_candidates is None:
            tree = snapshot.tree_for_source(candidate, source)
            values: set[str] = set()
            if tree is not None:
                for node in ast.walk(tree):
                    if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                        continue
                    value = node.value.strip()
                    if not value or len(value) > 512 or "\n" in value:
                        continue
                    suffix = Path(value).suffix.lower()
                    if suffix in {
                        ".csv", ".ini", ".json", ".md", ".toml", ".txt", ".tsv",
                        ".xml", ".yaml", ".yml", ".html", ".jinja", ".j2", ".template",
                    } or "/" in value or "\\" in value:
                        values.add(value)
            raw_candidates = tuple(sorted(values))
            snapshot.raw_data_cache[raw_key] = raw_candidates
        for raw in raw_candidates:
            path_value = Path(raw)
            bases = (candidate.parent, root) if not path_value.is_absolute() else (Path("/"),)
            for base in bases:
                try:
                    path = (base / path_value).resolve() if not path_value.is_absolute() else path_value.resolve()
                except (OSError, RuntimeError, ValueError):
                    continue
                if not (path == root or root in path.parents):
                    blockers.add("external-static-data-dependency")
                    continue
                relative_path = path.relative_to(root).as_posix()
                if any(fnmatchcase(relative_path, str(pattern)) for pattern in excluded_globs if str(pattern)):
                    break
                if path.is_file():
                    try:
                        size_bytes = path.stat().st_size
                    except OSError:
                        blockers.add("unreadable-static-data-dependency")
                        break
                    if size_bytes > max(1, int(max_file_size_bytes)):
                        blockers.add("data-dependency-too-large")
                        break
                    rows[relative_path] = snapshot.file_sha256(path)
                    break
        declared_key = (
            tuple(str(item) for item in declared_globs if str(item)),
            tuple(str(item) for item in excluded_globs if str(item)),
            max(1, int(max_file_size_bytes)),
        )
        declared_cached = snapshot.declared_data_cache.get(declared_key)
        if declared_cached is None:
            declared_rows: dict[str, str] = {}
            declared_blockers: set[str] = set()
            for pattern in declared_key[0]:
                try:
                    matches = sorted(
                        path.resolve()
                        for path in root.glob(pattern)
                        if path.is_file() and (path.resolve() == root or root in path.resolve().parents)
                        and not any(
                            fnmatchcase(path.resolve().relative_to(root).as_posix(), excluded)
                            for excluded in declared_key[1]
                        )
                    )
                except (OSError, RuntimeError, ValueError):
                    declared_blockers.add(f"invalid-data-glob:{pattern}")
                    continue
                digest_rows = []
                for path in matches:
                    relative_path = path.relative_to(root).as_posix()
                    try:
                        size_bytes = path.stat().st_size
                    except OSError:
                        declared_blockers.add("unreadable-declared-data-dependency")
                        continue
                    if size_bytes > declared_key[2]:
                        declared_blockers.add("data-dependency-too-large")
                        continue
                    digest_rows.append((relative_path, snapshot.file_sha256(path)))
                declared_rows[f"<glob:{pattern}>"] = stable_hash(digest_rows)
            declared_cached = (tuple(sorted(declared_rows.items())), tuple(sorted(declared_blockers)))
            snapshot.declared_data_cache[declared_key] = declared_cached
        rows.update(declared_cached[0])
        blockers.update(declared_cached[1])
        if runtime_record is not None:
            if not bool(runtime_record.get("complete", False)):
                blockers.update(str(item) for item in runtime_record.get("blockers", ()) if str(item))
                if not runtime_record.get("blockers"):
                    blockers.add("runtime-dependency-manifest-incomplete")
            dependencies = runtime_record.get("dependencies", ())
            if isinstance(dependencies, (list, tuple)):
                for item in dependencies:
                    if not isinstance(item, Mapping) or not item.get("path"):
                        blockers.add("malformed-runtime-dependency")
                        continue
                    if bool(item.get("outside_workspace", False)) or str(item.get("path")) == "<external>":
                        blockers.add("external-runtime-dependency")
                        continue
                    try:
                        path = (root / str(item["path"])).resolve()
                    except (OSError, RuntimeError, ValueError):
                        blockers.add("unresolvable-runtime-dependency")
                        continue
                    if not (path == root or root in path.parents):
                        blockers.add("external-runtime-dependency")
                        continue
                    relative_path = path.relative_to(root).as_posix()
                    if any(fnmatchcase(relative_path, str(pattern)) for pattern in excluded_globs if str(pattern)):
                        continue
                    if LocalCampaignCoordinator._runtime_dependency_is_static_code(
                        relative_path,
                        normalized_code_dependency_paths,
                    ):
                        continue
                    try:
                        size_bytes = path.stat().st_size if path.is_file() else 0
                        if size_bytes > max(1, int(max_file_size_bytes)):
                            blockers.add("data-dependency-too-large")
                            continue
                        rows[relative_path] = snapshot.file_sha256(path) if path.is_file() else "missing"
                    except (OSError, UnicodeError):
                        rows[relative_path] = "unreadable"
        return tuple(sorted(rows.items())), tuple(sorted(blockers))
    @staticmethod
    def _data_dependency_policy(
        root: Path,
        project: Any,
    ) -> tuple[tuple[str, ...], tuple[str, ...], int, tuple[str, ...]]:
        # Load explicit project fields and optional pyproject declarations into one bounded policy.
        includes = [str(item) for item in getattr(project, "data_dependency_globs", ()) if str(item)]
        excludes = [str(item) for item in getattr(project, "data_dependency_exclude_globs", ()) if str(item)]
        max_size = max(1, int(getattr(project, "data_dependency_max_file_size_bytes", 10 * 1024 * 1024)))
        blockers: list[str] = []
        path = root / "pyproject.toml"
        if path.is_file():
            try:
                with path.open("rb") as handle:
                    document = tomllib.load(handle)
                section = document.get("tool", {}).get("theseus", {}).get("test_dependencies", {})
                if isinstance(section, Mapping):
                    includes.extend(str(item) for item in section.get("include", ()) if isinstance(item, str) and item)
                    excludes.extend(str(item) for item in section.get("exclude", ()) if isinstance(item, str) and item)
                    raw_max = section.get("max_file_size_bytes", section.get("max_file_size"))
                    if raw_max is not None:
                        max_size = max(1, int(raw_max))
                    rules = section.get("rules", ())
                    if isinstance(rules, (list, tuple)):
                        for rule in rules:
                            if isinstance(rule, Mapping):
                                includes.extend(
                                    str(item)
                                    for item in rule.get("resources", ())
                                    if isinstance(item, str) and item
                                )
            except (OSError, TypeError, ValueError, tomllib.TOMLDecodeError):
                blockers.append("data-dependency-config-invalid")
        return tuple(sorted(set(includes))), tuple(sorted(set(excludes))), max_size, tuple(sorted(set(blockers)))
    @staticmethod
    def _environment_reads(source: str) -> tuple[tuple[str, ...], bool]:
        # Parse one standalone source and extract its environment dependency names.
        return LocalCampaignCoordinator._environment_reads_from_tree(_FingerprintSnapshot._parse_source(source))
    @staticmethod
    def _environment_reads_from_tree(tree: ast.AST | None) -> tuple[tuple[str, ...], bool]:
        # Extract environment names from one already parsed source tree.
        if tree is None:
            return (), True
        names: set[str] = set()
        dynamic = False
        for node in ast.walk(tree):
            if isinstance(node, ast.Subscript):
                value = node.value
                is_environment = (
                    isinstance(value, ast.Attribute)
                    and value.attr == "environ"
                    and isinstance(value.value, ast.Name)
                    and value.value.id == "os"
                )
                if not is_environment:
                    continue
                slice_node = node.slice
                if isinstance(slice_node, ast.Constant) and isinstance(slice_node.value, str):
                    names.add(slice_node.value)
                else:
                    dynamic = True
            elif isinstance(node, ast.Call):
                function = node.func
                function_name = (
                    function.attr
                    if isinstance(function, ast.Attribute)
                    and isinstance(function.value, ast.Name)
                    and function.value.id == "os"
                    and function.attr == "getenv"
                    else function.id
                    if isinstance(function, ast.Name) and function.id == "getenv"
                    else None
                )
                if function_name != "getenv":
                    continue
                if node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                    names.add(node.args[0].value)
                else:
                    dynamic = True
            elif isinstance(node, ast.Attribute) and node.attr in {"keys", "items", "values"}:
                value = node.value
                if (
                    isinstance(value, ast.Attribute)
                    and value.attr == "environ"
                    and isinstance(value.value, ast.Name)
                    and value.value.id == "os"
                ):
                    dynamic = True
        return tuple(sorted(names)), dynamic
    @staticmethod
    def _environment_dependency_contract(
        source: str,
        *,
        descriptor: Any,
        source_path: Path | None = None,
        fingerprint_snapshot: _FingerprintSnapshot | None = None,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        # Compare environment reads once per immutable source and descriptor contract.
        cache_key: tuple[str, tuple[Any, ...]] | None = None
        if fingerprint_snapshot is not None:
            cache_key = (
                source,
                fingerprint_snapshot.environment_descriptor_key(descriptor),
            )
            cached = fingerprint_snapshot.environment_cache.get(cache_key)
            if cached is not None:
                return cached
        if fingerprint_snapshot is not None and source_path is not None:
            tree = fingerprint_snapshot.tree_for_source(source_path, source)
            names, dynamic = LocalCampaignCoordinator._environment_reads_from_tree(tree)
        else:
            names, dynamic = LocalCampaignCoordinator._environment_reads(source)
        if descriptor is None:
            mode = "strict"
            declared_keys: set[str] = set()
            declared_patterns: tuple[str, ...] = ()
            ignored: set[str] = set()
        else:
            mode = str(getattr(descriptor, "inherit_policy", "allowlisted") or "allowlisted").lower()
            declared_keys = {
                str(item)
                for item in (
                    *getattr(descriptor, "declared_env_keys", ()),
                    *getattr(descriptor, "tracked_variables", ()),
                )
                if str(item)
            }
            declared_patterns = tuple(
                str(item)
                for item in (
                    *getattr(descriptor, "declared_env_patterns", ()),
                    *getattr(descriptor, "tracked_prefixes", ()),
                )
                if str(item)
            )
            ignored = {
                str(item)
                for item in getattr(descriptor, "ignored_variables", ())
                if str(item)
            }
        blockers: set[str] = {"dynamic-environment-read"} if dynamic else set()
        for name in names:
            declared = name in declared_keys or any(fnmatchcase(name, pattern) for pattern in declared_patterns)
            if name in ignored:
                continue
            if mode == "track_all_except":
                continue
            if not declared:
                blockers.add(f"undeclared-environment-read:{name}")
        result = (names, tuple(sorted(blockers)))
        if fingerprint_snapshot is not None and cache_key is not None:
            fingerprint_snapshot.environment_cache[cache_key] = result
        return result
    @staticmethod
    def _test_fingerprint_bundle(
        root: Path,
        collection: Mapping[str, Any] | None,
        selected_tests: tuple[str, ...],
        *,
        runtime_dependencies: Mapping[str, Mapping[str, Any]] | None = None,
        declared_globs: Sequence[str] = (),
        excluded_globs: Sequence[str] = (),
        max_file_size_bytes: int = 10 * 1024 * 1024,
        environment_descriptor: Any = None,
        fingerprint_snapshot: _FingerprintSnapshot | None = None,
    ) -> tuple[dict[str, str], tuple[str, ...]]:
        # Build node-level fingerprints through one immutable campaign-local snapshot.
        snapshot_cache = fingerprint_snapshot or _FingerprintSnapshot(root)
        root = snapshot_cache.root
        collection_snapshot = collection if isinstance(collection, Mapping) else {}
        policy_includes, policy_excludes, policy_max_size, policy_blockers = LocalCampaignCoordinator._data_dependency_policy(
            root,
            None,
        )
        effective_globs = tuple(sorted(set(str(item) for item in (*policy_includes, *declared_globs) if str(item))))
        effective_excludes = tuple(sorted(set(str(item) for item in (*policy_excludes, *excluded_globs) if str(item))))
        effective_max_size = min(max(1, int(max_file_size_bytes)), policy_max_size)
        nodeids = selected_tests or tuple(str(item) for item in collection_snapshot.get("nodeids", []) if item)
        configuration = collection_snapshot.get("pytest_configuration_fingerprint")
        plugins = (str(collection_snapshot["plugin_fingerprint"]),) if collection_snapshot.get("plugin_fingerprint") else ()
        result: dict[str, str] = {}
        blockers: set[str] = set(policy_blockers)
        for nodeid in sorted(set(str(item) for item in nodeids)):
            canonical = LocalCampaignCoordinator._canonical_nodeid(root, nodeid)
            test_code, candidate = LocalCampaignCoordinator._node_source(
                root,
                canonical,
                fingerprint_snapshot=snapshot_cache,
            )
            closure = LocalCampaignCoordinator._test_dependency_closure(
                root,
                candidate,
                test_code,
                fingerprint_snapshot=snapshot_cache,
            )
            for key, value in closure:
                if key.startswith("<pytest-plugin:") or key.startswith("<dynamic-") or value in {"uncertain", "unresolved"}:
                    blockers.add(f"{key}={value}")
            runtime_record = runtime_dependencies.get(canonical) if runtime_dependencies is not None else None
            if runtime_dependencies is not None and runtime_record is None:
                blockers.add(f"runtime-manifest-missing:{canonical}")
            readable, data_source = snapshot_cache.read_text(candidate)
            if not readable:
                data_source = test_code
            environment_sources: list[tuple[str, Path | None]] = [(data_source, candidate if readable else None)]
            for dependency_path, _ in closure:
                if dependency_path.startswith("<"):
                    continue
                candidate_dependency = root / dependency_path
                dependency_readable, dependency_source = snapshot_cache.read_text(candidate_dependency)
                if dependency_readable:
                    environment_sources.append((dependency_source, candidate_dependency))
                else:
                    blockers.add(f"environment-source-unreadable:{dependency_path}")
            environment_names: set[str] = set()
            environment_blockers: set[str] = set()
            for environment_source, environment_path in environment_sources:
                names, environment_errors = LocalCampaignCoordinator._environment_dependency_contract(
                    environment_source,
                    descriptor=environment_descriptor,
                    source_path=environment_path,
                    fingerprint_snapshot=snapshot_cache,
                )
                environment_names.update(names)
                environment_blockers.update(environment_errors)
            if runtime_record is not None:
                environment_names.update(
                    str(item)
                    for item in runtime_record.get("environment_dependencies", ())
                    if str(item)
                )
                environment_blockers.update(
                    str(item)
                    for item in runtime_record.get("environment_blockers", ())
                    if str(item)
                )
            blockers.update(f"{canonical}:{item}" for item in environment_blockers)
            data_manifest, data_blockers = LocalCampaignCoordinator._data_dependency_manifest(
                root,
                candidate,
                data_source,
                runtime_record=runtime_record,
                declared_globs=effective_globs,
                excluded_globs=effective_excludes,
                max_file_size_bytes=effective_max_size,
                code_dependency_paths=(
                    candidate.relative_to(root).as_posix(),
                    *(key for key, _ in closure if not key.startswith("<")),
                ),
                fingerprint_snapshot=snapshot_cache,
            )
            blockers.update(f"{canonical}:{item}" for item in data_blockers)
            result[canonical] = fingerprint_test(
                test_code=test_code,
                conftest_closure=(
                    *LocalCampaignCoordinator._conftest_closure(
                        root,
                        candidate,
                        fingerprint_snapshot=snapshot_cache,
                    ),
                    *closure,
                ),
                pytest_configuration=configuration,
                plugins=plugins,
                data_dependencies=data_manifest,
                environment_dependencies=tuple(sorted(environment_names)),
            )
        return result, tuple(sorted(blockers))
    @staticmethod
    def _resolve_local_module(
        root: Path,
        current: Path,
        module_name: str,
        *,
        fingerprint_snapshot: _FingerprintSnapshot | None = None,
    ) -> Path | None:
        # Resolve one local module once per importer and module name in the planning snapshot.
        snapshot = fingerprint_snapshot or _FingerprintSnapshot(root)
        root = snapshot.root
        current = current.resolve()
        cache_key = (current, module_name)
        if cache_key in snapshot.module_cache:
            return snapshot.module_cache[cache_key]
        if module_name.startswith("."):
            dots = len(module_name) - len(module_name.lstrip("."))
            tail = module_name[dots:]
            base = current.parent
            for _ in range(max(0, dots - 1)):
                base = base.parent
        else:
            tail = module_name
            base = root
        parts = [item for item in tail.split(".") if item]
        module_path = base.joinpath(*parts)
        result = None
        for candidate in (module_path.with_suffix(".py"), module_path / "__init__.py"):
            resolved = candidate.resolve()
            if resolved.is_file() and (resolved == root or root in resolved.parents):
                result = resolved
                break
        snapshot.module_cache[cache_key] = result
        return result
    @staticmethod
    def _node_source(
        root: Path,
        nodeid: str,
        *,
        fingerprint_snapshot: _FingerprintSnapshot | None = None,
    ) -> tuple[str, Path]:
        # Build one exact test context while reading and parsing its module only once per snapshot.
        snapshot = fingerprint_snapshot or _FingerprintSnapshot(root)
        root = snapshot.root
        nodeid = LocalCampaignCoordinator._canonical_nodeid(root, nodeid)
        cached = snapshot.node_source_cache.get(nodeid)
        if cached is not None:
            return cached
        relative_path = nodeid.split("::", 1)[0]
        candidate = Path(relative_path)
        if not candidate.is_absolute():
            candidate = root / candidate
        readable, source = snapshot.read_text(candidate)
        tree = snapshot.tree(candidate) if readable else None
        if tree is None:
            result = (nodeid, candidate)
            snapshot.node_source_cache[nodeid] = result
            return result
        selectors = [item.split("[", 1)[0] for item in nodeid.split("::")[1:] if item]
        if not selectors:
            result = (nodeid, candidate)
            snapshot.node_source_cache[nodeid] = result
            return result
        def find_class(body: Sequence[ast.stmt], names: Sequence[str]) -> ast.ClassDef | None:
            # Resolve nested classes by their full nodeid chain instead of a global name search.
            current_body = body
            selected: ast.ClassDef | None = None
            for name in names:
                selected = next(
                    (
                        item
                        for item in current_body
                        if isinstance(item, ast.ClassDef) and item.name == name
                    ),
                    None,
                )
                if selected is None:
                    return None
                current_body = selected.body
            return selected
        class_names = selectors[:-1]
        target_name = selectors[-1]
        selected_class = find_class(tree.body, class_names) if class_names else None
        selected_target: ast.AST | None = selected_class
        if selected_class is not None:
            selected_target = next(
                (
                    item
                    for item in selected_class.body
                    if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == target_name
                ),
                None,
            )
            if selected_target is None:
                result = (f"unresolved-node:{nodeid}\n{ast.dump(tree, include_attributes=False)}", candidate)
                snapshot.node_source_cache[nodeid] = result
                return result
        else:
            selected_target = next(
                (
                    item
                    for item in tree.body
                    if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == target_name
                ),
                None,
            )
            if selected_target is None:
                result = (f"unresolved-node:{nodeid}\n{ast.dump(tree, include_attributes=False)}", candidate)
                snapshot.node_source_cache[nodeid] = result
                return result
        def is_test_definition(node: ast.AST) -> bool:
            # Exclude sibling test bodies while retaining fixtures and module helpers in the dependency context.
            return isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_")
        context_nodes: list[ast.stmt] = []
        for node in tree.body:
            if isinstance(node, (ast.Import, ast.ImportFrom, ast.Assign, ast.AnnAssign, ast.AugAssign)):
                context_nodes.append(node)
            elif node is selected_class:
                context_nodes.append(node)
            elif node is selected_target:
                context_nodes.append(node)
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and not is_test_definition(node):
                context_nodes.append(node)
        context = ast.Module(body=context_nodes, type_ignores=[])
        node_source = json.dumps(
            {
                "node_path": selectors,
                "module": candidate.relative_to(root).as_posix()
                if candidate.resolve().is_relative_to(root)
                else candidate.name,
                "context": ast.dump(context, annotate_fields=True, include_attributes=False),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        result = (node_source, candidate)
        snapshot.node_source_cache[nodeid] = result
        return result
    @staticmethod
    def _test_fingerprints(
        root: Path,
        collection: Mapping[str, Any] | None,
        selected_tests: tuple[str, ...],
    ) -> dict[str, str]:
        # Preserve the public helper while routing production callers through dependency-aware fingerprinting.
        result, _ = LocalCampaignCoordinator._test_fingerprint_bundle(root, collection, selected_tests)
        return result
    @staticmethod
    def _conftest_fingerprint(
        root: Path,
        test_fingerprints: Mapping[str, str],
        *,
        fingerprint_snapshot: _FingerprintSnapshot | None = None,
    ) -> str:
        # Build one broad conftest identity from the shared campaign-local closures.
        snapshot = fingerprint_snapshot or _FingerprintSnapshot(root)
        rows: list[tuple[str, tuple[tuple[str, str], ...]]] = []
        for nodeid in sorted(test_fingerprints):
            canonical = LocalCampaignCoordinator._canonical_nodeid(snapshot.root, nodeid)
            candidate = Path(canonical.split("::", 1)[0])
            if not candidate.is_absolute():
                candidate = snapshot.root / candidate
            rows.append(
                (
                    canonical,
                    LocalCampaignCoordinator._conftest_closure(
                        snapshot.root,
                        candidate,
                        fingerprint_snapshot=snapshot,
                    ),
                )
            )
        return fingerprint_conftest(closure=tuple(rows))
    @staticmethod
    def _selection_reuse_fingerprint(
        configuration: CampaignConfiguration,
        prepared: PreparedCampaign,
        root: Path,
        selected_tests: tuple[str, ...],
    ) -> str:
        # Hash only execution-affecting selection semantics and exclude explanatory reason metadata.
        selection = prepared.selection
        return stable_hash(
            {
                "algorithm_version": selection.algorithm_version if selection else "none",
                "source_path": selection.source_path if selection else configuration.scope.source_path,
                "source_sha256": selection.source_sha256 if selection else prepared.source_sha256,
                "selected_tests": list(selected_tests),
                "dropped_nodeids": sorted(
                    {
                        LocalCampaignCoordinator._canonical_nodeid(root, item)
                        for item in (selection.dropped_nodeids if selection else ())
                    }
                ),
                "levels": [
                    {
                        "name": level.name,
                        "nodeids": [
                            LocalCampaignCoordinator._canonical_nodeid(root, item)
                            for item in level.nodeids
                        ],
                        "files": [
                            LocalCampaignCoordinator._canonical_nodeid(root, item)
                            for item in level.files
                        ],
                    }
                    for level in (selection.levels if selection else ())
                ],
                "no_escalation": bool(configuration.no_escalation),
            }
        )
    @staticmethod
    def _reuse_requests(
        configuration: CampaignConfiguration,
        prepared: PreparedCampaign,
        collection: Mapping[str, Any] | None,
    ) -> list[dict[str, Any]]:
        # Build deterministic reuse inputs from one immutable campaign-local fingerprint snapshot.
        root = Path(configuration.project.root_path).expanduser().resolve()
        fingerprint_snapshot = _FingerprintSnapshot(root)
        prepared_snapshot = LocalCampaignCoordinator._load_prepared_snapshot(prepared, root)
        function_info = prepared_snapshot.function_info if prepared_snapshot else None
        source = root / prepared.source_path
        readable, source_text = fingerprint_snapshot.read_text(source)
        if not readable:
            source_text = prepared.source_sha256
        if isinstance(function_info, Mapping):
            start = max(1, int(function_info.get("start_line", 1)))
            end = max(start, int(function_info.get("end_line", start)))
            source_text = "\n".join(source_text.splitlines()[start - 1 : end]) or source_text
        function_fingerprint = fingerprint_function(
            normalized_ast=source_text,
            signature=configuration.scope.function,
            dependency_closure=LocalCampaignCoordinator._local_dependency_closure(
                root,
                source,
                entrypoint_source=source_text,
                fingerprint_snapshot=fingerprint_snapshot,
            ),
        )
        selected_tests = (
            tuple(LocalCampaignCoordinator._canonical_nodeid(root, item) for item in prepared.selection.selected_tests)
            if prepared.selection
            else ()
        )
        runtime_dependencies: Mapping[str, Mapping[str, Any]] | None = None
        if configuration.reports_dir:
            runtime_dependencies = load_runtime_dependency_manifest(
                stats_db_path(Path(configuration.reports_dir)),
                run_id=f"engine-baseline-{configuration.campaign_id.value}",
                nodeids=selected_tests,
                root=root,
            )
        test_fingerprints, reuse_blockers = LocalCampaignCoordinator._test_fingerprint_bundle(
            root,
            collection,
            selected_tests,
            runtime_dependencies=runtime_dependencies,
            declared_globs=configuration.project.data_dependency_globs,
            excluded_globs=configuration.project.data_dependency_exclude_globs,
            max_file_size_bytes=configuration.project.data_dependency_max_file_size_bytes,
            environment_descriptor=configuration.project.environment,
            fingerprint_snapshot=fingerprint_snapshot,
        )
        conftest_fingerprint = LocalCampaignCoordinator._conftest_fingerprint(
            root,
            test_fingerprints,
            fingerprint_snapshot=fingerprint_snapshot,
        )
        test_fingerprint = stable_hash(sorted(test_fingerprints.items()))
        if prepared_snapshot and prepared_snapshot.environment_fingerprint:
            environment_fingerprint = prepared_snapshot.environment_fingerprint
        elif configuration.project.environment is not None:
            environment_fingerprint = configuration.project.environment.fingerprint
        else:
            command = configuration.project.test_command.argv if configuration.project.test_command else ()
            environment_fingerprint = fingerprint_environment(test_command=command)
        selection_fingerprint = LocalCampaignCoordinator._selection_reuse_fingerprint(
            configuration,
            prepared,
            root,
            selected_tests,
        )
        semantic_configuration_fingerprint = stable_hash(
            {
                "selection_fingerprint": selection_fingerprint,
                "no_escalation": bool(configuration.no_escalation),
                "test_command": list(configuration.project.test_command.argv)
                if configuration.project.test_command
                else None,
            }
        )
        requests: list[dict[str, Any]] = []
        for mutant in prepared.mutants:
            mutant_fingerprint = fingerprint_mutant(
                function_fingerprint=function_fingerprint,
                operator_id=mutant.mutation,
                operator_version=mutant.operator_version,
                position=(mutant.line_no, mutant.column_no),
                replacement=mutant.replacement,
            )
            evidence_identity_fingerprint = fingerprint_evidence_identity(
                mutation_fingerprint=mutant_fingerprint,
                test_fingerprint=test_fingerprint,
                environment_fingerprint=environment_fingerprint,
                function_fingerprint=function_fingerprint,
                conftest_fingerprint=conftest_fingerprint,
                test_fingerprints=test_fingerprints,
                configuration_fingerprint=semantic_configuration_fingerprint,
            )
            requests.append(
                {
                    "mutant_id": mutant.mutant_id.value,
                    "function_id": configuration.scope.function,
                    "function_fingerprint": function_fingerprint,
                    "mutant_fingerprint": mutant_fingerprint,
                    "test_fingerprint": test_fingerprint,
                    "test_fingerprints": test_fingerprints,
                    "reuse_blockers": reuse_blockers,
                    "conftest_fingerprint": conftest_fingerprint,
                    "environment_fingerprint": environment_fingerprint,
                    "selection_fingerprint": selection_fingerprint,
                    "evidence_identity_fingerprint": evidence_identity_fingerprint,
                    "result_fingerprint": evidence_identity_fingerprint,
                    "estimated_execution_seconds": 1.0,
                }
            )
        return requests
    @staticmethod
    def _annotate_knowledge_executions(
        executions: tuple[Any, ...],
        requests: Mapping[str, Mapping[str, Any]],
        *,
        source_kind: str,
        reuse_matches: Mapping[str, tuple[Mapping[str, Any], tuple[str, ...]]] | None = None,
        root: Path | None = None,
        runtime_class: str | None = None,
    ) -> list[dict[str, Any]]:
        # Attach fingerprints to real execution observations without fabricating per-test outcomes.
        rows: list[dict[str, Any]] = []
        for execution in executions:
            row = execution.to_dict()
            request = requests.get(str(execution.mutant_id.value), {})
            for key in (
                "function_id",
                "function_fingerprint",
                "mutant_fingerprint",
                "test_fingerprint",
                "test_fingerprints",
                "conftest_fingerprint",
                "environment_fingerprint",
                "selection_fingerprint",
                "result_fingerprint",
                "evidence_identity_fingerprint",
            ):
                if key in request:
                    row[key] = request[key]
            test_fingerprints = request.get("test_fingerprints", {})
            if isinstance(test_fingerprints, Mapping):
                source_match = (reuse_matches or {}).get(str(execution.mutant_id.value))
                source_observations: dict[str, dict[str, Any]] = {}
                matched_set: set[str] = set()
                if source_match is not None:
                    source_record, matched_ids = source_match
                    source_payload = source_record.get("payload", {})
                    matched_set = {
                        LocalCampaignCoordinator._canonical_nodeid(root, str(item)) if root is not None else str(item)
                        for item in matched_ids
                    }
                    if isinstance(source_payload, Mapping):
                        source_observations = {
                            (LocalCampaignCoordinator._canonical_nodeid(root, str(item.get("test_id"))) if root is not None else str(item.get("test_id"))): dict(item)
                            for item in source_payload.get("test_observations", [])
                            if isinstance(item, Mapping)
                            and (
                                LocalCampaignCoordinator._canonical_nodeid(root, str(item.get("test_id")))
                                if root is not None
                                else str(item.get("test_id"))
                            ) in matched_set
                        }
                observed = {
                    (LocalCampaignCoordinator._canonical_nodeid(root, str(item.get("test_id"))) if root is not None else str(item.get("test_id"))): dict(item)
                    for item in row.get("test_observations", [])
                    if isinstance(item, Mapping) and item.get("test_id")
                }
                observations: list[dict[str, Any]] = []
                for test_id, fingerprint in sorted(test_fingerprints.items()):
                    key = str(test_id)
                    observation = source_observations.get(key) if key in matched_set else observed.get(key)
                    if observation is None:
                        observation = {
                            "test_id": key,
                            "outcome": "unknown",
                            "evidence_kind": "missing_observation",
                            "observation_schema_version": 1,
                        }
                    observation["test_id"] = key
                    observation["test_fingerprint"] = str(fingerprint)
                    observations.append(observation)
                row["test_observations"] = observations
                row["evidence_origin"] = (
                    "pytest_test_stats"
                    if any(item.get("evidence_kind") == "pytest_test_event" for item in observations)
                    else "aggregate_projection"
                )
            else:
                row["evidence_origin"] = "aggregate_projection"
            row["source_kind"] = (
                "exact_reuse"
                if str(row.get("classification_reason", "")).startswith("exact_reuse:")
                else "partial_merged"
                if str(execution.mutant_id.value) in (reuse_matches or {})
                else source_kind
            )
            if row["source_kind"] == "observed" and runtime_class:
                row["performance_hint"] = {
                    "runtime_class": str(runtime_class),
                    "source": "worker.registration",
                    "authoritative": False,
                }
            row["evidence_schema_version"] = 2
            row["retention_class"] = "campaign"
            rows.append(row)
        return rows
    @staticmethod
    def _attach_test_fingerprints(
        result: ShardExecutionResult,
        requests: Mapping[str, Mapping[str, Any]],
        root: Path,
    ) -> ShardExecutionResult:
        # Bind raw pytest events to the immutable node-level fingerprints before Gallifrey commit.
        annotated: list[MutantExecutionResult] = []
        for execution in result.results:
            request = requests.get(execution.mutant_id.value, {})
            fingerprints = request.get("test_fingerprints", {})
            if not isinstance(fingerprints, Mapping):
                raise RuntimeError(f"test fingerprints are missing for mutant {execution.mutant_id.value}")
            observations: list[dict[str, Any]] = []
            for item in execution.test_observations:
                observation = dict(item)
                test_id = LocalCampaignCoordinator._canonical_nodeid(root, str(observation.get("test_id", "")))
                fingerprint = fingerprints.get(test_id)
                if not test_id or not fingerprint:
                    raise RuntimeError(f"test fingerprint is missing for observed test {test_id or '<unknown>'}")
                observation["test_id"] = test_id
                observation["test_fingerprint"] = str(fingerprint)
                observations.append(observation)
            annotated.append(replace(execution, test_observations=tuple(observations)))
        return replace(result, results=tuple(annotated))
    @staticmethod
    def _audit_reuse_decision(
        decision: Any,
        record: Mapping[str, Any] | None,
        request: Mapping[str, Any],
    ) -> dict[str, Any]:
        # Prove executable reuse from stored pytest events after the policy authority admits it.
        if not bool(decision.authorized):
            return {
                "mutant_id": decision.mutant_id,
                "kind": decision.kind.value,
                "source_event_id": decision.source_event_id,
                "passed": True,
                "authorized": False,
                "failures": [],
            }
        failures: list[str] = []
        payload = record.get("payload", {}) if isinstance(record, Mapping) else {}
        if not isinstance(payload, Mapping):
            failures.append("payload_missing")
            payload = {}
        if not isinstance(record, Mapping) or str(record.get("evidence_quality", "")) != "validated":
            failures.append("evidence_not_validated")
        if not bool(payload.get("restore_verified", False)):
            failures.append("restore_not_verified")
        if str(payload.get("semantic_result", "")) not in {"killed", "survived", "invalid_mutant"}:
            failures.append("semantic_result_not_terminal")
        observations = {
            str(item.get("test_id")): item
            for item in payload.get("test_observations", [])
            if isinstance(item, Mapping)
            and item.get("test_id")
            and item.get("evidence_kind") == "pytest_test_event"
            and item.get("outcome") not in {None, "", "unknown"}
            and item.get("test_fingerprint")
        }
        current = {
            str(key): str(value)
            for key, value in (request.get("test_fingerprints", {}) or {}).items()
        }
        if decision.kind == ReuseKind.EXACT:
            if not current or set(current) != set(observations):
                failures.append("exact_test_observations_incomplete")
            elif any(str(observations[key]["test_fingerprint"]) != value for key, value in current.items()):
                failures.append("exact_test_fingerprint_mismatch")
        elif decision.kind == ReuseKind.PARTIAL:
            matched = set(str(item) for item in decision.matched_test_ids)
            if not matched or not matched.issubset(observations):
                failures.append("partial_test_observations_incomplete")
            elif any(str(observations[key]["test_fingerprint"]) != current.get(key) for key in matched):
                failures.append("partial_test_fingerprint_mismatch")
        return {
            "mutant_id": decision.mutant_id,
            "kind": decision.kind.value,
            "source_event_id": decision.source_event_id,
            "passed": not failures,
            "failures": failures,
        }
    @staticmethod
    def _reuse_audit_policy() -> ReuseAuditPolicy:
        # Read an explicit opt-in sampling policy without changing the immutable campaign configuration.
        def read_rate(name: str) -> float:
            raw = os.environ.get(name, "0")
            try:
                return float(raw)
            except ValueError as exc:
                raise ValueError(f"{name} must be a number between 0 and 1") from exc
        def read_count(name: str) -> int:
            raw = os.environ.get(name, "0")
            try:
                return int(raw)
            except ValueError as exc:
                raise ValueError(f"{name} must be a non-negative integer") from exc
        return ReuseAuditPolicy(
            exact_sample_rate=read_rate("THESEUS_REUSE_AUDIT_EXACT_RATE"),
            partial_sample_rate=read_rate("THESEUS_REUSE_AUDIT_PARTIAL_RATE"),
            minimum_samples_per_rule=read_count("THESEUS_REUSE_AUDIT_MINIMUM_SAMPLES"),
            new_rule_warmup_samples=read_count("THESEUS_REUSE_AUDIT_WARMUP_SAMPLES"),
            random_seed=os.environ.get("THESEUS_REUSE_AUDIT_SEED", "theseus-reuse-audit-v1"),
        )
    def _fresh_reuse_audit_result(
        self,
        *,
        process: EngineProcessSession,
        campaign: MutationCampaign,
        mutant_id: str,
        request: Mapping[str, Any],
        root: Path,
    ) -> dict[str, Any]:
        # Execute one complete mutant outside the reuse projection so sampled proof is independently observed.
        audit_shard = ShardDescriptor(
            ShardId(deterministic_id("reuse-audit-shard", campaign.campaign_id.value, mutant_id)),
            (mutant_id,),
            estimated_cost=1.0,
            plan_id=str(campaign.plan_id or ""),
        )
        try:
            audit_request = ExecuteShardRequest(
                campaign.campaign_id,
                audit_shard,
                attempt=0,
                worker_id=None,
                lease_id=None,
            ).to_dict()
            prepared_snapshot_id = str(campaign.prepared_snapshot_id or "").strip()
            if not prepared_snapshot_id:
                raise RuntimeError(
                    "campaign has no prepared snapshot identity for fresh reuse audit"
                )
            audit_request["prepared_snapshot_id"] = prepared_snapshot_id
            result = ShardExecutionResult.from_dict(
                process.request("execute-shard", audit_request)
            )
            result = self._attach_test_fingerprints(
                result,
                {mutant_id: request},
                root,
            )
        except Exception as exc:
            return {
                "status": "error",
                "semantic_result": "error",
                "restore_verified": False,
                "test_observations": [],
                "error": str(exc),
            }
        execution = next(
            (item for item in result.results if item.mutant_id.value == mutant_id),
            None,
        )
        if execution is None:
            return {
                "status": "error",
                "semantic_result": "error",
                "restore_verified": False,
                "test_observations": [],
                "error": "fresh audit returned no execution for the requested mutant",
            }
        row = execution.to_dict()
        row["status"] = str(row.get("status", "error"))
        row["semantic_result"] = row["status"]
        row["evidence_origin"] = "fresh_reuse_audit"
        row["source_kind"] = "observed"
        row["evidence_schema_version"] = 2
        for key in (
            "function_id",
            "function_fingerprint",
            "mutant_fingerprint",
            "test_fingerprint",
            "conftest_fingerprint",
            "environment_fingerprint",
            "selection_fingerprint",
            "result_fingerprint",
            "evidence_identity_fingerprint",
        ):
            if request.get(key) is not None:
                row[key] = request[key]
        row["test_observations"] = [dict(item) for item in execution.test_observations]
        return row
    @staticmethod
    def _quarantine_reuse_mismatch(
        *,
        knowledge: KnowledgePlaneStore,
        campaign_id: str,
        decision: Any,
        request: Mapping[str, Any],
        expected: Mapping[str, Any],
        actual: Mapping[str, Any],
        mismatches: Sequence[str],
    ) -> tuple[str, tuple[str, ...]]:
        # Record one content-addressed incident and one canonical rule quarantine for the mismatch.
        rule_id = str(
            request.get("function_id")
            or request.get("function_fingerprint")
            or request.get("mutant_fingerprint")
            or decision.mutant_id
        )
        incident_id = knowledge.record_reuse_incident(
            campaign_id=campaign_id,
            mutant_id=str(decision.mutant_id),
            rule_id=rule_id,
            kind=str(decision.kind.value),
            mismatches=mismatches,
            expected_payload=expected,
            actual_payload=actual,
        )
        knowledge.quarantine_reuse_rule(
            scope_type="rule",
            scope_key=rule_id,
            reason="fresh reuse audit mismatch: " + ",".join(str(item) for item in mismatches),
            incident_id=incident_id,
        )
        for scope_type, scope_key in (
            ("mutant", str(decision.mutant_id)),
            ("function", str(request.get("function_id", ""))),
        ):
            if not scope_key:
                continue
            knowledge.invalidate(
                scope_type=scope_type,
                scope_key=scope_key,
                reason="fresh reuse audit mismatch",
                campaign_id=None,
                idempotency_key=f"reuse-incident:{incident_id}:{scope_type}:{scope_key}",
            )
        return incident_id, (f"rule:{rule_id}",)
    @staticmethod
    def _reuse_audit_id(plan_fingerprint: str, decision: Any) -> str:
        # Address one audit row by immutable plan and source-evidence identity.
        return stable_hash(
            {
                "plan_fingerprint": plan_fingerprint,
                "mutant_id": str(decision.mutant_id),
                "kind": str(decision.kind.value),
                "source_event_id": decision.source_event_id,
                "source_execution_id": decision.source_execution_id,
            }
        )[:32]
    @staticmethod
    def _load_reuse_plan_artifact(path: Path) -> ReusePlanArtifact:
        # Restore and verify the exact frozen reuse plan without consulting changed knowledge history.
        try:
            payload = read_json(path)
        except (OSError, TypeError, ValueError) as exc:
            raise RuntimeError(
                f"campaign resume requires a valid reuse plan: path={path}; error={exc}"
            ) from exc
        if not isinstance(payload, Mapping):
            raise RuntimeError(f"reuse plan artifact is not an object: path={path}")
        try:
            schema_version = int(payload.get("schema_version", 0))
            plan_mode = normalize_reuse_mode(payload.get("reuse_mode"))
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"reuse plan header is invalid: path={path}; error={exc}") from exc
        if schema_version != 2:
            raise RuntimeError(
                "reuse plan schema_version is unsupported: "
                f"path={path}; schema_version={schema_version}"
            )
        raw_decisions = payload.get("decisions")
        if not isinstance(raw_decisions, (list, tuple)):
            raise RuntimeError(f"reuse plan decisions must be an array: path={path}")
        raw_metrics = payload.get("metrics", {})
        if not isinstance(raw_metrics, Mapping):
            raise RuntimeError(f"reuse plan metrics must be an object: path={path}")
        decisions: list[ReuseDecision] = []
        seen: set[str] = set()
        try:
            for raw in raw_decisions:
                if not isinstance(raw, Mapping):
                    raise ValueError("reuse decision must be an object")
                mutant_id = str(raw.get("mutant_id", "")).strip()
                reason = str(raw.get("reason", "")).strip()
                evidence_quality = str(raw.get("evidence_quality", "")).strip()
                if not mutant_id or not reason or not evidence_quality:
                    raise ValueError(
                        "reuse decision requires mutant_id, reason and evidence_quality"
                    )
                if mutant_id in seen:
                    raise ValueError(f"duplicate reuse decision mutant_id: {mutant_id}")
                for field in ("matched_test_ids", "missing_test_ids", "blockers"):
                    if not isinstance(raw.get(field, ()), (list, tuple)):
                        raise ValueError(f"reuse decision {field} must be an array")
                for field in (
                    "eligible",
                    "audit_required",
                    "source_compacted",
                    "authorized",
                ):
                    if not isinstance(raw.get(field, False), bool):
                        raise ValueError(f"reuse decision {field} must be boolean")
                for field in ("matched_test_ids", "missing_test_ids", "blockers"):
                    if any(not str(item).strip() for item in raw.get(field, ())):
                        raise ValueError(f"reuse decision {field} contains an empty value")
                optional_values = {
                    field: (str(raw[field]).strip() if raw.get(field) is not None else None)
                    for field in ("source_event_id", "source_execution_id", "result_status")
                }
                if any(raw.get(field) is not None and not value for field, value in optional_values.items()):
                    raise ValueError("reuse decision optional identities must be non-empty when present")
                kind = ReuseKind(str(raw.get("kind", "")))
                decision_mode = normalize_reuse_mode(raw.get("reuse_mode", plan_mode.value))
                eligible = bool(raw.get("eligible", False))
                authorized = bool(raw.get("authorized", False))
                audit_required = bool(raw.get("audit_required", False))
                matched = tuple(str(item) for item in raw.get("matched_test_ids", ()))
                missing = tuple(str(item) for item in raw.get("missing_test_ids", ()))
                if set(matched).intersection(missing):
                    raise ValueError("reuse decision test partitions must not overlap")
                if kind is ReuseKind.PARTIAL and eligible and (not matched or not missing):
                    raise ValueError("eligible partial reuse requires matched and missing test partitions")
                evidence_kind = kind in {ReuseKind.EXACT, ReuseKind.PARTIAL}
                if eligible and evidence_kind and (
                    optional_values["source_event_id"] is None
                    or optional_values["source_execution_id"] is None
                    or optional_values["result_status"] is None
                ):
                    raise ValueError("eligible reuse requires source event, execution and result identity")
                allowed = (
                    decision_mode.value == "partial" and evidence_kind
                ) or (decision_mode.value == "exact" and kind is ReuseKind.EXACT)
                if authorized != bool(eligible and allowed):
                    raise ValueError("reuse decision authority conflicts with its canonical mode")
                if audit_required and not authorized:
                    raise ValueError("reuse audit requires an authorized decision")
                seen.add(mutant_id)
                decisions.append(
                    ReuseDecision(
                        mutant_id=mutant_id,
                        kind=kind,
                        eligible=eligible,
                        reason=reason,
                        source_event_id=optional_values["source_event_id"],
                        source_execution_id=optional_values["source_execution_id"],
                        result_status=optional_values["result_status"],
                        evidence_quality=evidence_quality,
                        matched_test_ids=matched,
                        missing_test_ids=missing,
                        audit_required=audit_required,
                        source_compacted=bool(raw.get("source_compacted", False)),
                        blockers=tuple(str(item) for item in raw.get("blockers", ())),
                        authorized=authorized,
                        reuse_mode=decision_mode,
                    )
                )
            artifact = ReusePlanArtifact(
                history_revision=int(payload.get("history_revision", -1)),
                input_fingerprint=str(payload.get("input_fingerprint", "")).strip(),
                plan_fingerprint=str(payload.get("plan_fingerprint", "")).strip(),
                decisions=tuple(decisions),
                reuse_mode=plan_mode,
                metrics=ReuseMetrics.from_dict(raw_metrics) if raw_metrics else ReuseMetrics.from_decisions(tuple(decisions)),
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"reuse plan artifact is invalid: path={path}; error={exc}") from exc
        if artifact.history_revision < 0 or not artifact.input_fingerprint or not artifact.plan_fingerprint:
            raise RuntimeError(f"reuse plan identity fields are incomplete: path={path}")
        if tuple(item.mutant_id for item in artifact.decisions) != tuple(
            sorted(item.mutant_id for item in artifact.decisions)
        ):
            raise RuntimeError(f"reuse plan decisions are not in canonical mutant order: path={path}")
        if any(item.reuse_mode is not artifact.reuse_mode for item in artifact.decisions):
            raise RuntimeError(f"reuse plan decision mode conflicts with plan mode: path={path}")
        expected_fingerprint = stable_hash(
            {
                "reuse_mode": artifact.reuse_mode.value,
                "history_revision": artifact.history_revision,
                "input_fingerprint": artifact.input_fingerprint,
                "decisions": [item.to_dict() for item in artifact.decisions],
            }
        )
        if expected_fingerprint != artifact.plan_fingerprint:
            raise RuntimeError(
                "reuse plan fingerprint mismatch: "
                f"path={path}; expected={expected_fingerprint}; actual={artifact.plan_fingerprint}"
            )
        return artifact
    @staticmethod
    def _load_reuse_audit_progress(
        path: Path,
        *,
        campaign_id: str,
        reuse_plan: Any,
        campaign_plan: CampaignPlan,
    ) -> dict[str, dict[str, Any]]:
        # Load only structurally valid audit rows bound to the exact plans and source evidence being resumed.
        try:
            payload = read_json(path)
        except (OSError, TypeError, ValueError):
            return {}
        if not isinstance(payload, Mapping):
            return {}
        expected_sample = tuple(sorted(str(item) for item in campaign_plan.audit_sample))
        raw_sample = payload.get("audit_sample", ())
        if not isinstance(raw_sample, (list, tuple)):
            return {}
        actual_sample = tuple(sorted(str(item) for item in raw_sample))
        if (
            int(payload.get("schema_version", 0)) != 2
            or str(payload.get("campaign_id", "")) != campaign_id
            or str(payload.get("campaign_plan_id", "")) != campaign_plan.plan_id
            or str(payload.get("plan_fingerprint", "")) != reuse_plan.plan_fingerprint
            or str(payload.get("input_fingerprint", "")) != reuse_plan.input_fingerprint
            or int(payload.get("history_revision", -1)) != reuse_plan.history_revision
            or actual_sample != expected_sample
        ):
            return {}
        expected = {
            LocalCampaignCoordinator._reuse_audit_id(reuse_plan.plan_fingerprint, decision): decision
            for decision in reuse_plan.decisions
        }
        rows = payload.get("decisions", ())
        if not isinstance(rows, (list, tuple)):
            return {}
        allowed_statuses = {"complete", "running", "inconclusive", "quarantined", "recovered_incomplete"}
        result: dict[str, dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            audit_id = str(row.get("audit_id", ""))
            decision = expected.get(audit_id)
            if decision is None:
                continue
            source_event_id = str(row["source_event_id"]) if row.get("source_event_id") is not None else None
            source_execution_id = str(row["source_execution_id"]) if row.get("source_execution_id") is not None else None
            if (
                str(row.get("mutant_id", "")) != str(decision.mutant_id)
                or str(row.get("kind", "")) != str(decision.kind.value)
                or source_event_id != decision.source_event_id
                or source_execution_id != decision.source_execution_id
                or str(row.get("status", "")) not in allowed_statuses
                or not isinstance(row.get("passed"), bool)
                or not isinstance(row.get("sampled"), bool)
            ):
                continue
            result[audit_id] = dict(row)
        return result
    @staticmethod
    def _write_reuse_audit_progress(
        path: Path,
        *,
        campaign_id: str,
        reuse_plan: Any,
        campaign_plan: CampaignPlan,
        rows: Mapping[str, Mapping[str, Any]],
    ) -> None:
        # Publish resumable audit progress atomically with complete plan and history bindings.
        ordered = sorted(
            (dict(item) for item in rows.values()),
            key=lambda item: (str(item.get("mutant_id", "")), str(item.get("audit_id", ""))),
        )
        atomic_write_json(
            path,
            {
                "schema_version": 2,
                "campaign_id": campaign_id,
                "campaign_plan_id": campaign_plan.plan_id,
                "reuse_mode": reuse_plan.reuse_mode.value,
                "plan_fingerprint": reuse_plan.plan_fingerprint,
                "input_fingerprint": reuse_plan.input_fingerprint,
                "history_revision": reuse_plan.history_revision,
                "audit_sample": sorted(str(item) for item in campaign_plan.audit_sample),
                "decisions": ordered,
            },
            durability="critical",
            category="report",
        )
    @staticmethod
    def _merge_partial_result(
        decision: Any,
        source_record: Mapping[str, Any],
        observed: MutantExecutionResult,
    ) -> MutantExecutionResult:
        # Merge only actual killer observations and never promote an aggregate status to a test outcome.
        payload = source_record.get("payload", {})
        matched_ids = set(str(item) for item in decision.matched_test_ids)
        source_observations = payload.get("test_observations", []) if isinstance(payload, Mapping) else []
        historical_kill_proof = any(
            isinstance(item, Mapping)
            and str(item.get("test_id", "")) in matched_ids
            and str(item.get("outcome", "")) == "failed"
            and str(item.get("evidence_kind", "")) == "pytest_test_event"
            for item in source_observations
        )
        if observed.status == "killed" or historical_kill_proof:
            status = "killed"
        elif observed.status == "survived":
            status = "survived"
        elif observed.status == "invalid_mutant":
            status = "invalid_mutant"
        else:
            status = observed.status
        reused_level = {"level": "reused", "nodeids": sorted(matched_ids)}
        historical = [
            dict(item)
            for item in source_observations
            if isinstance(item, Mapping) and str(item.get("test_id", "")) in matched_ids
        ]
        return MutantExecutionResult(
            execution_id=observed.execution_id,
            mutant_id=observed.mutant_id,
            status=status,
            classification_reason=f"partial_reuse:{decision.source_event_id}:{observed.classification_reason}",
            restore_verified=observed.restore_verified and bool(payload.get("restore_verified", False)),
            level_results=(reused_level, *observed.level_results),
            artifact_paths=observed.artifact_paths,
            error=observed.error,
            lease_id=observed.lease_id,
            attempt=observed.attempt,
            test_observations=tuple((*historical, *observed.test_observations)),
            duration_seconds=observed.duration_seconds,
        )
    @staticmethod
    def _reused_result(
        decision: Any,
        record: Mapping[str, Any],
        campaign_id: str,
        worker_id: WorkerId,
        lease_id: str,
        attempt: int,
    ) -> MutantExecutionResult | None:
        # Convert a verified raw knowledge result into a new lease-bound execution projection.
        payload = record.get("payload")
        if not isinstance(payload, Mapping) or not bool(payload.get("restore_verified", False)):
            return None
        if str(record.get("evidence_quality", "")) != "validated":
            return None
        status = str(payload.get("semantic_result") or record.get("status") or "")
        if status not in {"killed", "survived", "invalid_mutant"}:
            return None
        mutant_id = str(payload.get("mutant_id") or record.get("mutant_id") or "")
        if not mutant_id:
            return None
        selected_tests = tuple(str(item) for item in payload.get("selected_tests", []) if item)
        observations = tuple(
            dict(item)
            for item in payload.get("test_observations", [])
            if isinstance(item, Mapping)
        )
        return MutantExecutionResult(
            execution_id=ExecutionId(deterministic_id("exec", campaign_id, mutant_id, attempt, "exact-reuse")),
            mutant_id=MutantId(mutant_id),
            status=status,
            classification_reason=f"exact_reuse:{decision.source_event_id}",
            restore_verified=True,
            level_results=({"level": "reused", "nodeids": list(selected_tests)},),
            artifact_paths=(),
            error=str(payload["error"]) if payload.get("error") else None,
            lease_id=lease_id,
            attempt=attempt,
            test_observations=observations,
        )
    @staticmethod
    def _confirmed_execution_result(
        execution: MutationExecution,
        *,
        campaign_id: str,
        lease_id: str,
        attempt: int,
    ) -> MutantExecutionResult | None:
        # Carry one verified prior execution into a retry attempt without rerunning its mutant.
        semantic = getattr(execution.semantic_result, "value", execution.semantic_result)
        status = str(semantic or "")
        if (
            execution.status.value != "complete"
            or not execution.restore_verified
            or status not in {"killed", "survived", "invalid_mutant"}
        ):
            return None
        return MutantExecutionResult(
            execution_id=ExecutionId(
                deterministic_id(
                    "exec",
                    campaign_id,
                    execution.mutant_id.value,
                    attempt,
                    "confirmed-retry",
                )
            ),
            mutant_id=execution.mutant_id,
            status=status,
            classification_reason=f"confirmed_retry:{execution.execution_id.value}",
            restore_verified=True,
            level_results=(
                {
                    "level": "confirmed-retry",
                    "nodeids": list(execution.selected_tests),
                },
            ),
            artifact_paths=(),
            error=execution.error,
            lease_id=lease_id,
            attempt=attempt,
            test_observations=tuple(dict(item) for item in execution.test_observations),
        )
    @staticmethod
    def _validate_campaign_plan_authority(
        campaign_plan: CampaignPlan,
        prepared: PreparedCampaign,
        catalog: Sequence[Any],
    ) -> CampaignPlan:
        # Verify immutable preparation, decision ledger, ordering and shard topology before orchestration.
        try:
            campaign_plan.verify_integrity()
        except ValueError as exc:
            raise RuntimeError(str(exc)) from exc
        if campaign_plan.campaign_id != prepared.campaign_id:
            raise RuntimeError("campaign plan belongs to another prepared campaign")
        if campaign_plan.source_sha256 != prepared.source_sha256:
            raise RuntimeError("campaign plan source hash conflicts with prepared campaign")
        prepared_snapshot_id = str(prepared.snapshot_id or "").strip()
        if not prepared_snapshot_id or campaign_plan.prepared_snapshot_id != prepared_snapshot_id:
            raise RuntimeError("campaign plan prepared_snapshot_id conflicts with prepared campaign")
        catalog_by_id = {item.mutant_id.value: item for item in catalog}
        selected_ids = tuple(item.mutant.mutant_id.value for item in campaign_plan.selected)
        if any(mutant_id not in catalog_by_id for mutant_id in selected_ids):
            raise RuntimeError("campaign plan selects an unknown mutant_id")
        if any(item.mutant != catalog_by_id[item.mutant.mutant_id.value] for item in campaign_plan.selected):
            raise RuntimeError("campaign plan selected descriptor conflicts with prepared catalog")
        if tuple(item.rank for item in campaign_plan.selected) != tuple(range(len(campaign_plan.selected))):
            raise RuntimeError("campaign plan selected ordering is not contiguous")
        decision_ids = {item.mutant_id for item in campaign_plan.decisions}
        if decision_ids != set(catalog_by_id):
            raise RuntimeError("campaign plan decision ledger does not cover the complete mutant catalog")
        ranks = {item.mutant.mutant_id.value: item.rank for item in campaign_plan.selected}
        for descriptor in campaign_plan.shards:
            if descriptor.plan_id != campaign_plan.plan_id:
                raise RuntimeError("campaign plan contains a shard bound to another plan_id")
            expected_order = tuple(sorted(descriptor.mutant_ids, key=ranks.__getitem__))
            if descriptor.mutant_ids != expected_order:
                raise RuntimeError("campaign plan shard membership violates planner ordering")
        return campaign_plan
    @staticmethod
    def _load_or_build_campaign_plan(
        configuration: CampaignConfiguration,
        prepared: PreparedCampaign,
        reports_dir: Path,
        *,
        mutants: Sequence[Any] | None = None,
        existing_plan_id: str | None = None,
        reuse_decisions: Mapping[str, Any] | None = None,
        audit_policy: Any | None = None,
        adaptive_observations: Mapping[str, Mapping[str, object]] | None = None,
    ) -> CampaignPlan:
        # Load one immutable plan first and build only when no prior plan authority exists.
        catalog = tuple(mutants if mutants is not None else prepared.mutants)
        path = reports_dir / "campaign.plan.json"
        planner = CampaignPlanner()
        if path.is_file():
            raw = read_json(path)
            if not isinstance(raw, Mapping):
                raise RuntimeError("campaign plan artifact is not an object")
            try:
                stored = CampaignPlan.from_dict(raw)
                stored = LocalCampaignCoordinator._validate_campaign_plan_authority(
                    stored,
                    prepared,
                    catalog,
                )
                stored = planner.validate_resume(
                    stored,
                    configuration,
                    prepared,
                    mutants=catalog,
                    reuse_decisions=reuse_decisions,
                    audit_policy=audit_policy,
                )
            except (PlanError, RuntimeError, TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"stored campaign plan conflicts with immutable planning inputs: {exc}"
                ) from exc
            if existing_plan_id and existing_plan_id != stored.plan_id:
                raise RuntimeError("campaign aggregate references another immutable campaign plan")
            return stored
        if existing_plan_id:
            raise RuntimeError(
                "campaign aggregate references a missing immutable campaign plan: "
                f"plan_id={existing_plan_id}; path={path}"
            )
        try:
            candidate = planner.build(
                configuration,
                prepared,
                mutants=catalog,
                reuse_decisions=reuse_decisions,
                audit_policy=audit_policy,
                adaptive_observations=adaptive_observations,
            )
            candidate = LocalCampaignCoordinator._validate_campaign_plan_authority(
                candidate,
                prepared,
                catalog,
            )
        except PlanError as exc:
            raise RuntimeError(f"campaign planning failed: {exc}") from exc
        plan_content_hash = candidate.artifact_sha256
        if not plan_content_hash:
            raise RuntimeError("campaign plan content hash is missing")
        atomic_write_json(path, candidate.to_dict(), durability="critical", category="report")
        return candidate
    def _reconcile_shard_knowledge(
        self,
        *,
        store: SQLiteMutationStore,
        knowledge: KnowledgePlaneStore,
        legacy_knowledge: KnowledgePlaneStore,
        campaign: MutationCampaign,
        shard: MutationShard,
        requests_by_mutant: Mapping[str, Mapping[str, Any]],
        root: Path,
    ) -> None:
        # Re-enrich a committed shard after a crash between outbox acknowledgement and projection enrichment.
        effect_ids = (
            f"effect.execute-shard.{shard.shard_id.value}.{shard.attempt}",
            f"effect.execute-shard.{shard.attempt}",
        )
        for effect_id in effect_ids:
            receipt = self._require(store.get_effect(effect_id))
            if receipt is None:
                continue
            raw_executions = receipt.payload.get("executions", [])
            if not isinstance(raw_executions, (list, tuple)):
                raise RuntimeError(f"effect receipt has invalid executions: {effect_id}")
            executions = tuple(
                MutationExecution.from_dict(item)
                for item in raw_executions
                if isinstance(item, Mapping)
            )
            payload = {
                "campaign": receipt.payload.get("campaign", campaign.to_dict()),
                "shard": receipt.payload.get("shard", shard.to_dict()),
                "executions": self._annotate_knowledge_executions(
                    executions,
                    requests_by_mutant,
                    source_kind="observed",
                    root=root,
                ),
            }
            knowledge_effect_id = f"{campaign.campaign_id.value}:{effect_id}"
            knowledge.enrich_effect(
                effect_id=knowledge_effect_id,
                campaign_id=campaign.campaign_id.value,
                payload=payload,
            )
            legacy_knowledge.enrich_effect(
                effect_id=knowledge_effect_id,
                campaign_id=campaign.campaign_id.value,
                payload=payload,
            )
            return
    @staticmethod
    def _performance_phase_map(payload: Mapping[str, Any] | None) -> dict[str, float]:
        # Normalize one versioned performance payload into finite non-negative phase seconds.
        if not isinstance(payload, Mapping) or not bool(payload.get("exclusive", False)):
            return {}
        rows = payload.get("phases", ())
        if not isinstance(rows, (list, tuple)):
            return {}
        phases: dict[str, float] = {}
        for item in rows:
            if not isinstance(item, Mapping) or not item.get("phase"):
                continue
            try:
                seconds = max(0.0, float(item.get("wall_seconds", 0.0)))
            except (TypeError, ValueError):
                continue
            phases[str(item["phase"])] = phases.get(str(item["phase"]), 0.0) + seconds
        return phases
    @staticmethod
    def _load_worker_runner_timeline(
        report_path: str | None,
        *,
        fallback_root: Path | None = None,
    ) -> Mapping[str, Any] | None:
        # Load the runner-exclusive timeline from worker-local or already published canonical evidence.
        if not report_path:
            return None
        primary = Path(str(report_path))
        candidates = [primary]
        if fallback_root is not None:
            candidates.append(Path(fallback_root) / primary.name)
        for path in dict.fromkeys(candidate.resolve() for candidate in candidates):
            if not path.is_file():
                continue
            try:
                report = read_json(path)
            except (OSError, TypeError, ValueError):
                continue
            if not isinstance(report, Mapping):
                continue
            metrics = report.get("metrics", {})
            performance = metrics.get("performance", {}) if isinstance(metrics, Mapping) else {}
            timeline = performance.get("worker_execution_timeline") if isinstance(performance, Mapping) else None
            if isinstance(timeline, Mapping):
                return timeline
        return None
    @staticmethod
    def _load_engine_process_timeline(worker_base: Path) -> Mapping[str, Any] | None:
        # Load the single engine-process timeline produced inside one isolated worker attempt.
        try:
            candidates = tuple(sorted(worker_base.rglob("engine-process.performance.json")))
        except OSError:
            return None
        if not candidates:
            return None
        try:
            payload = read_json(candidates[-1])
        except (OSError, TypeError, ValueError):
            return None
        return payload if isinstance(payload, Mapping) else None
    @staticmethod
    def _worker_execution_breakdown(
        *,
        worker_id: str,
        total_seconds: float,
        assignment_dispatch_seconds: float,
        process_timeline: Mapping[str, Any] | None,
        runner_timeline: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        # Reconcile nested engine and runner evidence against the coordinator-observed worker wait.
        total = max(0.0, float(total_seconds))
        phases: list[dict[str, Any]] = []
        dispatch = max(0.0, float(assignment_dispatch_seconds))
        phases.append(
            {
                "phase": "assignment_dispatch",
                "wall_seconds": dispatch,
                "source": "coordinator.perf_counter",
            }
        )
        process_phases = LocalCampaignCoordinator._performance_phase_map(process_timeline)
        runner_phases = LocalCampaignCoordinator._performance_phase_map(runner_timeline)
        execute_seconds = process_phases.pop("engine_request_execute_shard", 0.0)
        process_phases.pop("engine_request_shutdown", None)
        process_phases.pop("engine_session_teardown", None)
        process_phases.pop("engine_process_unattributed_residual", None)
        for phase, seconds in process_phases.items():
            phases.append(
                {
                    "phase": phase,
                    "wall_seconds": seconds,
                    "source": "engine-process.monotonic",
                }
            )
        runner_total = 0.0
        if isinstance(runner_timeline, Mapping):
            try:
                runner_total = max(0.0, float(runner_timeline.get("total_wall_seconds", 0.0)))
            except (TypeError, ValueError):
                runner_total = 0.0
        runner_phase_total = sum(runner_phases.values())
        if (
            execute_seconds > 0.0
            and runner_phases
            and (runner_total <= 0.0 or runner_phase_total > runner_total + 0.05)
        ):
            for phase in (
                "runner_index",
                "runner_selection",
                "runner_snapshot",
                "runner_mutant_generation",
                "runner_unattributed_residual",
            ):
                runner_phases.pop(phase, None)
            runner_total = sum(runner_phases.values())
        runner_fits_execute = execute_seconds <= 0.0 or runner_total <= execute_seconds + 0.05
        runner_evidence_used = bool(runner_phases and runner_fits_execute)
        if runner_evidence_used:
            for phase, seconds in runner_phases.items():
                phases.append(
                    {
                        "phase": phase,
                        "wall_seconds": seconds,
                        "source": "runner.performance",
                    }
                )
            if execute_seconds > 0.0:
                phases.append(
                    {
                        "phase": "engine_execute_shard_protocol",
                        "wall_seconds": max(0.0, execute_seconds - runner_total),
                        "source": "engine-process.minus-runner",
                    }
                )
        elif execute_seconds > 0.0:
            phases.append(
                {
                    "phase": "engine_request_execute_shard",
                    "wall_seconds": execute_seconds,
                    "source": "engine-process.monotonic",
                }
            )
        observed = sum(float(item["wall_seconds"]) for item in phases)
        valid = observed <= total + 0.05
        if not valid:
            phases = [phases[0]]
            observed = dispatch
        residual = max(0.0, total - observed)
        phases.append(
            {
                "phase": "durable_delivery_spool_and_runtime",
                "wall_seconds": residual,
                "source": "coordinator.reconciliation",
            }
        )
        accounted = observed + residual
        runner_pytest_breakdown = (
            runner_timeline.get("pytest_process_breakdown")
            if valid
            and runner_evidence_used
            and isinstance(runner_timeline, Mapping)
            and isinstance(runner_timeline.get("pytest_process_breakdown"), Mapping)
            else None
        )
        runner_prepared_invocation = (
            runner_timeline.get("prepared_invocation")
            if valid
            and runner_evidence_used
            and isinstance(runner_timeline, Mapping)
            and isinstance(runner_timeline.get("prepared_invocation"), Mapping)
            else None
        )
        return {
            "schema_version": 1,
            "timeline_version": "worker-execution-exclusive-v1",
            "worker_id": str(worker_id),
            "exclusive": True,
            "valid_nested_evidence": valid,
            "total_wall_seconds": total,
            "observed_phase_seconds": observed,
            "residual_seconds": residual,
            "accounted_seconds": accounted,
            "accounting_error_seconds": abs(total - accounted),
            "phases": phases,
            "pytest_process_breakdown": dict(runner_pytest_breakdown) if runner_pytest_breakdown is not None else None,
            "prepared_invocation": dict(runner_prepared_invocation) if runner_prepared_invocation is not None else None,
        }
    @staticmethod
    def _publish_worker_execution_report(
        path: Path,
        results: Sequence[tuple[Mapping[str, Any], ShardExecutionResult | None, str | None]],
        timeline: _CoordinatorTimeline,
    ) -> dict[str, Any]:
        # Publish per-worker exclusive rows and bounded aggregate diagnostics for benchmark consumers.
        workers = [
            dict(item[0]["worker_execution_breakdown"])
            for item in results
            if isinstance(item[0].get("worker_execution_breakdown"), Mapping)
        ]
        phase_values: dict[str, list[float]] = {}
        for worker in workers:
            for item in worker.get("phases", ()):
                if not isinstance(item, Mapping) or not item.get("phase"):
                    continue
                phase_values.setdefault(str(item["phase"]), []).append(max(0.0, float(item.get("wall_seconds", 0.0))))
        for phase, values in sorted(phase_values.items()):
            timeline.observe_diagnostic(f"worker_phase_{phase}_sum_seconds", sum(values))
            timeline.observe_diagnostic(f"worker_phase_{phase}_max_seconds", max(values, default=0.0))
        pytest_phase_values: dict[str, list[float]] = {}
        for worker in workers:
            breakdown = worker.get("pytest_process_breakdown")
            if (
                not isinstance(breakdown, Mapping)
                or breakdown.get("exclusive") is not True
                or str(breakdown.get("status", "")) not in {"observed", "partial"}
            ):
                continue
            phase_seconds = breakdown.get("phase_seconds")
            if not isinstance(phase_seconds, Mapping):
                continue
            for phase, seconds in phase_seconds.items():
                try:
                    value = max(0.0, float(seconds))
                except (TypeError, ValueError):
                    continue
                pytest_phase_values.setdefault(str(phase), []).append(value)
        for phase, values in sorted(pytest_phase_values.items()):
            timeline.observe_diagnostic(f"worker_pytest_process_{phase}_sum_seconds", sum(values))
            timeline.observe_diagnostic(f"worker_pytest_process_{phase}_max_seconds", max(values, default=0.0))
        prepared_values: list[float] = []
        prepared_process_counts: list[float] = []
        prepared_command_counts: list[float] = []
        prepared_observed = 0
        prepared_inconsistent = 0
        for worker in workers:
            prepared = worker.get("prepared_invocation")
            if not isinstance(prepared, Mapping):
                continue
            status = str(prepared.get("status", ""))
            if status == "observed":
                prepared_observed += 1
            elif status == "inconsistent_total":
                prepared_inconsistent += 1
            try:
                prepared_values.append(max(0.0, float(prepared.get("total_seconds", 0.0))))
                prepared_process_counts.append(max(0.0, float(prepared.get("process_preparations", 0.0))))
                prepared_command_counts.append(max(0.0, float(prepared.get("command_builds", 0.0))))
            except (TypeError, ValueError):
                continue
        if prepared_values:
            timeline.observe_diagnostic("worker_prepared_invocation_measured_sum_seconds", sum(prepared_values))
            timeline.observe_diagnostic("worker_prepared_invocation_measured_max_seconds", max(prepared_values))
            timeline.observe_diagnostic(
                "worker_prepared_invocation_process_preparations_sum",
                sum(prepared_process_counts),
            )
            timeline.observe_diagnostic(
                "worker_prepared_invocation_command_builds_sum",
                sum(prepared_command_counts),
            )
        timeline.observe_diagnostic("worker_prepared_invocation_observed_workers", float(prepared_observed))
        timeline.observe_diagnostic("worker_prepared_invocation_inconsistent_workers", float(prepared_inconsistent))
        ratios = [
            float(worker.get("accounted_seconds", 0.0)) / float(worker.get("total_wall_seconds", 1.0))
            for worker in workers
            if float(worker.get("total_wall_seconds", 0.0)) > 0.0
        ]
        timeline.observe_diagnostic("worker_phase_accounting_ratio_min", min(ratios, default=1.0))
        timeline.observe_diagnostic(
            "worker_phase_residual_max_seconds",
            max((float(worker.get("residual_seconds", 0.0)) for worker in workers), default=0.0),
        )
        payload = {
            "schema_version": 1,
            "timeline_version": "worker-execution-exclusive-v1",
            "exclusive": True,
            "workers": workers,
        }
        try:
            atomic_write_json(path, payload, durability="normal", category="report")
        except (OSError, TypeError, ValueError):
            return payload
        return payload
    @staticmethod
    def _scheduler_diagnostics(
        results: Sequence[tuple[Mapping[str, Any], ShardExecutionResult | None, str | None]],
        *,
        worker_slots: int,
    ) -> dict[str, float]:
        # Reconcile bounded dispatch, queue delay and deterministic shard costs without changing fan-in authority.
        slots = max(1, int(worker_slots))
        def non_negative(value: Any) -> float:
            try:
                normalized = float(value)
            except (TypeError, ValueError):
                return 0.0
            return normalized if normalized >= 0.0 and normalized < float("inf") else 0.0
        queue_wait_values = [
            non_negative(spec.get("scheduler_queue_wait_seconds", 0.0))
            for spec, _observed, _error in results
        ]
        lifecycle_values = [
            non_negative(spec.get("worker_setup_seconds", 0.0))
            + non_negative(spec.get("worker_execution_seconds", 0.0))
            for spec, _observed, _error in results
        ]
        estimated_costs = [
            non_negative(getattr(spec.get("descriptor"), "estimated_cost", 0.0))
            for spec, _observed, _error in results
        ]
        critical_path = max(lifecycle_values, default=0.0)
        lifecycle_total = sum(lifecycle_values)
        capacity = critical_path * slots
        idle_seconds = max(0.0, capacity - lifecycle_total)
        average_cost = sum(estimated_costs) / len(estimated_costs) if estimated_costs else 0.0
        cost_imbalance = (
            max(estimated_costs) / average_cost
            if average_cost > 0.0
            else 0.0
        )
        data_plane_metrics = {
            "workspace_setup_sum_seconds": sum(
                non_negative(spec.get("workspace_setup_seconds", 0.0))
                for spec, _observed, _error in results
            ),
            "workspace_setup_max_seconds": max(
                (non_negative(spec.get("workspace_setup_seconds", 0.0)) for spec, _observed, _error in results),
                default=0.0,
            ),
            "workspace_cleanup_seconds": sum(
                non_negative(spec.get("workspace_cleanup_seconds", 0.0))
                for spec, _observed, _error in results
            ),
        }
        for name in (
            "workspace_bytes_read",
            "workspace_bytes_written",
            "workspace_physical_bytes_allocated",
            "workspace_linked_files",
            "workspace_copied_files",
            "workspace_cache_hits",
            "workspace_cache_misses",
            "prepared_snapshot_cache_hits",
            "prepared_snapshot_cache_misses",
            "prepared_snapshot_bytes_read",
            "prepared_snapshot_bytes_written",
            "prepared_snapshot_hardlink_hits",
            "prepared_snapshot_destination_hits",
            "prepared_snapshot_lookup_seconds",
            "prepared_snapshot_materialization_seconds",
        ):
            data_plane_metrics[name] = sum(
                non_negative(spec.get(name, 0.0))
                for spec, _observed, _error in results
            )
        return {
            "scheduler_worker_slots": float(slots),
            "scheduler_dispatched_units": float(len(results)),
            "scheduler_queue_wait_seconds": sum(queue_wait_values),
            "scheduler_queue_wait_max_seconds": max(queue_wait_values, default=0.0),
            "scheduler_critical_path_seconds": critical_path,
            "worker_idle_seconds": idle_seconds,
            "worker_utilization": min(1.0, lifecycle_total / capacity) if capacity > 0.0 else 0.0,
            "scheduler_cost_imbalance_ratio": cost_imbalance,
            **data_plane_metrics,
        }
    def _execute_parallel_shards(
        self,
        *,
        service: MutationCampaignService,
        store: SQLiteMutationStore,
        knowledge: KnowledgePlaneStore,
        legacy_knowledge: KnowledgePlaneStore,
        current: MutationCampaign,
        configuration: CampaignConfiguration,
        runtime_configuration: CampaignConfiguration,
        campaign_workspace: Path,
        prepared: PreparedCampaign,
        campaign_plan: CampaignPlan,
        requests_by_mutant: Mapping[str, Mapping[str, Any]],
        decisions_by_mutant: Mapping[str, Any],
        audit_by_mutant: Mapping[str, bool],
        root: Path,
        timeline: _CoordinatorTimeline,
    ) -> MutationCampaign:
        # Run only the immutable planner-owned topology in isolated workers with durable registry state.
        timeline.switch("worker_dispatch")
        if current.plan_id != campaign_plan.plan_id:
            raise RuntimeError("campaign aggregate references another immutable campaign plan")
        if current.prepared_snapshot_id != campaign_plan.prepared_snapshot_id:
            raise RuntimeError("campaign aggregate prepared snapshot conflicts with campaign plan")
        if prepared.snapshot_id != campaign_plan.prepared_snapshot_id:
            raise RuntimeError("execution prepared snapshot conflicts with campaign plan")
        shard_descriptors = campaign_plan.shards
        selected_ids = {item.mutant.mutant_id.value for item in campaign_plan.selected}
        estimated_seconds_by_mutant = {
            item.mutant.mutant_id.value: float(item.estimated_seconds)
            for item in campaign_plan.selected
        }
        requested_ids = set(requests_by_mutant)
        if not selected_ids.issubset(requested_ids):
            missing = sorted(selected_ids - requested_ids)
            raise RuntimeError("campaign plan has no reuse request for selected mutants: " + ", ".join(missing))
        descriptors_by_id = {item.shard_id.value: (ordinal, item) for ordinal, item in enumerate(shard_descriptors)}
        stored_shards = self._require(store.list_shards(current.campaign_id))
        prior_executions = self._require(store.list_executions(current.campaign_id))
        confirmed_executions: dict[tuple[str, str], MutationExecution] = {}
        for execution in prior_executions:
            key = (execution.shard_id.value, execution.mutant_id.value)
            previous = confirmed_executions.get(key)
            if (
                execution.status.value == "complete"
                and execution.restore_verified
                and (previous is None or execution.attempt > previous.attempt)
            ):
                confirmed_executions[key] = execution
        unknown_shards = sorted(item.shard_id.value for item in stored_shards if item.shard_id.value not in descriptors_by_id)
        if unknown_shards:
            raise RuntimeError("stored shard topology contains unknown shard_id values: " + ", ".join(unknown_shards))
        if len(stored_shards) != len(shard_descriptors):
            raise RuntimeError(
                "stored shard topology is not the complete immutable campaign plan: "
                f"expected={len(shard_descriptors)}; actual={len(stored_shards)}"
            )
        for stored in stored_shards:
            ordinal, descriptor = descriptors_by_id[stored.shard_id.value]
            if not stored.matches_plan_descriptor(campaign_plan.plan_id, descriptor, ordinal=ordinal):
                raise RuntimeError(f"stored shard topology conflicts with campaign plan: shard_id={stored.shard_id.value}")
        control_lock = RLock()
        if current.status == CampaignState.PLANNING:
            current = self._require(
                service.start(
                    "effect.start",
                    current.campaign_id,
                    expected_revision=current.revision_number,
                )
            )
        registry_root = Path(runtime_configuration.reports_dir or root.parent) / "../state" / "workers" / current.campaign_id.value
        registry_root = registry_root.resolve()
        canonical_engine_root = (
            WorkspaceProvider(runtime_configuration).reports_root
            / "engine"
            / current.campaign_id.value
        )
        registry = WorkerRegistryProjection(
            registry_root / "worker-registry.json",
            campaign_id=current.campaign_id.value,
            store=store,
        )
        registry.refresh()
        active: list[dict[str, Any]] = []
        def stop_claim_heartbeat(spec: Mapping[str, Any]) -> None:
            # Join the pre-engine lease guard before the process-owned heartbeat takes over.
            stop_event = spec.get("claim_heartbeat_stop")
            if isinstance(stop_event, Event):
                stop_event.set()
            thread = spec.get("claim_heartbeat_thread")
            if isinstance(thread, Thread):
                thread.join(timeout=2.0)
        def start_claim_heartbeat(spec: dict[str, Any]) -> None:
            # Keep a freshly claimed lease alive while workspace materialization and process startup are still pending.
            stop_event = Event()
            def renew_claim() -> None:
                # Renew the authoritative pre-engine lease and shard projection in one transaction.
                sequence = int(spec["lease_state"].lease.heartbeat_sequence)
                while not stop_event.wait(self._heartbeat_interval_seconds(runtime_configuration)):
                    try:
                        with control_lock:
                            state_now = spec["lease_state"]
                            sequence += 1
                            renewed = service.renew_claimed_lease(
                                f"effect.lease-renew-claim.{spec['descriptor'].shard_id.value}.{state_now.lease.attempt}.{sequence}",
                                state_now.lease.lease_id,
                                heartbeat_at=utc_now(),
                                heartbeat_sequence=sequence,
                                expected_lease_revision=state_now.lease.revision_number,
                                expected_shard_revision=state_now.shard.revision_number,
                                now=utc_now(),
                            )
                            spec["lease_state"] = self._require(renewed)
                            spec["claimed"] = spec["lease_state"].shard
                    except BaseException as exc:
                        spec["lease_error"] = str(exc)
                        stop_event.set()
                        return
            thread = Thread(
                target=renew_claim,
                name=f"theseus-claim-lease-{spec['descriptor'].shard_id.value}",
                daemon=False,
            )
            spec["claim_heartbeat_stop"] = stop_event
            spec["claim_heartbeat_thread"] = thread
            thread.start()
        def stop_bound_heartbeat(spec: Mapping[str, Any]) -> None:
            # Join the coordinator-owned bound lease guard before delivery fencing or worker cleanup.
            stop_event = spec.get("bound_heartbeat_stop")
            if isinstance(stop_event, Event):
                stop_event.set()
            thread = spec.get("bound_heartbeat_thread")
            if isinstance(thread, Thread):
                thread.join(timeout=2.0)
        def start_bound_heartbeat(
            spec: dict[str, Any],
            identity: Any,
            worker: PersistentWorkerProcess,
        ) -> None:
            # Keep lease renewal independent from worker stdout scheduling under host CPU contention.
            stop_event = Event()
            def renew_bound() -> None:
                sequence = int(spec["lease_state"].lease.worker_heartbeat_sequence)
                while not stop_event.wait(self._heartbeat_interval_seconds(runtime_configuration)):
                    try:
                        with control_lock:
                            state_now = spec.get("lease_state")
                            if state_now is None:
                                raise RuntimeError("bound lease state disappeared before heartbeat")
                            sequence = max(sequence, int(state_now.lease.worker_heartbeat_sequence)) + 1
                            sent_at = utc_now()
                            child_process_id = worker.last_engine_child_pid
                            heartbeat = WorkerHeartbeat(
                                worker=identity,
                                sequence=sequence,
                                sent_at=sent_at,
                                current_campaign_id=current.campaign_id.value,
                                current_shard_id=spec["descriptor"].shard_id.value,
                                current_lease_id=state_now.lease.lease_id,
                                current_attempt=state_now.lease.attempt,
                                child_process_id=child_process_id,
                                child_process_birth_token=(
                                    current_process_birth_token(child_process_id)
                                    if child_process_id is not None
                                    else None
                                ),
                                workspace_healthy=True,
                            )
                            renewed = service.renew_worker_lease(
                                (
                                    f"effect.lease-heartbeat-parent.{identity.worker_id}."
                                    f"{identity.instance_id}.{state_now.lease.attempt}.{sequence}"
                                ),
                                current.campaign_id,
                                heartbeat,
                                expected_lease_revision=state_now.lease.revision_number,
                                expected_shard_revision=state_now.shard.revision_number,
                                expected_worker_revision=(
                                    state_now.worker.revision_number if state_now.worker is not None else 0
                                ),
                                completed_assignments=0,
                                now=sent_at,
                            )
                            state_now = self._require(renewed)
                            spec["lease_state"] = state_now
                            spec["claimed"] = state_now.shard
                            spec["worker_record"] = state_now.worker
                    except BaseException as exc:
                        spec["lease_error"] = str(exc)
                        stop_event.set()
                        return
            thread = Thread(
                target=renew_bound,
                name=f"theseus-bound-lease-{spec['descriptor'].shard_id.value}",
                daemon=False,
            )
            spec["bound_heartbeat_stop"] = stop_event
            spec["bound_heartbeat_thread"] = thread
            thread.start()
        for ordinal, shard_descriptor in enumerate(shard_descriptors):
            # Claim all runnable shards before starting threads so membership and lease ownership are frozen.
            with control_lock:
                shard = self._require(store.get_shard(shard_descriptor.shard_id))
                if shard is None:
                    raise RuntimeError(
                        f"stored shard topology is missing planner shard: shard_id={shard_descriptor.shard_id.value}"
                    )
                if not shard.matches_plan_descriptor(campaign_plan.plan_id, shard_descriptor, ordinal=ordinal):
                    raise RuntimeError(
                        f"stored shard topology conflicts with campaign plan: shard_id={shard.shard_id.value}"
                    )
                if shard.status == MutationShardState.COMPLETE:
                    self._reconcile_shard_knowledge(
                        store=store,
                        knowledge=knowledge,
                        legacy_knowledge=legacy_knowledge,
                        campaign=current,
                        shard=shard,
                        requests_by_mutant=requests_by_mutant,
                        root=root,
                    )
                    continue
                if shard.status in {MutationShardState.ORPHANED, MutationShardState.FAILED, MutationShardState.PARTIAL}:
                    shard = self._require(
                        service.retry_shard(
                            f"effect.retry-shard.{shard_descriptor.shard_id.value}.{shard.attempt + 1}",
                            shard.shard_id,
                            expected_revision=shard.revision_number,
                        )
                    )
                if shard.status in {MutationShardState.LEASED, MutationShardState.RUNNING}:
                    if not shard.lease_expired(utc_now()):
                        raise RuntimeError("active shard lease has not expired; refusing concurrent resume")
                    authoritative_lease = self._require(store.get_lease(shard.lease.lease_id)) if shard.lease else None
                    if authoritative_lease is not None:
                        owner = self._require(
                            store.get_worker(current.campaign_id, authoritative_lease.worker_id.value)
                        )
                        orphaned_state = self._require(
                            service.orphan_lease_assignment(
                                f"effect.lease-orphan.{shard_descriptor.shard_id.value}.{authoritative_lease.revision_number}",
                                authoritative_lease.lease_id,
                                expected_lease_revision=authoritative_lease.revision_number,
                                expected_shard_revision=shard.revision_number,
                                expected_worker_revision=owner.revision_number if owner is not None else None,
                                now=utc_now(),
                            )
                        )
                        shard = orphaned_state.shard
                    else:
                        shard = self._require(
                            service.orphan_expired_shard(
                                f"effect.orphan-shard.{shard_descriptor.shard_id.value}.{shard.revision_number}",
                                shard.shard_id,
                                expected_revision=shard.revision_number,
                            )
                        )
                    shard = self._require(
                        service.retry_shard(
                            f"effect.retry-shard.{shard_descriptor.shard_id.value}.{shard.attempt + 1}",
                            shard.shard_id,
                            expected_revision=shard.revision_number,
                        )
                    )
                worker_id = WorkerId(f"local-worker-{ordinal:03d}")
                instance_id = deterministic_id(
                    "worker-instance",
                    current.campaign_id.value,
                    shard_descriptor.shard_id.value,
                    shard.attempt,
                    ordinal,
                )
                lease = ShardLease(
                    worker_id=worker_id,
                    lease_id=(
                        f"{current.campaign_id.value}:{shard_descriptor.shard_id.value}:attempt-{shard.attempt}:"
                        f"instance-{instance_id}"
                    ),
                    status=WorkerStatus.RUNNING,
                    lease_seconds=float(runtime_configuration.budget.lease_seconds),
                    heartbeat_at=utc_now(),
                    heartbeat_seq=0,
                    attempt=shard.attempt,
                    worker_instance_id=instance_id,
                )
                lease_state = self._require(
                    service.claim_shard_lease(
                        f"effect.lease-claim.{shard_descriptor.shard_id.value}.{shard.attempt}",
                        shard.shard_id,
                        lease,
                        expected_shard_revision=shard.revision_number,
                    )
                )
                lease_state = self._require(
                    service.renew_claimed_lease(
                        f"effect.lease-renew-claim.{shard_descriptor.shard_id.value}.{shard.attempt}.1",
                        lease_state.lease.lease_id,
                        heartbeat_at=utc_now(),
                        heartbeat_sequence=1,
                        expected_lease_revision=lease_state.lease.revision_number,
                        expected_shard_revision=lease_state.shard.revision_number,
                        now=utc_now(),
                    )
                )
                claimed = lease_state.shard
            reused_results: list[MutantExecutionResult] = []
            execute_mutant_ids: list[str] = []
            test_overrides: dict[str, tuple[str, ...]] = {}
            partial_sources: dict[str, tuple[Mapping[str, Any], tuple[str, ...]]] = {}
            for mutant_id in shard_descriptor.mutant_ids:
                confirmed = confirmed_executions.get((shard_descriptor.shard_id.value, mutant_id))
                if confirmed is not None and confirmed.attempt < claimed.attempt:
                    carried = self._confirmed_execution_result(
                        confirmed,
                        campaign_id=current.campaign_id.value,
                        lease_id=lease_state.lease.lease_id,
                        attempt=claimed.attempt,
                    )
                    if carried is not None:
                        reused_results.append(carried)
                        continue
                decision = decisions_by_mutant.get(mutant_id)
                if (
                    audit_by_mutant.get(mutant_id, False)
                    and decision is not None
                    and decision.kind == ReuseKind.PARTIAL
                    and bool(decision.authorized)
                ):
                    source_record = knowledge.get_execution_record(decision.source_event_id) if decision.source_event_id else None
                    if source_record is not None and decision.missing_test_ids:
                        execute_mutant_ids.append(mutant_id)
                        test_overrides[mutant_id] = tuple(decision.missing_test_ids)
                        partial_sources[mutant_id] = (source_record, tuple(decision.matched_test_ids))
                        continue
                if (
                    not audit_by_mutant.get(mutant_id, False)
                    or decision is None
                    or decision.kind != ReuseKind.EXACT
                    or not bool(decision.authorized)
                ):
                    execute_mutant_ids.append(mutant_id)
                    continue
                source_record = knowledge.get_execution_record(decision.source_event_id) if decision.source_event_id else None
                reused = (
                    self._reused_result(
                        decision,
                        source_record,
                        current.campaign_id.value,
                        claimed.lease.worker_id,
                        claimed.lease.lease_id,
                        claimed.attempt,
                    )
                    if source_record is not None and claimed.lease is not None
                    else None
                )
                if reused is None:
                    execute_mutant_ids.append(mutant_id)
                else:
                    reused_results.append(reused)
            execute_estimated_cost = sum(
                estimated_seconds_by_mutant.get(mutant_id, 0.0)
                for mutant_id in execute_mutant_ids
            )
            worker_id = str(claimed.lease.worker_id.value if claimed.lease else f"local-worker-{ordinal:03d}")
            active.append(
                {
                    "ordinal": ordinal,
                    "descriptor": shard_descriptor,
                    "shard": shard,
                    "claimed": claimed,
                    "lease_state": lease_state,
                    "instance_id": instance_id,
                    "execute_mutant_ids": tuple(execute_mutant_ids),
                    "execute_estimated_cost": execute_estimated_cost,
                    "test_overrides": test_overrides,
                    "partial_sources": partial_sources,
                    "reused_results": tuple(reused_results),
                }
            )
            start_claim_heartbeat(active[-1])
        def run_attempt(spec: dict[str, Any]) -> tuple[dict[str, Any], ShardExecutionResult | None, str | None]:
            # Dispatch one shard attempt to a persistent worker process that owns its engine child.
            attempt_started = time.perf_counter()
            data_plane_metrics: dict[str, float] = {}
            descriptor = spec["descriptor"]
            claimed = spec["claimed"]
            if claimed.lease is None:
                raise RuntimeError("parallel shard has no claimed lease")
            worker_id = str(claimed.lease.worker_id.value)
            worker_slot_root = registry_root / worker_id
            worker_base = worker_slot_root / (
                f"attempt-{claimed.attempt}-{spec['instance_id']}"
            )
            worker_workspace = prepare_worker_workspace(
                campaign_workspace,
                worker_base / "workspace",
                worker_id=worker_id,
                campaign_id=current.campaign_id.value,
                metrics=data_plane_metrics,
            )
            spec.update(data_plane_metrics)
            workspace_backend = worker_workspace_backend(worker_workspace)
            spec["workspace_backend"] = workspace_backend
            worker_reports = worker_base / "reports"
            worker_reports.mkdir(parents=True, exist_ok=True)
            spool_root = worker_slot_root / "spool"
            spool = DurableExecutionSpool(spool_root)
            quarantine_non_current_deliveries(
                spool,
                campaign_id=current.campaign_id.value,
                shard_id=descriptor.shard_id.value,
                current_lease_id=claimed.lease.lease_id,
                current_attempt=claimed.attempt,
            )
            command = runtime_configuration.project.test_command
            worker_command = command
            if command is not None:
                command_cwd = Path(command.cwd).expanduser() if command.cwd else campaign_workspace
                if not command_cwd.is_absolute():
                    command_cwd = campaign_workspace / command_cwd
                try:
                    relative_cwd = command_cwd.resolve().relative_to(campaign_workspace.resolve())
                    worker_cwd = worker_workspace / relative_cwd
                except ValueError:
                    worker_cwd = command_cwd.resolve()
                worker_command = replace(command, cwd=str(worker_cwd))
            worker_project = replace(
                runtime_configuration.project,
                root_path=str(worker_workspace),
                test_command=worker_command,
            )
            prepared_snapshot_source = Path(prepared.snapshot_path) if prepared.snapshot_path else None
            if prepared_snapshot_source is not None and not prepared_snapshot_source.is_absolute():
                prepared_snapshot_source = campaign_workspace / prepared_snapshot_source
            snapshot_payload = (
                read_json(prepared_snapshot_source)
                if prepared_snapshot_source is not None and prepared_snapshot_source.is_file()
                else None
            )
            snapshot = (
                PreparedCampaignSnapshot.from_dict(snapshot_payload)
                if isinstance(snapshot_payload, Mapping)
                else None
            )
            if prepared_snapshot_source is None or snapshot is None:
                raise RuntimeError("coordinator prepared snapshot is unavailable for OS worker")
            prepared_snapshot_id = self._prepared_snapshot_identity(prepared, snapshot)
            if not prepared_snapshot_id:
                raise RuntimeError("coordinator prepared snapshot has no immutable identity")
            worker_snapshot_path = worker_reports / "engine" / current.campaign_id.value / "prepared.snapshot.json"
            materialize_prepared_snapshot(
                prepared_snapshot_source,
                worker_snapshot_path,
                expected_snapshot_id=prepared_snapshot_id,
                cache_root=registry_root / "prepared-snapshot-cache",
                metrics=data_plane_metrics,
            )
            spec.update(data_plane_metrics)
            worker_configuration = replace(
                runtime_configuration,
                project=worker_project,
                reports_dir=str(worker_reports),
                configuration_fingerprint=snapshot.configuration_fingerprint,
            )
            report_root = worker_reports / current.campaign_id.value
            worker = PersistentWorkerProcess(
                worker_id=worker_id,
                instance_id=str(spec["instance_id"]),
                spool_root=spool_root,
                heartbeat_interval_seconds=self._heartbeat_interval_seconds(runtime_configuration),
                cwd=Path(__file__).resolve().parent.parent,
                auto_accept_registration=False,
            )
            spec["worker_process"] = worker
            lease_state = spec["lease_state"]
            worker_record = None
            def renew_from_worker(frame: WorkerProtocolFrame) -> HeartbeatReceipt:
                # A coordinator-owned guard renews the bound lease; the child heartbeat remains a liveness signal.
                if frame.message_type != WorkerMessageType.WORKER_HEARTBEAT:
                    raise RuntimeError("worker renewal callback received a non-heartbeat frame")
                heartbeat_payload = frame.payload
                if not isinstance(heartbeat_payload, WorkerHeartbeatFrame):
                    raise RuntimeError("worker heartbeat frame has an invalid typed payload")
                if self.cancel_path(configuration).is_file():
                    raise RuntimeError("campaign cancellation requested")
                del heartbeat_payload
                return HeartbeatReceipt(True, True)
            try:
                worker.start()
                registered_frame = worker.wait_for_frame(
                    WorkerMessageType.REGISTER_WORKER,
                    timeout=10.0,
                )
                registration = registered_frame.payload
                if not isinstance(registration, RegisterWorker):
                    raise RuntimeError("worker registration frame has an invalid typed payload")
                identity = registration.identity
                spec["observed_identity"] = identity
                if identity.worker_id != worker_id or identity.instance_id != str(spec["instance_id"]):
                    worker.respond_registration(
                        registered_frame,
                        accepted=False,
                        reason="worker registration conflicts with the claimed lease",
                    )
                    raise RuntimeError("worker registration conflicts with the claimed lease")
                if registration.runtime_identity is None:
                    worker.respond_registration(
                        registered_frame,
                        accepted=False,
                        reason="worker registration has no runtime identity",
                    )
                    raise RuntimeError("worker registration has no runtime identity")
                try:
                    worker_runtime_identity = RuntimeIdentity.from_dict(registration.runtime_identity)
                    assert_runtime_compatible(
                        current_runtime_identity(),
                        worker_runtime_identity,
                    )
                except (RuntimeCompatibilityError, ValueError) as exc:
                    worker.respond_registration(
                        registered_frame,
                        accepted=False,
                        reason=f"worker runtime is incompatible: {exc}",
                    )
                    raise RuntimeError(f"worker runtime is incompatible: {exc}") from exc
                runtime_class = str(worker_runtime_identity.runtime_fingerprint).strip()
                if runtime_class:
                    spec["runtime_class"] = runtime_class
                stop_claim_heartbeat(spec)
                if spec.get("lease_error"):
                    worker.respond_registration(
                        registered_frame,
                        accepted=False,
                        reason=str(spec["lease_error"]),
                    )
                    raise RuntimeError(str(spec["lease_error"]))
                with control_lock:
                    registration_outcome = service.register_worker(
                        (
                            f"effect.worker-register.{current.campaign_id.value}."
                            f"{worker_id}.{identity.instance_id}"
                        ),
                        current.campaign_id,
                        identity,
                        registration.capabilities,
                        workspace=str(worker_workspace.resolve()),
                        spool_path=str(spool_root.resolve()),
                        launcher_process_id=worker.launcher_pid,
                        launcher_process_birth_token=current_process_birth_token(worker.launcher_pid),
                    )
                    if not isinstance(registration_outcome, Success):
                        worker.respond_registration(
                            registered_frame,
                            accepted=False,
                            reason=getattr(registration_outcome, "message", "worker registration rejected"),
                        )
                        self._require(registration_outcome)
                    worker_record = registration_outcome.value
                    lease_state = spec["lease_state"]
                    bound = service.bind_worker_lease(
                        f"effect.lease-bind-worker.{descriptor.shard_id.value}.{lease_state.lease.attempt}",
                        current.campaign_id,
                        lease_state.lease.lease_id,
                        identity,
                        expected_lease_revision=lease_state.lease.revision_number,
                        expected_shard_revision=lease_state.shard.revision_number,
                        expected_worker_revision=worker_record.revision_number,
                        engine_protocol_version=1,
                        workspace_backend=str(spec.get("workspace_backend") or "copy"),
                    )
                    lease_state = self._require(bound)
                    worker_record = lease_state.worker
                    if worker_record is None:
                        raise RuntimeError("lease binding lost its worker projection")
                    spec["lease_state"] = lease_state
                    spec["claimed"] = lease_state.shard
                    spec["worker_record"] = worker_record
                    start_bound_heartbeat(spec, identity, worker)
                    worker.set_heartbeat_handler(renew_from_worker)
                    worker.respond_registration(registered_frame, accepted=True)
                    registry.refresh()
                spec["worker_setup_seconds"] = (
                    float(spec.get("worker_setup_seconds", 0.0))
                    + max(0.0, time.perf_counter() - attempt_started)
                )
                worker.wait_for_frame(WorkerMessageType.ACQUIRE_ASSIGNMENT, timeout=10.0)
                expires_at = lease_state.lease.expires_at
                assignment = ShardAssignment(
                    campaign_id=current.campaign_id.value,
                    shard_id=descriptor.shard_id.value,
                    lease_id=lease_state.lease.lease_id,
                    attempt=lease_state.lease.attempt,
                    mutant_ids=tuple(str(item) for item in descriptor.mutant_ids),
                    prepared_snapshot_id=prepared_snapshot_id,
                    workspace_descriptor_id=stable_hash(
                        {"worker_id": worker_id, "workspace": str(worker_workspace)}
                    )[:32],
                    expires_at=expires_at,
                )
                execute_ids = tuple(spec["execute_mutant_ids"])
                execute_request = None
                if execute_ids:
                    execute_request = ExecuteShardRequest(
                        current.campaign_id,
                        ShardDescriptor(
                            descriptor.shard_id,
                            execute_ids,
                            estimated_cost=float(spec["execute_estimated_cost"]),
                            plan_id=campaign_plan.plan_id,
                        ),
                        attempt=lease_state.lease.attempt,
                        worker_id=lease_state.lease.worker_id,
                        lease_id=lease_state.lease.lease_id,
                        test_overrides=spec["test_overrides"],
                    ).to_dict()
                    # The engine command validates this independently from the
                    # outer worker assignment. Keep both protocol layers bound
                    # to the same immutable prepared snapshot.
                    execute_request["prepared_snapshot_id"] = prepared_snapshot_id
                test_fingerprints: dict[str, dict[str, str]] = {}
                for mutant_id in execute_ids:
                    raw_fingerprints = requests_by_mutant.get(mutant_id, {}).get("test_fingerprints", {})
                    if not isinstance(raw_fingerprints, Mapping):
                        raise RuntimeError(f"test fingerprints are missing for mutant {mutant_id}")
                    test_fingerprints[mutant_id] = {
                        str(nodeid): str(fingerprint)
                        for nodeid, fingerprint in raw_fingerprints.items()
                    }
                with control_lock:
                    if worker_record is None or lease_state.worker is None:
                        raise RuntimeError("authoritative worker lease binding is unavailable")
                    registry.refresh()
                execution_started = time.perf_counter()
                assignment_dispatch_started = time.perf_counter()
                correlation_id = worker.send_engine_assignment(
                    assignment,
                    configuration=worker_configuration.to_dict(),
                    execute_request=execute_request,
                    workspace=worker_workspace,
                    report_root=report_root,
                    publish_engine_root=canonical_engine_root,
                    expected_source_sha256=prepared.source_sha256,
                    prepared_snapshot_id=prepared_snapshot_id,
                    expected_mutant_ids=tuple(item.mutant_id.value for item in prepared.mutants),
                    test_fingerprints=test_fingerprints,
                    command_timeouts=self._engine_command_timeouts(
                        worker_configuration,
                        len(execute_ids),
                        estimated_shard_seconds=float(spec["execute_estimated_cost"]),
                        timeout_multiplier=float(campaign_plan.adaptive_policy.get("timeout_multiplier", 3.0)),
                    ),
                    engine_command=self.process_command,
                    cancel_path=self.cancel_path(configuration),
                )
                spec["assignment_correlation_id"] = correlation_id
                spec["assignment_dispatch_seconds"] = max(
                    0.0,
                    time.perf_counter() - assignment_dispatch_started,
                )
                delivery_timeout = (
                    self._engine_command_timeouts(
                        worker_configuration,
                        len(execute_ids),
                        estimated_shard_seconds=float(spec["execute_estimated_cost"]),
                        timeout_multiplier=float(campaign_plan.adaptive_policy.get("timeout_multiplier", 3.0)),
                    )["execute-shard"]
                    + 60.0
                )
                delivery_frame = worker.wait_for_frame(
                    WorkerMessageType.EXECUTION_DELIVERY,
                    timeout=delivery_timeout,
                    correlation_id=correlation_id,
                )
                delivery_payload = delivery_frame.payload
                spec["worker_execution_seconds"] = (
                    float(spec.get("worker_execution_seconds", 0.0))
                    + max(0.0, time.perf_counter() - execution_started)
                )
                if not isinstance(delivery_payload, ExecutionDelivery):
                    raise RuntimeError("worker delivery frame has an invalid typed payload")
                observed_shard_result = delivery_payload.shard_result
                runner_report_path = (
                    observed_shard_result.report_path
                    if observed_shard_result is not None
                    else None
                )
                spec["worker_execution_breakdown"] = self._worker_execution_breakdown(
                    worker_id=worker_id,
                    total_seconds=float(spec.get("worker_execution_seconds", 0.0)),
                    assignment_dispatch_seconds=float(spec.get("assignment_dispatch_seconds", 0.0)),
                    process_timeline=self._load_engine_process_timeline(worker_base),
                    runner_timeline=self._load_worker_runner_timeline(
                        runner_report_path,
                        fallback_root=canonical_engine_root,
                    ),
                )
                spec["delivery_event_id"] = delivery_payload.event_id
                stop_bound_heartbeat(spec)
                with worker.heartbeat_barrier():
                    with control_lock:
                        # Fence the durably received result before any later fan-in wait can expire ownership.
                        lease_state = spec.get("lease_state")
                        current_worker = spec.get("worker_record")
                        if lease_state is None or current_worker is None:
                            raise RuntimeError(
                                "authoritative worker lease is unavailable before durable delivery fencing"
                            )
                        delivery_started_at = utc_now()
                        delivering = service.begin_lease_delivery(
                            (
                                f"effect.lease-delivering.{worker_id}."
                                f"{descriptor.shard_id.value}."
                                f"{lease_state.lease.attempt}"
                            ),
                            current.campaign_id,
                            lease_state.lease.lease_id,
                            expected_lease_revision=lease_state.lease.revision_number,
                            expected_worker_revision=current_worker.revision_number,
                            now=delivery_started_at,
                        )
                        lease_state = self._require(delivering)
                        current_worker = lease_state.worker
                        if current_worker is None:
                            raise RuntimeError("delivery transition lost its worker projection")
                        spec["lease_state"] = lease_state
                        spec["claimed"] = lease_state.shard
                        spec["worker_record"] = current_worker
                    # DELIVERING is the durable ownership fence; later heartbeats no longer renew execution time.
                    worker.set_heartbeat_handler(None)
                return spec, observed_shard_result, None
            except Exception as exc:
                if "worker_setup_seconds" not in spec:
                    spec["worker_setup_seconds"] = max(0.0, time.perf_counter() - attempt_started)
                stop_claim_heartbeat(spec)
                stop_bound_heartbeat(spec)
                cleanup_error: str | None = None
                try:
                    worker.set_heartbeat_handler(None)
                    worker.close()
                except BaseException as cleanup_exc:
                    cleanup_error = f"{type(cleanup_exc).__name__}: {cleanup_exc}"
                error = str(exc)
                if cleanup_error is not None:
                    error = f"{error}; worker_cleanup_error={cleanup_error}"
                spec.update(data_plane_metrics)
                return spec, None, error
        def worker_death_is_confirmed(spec: Mapping[str, Any]) -> bool:
            # Require a stopped launcher and a mismatched process birth token before automatic reassignment.
            worker = spec.get("worker_process")
            if not isinstance(worker, PersistentWorkerProcess):
                return False
            if worker.returncode is None:
                return False
            worker_record = spec.get("worker_record")
            observed_identity = spec.get("observed_identity")
            identity = (
                worker_record.identity
                if worker_record is not None
                else observed_identity
            )
            if identity is None:
                return True
            observed_birth = current_process_birth_token(identity.process_id)
            return observed_birth != identity.process_birth_token
        def reassign_dead_worker(spec: dict[str, Any]) -> None:
            # Orphan the dead PID, create attempt+1 and start a fresh pre-registration lease generation.
            if self.cancel_path(configuration).is_file():
                raise RuntimeError("campaign cancellation requested")
            with control_lock:
                lease_state = spec.get("lease_state")
                if lease_state is None:
                    raise RuntimeError("dead worker has no authoritative lease state")
                worker_record = spec.get("worker_record")
                current_lease = self._require(store.get_lease(lease_state.lease.lease_id))
                if current_lease is None:
                    raise RuntimeError("dead worker lease disappeared before reassignment")
                current_shard = self._require(store.get_shard(current_lease.shard_id))
                if current_shard is None:
                    raise RuntimeError("dead worker shard disappeared before reassignment")
                if current_lease.status.value != "orphaned":
                    orphaned = service.orphan_lease_assignment(
                        (
                            f"effect.pid-reassignment.orphan.{current_lease.shard_id.value}."
                            f"{current_lease.lease_id}.r{current_lease.revision_number}"
                        ),
                        current_lease.lease_id,
                        expected_lease_revision=current_lease.revision_number,
                        expected_shard_revision=current_shard.revision_number,
                        expected_worker_revision=(
                            worker_record.revision_number if worker_record is not None else None
                        ),
                        now=utc_now(),
                        require_expired=False,
                    )
                    orphaned_state = self._require(orphaned)
                    current_shard = orphaned_state.shard
                next_attempt = current_shard.attempt + 1
                ordinal = int(spec["ordinal"])
                descriptor = spec["descriptor"]
                worker_id = WorkerId(f"local-worker-{ordinal:03d}")
                instance_id = deterministic_id(
                    "worker-instance",
                    current.campaign_id.value,
                    descriptor.shard_id.value,
                    next_attempt,
                    ordinal,
                )
                replacement = ShardLease(
                    worker_id=worker_id,
                    lease_id=(
                        f"{current.campaign_id.value}:{descriptor.shard_id.value}:attempt-{next_attempt}:"
                        f"instance-{instance_id}"
                    ),
                    status=WorkerStatus.RUNNING,
                    lease_seconds=float(runtime_configuration.budget.lease_seconds),
                    heartbeat_at=utc_now(),
                    heartbeat_seq=0,
                    attempt=next_attempt,
                    worker_instance_id=instance_id,
                )
                reassigned = service.reassign_orphaned_shard(
                    f"effect.pid-reassignment.claim.{descriptor.shard_id.value}.{next_attempt}",
                    descriptor.shard_id,
                    replacement,
                    expected_shard_revision=current_shard.revision_number,
                )
                new_state = self._require(reassigned)
                new_state = self._require(
                    service.renew_claimed_lease(
                        f"effect.pid-reassignment.renew.{descriptor.shard_id.value}.{next_attempt}.1",
                        new_state.lease.lease_id,
                        heartbeat_at=utc_now(),
                        heartbeat_sequence=1,
                        expected_lease_revision=new_state.lease.revision_number,
                        expected_shard_revision=new_state.shard.revision_number,
                        now=utc_now(),
                    )
                )
                spec["shard"] = new_state.shard
                spec["claimed"] = new_state.shard
                spec["lease_state"] = new_state
                spec["instance_id"] = instance_id
                spec["worker_record"] = None
                spec["worker_process"] = None
                spec.pop("observed_identity", None)
                spec.pop("delivery_event_id", None)
                spec.pop("assignment_correlation_id", None)
                spec.pop("lease_error", None)
                registry.refresh()
            start_claim_heartbeat(spec)
        def run_one(spec: dict[str, Any]) -> tuple[dict[str, Any], ShardExecutionResult | None, str | None]:
            # Retry one shard with a genuinely new worker PID only after the previous process death is proven.
            queued_at = spec.pop("_scheduler_queued_at", None)
            if isinstance(queued_at, (int, float)):
                spec["scheduler_queue_wait_seconds"] = max(0.0, time.perf_counter() - float(queued_at))
            attempts = 0
            while True:
                result_spec, observed, error = run_attempt(spec)
                if error is None:
                    return result_spec, observed, None
                if attempts >= 1 or not worker_death_is_confirmed(result_spec):
                    return result_spec, observed, error
                attempts += 1
                reassign_dead_worker(result_spec)
        results: list[tuple[Mapping[str, Any], ShardExecutionResult | None, str | None]] = []
        timeline.switch("worker_execution_wait")
        plan_worker_count = campaign_plan.worker_count
        if plan_worker_count != max(1, len(shard_descriptors)):
            raise RuntimeError("campaign plan shard-unit count changed before scheduler dispatch")
        scheduler_worker_count = campaign_plan.scheduler_worker_count
        dispatch_order = tuple(
            sorted(
                active,
                key=lambda spec: (-float(spec["descriptor"].estimated_cost), int(spec["ordinal"])),
            )
        )
        with ThreadPoolExecutor(max_workers=scheduler_worker_count, thread_name_prefix="theseus-worker") as executor:
            futures = []
            for spec in dispatch_order:
                spec["_scheduler_queued_at"] = time.perf_counter()
                futures.append(executor.submit(run_one, spec))
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except BaseException as exc:
                    cleanup_errors: list[str] = []
                    for active_spec in active:
                        active_worker = active_spec.get("worker_process")
                        if isinstance(active_worker, PersistentWorkerProcess):
                            try:
                                active_worker.set_heartbeat_handler(None)
                                active_worker.terminate_tree()
                                active_worker.close()
                            except BaseException as cleanup_exc:
                                cleanup_errors.append(
                                    f"worker_id={active_worker.worker_id}: "
                                    f"{type(cleanup_exc).__name__}: {cleanup_exc}"
                                )
                    if cleanup_errors and hasattr(exc, "add_note"):
                        exc.add_note("worker cleanup failures: " + "; ".join(cleanup_errors))
                    raise
        setup_values = [float(item[0].get("worker_setup_seconds", 0.0)) for item in results]
        execution_values = [float(item[0].get("worker_execution_seconds", 0.0)) for item in results]
        scheduler_metrics = self._scheduler_diagnostics(results, worker_slots=scheduler_worker_count)
        for name, value in scheduler_metrics.items():
            timeline.observe_diagnostic(name, value)
        timeline.observe_diagnostic("worker_setup_sum_seconds", sum(setup_values))
        timeline.observe_diagnostic("worker_setup_max_seconds", max(setup_values, default=0.0))
        timeline.observe_diagnostic("worker_execution_sum_seconds", sum(execution_values))
        timeline.observe_diagnostic("worker_execution_max_seconds", max(execution_values, default=0.0))
        self._publish_worker_execution_report(
            canonical_engine_root / "worker-execution.performance.json",
            results,
            timeline,
        )
        errors: list[str] = []
        for spec, observed_result, error in sorted(results, key=lambda item: int(item[0]["ordinal"])):
            timeline.switch("fan_in")
            descriptor = spec["descriptor"]
            claimed = spec["claimed"]
            worker = spec.get("worker_process")
            worker_id = str(claimed.worker_id.value)
            result_by_mutant = {item.mutant_id.value: item for item in spec["reused_results"]}
            if observed_result is not None:
                observed_by_mutant = {item.mutant_id.value: item for item in observed_result.results}
                for mutant_id, (source_record, _) in spec["partial_sources"].items():
                    observed = observed_by_mutant.get(mutant_id)
                    if observed is not None:
                        observed_by_mutant[mutant_id] = self._merge_partial_result(
                            decisions_by_mutant[mutant_id],
                            source_record,
                            observed,
                        )
                result_by_mutant.update(observed_by_mutant)
            ordered_results = tuple(
                result_by_mutant[item]
                for item in descriptor.mutant_ids
                if item in result_by_mutant
            )
            if error is not None:
                errors.append(f"{descriptor.shard_id.value}: {error}")
                shard_result = ShardExecutionResult(
                    shard_id=descriptor.shard_id,
                    worker_id=claimed.worker_id,
                    status=WorkerStatus.ERROR,
                    completed_mutants=0,
                    results=(),
                    report_path=None,
                    error=error,
                )
            elif observed_result is None:
                shard_result = ShardExecutionResult(
                    shard_id=descriptor.shard_id,
                    worker_id=claimed.worker_id,
                    status=WorkerStatus.COMPLETE,
                    completed_mutants=len(ordered_results),
                    results=ordered_results,
                    report_path="knowledge://exact-reuse",
                )
            else:
                shard_result = replace(
                    observed_result,
                    completed_mutants=len(ordered_results),
                    results=ordered_results,
                )
            try:
                if isinstance(worker, PersistentWorkerProcess):
                    with worker.heartbeat_barrier():
                        with control_lock:
                            # Durable delivery already fenced ownership before this potentially delayed fan-in.
                            fan_in_now = utc_now()
                            if spec.get("delivery_event_id"):
                                lease_state = spec.get("lease_state")
                                current_worker = spec.get("worker_record")
                                if lease_state is None or current_worker is None:
                                    raise RuntimeError(
                                        "authoritative delivery fence is unavailable before result fan-in"
                                    )
                                # The receipt cached on the attempt can be an older local projection when
                                # a heartbeat/replay crossed the delivery fence under host contention. Re-read
                                # every lease-owned projection from the durable store before fan-in; the durable
                                # lease, rather than the local receipt, is the authority for this fence.
                                authoritative_lease = self._require(
                                    service.get_lease(lease_state.lease.lease_id)
                                )
                                authoritative_shard = self._require(
                                    store.get_shard(authoritative_lease.shard_id)
                                )
                                authoritative_worker = self._require(
                                    service.get_worker(
                                        current.campaign_id,
                                        authoritative_lease.worker_id.value,
                                    )
                                )
                                if authoritative_shard is None or authoritative_worker is None:
                                    raise RuntimeError(
                                        "authoritative delivery fence projections are unavailable before result fan-in"
                                    )
                                lease_status = getattr(
                                    authoritative_lease.status,
                                    "value",
                                    authoritative_lease.status,
                                )
                                if lease_status != "delivering":
                                    raise RuntimeError(
                                        "durably received result has no authoritative delivering lease"
                                    )
                                lease_state = replace(
                                    lease_state,
                                    lease=authoritative_lease,
                                    shard=authoritative_shard,
                                    worker=authoritative_worker,
                                )
                                spec["lease_state"] = lease_state
                                spec["claimed"] = authoritative_shard
                                spec["worker_record"] = authoritative_worker
                            recorded = self._require(
                                service.record_shard_result(
                                    f"effect.execute-shard.{descriptor.shard_id.value}.{claimed.attempt}",
                                    current.campaign_id,
                                    descriptor.shard_id,
                                    shard_result,
                                    expected_campaign_revision=current.revision_number,
                                    now=fan_in_now,
                                )
                            )
                            current = recorded.campaign
                        # Keep renewal disabled after the authoritative delivery fence and fan-in commit.
                        worker.set_heartbeat_handler(None)
                else:
                    with control_lock:
                        recorded = self._require(
                            service.record_shard_result(
                                f"effect.execute-shard.{descriptor.shard_id.value}.{claimed.attempt}",
                                current.campaign_id,
                                descriptor.shard_id,
                                shard_result,
                                expected_campaign_revision=current.revision_number,
                                now=utc_now(),
                            )
                        )
                        current = recorded.campaign
                if error is not None:
                    with control_lock:
                        lease_state = spec.get("lease_state")
                        current_worker = spec.get("worker_record")
                        if lease_state is not None:
                            failed = service.fail_lease_assignment(
                                f"effect.lease-fail.{descriptor.shard_id.value}.{lease_state.lease.attempt}",
                                current.campaign_id,
                                lease_state.lease.lease_id,
                                expected_lease_revision=lease_state.lease.revision_number,
                                expected_worker_revision=(
                                    current_worker.revision_number if current_worker is not None else None
                                ),
                            )
                            lease_state = self._require(failed)
                            spec["lease_state"] = lease_state
                            spec["worker_record"] = lease_state.worker
                            registry.refresh()
                knowledge_effect_id = (
                    f"{current.campaign_id.value}:effect.execute-shard."
                    f"{descriptor.shard_id.value}.{claimed.attempt}"
                )
                knowledge_payload = {
                    "campaign": recorded.campaign.to_dict(),
                    "shard": recorded.shard.to_dict(),
                    "executions": self._annotate_knowledge_executions(
                        recorded.executions,
                        requests_by_mutant,
                        source_kind="observed",
                        reuse_matches=spec["partial_sources"],
                        root=root,
                        runtime_class=str(spec.get("runtime_class", "")) or None,
                    ),
                }
                knowledge.enrich_effect(
                    effect_id=knowledge_effect_id,
                    campaign_id=current.campaign_id.value,
                    payload=knowledge_payload,
                )
                legacy_knowledge.enrich_effect(
                    effect_id=knowledge_effect_id,
                    campaign_id=current.campaign_id.value,
                    payload=knowledge_payload,
                )
                delivery_event_id = str(spec.get("delivery_event_id", ""))
                correlation_id = str(spec.get("assignment_correlation_id", "")) or None
                if isinstance(worker, PersistentWorkerProcess) and delivery_event_id:
                    timeline.switch("worker_cleanup")
                    worker.acknowledge(delivery_event_id)
                    acknowledged_frame = worker.wait_for_frame(
                        WorkerMessageType.EXECUTION_ACKNOWLEDGED,
                        timeout=10.0,
                        correlation_id=correlation_id,
                    )
                    if not isinstance(acknowledged_frame.payload, ExecutionAcknowledged):
                        raise RuntimeError("worker acknowledgement frame has an invalid typed payload")
                    with control_lock:
                        current_worker = spec.get("worker_record")
                        if current_worker is None:
                            raise RuntimeError("worker acknowledgement has no authoritative registry row")
                        lease_state = spec.get("lease_state")
                        if lease_state is None:
                            raise RuntimeError("worker acknowledgement has no authoritative lease state")
                        released = service.release_lease_assignment(
                            (
                                f"effect.lease-release.{worker_id}."
                                f"{descriptor.shard_id.value}.{lease_state.lease.attempt}"
                            ),
                            current.campaign_id,
                            lease_state.lease.lease_id,
                            expected_lease_revision=lease_state.lease.revision_number,
                            expected_worker_revision=current_worker.revision_number,
                        )
                        lease_state = self._require(released)
                        spec["lease_state"] = lease_state
                        spec["worker_record"] = lease_state.worker
                        registry.refresh()
                    worker.wait_for_frame(WorkerMessageType.ACQUIRE_ASSIGNMENT, timeout=10.0)
                    worker.shutdown()
                    terminated_frame = worker.wait_for_frame(
                        WorkerMessageType.WORKER_TERMINATED,
                        timeout=10.0,
                    )
                    if not isinstance(terminated_frame.payload, WorkerTerminated):
                        raise RuntimeError("worker termination frame has an invalid typed payload")
                    worker.wait(timeout=10.0)
                    with control_lock:
                        current_worker = spec.get("worker_record")
                        if current_worker is None:
                            raise RuntimeError("worker termination has no authoritative registry row")
                        stopped_worker = service.stop_worker(
                            (
                                f"effect.worker-stop.{worker_id}."
                                f"{current_worker.identity.instance_id}.r{current_worker.revision_number}"
                            ),
                            current.campaign_id,
                            worker_id,
                            expected_revision=current_worker.revision_number,
                        )
                        spec["worker_record"] = self._require(stopped_worker)
                        registry.refresh()
            finally:
                if isinstance(worker, PersistentWorkerProcess):
                    timeline.switch("worker_cleanup")
                    try:
                        worker.set_heartbeat_handler(None)
                        worker.close()
                    except BaseException as cleanup_exc:
                        errors.append(
                            "worker cleanup failed: "
                            f"worker_id={worker.worker_id}; shard_id={descriptor.shard_id.value}; "
                            f"attempt={claimed.attempt}; error={type(cleanup_exc).__name__}: {cleanup_exc}"
                        )
        if errors:
            raise RuntimeError("OS worker execution failed: " + "; ".join(errors))
        return current
    @staticmethod
    def _require(result: Any) -> Any:
        # Turn typed domain rejections into one coordinator exception at the process boundary.
        if not isinstance(result, Success):
            code = getattr(result, "code", "domain_failure")
            message = getattr(result, "message", str(result))
            raise RuntimeError(f"{code}: {message}")
        return result.value
    @staticmethod
    def _canonical_markdown(report: Mapping[str, Any]) -> str:
        # Render the deterministic human-readable alias from the canonical report module.
        return canonical_markdown(report)
    def _canonical_result(
        self,
        *,
        store: SQLiteMutationStore,
        campaign: MutationCampaign,
        raw_engine_result: CampaignResult | None,
        report_path: Path,
        campaign_plan: CampaignPlan | None = None,
        publish: bool = True,
        status_override: str | None = None,
    ) -> CampaignResult:
        # Build every public projection from the validated Gallifrey execution set and publish only when requested.
        shards = self._require(store.list_shards(campaign.campaign_id))
        executions = self._require(store.list_executions(campaign.campaign_id))
        # Build the semantic projection from durable authority; the engine result is diagnostics only.
        raw_engine_report_path = (
            raw_engine_result.summary.report_path if raw_engine_result is not None else None
        )
        report = build_canonical_report(
            campaign,
            shards,
            executions,
            campaign_plan=campaign_plan,
            database_path=Path(store.path) if hasattr(store, "path") else None,
            report_path=report_path,
            raw_engine_report_path=raw_engine_report_path,
            status_override=(
                str(status_override)
                if status_override is not None
                else CampaignStatus.COMPLETE.value
                if campaign.status == CampaignState.COMPLETED
                else campaign.status.value
            ),
        )
        status = str(report["status"])
        if publish:
            atomic_write_text(
                report_path,
                canonical_json_text(report),
                durability="critical",
                category="report",
            )
            atomic_write_text(
                report_path.with_suffix(".md"),
                self._canonical_markdown(report),
                durability="critical",
                category="report",
            )
        return CampaignResult(
            summary=CampaignSummary(
                campaign_id=campaign.campaign_id,
                status=status,
                total_mutants=int(report["total_mutants"]),
                completed_mutants=int(report["completed_mutants"]),
                counts={str(key): int(value) for key, value in report["counts"].items()},
                report_path=str(report_path),
                workers=campaign_plan.worker_count if campaign_plan is not None else max(1, len(shards)),
                error=None,
            ),
            report=report,
        )
    def _finalization_sources(
        self,
        *,
        database_path: Path,
        result: CampaignResult,
    ) -> tuple[ArtifactSource, ...]:
        # Collect canonical generated bytes and every durable campaign/engine report into one immutable expected set.
        report_root = Path(database_path).resolve().parent
        reports_root = report_root.parent
        campaign_id = result.summary.campaign_id.value
        canonical_json = canonical_json_text(result.report).encode("utf-8")
        canonical_markdown = self._canonical_markdown(result.report).encode("utf-8")
        sources: list[ArtifactSource] = [
            ArtifactSource(
                logical_key="canonical.report.json",
                logical_role="canonical_report",
                logical_path=report_root / "canonical.report.json",
                producer="theseus_local.coordinator",
                schema_version=int(result.report.get("schema_version", 1)),
                payload=canonical_json,
                metadata={"media_type": "application/json"},
                required=True,
            ),
            ArtifactSource(
                logical_key="canonical.report.md",
                logical_role="canonical_report_markdown",
                logical_path=report_root / "canonical.report.md",
                producer="theseus_local.coordinator",
                schema_version=1,
                payload=canonical_markdown,
                metadata={"media_type": "text/markdown"},
                required=True,
            ),
        ]
        campaign_plan = report_root / "campaign.plan.json"
        if not campaign_plan.is_file():
            raise RuntimeError(
                "finalization requires the durable campaign plan: "
                f"campaign_id={campaign_id}; expected_path={campaign_plan}; database={database_path}"
            )
        sources.append(
            ArtifactSource(
                logical_key="campaign.plan.json",
                logical_role="campaign_plan",
                logical_path=campaign_plan,
                producer="theseus_planner",
                schema_version=1,
                source_path=campaign_plan,
                metadata={"media_type": "application/json"},
                required=True,
            )
        )
        for name, role in (
            ("reuse.plan.json", "reuse_plan"),
            ("reuse.audit.json", "reuse_audit"),
        ):
            candidate = report_root / name
            if candidate.is_file():
                sources.append(
                    ArtifactSource(
                        logical_key=f"campaign/{name}",
                        logical_role=role,
                        logical_path=candidate,
                        producer="theseus_local.coordinator",
                        schema_version=1,
                        source_path=candidate,
                        required=False,
                    )
                )
        engine_root = reports_root / "engine" / campaign_id
        if engine_root.is_dir():
            for candidate in sorted(
                (item for item in engine_root.rglob("*") if item.is_file()),
                key=lambda item: item.as_posix(),
            ):
                if candidate.name.endswith(".tmp"):
                    continue
                relative = candidate.relative_to(engine_root).as_posix()
                sources.append(
                    ArtifactSource(
                        logical_key=f"engine/{relative}",
                        logical_role="engine_artifact",
                        logical_path=candidate,
                        producer="test_intelligence_unified_v1.engine",
                        schema_version=1,
                        source_path=candidate,
                        metadata={"engine_relative_path": relative},
                        required=False,
                    )
                )
        return tuple(sources)
    @staticmethod
    def _load_canonical_result(
        report_path: Path,
        campaign: MutationCampaign,
    ) -> CampaignResult:
        # Rehydrate the already registered canonical result instead of rerunning engine finalization after recovery.
        value = read_json(report_path)
        if not isinstance(value, Mapping):
            raise RuntimeError(
                f"canonical report is missing or invalid: campaign_id={campaign.campaign_id.value}; path={report_path}"
            )
        if (
            str(value.get("campaign_id", "")) != campaign.campaign_id.value
            or str(value.get("status", "")) != CampaignStatus.COMPLETE.value
            or str(value.get("source", "")) != "gallifrey_authoritative"
        ):
            raise RuntimeError(
                "canonical report identity or status conflicts with completed campaign: "
                f"campaign_id={campaign.campaign_id.value}; campaign_status={campaign.status.value}; "
                f"report_campaign_id={value.get('campaign_id')!r}; report_status={value.get('status')!r}; "
                f"report_source={value.get('source')!r}; path={report_path}"
            )
        counts = value.get("counts", {})
        if not isinstance(counts, Mapping):
            raise RuntimeError(f"canonical report counts must be an object: path={report_path}")
        return CampaignResult(
            summary=CampaignSummary(
                campaign_id=campaign.campaign_id,
                status=CampaignStatus.COMPLETE.value,
                total_mutants=max(0, int(value.get("total_mutants", campaign.total_mutants))),
                completed_mutants=max(0, int(value.get("completed_mutants", 0))),
                counts={str(key): int(item) for key, item in counts.items()},
                report_path=str(report_path),
                workers=max(1, int(value.get("worker_count", 1))),
                error=None,
            ),
            report=dict(value),
        )
    def _finalize_with_artifact_registry(
        self,
        *,
        service: MutationCampaignService,
        store: SQLiteMutationStore,
        campaign: MutationCampaign,
        raw_engine_result: CampaignResult | None,
        database_path: Path,
        campaign_plan: CampaignPlan | None,
    ) -> tuple[MutationCampaign, CampaignResult]:
        # Replay finalization intent, publication, registry commit and atomic completion from any durable boundary.
        report_path = Path(database_path).resolve().parent / "canonical.report.json"
        current = campaign
        intent = self._require(store.get_finalization_intent(current.campaign_id))
        provisional: CampaignResult | None = None
        if current.status == CampaignState.MATERIALIZING:
            provisional = self._canonical_result(
                store=store,
                campaign=current,
                raw_engine_result=raw_engine_result,
                report_path=report_path,
                campaign_plan=campaign_plan,
                publish=False,
                status_override=CampaignStatus.COMPLETE.value,
            )
            result_fingerprint = stable_hash(provisional.to_dict())
            if intent is None:
                intent = build_finalization_intent(
                    database_path,
                    current.campaign_id,
                    self._finalization_sources(
                        database_path=database_path,
                        result=provisional,
                    ),
                    canonical_report_key="canonical.report.json",
                    result_fingerprint=result_fingerprint,
                )
                intent = self._require(
                    service.create_finalization_intent(
                        "effect.finalization.intent",
                        current.campaign_id,
                        intent,
                    )
                )
            elif intent.result_fingerprint != result_fingerprint:
                raise RuntimeError(
                    "replayed finalization result conflicts with durable intent: "
                    f"campaign_id={current.campaign_id.value}; intent_id={intent.intent_id}; "
                    f"expected_fingerprint={intent.result_fingerprint}; actual_fingerprint={result_fingerprint}"
                )
            published = publish_finalization_intent(database_path, intent)
            current = self._require(
                service.register_finalization(
                    "effect.finalization.registry",
                    current.campaign_id,
                    intent,
                    published,
                    expected_revision=current.revision_number,
                )
            )
            intent = self._require(store.get_finalization_intent(current.campaign_id))
        if current.status == CampaignState.READY_TO_COMMIT:
            if intent is None:
                raise RuntimeError(
                    "ready_to_commit campaign has no durable finalization intent: "
                    f"campaign_id={current.campaign_id.value}; database={database_path}"
                )
            publish_finalization_intent(database_path, intent)
            registry = self._require(store.list_artifacts(current.campaign_id))
            validate_registered_artifacts(database_path, intent, registry)
            current = self._require(
                service.complete_finalization(
                    "effect.finalize",
                    current.campaign_id,
                    expected_revision=current.revision_number,
                )
            )
            intent = self._require(store.get_finalization_intent(current.campaign_id))
        if current.status != CampaignState.COMPLETED:
            raise RuntimeError(
                "finalization did not reach completed: "
                f"campaign_id={current.campaign_id.value}; status={current.status.value}; database={database_path}"
            )
        if intent is None or intent.status != "completed":
            raise RuntimeError(
                "completed campaign has no completed finalization receipt: "
                f"campaign_id={current.campaign_id.value}; intent={intent}; database={database_path}"
            )
        registry = self._require(store.list_artifacts(current.campaign_id))
        validate_registered_artifacts(database_path, intent, registry)
        cleanup_finalization_staging(database_path, keep_intent_id=None)
        result = provisional or self._load_canonical_result(report_path, current)
        return current, result
    @staticmethod
    def reconcile_startup(
        database_path: Path,
        *,
        acquire_lock: bool = True,
    ) -> tuple[dict[str, Any], ...]:
        # Reconcile durable delivery, process ownership and expired leases before any new claim.
        database_path = Path(database_path).resolve()
        started_at = utc_now()
        reconciled: list[dict[str, Any]] = []
        lock_context = (
            StartupRecoveryLock(
                database_path.parent / "campaign.coordinator.lock",
                database_path=database_path,
            )
            if acquire_lock
            else nullcontext()
        )
        try:
            with lock_context:
                store = SQLiteMutationStore(database_path)
                service = MutationCampaignService(store)
                try:
                    manifests_seen: set[str] = set()
                    recovery_roots = tuple(
                        dict.fromkeys(
                            (database_path.parent, database_path.parent.parent)
                        )
                    )
                    for recovery_root in recovery_roots:
                        diagnostics = inspect_campaign_recovery(recovery_root)
                        for item in diagnostics.get("orphaned_campaigns", []):
                            manifest_value = item.get("manifest") if isinstance(item, dict) else None
                            if not manifest_value or str(manifest_value) in manifests_seen:
                                continue
                            manifests_seen.add(str(manifest_value))
                            try:
                                recover_manifest(Path(str(manifest_value)), force=False)
                                reconciled.append(
                                    {
                                        "manifest": str(manifest_value),
                                        "status": "workspace_restored",
                                    }
                                )
                            except (OSError, ValueError, RuntimeError) as exc:
                                reconciled.append(
                                    {
                                        "manifest": str(manifest_value),
                                        "status": "workspace_unresolved",
                                        "error": str(exc),
                                    }
                                )
                    campaigns = LocalCampaignCoordinator._require(store.list_campaigns())
                    for campaign in campaigns:
                        if campaign.plan_id:
                            validate_campaign_plan_topology(
                                database_path,
                                store,
                                campaign,
                                allow_unmaterialized=True,
                            )
                        else:
                            unbound_shards = LocalCampaignCoordinator._require(
                                store.list_shards(campaign.campaign_id)
                            )
                            if unbound_shards:
                                raise RuntimeError(
                                    "campaign has shard topology without an immutable plan binding: "
                                    f"campaign_id={campaign.campaign_id.value}"
                                )
                        if campaign.status not in {
                            CampaignState.COMPLETED,
                            CampaignState.FAILED,
                            CampaignState.CANCELLED,
                        }:
                            reconciled.extend(
                                replay_pending_deliveries(
                                    database_path=database_path,
                                    store=store,
                                    service=service,
                                    campaign_id=campaign.campaign_id,
                                    now=utc_now(),
                                )
                            )
                        campaign = LocalCampaignCoordinator._require(
                            store.get_campaign(campaign.campaign_id)
                        ) or campaign
                        if campaign.status in {
                            CampaignState.MATERIALIZING,
                            CampaignState.READY_TO_COMMIT,
                            CampaignState.COMPLETED,
                        }:
                            reconciled.extend(
                                replay_finalization(
                                    database_path=database_path,
                                    store=store,
                                    service=service,
                                    campaign_id=campaign.campaign_id,
                                )
                            )
                            campaign = LocalCampaignCoordinator._require(
                                store.get_campaign(campaign.campaign_id)
                            ) or campaign
                        workers = LocalCampaignCoordinator._require(
                            store.list_workers(campaign.campaign_id)
                        )
                        if workers:
                            restored_workers = []
                            for worker in workers:
                                restored = worker
                                terminal = worker.status.value in {
                                    "stopped",
                                    "failed",
                                    "orphaned",
                                }
                                terminated_processes: list[str] = []
                                observed_birth = current_process_birth_token(
                                    worker.identity.process_id
                                )
                                exact_worker_process = (
                                    observed_birth == worker.identity.process_birth_token
                                )
                                if exact_worker_process:
                                    child_process_id = worker.child_process_id
                                    child_process_birth_token = worker.child_process_birth_token
                                    if child_process_id is None:
                                        child_process_id = worker.last_child_process_id
                                        child_process_birth_token = worker.last_child_process_birth_token
                                    if terminate_recorded_process(
                                        child_process_id,
                                        child_process_birth_token,
                                    ):
                                        terminated_processes.append("engine_child")
                                    if terminate_recorded_process(
                                        worker.identity.process_id,
                                        worker.identity.process_birth_token,
                                    ):
                                        terminated_processes.append("worker")
                                    if (
                                        worker.launcher_process_id
                                        not in {None, worker.identity.process_id}
                                        and terminate_recorded_process(
                                            worker.launcher_process_id,
                                            worker.launcher_process_birth_token,
                                        )
                                    ):
                                        terminated_processes.append("launcher")
                                if not terminal:
                                    authoritative_lease = (
                                        LocalCampaignCoordinator._require(
                                            store.get_lease(worker.current_lease_id)
                                        )
                                        if worker.current_lease_id
                                        else None
                                    )
                                    if (
                                        authoritative_lease is not None
                                        and authoritative_lease.status.value
                                        not in {
                                            "released",
                                            "failed",
                                            "cancelled",
                                            "orphaned",
                                        }
                                    ):
                                        owned_shard = LocalCampaignCoordinator._require(
                                            store.get_shard(authoritative_lease.shard_id)
                                        )
                                        if owned_shard is None:
                                            raise RuntimeError(
                                                "authoritative worker lease has no shard: "
                                                f"campaign={campaign.campaign_id.value}; "
                                                f"worker={worker.worker_id}; "
                                                f"lease={authoritative_lease.lease_id}"
                                            )
                                        orphaned_state = service.orphan_lease_assignment(
                                            (
                                                f"recovery.orphan-lease.{worker.worker_id}."
                                                f"{authoritative_lease.lease_id}."
                                                f"r{authoritative_lease.revision_number}"
                                            ),
                                            authoritative_lease.lease_id,
                                            expected_lease_revision=(
                                                authoritative_lease.revision_number
                                            ),
                                            expected_shard_revision=(
                                                owned_shard.revision_number
                                            ),
                                            expected_worker_revision=worker.revision_number,
                                            now=utc_now(),
                                            require_expired=False,
                                        )
                                        if (
                                            isinstance(orphaned_state, Success)
                                            and orphaned_state.value.worker is not None
                                        ):
                                            restored = orphaned_state.value.worker
                                    else:
                                        orphaned = service.orphan_worker(
                                            (
                                                f"recovery.orphan-worker.{worker.worker_id}."
                                                f"{worker.identity.instance_id}."
                                                f"r{worker.revision_number}"
                                            ),
                                            campaign.campaign_id,
                                            worker.worker_id,
                                            expected_revision=worker.revision_number,
                                        )
                                        if isinstance(orphaned, Success):
                                            restored = orphaned.value
                                restored_workers.append(restored)
                                reconciled.append(
                                    {
                                        "campaign_id": campaign.campaign_id.value,
                                        "worker_id": restored.worker_id,
                                        "instance_id": restored.identity.instance_id,
                                        "status": (
                                            "worker_orphaned"
                                            if restored.status.value == "orphaned"
                                            and worker.status.value != "orphaned"
                                            else "worker_restored"
                                        ),
                                        "worker_state": restored.status.value,
                                        "revision_number": restored.revision_number,
                                        "process_identity": (
                                            "exact_owner_terminated"
                                            if exact_worker_process
                                            else "owner_missing_or_pid_reused"
                                            if not terminal
                                            else "terminal"
                                        ),
                                        "terminated_processes": tuple(
                                            terminated_processes
                                        ),
                                    }
                                )
                            worker_workspace = Path(
                                restored_workers[0].workspace
                            ).resolve()
                            worker_slot = next(
                                (
                                    parent
                                    for parent in worker_workspace.parents
                                    if parent.name == restored_workers[0].worker_id
                                ),
                                worker_workspace.parent,
                            )
                            registry_root = worker_slot.parent
                            WorkerRegistryProjection(
                                registry_root / "worker-registry.json",
                                campaign_id=campaign.campaign_id.value,
                                store=store,
                            ).refresh()
                        if campaign.status in {
                            CampaignState.COMPLETED,
                            CampaignState.FAILED,
                            CampaignState.CANCELLED,
                        }:
                            continue
                        shards = LocalCampaignCoordinator._require(
                            store.list_shards(campaign.campaign_id)
                        )
                        for shard in shards:
                            if shard.status not in {
                                MutationShardState.LEASED,
                                MutationShardState.RUNNING,
                            }:
                                continue
                            if not shard.lease_expired(utc_now()):
                                continue
                            authoritative_lease = (
                                LocalCampaignCoordinator._require(
                                    store.get_lease(shard.lease.lease_id)
                                )
                                if shard.lease is not None
                                else None
                            )
                            if (
                                authoritative_lease is not None
                                and authoritative_lease.status.value
                                not in {
                                    "released",
                                    "failed",
                                    "cancelled",
                                    "orphaned",
                                }
                            ):
                                owner = LocalCampaignCoordinator._require(
                                    store.get_worker(
                                        campaign.campaign_id,
                                        authoritative_lease.worker_id.value,
                                    )
                                )
                                outcome = service.orphan_lease_assignment(
                                    (
                                        f"recovery.orphan-lease."
                                        f"{shard.shard_id.value}."
                                        f"r{authoritative_lease.revision_number}"
                                    ),
                                    authoritative_lease.lease_id,
                                    expected_lease_revision=(
                                        authoritative_lease.revision_number
                                    ),
                                    expected_shard_revision=shard.revision_number,
                                    expected_worker_revision=(
                                        owner.revision_number
                                        if owner is not None
                                        else None
                                    ),
                                    now=utc_now(),
                                )
                            else:
                                outcome = service.orphan_expired_shard(
                                    (
                                        f"recovery.orphan."
                                        f"{shard.shard_id.value}."
                                        f"r{shard.revision_number}"
                                    ),
                                    shard.shard_id,
                                    expected_revision=shard.revision_number,
                                )
                            if isinstance(outcome, Success):
                                reconciled.append(
                                    {
                                        "campaign_id": campaign.campaign_id.value,
                                        "shard_id": shard.shard_id.value,
                                        "status": "orphaned",
                                        "attempt": shard.attempt,
                                    }
                                )
                    write_recovery_report(
                        database_path,
                        started_at=started_at,
                        completed_at=utc_now(),
                        status="complete",
                        actions=reconciled,
                    )
                    return tuple(reconciled)
                finally:
                    store.close()
        except Exception as exc:
            write_recovery_report(
                database_path,
                started_at=started_at,
                completed_at=utc_now(),
                status="failed",
                actions=reconciled,
                error=str(exc),
            )
            raise
    @classmethod
    def reconcile_campaign(
        cls,
        database_path: Path,
        campaign_id: str,
    ) -> tuple[dict[str, Any], ...]:
        # Reconcile one campaign database without permitting cross-campaign side effects.
        resolved_database = Path(database_path).expanduser().resolve()
        store = SQLiteMutationStore(resolved_database)
        try:
            requested = cls._require(store.get_campaign(CampaignId(str(campaign_id))))
            if requested is None:
                raise RuntimeError("campaign_not_found: durable campaign does not exist")
            campaigns = cls._require(store.list_campaigns())
            foreign = tuple(item.campaign_id.value for item in campaigns if item.campaign_id != requested.campaign_id)
            if foreign:
                raise RuntimeError("campaign_identity_mismatch: campaign database contains another identity")
        finally:
            store.close()
        return cls.reconcile_startup(resolved_database)
    @classmethod
    def recover_campaign(
        cls,
        database_path: Path,
        campaign_id: str,
        *,
        process_command: tuple[str, ...] | None = None,
    ) -> LocalCampaignResult:
        # Reconcile and resume one exact campaign under a single process-fenced ownership lock.
        resolved_database = Path(database_path).expanduser().resolve()
        lock_path = resolved_database.parent / "campaign.coordinator.lock"
        with StartupRecoveryLock(lock_path, database_path=resolved_database):
            store = SQLiteMutationStore(resolved_database)
            try:
                campaign = cls._require(store.get_campaign(CampaignId(str(campaign_id))))
                if campaign is None:
                    raise RuntimeError("campaign_not_found: durable campaign does not exist")
                campaigns = cls._require(store.list_campaigns())
                foreign = tuple(item.campaign_id.value for item in campaigns if item.campaign_id != campaign.campaign_id)
                if foreign:
                    raise RuntimeError("campaign_identity_mismatch: campaign database contains another identity")
                if campaign.status in {CampaignState.FAILED, CampaignState.CANCELLED}:
                    raise RuntimeError("campaign_not_resumable: terminal failed or cancelled campaign cannot recover")
                configuration = campaign.configuration
            finally:
                store.close()
            cls.reconcile_startup(resolved_database, acquire_lock=False)
            result = cls(process_command=process_command).run(configuration, acquire_lock=False)
            if result.database_path.resolve() != resolved_database:
                raise RuntimeError("campaign_database_mismatch: configuration resolves to another database")
            return result
    @staticmethod
    def scan_startup(root: Path) -> tuple[Path, ...]:
        # Find every durable campaign database eligible for startup reconciliation.
        return scan_campaign_databases(root)
    @classmethod
    def resume_campaign(
        cls,
        database_path: Path,
        campaign_id: str,
        *,
        process_command: tuple[str, ...] | None = None,
    ) -> LocalCampaignResult:
        # Resume one exact durable campaign through the existing coordinator and immutable plan authority.
        resolved_database = Path(database_path).expanduser().resolve()
        store = SQLiteMutationStore(resolved_database)
        try:
            campaign = cls._require(store.get_campaign(CampaignId(str(campaign_id))))
            if campaign is None:
                raise RuntimeError("campaign_not_found: durable campaign does not exist")
            if campaign.campaign_id.value != str(campaign_id):
                raise RuntimeError("campaign_identity_mismatch: durable campaign belongs to another identity")
            if campaign.status in {CampaignState.FAILED, CampaignState.CANCELLED}:
                raise RuntimeError(
                    "campaign_not_resumable: terminal failed or cancelled campaign cannot resume"
                )
            configuration = campaign.configuration
        finally:
            store.close()
        result = cls(process_command=process_command).run(configuration)
        if result.database_path.resolve() != resolved_database:
            raise RuntimeError("campaign_database_mismatch: configuration resolves to another database")
        return result
    @classmethod
    def recover_unfinished(
        cls,
        state_root: Path,
        *,
        process_command: tuple[str, ...] | None = None,
    ) -> tuple[LocalCampaignResult, ...]:
        # Reconcile every discovered database and resume each non-terminal campaign exactly once.
        results: list[LocalCampaignResult] = []
        seen: set[tuple[str, str]] = set()
        for database_path in scan_campaign_databases(state_root):
            lock_path = database_path.resolve().parent / "campaign.coordinator.lock"
            with StartupRecoveryLock(lock_path, database_path=database_path):
                cls.reconcile_startup(database_path, acquire_lock=False)
                for configuration in unfinished_campaign_configurations(database_path):
                    identity = (
                        str(database_path.resolve()),
                        configuration.campaign_id.value,
                    )
                    if identity in seen:
                        continue
                    seen.add(identity)
                    results.append(
                        cls(process_command=process_command).run(
                            configuration,
                            acquire_lock=False,
                        )
                    )
        return tuple(results)
    def run(
        self,
        configuration: CampaignConfiguration,
        *,
        acquire_lock: bool = True,
    ) -> LocalCampaignResult:
        # Resume the durable campaign state machine under one process-fenced coordinator ownership scope.
        timeline = _CoordinatorTimeline(configuration.campaign_id.value)
        timeline.switch("authority_setup")
        timeline_error: str | None = None
        provider = WorkspaceProvider(configuration)
        reports_dir = provider.reports_root / configuration.campaign_id.value
        reports_dir.mkdir(parents=True, exist_ok=True)
        provider.knowledge_root.mkdir(parents=True, exist_ok=True)
        timeline_path = reports_dir / "coordinator.performance.json"
        database_path = self.campaign_database_path(configuration)
        events_path = reports_dir / "protocol" / "events.jsonl"
        protocol_path = reports_dir / "protocol" / "responses.jsonl"
        stdout_path = reports_dir / "artifacts" / "stdout.log"
        stderr_path = reports_dir / "artifacts" / "stderr.log"
        coordinator_lock = (
            StartupRecoveryLock(
                reports_dir / "campaign.coordinator.lock",
                database_path=database_path,
            )
            if acquire_lock
            else nullcontext()
        )
        coordinator_lock.__enter__()
        sqlite_metrics = SQLiteMetrics()
        try:
            store = SQLiteMutationStore(database_path, metrics=sqlite_metrics)
            service = MutationCampaignService(store)
            knowledge = KnowledgePlaneStore(
                provider.knowledge_root
                / f"{configuration.project.project_id.value}.sqlite3",
                metrics=sqlite_metrics,
            )
            legacy_knowledge = KnowledgePlaneStore(reports_dir / "knowledge.sqlite3", metrics=sqlite_metrics)
            statistics_store = StatisticsEventStore(reports_dir / "statistics.sqlite3", metrics=sqlite_metrics)
            statistics_projection = StatisticsProjectionStore(statistics_store)
            sqlite_metrics.reset()
        except BaseException:
            coordinator_lock.__exit__(None, None, None)
            raise
        current: MutationCampaign | None = None
        engine_result: CampaignResult | None = None
        campaign_plan: CampaignPlan | None = None
        collection_snapshot_payload: Mapping[str, Any] | None = None
        try:
            timeline.switch("startup_recovery")
            self._dispatch_outbox(
                store,
                knowledge,
                legacy_knowledge,
                statistics_store,
                statistics_projection,
            )
            self.reconcile_startup(database_path, acquire_lock=False)
            existing = self._require(store.get_campaign(configuration.campaign_id))
            current = existing
            if current is not None and current.status in {
                CampaignState.COMPLETED,
                CampaignState.FAILED,
                CampaignState.CANCELLED,
            }:
                self.clear_cancel(configuration)
            if current is not None and current.status == CampaignState.COMPLETED:
                timeline.switch("finalization")
                current, engine_result = self._finalize_with_artifact_registry(
                    service=service,
                    store=store,
                    campaign=current,
                    raw_engine_result=None,
                    database_path=database_path,
                    campaign_plan=None,
                )
                timeline.switch("projection")
                self._dispatch_outbox(
                    store,
                    knowledge,
                    legacy_knowledge,
                    statistics_store,
                    statistics_projection,
                )
                return LocalCampaignResult(
                    campaign=current,
                    engine_result=engine_result,
                    database_path=database_path,
                    events_path=events_path,
                    protocol_path=protocol_path,
                    stdout_path=stdout_path,
                    stderr_path=stderr_path,
                )
            timeline.switch("workspace_setup")
            workspace = provider.prepare(configuration.campaign_id.value)
            provider.publish_compatibility_projection(workspace)
            runtime_configuration = workspace.runtime_configuration(configuration)
            preparation_configuration = replace(
                runtime_configuration,
                budget=replace(runtime_configuration.budget, max_mutants=None),
            )
            if current is None:
                current = self._require(service.create_campaign(configuration))
            cwd = workspace.workspace_root
            timeline.switch("engine_startup")
            with EngineProcessSession(
                events_path=events_path,
                protocol_path=protocol_path,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                cwd=cwd,
                command=self.process_command,
                command_timeout_seconds=self._engine_command_timeouts(
                    runtime_configuration,
                    int(configuration.budget.max_mutants or 1),
                ),
                heartbeat_interval_seconds=self._heartbeat_interval_seconds(runtime_configuration),
            ) as process:
                timeline.switch("campaign_preparation")
                prepared = PreparedCampaign.from_dict(
                    process.request("prepare", {"configuration": preparation_configuration.to_dict()})
                )
                process.command_timeout_seconds = self._engine_command_timeouts(
                    runtime_configuration,
                    len(prepared.mutants),
                )
                prepared_snapshot = self._load_prepared_snapshot(prepared, cwd)
                prepared_snapshot_id = self._prepared_snapshot_identity(
                    prepared,
                    prepared_snapshot,
                )
                if not prepared_snapshot_id:
                    raise RuntimeError(
                        "prepared campaign did not expose an immutable snapshot identity"
                    )
                if prepared.snapshot_id != prepared_snapshot_id:
                    prepared = replace(prepared, snapshot_id=prepared_snapshot_id)
                collection_artifact = reports_dir.parent / "engine" / current.campaign_id.value / "collection.snapshot.json"
                if collection_artifact.is_file():
                    try:
                        candidate = read_json(collection_artifact)
                        if isinstance(candidate, Mapping):
                            collection_snapshot_payload = candidate
                    except (OSError, TypeError, ValueError):
                        collection_snapshot_payload = None
                if current.status == CampaignState.CREATED:
                    current = self._require(
                        service.prepare(
                            "effect.prepare",
                            current.campaign_id,
                            expected_revision=current.revision_number,
                        )
                    )
                if current.status == CampaignState.PREPARING:
                    timeline.switch("test_collection")
                    collect = process.request("collect", {"campaign_id": current.campaign_id.value})
                    snapshot = collect.get("snapshot") if isinstance(collect, dict) else None
                    if isinstance(snapshot, Mapping):
                        collection_snapshot_payload = snapshot
                    snapshot_id = str(collect.get("snapshot_id")) if collect.get("snapshot_id") else None
                    if collect.get("status") not in {"complete", "no_tests"}:
                        raise RuntimeError(f"collect stage failed: {collect.get('status')}")
                    current = self._require(
                        service.complete_stage(
                            "effect.collect",
                            current.campaign_id,
                            CampaignState.COLLECTING,
                            expected_revision=current.revision_number,
                            collection_snapshot_id=snapshot_id,
                            prepared_snapshot_id=prepared_snapshot_id,
                        )
                    )
                if current.status == CampaignState.COLLECTING:
                    timeline.switch("project_index")
                    index = process.request("index", {"campaign_id": current.campaign_id.value})
                    current = self._require(
                        service.complete_stage(
                            "effect.index",
                            current.campaign_id,
                            CampaignState.INDEXING,
                            expected_revision=current.revision_number,
                            index_snapshot_id=str(index.get("snapshot_id")) if index.get("snapshot_id") else None,
                            prepared_snapshot_id=prepared_snapshot_id,
                        )
                    )
                if current.status == CampaignState.INDEXING:
                    timeline.switch("baseline")
                    baseline = process.request("baseline", {"campaign_id": current.campaign_id.value})
                    if baseline.get("status") not in {"complete", "no_l1_selection", "no_mutants"}:
                        raise RuntimeError(f"baseline stage failed: {baseline.get('status')}")
                    current = self._require(
                        service.complete_stage(
                            "effect.baseline",
                            current.campaign_id,
                            CampaignState.BASELINING,
                            expected_revision=current.revision_number,
                            baseline_snapshot_id=str(baseline.get("baseline_snapshot_id"))
                            if baseline.get("baseline_snapshot_id")
                            else None,
                            prepared_snapshot_id=prepared_snapshot_id,
                        )
                    )
                if current.status == CampaignState.BASELINING:
                    current = self._require(
                        service.discover(
                            "effect.discover.stage",
                            current.campaign_id,
                            expected_revision=current.revision_number,
                        )
                    )
                planning_from_discovery = current.status == CampaignState.DISCOVERING
                discovered_mutants: tuple[Any, ...] = ()
                if planning_from_discovery:
                    timeline.switch("mutation_discovery")
                    discovery = MutationDiscoveryResult.from_dict(
                        process.request("discover", {"campaign_id": current.campaign_id.value})
                    )
                    discovered_mutants = discovery.mutants
                timeline.switch("planning")
                planning_catalog = tuple(discovered_mutants or prepared.mutants)
                reuse_prepared = replace(prepared, mutants=planning_catalog)
                timeline.switch("planning_reuse_fingerprints")
                reuse_requests = self._reuse_requests(
                    runtime_configuration,
                    reuse_prepared,
                    collection_snapshot_payload,
                )
                reuse_path = reports_dir / "reuse.plan.json"
                campaign_plan_path = reports_dir / "campaign.plan.json"
                protect_frozen_reuse = False
                timeline.switch("planning_reuse_decisions")
                if current.plan_id or campaign_plan_path.is_file():
                    reuse_plan = self._load_reuse_plan_artifact(reuse_path)
                    protect_frozen_reuse = True
                else:
                    knowledge.invalidate_changed_inputs(
                        reuse_requests,
                        campaign_id=current.campaign_id.value,
                    )
                    reuse_mode = normalize_reuse_mode(configuration.reuse_mode)
                    reuse_plan = knowledge.write_reuse_plan(
                        reuse_path,
                        reuse_requests,
                        campaign_id=current.campaign_id.value,
                        reuse_mode=reuse_mode,
                    )
                timeline.switch("planning_adaptive_history")
                requests_by_mutant = {
                    str(request["mutant_id"]): request for request in reuse_requests
                }
                decisions_by_mutant = {item.mutant_id: item for item in reuse_plan.decisions}
                audit_policy = self._reuse_audit_policy()
                adaptive_observations = (
                    self._adaptive_planner_observations(
                        statistics_projection,
                        planning_catalog,
                        decisions_by_mutant,
                        knowledge=knowledge,
                        project_id=runtime_configuration.project.project_id.value,
                        runtime_class=current_runtime_identity().runtime_fingerprint,
                    )
                    if planning_from_discovery and not campaign_plan_path.is_file()
                    else None
                )
                timeline.switch("planning_plan_build")
                campaign_plan = self._load_or_build_campaign_plan(
                    runtime_configuration,
                    prepared,
                    reports_dir,
                    mutants=planning_catalog,
                    existing_plan_id=None if planning_from_discovery else current.plan_id,
                    reuse_decisions=decisions_by_mutant,
                    audit_policy=audit_policy,
                    adaptive_observations=adaptive_observations,
                )
                timeline.switch("planning_plan_binding")
                if protect_frozen_reuse:
                    knowledge.protect_reuse_plan(current.campaign_id.value, reuse_plan)
                planned_prepared = self._prepared_for_campaign_plan(
                    prepared,
                    campaign_plan,
                )
                timeline.switch("planning_topology_persistence")
                stored_shards = self._require(store.list_shards(current.campaign_id))
                if stored_shards:
                    if planning_from_discovery:
                        raise RuntimeError(
                            "campaign has persisted shard topology before its immutable plan binding"
                        )
                    expected_by_id = {
                        item.shard_id.value: (ordinal, item)
                        for ordinal, item in enumerate(campaign_plan.shards)
                    }
                    if len(stored_shards) != len(campaign_plan.shards):
                        raise RuntimeError(
                            "campaign plan topology is partially materialized: "
                            f"expected={len(campaign_plan.shards)}; actual={len(stored_shards)}"
                        )
                    for shard in stored_shards:
                        row = expected_by_id.get(shard.shard_id.value)
                        if row is None:
                            raise RuntimeError(
                                f"campaign topology contains unknown shard_id: {shard.shard_id.value}"
                            )
                        ordinal, descriptor = row
                        if not shard.matches_plan_descriptor(
                            campaign_plan.plan_id,
                            descriptor,
                            ordinal=ordinal,
                        ):
                            raise RuntimeError(
                                "campaign topology conflicts with immutable plan: "
                                f"shard_id={shard.shard_id.value}"
                            )
                elif planning_from_discovery or campaign_plan.shards:
                    current = self._require(
                        service.commit_plan_topology(
                            "effect.plan-topology",
                            current.campaign_id,
                            plan_id=campaign_plan.plan_id,
                            prepared_snapshot_id=str(campaign_plan.prepared_snapshot_id),
                            selected_count=campaign_plan.selected_count,
                            shard_descriptors=campaign_plan.shards,
                            expected_revision=current.revision_number,
                            plan_decisions=campaign_plan.decisions,
                            plan_artifact_sha256=campaign_plan.artifact_sha256,
                        )
                    )
                validate_campaign_plan_topology(
                    database_path,
                    store,
                    current,
                    allow_unmaterialized=False,
                )
                timeline.switch("planning_outbox_projection")
                self._dispatch_outbox(
                    store,
                    knowledge,
                    legacy_knowledge,
                    statistics_store,
                    statistics_projection,
                )
                timeline.switch("planning_audit_checkpoint")
                audit_path = reports_dir / "reuse.audit.json"
                audit_progress = self._load_reuse_audit_progress(
                    audit_path,
                    campaign_id=current.campaign_id.value,
                    reuse_plan=reuse_plan,
                    campaign_plan=campaign_plan,
                )
                self._write_reuse_audit_progress(
                    audit_path,
                    campaign_id=current.campaign_id.value,
                    reuse_plan=reuse_plan,
                    campaign_plan=campaign_plan,
                    rows=audit_progress,
                )
                timeline.switch("reuse_audit")
                audit_by_mutant: dict[str, bool] = {}
                for decision in reuse_plan.decisions:
                    audit_id = self._reuse_audit_id(reuse_plan.plan_fingerprint, decision)
                    existing_audit = audit_progress.get(audit_id)
                    if existing_audit is not None:
                        audit = dict(existing_audit)
                        if str(audit.get("status", "")) == "running":
                            failures = [str(item) for item in audit.get("failures", ()) if str(item)]
                            failures.append("audit_interrupted_before_commit")
                            audit.update(
                                {
                                    "status": "recovered_incomplete",
                                    "passed": False,
                                    "inconclusive": True,
                                    "failures": sorted(dict.fromkeys(failures)),
                                }
                            )
                            audit_progress[audit_id] = audit
                            self._write_reuse_audit_progress(
                                audit_path,
                                campaign_id=current.campaign_id.value,
                                reuse_plan=reuse_plan,
                                campaign_plan=campaign_plan,
                                rows=audit_progress,
                            )
                        audit_by_mutant[decision.mutant_id] = bool(audit.get("passed", False))
                        continue
                    record = (
                        knowledge.get_execution_record(decision.source_event_id)
                        if decision.source_event_id
                        else None
                    )
                    audit = self._audit_reuse_decision(
                        decision,
                        record,
                        requests_by_mutant.get(decision.mutant_id, {}),
                    )
                    request = requests_by_mutant.get(decision.mutant_id, {})
                    audit.update(
                        {
                            "audit_id": audit_id,
                            "source_execution_id": decision.source_execution_id,
                            "authorized": bool(decision.authorized),
                            "sampled": False,
                            "status": "complete",
                        }
                    )
                    if (
                        bool(decision.authorized)
                        and decision.kind in {ReuseKind.EXACT, ReuseKind.PARTIAL}
                        and decision.mutant_id in campaign_plan.audit_sample
                        and bool(audit.get("passed"))
                        and isinstance(record, Mapping)
                        and isinstance(record.get("payload"), Mapping)
                    ):
                        sampled = decision.mutant_id in campaign_plan.audit_sample
                        audit["sampled"] = sampled
                        if sampled:
                            audit["status"] = "running"
                            audit["passed"] = False
                            audit_progress[audit_id] = dict(audit)
                            self._write_reuse_audit_progress(
                                audit_path,
                                campaign_id=current.campaign_id.value,
                                reuse_plan=reuse_plan,
                                campaign_plan=campaign_plan,
                                rows=audit_progress,
                            )
                            fresh = self._fresh_reuse_audit_result(
                                process=process,
                                campaign=current,
                                mutant_id=decision.mutant_id,
                                request=request,
                                root=Path(runtime_configuration.project.root_path),
                            )
                            mismatches = compare_reuse_audit(record["payload"], fresh)
                            audit["fresh_execution"] = fresh
                            audit["mismatches"] = list(mismatches)
                            if not mismatches:
                                audit["passed"] = True
                                audit["status"] = "complete"
                            elif mismatches == ("infrastructure_inconclusive",):
                                audit["passed"] = False
                                audit["status"] = "inconclusive"
                                audit["inconclusive"] = True
                            else:
                                audit["passed"] = False
                                audit["status"] = "quarantined"
                                incident_id, quarantine_keys = self._quarantine_reuse_mismatch(
                                    knowledge=knowledge,
                                    campaign_id=current.campaign_id.value,
                                    decision=decision,
                                    request=request,
                                    expected=record["payload"],
                                    actual=fresh,
                                    mismatches=mismatches,
                                )
                                audit["incident_id"] = incident_id
                                audit["quarantined"] = list(quarantine_keys)
                    audit_progress[audit_id] = dict(audit)
                    self._write_reuse_audit_progress(
                        audit_path,
                        campaign_id=current.campaign_id.value,
                        reuse_plan=reuse_plan,
                        campaign_plan=campaign_plan,
                        rows=audit_progress,
                    )
                    audit_by_mutant[decision.mutant_id] = bool(audit.get("passed", False))
                shard_descriptors = campaign_plan.shards
                if shard_descriptors:
                    current = self._execute_parallel_shards(
                        service=service,
                        store=store,
                        knowledge=knowledge,
                        legacy_knowledge=legacy_knowledge,
                        current=current,
                        configuration=configuration,
                        runtime_configuration=runtime_configuration,
                        campaign_workspace=Path(runtime_configuration.project.root_path),
                        prepared=planned_prepared,
                        campaign_plan=campaign_plan,
                        requests_by_mutant=requests_by_mutant,
                        decisions_by_mutant=decisions_by_mutant,
                        audit_by_mutant=audit_by_mutant,
                        root=Path(runtime_configuration.project.root_path),
                        timeline=timeline,
                    )
                elif current.status == CampaignState.PLANNING:
                    current = self._require(
                        service.start(
                            "effect.start",
                            current.campaign_id,
                            expected_revision=current.revision_number,
                        )
                    )
                timeline.switch("aggregation")
                if current.status == CampaignState.RUNNING:
                    current = self._require(
                        service.aggregate(
                            "effect.aggregate",
                            current.campaign_id,
                            expected_revision=current.revision_number,
                        )
                    )
                if current.status == CampaignState.AGGREGATING:
                    current = self._require(
                        service.materialize(
                            "effect.materialize",
                            current.campaign_id,
                            expected_revision=current.revision_number,
                        )
                    )
                if current.status == CampaignState.MATERIALIZING:
                    timeline.switch("finalization")
                    engine_result = CampaignResult.from_dict(
                        process.request("finalize", FinalizeCampaignRequest(current.campaign_id).to_dict())
                    )
                if current.status in {
                    CampaignState.MATERIALIZING,
                    CampaignState.READY_TO_COMMIT,
                    CampaignState.COMPLETED,
                }:
                    timeline.switch("finalization")
                    current, engine_result = self._finalize_with_artifact_registry(
                        service=service,
                        store=store,
                        campaign=current,
                        raw_engine_result=engine_result,
                        database_path=database_path,
                        campaign_plan=campaign_plan,
                    )
            if current.status in {CampaignState.COMPLETED, CampaignState.FAILED, CampaignState.CANCELLED}:
                knowledge.release_reuse_plans(current.campaign_id.value)
            if current.status == CampaignState.COMPLETED:
                self.clear_cancel(configuration)
            timeline.switch("projection")
            self._dispatch_outbox(
                store,
                knowledge,
                legacy_knowledge,
                statistics_store,
                statistics_projection,
            )
            return LocalCampaignResult(
                campaign=current,
                engine_result=engine_result,
                database_path=database_path,
                events_path=events_path,
                protocol_path=protocol_path,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
            )
        except Exception as exc:
            # Keep a prepared non-terminal campaign resumable; only pre-snapshot failures become terminal.
            error = str(exc)
            timeline_error = error
            if current is None:
                current = MutationCampaign.create(configuration)
            prepared_artifact = (
                # Reuse the same resolved report root as the live run so resume decisions do not depend on cwd.
                reports_dir.parent / "engine" / configuration.campaign_id.value / "prepared.snapshot.json"
            )
            if not prepared_artifact.exists() and current.status not in {
                CampaignState.COMPLETED,
                CampaignState.FAILED,
                CampaignState.CANCELLED,
            }:
                failure = service.apply_transition(
                    "effect.failure",
                    current.campaign_id,
                    CampaignState.FAILED,
                    expected_revision=current.revision_number,
                )
                if isinstance(failure, Success):
                    current = failure.value
            if current.status in {CampaignState.COMPLETED, CampaignState.FAILED, CampaignState.CANCELLED}:
                try:
                    knowledge.release_reuse_plans(current.campaign_id.value)
                except Exception as release_error:
                    error = f"{error}; knowledge reuse plan release failed: {release_error}"
                self.clear_cancel(configuration)
            timeline_error = error
            return LocalCampaignResult(
                campaign=current,
                engine_result=engine_result,
                database_path=database_path,
                events_path=events_path,
                protocol_path=protocol_path,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                error=error,
            )
        finally:
            # Release durable stores before dropping the process-fenced coordinator ownership lock.
            timeline.switch("cleanup")
            knowledge.close()
            legacy_knowledge.close()
            store.close()
            sqlite_database_bytes = 0
            for database in (
                database_path,
                knowledge.path,
                legacy_knowledge.path,
                statistics_store.path,
            ):
                for candidate in (database, Path(f"{database}-wal"), Path(f"{database}-shm")):
                    try:
                        sqlite_database_bytes += candidate.stat().st_size
                    except OSError:
                        pass
            timeline.observe_diagnostic("sqlite_database_bytes", float(sqlite_database_bytes))
            sqlite_snapshot = sqlite_metrics.snapshot()
            for name, value in sqlite_snapshot.items():
                timeline.observe_diagnostic(name, value)
            if current is not None:
                denominator = float(max(1, int(current.total_mutants)))
                timeline.observe_diagnostic(
                    "sqlite_queries_per_mutant",
                    sqlite_snapshot["sqlite_query_count"] / denominator,
                )
                timeline.observe_diagnostic(
                    "sqlite_writes_per_mutant",
                    sqlite_snapshot["sqlite_write_count"] / denominator,
                )
            coordinator_lock.__exit__(None, None, None)
            timeline_status = (
                str(getattr(current.status, "value", current.status))
                if current is not None
                else "failed"
            )
            try:
                timeline.finish(timeline_path, status=timeline_status, error=timeline_error)
            except (OSError, TypeError, ValueError, RuntimeError) as timeline_exc:
                try:
                    stderr_path.parent.mkdir(parents=True, exist_ok=True)
                    with stderr_path.open("a", encoding="utf-8") as handle:
                        handle.write(
                            "coordinator performance timeline publication failed: "
                            f"{type(timeline_exc).__name__}: {timeline_exc}\n"
                        )
                except OSError:
                    pass
    async def run_async(self, configuration: CampaignConfiguration) -> LocalCampaignResult:
        # Offer an async application boundary while isolating blocking SQLite and process I/O.
        return await asyncio.to_thread(self.run, configuration)
