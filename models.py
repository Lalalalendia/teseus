"""Small, JSON-friendly data contracts shared by the unified tool."""
from __future__ import annotations
from collections import Counter
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Mapping
WRITE_CATEGORIES = (
    "source_mutation",
    "recovery",
    "manifest",
    "result_journal",
    "state_checkpoint",
    "report",
    "baseline_artifact",
    "stats_event",
    "stats_database",
    "other",
)
class TestOutcome(str, Enum):
    """Canonical outcome values stored in the test execution journal."""
    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    SKIPPED = "skipped"
    XFAILED = "xfailed"
    XPASSED = "xpassed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"
class TestHealthStatus(str, Enum):
    """Derived health categories exposed by test statistics and reports."""
    HEALTHY = "healthy"
    FLAKY = "flaky"
    FAILING = "failing"
    ERRORING = "erroring"
    MOSTLY_SKIPPED = "mostly_skipped"
    NEVER_PASSED = "never_passed"
    INSUFFICIENT_DATA = "insufficient_data"
    UNKNOWN = "unknown"
@dataclass
class PerformanceMetrics:
    """Counters and phase timings collected without changing campaign semantics."""
    index_seconds: float = 0.0
    selection_seconds: float = 0.0
    snapshot_seconds: float = 0.0
    mutant_generation_seconds: float = 0.0
    baseline_seconds: float = 0.0
    mutation_preparation_seconds: float = 0.0
    mutant_apply_seconds: float = 0.0
    pytest_seconds: float = 0.0
    pytest_wrapper_seconds: float = 0.0
    restore_seconds: float = 0.0
    report_write_seconds: float = 0.0
    selection_audit_seconds: float = 0.0
    subprocess_elapsed_seconds: float = 0.0
    subprocess_output_bytes: int = 0
    processes_started: int = 0
    process_timeouts: int = 0
    process_tree_leaks: int = 0
    bytes_written: int = 0
    source_bytes_read: int = 0
    report_bytes_written: int = 0
    critical_writes: int = 0
    normal_writes: int = 0
    critical_bytes_written: int = 0
    normal_bytes_written: int = 0
    baseline_cache_hits: int = 0
    baseline_cache_misses: int = 0
    source_mutation_writes: int = 0
    source_mutation_bytes: int = 0
    recovery_writes: int = 0
    recovery_bytes: int = 0
    manifest_writes: int = 0
    manifest_bytes: int = 0
    result_journal_writes: int = 0
    result_journal_bytes: int = 0
    state_checkpoint_writes: int = 0
    state_checkpoint_bytes: int = 0
    report_writes: int = 0
    report_bytes: int = 0
    baseline_artifact_writes: int = 0
    baseline_artifact_bytes: int = 0
    stats_event_writes: int = 0
    stats_event_bytes: int = 0
    stats_database_writes: int = 0
    stats_database_bytes: int = 0
    other_writes: int = 0
    other_bytes: int = 0
    campaign_wall_seconds: float = 0.0
    preparation_seconds: float = 0.0
    mutant_execution_seconds: float = 0.0
    test_stats_ingestion_seconds: float = 0.0
    health_aggregation_seconds: float = 0.0
    report_materialization_seconds: float = 0.0
    worker_setup_seconds: float = 0.0
    worker_teardown_seconds: float = 0.0
    coordinator_wall_seconds: float = 0.0
    worker_sum_seconds: float = 0.0
    worker_max_seconds: float = 0.0
    worker_critical_path_seconds: float = 0.0
    workspace_setup_seconds: float = 0.0
    workspace_teardown_seconds: float = 0.0
    shared_baseline_count: int = 0
    reused_baseline_count: int = 0
    index_build_count: int = 0
    def record_write(self, size: int, *, durability: str, category: str = "other") -> None:
        # Count one write in an explicit category while retaining legacy totals.
        self.bytes_written += size
        self.report_bytes_written += size
        normalized_category = category if category in WRITE_CATEGORIES else "other"
        setattr(
            self,
            f"{normalized_category}_writes",
            getattr(self, f"{normalized_category}_writes") + 1,
        )
        setattr(
            self,
            f"{normalized_category}_bytes",
            getattr(self, f"{normalized_category}_bytes") + size,
        )
        if durability == "critical":
            self.critical_writes += 1
            self.critical_bytes_written += size
        else:
            self.normal_writes += 1
            self.normal_bytes_written += size
    def to_dict(self) -> dict[str, Any]:
        # Serialize metrics with derived phase totals for stable report consumers.
        payload = asdict(self)
        if not self.preparation_seconds:
            payload["preparation_seconds"] = (
                self.index_seconds
                + self.selection_seconds
                + self.snapshot_seconds
                + self.mutant_generation_seconds
            )
        if not self.mutant_execution_seconds:
            payload["mutant_execution_seconds"] = (
                self.mutant_apply_seconds + self.pytest_seconds + self.restore_seconds
            )
        if not self.report_materialization_seconds:
            payload["report_materialization_seconds"] = self.report_write_seconds
        worker_phases = {
            "runner_index": max(0.0, float(self.index_seconds)),
            "runner_selection": max(0.0, float(self.selection_seconds)),
            "runner_snapshot": max(0.0, float(self.snapshot_seconds)),
            "runner_mutant_generation": max(0.0, float(self.mutant_generation_seconds)),
            "mutation_preparation": max(0.0, float(self.mutation_preparation_seconds)),
            "mutation_application": max(0.0, float(self.mutant_apply_seconds)),
            "pytest_process": max(0.0, float(self.pytest_seconds)),
            "pytest_wrapper": max(0.0, float(self.pytest_wrapper_seconds)),
            "test_stats_ingestion": max(0.0, float(self.test_stats_ingestion_seconds)),
            "source_restoration": max(0.0, float(self.restore_seconds)),
            "health_aggregation": max(0.0, float(self.health_aggregation_seconds)),
            "report_publication": max(0.0, float(self.report_write_seconds)),
        }
        worker_observed = sum(worker_phases.values())
        worker_total = max(0.0, float(self.campaign_wall_seconds))
        worker_residual = max(0.0, worker_total - worker_observed)
        payload["worker_execution_timeline"] = {
            "schema_version": 1,
            "timeline_version": "runner-worker-exclusive-v1",
            "exclusive": True,
            "total_wall_seconds": worker_total,
            "observed_phase_seconds": worker_observed,
            "residual_seconds": worker_residual,
            "accounted_seconds": worker_observed + worker_residual,
            "accounting_error_seconds": abs(worker_total - worker_observed - worker_residual),
            "phases": [
                {"phase": phase, "wall_seconds": seconds, "source": "runner.performance"}
                for phase, seconds in worker_phases.items()
            ]
            + [
                {
                    "phase": "runner_unattributed_residual",
                    "wall_seconds": worker_residual,
                    "source": "runner.reconciliation",
                }
            ],
        }
        return payload
@dataclass
class CampaignAccumulator:
    """Incremental mutation-result counters used by report checkpoints."""
    counts: Counter[str] = field(default_factory=Counter)
    selection_sources: Counter[str] = field(default_factory=Counter)
    operator_stats: dict[str, dict[str, Any]] = field(default_factory=dict)
    dropped_nodeids: int = 0
    l1_survivors_checked: int = 0
    total_results: int = 0
    configured_test_count: int = 0
    selected_tests_total: int = 0
    candidate_tests_total: int = 0
    selection_decisions: int = 0
    escalated_mutants: int = 0
    safe_fallbacks: int = 0
    def add_result(self, result: dict[str, Any]) -> None:
        # Update all report counters from one completed mutant in constant time.
        self.total_results += 1
        status = str(result.get("status", "error"))
        self.counts[status] += 1
        if any(
            level.get("level") == "L1" and level.get("result", {}).get("passed")
            for level in result.get("level_results", [])
            if isinstance(level, dict)
        ):
            self.l1_survivors_checked += 1
        selection = result.get("selection")
        if isinstance(selection, dict):
            levels = selection.get("levels")
            if isinstance(levels, list) and levels and isinstance(levels[0], dict):
                self.selection_sources[str(levels[0].get("source", "unknown"))] += 1
                dropped = levels[0].get("dropped_nodeids", [])
                if isinstance(dropped, list):
                    self.dropped_nodeids += len(dropped)
                first_level = levels[0]
                selected = first_level.get("nodeids", [])
                if isinstance(selected, list):
                    self.selected_tests_total += len(selected)
                self.candidate_tests_total += max(0, int(first_level.get("candidate_count", 0) or 0))
                self.selection_decisions += 1
                if bool(first_level.get("fallback_reason")) or first_level.get("source") == "domain-fallback":
                    self.safe_fallbacks += 1
            level_results = result.get("level_results", [])
            if isinstance(level_results, list) and any(
                isinstance(level, dict) and str(level.get("level")) != "L1" for level in level_results
            ):
                self.escalated_mutants += 1
        mutant = result.get("mutant")
        if not isinstance(mutant, dict):
            return
        operator = str(mutant.get("mutation", "unknown"))
        row = self.operator_stats.setdefault(
            operator,
            {
                "operator_version": str(mutant.get("operator_version", "unknown")),
                "total_mutants": 0,
                "counts": {},
            },
        )
        row["total_mutants"] += 1
        row["counts"][status] = int(row["counts"].get(status, 0)) + 1
    def metric_payload(self) -> dict[str, Any]:
        # Materialize the bounded aggregate without scanning prior mutant results.
        operators: dict[str, dict[str, Any]] = {}
        for operator, value in self.operator_stats.items():
            row = dict(value)
            row["counts"] = dict(value.get("counts", {}))
            killed = int(row["counts"].get("killed", 0))
            survivors = int(row["counts"].get("survived", 0))
            row["mutation_score"] = killed / (killed + survivors) if killed + survivors else None
            operators[operator] = row
        killed = int(self.counts.get("killed", 0))
        survivors = int(self.counts.get("survived", 0))
        escapes = int(self.counts.get("selection_escape", 0))
        denominator = self.configured_test_count * self.total_results
        return {
            "counts": dict(self.counts),
            "mutation_score": killed / (killed + survivors) if killed + survivors else None,
            "selection_escapes": escapes,
            "l1_survivors_checked": self.l1_survivors_checked,
            "selection_precision": 1.0 - escapes / self.l1_survivors_checked if self.l1_survivors_checked else None,
            "selection_sources": dict(self.selection_sources),
            "selected_tests_total": self.selected_tests_total,
            "candidate_tests_total": self.candidate_tests_total,
            "tests_executed_per_mutant": (
                self.selected_tests_total / self.total_results if self.total_results else None
            ),
            "selected_tests_ratio": self.selected_tests_total / denominator if denominator else None,
            "escalation_rate": self.escalated_mutants / self.selection_decisions if self.selection_decisions else None,
            "safe_fallbacks": self.safe_fallbacks,
            "operator_stats": dict(sorted(operators.items())),
            "dropped_nodeids": self.dropped_nodeids,
            "total_mutants": self.total_results,
        }
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "CampaignAccumulator":
        # Restore checkpoint counters so resume can process only the journal suffix.
        counts = value.get("counts", {})
        selection_sources = value.get("selection_sources", {})
        operator_stats = value.get("operator_stats", {})
        return cls(
            counts=Counter({str(key): int(item) for key, item in counts.items()})
            if isinstance(counts, Mapping)
            else Counter(),
            selection_sources=Counter({str(key): int(item) for key, item in selection_sources.items()})
            if isinstance(selection_sources, Mapping)
            else Counter(),
            operator_stats={str(key): dict(item) for key, item in operator_stats.items() if isinstance(item, Mapping)}
            if isinstance(operator_stats, Mapping)
            else {},
            dropped_nodeids=max(0, int(value.get("dropped_nodeids", 0))),
            l1_survivors_checked=max(0, int(value.get("l1_survivors_checked", 0))),
            total_results=max(0, int(value.get("total_mutants", value.get("total_results", 0)))),
            configured_test_count=max(0, int(value.get("configured_test_count", 0) or 0)),
            selected_tests_total=max(0, int(value.get("selected_tests_total", 0) or 0)),
            candidate_tests_total=max(0, int(value.get("candidate_tests_total", 0) or 0)),
            selection_decisions=max(0, int(value.get("selection_decisions", 0) or 0)),
            escalated_mutants=max(0, int(value.get("escalated_mutants", 0) or 0)),
            safe_fallbacks=max(0, int(value.get("safe_fallbacks", 0) or 0)),
        )
    def checkpoint_dict(self) -> dict[str, Any]:
        # Serialize the complete bounded accumulator used by checkpoint v2.
        return {
            "counts": dict(self.counts),
            "selection_sources": dict(self.selection_sources),
            "operator_stats": {
                str(key): {
                    **dict(item),
                    "counts": dict(item.get("counts", {})),
                }
                for key, item in self.operator_stats.items()
            },
            "dropped_nodeids": self.dropped_nodeids,
            "l1_survivors_checked": self.l1_survivors_checked,
            "total_results": self.total_results,
            "configured_test_count": self.configured_test_count,
            "selected_tests_total": self.selected_tests_total,
            "candidate_tests_total": self.candidate_tests_total,
            "selection_decisions": self.selection_decisions,
            "escalated_mutants": self.escalated_mutants,
            "safe_fallbacks": self.safe_fallbacks,
        }
@dataclass(frozen=True)
class TerminationResult:
    """Diagnostics for a timeout cleanup attempt."""
    requested: bool
    tree_kill_succeeded: bool
    parent_kill_succeeded: bool
    return_code: int | None
    error: str | None = None
@dataclass(frozen=True)
class FunctionInfo:
    function_id: str
    rel_path: str
    qualname: str
    name: str
    class_name: str | None
    start_line: int
    end_line: int
    is_method: bool
    is_async: bool
    risk_score: float = 0.0
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
@dataclass(frozen=True)
class TestInfo:
    nodeid: str
    rel_path: str
    name: str
    class_name: str | None
    start_line: int
    end_line: int
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
@dataclass(frozen=True)
class CollectionSnapshot:
    """Authoritative pytest collection inventory for one environment."""
    collection_id: str
    revision: str | None
    environment_fingerprint: str
    pytest_version: str | None
    plugin_fingerprint: str
    nodeids: tuple[str, ...]
    collection_errors: tuple[str, ...] = ()
    created_at: str = ""
    def to_dict(self) -> dict[str, Any]:
        # Serialize the collection inventory without exposing tuple fields.
        return asdict(self) | {
            "nodeids": list(self.nodeids),
            "collection_errors": list(self.collection_errors),
        }
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "CollectionSnapshot":
        # Restore a collection snapshot while tolerating compatible extra fields.
        return cls(
            collection_id=str(value.get("collection_id", "")),
            revision=str(value["revision"]) if value.get("revision") is not None else None,
            environment_fingerprint=str(value.get("environment_fingerprint", "")),
            pytest_version=str(value["pytest_version"]) if value.get("pytest_version") is not None else None,
            plugin_fingerprint=str(value.get("plugin_fingerprint", "")),
            nodeids=tuple(str(item) for item in value.get("nodeids", [])),
            collection_errors=tuple(str(item) for item in value.get("collection_errors", [])),
            created_at=str(value.get("created_at", "")),
        )
SELECTION_EVIDENCE_SOURCES = (
    "runtime_line",
    "runtime_branch",
    "runtime_function",
    "historical_kill",
    "static_dependency",
    "static_direct",
    "static_probable",
    "domain_fallback",
    "manual",
)
@dataclass(frozen=True)
class SelectionEvidence:
    """Provenance attached to one selected nodeid or escalation level."""
    source: str
    source_snapshot: str = ""
    revision: str = ""
    environment: str = ""
    confidence_class: str = "heuristic"
    detail: str = ""
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "SelectionEvidence":
        # Restore evidence defensively so older snapshots remain readable.
        source = str(value.get("source", "static_probable"))
        if source not in SELECTION_EVIDENCE_SOURCES:
            source = "static_probable"
        return cls(
            source=source,
            source_snapshot=str(value.get("source_snapshot", "")),
            revision=str(value.get("revision", "")),
            environment=str(value.get("environment", "")),
            confidence_class=str(value.get("confidence_class", "heuristic")),
            detail=str(value.get("detail", "")),
        )
    def to_dict(self) -> dict[str, str]:
        # Keep evidence JSON-friendly while retaining every provenance dimension.
        return {
            "source": self.source,
            "source_snapshot": self.source_snapshot,
            "revision": self.revision,
            "environment": self.environment,
            "confidence_class": self.confidence_class,
            "detail": self.detail,
        }
def selection_evidence_source(reason: str) -> str:
    # Normalize legacy planner reasons into the finite E-12 evidence vocabulary.
    value = str(reason).strip().lower()
    if value in {"selected-tests-file", "frozen-selection", "manual"}:
        return "manual"
    if "historical" in value or "kill" in value:
        return "historical_kill"
    if "line" in value:
        return "runtime_line"
    if "branch" in value or "context" in value:
        return "runtime_branch"
    if "function" in value:
        return "runtime_function"
    if "dependency" in value or "impact-graph" in value:
        return "static_dependency"
    if "static-name" in value:
        return "static_direct"
    if "domain" in value or "fallback" in value or value == "common":
        return "domain_fallback"
    return "static_probable"
def selection_evidence_confidence(source: str) -> str:
    # Map evidence provenance to a small, stable confidence class.
    return {
        "runtime_line": "exact",
        "runtime_branch": "strong",
        "runtime_function": "strong",
        "historical_kill": "strong",
        "static_dependency": "heuristic",
        "static_direct": "heuristic",
        "static_probable": "heuristic",
        "domain_fallback": "fallback",
        "manual": "exact",
    }.get(source, "heuristic")
def make_selection_evidence(
    reason: str,
    *,
    source_snapshot: str,
    revision: str,
    environment: str,
    detail: str = "",
) -> SelectionEvidence:
    # Create one normalized evidence row from a legacy reason and campaign identity.
    source = selection_evidence_source(reason)
    return SelectionEvidence(
        source=source,
        source_snapshot=source_snapshot,
        revision=revision,
        environment=environment,
        confidence_class=selection_evidence_confidence(source),
        detail=detail or str(reason),
    )
@dataclass(frozen=True)
class SelectionLevel:
    name: str
    reason: str
    nodeids: tuple[str, ...] = ()
    files: tuple[str, ...] = ()
    command_argv: tuple[str, ...] = ()
    def to_dict(self) -> dict[str, Any]:
        return asdict(self) | {
            "nodeids": list(self.nodeids),
            "files": list(self.files),
            "command_argv": list(self.command_argv),
        }
@dataclass(frozen=True)
class SelectionSnapshot:
    schema_version: int
    snapshot_id: str
    created_at: str
    project_root: str
    source_path: str
    function_id: str | None
    source_sha256: str
    map_version: str
    levels: tuple[SelectionLevel, ...]
    selected_tests: tuple[str, ...] = ()
    reasons: dict[str, list[str]] = field(default_factory=dict)
    impact_status: str = "missing"
    impact_schema_version: int | None = None
    impact_warning: str | None = None
    impact_error: str | None = None
    algorithm_version: str = "selection-v5"
    index_version: str | None = None
    test_config_fingerprint: str | None = None
    dropped_nodeids: tuple[str, ...] = ()
    evidence: dict[str, tuple[SelectionEvidence, ...]] = field(default_factory=dict)
    def to_dict(self) -> dict[str, Any]:
        # Serialize the frozen selection and freshness diagnostics.
        return {
            "schema_version": self.schema_version,
            "snapshot_id": self.snapshot_id,
            "created_at": self.created_at,
            "project_root": self.project_root,
            "source_path": self.source_path,
            "function_id": self.function_id,
            "source_sha256": self.source_sha256,
            "map_version": self.map_version,
            "levels": [level.to_dict() for level in self.levels],
            "selected_tests": list(self.selected_tests),
            "reasons": self.reasons,
            "impact_status": self.impact_status,
            "impact_schema_version": self.impact_schema_version,
            "impact_warning": self.impact_warning,
            "impact_error": self.impact_error,
            "algorithm_version": self.algorithm_version,
            "index_version": self.index_version,
            "test_config_fingerprint": self.test_config_fingerprint,
            "dropped_nodeids": list(self.dropped_nodeids),
            "evidence": {
                str(nodeid): [item.to_dict() for item in rows]
                for nodeid, rows in self.evidence.items()
            },
        }
@dataclass(frozen=True)
class Mutant:
    mutant_id: str
    mutation: str
    line_no: int
    column_no: int
    original: str
    replacement: str
    start: int
    end: int
    operator_version: str = "m2"
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
@dataclass(frozen=True)
class ProcessResult:
    argv: tuple[str, ...]
    cwd: str
    exit_code: int | None
    elapsed_seconds: float
    timed_out: bool
    output: str
    retry: bool = False
    output_artifact: str | None = None
    output_bytes: int = 0
    output_sha256: str | None = None
    output_head: str = ""
    output_tail: str = ""
    process_tree_leak: bool = False
    termination: dict[str, Any] | None = None
    infrastructure_flags: tuple[str, ...] = ()
    diagnostic_excerpts: tuple[str, ...] = ()
    @property
    def passed(self) -> bool:
        return self.exit_code == 0 and not self.timed_out
    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        # Full subprocess output belongs in the artifact, never in the report.
        result.pop("output", None)
        return result | {
            "argv": list(self.argv),
            "passed": self.passed,
            "infrastructure_flags": list(self.infrastructure_flags),
            "diagnostic_excerpts": list(self.diagnostic_excerpts),
        }
@dataclass(frozen=True)
class MutantResult:
    mutant: dict[str, Any]
    status: str
    classification_reason: str
    level_results: tuple[dict[str, Any], ...]
    source_sha256_before: str
    source_sha256_after: str
    restore_sha256: str
    restore_verified: bool
    output_artifacts: tuple[str, ...] = ()
    selection: dict[str, Any] | None = None
    duration_seconds: float | None = None
    def to_dict(self) -> dict[str, Any]:
        # Serialize tuple fields while retaining selection provenance.
        return asdict(self) | {
            "level_results": list(self.level_results),
            "output_artifacts": list(self.output_artifacts),
        }
@dataclass(frozen=True)
class MutantSelection:
    """Ranked tests selected for one mutant line."""
    mutant_id: str
    line_no: int
    nodeids: tuple[str, ...]
    source: str
    confidence: float
    reasons: dict[str, list[str]] = field(default_factory=dict)
    proof_level: str = "insufficient"
    proof_sources: tuple[str, ...] = ()
    requires_escalation: bool = False
    candidate_count: int = 0
    fallback_reason: str | None = None
    scores: dict[str, float] = field(default_factory=dict)
    dropped_nodeids: tuple[str, ...] = ()
    evidence: dict[str, tuple[SelectionEvidence, ...]] = field(default_factory=dict)
    def to_dict(self) -> dict[str, Any]:
        # Serialize selection provenance without exposing internal tuple types.
        return asdict(self) | {
            "nodeids": list(self.nodeids),
            "dropped_nodeids": list(self.dropped_nodeids),
            "proof_sources": list(self.proof_sources),
            "evidence": {
                str(nodeid): [item.to_dict() for item in rows]
                for nodeid, rows in self.evidence.items()
            },
        }
