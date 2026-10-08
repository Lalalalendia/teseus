"""Durable performance measurement authority for offline Theseus workloads."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from statistics import median
import time
import tracemalloc
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Literal, Mapping, Protocol, Sequence

from .accounting import reconcile_exclusive_timeline

PERFORMANCE_SCHEMA_VERSION = 1
PERFORMANCE_AUTHORITY_VERSION = "performance-authority-v1"
PERFORMANCE_AGGREGATION_VERSION = "performance-aggregation-v1"
PERFORMANCE_QUERY_MAX_LIMIT = 500
PERFORMANCE_MIN_REPETITIONS = 3
MetricStatus = Literal["observed", "unavailable", "not_applicable"]
RunStatus = Literal["completed", "failed", "cancelled"]
STANDARD_PHASES = (
    "project_scan",
    "test_collection",
    "mutation_discovery",
    "planning",
    "workspace_preparation",
    "execution",
    "fan_in",
    "projection",
    "finalization",
)
STANDARD_METRICS = {
    "wall_seconds": "seconds",
    "cpu_seconds": "seconds",
    "peak_memory_bytes": "bytes",
    "sqlite_query_count": "count",
    "sqlite_write_count": "count",
    "process_spawn_count": "count",
    "worker_utilization": "ratio",
    "queue_wait_seconds": "seconds",
    "reuse_hit_rate": "ratio",
    "selected_tests_ratio": "ratio",
}


class PerformanceAuthorityError(RuntimeError):
    """Base error for invalid or conflicting performance evidence."""


class PerformanceConflictError(PerformanceAuthorityError):
    """Raised when one durable identity is reused for different content."""


def _canonical_json(value: object) -> str:
    # Serialize one authority payload deterministically for hashing and persistence.
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _digest(value: object) -> str:
    # Hash one canonical JSON value without depending on process-local object identity.
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _non_empty(value: object, field_name: str) -> str:
    # Normalize one required public identity and reject empty values early.
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _finite_non_negative(value: object, field_name: str) -> float:
    # Normalize one non-negative finite numeric measurement without accepting booleans.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be numeric")
    normalized = float(value)
    if normalized < 0.0 or normalized in {float("inf"), float("-inf")} or normalized != normalized:
        raise ValueError(f"{field_name} must be finite and non-negative")
    return normalized


@dataclass(frozen=True, slots=True)
class WorkloadIdentity:
    """Stable comparison boundary for one benchmark or campaign workload."""

    workload_name: str
    workload_version: str
    project_key: str
    input_fingerprint: str
    environment_fingerprint: str
    runtime_fingerprint: str

    def __post_init__(self) -> None:
        # Validate every identity component before it can select a performance baseline.
        for name in (
            "workload_name",
            "workload_version",
            "project_key",
            "input_fingerprint",
            "environment_fingerprint",
            "runtime_fingerprint",
        ):
            object.__setattr__(self, name, _non_empty(getattr(self, name), name))

    @property
    def comparison_key(self) -> str:
        # Bind comparisons to exact workload, project, input, environment and runtime identities.
        return _digest(self.to_dict())

    def to_dict(self) -> dict[str, str]:
        # Convert the immutable comparison identity to canonical JSON-compatible data.
        return {
            "workload_name": self.workload_name,
            "workload_version": self.workload_version,
            "project_key": self.project_key,
            "input_fingerprint": self.input_fingerprint,
            "environment_fingerprint": self.environment_fingerprint,
            "runtime_fingerprint": self.runtime_fingerprint,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "WorkloadIdentity":
        # Restore one comparison identity from persisted canonical data.
        return cls(
            workload_name=str(value.get("workload_name", "")),
            workload_version=str(value.get("workload_version", "")),
            project_key=str(value.get("project_key", "")),
            input_fingerprint=str(value.get("input_fingerprint", "")),
            environment_fingerprint=str(value.get("environment_fingerprint", "")),
            runtime_fingerprint=str(value.get("runtime_fingerprint", "")),
        )


@dataclass(frozen=True, slots=True)
class PerformanceMetric:
    """One measured, unavailable or inapplicable performance value."""

    name: str
    unit: str
    status: MetricStatus
    value: float | None = None
    source: str = ""
    reason: str | None = None

    def __post_init__(self) -> None:
        # Require explicit evidence status so missing measurements are never represented as zero.
        object.__setattr__(self, "name", _non_empty(self.name, "metric.name"))
        object.__setattr__(self, "unit", _non_empty(self.unit, "metric.unit"))
        if self.status not in {"observed", "unavailable", "not_applicable"}:
            raise ValueError("metric.status is unsupported")
        if self.status == "observed":
            object.__setattr__(self, "value", _finite_non_negative(self.value, "metric.value"))
            object.__setattr__(self, "source", _non_empty(self.source, "metric.source"))
            if self.reason is not None:
                raise ValueError("observed metric cannot carry an unavailable reason")
        else:
            if self.value is not None:
                raise ValueError("unobserved metric cannot carry a numeric value")
            object.__setattr__(self, "reason", _non_empty(self.reason, "metric.reason"))

    def to_dict(self) -> dict[str, object]:
        # Serialize one typed metric without collapsing missing evidence into a numeric default.
        return {
            "name": self.name,
            "unit": self.unit,
            "status": self.status,
            "value": self.value,
            "source": self.source,
            "reason": self.reason,
        }

    @classmethod
    def observed(cls, name: str, value: float | int, unit: str, source: str) -> "PerformanceMetric":
        # Build one observed metric from an explicit measurement source.
        return cls(name=name, unit=unit, status="observed", value=float(value), source=source)

    @classmethod
    def unavailable(cls, name: str, unit: str, reason: str) -> "PerformanceMetric":
        # Build one explicit unavailable metric instead of inventing a zero measurement.
        return cls(name=name, unit=unit, status="unavailable", reason=reason)

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "PerformanceMetric":
        # Restore one typed metric from durable JSON data.
        raw_value = value.get("value")
        return cls(
            name=str(value.get("name", "")),
            unit=str(value.get("unit", "")),
            status=str(value.get("status", "unavailable")),
            value=float(raw_value) if raw_value is not None else None,
            source=str(value.get("source", "")),
            reason=str(value["reason"]) if value.get("reason") is not None else None,
        )


@dataclass(frozen=True, slots=True)
class PhaseMeasurement:
    """Wall, CPU and memory evidence for one named execution phase."""

    phase: str
    wall_seconds: PerformanceMetric
    cpu_seconds: PerformanceMetric
    peak_memory_bytes: PerformanceMetric

    def __post_init__(self) -> None:
        # Keep phase identities stable and metric names scoped to the phase contract.
        object.__setattr__(self, "phase", _non_empty(self.phase, "phase"))
        if (
            self.wall_seconds.name != "wall_seconds"
            or self.cpu_seconds.name != "cpu_seconds"
            or self.peak_memory_bytes.name != "peak_memory_bytes"
        ):
            raise ValueError("phase metrics must use canonical metric names")

    @property
    def status(self) -> MetricStatus:
        # Summarize the phase as observed only when its wall-clock evidence exists.
        return self.wall_seconds.status

    def to_dict(self) -> dict[str, object]:
        # Serialize one phase without flattening its resource evidence.
        return {
            "phase": self.phase,
            "status": self.status,
            "metrics": {
                "wall_seconds": self.wall_seconds.to_dict(),
                "cpu_seconds": self.cpu_seconds.to_dict(),
                "peak_memory_bytes": self.peak_memory_bytes.to_dict(),
            },
        }

    @classmethod
    def unavailable(cls, phase: str, reason: str) -> "PhaseMeasurement":
        # Build one explicit unavailable standard phase for incomplete legacy instrumentation.
        return cls(
            phase=phase,
            wall_seconds=PerformanceMetric.unavailable("wall_seconds", "seconds", reason),
            cpu_seconds=PerformanceMetric.unavailable("cpu_seconds", "seconds", reason),
            peak_memory_bytes=PerformanceMetric.unavailable("peak_memory_bytes", "bytes", reason),
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "PhaseMeasurement":
        # Restore one phase measurement from persisted metric objects.
        raw_metrics = value.get("metrics", {})
        metrics = raw_metrics if isinstance(raw_metrics, Mapping) else {}
        return cls(
            phase=str(value.get("phase", "")),
            wall_seconds=PerformanceMetric.from_dict(_mapping(metrics.get("wall_seconds"))),
            cpu_seconds=PerformanceMetric.from_dict(_mapping(metrics.get("cpu_seconds"))),
            peak_memory_bytes=PerformanceMetric.from_dict(_mapping(metrics.get("peak_memory_bytes"))),
        )


def _mapping(value: object) -> Mapping[str, object]:
    # Require one mapping while decoding durable authority records.
    if not isinstance(value, Mapping):
        raise ValueError("expected an object")
    return value


@dataclass(frozen=True, slots=True)
class PerformanceRun:
    """Immutable performance record stored and compared by the authority."""

    run_id: str
    identity: WorkloadIdentity
    status: RunStatus
    started_at: str
    completed_at: str
    phases: tuple[PhaseMeasurement, ...]
    metrics: tuple[PerformanceMetric, ...]
    metadata: Mapping[str, object] = field(default_factory=dict)
    schema_version: int = PERFORMANCE_SCHEMA_VERSION
    authority_version: str = PERFORMANCE_AUTHORITY_VERSION

    def __post_init__(self) -> None:
        # Validate uniqueness and freeze metadata before the record enters durable history.
        object.__setattr__(self, "run_id", _non_empty(self.run_id, "run_id"))
        object.__setattr__(self, "started_at", _non_empty(self.started_at, "started_at"))
        object.__setattr__(self, "completed_at", _non_empty(self.completed_at, "completed_at"))
        if self.status not in {"completed", "failed", "cancelled"}:
            raise ValueError("run status is unsupported")
        if self.schema_version != PERFORMANCE_SCHEMA_VERSION:
            raise ValueError("performance schema version is unsupported")
        if self.authority_version != PERFORMANCE_AUTHORITY_VERSION:
            raise ValueError("performance authority version is unsupported")
        phase_names = tuple(item.phase for item in self.phases)
        metric_names = tuple(item.name for item in self.metrics)
        if len(phase_names) != len(set(phase_names)):
            raise ValueError("phase identities must be unique")
        if len(metric_names) != len(set(metric_names)):
            raise ValueError("metric identities must be unique")
        frozen_metadata = dict(self.metadata)
        try:
            _canonical_json(frozen_metadata)
        except (TypeError, ValueError) as exc:
            raise ValueError("performance metadata must be JSON-compatible") from exc
        object.__setattr__(self, "metadata", frozen_metadata)

    @property
    def content_sha256(self) -> str:
        # Hash the complete immutable run for exact replay and conflict detection.
        return _digest(self.to_dict())

    def metric_map(self) -> dict[str, PerformanceMetric]:
        # Expose global and phase wall metrics through one comparison namespace.
        result = {item.name: item for item in self.metrics}
        for phase in self.phases:
            result[f"phase.{phase.phase}.wall_seconds"] = PerformanceMetric(
                name=f"phase.{phase.phase}.wall_seconds",
                unit=phase.wall_seconds.unit,
                status=phase.wall_seconds.status,
                value=phase.wall_seconds.value,
                source=phase.wall_seconds.source,
                reason=phase.wall_seconds.reason,
            )
        return result

    def to_dict(self) -> dict[str, object]:
        # Serialize the complete authority record in deterministic field order.
        return {
            "schema_version": self.schema_version,
            "authority_version": self.authority_version,
            "run_id": self.run_id,
            "identity": self.identity.to_dict(),
            "comparison_key": self.identity.comparison_key,
            "status": self.status,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "phases": [item.to_dict() for item in self.phases],
            "metrics": [item.to_dict() for item in self.metrics],
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "PerformanceRun":
        # Restore one complete run while revalidating every nested contract.
        raw_phases = value.get("phases", ())
        raw_metrics = value.get("metrics", ())
        if not isinstance(raw_phases, Sequence) or isinstance(raw_phases, (str, bytes)):
            raise ValueError("phases must be an array")
        if not isinstance(raw_metrics, Sequence) or isinstance(raw_metrics, (str, bytes)):
            raise ValueError("metrics must be an array")
        return cls(
            run_id=str(value.get("run_id", "")),
            identity=WorkloadIdentity.from_dict(_mapping(value.get("identity"))),
            status=str(value.get("status", "failed")),
            started_at=str(value.get("started_at", "")),
            completed_at=str(value.get("completed_at", "")),
            phases=tuple(PhaseMeasurement.from_dict(_mapping(item)) for item in raw_phases),
            metrics=tuple(PerformanceMetric.from_dict(_mapping(item)) for item in raw_metrics),
            metadata=_mapping(value.get("metadata", {})),
            schema_version=int(value.get("schema_version", 0)),
            authority_version=str(value.get("authority_version", "")),
        )


@dataclass(frozen=True, slots=True)
class PerformanceMetricSummary:
    """Distribution of one metric across exact-identity repetitions."""

    name: str
    unit: str
    status: Literal["observed", "partial", "unavailable"]
    sample_count: int
    observed_count: int
    unavailable_count: int
    minimum: float | None
    median: float | None
    maximum: float | None
    spread: float | None
    relative_spread: float | None
    sources: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        # Validate aggregate counts and never represent missing samples as numeric zeroes.
        object.__setattr__(self, "name", _non_empty(self.name, "summary.name"))
        object.__setattr__(self, "unit", _non_empty(self.unit, "summary.unit"))
        if self.status not in {"observed", "partial", "unavailable"}:
            raise ValueError("summary.status is unsupported")
        for name in ("sample_count", "observed_count", "unavailable_count"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.sample_count != self.observed_count + self.unavailable_count:
            raise ValueError("summary sample counts do not reconcile")
        if self.observed_count == 0:
            if any(value is not None for value in (self.minimum, self.median, self.maximum, self.spread)):
                raise ValueError("unavailable summary cannot carry numeric distribution values")
            if self.relative_spread is not None:
                raise ValueError("unavailable summary cannot carry relative spread")
        else:
            for name in ("minimum", "median", "maximum", "spread"):
                value = getattr(self, name)
                if value is None:
                    raise ValueError(f"observed summary is missing {name}")
                _finite_non_negative(value, f"summary.{name}")
            if self.minimum > self.median or self.median > self.maximum:
                raise ValueError("summary distribution ordering is invalid")
            if self.spread != self.maximum - self.minimum:
                raise ValueError("summary spread does not match min/max")
            if self.relative_spread is not None:
                _finite_non_negative(self.relative_spread, "summary.relative_spread")
        object.__setattr__(self, "sources", tuple(sorted(set(str(item) for item in self.sources if str(item)))))

    def to_dict(self) -> dict[str, object]:
        # Serialize a bounded distribution; individual samples remain available through run_ids.
        return {
            "name": self.name,
            "unit": self.unit,
            "status": self.status,
            "sample_count": self.sample_count,
            "observed_count": self.observed_count,
            "unavailable_count": self.unavailable_count,
            "min": self.minimum,
            "median": self.median,
            "max": self.maximum,
            "spread": self.spread,
            "relative_spread": self.relative_spread,
            "sources": list(self.sources),
        }


@dataclass(frozen=True, slots=True)
class PerformanceRepetitionSummary:
    """Authoritative median summary for one exact workload identity."""

    identity: WorkloadIdentity
    run_ids: tuple[str, ...]
    metrics: tuple[PerformanceMetricSummary, ...]
    phases: tuple[PerformanceMetricSummary, ...]
    accounting: Mapping[str, object]
    minimum_repetitions: int = PERFORMANCE_MIN_REPETITIONS
    representative_run_id: str = ""
    aggregation_version: str = PERFORMANCE_AGGREGATION_VERSION

    def __post_init__(self) -> None:
        # Keep summaries comparable only inside one immutable workload identity.
        run_ids = tuple(_non_empty(item, "run_id") for item in self.run_ids)
        if not run_ids or len(run_ids) != len(set(run_ids)):
            raise ValueError("repetition summary requires unique non-empty run IDs")
        if isinstance(self.minimum_repetitions, bool) or not isinstance(self.minimum_repetitions, int):
            raise ValueError("minimum_repetitions must be an integer")
        if self.minimum_repetitions < 1:
            raise ValueError("minimum_repetitions must be positive")
        metric_names = tuple(item.name for item in self.metrics)
        phase_names = tuple(item.name for item in self.phases)
        if len(metric_names) != len(set(metric_names)):
            raise ValueError("summary metric identities must be unique")
        if len(phase_names) != len(set(phase_names)):
            raise ValueError("summary phase identities must be unique")
        if self.aggregation_version != PERFORMANCE_AGGREGATION_VERSION:
            raise ValueError("performance aggregation version is unsupported")
        representative = self.representative_run_id or run_ids[0]
        if representative not in run_ids:
            raise ValueError("representative_run_id must reference one repetition")
        object.__setattr__(self, "run_ids", run_ids)
        object.__setattr__(self, "representative_run_id", representative)
        try:
            _canonical_json(dict(self.accounting))
        except (TypeError, ValueError) as exc:
            raise ValueError("performance accounting summary must be JSON-compatible") from exc
        object.__setattr__(self, "accounting", dict(self.accounting))

    @property
    def sample_count(self) -> int:
        # Expose the exact number of physical repetitions represented by this summary.
        return len(self.run_ids)

    @property
    def repetition_status(self) -> Literal["sufficient", "insufficient_repetitions"]:
        # Mark one-sample and two-sample reports explicitly instead of implying statistical confidence.
        return "sufficient" if self.sample_count >= self.minimum_repetitions else "insufficient_repetitions"

    def metric_value(self, name: str) -> float | None:
        # Read the median only when at least one repetition produced observed evidence.
        for item in self.metrics:
            if item.name == name:
                return item.median
        for item in self.phases:
            if item.name == name:
                return item.median
        return None
    @property
    def run_id(self) -> str:
        # Provide one stable physical reference for legacy comparison projections.
        return self.representative_run_id


    def to_dict(self) -> dict[str, object]:
        # Serialize the bounded authority summary while keeping physical run records addressable.
        return {
            "aggregation_version": self.aggregation_version,
            "identity": self.identity.to_dict(),
            "comparison_key": self.identity.comparison_key,
            "workload_name": self.identity.workload_name,
            "run_ids": list(self.run_ids),
            "representative_run_id": self.representative_run_id,
            "sample_count": self.sample_count,
            "minimum_repetitions": self.minimum_repetitions,
            "repetition_status": self.repetition_status,
            "metrics": [item.to_dict() for item in self.metrics],
            "phases": [item.to_dict() for item in self.phases],
            "accounting": dict(self.accounting),
        }


def evaluate_phase_accounting(
    run: PerformanceRun,
    *,
    max_residual_ratio: float = 0.10,
    max_accounting_error_seconds: float = 0.001,
) -> dict[str, object]:
    # Reconcile exclusive phase evidence before evaluating residual accounting.
    """Evaluate whether observed phases explain one run without a large residual bucket."""
    max_residual_ratio = _finite_non_negative(max_residual_ratio, "max_residual_ratio")
    max_accounting_error_seconds = _finite_non_negative(
        max_accounting_error_seconds,
        "max_accounting_error_seconds",
    )
    metric_map = run.metric_map()

    def observed_value(name: str) -> float | None:
        # Read one observed timeline metric without turning unavailable evidence into zero.
        metric = metric_map.get(name)
        return float(metric.value) if metric is not None and metric.status == "observed" and metric.value is not None else None

    timeline_seconds = observed_value("coordinator_timeline_wall_seconds")
    accounted_seconds = observed_value("phase_accounted_seconds")
    residual_seconds = observed_value("phase_residual_seconds")
    wall_seconds = observed_value("wall_seconds")
    if timeline_seconds is None or accounted_seconds is None or residual_seconds is None:
        return {
            "status": "insufficient_evidence",
            "reason": "exclusive coordinator timeline accounting was not observed",
            "wall_seconds": wall_seconds,
            "coordinator_timeline_seconds": timeline_seconds,
            "accounted_seconds": accounted_seconds,
            "residual_seconds": residual_seconds,
            "residual_ratio": None,
            "accounting_error_seconds": None,
            "wrapper_overhead_seconds": observed_value("benchmark_wrapper_overhead_seconds"),
            "wrapper_overhead_ratio": None,
            "max_residual_ratio": max_residual_ratio,
        }
    phase_seconds = sum(
        float(item.wall_seconds.value or 0.0)
        for item in run.phases
        if item.wall_seconds.status == "observed" and "unattributed" not in item.phase.lower()
    )
    accounting_error = abs(timeline_seconds - accounted_seconds)
    residual_ratio = residual_seconds / timeline_seconds if timeline_seconds > 0.0 else 0.0
    wrapper_seconds = observed_value("benchmark_wrapper_overhead_seconds")
    wrapper_ratio = (
        wrapper_seconds / wall_seconds
        if wrapper_seconds is not None and wall_seconds is not None and wall_seconds > 0.0
        else None
    )
    reconciliation = reconcile_exclusive_timeline(
        timeline_seconds,
        (
            *({"phase": item.phase, "wall_seconds": float(item.wall_seconds.value or 0.0)} for item in run.phases if item.wall_seconds.status == "observed" and "unattributed" not in item.phase.lower()),
            {"phase": "authority_unattributed_residual", "wall_seconds": residual_seconds},
        ),
        tolerance_seconds=max_accounting_error_seconds,
    )
    phase_sum_error = reconciliation.accounting_error_seconds
    failed_reason: str | None = None
    if accounting_error > max_accounting_error_seconds or reconciliation.status != "passed":
        failed_reason = "exclusive phase durations do not reconcile with coordinator wall time"
    elif residual_ratio > max_residual_ratio:
        failed_reason = "unattributed residual exceeds the configured authority threshold"
    return {
        "status": "failed" if failed_reason else "passed",
        "reason": failed_reason,
        "wall_seconds": wall_seconds,
        "coordinator_timeline_seconds": timeline_seconds,
        "known_phase_seconds": phase_seconds,
        "accounted_seconds": accounted_seconds,
        "residual_seconds": residual_seconds,
        "residual_ratio": residual_ratio,
        "accounting_error_seconds": accounting_error,
        "wrapper_overhead_seconds": wrapper_seconds,
        "wrapper_overhead_ratio": wrapper_ratio,
        "max_residual_ratio": max_residual_ratio,
    }


def _summarize_metric_samples(name: str, samples: Sequence[PerformanceMetric]) -> PerformanceMetricSummary:
    # Aggregate only observed values and preserve unavailable repetition count and provenance.
    if not samples:
        raise ValueError("metric summary requires at least one sample")
    units = {item.unit for item in samples}
    if len(units) != 1:
        raise PerformanceAuthorityError(f"metric unit changed across repetitions: {name}")
    values = [float(item.value) for item in samples if item.status == "observed" and item.value is not None]
    observed_count = len(values)
    unavailable_count = len(samples) - observed_count
    minimum = min(values) if values else None
    maximum = max(values) if values else None
    middle = float(median(values)) if values else None
    spread = maximum - minimum if values else None
    relative_spread = (
        spread / middle
        if spread is not None and middle is not None and middle > 0.0
        else 0.0
        if spread == 0.0
        else None
    )
    return PerformanceMetricSummary(
        name=name,
        unit=next(iter(units)),
        status=("observed" if observed_count == len(samples) else "partial" if observed_count else "unavailable"),
        sample_count=len(samples),
        observed_count=observed_count,
        unavailable_count=unavailable_count,
        minimum=minimum,
        median=middle,
        maximum=maximum,
        spread=spread,
        relative_spread=relative_spread,
        sources=tuple(item.source for item in samples if item.status == "observed"),
    )


def summarize_performance_runs(
    runs: Sequence[PerformanceRun],
    *,
    minimum_repetitions: int = PERFORMANCE_MIN_REPETITIONS,
) -> PerformanceRepetitionSummary:
    # Aggregate repeated exact-identity runs before selecting the representative measurement.
    """Aggregate exact-identity physical runs into one auditable median summary."""
    items = tuple(runs)
    if not items:
        raise ValueError("performance repetition summary requires at least one run")
    if isinstance(minimum_repetitions, bool) or not isinstance(minimum_repetitions, int) or minimum_repetitions < 1:
        raise ValueError("minimum_repetitions must be a positive integer")
    if any(not isinstance(item, PerformanceRun) for item in items):
        raise TypeError("performance repetition summary accepts PerformanceRun values only")
    comparison_key = items[0].identity.comparison_key
    if any(item.identity.comparison_key != comparison_key for item in items[1:]):
        raise PerformanceConflictError("cannot aggregate runs with different workload identities")
    run_ids = tuple(item.run_id for item in items)
    if len(run_ids) != len(set(run_ids)):
        raise PerformanceConflictError("cannot aggregate one run identity more than once")
    metric_maps = [item.metric_map() for item in items]
    metric_names = sorted(set().union(*(mapping.keys() for mapping in metric_maps)))
    metric_summaries: list[PerformanceMetricSummary] = []
    for name in metric_names:
        present = next((mapping[name] for mapping in metric_maps if name in mapping), None)
        if present is None:
            continue
        samples = [
            mapping.get(name)
            or PerformanceMetric.unavailable(name, present.unit, "metric was not present in this repetition")
            for mapping in metric_maps
        ]
        metric_summaries.append(_summarize_metric_samples(name, samples))
    phase_names = sorted(set().union(*(item.phase for run in items for item in run.phases)))
    phase_summaries: list[PerformanceMetricSummary] = []
    for phase_name in phase_names:
        samples = []
        for run in items:
            phase = next((item for item in run.phases if item.phase == phase_name), None)
            samples.append(
                phase.wall_seconds
                if phase is not None
                else PerformanceMetric.unavailable(
                    "wall_seconds",
                    "seconds",
                    "phase was not present in this repetition",
                )
            )
        phase_summaries.append(
            _summarize_metric_samples(f"phase.{phase_name}.wall_seconds", samples)
        )
    accounting_runs = tuple(evaluate_phase_accounting(item) for item in items)
    passed_count = sum(item.get("status") == "passed" for item in accounting_runs)
    failed_count = sum(item.get("status") == "failed" for item in accounting_runs)
    observed_residuals = [
        float(item["residual_ratio"])
        for item in accounting_runs
        if isinstance(item.get("residual_ratio"), (int, float))
        and not isinstance(item.get("residual_ratio"), bool)
    ]
    if failed_count:
        accounting_status = "failed"
    elif passed_count == len(accounting_runs):
        accounting_status = "passed"
    else:
        accounting_status = "insufficient_evidence"
    accounting: dict[str, object] = {
        "status": accounting_status,
        "sample_count": len(accounting_runs),
        "passed_count": passed_count,
        "failed_count": failed_count,
        "insufficient_count": len(accounting_runs) - passed_count - failed_count,
        "max_residual_ratio": max(observed_residuals) if observed_residuals else None,
        "median_residual_ratio": float(median(observed_residuals)) if observed_residuals else None,
        "runs": [dict(item) for item in accounting_runs],
    }
    wall_values = [
        float(mapping["wall_seconds"].value)
        for mapping in metric_maps
        if mapping.get("wall_seconds") is not None
        and mapping["wall_seconds"].status == "observed"
        and mapping["wall_seconds"].value is not None
    ]
    target_median = float(median(wall_values)) if wall_values else None
    # Select the repetition closest to the exact-identity median wall time.
    def wall_distance(item: PerformanceRun) -> float:
        # Measure one repetition's distance from the median wall-clock sample.
        metric = item.metric_map().get("wall_seconds")
        value = float(metric.value) if metric is not None and metric.value is not None else float("inf")
        return abs(value - target_median) if target_median is not None else float("inf")
    representative = min(items, key=wall_distance)
    return PerformanceRepetitionSummary(
        identity=items[0].identity,
        run_ids=run_ids,
        metrics=tuple(metric_summaries),
        phases=tuple(phase_summaries),
        accounting=accounting,
        minimum_repetitions=minimum_repetitions,
        representative_run_id=representative.run_id,
    )


@dataclass(frozen=True, slots=True)
class RegressionBudget:
    """Explicit opt-in threshold for one comparable observed metric."""

    metric_name: str
    direction: Literal["lower_is_better", "higher_is_better"] = "lower_is_better"
    max_relative_regression: float | None = None
    max_absolute_regression: float | None = None

    def __post_init__(self) -> None:
        # Reject empty, directionless and negative thresholds before comparison.
        object.__setattr__(self, "metric_name", _non_empty(self.metric_name, "metric_name"))
        if self.direction not in {"lower_is_better", "higher_is_better"}:
            raise ValueError("regression budget direction is unsupported")
        if self.max_relative_regression is None and self.max_absolute_regression is None:
            raise ValueError("at least one regression threshold is required")
        for name in ("max_relative_regression", "max_absolute_regression"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _finite_non_negative(value, name))


@dataclass(frozen=True, slots=True)
class RegressionFinding:
    """Comparison result for one explicitly budgeted metric."""

    metric_name: str
    status: Literal["passed", "regressed", "insufficient_evidence"]
    baseline_value: float | None
    current_value: float | None
    delta: float | None
    relative_delta: float | None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class RegressionReport:
    """Pinned-baseline evaluation that never blocks without an explicit budget."""

    comparison_key: str
    run_id: str
    baseline_run_id: str | None
    status: Literal["observe_only", "no_baseline", "passed", "regressed", "insufficient_evidence"]
    findings: tuple[RegressionFinding, ...]

    @property
    def blocking(self) -> bool:
        # Block only an explicit regression verdict produced from observed comparable evidence.
        return self.status == "regressed"
@dataclass(frozen=True, slots=True)
class InvariantFinding:
    """One hard correctness/integrity invariant evaluated alongside performance evidence."""

    name: str
    status: Literal["passed", "failed", "insufficient_evidence"]
    reason: str | None = None
    evidence: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Hard-invariant findings are explicit so missing evidence cannot silently look like a pass.
        object.__setattr__(self, "name", _non_empty(self.name, "invariant.name"))
        if self.status not in {"passed", "failed", "insufficient_evidence"}:
            raise ValueError("invariant status is unsupported")
        frozen_evidence = dict(self.evidence)
        try:
            _canonical_json(frozen_evidence)
        except (TypeError, ValueError) as exc:
            raise ValueError("invariant evidence must be JSON-compatible") from exc
        object.__setattr__(self, "evidence", frozen_evidence)

    @property
    def blocking(self) -> bool:
        # Only a witnessed invariant violation blocks immediately; missing evidence remains visible as a warning.
        return self.status == "failed"

    def to_dict(self) -> dict[str, object]:
        # Serialize the invariant finding for durable governance reports.
        return {
            "name": self.name,
            "status": self.status,
            "blocking": self.blocking,
            "reason": self.reason,
            "evidence": dict(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class OptimizationGateReport:
    """Governance result combining hard invariants with explicitly measured performance evidence."""

    status: Literal["passed", "warning", "blocked"]
    performance: RegressionReport
    invariants: tuple[InvariantFinding, ...]
    reasons: tuple[str, ...] = ()
    stable_regression_threshold: float = 0.10

    def __post_init__(self) -> None:
        # Keep one deterministic gate decision for CI and operator review.
        if self.status not in {"passed", "warning", "blocked"}:
            raise ValueError("optimization gate status is unsupported")
        threshold = _finite_non_negative(self.stable_regression_threshold, "stable_regression_threshold")
        object.__setattr__(self, "stable_regression_threshold", threshold)
        invariants = tuple(self.invariants)
        names = tuple(item.name for item in invariants)
        if len(names) != len(set(names)):
            raise ValueError("optimization invariant names must be unique")
        object.__setattr__(self, "invariants", invariants)
        object.__setattr__(self, "reasons", tuple(_non_empty(str(item), "gate.reason") for item in self.reasons))

    @property
    def blocking(self) -> bool:
        # A gate blocks on a hard invariant failure or a configured/stable performance regression.
        return self.status == "blocked"

    def to_dict(self) -> dict[str, object]:
        # Publish the evidence needed to audit why optimization was accepted, warned or blocked.
        return {
            "status": self.status,
            "blocking": self.blocking,
            "comparison_key": self.performance.comparison_key,
            "run_id": self.performance.run_id,
            "baseline_run_id": self.performance.baseline_run_id,
            "performance_status": self.performance.status,
            "invariants": [item.to_dict() for item in self.invariants],
            "reasons": list(self.reasons),
            "stable_regression_threshold": self.stable_regression_threshold,
        }


def evaluate_optimization_gate(
    performance: RegressionReport,
    *,
    invariants: Sequence[InvariantFinding] = (),
    stable_regression_threshold: float = 0.10,
) -> OptimizationGateReport:
    # Apply hard invariants and measured regression policy at the optimization boundary.
    """Apply PR84 governance: hard failures block, small drift warns, missing baseline never blocks."""
    threshold = _finite_non_negative(stable_regression_threshold, "stable_regression_threshold")
    normalized_invariants = tuple(invariants)
    if any(not isinstance(item, InvariantFinding) for item in normalized_invariants):
        raise TypeError("optimization gate invariants must be InvariantFinding values")
    failed = [item for item in normalized_invariants if item.status == "failed"]
    incomplete = [item for item in normalized_invariants if item.status == "insufficient_evidence"]
    stable_regressions = [
        item for item in performance.findings
        if item.status == "passed" and item.relative_delta is not None and item.relative_delta > threshold
    ]
    small_drifts = [
        item for item in performance.findings
        if item.status == "passed" and item.relative_delta is not None and 0.0 < item.relative_delta <= threshold
    ]
    reasons: list[str] = [f"hard invariant failed: {item.name}" for item in failed]
    reasons.extend(f"hard invariant evidence incomplete: {item.name}" for item in incomplete)
    if performance.blocking:
        reasons.append("explicit performance budget regressed")
    reasons.extend(f"stable performance regression exceeds {threshold:.3f}: {item.metric_name}" for item in stable_regressions)
    reasons.extend(f"small performance drift: {item.metric_name}" for item in small_drifts)
    if performance.status in {"no_baseline", "observe_only", "insufficient_evidence"}:
        reasons.append(f"performance evidence status: {performance.status}")
    if failed or performance.blocking or stable_regressions:
        status: Literal["passed", "warning", "blocked"] = "blocked"
    elif incomplete or small_drifts or performance.status in {"no_baseline", "observe_only", "insufficient_evidence"}:
        status = "warning"
    else:
        status = "passed"
    return OptimizationGateReport(
        status=status,
        performance=performance,
        invariants=normalized_invariants,
        reasons=tuple(reasons),
        stable_regression_threshold=threshold,
    )


class PerformanceClock(Protocol):
    """Clock boundary used by deterministic collector tests and production timing."""

    def perf_counter(self) -> float:
        # Return one monotonic wall-clock sample.
        ...

    def process_time(self) -> float:
        # Return one process CPU-time sample.
        ...


class PerformanceCollector:
    """Low-overhead collector for one serially measured workload."""

    def __init__(
        self,
        identity: WorkloadIdentity,
        *,
        run_id: str | None = None,
        clock: PerformanceClock = time,
    ) -> None:
        # Capture total clocks and start a private memory trace only when no trace already exists.
        self.identity = identity
        self.run_id = run_id or f"perf-{uuid.uuid4().hex}"
        self._clock = clock
        self._started_wall = float(clock.perf_counter())
        self._started_cpu = float(clock.process_time())
        self._started_at = _utc_timestamp()
        self._phases: dict[str, PhaseMeasurement] = {}
        self._metrics: dict[str, PerformanceMetric] = {}
        self._active_phase: str | None = None
        self._finished = False
        self._owns_trace = not tracemalloc.is_tracing()
        if self._owns_trace:
            tracemalloc.start()
            tracemalloc.reset_peak()

    def record_phase(self, measurement: PhaseMeasurement) -> None:
        # Record one externally measured phase without replacing conflicting evidence.
        if self._active_phase is not None:
            raise PerformanceAuthorityError("cannot record a phase while another phase is active")
        existing = self._phases.get(measurement.phase)
        if existing is not None and existing != measurement:
            raise PerformanceConflictError(f"phase already recorded with different content: {measurement.phase}")
        self._phases[measurement.phase] = measurement

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        # Measure one non-overlapping phase and retain wall, CPU and owned memory evidence.
        phase_name = _non_empty(name, "phase")
        if self._finished:
            raise PerformanceAuthorityError("cannot record a phase after collector finish")
        if self._active_phase is not None:
            raise PerformanceAuthorityError("performance phases cannot overlap")
        if phase_name in self._phases:
            raise PerformanceAuthorityError(f"performance phase already recorded: {phase_name}")
        self._active_phase = phase_name
        started_wall = float(self._clock.perf_counter())
        started_cpu = float(self._clock.process_time())
        if self._owns_trace:
            tracemalloc.reset_peak()
        try:
            yield
        finally:
            wall = max(0.0, float(self._clock.perf_counter()) - started_wall)
            cpu = max(0.0, float(self._clock.process_time()) - started_cpu)
            memory = (
                PerformanceMetric.observed(
                    "peak_memory_bytes",
                    float(tracemalloc.get_traced_memory()[1]),
                    "bytes",
                    "collector.tracemalloc",
                )
                if self._owns_trace
                else PerformanceMetric.unavailable(
                    "peak_memory_bytes",
                    "bytes",
                    "external tracemalloc ownership prevents isolated phase peak measurement",
                )
            )
            self._phases[phase_name] = PhaseMeasurement(
                phase=phase_name,
                wall_seconds=PerformanceMetric.observed("wall_seconds", wall, "seconds", "collector.perf_counter"),
                cpu_seconds=PerformanceMetric.observed("cpu_seconds", cpu, "seconds", "collector.process_time"),
                peak_memory_bytes=memory,
            )
            self._active_phase = None

    def observe(self, name: str, value: float | int, *, unit: str, source: str) -> None:
        # Record one explicit global metric and reject conflicting duplicate observations.
        if self._finished:
            raise PerformanceAuthorityError("cannot record a metric after collector finish")
        metric = PerformanceMetric.observed(name, value, unit, source)
        existing = self._metrics.get(metric.name)
        if existing is not None and existing != metric:
            raise PerformanceConflictError(f"metric already recorded with different content: {metric.name}")
        self._metrics[metric.name] = metric

    def unavailable(self, name: str, *, unit: str, reason: str) -> None:
        # Record one unavailable metric without replacing an existing observed measurement.
        if self._finished:
            raise PerformanceAuthorityError("cannot record a metric after collector finish")
        metric = PerformanceMetric.unavailable(name, unit, reason)
        existing = self._metrics.get(metric.name)
        if existing is not None and existing != metric:
            raise PerformanceConflictError(f"metric already recorded with different content: {metric.name}")
        self._metrics[metric.name] = metric

    def finish(
        self,
        *,
        status: RunStatus = "completed",
        metadata: Mapping[str, object] | None = None,
    ) -> PerformanceRun:
        # Close total resource measurements and materialize every standard metric and phase explicitly.
        if self._finished:
            raise PerformanceAuthorityError("performance collector was already finished")
        if self._active_phase is not None:
            raise PerformanceAuthorityError("cannot finish while a performance phase is active")
        completed_at = _utc_timestamp()
        total_wall = max(0.0, float(self._clock.perf_counter()) - self._started_wall)
        total_cpu = max(0.0, float(self._clock.process_time()) - self._started_cpu)
        self._metrics.setdefault(
            "wall_seconds",
            PerformanceMetric.observed("wall_seconds", total_wall, "seconds", "collector.perf_counter"),
        )
        self._metrics.setdefault(
            "cpu_seconds",
            PerformanceMetric.observed("cpu_seconds", total_cpu, "seconds", "collector.process_time"),
        )
        if self._owns_trace:
            self._metrics.setdefault(
                "peak_memory_bytes",
                PerformanceMetric.observed(
                    "peak_memory_bytes",
                    float(tracemalloc.get_traced_memory()[1]),
                    "bytes",
                    "collector.tracemalloc",
                ),
            )
        else:
            self._metrics.setdefault(
                "peak_memory_bytes",
                PerformanceMetric.unavailable(
                    "peak_memory_bytes",
                    "bytes",
                    "external tracemalloc ownership prevents isolated peak measurement",
                ),
            )
        for name, unit in STANDARD_METRICS.items():
            self._metrics.setdefault(
                name,
                PerformanceMetric.unavailable(name, unit, "measurement source was not connected"),
            )
        for phase in STANDARD_PHASES:
            self._phases.setdefault(
                phase,
                PhaseMeasurement.unavailable(phase, "phase instrumentation was not connected"),
            )
        if self._owns_trace:
            tracemalloc.stop()
        self._finished = True
        return PerformanceRun(
            run_id=self.run_id,
            identity=self.identity,
            status=status,
            started_at=self._started_at,
            completed_at=completed_at,
            phases=tuple(
                self._phases[name]
                for name in (*STANDARD_PHASES, *sorted(set(self._phases) - set(STANDARD_PHASES)))
            ),
            metrics=tuple(self._metrics[name] for name in sorted(self._metrics)),
            metadata=dict(metadata or {}),
        )


def _utc_timestamp() -> str:
    # Produce a stable UTC timestamp without making it part of comparison identity.
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def performance_run_from_runner_report(
    report: Mapping[str, object],
    *,
    identity: WorkloadIdentity,
    run_id: str | None = None,
) -> PerformanceRun:
    # Adapt the existing runner performance dictionary without inventing unavailable phase evidence.
    metrics_root = report.get("metrics", {})
    metrics_mapping = metrics_root if isinstance(metrics_root, Mapping) else {}
    raw = metrics_mapping.get("performance", metrics_mapping)
    performance = raw if isinstance(raw, Mapping) else {}
    collector = PerformanceCollector(identity, run_id=run_id)
    phase_fields = {
        "project_scan": "index_seconds",
        "mutation_discovery": "mutant_generation_seconds",
        "planning": "selection_seconds",
        "workspace_preparation": "snapshot_seconds",
        "execution": "pytest_seconds",
        "projection": "test_stats_ingestion_seconds",
        "finalization": "report_materialization_seconds",
    }
    for phase, field_name in phase_fields.items():
        value = performance.get(field_name)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            collector.record_phase(
                PhaseMeasurement(
                    phase=phase,
                    wall_seconds=PerformanceMetric.observed(
                        "wall_seconds", value, "seconds", f"runner.{field_name}"
                    ),
                    cpu_seconds=PerformanceMetric.unavailable(
                        "cpu_seconds",
                        "seconds",
                        "legacy runner report has no per-phase CPU evidence",
                    ),
                    peak_memory_bytes=PerformanceMetric.unavailable(
                        "peak_memory_bytes",
                        "bytes",
                        "legacy runner report has no per-phase memory evidence",
                    ),
                )
            )
    campaign_wall = performance.get("campaign_wall_seconds")
    if isinstance(campaign_wall, (int, float)) and not isinstance(campaign_wall, bool):
        collector.observe("wall_seconds", campaign_wall, unit="seconds", source="runner.campaign_wall_seconds")
    else:
        collector.unavailable(
            "wall_seconds",
            unit="seconds",
            reason="legacy runner report has no campaign wall-clock evidence",
        )
    collector.unavailable(
        "cpu_seconds",
        unit="seconds",
        reason="legacy runner report has no campaign CPU-time evidence",
    )
    collector.unavailable(
        "peak_memory_bytes",
        unit="bytes",
        reason="legacy runner report has no campaign peak-memory evidence",
    )
    process_count = performance.get("processes_started")
    if isinstance(process_count, (int, float)) and not isinstance(process_count, bool):
        collector.observe("process_spawn_count", process_count, unit="count", source="runner.processes_started")
    for metric_name, field_name, unit in (
        ("sqlite_write_count", "database_write_count", "count"),
        ("queue_wait_seconds", "queue_wait_seconds", "seconds"),
        ("worker_utilization", "worker_utilization", "ratio"),
        ("reuse_hit_rate", "reuse_hit_rate", "ratio"),
        ("selected_tests_ratio", "selected_tests_ratio", "ratio"),
    ):
        value = performance.get(field_name)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            collector.observe(metric_name, value, unit=unit, source=f"runner.{field_name}")
    runner_status = str(report.get("status", "unknown")).strip().lower()
    status: RunStatus = (
        "completed"
        if runner_status in {"complete", "completed"}
        else "cancelled"
        if runner_status in {"cancelled", "canceled"}
        else "failed"
    )
    return collector.finish(
        status=status,
        metadata={"adapter": "runner-report-v1", "runner_status": runner_status},
    )


def performance_run_from_benchmark_report(
    report: Mapping[str, object],
    *,
    identity: WorkloadIdentity,
    run_id: str | None = None,
) -> PerformanceRun:
    # Adapt current benchmark workloads as named evidence while leaving unmapped campaign phases unavailable.
    collector = PerformanceCollector(identity, run_id=run_id)
    workloads = report.get("workloads", {})
    if not isinstance(workloads, Mapping):
        raise ValueError("benchmark workloads must be an object")
    collector.unavailable(
        "wall_seconds",
        unit="seconds",
        reason="legacy benchmark report contains separate workloads but no authoritative total wall time",
    )
    collector.unavailable(
        "cpu_seconds",
        unit="seconds",
        reason="legacy benchmark report has no CPU-time evidence",
    )
    collector.unavailable(
        "peak_memory_bytes",
        unit="bytes",
        reason="legacy benchmark report has no peak-memory evidence",
    )
    details: dict[str, object] = {}
    for name in report.get("workload_order", tuple(workloads)):
        workload_name = str(name)
        raw = workloads.get(workload_name)
        if not isinstance(raw, Mapping):
            raise ValueError(f"benchmark workload is missing: {workload_name}")
        elapsed = raw.get("elapsed_seconds")
        if not isinstance(elapsed, (int, float)) or isinstance(elapsed, bool):
            raise ValueError(f"benchmark workload has no elapsed_seconds: {workload_name}")
        collector.observe(
            f"workload.{workload_name}.wall_seconds",
            elapsed,
            unit="seconds",
            source="legacy-benchmark-report",
        )
        details[workload_name] = raw.get("details", {})
    return collector.finish(
        status="failed" if report.get("subprocess_passed") is False else "completed",
        metadata={
            "adapter": "legacy-benchmark-v1",
            "benchmark_version": str(report.get("benchmark_version", "unknown")),
            "workload_details": details,
        }
    )


class PerformanceStore:
    """SQLite authority for append-only runs, pinned baselines and explicit budgets."""

    def __init__(self, path: str | Path) -> None:
        # Initialize one standalone authority database without mutating campaign storage.
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = self._connect()
        connection.close()

    def _connect(self) -> sqlite3.Connection:
        # Open one bounded SQLite connection and verify the exact performance schema version.
        connection = sqlite3.connect(self.path, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS performance_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS performance_runs (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL UNIQUE,
                comparison_key TEXT NOT NULL,
                workload_name TEXT NOT NULL,
                status TEXT NOT NULL,
                content_sha256 TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_performance_runs_comparison_sequence
            ON performance_runs(comparison_key, sequence, run_id);
            CREATE TRIGGER IF NOT EXISTS trg_performance_runs_no_update
            BEFORE UPDATE ON performance_runs BEGIN
                SELECT RAISE(ABORT, 'performance_runs is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_performance_runs_no_delete
            BEFORE DELETE ON performance_runs BEGIN
                SELECT RAISE(ABORT, 'performance_runs is append-only');
            END;
            CREATE TABLE IF NOT EXISTS performance_baselines (
                comparison_key TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES performance_runs(run_id)
            );
            CREATE TABLE IF NOT EXISTS performance_budgets (
                comparison_key TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                direction TEXT NOT NULL,
                max_relative_regression REAL,
                max_absolute_regression REAL,
                PRIMARY KEY(comparison_key, metric_name)
            );
            """
        )
        row = connection.execute("SELECT value FROM performance_metadata WHERE key = 'schema_version'").fetchone()
        if row is None:
            connection.execute(
                "INSERT INTO performance_metadata(key, value) VALUES ('schema_version', ?)",
                (str(PERFORMANCE_SCHEMA_VERSION),),
            )
        elif int(row["value"]) != PERFORMANCE_SCHEMA_VERSION:
            connection.close()
            raise PerformanceAuthorityError("performance store schema version mismatch")
        return connection

    def append(self, run: PerformanceRun) -> bool:
        # Append one immutable run and return False only for exact duplicate replay.
        payload = _canonical_json(run.to_dict())
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT content_sha256 FROM performance_runs WHERE run_id = ?",
                (run.run_id,),
            ).fetchone()
            if existing is not None:
                if str(existing["content_sha256"]) != digest:
                    raise PerformanceConflictError("performance run ID was reused for different content")
                connection.commit()
                return False
            connection.execute(
                """
                INSERT INTO performance_runs(
                    run_id, comparison_key, workload_name, status,
                    content_sha256, payload_json, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run.run_id,
                    run.identity.comparison_key,
                    run.identity.workload_name,
                    run.status,
                    digest,
                    payload,
                    run.completed_at,
                ),
            )
            connection.commit()
            return True
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def get(self, run_id: str) -> PerformanceRun | None:
        # Load one immutable performance run by its stable request identity.
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT payload_json FROM performance_runs WHERE run_id = ?",
                (_non_empty(run_id, "run_id"),),
            ).fetchone()
            return PerformanceRun.from_dict(json.loads(str(row["payload_json"]))) if row is not None else None
        finally:
            connection.close()

    def list_runs(
        self,
        comparison_key: str,
        *,
        limit: int = 100,
        after_sequence: int = 0,
    ) -> tuple[tuple[PerformanceRun, ...], int | None]:
        # Return one bounded keyset page without OFFSET or full-history materialization.
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1 or limit > PERFORMANCE_QUERY_MAX_LIMIT:
            raise ValueError(f"limit must be between 1 and {PERFORMANCE_QUERY_MAX_LIMIT}")
        if isinstance(after_sequence, bool) or not isinstance(after_sequence, int) or after_sequence < 0:
            raise ValueError("after_sequence must be a non-negative integer")
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT sequence, payload_json
                FROM performance_runs
                WHERE comparison_key = ? AND sequence > ?
                ORDER BY sequence, run_id
                LIMIT ?
                """,
                (_non_empty(comparison_key, "comparison_key"), after_sequence, limit + 1),
            ).fetchall()
            selected = rows[:limit]
            items = tuple(PerformanceRun.from_dict(json.loads(str(row["payload_json"]))) for row in selected)
            next_sequence = int(selected[-1]["sequence"]) if len(rows) > limit and selected else None
            return items, next_sequence
        finally:
            connection.close()

    def pin_baseline(self, run_id: str) -> None:
        # Pin one existing run as the sole baseline for its exact comparison identity.
        run = self.get(run_id)
        if run is None:
            raise ValueError("baseline run does not exist")
        connection = self._connect()
        try:
            connection.execute(
                """
                INSERT INTO performance_baselines(comparison_key, run_id)
                VALUES (?, ?)
                ON CONFLICT(comparison_key) DO UPDATE SET run_id = excluded.run_id
                """,
                (run.identity.comparison_key, run.run_id),
            )
        finally:
            connection.close()

    def set_budget(self, comparison_key: str, budget: RegressionBudget) -> None:
        # Persist one opt-in threshold after a measured baseline has been selected.
        connection = self._connect()
        try:
            connection.execute(
                """
                INSERT INTO performance_budgets(
                    comparison_key, metric_name, direction, max_relative_regression, max_absolute_regression
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(comparison_key, metric_name) DO UPDATE SET
                    direction = excluded.direction,
                    max_relative_regression = excluded.max_relative_regression,
                    max_absolute_regression = excluded.max_absolute_regression
                """,
                (
                    _non_empty(comparison_key, "comparison_key"),
                    budget.metric_name,
                    budget.direction,
                    budget.max_relative_regression,
                    budget.max_absolute_regression,
                ),
            )
        finally:
            connection.close()

    def evaluate(self, run: PerformanceRun) -> RegressionReport:
        # Compare one run only with its pinned baseline and explicitly configured budgets.
        connection = self._connect()
        try:
            baseline_row = connection.execute(
                "SELECT run_id FROM performance_baselines WHERE comparison_key = ?",
                (run.identity.comparison_key,),
            ).fetchone()
            if baseline_row is None:
                return RegressionReport(run.identity.comparison_key, run.run_id, None, "no_baseline", ())
            baseline_run_id = str(baseline_row["run_id"])
            baseline = self.get(baseline_run_id)
            if baseline is None:
                raise PerformanceAuthorityError("pinned performance baseline is missing")
            rows = connection.execute(
                """
                SELECT metric_name, direction, max_relative_regression, max_absolute_regression
                FROM performance_budgets
                WHERE comparison_key = ?
                ORDER BY metric_name
                """,
                (run.identity.comparison_key,),
            ).fetchall()
            if not rows:
                return RegressionReport(run.identity.comparison_key, run.run_id, baseline_run_id, "observe_only", ())
            current_metrics = run.metric_map()
            baseline_metrics = baseline.metric_map()
            findings: list[RegressionFinding] = []
            for row in rows:
                name = str(row["metric_name"])
                current = current_metrics.get(name)
                previous = baseline_metrics.get(name)
                if (
                    current is None
                    or previous is None
                    or current.status != "observed"
                    or previous.status != "observed"
                    or current.value is None
                    or previous.value is None
                ):
                    findings.append(
                        RegressionFinding(
                            name,
                            "insufficient_evidence",
                            previous.value if previous else None,
                            current.value if current else None,
                            None,
                            None,
                            "metric is not observed in both runs",
                        )
                    )
                    continue
                raw_delta = current.value - previous.value
                direction = str(row["direction"])
                regression_delta = raw_delta if direction == "lower_is_better" else -raw_delta
                relative = (
                    regression_delta / previous.value
                    if previous.value > 0.0
                    else (0.0 if regression_delta <= 0.0 else None)
                )
                relative_limit = (
                    float(row["max_relative_regression"])
                    if row["max_relative_regression"] is not None
                    else None
                )
                absolute_limit = (
                    float(row["max_absolute_regression"])
                    if row["max_absolute_regression"] is not None
                    else None
                )
                relative_failed = relative_limit is not None and (relative is None or relative > relative_limit)
                absolute_failed = absolute_limit is not None and regression_delta > absolute_limit
                findings.append(
                    RegressionFinding(
                        name,
                        "regressed" if relative_failed or absolute_failed else "passed",
                        previous.value,
                        current.value,
                        raw_delta,
                        relative,
                    )
                )
            if any(item.status == "regressed" for item in findings):
                status = "regressed"
            elif any(item.status == "insufficient_evidence" for item in findings):
                status = "insufficient_evidence"
            else:
                status = "passed"
            return RegressionReport(run.identity.comparison_key, run.run_id, baseline_run_id, status, tuple(findings))
        finally:
            connection.close()
    def evaluate_gate(
        self,
        run: PerformanceRun,
        *,
        invariants: Sequence[InvariantFinding] = (),
        stable_regression_threshold: float = 0.10,
    ) -> OptimizationGateReport:
        # Evaluate persisted performance evidence and PR84 hard-invariant governance as one caller-facing decision.
        return evaluate_optimization_gate(
            self.evaluate(run),
            invariants=invariants,
            stable_regression_threshold=stable_regression_threshold,
        )
