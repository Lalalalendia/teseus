"""Durable, queryable Knowledge Plane projection for committed mutation effects."""
from __future__ import annotations
import json
import math
import os
import sqlite3
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from test_intelligence_unified_v1.io_utils import atomic_write_json, stable_hash, utc_now_iso
from theseus_contracts import normalize_reuse_mode
from .schema import (
    KNOWLEDGE_SCHEMA_MIGRATIONS,
    KNOWLEDGE_SCHEMA_VERSION,
    KnowledgeSchemaState,
    KnowledgeScopeIdentity,
    migration_checksum,
)
from .evidence import (
    EvidenceNodeType,
    EvidenceRelation,
    KnowledgeEvidenceEdge,
    KnowledgeEvidenceGraph,
    KnowledgeEvidenceNode,
)
from .reuse import (
    EvidenceQuality,
    KnowledgePage,
    KnowledgeRetentionPolicy,
    KnowledgeRetentionResult,
    ReuseDecision,
    ReuseKind,
    ReuseMetrics,
    ReuseMode,
    ReusePlanArtifact,
    authorize_reuse_decision,
    decode_cursor,
    encode_cursor,
    fingerprint_result,
)
from theseus_performance.sqlite_metrics import SQLiteMetrics
class KnowledgeConflict(RuntimeError):
    """Raised when one committed effect identity is reused with another payload."""
@dataclass(frozen=True, slots=True)
class KnowledgeIngestResult:
    """Bounded diagnostics from one idempotent effect projection."""
    effect_id: str
    inserted_events: int
    duplicate: bool = False
    inserted_observations: int = 0
    scope_id: str | None = None
    schema_version: int = KNOWLEDGE_SCHEMA_VERSION
@dataclass(frozen=True, slots=True)
class KnowledgeSummary:
    """Read model for one campaign's accumulated execution knowledge."""
    campaign_id: str
    executions: int
    mutants: int
    attempts: int
    counts: Mapping[str, int]
    revision: int = 0
    schema_version: int = KNOWLEDGE_SCHEMA_VERSION
@dataclass(frozen=True, slots=True)
class KnowledgeConflictRecord:
    """One durable quarantined fact conflict that never rewrites accepted history."""
    conflict_id: str
    conflict_type: str
    identity_type: str
    identity_key: str
    scope_id: str | None
    existing_sha256: str | None
    incoming_sha256: str
    reason: str
    created_at: str
    status: str = "quarantined"
    def to_dict(self) -> dict[str, Any]:
        # Serialize one quarantined conflict without exposing raw SQLite rows.
        return {
            "conflict_id": self.conflict_id,
            "conflict_type": self.conflict_type,
            "identity_type": self.identity_type,
            "identity_key": self.identity_key,
            "scope_id": self.scope_id,
            "existing_sha256": self.existing_sha256,
            "incoming_sha256": self.incoming_sha256,
            "reason": self.reason,
            "created_at": self.created_at,
            "status": self.status,
        }
@dataclass(frozen=True, slots=True)
class KnowledgeCompactionRecord:
    """One atomically committed compaction manifest and its preserved evidence counts."""
    compaction_id: str
    source_revision: int
    cutoff: str
    manifest_sha256: str
    event_count: int
    observation_count: int
    created_at: str
    committed_at: str
    def to_dict(self) -> dict[str, Any]:
        # Serialize one completed compaction receipt for recovery diagnostics.
        return {
            "compaction_id": self.compaction_id,
            "source_revision": self.source_revision,
            "cutoff": self.cutoff,
            "manifest_sha256": self.manifest_sha256,
            "event_count": self.event_count,
            "observation_count": self.observation_count,
            "created_at": self.created_at,
            "committed_at": self.committed_at,
        }
@dataclass(frozen=True, slots=True)
class KnowledgeIntegrityReport:
    """Bounded corruption audit over SQLite, payload hashes, graph links, and compaction receipts."""
    healthy: bool
    quick_check: str
    checked_rows: int
    hash_mismatches: tuple[str, ...]
    orphaned_references: tuple[str, ...]
    schema_version: int = KNOWLEDGE_SCHEMA_VERSION
    def to_dict(self) -> dict[str, Any]:
        # Publish integrity diagnostics as stable JSON-compatible data.
        return {
            "healthy": self.healthy,
            "quick_check": self.quick_check,
            "checked_rows": self.checked_rows,
            "hash_mismatches": list(self.hash_mismatches),
            "orphaned_references": list(self.orphaned_references),
            "schema_version": self.schema_version,
        }
def _canonical(value: Any) -> str:
    # Encode one knowledge payload deterministically for conflict detection.
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
def _normalize_timestamp(value: Any) -> str:
    # Convert every persisted timestamp to a comparable UTC representation.
    raw = str(value or utc_now_iso()).strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
def _evidence_quality(value: Mapping[str, Any]) -> EvidenceQuality:
    # Admit only terminal, restored, schema-proven deterministic outcomes to executable reuse.
    semantic = str(value.get("semantic_result") or value.get("result") or "")
    process_status = str(value.get("status") or "")
    source_kind = str(value.get("source_kind", "observed"))
    evidence_origin = str(value.get("evidence_origin", ""))
    schema_version = int(value.get("evidence_schema_version", value.get("schema_version", 0)) or 0)
    observations = value.get("test_observations", ())
    observation_proof = isinstance(observations, (list, tuple)) and any(
        isinstance(item, Mapping)
        and str(item.get("test_id", ""))
        and str(item.get("test_fingerprint", ""))
        and str(item.get("evidence_kind", "")) == "pytest_test_event"
        and int(item.get("observation_schema_version", 0) or 0) >= 1
        and str(item.get("outcome", "")) not in {"", "unknown"}
        for item in observations
    )
    allowed_semantic = {"killed", "survived", "invalid_mutant"}
    allowed_process = {"complete", "completed", "success"}
    allowed_sources = {"observed", "validated"}
    if (
        semantic in allowed_semantic
        and process_status in allowed_process
        and bool(value.get("restore_verified", False))
        and schema_version >= 1
        and observation_proof
        and source_kind in allowed_sources
        and evidence_origin not in {"aggregate_projection", "synthetic", "partial_merged"}
    ):
        return EvidenceQuality.VALIDATED
    if any(value.get(key) is not None for key in ("semantic_result", "status", "restore_verified", "schema_version")):
        return EvidenceQuality.INELIGIBLE
    return EvidenceQuality.UNKNOWN
def _execution_identity_payload(
    *,
    campaign_id: str,
    execution_id: str,
    mutant_id: str,
    attempt: int,
    status: str,
    payload: Mapping[str, Any],
) -> str:
    # Create the canonical identity used to distinguish a replay from execution corruption.
    return _canonical(
        {
            "campaign_id": campaign_id,
            "execution_id": execution_id,
            "mutant_id": mutant_id,
            "attempt": int(attempt),
            "status": status,
            "payload": payload,
        }
    )
def _nested_value(value: Mapping[str, Any], key: str) -> Any:
    # Read a fingerprint from either the flat E16 shape or its nested compatibility shape.
    fingerprints = value.get("fingerprints")
    if value.get(key) is not None:
        return value.get(key)
    if isinstance(fingerprints, Mapping):
        return fingerprints.get(key)
    return None
def _normalize_test_observations(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    # Normalize per-test evidence without inventing fingerprints for incomplete observations.
    raw = value.get("test_observations", value.get("tests", []))
    if raw is None:
        return []
    if not isinstance(raw, (list, tuple)):
        raise ValueError("test_observations must be an array")
    observations: list[dict[str, Any]] = []
    seen_test_ids: set[str] = set()
    for item in raw:
        if not isinstance(item, Mapping):
            raise ValueError("test observation must be an object")
        test_id = str(item.get("test_id") or item.get("nodeid") or "").strip()
        test_fingerprint = str(item.get("test_fingerprint") or item.get("fingerprint") or "").strip()
        if not test_id or not test_fingerprint:
            raise ValueError("test observation requires test_id and test_fingerprint")
        if test_id in seen_test_ids:
            raise ValueError(f"test observation identity is duplicated: test_id={test_id}")
        seen_test_ids.add(test_id)
        observations.append(
            {
                "test_id": test_id,
                "test_fingerprint": test_fingerprint,
                "outcome": str(item.get("outcome") or item.get("status") or "unknown"),
                "payload": dict(item),
            }
        )
    return observations
def _normalize_execution(value: Mapping[str, Any]) -> dict[str, Any]:
    # Normalize one execution and retain all optional E16 fingerprint evidence.
    execution_id = str(value.get("execution_id", "")).strip()
    mutant_id = str(value.get("mutant_id", "")).strip()
    if not execution_id or not mutant_id:
        raise ValueError("committed execution requires execution_id and mutant_id")
    observations = _normalize_test_observations(value)
    test_fingerprint = _nested_value(value, "test_fingerprint")
    if test_fingerprint is None and observations:
        test_fingerprint = stable_hash(
            {item["test_id"]: item["test_fingerprint"] for item in observations}
        )
    mutant_fingerprint = _nested_value(value, "mutant_fingerprint")
    environment_fingerprint = _nested_value(value, "environment_fingerprint")
    selection_fingerprint = _nested_value(value, "selection_fingerprint")
    result_fingerprint = _nested_value(value, "result_fingerprint")
    if (
        result_fingerprint is None
        and mutant_fingerprint
        and test_fingerprint
        and environment_fingerprint
    ):
        result_fingerprint = fingerprint_result(
            mutant_fingerprint=str(mutant_fingerprint),
            test_fingerprint=str(test_fingerprint),
            environment_fingerprint=str(environment_fingerprint),
            selection_configuration=value.get("selection_configuration", selection_fingerprint),
        )
    quality = _evidence_quality(value)
    attempt = int(value.get("attempt", 0))
    if attempt < 0:
        raise ValueError(f"committed execution attempt must not be negative: execution_id={execution_id}; attempt={attempt}")
    knowledge_revision = int(value.get("knowledge_revision", value.get("revision", 0)) or 0)
    if knowledge_revision < 0:
        raise ValueError(
            f"knowledge_revision must not be negative: execution_id={execution_id}; revision={knowledge_revision}"
        )
    source_kind = str(value.get("source_kind", "observed")).strip()
    retention_class = str(value.get("retention_class", "campaign")).strip()
    if not source_kind or not retention_class:
        raise ValueError(f"source_kind and retention_class must be non-empty: execution_id={execution_id}")
    return {
        "execution_id": execution_id,
        "mutant_id": mutant_id,
        "attempt": attempt,
        "status": str(value.get("semantic_result") or value.get("status") or "unknown"),
        "payload": dict(value),
        "function_id": str(value.get("function_id")) if value.get("function_id") else None,
        "function_fingerprint": str(_nested_value(value, "function_fingerprint"))
        if _nested_value(value, "function_fingerprint")
        else None,
        "mutant_fingerprint": str(mutant_fingerprint) if mutant_fingerprint else None,
        "test_fingerprint": str(test_fingerprint) if test_fingerprint else None,
        "conftest_fingerprint": str(_nested_value(value, "conftest_fingerprint"))
        if _nested_value(value, "conftest_fingerprint")
        else None,
        "environment_fingerprint": str(environment_fingerprint) if environment_fingerprint else None,
        "selection_fingerprint": str(selection_fingerprint) if selection_fingerprint else None,
        "result_fingerprint": str(result_fingerprint) if result_fingerprint else None,
        "source_kind": source_kind,
        "evidence_quality": quality.value,
        "evidence_schema_version": int(value.get("evidence_schema_version", value.get("schema_version", 0)) or 0),
        "retention_class": retention_class,
        "knowledge_revision": knowledge_revision,
        "test_observations": observations,
    }


def _finite_cost_seconds(value: Any, *, multiplier: float = 1.0) -> float | None:
    # Accept only finite non-negative numeric performance hints from advisory payloads.
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value) * float(multiplier)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) and number >= 0.0 else None


def _payload_runtime_class(payload: Mapping[str, Any]) -> str | None:
    # Read an optional worker/runtime class without requiring it for authoritative evidence.
    candidates: list[Any] = [
        payload.get("runtime_class"),
        payload.get("worker_runtime_class"),
        payload.get("worker_runtime_fingerprint"),
        payload.get("runtime_fingerprint"),
    ]
    hint = payload.get("performance_hint")
    if isinstance(hint, Mapping):
        candidates.extend(
            (
                hint.get("runtime_class"),
                hint.get("worker_runtime_class"),
                hint.get("worker_runtime_fingerprint"),
                hint.get("runtime_fingerprint"),
            )
        )
    identity = payload.get("runtime_identity")
    if isinstance(identity, Mapping):
        candidates.append(identity.get("runtime_fingerprint"))
    shard = payload.get("shard")
    if isinstance(shard, Mapping):
        candidates.append(shard.get("runtime_class"))
        lease = shard.get("lease")
        if isinstance(lease, Mapping):
            lease_identity = lease.get("runtime_identity")
            if isinstance(lease_identity, Mapping):
                candidates.append(lease_identity.get("runtime_fingerprint"))
    for candidate in candidates:
        value = str(candidate).strip() if candidate is not None else ""
        if value:
            return value
    return None


def _payload_duration_seconds(payload: Mapping[str, Any]) -> float | None:
    # Prefer explicit execution seconds and tolerate legacy millisecond fields.
    for name in ("duration_seconds", "elapsed_seconds", "runtime_seconds"):
        if name in payload:
            return _finite_cost_seconds(payload.get(name))
    for name in ("duration_ms", "elapsed_ms", "runtime_ms"):
        if name in payload:
            return _finite_cost_seconds(payload.get(name), multiplier=0.001)
    return None


def _test_subset_cost(payload: Mapping[str, Any]) -> tuple[float, int]:
    # Sum bounded per-test durations while retaining the selected-subset cardinality as a hint.
    raw_selected = payload.get("selected_tests", ())
    selected = {
        str(item).strip()
        for item in raw_selected
        if str(item).strip()
    } if isinstance(raw_selected, (list, tuple, set, frozenset)) else set()
    raw_observations = payload.get("test_observations", ())
    observations = raw_observations if isinstance(raw_observations, (list, tuple)) else ()
    total = 0.0
    measured_tests: set[str] = set()
    for item in observations:
        if not isinstance(item, Mapping):
            continue
        test_id = str(item.get("test_id") or item.get("nodeid") or "").strip()
        duration = _payload_duration_seconds(item)
        if duration is None and isinstance(item.get("payload"), Mapping):
            duration = _payload_duration_seconds(item["payload"])
        if duration is not None:
            total += duration
            if test_id:
                measured_tests.add(test_id)
        if test_id:
            selected.add(test_id)
    if total <= 0.0:
        explicit = _finite_cost_seconds(payload.get("test_subset_cost_seconds"))
        total = explicit or 0.0
    return total, max(len(selected), len(measured_tests))


def _cost_p95(values: Sequence[float]) -> float:
    # Calculate a deterministic nearest-rank p95 for a small bounded history sample.
    if not values:
        return 0.0
    ordered = sorted(float(item) for item in values)
    index = min(len(ordered) - 1, max(0, math.ceil(len(ordered) * 0.95) - 1))
    return ordered[index]


def _payload_contains(container: Any, subset: Any) -> bool:
    # Check whether an enriched JSON payload still contains the exact committed base payload.
    if isinstance(container, Mapping) and isinstance(subset, Mapping):
        return all(key in container and _payload_contains(container[key], value) for key, value in subset.items())
    if isinstance(container, (list, tuple)) and isinstance(subset, (list, tuple)):
        if not subset:
            return True
        if len(container) < len(subset):
            return False
        remaining = list(container)
        for subset_item in subset:
            match = next(
                (index for index, container_item in enumerate(remaining) if _payload_contains(container_item, subset_item)),
                None,
            )
            if match is None:
                return False
            remaining.pop(match)
        return True
    return container == subset
def _cutoff_iso(max_age_seconds: float, now: str | None) -> str:
    # Compute a UTC retention boundary with second precision matching stored timestamps.
    current = datetime.fromisoformat(_normalize_timestamp(now)) if now else datetime.now(timezone.utc)
    return _normalize_timestamp(current - timedelta(seconds=max_age_seconds))
def _fsync_parent(path: Path) -> None:
    # Persist one replaced database directory entry where the platform supports directory fsync.
    if os.name == "nt":
        return
    try:
        descriptor = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)
class KnowledgePlaneStore:
    """SQLite projection that accepts only already committed Gallifrey effects."""
    _INVALIDATION_SCOPES = {"function", "test", "conftest", "environment", "selection", "mutant", "result"}
    def __init__(
        self,
        path: str | Path,
        *,
        metrics: SQLiteMetrics | None = None,
    ) -> None:
        # Open a separate projection database so knowledge writes cannot mutate control aggregates.
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(str(self.path))
        self._connection.row_factory = sqlite3.Row
        if metrics is not None:
            self._connection.set_trace_callback(metrics.trace)
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute("PRAGMA busy_timeout = 5000")
        try:
            quick_row = self._connection.execute("PRAGMA quick_check(1)").fetchone()
        except sqlite3.DatabaseError as exc:
            self._connection.close()
            raise KnowledgeConflict(f"knowledge database is corrupt: database={self.path}; error={exc}") from exc
        if quick_row is not None and str(quick_row[0]).lower() != "ok":
            self._connection.close()
            raise KnowledgeConflict(
                f"knowledge database is corrupt: database={self.path}; quick_check={quick_row[0]}"
            )
        self._initialize()
    def _initialize(self) -> None:
        # Create E15 tables and additive E16 projections in one schema transaction.
        with self._connection:
            self._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS knowledge_effects (
                    effect_id TEXT PRIMARY KEY,
                    campaign_id TEXT NOT NULL,
                    effect_type TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    control_revision INTEGER,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS knowledge_executions (
                    event_id TEXT PRIMARY KEY,
                    effect_id TEXT NOT NULL,
                    campaign_id TEXT NOT NULL,
                    execution_id TEXT NOT NULL,
                    mutant_id TEXT NOT NULL,
                    attempt INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(effect_id, execution_id)
                );
                CREATE INDEX IF NOT EXISTS ix_knowledge_execution_campaign
                    ON knowledge_executions(campaign_id, mutant_id, attempt);
                CREATE TABLE IF NOT EXISTS knowledge_execution_fingerprints (
                    event_id TEXT PRIMARY KEY,
                    effect_id TEXT NOT NULL,
                    campaign_id TEXT NOT NULL,
                    execution_id TEXT NOT NULL,
                    mutant_id TEXT NOT NULL,
                    function_id TEXT,
                    function_fingerprint TEXT,
                    mutant_fingerprint TEXT,
                    test_fingerprint TEXT,
                    conftest_fingerprint TEXT,
                    environment_fingerprint TEXT,
                    selection_fingerprint TEXT,
                    result_fingerprint TEXT,
                    source_kind TEXT NOT NULL,
                    evidence_quality TEXT NOT NULL DEFAULT 'unknown',
                    evidence_schema_version INTEGER NOT NULL DEFAULT 0,
                    retention_class TEXT NOT NULL,
                    knowledge_revision INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_knowledge_fingerprint_lookup
                    ON knowledge_execution_fingerprints(mutant_id, mutant_fingerprint, environment_fingerprint);
                CREATE TABLE IF NOT EXISTS knowledge_test_observations (
                    observation_id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    effect_id TEXT NOT NULL,
                    campaign_id TEXT NOT NULL,
                    execution_id TEXT NOT NULL,
                    mutant_id TEXT NOT NULL,
                    test_id TEXT NOT NULL,
                    test_fingerprint TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(event_id, test_id)
                );
                CREATE INDEX IF NOT EXISTS ix_knowledge_test_observation_lookup
                    ON knowledge_test_observations(campaign_id, mutant_id, test_id, test_fingerprint);
                CREATE TABLE IF NOT EXISTS knowledge_campaign_mutants (
                    campaign_id TEXT NOT NULL,
                    mutant_id TEXT NOT NULL,
                    PRIMARY KEY(campaign_id, mutant_id)
                );
                CREATE TABLE IF NOT EXISTS knowledge_campaign_counters (
                    campaign_id TEXT PRIMARY KEY,
                    executions INTEGER NOT NULL,
                    mutants INTEGER NOT NULL,
                    attempts INTEGER NOT NULL,
                    counts TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS knowledge_invalidations (
                    invalidation_id TEXT PRIMARY KEY,
                    scope_type TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    campaign_id TEXT,
                    reason TEXT NOT NULL,
                    fingerprint TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_knowledge_invalidation_lookup
                    ON knowledge_invalidations(scope_type, scope_key, created_at);
                CREATE TABLE IF NOT EXISTS knowledge_reuse_incidents (
                    incident_id TEXT PRIMARY KEY,
                    campaign_id TEXT NOT NULL,
                    mutant_id TEXT NOT NULL,
                    rule_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    mismatches TEXT NOT NULL,
                    expected_payload TEXT NOT NULL,
                    actual_payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_knowledge_reuse_incident_lookup
                    ON knowledge_reuse_incidents(mutant_id, rule_id, created_at);
                CREATE TABLE IF NOT EXISTS knowledge_reuse_quarantine (
                    scope_type TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    incident_id TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    released_at TEXT,
                    success_count INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(scope_type, scope_key)
                );
                CREATE INDEX IF NOT EXISTS ix_knowledge_reuse_quarantine_active
                    ON knowledge_reuse_quarantine(scope_type, scope_key, released_at);
                CREATE TABLE IF NOT EXISTS knowledge_execution_rollups (
                    event_id TEXT PRIMARY KEY,
                    effect_id TEXT NOT NULL,
                    campaign_id TEXT NOT NULL,
                    execution_id TEXT NOT NULL,
                    mutant_id TEXT NOT NULL,
                    attempt INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    function_id TEXT,
                    function_fingerprint TEXT,
                    mutant_fingerprint TEXT,
                    test_fingerprint TEXT,
                    conftest_fingerprint TEXT,
                    environment_fingerprint TEXT,
                    selection_fingerprint TEXT,
                    result_fingerprint TEXT,
                    source_kind TEXT NOT NULL,
                    evidence_quality TEXT NOT NULL DEFAULT 'unknown',
                    evidence_schema_version INTEGER NOT NULL DEFAULT 0,
                    retention_class TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    compacted_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_knowledge_rollup_lookup
                    ON knowledge_execution_rollups(mutant_id, mutant_fingerprint, created_at);
                CREATE TABLE IF NOT EXISTS knowledge_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                INSERT OR IGNORE INTO knowledge_meta(key, value) VALUES ('projection_revision', '0');
                """
            )
            self._apply_schema_migrations()
            duplicate_execution = self._connection.execute(
                "SELECT execution_id FROM knowledge_executions GROUP BY execution_id HAVING COUNT(*) > 1 LIMIT 1"
            ).fetchone()
            if duplicate_execution is not None:
                raise KnowledgeConflict(
                    f"knowledge execution identity is already duplicated: {duplicate_execution['execution_id']}"
                )
            self._connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_knowledge_execution_id "
                "ON knowledge_executions(execution_id)"
            )
            self._backfill_counters()
    @staticmethod
    def _migration_statements(version: int) -> tuple[str, ...]:
        # Return the canonical migration contract used for checksum verification.
        statements = {
            1: (
                "add canonical scope columns",
                "create knowledge_scopes",
                "create knowledge_schema_migrations",
            ),
            2: (
                "create knowledge_identity_bindings",
                "backfill canonical identities",
                "install canonical insert guards and append-only triggers",
            ),
            3: (
                "create event-scoped knowledge_evidence_nodes and knowledge_evidence_edges",
                "backfill execution provenance graphs",
                "install append-only graph triggers",
            ),
            4: (
                "create durable conflict quarantine and tombstones",
                "create live reuse-plan evidence references",
                "extend rollups with evidence bundles and graph fingerprints",
                "backfill pre-PR13 rollups into manifest-backed evidence bundles",
                "create atomic compaction manifests",
            ),
        }
        return statements[version]
    def _table_columns(self, table: str) -> set[str]:
        # Read one SQLite table shape for additive, idempotent schema migrations.
        return {
            str(row[1])
            for row in self._connection.execute(f"PRAGMA table_info({table})").fetchall()
        }
    def _add_column(self, table: str, definition: str) -> None:
        # Add one column only when an older database does not already expose it.
        column = definition.split(None, 1)[0]
        if column not in self._table_columns(table):
            self._connection.execute(f"ALTER TABLE {table} ADD COLUMN {definition}")
    def _apply_schema_migrations(self) -> None:
        # Upgrade every legacy Knowledge Plane database through a checksummed monotonic migration chain.
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS knowledge_schema_migrations ("
            "version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TEXT NOT NULL)"
        )
        self._connection.execute(
            "INSERT OR IGNORE INTO knowledge_meta(key, value) VALUES ('schema_version', '0')"
        )
        self._connection.commit()
        row = self._connection.execute(
            "SELECT value FROM knowledge_meta WHERE key = 'schema_version'"
        ).fetchone()
        current = int(row[0]) if row is not None else 0
        if current > KNOWLEDGE_SCHEMA_VERSION:
            raise KnowledgeConflict(
                "knowledge schema is newer than this runtime: "
                f"database={self.path}; actual_version={current}; "
                f"supported_version={KNOWLEDGE_SCHEMA_VERSION}"
            )
        receipt_versions = tuple(
            int(item[0])
            for item in self._connection.execute(
                "SELECT version FROM knowledge_schema_migrations ORDER BY version"
            ).fetchall()
        )
        expected_receipts = tuple(range(1, current + 1))
        if receipt_versions != expected_receipts:
            raise KnowledgeConflict(
                "knowledge schema version and migration receipts disagree: "
                f"database={self.path}; schema_version={current}; "
                f"expected_receipts={expected_receipts}; actual_receipts={receipt_versions}"
            )
        for version, name in KNOWLEDGE_SCHEMA_MIGRATIONS:
            checksum = migration_checksum(version, name, self._migration_statements(version))
            recorded = self._connection.execute(
                "SELECT name, checksum FROM knowledge_schema_migrations WHERE version = ?",
                (version,),
            ).fetchone()
            if recorded is not None:
                if str(recorded["name"]) != name or str(recorded["checksum"]) != checksum:
                    raise KnowledgeConflict(
                        "knowledge migration identity conflict: "
                        f"database={self.path}; version={version}; expected_name={name}; "
                        f"actual_name={recorded['name']}; expected_checksum={checksum}; "
                        f"actual_checksum={recorded['checksum']}"
                    )
                current = max(current, version)
                continue
            if version <= current:
                raise KnowledgeConflict(
                    "knowledge schema version has no matching migration receipt: "
                    f"database={self.path}; schema_version={current}; missing_migration={version}"
                )
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                if version == 1:
                    self._migrate_canonical_scope_columns()
                elif version == 2:
                    self._migrate_identity_bindings()
                elif version == 3:
                    self._migrate_provenance_evidence_graph()
                elif version == 4:
                    self._migrate_conflict_compaction_closure()
                self._connection.execute(
                    "INSERT INTO knowledge_schema_migrations(version, name, checksum, applied_at) VALUES (?, ?, ?, ?)",
                    (version, name, checksum, _normalize_timestamp(utc_now_iso())),
                )
                self._connection.execute(
                    "UPDATE knowledge_meta SET value = ? WHERE key = 'schema_version'",
                    (str(version),),
                )
                self._connection.commit()
            except BaseException:
                self._connection.rollback()
                raise
            current = version
    def _migrate_canonical_scope_columns(self) -> None:
        # Add project, revision, and environment identity to every historical fact table.
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS knowledge_scopes ("
            "scope_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, revision_id TEXT NOT NULL, "
            "environment_id TEXT NOT NULL, identity_source TEXT NOT NULL, created_at TEXT NOT NULL, "
            "UNIQUE(project_id, revision_id, environment_id))"
        )
        self._connection.execute(
            "CREATE INDEX IF NOT EXISTS ix_knowledge_scope_lookup "
            "ON knowledge_scopes(project_id, revision_id, environment_id)"
        )
        additions = {
            "knowledge_effects": (
                "scope_id TEXT",
                "project_id TEXT",
                "revision_id TEXT",
                "environment_id TEXT",
                "identity_source TEXT",
                "observed_at TEXT",
                "ingested_at TEXT",
            ),
            "knowledge_executions": ("scope_id TEXT", "identity_sha256 TEXT"),
            "knowledge_execution_fingerprints": ("scope_id TEXT",),
            "knowledge_test_observations": ("scope_id TEXT", "observation_sha256 TEXT"),
            "knowledge_execution_rollups": (
                "scope_id TEXT",
                "project_id TEXT",
                "revision_id TEXT",
                "environment_id TEXT",
                "identity_sha256 TEXT",
            ),
        }
        for table, definitions in additions.items():
            for definition in definitions:
                self._add_column(table, definition)
        for table in ("knowledge_execution_fingerprints", "knowledge_execution_rollups"):
            if "conftest_fingerprint" not in self._table_columns(table):
                self._connection.execute(f"ALTER TABLE {table} ADD COLUMN conftest_fingerprint TEXT")
            if "evidence_quality" not in self._table_columns(table):
                self._connection.execute(
                    f"ALTER TABLE {table} ADD COLUMN evidence_quality TEXT NOT NULL DEFAULT 'unknown'"
                )
            if "evidence_schema_version" not in self._table_columns(table):
                self._connection.execute(
                    f"ALTER TABLE {table} ADD COLUMN evidence_schema_version INTEGER NOT NULL DEFAULT 0"
                )
    def _migrate_identity_bindings(self) -> None:
        # Backfill legacy scope rows and immutable entity bindings before enabling strict ingestion.
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS knowledge_identity_bindings ("
            "scope_id TEXT NOT NULL, identity_type TEXT NOT NULL, identity_key TEXT NOT NULL, "
            "fingerprint TEXT NOT NULL, first_event_id TEXT NOT NULL, payload_sha256 TEXT NOT NULL, "
            "created_at TEXT NOT NULL, PRIMARY KEY(scope_id, identity_type, identity_key))"
        )
        self._connection.execute(
            "CREATE INDEX IF NOT EXISTS ix_knowledge_identity_fingerprint "
            "ON knowledge_identity_bindings(identity_type, fingerprint, scope_id)"
        )
        effects = self._connection.execute(
            "SELECT effect_id, campaign_id, payload, created_at, scope_id FROM knowledge_effects ORDER BY created_at, effect_id"
        ).fetchall()
        for effect in effects:
            payload = json.loads(str(effect["payload"]))
            scope = KnowledgeScopeIdentity.resolve(
                campaign_id=str(effect["campaign_id"]),
                payload=payload if isinstance(payload, Mapping) else {},
            )
            self._insert_scope(scope, created_at=str(effect["created_at"]))
            self._connection.execute(
                "UPDATE knowledge_effects SET scope_id = ?, project_id = ?, revision_id = ?, environment_id = ?, "
                "identity_source = ?, observed_at = COALESCE(observed_at, created_at), "
                "ingested_at = COALESCE(ingested_at, created_at) WHERE effect_id = ?",
                (
                    scope.scope_id,
                    scope.project_id,
                    scope.revision_id,
                    scope.environment_id,
                    scope.identity_source,
                    str(effect["effect_id"]),
                ),
            )
        rows = self._connection.execute(
            "SELECT e.event_id, e.effect_id, e.campaign_id, e.execution_id, e.mutant_id, e.payload, e.created_at, "
            "k.scope_id, f.function_id, f.function_fingerprint, f.mutant_fingerprint "
            "FROM knowledge_executions e JOIN knowledge_effects k ON k.effect_id = e.effect_id "
            "LEFT JOIN knowledge_execution_fingerprints f ON f.event_id = e.event_id "
            "ORDER BY e.created_at, e.event_id"
        ).fetchall()
        for row in rows:
            payload = json.loads(str(row["payload"]))
            identity_sha256 = stable_hash(payload)
            scope_id = str(row["scope_id"] or "")
            if not scope_id:
                raise KnowledgeConflict(
                    f"knowledge migration could not resolve execution scope: event_id={row['event_id']}"
                )
            self._connection.execute(
                "UPDATE knowledge_executions SET scope_id = ?, identity_sha256 = ? WHERE event_id = ?",
                (scope_id, identity_sha256, row["event_id"]),
            )
            self._connection.execute(
                "UPDATE knowledge_execution_fingerprints SET scope_id = ? WHERE event_id = ?",
                (scope_id, row["event_id"]),
            )
            self._bind_identity(
                scope_id=scope_id,
                identity_type="execution",
                identity_key=str(row["execution_id"]),
                fingerprint=identity_sha256,
                event_id=str(row["event_id"]),
                payload_sha256=identity_sha256,
                created_at=str(row["created_at"]),
            )
            if row["function_id"] and row["function_fingerprint"]:
                self._bind_identity(
                    scope_id=scope_id,
                    identity_type="function",
                    identity_key=str(row["function_id"]),
                    fingerprint=str(row["function_fingerprint"]),
                    event_id=str(row["event_id"]),
                    payload_sha256=identity_sha256,
                    created_at=str(row["created_at"]),
                )
            if row["mutant_fingerprint"]:
                self._bind_identity(
                    scope_id=scope_id,
                    identity_type="mutant",
                    identity_key=str(row["mutant_id"]),
                    fingerprint=str(row["mutant_fingerprint"]),
                    event_id=str(row["event_id"]),
                    payload_sha256=identity_sha256,
                    created_at=str(row["created_at"]),
                )
        observations = self._connection.execute(
            "SELECT o.observation_id, o.event_id, o.test_id, o.test_fingerprint, o.payload, o.created_at, e.scope_id "
            "FROM knowledge_test_observations o JOIN knowledge_executions e ON e.event_id = o.event_id "
            "ORDER BY o.created_at, o.observation_id"
        ).fetchall()
        for row in observations:
            payload_sha256 = stable_hash(json.loads(str(row["payload"])))
            scope_id = str(row["scope_id"] or "")
            self._connection.execute(
                "UPDATE knowledge_test_observations SET scope_id = ?, observation_sha256 = ? WHERE observation_id = ?",
                (scope_id, payload_sha256, row["observation_id"]),
            )
            self._bind_identity(
                scope_id=scope_id,
                identity_type="test",
                identity_key=str(row["test_id"]),
                fingerprint=str(row["test_fingerprint"]),
                event_id=str(row["event_id"]),
                payload_sha256=payload_sha256,
                created_at=str(row["created_at"]),
            )
        trigger_statements = (
            """CREATE TRIGGER IF NOT EXISTS knowledge_effects_canonical_insert
            BEFORE INSERT ON knowledge_effects
            WHEN NEW.scope_id IS NULL OR NEW.scope_id = '' OR NEW.project_id IS NULL OR NEW.project_id = ''
              OR NEW.revision_id IS NULL OR NEW.revision_id = '' OR NEW.environment_id IS NULL OR NEW.environment_id = ''
              OR NEW.identity_source IS NULL OR NEW.identity_source = '' OR NEW.observed_at IS NULL OR NEW.observed_at = ''
              OR NEW.ingested_at IS NULL OR NEW.ingested_at = ''
            BEGIN
                SELECT RAISE(ABORT, 'knowledge_effects requires canonical scope identity');
            END""",
            """CREATE TRIGGER IF NOT EXISTS knowledge_executions_canonical_insert
            BEFORE INSERT ON knowledge_executions
            WHEN NEW.scope_id IS NULL OR NEW.scope_id = '' OR NEW.identity_sha256 IS NULL OR NEW.identity_sha256 = ''
            BEGIN
                SELECT RAISE(ABORT, 'knowledge_executions requires canonical identity');
            END""",
            """CREATE TRIGGER IF NOT EXISTS knowledge_observations_canonical_insert
            BEFORE INSERT ON knowledge_test_observations
            WHEN NEW.scope_id IS NULL OR NEW.scope_id = '' OR NEW.observation_sha256 IS NULL OR NEW.observation_sha256 = ''
            BEGIN
                SELECT RAISE(ABORT, 'knowledge_test_observations requires canonical identity');
            END""",
            """CREATE TRIGGER IF NOT EXISTS knowledge_effects_append_only_update
            BEFORE UPDATE OF effect_id, campaign_id, effect_type, payload_sha256, payload, control_revision, created_at, scope_id, project_id, revision_id, environment_id, identity_source, observed_at, ingested_at
            ON knowledge_effects BEGIN
                SELECT RAISE(ABORT, 'knowledge_effects is append-only');
            END""",
            """CREATE TRIGGER IF NOT EXISTS knowledge_executions_append_only_update
            BEFORE UPDATE OF event_id, effect_id, campaign_id, execution_id, mutant_id, attempt, status, payload, created_at, scope_id, identity_sha256
            ON knowledge_executions BEGIN
                SELECT RAISE(ABORT, 'knowledge_executions is append-only');
            END""",
            """CREATE TRIGGER IF NOT EXISTS knowledge_observations_append_only_update
            BEFORE UPDATE OF observation_id, event_id, effect_id, campaign_id, execution_id, mutant_id, test_id, test_fingerprint, outcome, payload, created_at, scope_id, observation_sha256
            ON knowledge_test_observations BEGIN
                SELECT RAISE(ABORT, 'knowledge_test_observations is append-only');
            END""",
            """CREATE TRIGGER IF NOT EXISTS knowledge_effects_append_only_delete
            BEFORE DELETE ON knowledge_effects BEGIN
                SELECT RAISE(ABORT, 'knowledge_effects is append-only');
            END""",
            """CREATE TRIGGER IF NOT EXISTS knowledge_scopes_append_only_update
            BEFORE UPDATE ON knowledge_scopes BEGIN
                SELECT RAISE(ABORT, 'knowledge_scopes is append-only');
            END""",
            """CREATE TRIGGER IF NOT EXISTS knowledge_scopes_append_only_delete
            BEFORE DELETE ON knowledge_scopes BEGIN
                SELECT RAISE(ABORT, 'knowledge_scopes is append-only');
            END""",
            """CREATE TRIGGER IF NOT EXISTS knowledge_identity_bindings_append_only_update
            BEFORE UPDATE ON knowledge_identity_bindings BEGIN
                SELECT RAISE(ABORT, 'knowledge_identity_bindings is append-only');
            END""",
            """CREATE TRIGGER IF NOT EXISTS knowledge_identity_bindings_append_only_delete
            BEFORE DELETE ON knowledge_identity_bindings BEGIN
                SELECT RAISE(ABORT, 'knowledge_identity_bindings is append-only');
            END""",
        )
        for statement in trigger_statements:
            self._connection.execute(statement)
    def _migrate_provenance_evidence_graph(self) -> None:
        # Create append-only provenance graph tables and backfill every existing execution deterministically.
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS knowledge_evidence_nodes (
                node_id TEXT PRIMARY KEY,
                scope_id TEXT NOT NULL,
                node_type TEXT NOT NULL,
                identity_key TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                effect_id TEXT,
                execution_id TEXT,
                lease_id TEXT,
                worker_id TEXT,
                producer_type TEXT NOT NULL,
                producer_id TEXT NOT NULL,
                producer_version TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                payload TEXT NOT NULL,
                UNIQUE(scope_id, node_type, identity_key, fingerprint, event_id)
            );
            CREATE INDEX IF NOT EXISTS ix_knowledge_evidence_node_execution
                ON knowledge_evidence_nodes(execution_id, node_type, node_id);
            CREATE INDEX IF NOT EXISTS ix_knowledge_evidence_node_scope
                ON knowledge_evidence_nodes(scope_id, node_type, identity_key);
            CREATE TABLE IF NOT EXISTS knowledge_evidence_edges (
                edge_id TEXT PRIMARY KEY,
                scope_id TEXT NOT NULL,
                source_node_id TEXT NOT NULL,
                relation TEXT NOT NULL,
                target_node_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                execution_id TEXT,
                created_at TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                payload TEXT NOT NULL,
                UNIQUE(scope_id, source_node_id, relation, target_node_id, event_id)
            );
            CREATE INDEX IF NOT EXISTS ix_knowledge_evidence_edge_execution
                ON knowledge_evidence_edges(execution_id, relation, edge_id);
            CREATE INDEX IF NOT EXISTS ix_knowledge_evidence_edge_source
                ON knowledge_evidence_edges(scope_id, source_node_id, relation);
            CREATE TRIGGER IF NOT EXISTS knowledge_evidence_nodes_append_only_update
            BEFORE UPDATE ON knowledge_evidence_nodes BEGIN
                SELECT RAISE(ABORT, 'knowledge_evidence_nodes is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS knowledge_evidence_nodes_append_only_delete
            BEFORE DELETE ON knowledge_evidence_nodes BEGIN
                SELECT RAISE(ABORT, 'knowledge_evidence_nodes is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS knowledge_evidence_edges_append_only_update
            BEFORE UPDATE ON knowledge_evidence_edges BEGIN
                SELECT RAISE(ABORT, 'knowledge_evidence_edges is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS knowledge_evidence_edges_append_only_delete
            BEFORE DELETE ON knowledge_evidence_edges BEGIN
                SELECT RAISE(ABORT, 'knowledge_evidence_edges is append-only');
            END;
            """
        )
        rows = self._connection.execute(
            "SELECT e.event_id, e.effect_id, e.campaign_id, e.execution_id, e.payload, e.created_at, e.scope_id, "
            "k.payload AS effect_payload, k.effect_type, k.project_id, k.revision_id, k.environment_id "
            "FROM knowledge_executions e JOIN knowledge_effects k ON k.effect_id = e.effect_id "
            "ORDER BY e.created_at, e.event_id"
        ).fetchall()
        for row in rows:
            execution_payload = json.loads(str(row["payload"]))
            effect_payload = json.loads(str(row["effect_payload"]))
            self._project_execution_evidence(
                scope_id=str(row["scope_id"] or ""),
                project_id=str(row["project_id"] or ""),
                revision_id=str(row["revision_id"] or ""),
                environment_id=str(row["environment_id"] or ""),
                effect_id=str(row["effect_id"]),
                effect_type=str(row["effect_type"]),
                campaign_id=str(row["campaign_id"]),
                event_id=str(row["event_id"]),
                execution_payload=execution_payload if isinstance(execution_payload, Mapping) else {},
                effect_payload=effect_payload if isinstance(effect_payload, Mapping) else {},
                created_at=str(row["created_at"]),
            )
    def _migrate_conflict_compaction_closure(self) -> None:
        # Add durable conflict quarantine, live evidence references, tombstones, and atomic compaction receipts.
        for definition in (
            "graph_fingerprint TEXT",
            "evidence_payload TEXT",
            "evidence_sha256 TEXT",
            "observation_count INTEGER NOT NULL DEFAULT 0",
            "compaction_id TEXT",
        ):
            self._add_column("knowledge_execution_rollups", definition)
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS knowledge_conflict_quarantine (
                conflict_id TEXT PRIMARY KEY,
                conflict_type TEXT NOT NULL,
                identity_type TEXT NOT NULL,
                identity_key TEXT NOT NULL,
                scope_id TEXT,
                existing_sha256 TEXT,
                incoming_sha256 TEXT NOT NULL,
                existing_payload TEXT,
                incoming_payload TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_knowledge_conflict_quarantine_status
                ON knowledge_conflict_quarantine(status, created_at, conflict_id);
            CREATE TABLE IF NOT EXISTS knowledge_reuse_plans (
                plan_id TEXT PRIMARY KEY,
                campaign_id TEXT NOT NULL,
                plan_fingerprint TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                released_at TEXT
            );
            CREATE INDEX IF NOT EXISTS ix_knowledge_reuse_plan_live
                ON knowledge_reuse_plans(campaign_id, released_at, plan_id);
            CREATE TABLE IF NOT EXISTS knowledge_evidence_references (
                reference_id TEXT PRIMARY KEY,
                plan_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                execution_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(plan_id, event_id),
                FOREIGN KEY(plan_id) REFERENCES knowledge_reuse_plans(plan_id)
            );
            CREATE INDEX IF NOT EXISTS ix_knowledge_evidence_reference_event
                ON knowledge_evidence_references(event_id, plan_id);
            CREATE TABLE IF NOT EXISTS knowledge_compaction_runs (
                compaction_id TEXT PRIMARY KEY,
                source_revision INTEGER NOT NULL,
                cutoff TEXT NOT NULL,
                manifest_sha256 TEXT NOT NULL,
                manifest TEXT NOT NULL,
                event_count INTEGER NOT NULL,
                observation_count INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                committed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_knowledge_compaction_created
                ON knowledge_compaction_runs(created_at, compaction_id);
            CREATE TABLE IF NOT EXISTS knowledge_tombstones (
                tombstone_id TEXT PRIMARY KEY,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                source_event_id TEXT NOT NULL,
                replacement_id TEXT NOT NULL,
                reason TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(entity_type, entity_id, source_event_id)
            );
            CREATE INDEX IF NOT EXISTS ix_knowledge_tombstone_source
                ON knowledge_tombstones(source_event_id, entity_type);
            CREATE TRIGGER IF NOT EXISTS knowledge_conflict_quarantine_append_only_update
            BEFORE UPDATE ON knowledge_conflict_quarantine BEGIN
                SELECT RAISE(ABORT, 'knowledge_conflict_quarantine is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS knowledge_conflict_quarantine_append_only_delete
            BEFORE DELETE ON knowledge_conflict_quarantine BEGIN
                SELECT RAISE(ABORT, 'knowledge_conflict_quarantine is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS knowledge_compaction_runs_append_only_update
            BEFORE UPDATE ON knowledge_compaction_runs BEGIN
                SELECT RAISE(ABORT, 'knowledge_compaction_runs is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS knowledge_compaction_runs_append_only_delete
            BEFORE DELETE ON knowledge_compaction_runs BEGIN
                SELECT RAISE(ABORT, 'knowledge_compaction_runs is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS knowledge_tombstones_append_only_update
            BEFORE UPDATE ON knowledge_tombstones BEGIN
                SELECT RAISE(ABORT, 'knowledge_tombstones is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS knowledge_tombstones_append_only_delete
            BEFORE DELETE ON knowledge_tombstones BEGIN
                SELECT RAISE(ABORT, 'knowledge_tombstones is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS knowledge_evidence_references_append_only_update
            BEFORE UPDATE ON knowledge_evidence_references BEGIN
                SELECT RAISE(ABORT, 'knowledge_evidence_references is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS knowledge_evidence_references_append_only_delete
            BEFORE DELETE ON knowledge_evidence_references BEGIN
                SELECT RAISE(ABORT, 'knowledge_evidence_references is append-only');
            END;
            """
        )
        self._backfill_legacy_compactions()
    def _backfill_legacy_compactions(self) -> None:
        # Upgrade pre-PR13 rollups into manifest-backed evidence bundles without inventing unavailable raw facts.
        rows = self._connection.execute(
            "SELECT * FROM knowledge_execution_rollups WHERE evidence_payload IS NULL "
            "ORDER BY created_at, event_id"
        ).fetchall()
        if not rows:
            return
        source_revision = self._current_revision()
        timestamp = _normalize_timestamp(utc_now_iso())
        compaction_id = stable_hash(
            {
                "migration": "knowledge-schema-v4-legacy-compaction-backfill",
                "source_revision": source_revision,
                "events": tuple(str(row["event_id"]) for row in rows),
            }
        )[:32]
        manifest_rows: list[dict[str, Any]] = []
        observation_count = 0
        for row in rows:
            event_id = str(row["event_id"])
            execution_id = str(row["execution_id"])
            try:
                graph = self.evidence_graph(execution_id)
                graph_fingerprint = graph.graph_fingerprint
                graph_complete = graph.complete
                missing_requirements = list(graph.missing_requirements)
            except KnowledgeConflict:
                graph_fingerprint = stable_hash(
                    {
                        "legacy_event_id": event_id,
                        "execution_id": execution_id,
                        "identity_sha256": str(row["identity_sha256"] or ""),
                    }
                )
                graph_complete = False
                missing_requirements = ["provenance_graph"]
            observation_rows = self._connection.execute(
                "SELECT payload FROM knowledge_evidence_nodes WHERE event_id = ? AND execution_id = ? "
                "AND node_type = ? ORDER BY node_id",
                (event_id, execution_id, EvidenceNodeType.OBSERVATION.value),
            ).fetchall()
            observations = [json.loads(str(item["payload"])) for item in observation_rows]
            observation_count += len(observations)
            legacy_rollup = {
                key: row[key]
                for key in row.keys()
                if key not in {
                    "evidence_payload",
                    "evidence_sha256",
                    "graph_fingerprint",
                    "observation_count",
                    "compaction_id",
                }
            }
            evidence_bundle = {
                "schema_version": 1,
                "event_id": event_id,
                "execution_id": execution_id,
                "execution": None,
                "legacy_rollup": legacy_rollup,
                "observations": observations,
                "graph_fingerprint": graph_fingerprint,
                "graph_complete": graph_complete,
                "graph_missing_requirements": missing_requirements,
                "raw_execution_available": False,
            }
            evidence_sha256 = stable_hash(evidence_bundle)
            self._connection.execute(
                "UPDATE knowledge_execution_rollups SET graph_fingerprint = ?, evidence_payload = ?, "
                "evidence_sha256 = ?, observation_count = ?, compaction_id = ? WHERE event_id = ?",
                (
                    graph_fingerprint,
                    _canonical(evidence_bundle),
                    evidence_sha256,
                    len(observations),
                    compaction_id,
                    event_id,
                ),
            )
            tombstone_payload = {
                "event_id": event_id,
                "execution_id": execution_id,
                "compaction_id": compaction_id,
                "evidence_sha256": evidence_sha256,
                "graph_fingerprint": graph_fingerprint,
                "legacy_backfill": True,
            }
            tombstone_id = stable_hash(
                {"entity_type": "execution", "entity_id": execution_id, "event_id": event_id}
            )[:32]
            self._connection.execute(
                "INSERT OR IGNORE INTO knowledge_tombstones(tombstone_id, entity_type, entity_id, source_event_id, "
                "replacement_id, reason, payload_sha256, payload, created_at) "
                "VALUES (?, 'execution', ?, ?, ?, ?, ?, ?, ?)",
                (
                    tombstone_id,
                    execution_id,
                    event_id,
                    compaction_id,
                    "legacy rollup upgraded to schema-v4 evidence bundle",
                    stable_hash(tombstone_payload),
                    _canonical(tombstone_payload),
                    timestamp,
                ),
            )
            manifest_rows.append(
                {
                    "event_id": event_id,
                    "execution_id": execution_id,
                    "identity_sha256": str(row["identity_sha256"] or ""),
                    "evidence_sha256": evidence_sha256,
                    "graph_fingerprint": graph_fingerprint,
                    "observation_count": len(observations),
                    "tombstone_id": tombstone_id,
                    "legacy_backfill": True,
                }
            )
        manifest = {
            "schema_version": 1,
            "compaction_id": compaction_id,
            "source_revision": source_revision,
            "cutoff": timestamp,
            "events": manifest_rows,
            "legacy_backfill": True,
        }
        self._connection.execute(
            "INSERT OR IGNORE INTO knowledge_compaction_runs(compaction_id, source_revision, cutoff, manifest_sha256, "
            "manifest, event_count, observation_count, created_at, committed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                compaction_id,
                source_revision,
                timestamp,
                stable_hash(manifest),
                _canonical(manifest),
                len(rows),
                observation_count,
                timestamp,
                timestamp,
            ),
        )
    def _insert_scope(self, scope: KnowledgeScopeIdentity, *, created_at: str) -> None:
        # Persist one canonical scope and reject hash or component collisions.
        existing = self._connection.execute(
            "SELECT project_id, revision_id, environment_id FROM knowledge_scopes WHERE scope_id = ?",
            (scope.scope_id,),
        ).fetchone()
        if existing is not None and (
            str(existing["project_id"]),
            str(existing["revision_id"]),
            str(existing["environment_id"]),
        ) != (scope.project_id, scope.revision_id, scope.environment_id):
            raise KnowledgeConflict(
                "knowledge scope identity conflict: "
                f"scope_id={scope.scope_id}; expected={(scope.project_id, scope.revision_id, scope.environment_id)}; "
                f"actual={(existing['project_id'], existing['revision_id'], existing['environment_id'])}"
            )
        self._connection.execute(
            "INSERT OR IGNORE INTO knowledge_scopes(scope_id, project_id, revision_id, environment_id, identity_source, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                scope.scope_id,
                scope.project_id,
                scope.revision_id,
                scope.environment_id,
                scope.identity_source,
                created_at,
            ),
        )
    def _bind_identity(
        self,
        *,
        scope_id: str,
        identity_type: str,
        identity_key: str,
        fingerprint: str,
        event_id: str,
        payload_sha256: str,
        created_at: str,
    ) -> None:
        # Bind one scoped entity key to exactly one immutable fingerprint.
        if not all(str(item).strip() for item in (scope_id, identity_type, identity_key, fingerprint, event_id, payload_sha256)):
            raise ValueError(
                "knowledge identity binding requires scope, type, key, fingerprint, event, and payload hash"
            )
        existing = self._connection.execute(
            "SELECT fingerprint, first_event_id, payload_sha256 FROM knowledge_identity_bindings "
            "WHERE scope_id = ? AND identity_type = ? AND identity_key = ?",
            (scope_id, identity_type, identity_key),
        ).fetchone()
        if existing is not None:
            if str(existing["fingerprint"]) != fingerprint:
                raise KnowledgeConflict(
                    "knowledge identity binding conflict: "
                    f"scope_id={scope_id}; identity_type={identity_type}; identity_key={identity_key}; "
                    f"existing_fingerprint={existing['fingerprint']}; received_fingerprint={fingerprint}; "
                    f"existing_event_id={existing['first_event_id']}; received_event_id={event_id}"
                )
            return
        self._connection.execute(
            "INSERT INTO knowledge_identity_bindings(scope_id, identity_type, identity_key, fingerprint, first_event_id, payload_sha256, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (scope_id, identity_type, identity_key, fingerprint, event_id, payload_sha256, created_at),
        )
    @staticmethod
    def _optional_text(value: Any) -> str | None:
        # Normalize optional provenance fields without accepting whitespace-only identities.
        text = str(value).strip() if value is not None else ""
        return text or None
    def _insert_evidence_node(
        self,
        *,
        scope_id: str,
        node_type: EvidenceNodeType,
        identity_key: str,
        fingerprint: str,
        campaign_id: str,
        event_id: str,
        effect_id: str | None,
        execution_id: str | None,
        lease_id: str | None,
        worker_id: str | None,
        producer_type: str,
        producer_id: str,
        producer_version: str,
        observed_at: str,
        payload: Mapping[str, Any],
    ) -> str:
        # Insert one immutable graph node or verify the exact existing entity projection.
        if not all(
            str(item).strip()
            for item in (
                scope_id,
                node_type.value,
                identity_key,
                fingerprint,
                campaign_id,
                event_id,
                producer_type,
                producer_id,
                producer_version,
                observed_at,
            )
        ):
            raise ValueError("knowledge evidence node identity fields must be non-empty")
        payload_value = dict(payload)
        payload_json = _canonical(payload_value)
        payload_sha256 = stable_hash(payload_value)
        node_id = stable_hash(
            {
                "scope_id": scope_id,
                "node_type": node_type.value,
                "identity_key": identity_key,
                "fingerprint": fingerprint,
                "event_id": event_id,
            }
        )[:32]
        existing = self._connection.execute(
            "SELECT scope_id, node_type, identity_key, fingerprint, event_id, payload_sha256 FROM knowledge_evidence_nodes "
            "WHERE node_id = ?",
            (node_id,),
        ).fetchone()
        if existing is not None:
            actual = (
                str(existing["scope_id"]),
                str(existing["node_type"]),
                str(existing["identity_key"]),
                str(existing["fingerprint"]),
                str(existing["event_id"]),
                str(existing["payload_sha256"]),
            )
            expected = (scope_id, node_type.value, identity_key, fingerprint, event_id, payload_sha256)
            if actual != expected:
                raise KnowledgeConflict(
                    "knowledge evidence node conflict: "
                    f"node_id={node_id}; expected={expected}; actual={actual}; database={self.path}"
                )
            return node_id
        self._connection.execute(
            "INSERT INTO knowledge_evidence_nodes(node_id, scope_id, node_type, identity_key, fingerprint, campaign_id, "
            "event_id, effect_id, execution_id, lease_id, worker_id, producer_type, producer_id, producer_version, observed_at, "
            "payload_sha256, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                node_id,
                scope_id,
                node_type.value,
                identity_key,
                fingerprint,
                campaign_id,
                event_id,
                effect_id,
                execution_id,
                lease_id,
                worker_id,
                producer_type,
                producer_id,
                producer_version,
                observed_at,
                payload_sha256,
                payload_json,
            ),
        )
        return node_id
    def _insert_evidence_edge(
        self,
        *,
        scope_id: str,
        source_node_id: str,
        relation: EvidenceRelation,
        target_node_id: str,
        event_id: str,
        execution_id: str | None,
        created_at: str,
        payload: Mapping[str, Any] | None = None,
    ) -> str:
        # Insert one event-scoped graph edge or verify that an idempotent replay is exact.
        payload_value = dict(payload or {})
        payload_json = _canonical(payload_value)
        payload_sha256 = stable_hash(payload_value)
        edge_id = stable_hash(
            {
                "scope_id": scope_id,
                "source_node_id": source_node_id,
                "relation": relation.value,
                "target_node_id": target_node_id,
                "event_id": event_id,
            }
        )[:32]
        existing = self._connection.execute(
            "SELECT scope_id, source_node_id, relation, target_node_id, event_id, payload_sha256 "
            "FROM knowledge_evidence_edges WHERE edge_id = ?",
            (edge_id,),
        ).fetchone()
        if existing is not None:
            actual = (
                str(existing["scope_id"]),
                str(existing["source_node_id"]),
                str(existing["relation"]),
                str(existing["target_node_id"]),
                str(existing["event_id"]),
                str(existing["payload_sha256"]),
            )
            expected = (
                scope_id,
                source_node_id,
                relation.value,
                target_node_id,
                event_id,
                payload_sha256,
            )
            if actual != expected:
                raise KnowledgeConflict(
                    "knowledge evidence edge conflict: "
                    f"edge_id={edge_id}; expected={expected}; actual={actual}; database={self.path}"
                )
            return edge_id
        self._connection.execute(
            "INSERT INTO knowledge_evidence_edges(edge_id, scope_id, source_node_id, relation, target_node_id, event_id, "
            "execution_id, created_at, payload_sha256, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                edge_id,
                scope_id,
                source_node_id,
                relation.value,
                target_node_id,
                event_id,
                execution_id,
                created_at,
                payload_sha256,
                payload_json,
            ),
        )
        return edge_id
    def _project_execution_evidence(
        self,
        *,
        scope_id: str,
        project_id: str,
        revision_id: str,
        environment_id: str,
        effect_id: str,
        effect_type: str,
        campaign_id: str,
        event_id: str,
        execution_payload: Mapping[str, Any],
        effect_payload: Mapping[str, Any],
        created_at: str,
    ) -> None:
        # Materialize one bounded revision-to-artifact provenance chain from committed execution evidence.
        if not all(str(item).strip() for item in (scope_id, revision_id, environment_id, effect_id, campaign_id, event_id)):
            raise KnowledgeConflict(
                "knowledge provenance graph requires canonical scope and event identity: "
                f"scope_id={scope_id!r}; revision_id={revision_id!r}; environment_id={environment_id!r}; "
                f"effect_id={effect_id!r}; event_id={event_id!r}"
            )
        execution_id = str(execution_payload.get("execution_id", "")).strip()
        mutant_id = str(execution_payload.get("mutant_id", "")).strip()
        if not execution_id or not mutant_id:
            raise KnowledgeConflict(
                "knowledge provenance graph requires execution and mutant identity: "
                f"effect_id={effect_id}; event_id={event_id}; execution_id={execution_id!r}; mutant_id={mutant_id!r}"
            )
        raw_shard = effect_payload.get("shard", {})
        shard = raw_shard if isinstance(raw_shard, Mapping) else {}
        raw_lease = shard.get("lease", {})
        lease = raw_lease if isinstance(raw_lease, Mapping) else {}
        lease_id = self._optional_text(execution_payload.get("lease_id") or lease.get("lease_id"))
        worker_id = self._optional_text(shard.get("worker_id") or lease.get("worker_id"))
        worker_instance_id = self._optional_text(lease.get("worker_instance_id"))
        process_birth_token = self._optional_text(lease.get("process_birth_token"))
        producer_type = "worker" if worker_id else "gallifrey_effect"
        producer_id = worker_id or effect_id
        producer_version = f"knowledge-schema-{KNOWLEDGE_SCHEMA_VERSION}:{effect_type or 'unknown-effect'}"
        common = {
            "scope_id": scope_id,
            "campaign_id": campaign_id,
            "event_id": event_id,
            "effect_id": effect_id,
            "execution_id": execution_id,
            "lease_id": lease_id,
            "worker_id": worker_id,
            "producer_type": producer_type,
            "producer_id": producer_id,
            "producer_version": producer_version,
            "observed_at": created_at,
        }
        revision_node = self._insert_evidence_node(
            node_type=EvidenceNodeType.REVISION,
            identity_key=revision_id,
            fingerprint=revision_id,
            payload={"project_id": project_id, "revision_id": revision_id},
            **common,
        )
        environment_node = self._insert_evidence_node(
            node_type=EvidenceNodeType.ENVIRONMENT,
            identity_key=environment_id,
            fingerprint=environment_id,
            payload={"environment_id": environment_id},
            **common,
        )
        execution_fingerprint = stable_hash(
            {
                "execution_id": execution_id,
                "mutant_id": mutant_id,
                "attempt": int(execution_payload.get("attempt", 0) or 0),
                "status": str(execution_payload.get("semantic_result") or execution_payload.get("status") or "unknown"),
                "payload": dict(execution_payload),
            }
        )
        execution_node = self._insert_evidence_node(
            node_type=EvidenceNodeType.EXECUTION,
            identity_key=execution_id,
            fingerprint=execution_fingerprint,
            payload={
                "execution_id": execution_id,
                "shard_id": self._optional_text(execution_payload.get("shard_id") or shard.get("shard_id")),
                "attempt": int(execution_payload.get("attempt", 0) or 0),
                "restore_verified": bool(execution_payload.get("restore_verified", False)),
                "source_kind": str(execution_payload.get("source_kind", "observed")),
                "evidence_quality": _evidence_quality(execution_payload).value,
            },
            **common,
        )
        self._insert_evidence_edge(
            scope_id=scope_id,
            source_node_id=environment_node,
            relation=EvidenceRelation.ENVIRONMENT_GOVERNS_EXECUTION,
            target_node_id=execution_node,
            event_id=event_id,
            execution_id=execution_id,
            created_at=created_at,
        )
        function_id = self._optional_text(execution_payload.get("function_id"))
        function_fingerprint = self._optional_text(_nested_value(execution_payload, "function_fingerprint"))
        function_node: str | None = None
        if function_id or function_fingerprint:
            function_key = function_id or f"fingerprint:{function_fingerprint}"
            function_node = self._insert_evidence_node(
                node_type=EvidenceNodeType.FUNCTION,
                identity_key=str(function_key),
                fingerprint=function_fingerprint or stable_hash({"function_id": function_key}),
                payload={"function_id": function_id, "function_fingerprint": function_fingerprint},
                **common,
            )
            self._insert_evidence_edge(
                scope_id=scope_id,
                source_node_id=revision_node,
                relation=EvidenceRelation.REVISION_CONTAINS_FUNCTION,
                target_node_id=function_node,
                event_id=event_id,
                execution_id=execution_id,
                created_at=created_at,
            )
        mutant_fingerprint = self._optional_text(_nested_value(execution_payload, "mutant_fingerprint"))
        mutant_node = self._insert_evidence_node(
            node_type=EvidenceNodeType.MUTANT,
            identity_key=mutant_id,
            fingerprint=mutant_fingerprint or stable_hash({"scope_id": scope_id, "mutant_id": mutant_id}),
            payload={"mutant_id": mutant_id, "mutant_fingerprint": mutant_fingerprint},
            **common,
        )
        if function_node is not None:
            self._insert_evidence_edge(
                scope_id=scope_id,
                source_node_id=function_node,
                relation=EvidenceRelation.FUNCTION_DEFINES_MUTANT,
                target_node_id=mutant_node,
                event_id=event_id,
                execution_id=execution_id,
                created_at=created_at,
            )
        self._insert_evidence_edge(
            scope_id=scope_id,
            source_node_id=mutant_node,
            relation=EvidenceRelation.MUTANT_EXECUTED_AS,
            target_node_id=execution_node,
            event_id=event_id,
            execution_id=execution_id,
            created_at=created_at,
        )
        if worker_id:
            worker_node = self._insert_evidence_node(
                node_type=EvidenceNodeType.WORKER,
                identity_key=worker_id,
                fingerprint=stable_hash(
                    {
                        "worker_id": worker_id,
                        "worker_instance_id": worker_instance_id,
                        "process_birth_token": process_birth_token,
                    }
                ),
                payload={
                    "worker_id": worker_id,
                    "worker_instance_id": worker_instance_id,
                    "process_id": lease.get("process_id"),
                    "process_birth_token": process_birth_token,
                },
                **common,
            )
            self._insert_evidence_edge(
                scope_id=scope_id,
                source_node_id=worker_node,
                relation=EvidenceRelation.WORKER_PERFORMED_EXECUTION,
                target_node_id=execution_node,
                event_id=event_id,
                execution_id=execution_id,
                created_at=created_at,
            )
        if lease_id:
            lease_node = self._insert_evidence_node(
                node_type=EvidenceNodeType.LEASE,
                identity_key=lease_id,
                fingerprint=stable_hash(
                    {
                        "lease_id": lease_id,
                        "worker_id": worker_id,
                        "attempt": execution_payload.get("attempt", lease.get("attempt")),
                        "worker_instance_id": worker_instance_id,
                    }
                ),
                payload={
                    "lease_id": lease_id,
                    "worker_id": worker_id,
                    "attempt": execution_payload.get("attempt", lease.get("attempt")),
                    "worker_instance_id": worker_instance_id,
                },
                **common,
            )
            self._insert_evidence_edge(
                scope_id=scope_id,
                source_node_id=lease_node,
                relation=EvidenceRelation.LEASE_AUTHORIZED_EXECUTION,
                target_node_id=execution_node,
                event_id=event_id,
                execution_id=execution_id,
                created_at=created_at,
            )
        observations_raw = execution_payload.get("test_observations", ())
        observations = tuple(item for item in observations_raw if isinstance(item, Mapping)) if isinstance(observations_raw, (list, tuple)) else ()
        fingerprints_raw = execution_payload.get("test_fingerprints", {})
        test_fingerprints = {
            str(key): str(value)
            for key, value in fingerprints_raw.items()
        } if isinstance(fingerprints_raw, Mapping) else {}
        for observation in observations:
            test_id = str(observation.get("test_id") or observation.get("nodeid") or "").strip()
            test_fingerprint = str(observation.get("test_fingerprint") or observation.get("fingerprint") or "").strip()
            if test_id and test_fingerprint:
                test_fingerprints.setdefault(test_id, test_fingerprint)
        selected_raw = execution_payload.get("selected_tests", ())
        selected_tests = {
            str(item).strip()
            for item in selected_raw
            if str(item).strip()
        } if isinstance(selected_raw, (list, tuple)) else set()
        selected_tests.update(test_fingerprints)
        selected_nodes: dict[str, str] = {}
        for test_id in sorted(selected_tests):
            test_fingerprint = test_fingerprints.get(test_id)
            selected_node = self._insert_evidence_node(
                node_type=EvidenceNodeType.SELECTED_TEST,
                identity_key=f"{execution_id}\x1f{test_id}",
                fingerprint=test_fingerprint or stable_hash(
                    {"scope_id": scope_id, "execution_id": execution_id, "test_id": test_id, "state": "unresolved"}
                ),
                payload={
                    "test_id": test_id,
                    "test_fingerprint": test_fingerprint,
                    "fingerprint_resolved": bool(test_fingerprint),
                },
                **common,
            )
            selected_nodes[test_id] = selected_node
            for source_node, relation in (
                (execution_node, EvidenceRelation.EXECUTION_SELECTED_TEST),
                (mutant_node, EvidenceRelation.MUTANT_SELECTED_TEST),
            ):
                self._insert_evidence_edge(
                    scope_id=scope_id,
                    source_node_id=source_node,
                    relation=relation,
                    target_node_id=selected_node,
                    event_id=event_id,
                    execution_id=execution_id,
                    created_at=created_at,
                )
        observation_nodes: list[str] = []
        for observation in observations:
            test_id = str(observation.get("test_id") or observation.get("nodeid") or "").strip()
            if not test_id:
                continue
            observation_payload = dict(observation)
            observation_fingerprint = stable_hash(observation_payload)
            observation_node = self._insert_evidence_node(
                node_type=EvidenceNodeType.OBSERVATION,
                identity_key=f"{event_id}\x1f{test_id}",
                fingerprint=observation_fingerprint,
                payload=observation_payload,
                **common,
            )
            observation_nodes.append(observation_node)
            selected_node = selected_nodes.get(test_id)
            if selected_node is not None:
                self._insert_evidence_edge(
                    scope_id=scope_id,
                    source_node_id=selected_node,
                    relation=EvidenceRelation.SELECTED_TEST_PRODUCED_OBSERVATION,
                    target_node_id=observation_node,
                    event_id=event_id,
                    execution_id=execution_id,
                    created_at=created_at,
                )
        semantic_result = str(execution_payload.get("semantic_result") or execution_payload.get("status") or "unknown")
        result_fingerprint = self._optional_text(_nested_value(execution_payload, "result_fingerprint"))
        artifact_values: list[str] = []
        for key in ("artifacts", "artifact_paths", "output_artifacts"):
            raw_values = execution_payload.get(key, ())
            if isinstance(raw_values, (list, tuple)):
                artifact_values.extend(str(item).strip() for item in raw_values if str(item).strip())
            if artifact_values and key == "artifacts":
                break
        artifact_values = list(dict.fromkeys(artifact_values))
        result_node = self._insert_evidence_node(
            node_type=EvidenceNodeType.RESULT,
            identity_key=execution_id,
            fingerprint=result_fingerprint or stable_hash(
                {
                    "execution_id": execution_id,
                    "semantic_result": semantic_result,
                    "restore_verified": bool(execution_payload.get("restore_verified", False)),
                    "attempt": int(execution_payload.get("attempt", 0) or 0),
                }
            ),
            payload={
                "semantic_result": semantic_result,
                "restore_verified": bool(execution_payload.get("restore_verified", False)),
                "evidence_quality": _evidence_quality(execution_payload).value,
                "result_fingerprint": result_fingerprint,
                "artifact_count": len(artifact_values),
            },
            **common,
        )
        self._insert_evidence_edge(
            scope_id=scope_id,
            source_node_id=execution_node,
            relation=EvidenceRelation.EXECUTION_PRODUCED_RESULT,
            target_node_id=result_node,
            event_id=event_id,
            execution_id=execution_id,
            created_at=created_at,
        )
        for observation_node in observation_nodes:
            self._insert_evidence_edge(
                scope_id=scope_id,
                source_node_id=observation_node,
                relation=EvidenceRelation.OBSERVATION_SUPPORTS_RESULT,
                target_node_id=result_node,
                event_id=event_id,
                execution_id=execution_id,
                created_at=created_at,
            )
        for artifact_id in sorted(artifact_values):
            artifact_node = self._insert_evidence_node(
                node_type=EvidenceNodeType.ARTIFACT,
                identity_key=artifact_id,
                fingerprint=artifact_id,
                payload={"artifact_id": artifact_id},
                **common,
            )
            self._insert_evidence_edge(
                scope_id=scope_id,
                source_node_id=result_node,
                relation=EvidenceRelation.RESULT_PRODUCED_ARTIFACT,
                target_node_id=artifact_node,
                event_id=event_id,
                execution_id=execution_id,
                created_at=created_at,
            )
    @staticmethod
    def _evidence_node_from_row(row: sqlite3.Row) -> KnowledgeEvidenceNode:
        # Decode one SQLite graph node into the public immutable evidence model.
        payload = json.loads(str(row["payload"]))
        return KnowledgeEvidenceNode(
            node_id=str(row["node_id"]),
            scope_id=str(row["scope_id"]),
            node_type=EvidenceNodeType(str(row["node_type"])),
            identity_key=str(row["identity_key"]),
            fingerprint=str(row["fingerprint"]),
            campaign_id=str(row["campaign_id"]),
            event_id=str(row["event_id"]),
            effect_id=str(row["effect_id"]) if row["effect_id"] is not None else None,
            execution_id=str(row["execution_id"]) if row["execution_id"] is not None else None,
            lease_id=str(row["lease_id"]) if row["lease_id"] is not None else None,
            worker_id=str(row["worker_id"]) if row["worker_id"] is not None else None,
            producer_type=str(row["producer_type"]),
            producer_id=str(row["producer_id"]),
            producer_version=str(row["producer_version"]),
            observed_at=str(row["observed_at"]),
            payload_sha256=str(row["payload_sha256"]),
            payload=payload if isinstance(payload, Mapping) else {},
        )
    @staticmethod
    def _evidence_edge_from_row(row: sqlite3.Row) -> KnowledgeEvidenceEdge:
        # Decode one SQLite graph edge into the public immutable evidence model.
        payload = json.loads(str(row["payload"]))
        return KnowledgeEvidenceEdge(
            edge_id=str(row["edge_id"]),
            scope_id=str(row["scope_id"]),
            source_node_id=str(row["source_node_id"]),
            relation=EvidenceRelation(str(row["relation"])),
            target_node_id=str(row["target_node_id"]),
            event_id=str(row["event_id"]),
            execution_id=str(row["execution_id"]) if row["execution_id"] is not None else None,
            created_at=str(row["created_at"]),
            payload_sha256=str(row["payload_sha256"]),
            payload=payload if isinstance(payload, Mapping) else {},
        )
    def evidence_graph(self, execution_id: str, *, limit: int = 1000) -> KnowledgeEvidenceGraph:
        # Return one bounded provenance graph with deterministic completeness and revision diagnostics.
        if not isinstance(execution_id, str) or not execution_id.strip():
            raise ValueError("execution_id must be a non-empty string")
        if limit < 1 or limit > 5000:
            raise ValueError("limit must be between 1 and 5000")
        edge_rows = self._connection.execute(
            "SELECT * FROM knowledge_evidence_edges WHERE execution_id = ? "
            "ORDER BY relation, source_node_id, target_node_id, edge_id LIMIT ?",
            (execution_id, limit + 1),
        ).fetchall()
        if len(edge_rows) > limit:
            raise KnowledgeConflict(
                f"knowledge evidence graph exceeds bounded edge limit: execution_id={execution_id}; limit={limit}"
            )
        edges = tuple(self._evidence_edge_from_row(row) for row in edge_rows)
        node_ids = tuple(
            sorted(
                {
                    node_id
                    for edge in edges
                    for node_id in (edge.source_node_id, edge.target_node_id)
                }
            )
        )
        if not node_ids:
            raise KnowledgeConflict(
                f"knowledge evidence graph does not exist: execution_id={execution_id}; database={self.path}"
            )
        if len(node_ids) > limit:
            raise KnowledgeConflict(
                f"knowledge evidence graph exceeds bounded node limit: execution_id={execution_id}; limit={limit}"
            )
        placeholders = ",".join("?" for _ in node_ids)
        node_rows = self._connection.execute(
            "SELECT * FROM knowledge_evidence_nodes WHERE node_id IN ("
            + placeholders
            + ") ORDER BY node_type, identity_key, fingerprint, node_id",
            node_ids,
        ).fetchall()
        nodes = tuple(self._evidence_node_from_row(row) for row in node_rows)
        node_types = {item.node_type for item in nodes}
        result_nodes = [item for item in nodes if item.node_type is EvidenceNodeType.RESULT]
        result_payload = result_nodes[-1].payload if result_nodes else {}
        evidence_quality = str(result_payload.get("evidence_quality", EvidenceQuality.UNKNOWN.value))
        semantic_result = str(result_payload.get("semantic_result", ""))
        missing: list[str] = []
        for node_type in (
            EvidenceNodeType.REVISION,
            EvidenceNodeType.ENVIRONMENT,
            EvidenceNodeType.FUNCTION,
            EvidenceNodeType.MUTANT,
            EvidenceNodeType.EXECUTION,
            EvidenceNodeType.WORKER,
            EvidenceNodeType.LEASE,
            EvidenceNodeType.RESULT,
        ):
            if node_type not in node_types:
                missing.append(node_type.value)
        if evidence_quality == EvidenceQuality.VALIDATED.value or semantic_result in {"killed", "survived", "invalid_mutant"}:
            for node_type in (
                EvidenceNodeType.SELECTED_TEST,
                EvidenceNodeType.OBSERVATION,
            ):
                if node_type not in node_types:
                    missing.append(node_type.value)
        if int(result_payload.get("artifact_count", 0) or 0) > 0 and EvidenceNodeType.ARTIFACT not in node_types:
            missing.append(EvidenceNodeType.ARTIFACT.value)
        graph_fingerprint = stable_hash(
            {
                "execution_id": execution_id,
                "nodes": [item.to_dict() for item in nodes],
                "edges": [item.to_dict() for item in edges],
            }
        )
        return KnowledgeEvidenceGraph(
            scope_id=nodes[0].scope_id,
            execution_id=execution_id,
            nodes=nodes,
            edges=edges,
            graph_fingerprint=graph_fingerprint,
            complete=not missing,
            missing_requirements=tuple(sorted(set(missing))),
            snapshot_revision=self._current_revision(),
        )
    def schema_state(self) -> KnowledgeSchemaState:
        # Return migration, scope, and identity counts for startup and test diagnostics.
        version_row = self._connection.execute(
            "SELECT value FROM knowledge_meta WHERE key = 'schema_version'"
        ).fetchone()
        migrations = tuple(
            (int(row["version"]), str(row["name"]), str(row["checksum"]))
            for row in self._connection.execute(
                "SELECT version, name, checksum FROM knowledge_schema_migrations ORDER BY version"
            ).fetchall()
        )
        scope_count = int(self._connection.execute("SELECT COUNT(*) FROM knowledge_scopes").fetchone()[0])
        identity_count = int(
            self._connection.execute("SELECT COUNT(*) FROM knowledge_identity_bindings").fetchone()[0]
        )
        evidence_node_count = int(
            self._connection.execute("SELECT COUNT(*) FROM knowledge_evidence_nodes").fetchone()[0]
        )
        evidence_edge_count = int(
            self._connection.execute("SELECT COUNT(*) FROM knowledge_evidence_edges").fetchone()[0]
        )
        conflict_count = int(
            self._connection.execute("SELECT COUNT(*) FROM knowledge_conflict_quarantine").fetchone()[0]
        )
        tombstone_count = int(
            self._connection.execute("SELECT COUNT(*) FROM knowledge_tombstones").fetchone()[0]
        )
        compaction_count = int(
            self._connection.execute("SELECT COUNT(*) FROM knowledge_compaction_runs").fetchone()[0]
        )
        live_reference_count = int(
            self._connection.execute(
                "SELECT COUNT(*) FROM knowledge_evidence_references r "
                "JOIN knowledge_reuse_plans p ON p.plan_id = r.plan_id WHERE p.released_at IS NULL"
            ).fetchone()[0]
        )
        return KnowledgeSchemaState(
            int(version_row[0]) if version_row is not None else 0,
            KNOWLEDGE_SCHEMA_VERSION,
            migrations,
            scope_count,
            identity_count,
            evidence_node_count,
            evidence_edge_count,
            conflict_count,
            tombstone_count,
            compaction_count,
            live_reference_count,
        )
    def identity_bindings(
        self,
        *,
        scope_id: str | None = None,
        identity_type: str | None = None,
        limit: int = 1000,
    ) -> tuple[dict[str, str], ...]:
        # Read a bounded deterministic identity registry for diagnostics and migrations.
        if limit < 1 or limit > 10_000:
            raise ValueError("limit must be between 1 and 10000")
        clauses: list[str] = []
        params: list[Any] = []
        if scope_id is not None:
            clauses.append("scope_id = ?")
            params.append(str(scope_id))
        if identity_type is not None:
            clauses.append("identity_type = ?")
            params.append(str(identity_type))
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self._connection.execute(
            "SELECT scope_id, identity_type, identity_key, fingerprint, first_event_id, payload_sha256, created_at "
            f"FROM knowledge_identity_bindings{where} "
            "ORDER BY scope_id, identity_type, identity_key LIMIT ?",
            (*params, limit),
        ).fetchall()
        return tuple({key: str(row[key]) for key in row.keys()} for row in rows)
    def _backfill_counters(self) -> None:
        # Materialize counters for E15 databases created before the E16 schema existed.
        campaigns = self._connection.execute(
            "SELECT DISTINCT campaign_id FROM knowledge_executions"
        ).fetchall()
        for campaign in campaigns:
            campaign_id = str(campaign[0])
            existing = self._connection.execute(
                "SELECT 1 FROM knowledge_campaign_counters WHERE campaign_id = ?",
                (campaign_id,),
            ).fetchone()
            if existing is not None:
                continue
            self._connection.execute(
                "INSERT OR IGNORE INTO knowledge_campaign_mutants(campaign_id, mutant_id) "
                "SELECT DISTINCT campaign_id, mutant_id FROM knowledge_executions WHERE campaign_id = ?",
                (campaign_id,),
            )
            rows = self._connection.execute(
                "SELECT status, COUNT(*) AS count FROM knowledge_executions WHERE campaign_id = ? GROUP BY status",
                (campaign_id,),
            ).fetchall()
            total = self._connection.execute(
                "SELECT COUNT(*), MAX(created_at) FROM knowledge_executions WHERE campaign_id = ?",
                (campaign_id,),
            ).fetchone()
            counts = {str(row["status"]): int(row["count"]) for row in rows}
            self._connection.execute(
                "INSERT INTO knowledge_campaign_counters(campaign_id, executions, mutants, attempts, counts, revision, updated_at) "
                "VALUES (?, ?, ?, ?, ?, 0, ?)",
                (
                    campaign_id,
                    int(total[0] or 0),
                    len(
                        self._connection.execute(
                            "SELECT 1 FROM knowledge_campaign_mutants WHERE campaign_id = ?",
                            (campaign_id,),
                        ).fetchall()
                    ),
                    int(total[0] or 0),
                    _canonical(counts),
                    str(total[1] or utc_now_iso()),
                ),
            )
    def _bump_revision(self) -> int:
        # Advance the projection revision inside the caller's active SQLite transaction.
        self._connection.execute(
            "UPDATE knowledge_meta SET value = CAST(value AS INTEGER) + 1 WHERE key = 'projection_revision'"
        )
        row = self._connection.execute(
            "SELECT value FROM knowledge_meta WHERE key = 'projection_revision'"
        ).fetchone()
        return int(row[0]) if row is not None else 0
    def _current_revision(self) -> int:
        # Read the monotonic projection revision used to make query pages diagnosable.
        row = self._connection.execute(
            "SELECT value FROM knowledge_meta WHERE key = 'projection_revision'"
        ).fetchone()
        return int(row[0]) if row is not None else 0
    def _update_counters(self, campaign_id: str, executions: Sequence[Mapping[str, Any]], updated_at: str) -> None:
        # Update materialized campaign counters only after execution rows are inserted successfully.
        row = self._connection.execute(
            "SELECT executions, mutants, attempts, counts, revision FROM knowledge_campaign_counters WHERE campaign_id = ?",
            (campaign_id,),
        ).fetchone()
        counts = json.loads(str(row["counts"])) if row is not None else {}
        if not isinstance(counts, dict):
            counts = {}
        for execution in executions:
            status = str(execution["status"])
            counts[status] = int(counts.get(status, 0)) + 1
        total = self._connection.execute(
            "SELECT COUNT(*) FROM knowledge_campaign_mutants WHERE campaign_id = ?",
            (campaign_id,),
        ).fetchone()
        previous_revision = int(row["revision"]) if row is not None else 0
        previous_executions = int(row["executions"]) if row is not None else 0
        self._connection.execute(
            "INSERT INTO knowledge_campaign_counters(campaign_id, executions, mutants, attempts, counts, revision, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(campaign_id) DO UPDATE SET executions=excluded.executions, mutants=excluded.mutants, "
            "attempts=excluded.attempts, counts=excluded.counts, revision=excluded.revision, updated_at=excluded.updated_at",
            (
                campaign_id,
                previous_executions + len(executions),
                int(total[0] if total is not None else 0),
                previous_executions + len(executions),
                _canonical(counts),
                previous_revision + 1,
                updated_at,
            ),
        )
    def _bind_execution_identities(
        self,
        *,
        scope_id: str,
        execution: Mapping[str, Any],
        event_id: str,
        created_at: str,
    ) -> str:
        # Bind execution, function, and mutant keys to immutable fingerprints in one scope.
        payload_sha256 = stable_hash(execution["payload"])
        self._bind_identity(
            scope_id=scope_id,
            identity_type="execution",
            identity_key=str(execution["execution_id"]),
            fingerprint=payload_sha256,
            event_id=event_id,
            payload_sha256=payload_sha256,
            created_at=created_at,
        )
        if execution.get("function_id") and execution.get("function_fingerprint"):
            self._bind_identity(
                scope_id=scope_id,
                identity_type="function",
                identity_key=str(execution["function_id"]),
                fingerprint=str(execution["function_fingerprint"]),
                event_id=event_id,
                payload_sha256=payload_sha256,
                created_at=created_at,
            )
        if execution.get("mutant_fingerprint"):
            self._bind_identity(
                scope_id=scope_id,
                identity_type="mutant",
                identity_key=str(execution["mutant_id"]),
                fingerprint=str(execution["mutant_fingerprint"]),
                event_id=event_id,
                payload_sha256=payload_sha256,
                created_at=created_at,
            )
        return payload_sha256
    def _insert_observation(
        self,
        *,
        scope_id: str,
        event_id: str,
        effect_id: str,
        campaign_id: str,
        execution_id: str,
        mutant_id: str,
        observation: Mapping[str, Any],
        created_at: str,
    ) -> bool:
        # Append one test observation or reject a conflicting replay instead of ignoring it.
        payload_json = _canonical(observation["payload"])
        payload_sha256 = stable_hash(observation["payload"])
        observation_id = stable_hash(
            {"event_id": event_id, "test_id": observation["test_id"], "payload": observation["payload"]}
        )[:32]
        existing = self._connection.execute(
            "SELECT observation_id, test_fingerprint, outcome, payload, observation_sha256 "
            "FROM knowledge_test_observations WHERE event_id = ? AND test_id = ?",
            (event_id, observation["test_id"]),
        ).fetchone()
        if existing is not None:
            exact = (
                str(existing["test_fingerprint"]) == str(observation["test_fingerprint"])
                and str(existing["outcome"]) == str(observation["outcome"])
                and str(existing["payload"]) == payload_json
                and str(existing["observation_sha256"] or payload_sha256) == payload_sha256
            )
            if exact:
                return False
            raise KnowledgeConflict(
                "knowledge test observation conflict: "
                f"scope_id={scope_id}; event_id={event_id}; execution_id={execution_id}; "
                f"test_id={observation['test_id']}; existing_observation_id={existing['observation_id']}; "
                f"received_observation_id={observation_id}; existing_fingerprint={existing['test_fingerprint']}; "
                f"received_fingerprint={observation['test_fingerprint']}; existing_outcome={existing['outcome']}; "
                f"received_outcome={observation['outcome']}"
            )
        self._bind_identity(
            scope_id=scope_id,
            identity_type="test",
            identity_key=str(observation["test_id"]),
            fingerprint=str(observation["test_fingerprint"]),
            event_id=event_id,
            payload_sha256=payload_sha256,
            created_at=created_at,
        )
        self._connection.execute(
            "INSERT INTO knowledge_test_observations(observation_id, event_id, effect_id, campaign_id, execution_id, "
            "mutant_id, test_id, test_fingerprint, outcome, payload, created_at, scope_id, observation_sha256) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                observation_id,
                event_id,
                effect_id,
                campaign_id,
                execution_id,
                mutant_id,
                observation["test_id"],
                observation["test_fingerprint"],
                observation["outcome"],
                payload_json,
                created_at,
                scope_id,
                payload_sha256,
            ),
        )
        return True
    def close(self) -> None:
        # Close the projection connection after callers finish replaying committed effects.
        self._connection.close()
    def _existing_conflict_payload(
        self,
        *,
        effect_id: str,
        executions: Sequence[Mapping[str, Any]],
    ) -> Mapping[str, Any] | None:
        # Load the accepted fact currently bound to the conflicting effect or execution identity.
        row = self._connection.execute(
            "SELECT campaign_id, effect_type, payload, control_revision, scope_id FROM knowledge_effects WHERE effect_id = ?",
            (effect_id,),
        ).fetchone()
        if row is not None:
            return {
                "effect_id": effect_id,
                "campaign_id": str(row["campaign_id"]),
                "effect_type": str(row["effect_type"]),
                "control_revision": row["control_revision"],
                "scope_id": row["scope_id"],
                "payload": json.loads(str(row["payload"])),
            }
        for execution in executions:
            execution_id = str(execution.get("execution_id", ""))
            if not execution_id:
                continue
            row = self._connection.execute(
                "SELECT campaign_id, execution_id, mutant_id, attempt, status, payload, scope_id "
                "FROM knowledge_executions WHERE execution_id = ?",
                (execution_id,),
            ).fetchone()
            if row is not None:
                return {key: row[key] for key in row.keys() if key != "payload"} | {
                    "payload": json.loads(str(row["payload"]))
                }
        return None
    def _record_conflict(
        self,
        *,
        conflict_type: str,
        identity_type: str,
        identity_key: str,
        scope_id: str | None,
        incoming_payload: Mapping[str, Any],
        existing_payload: Mapping[str, Any] | None,
        reason: str,
    ) -> str:
        # Persist one conflict after the rejected fact transaction rolls back, without changing accepted history.
        incoming_json = _canonical(incoming_payload)
        existing_json = _canonical(existing_payload) if existing_payload is not None else None
        incoming_sha256 = stable_hash(incoming_payload)
        existing_sha256 = stable_hash(existing_payload) if existing_payload is not None else None
        conflict_id = stable_hash(
            {
                "conflict_type": conflict_type,
                "identity_type": identity_type,
                "identity_key": identity_key,
                "scope_id": scope_id,
                "existing_sha256": existing_sha256,
                "incoming_sha256": incoming_sha256,
                "reason": reason,
            }
        )[:32]
        created_at = _normalize_timestamp(utc_now_iso())
        try:
            self._connection.execute("BEGIN IMMEDIATE")
            cursor = self._connection.execute(
                "INSERT OR IGNORE INTO knowledge_conflict_quarantine(conflict_id, conflict_type, identity_type, "
                "identity_key, scope_id, existing_sha256, incoming_sha256, existing_payload, incoming_payload, "
                "reason, created_at, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'quarantined')",
                (
                    conflict_id,
                    conflict_type,
                    identity_type,
                    identity_key,
                    scope_id,
                    existing_sha256,
                    incoming_sha256,
                    existing_json,
                    incoming_json,
                    reason,
                    created_at,
                ),
            )
            if cursor.rowcount:
                self._bump_revision()
            self._connection.commit()
        except BaseException:
            self._connection.rollback()
            raise
        return conflict_id
    def query_conflicts(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
    ) -> KnowledgePage:
        # Return one newest-first keyset page without exposing quarantined raw payloads.
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        position = decode_cursor(cursor) if cursor else None
        params: list[Any] = []
        where = ""
        if position is not None:
            where = "WHERE created_at < ? OR (created_at = ? AND conflict_id < ?)"
            params.extend((position[0], position[0], position[1]))
        params.append(limit + 1)
        rows = self._connection.execute(
            "SELECT conflict_id, conflict_type, identity_type, identity_key, scope_id, existing_sha256, "
            "incoming_sha256, reason, created_at, status FROM knowledge_conflict_quarantine "
            f"{where} ORDER BY created_at DESC, conflict_id DESC LIMIT ?",
            params,
        ).fetchall()
        has_more = len(rows) > limit
        visible = rows[:limit]
        records = tuple(
            {
                "conflict_id": str(row["conflict_id"]),
                "conflict_type": str(row["conflict_type"]),
                "identity_type": str(row["identity_type"]),
                "identity_key": str(row["identity_key"]),
                "scope_id": str(row["scope_id"]) if row["scope_id"] is not None else None,
                "existing_sha256": str(row["existing_sha256"]) if row["existing_sha256"] is not None else None,
                "incoming_sha256": str(row["incoming_sha256"]),
                "reason": str(row["reason"]),
                "created_at": str(row["created_at"]),
                "status": str(row["status"]),
            }
            for row in visible
        )
        next_cursor = (
            encode_cursor(str(visible[-1]["created_at"]), str(visible[-1]["conflict_id"]))
            if has_more and visible
            else None
        )
        return KnowledgePage(records, next_cursor, self._current_revision())
    def list_conflicts(self, *, limit: int = 100) -> tuple[KnowledgeConflictRecord, ...]:
        # Preserve the original bounded tuple contract over the first quarantine page.
        page = self.query_conflicts(limit=limit)
        return tuple(
            KnowledgeConflictRecord(
                conflict_id=str(row["conflict_id"]),
                conflict_type=str(row["conflict_type"]),
                identity_type=str(row["identity_type"]),
                identity_key=str(row["identity_key"]),
                scope_id=str(row["scope_id"]) if row.get("scope_id") is not None else None,
                existing_sha256=(
                    str(row["existing_sha256"])
                    if row.get("existing_sha256") is not None
                    else None
                ),
                incoming_sha256=str(row["incoming_sha256"]),
                reason=str(row["reason"]),
                created_at=str(row["created_at"]),
                status=str(row["status"]),
            )
            for row in page.rows
        )
    def query_reuse_decisions(
        self,
        campaign_id: str,
        *,
        limit: int = 100,
        after_mutant_id: str | None = None,
    ) -> KnowledgePage:
        # Page the newest frozen reuse plan through SQLite JSON keysets without loading the whole decision ledger.
        if not isinstance(campaign_id, str) or not campaign_id.strip():
            raise ValueError("campaign_id must be non-empty")
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        params: list[Any] = [campaign_id.strip()]
        predicate = ""
        if after_mutant_id is not None:
            if not isinstance(after_mutant_id, str) or not after_mutant_id:
                raise ValueError("after_mutant_id must be non-empty")
            predicate = "AND json_extract(item.value, '$.mutant_id') > ?"
            params.append(after_mutant_id)
        params.append(limit + 1)
        rows = self._connection.execute(
            "WITH latest AS ("
            "SELECT payload FROM knowledge_reuse_plans WHERE campaign_id = ? "
            "ORDER BY created_at DESC, plan_id DESC LIMIT 1"
            ") "
            "SELECT item.value AS payload, json_extract(item.value, '$.mutant_id') AS mutant_id "
            "FROM latest, json_each(latest.payload, '$.decisions') AS item "
            "WHERE json_type(item.value) = 'object' "
            "AND json_extract(item.value, '$.mutant_id') IS NOT NULL "
            "AND json_extract(item.value, '$.mutant_id') != '' "
            f"{predicate} ORDER BY mutant_id ASC LIMIT ?",
            params,
        ).fetchall()
        has_more = len(rows) > limit
        visible = rows[:limit]
        records = tuple(json.loads(str(row["payload"])) for row in visible)
        if any(not isinstance(item, Mapping) for item in records):
            raise KnowledgeConflict("knowledge reuse decision payload is not an object")
        return KnowledgePage(
            tuple(dict(item) for item in records),
            str(visible[-1]["mutant_id"]) if has_more and visible else None,
            self._current_revision(),
        )
    def get_reuse_plan_payload(self, campaign_id: str) -> Mapping[str, Any] | None:
        # Return the newest protected or released reuse plan as detached canonical JSON.
        if not isinstance(campaign_id, str) or not campaign_id.strip():
            raise ValueError("campaign_id must be non-empty")
        row = self._connection.execute(
            "SELECT payload FROM knowledge_reuse_plans WHERE campaign_id = ? "
            "ORDER BY created_at DESC, plan_id DESC LIMIT 1",
            (campaign_id.strip(),),
        ).fetchone()
        if row is None:
            return None
        value = json.loads(str(row["payload"]))
        if not isinstance(value, Mapping):
            raise KnowledgeConflict("knowledge reuse plan payload is not an object")
        return dict(value)
    def protect_reuse_plan(
        self,
        campaign_id: str,
        plan: ReusePlanArtifact,
        *,
        created_at: str | None = None,
    ) -> str:
        # Protect every raw event used by an executable live reuse decision until the campaign releases its plan.
        if not campaign_id.strip():
            raise ValueError("campaign_id must be non-empty")
        payload = plan.to_dict()
        payload_sha256 = stable_hash(payload)
        plan_id = stable_hash({"campaign_id": campaign_id, "plan_fingerprint": plan.plan_fingerprint})[:32]
        timestamp = _normalize_timestamp(created_at or utc_now_iso())
        incomplete = tuple(
            item.mutant_id
            for item in plan.decisions
            if bool(item.authorized) and (not item.source_event_id or not item.source_execution_id)
        )
        if incomplete:
            raise KnowledgeConflict(
                "executable reuse decisions require source event and execution identity: "
                f"campaign_id={campaign_id}; mutants={incomplete}"
            )
        references = tuple(
            (str(item.source_event_id), str(item.source_execution_id or ""))
            for item in plan.decisions
            if bool(item.authorized) and item.source_event_id and item.source_execution_id
        )
        try:
            self._connection.execute("BEGIN IMMEDIATE")
            existing = self._connection.execute(
                "SELECT campaign_id, plan_fingerprint, payload_sha256, released_at FROM knowledge_reuse_plans WHERE plan_id = ?",
                (plan_id,),
            ).fetchone()
            if existing is not None:
                exact = (
                    str(existing["campaign_id"]) == campaign_id
                    and str(existing["plan_fingerprint"]) == plan.plan_fingerprint
                    and str(existing["payload_sha256"]) == payload_sha256
                    and existing["released_at"] is None
                )
                if not exact:
                    self._connection.rollback()
                    raise KnowledgeConflict(
                        "knowledge reuse plan identity conflict: "
                        f"plan_id={plan_id}; campaign_id={campaign_id}; plan_fingerprint={plan.plan_fingerprint}"
                    )
                self._connection.commit()
                return plan_id
            for event_id, execution_id in references:
                source = self._connection.execute(
                    "SELECT execution_id FROM knowledge_executions WHERE event_id = ?",
                    (event_id,),
                ).fetchone()
                if source is None or str(source["execution_id"]) != execution_id:
                    self._connection.rollback()
                    raise KnowledgeConflict(
                        "live reuse plan references unavailable executable evidence: "
                        f"plan_id={plan_id}; event_id={event_id}; execution_id={execution_id}"
                    )
            self._connection.execute(
                "INSERT INTO knowledge_reuse_plans(plan_id, campaign_id, plan_fingerprint, payload_sha256, payload, created_at, released_at) "
                "VALUES (?, ?, ?, ?, ?, ?, NULL)",
                (plan_id, campaign_id, plan.plan_fingerprint, payload_sha256, _canonical(payload), timestamp),
            )
            for event_id, execution_id in references:
                reference_id = stable_hash({"plan_id": plan_id, "event_id": event_id})[:32]
                self._connection.execute(
                    "INSERT INTO knowledge_evidence_references(reference_id, plan_id, event_id, execution_id, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (reference_id, plan_id, event_id, execution_id, timestamp),
                )
            self._connection.commit()
            return plan_id
        except BaseException:
            self._connection.rollback()
            raise
    def release_reuse_plans(self, campaign_id: str, *, released_at: str | None = None) -> int:
        # Release all live evidence references for one terminal campaign without deleting their audit history.
        if not campaign_id.strip():
            raise ValueError("campaign_id must be non-empty")
        timestamp = _normalize_timestamp(released_at or utc_now_iso())
        try:
            self._connection.execute("BEGIN IMMEDIATE")
            cursor = self._connection.execute(
                "UPDATE knowledge_reuse_plans SET released_at = ? WHERE campaign_id = ? AND released_at IS NULL",
                (timestamp, campaign_id),
            )
            self._connection.commit()
            return int(cursor.rowcount)
        except BaseException:
            self._connection.rollback()
            raise
    def active_evidence_references(self, *, campaign_id: str | None = None, limit: int = 1000) -> tuple[dict[str, str], ...]:
        # Return bounded live plan-to-event references used by retention and closure diagnostics.
        if limit < 1 or limit > 10_000:
            raise ValueError("limit must be between 1 and 10000")
        where = " AND p.campaign_id = ?" if campaign_id is not None else ""
        params: tuple[Any, ...] = (campaign_id, limit) if campaign_id is not None else (limit,)
        rows = self._connection.execute(
            "SELECT p.plan_id, p.campaign_id, p.plan_fingerprint, r.event_id, r.execution_id, r.created_at "
            "FROM knowledge_reuse_plans p JOIN knowledge_evidence_references r ON r.plan_id = p.plan_id "
            "WHERE p.released_at IS NULL" + where + " ORDER BY r.created_at, r.reference_id LIMIT ?",
            params,
        ).fetchall()
        return tuple({key: str(row[key]) for key in row.keys()} for row in rows)
    def compaction_records(self, *, limit: int = 100) -> tuple[KnowledgeCompactionRecord, ...]:
        # Return bounded completed compaction receipts without scanning their full manifests.
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        rows = self._connection.execute(
            "SELECT compaction_id, source_revision, cutoff, manifest_sha256, event_count, observation_count, "
            "created_at, committed_at FROM knowledge_compaction_runs ORDER BY committed_at DESC, compaction_id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return tuple(
            KnowledgeCompactionRecord(
                compaction_id=str(row["compaction_id"]),
                source_revision=int(row["source_revision"]),
                cutoff=str(row["cutoff"]),
                manifest_sha256=str(row["manifest_sha256"]),
                event_count=int(row["event_count"]),
                observation_count=int(row["observation_count"]),
                created_at=str(row["created_at"]),
                committed_at=str(row["committed_at"]),
            )
            for row in rows
        )
    def tombstones(self, *, event_id: str | None = None, limit: int = 1000) -> tuple[dict[str, str], ...]:
        # Return bounded compaction tombstones proving which full replacement owns each removed raw event.
        if limit < 1 or limit > 10_000:
            raise ValueError("limit must be between 1 and 10000")
        where = " WHERE source_event_id = ?" if event_id is not None else ""
        params: tuple[Any, ...] = (event_id, limit) if event_id is not None else (limit,)
        rows = self._connection.execute(
            "SELECT tombstone_id, entity_type, entity_id, source_event_id, replacement_id, reason, "
            "payload_sha256, created_at FROM knowledge_tombstones" + where +
            " ORDER BY created_at, tombstone_id LIMIT ?",
            params,
        ).fetchall()
        return tuple({key: str(row[key]) for key in row.keys()} for row in rows)
    def compacted_evidence(self, event_id: str) -> Mapping[str, Any] | None:
        # Load one preserved compaction evidence bundle by immutable source event identity.
        if not event_id.strip():
            raise ValueError("event_id must be non-empty")
        row = self._connection.execute(
            "SELECT evidence_payload, evidence_sha256, graph_fingerprint, observation_count, compaction_id "
            "FROM knowledge_execution_rollups WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        if row is None or row["evidence_payload"] is None:
            return None
        payload = json.loads(str(row["evidence_payload"]))
        if stable_hash(payload) != str(row["evidence_sha256"]):
            raise KnowledgeConflict(f"compacted evidence hash mismatch: event_id={event_id}; database={self.path}")
        return {
            "event_id": event_id,
            "graph_fingerprint": str(row["graph_fingerprint"] or ""),
            "observation_count": int(row["observation_count"] or 0),
            "compaction_id": str(row["compaction_id"] or ""),
            "evidence": payload,
        }
    def _after_compaction_event(self, compaction_id: str, compacted_events: int) -> None:
        # Provide one no-op fault-injection boundary inside the still-uncommitted compaction transaction.
        del compaction_id, compacted_events
    def backup_to(self, destination: str | Path) -> Path:
        # Publish one consistent SQLite snapshot through a validated sibling temporary file and atomic replacement.
        target = Path(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.resolve() == self.path.resolve():
            raise ValueError("backup destination must differ from the live knowledge database")
        fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
        os.close(fd)
        temp_path = Path(temp_name)
        try:
            backup_connection = sqlite3.connect(str(temp_path))
            try:
                self._connection.backup(backup_connection)
                row = backup_connection.execute("PRAGMA quick_check(1)").fetchone()
                if row is None or str(row[0]).lower() != "ok":
                    raise KnowledgeConflict(f"knowledge backup integrity check failed: destination={target}; result={row}")
            finally:
                backup_connection.close()
            with temp_path.open("rb+") as handle:
                os.fsync(handle.fileno())
            os.replace(temp_path, target)
            _fsync_parent(target)
            return target
        finally:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                if sys.exc_info()[0] is None:
                    raise
    @classmethod
    def restore_from(cls, backup: str | Path, destination: str | Path) -> Path:
        # Validate a complete backup before atomically replacing the destination with the restored database.
        source = Path(backup)
        target = Path(destination)
        if not source.is_file():
            raise FileNotFoundError(source)
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".restore", dir=target.parent)
        temp_path = Path(temp_name)
        try:
            with source.open("rb") as reader, os.fdopen(fd, "wb") as writer:
                for chunk in iter(lambda: reader.read(1024 * 1024), b""):
                    writer.write(chunk)
                writer.flush()
                os.fsync(writer.fileno())
            candidate = cls(temp_path)
            try:
                candidate.require_integrity()
            finally:
                candidate.close()
            os.replace(temp_path, target)
            _fsync_parent(target)
            return target
        finally:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass
    def verify_integrity(self, *, max_rows: int = 100_000) -> KnowledgeIntegrityReport:
        # Verify bounded payload hashes, graph links, live references, tombstones, and compaction manifests.
        if max_rows < 1 or max_rows > 1_000_000:
            raise ValueError("max_rows must be between 1 and 1000000")
        quick_row = self._connection.execute("PRAGMA quick_check(1)").fetchone()
        quick_check = str(quick_row[0]) if quick_row is not None else "missing"
        mismatches: list[str] = []
        orphaned: list[str] = []
        checked_rows = 0
        checks = (
            ("knowledge_effects", "effect_id", "payload", "payload_sha256"),
            ("knowledge_executions", "event_id", "payload", "identity_sha256"),
            ("knowledge_test_observations", "observation_id", "payload", "observation_sha256"),
            ("knowledge_evidence_nodes", "node_id", "payload", "payload_sha256"),
            ("knowledge_evidence_edges", "edge_id", "payload", "payload_sha256"),
            ("knowledge_tombstones", "tombstone_id", "payload", "payload_sha256"),
            ("knowledge_reuse_plans", "plan_id", "payload", "payload_sha256"),
            ("knowledge_conflict_quarantine", "conflict_id", "incoming_payload", "incoming_sha256"),
        )
        for table, key_column, payload_column, hash_column in checks:
            rows = self._connection.execute(
                f"SELECT {key_column}, {payload_column}, {hash_column} FROM {table} ORDER BY {key_column} LIMIT ?",
                (max_rows + 1,),
            ).fetchall()
            if len(rows) > max_rows:
                raise ValueError(f"integrity audit row limit exceeded: table={table}; max_rows={max_rows}")
            checked_rows += len(rows)
            for row in rows:
                try:
                    payload = json.loads(str(row[payload_column]))
                    actual = stable_hash(payload)
                except (TypeError, ValueError):
                    actual = "invalid-json"
                expected = str(row[hash_column] or "")
                if actual != expected:
                    mismatches.append(f"{table}:{row[key_column]}:{expected}:{actual}")
        conflict_existing = self._connection.execute(
            "SELECT conflict_id, existing_payload, existing_sha256 FROM knowledge_conflict_quarantine "
            "WHERE existing_payload IS NOT NULL ORDER BY conflict_id LIMIT ?",
            (max_rows + 1,),
        ).fetchall()
        if len(conflict_existing) > max_rows:
            raise ValueError(
                f"integrity audit row limit exceeded: table=knowledge_conflict_quarantine; max_rows={max_rows}"
            )
        checked_rows += len(conflict_existing)
        for row in conflict_existing:
            try:
                payload = json.loads(str(row["existing_payload"]))
                actual = stable_hash(payload)
            except (TypeError, ValueError):
                actual = "invalid-json"
            expected = str(row["existing_sha256"] or "")
            if actual != expected:
                mismatches.append(
                    f"knowledge_conflict_quarantine:{row['conflict_id']}:existing:{expected}:{actual}"
                )
        rollups = self._connection.execute(
            "SELECT event_id, evidence_payload, evidence_sha256, graph_fingerprint, observation_count, compaction_id "
            "FROM knowledge_execution_rollups WHERE evidence_payload IS NOT NULL ORDER BY event_id LIMIT ?",
            (max_rows + 1,),
        ).fetchall()
        if len(rollups) > max_rows:
            raise ValueError(f"integrity audit row limit exceeded: table=knowledge_execution_rollups; max_rows={max_rows}")
        checked_rows += len(rollups)
        rollup_index = {str(row["event_id"]): row for row in rollups}
        for row in rollups:
            try:
                payload = json.loads(str(row["evidence_payload"]))
                actual = stable_hash(payload)
            except (TypeError, ValueError):
                actual = "invalid-json"
            expected = str(row["evidence_sha256"] or "")
            if actual != expected:
                mismatches.append(f"knowledge_execution_rollups:{row['event_id']}:{expected}:{actual}")
        compactions = self._connection.execute(
            "SELECT compaction_id, manifest, manifest_sha256, event_count, observation_count "
            "FROM knowledge_compaction_runs ORDER BY compaction_id LIMIT ?",
            (max_rows + 1,),
        ).fetchall()
        if len(compactions) > max_rows:
            raise ValueError(f"integrity audit row limit exceeded: table=knowledge_compaction_runs; max_rows={max_rows}")
        checked_rows += len(compactions)
        for row in compactions:
            try:
                manifest = json.loads(str(row["manifest"]))
                actual = stable_hash(manifest)
            except (TypeError, ValueError):
                actual = "invalid-json"
            expected = str(row["manifest_sha256"] or "")
            if actual != expected:
                mismatches.append(f"knowledge_compaction_runs:{row['compaction_id']}:{expected}:{actual}")
                continue
            events = manifest.get("events", ()) if isinstance(manifest, Mapping) else ()
            if not isinstance(events, list):
                mismatches.append(f"knowledge_compaction_runs:{row['compaction_id']}:events:not-array")
                continue
            expected_event_count = int(row["event_count"] or 0)
            expected_observation_count = int(row["observation_count"] or 0)
            actual_observation_count = sum(
                int(item.get("observation_count", 0))
                for item in events
                if isinstance(item, Mapping)
            )
            if len(events) != expected_event_count or actual_observation_count != expected_observation_count:
                mismatches.append(
                    f"knowledge_compaction_runs:{row['compaction_id']}:counts:"
                    f"{expected_event_count}/{expected_observation_count}:{len(events)}/{actual_observation_count}"
                )
            for item in events:
                if not isinstance(item, Mapping):
                    mismatches.append(f"knowledge_compaction_runs:{row['compaction_id']}:event:not-object")
                    continue
                event_id = str(item.get("event_id", ""))
                rollup = rollup_index.get(event_id)
                if rollup is None:
                    orphaned.append(f"compaction-event:{row['compaction_id']}:{event_id}")
                    continue
                if (
                    str(rollup["compaction_id"] or "") != str(row["compaction_id"])
                    or str(rollup["evidence_sha256"] or "") != str(item.get("evidence_sha256", ""))
                    or str(rollup["graph_fingerprint"] or "") != str(item.get("graph_fingerprint", ""))
                    or int(rollup["observation_count"] or 0) != int(item.get("observation_count", 0))
                ):
                    mismatches.append(
                        f"knowledge_compaction_runs:{row['compaction_id']}:event:{event_id}:rollup-mismatch"
                    )
        edge_rows = self._connection.execute(
            "SELECT e.edge_id FROM knowledge_evidence_edges e "
            "LEFT JOIN knowledge_evidence_nodes s ON s.node_id = e.source_node_id "
            "LEFT JOIN knowledge_evidence_nodes t ON t.node_id = e.target_node_id "
            "WHERE s.node_id IS NULL OR t.node_id IS NULL ORDER BY e.edge_id LIMIT ?",
            (max_rows + 1,),
        ).fetchall()
        orphaned.extend(f"edge:{row['edge_id']}" for row in edge_rows[:max_rows])
        reference_rows = self._connection.execute(
            "SELECT r.reference_id FROM knowledge_evidence_references r "
            "LEFT JOIN knowledge_executions e ON e.event_id = r.event_id "
            "LEFT JOIN knowledge_execution_rollups c ON c.event_id = r.event_id "
            "WHERE e.event_id IS NULL AND c.event_id IS NULL ORDER BY r.reference_id LIMIT ?",
            (max_rows + 1,),
        ).fetchall()
        orphaned.extend(f"reference:{row['reference_id']}" for row in reference_rows[:max_rows])
        tombstone_rows = self._connection.execute(
            "SELECT t.tombstone_id FROM knowledge_tombstones t "
            "LEFT JOIN knowledge_compaction_runs c ON c.compaction_id = t.replacement_id "
            "WHERE c.compaction_id IS NULL ORDER BY t.tombstone_id LIMIT ?",
            (max_rows + 1,),
        ).fetchall()
        orphaned.extend(f"tombstone:{row['tombstone_id']}" for row in tombstone_rows[:max_rows])
        rollup_orphans = self._connection.execute(
            "SELECT r.event_id FROM knowledge_execution_rollups r "
            "LEFT JOIN knowledge_compaction_runs c ON c.compaction_id = r.compaction_id "
            "WHERE r.compaction_id IS NULL OR c.compaction_id IS NULL ORDER BY r.event_id LIMIT ?",
            (max_rows + 1,),
        ).fetchall()
        orphaned.extend(f"rollup:{row['event_id']}" for row in rollup_orphans[:max_rows])
        required_triggers = {
            "knowledge_effects_append_only_update",
            "knowledge_executions_append_only_update",
            "knowledge_observations_append_only_update",
            "knowledge_evidence_nodes_append_only_update",
            "knowledge_evidence_edges_append_only_update",
            "knowledge_conflict_quarantine_append_only_update",
            "knowledge_compaction_runs_append_only_update",
            "knowledge_tombstones_append_only_update",
            "knowledge_evidence_references_append_only_update",
        }
        actual_triggers = {
            str(row[0])
            for row in self._connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'trigger'"
            ).fetchall()
        }
        orphaned.extend(f"trigger:{name}" for name in sorted(required_triggers - actual_triggers))
        healthy = quick_check.lower() == "ok" and not mismatches and not orphaned
        return KnowledgeIntegrityReport(healthy, quick_check, checked_rows, tuple(mismatches), tuple(orphaned))
    def closure_audit(self, *, max_rows: int = 100_000) -> dict[str, Any]:
        # Combine E15 integrity with bounded E16 invalidation, incident, quarantine, and reference invariants.
        if max_rows < 1:
            raise ValueError("max_rows must be positive")
        state = self.schema_state()
        integrity = self.verify_integrity(max_rows=max_rows)
        conflicts = self.list_conflicts(limit=1000)
        references = self.active_evidence_references(limit=10_000)
        compactions = self.compaction_records(limit=100)
        e15_closed = state.version == state.supported_version == KNOWLEDGE_SCHEMA_VERSION and integrity.healthy
        violations: list[str] = []
        invalidations = self._connection.execute(
            "SELECT invalidation_id, scope_type, scope_key, reason FROM knowledge_invalidations "
            "ORDER BY created_at, invalidation_id LIMIT ?",
            (max_rows + 1,),
        ).fetchall()
        incidents = self._connection.execute(
            "SELECT incident_id, mismatches, expected_payload, actual_payload, status FROM knowledge_reuse_incidents "
            "ORDER BY created_at, incident_id LIMIT ?",
            (max_rows + 1,),
        ).fetchall()
        quarantines = self._connection.execute(
            "SELECT q.scope_type, q.scope_key, q.incident_id, q.reason, q.released_at, "
            "i.incident_id AS existing_incident_id FROM knowledge_reuse_quarantine q "
            "LEFT JOIN knowledge_reuse_incidents i ON i.incident_id = q.incident_id "
            "ORDER BY q.created_at, q.scope_type, q.scope_key LIMIT ?",
            (max_rows + 1,),
        ).fetchall()
        if len(invalidations) > max_rows:
            violations.append("invalidation-audit-limit-exceeded")
        if len(incidents) > max_rows:
            violations.append("incident-audit-limit-exceeded")
        if len(quarantines) > max_rows:
            violations.append("quarantine-audit-limit-exceeded")
        for row in invalidations[:max_rows]:
            if str(row["scope_type"]) not in self._INVALIDATION_SCOPES:
                violations.append(f"invalidation-scope:{row['invalidation_id']}")
            if not str(row["scope_key"]).strip() or not str(row["reason"]).strip():
                violations.append(f"invalidation-payload:{row['invalidation_id']}")
        for row in incidents[:max_rows]:
            incident_id = str(row["incident_id"])
            try:
                mismatches = json.loads(str(row["mismatches"]))
                expected = json.loads(str(row["expected_payload"]))
                actual = json.loads(str(row["actual_payload"]))
            except (TypeError, ValueError, json.JSONDecodeError):
                violations.append(f"incident-json:{incident_id}")
                continue
            if not isinstance(mismatches, list) or not mismatches or not all(str(item).strip() for item in mismatches):
                violations.append(f"incident-mismatches:{incident_id}")
            if not isinstance(expected, Mapping) or not isinstance(actual, Mapping) or not str(row["status"]).strip():
                violations.append(f"incident-payload:{incident_id}")
        active_quarantines = 0
        for row in quarantines[:max_rows]:
            scope_type = str(row["scope_type"])
            scope_key = str(row["scope_key"])
            if not scope_type.strip() or not scope_key.strip() or not str(row["reason"]).strip():
                violations.append(f"quarantine-payload:{scope_type}:{scope_key}")
            if row["existing_incident_id"] is None:
                violations.append(f"quarantine-incident:{scope_type}:{scope_key}")
            if row["released_at"] is None:
                active_quarantines += 1
        return {
            "e15_closed": e15_closed,
            "e16_closed": e15_closed and not violations,
            "schema": state.to_dict(),
            "integrity": integrity.to_dict(),
            "quarantined_conflicts": [item.to_dict() for item in conflicts],
            "active_evidence_references": list(references),
            "compactions": [item.to_dict() for item in compactions],
            "reuse_invariants": {
                "invalidation_count": min(len(invalidations), max_rows),
                "incident_count": min(len(incidents), max_rows),
                "quarantine_count": min(len(quarantines), max_rows),
                "active_quarantine_count": active_quarantines,
                "violations": sorted(dict.fromkeys(violations)),
            },
        }
    def require_integrity(self, *, max_rows: int = 100_000) -> KnowledgeIntegrityReport:
        # Fail closed when a bounded integrity audit finds SQLite, payload, graph, or compaction corruption.
        report = self.verify_integrity(max_rows=max_rows)
        if not report.healthy:
            raise KnowledgeConflict(
                "knowledge integrity check failed: "
                f"database={self.path}; quick_check={report.quick_check}; "
                f"hash_mismatches={report.hash_mismatches}; orphaned_references={report.orphaned_references}"
            )
        return report
    def ingest_effect(
        self,
        *,
        effect_id: str,
        campaign_id: str,
        effect_type: str,
        payload: Mapping[str, Any],
        control_revision: int | None = None,
        observed_at: str | None = None,
        allow_compatible_duplicate: bool = False,
        project_id: str | None = None,
        revision_id: str | None = None,
        environment_id: str | None = None,
    ) -> KnowledgeIngestResult:
        # Append one committed effect and all canonical identity bindings in one projection transaction.
        if not all(isinstance(item, str) and item.strip() for item in (effect_id, campaign_id, effect_type)):
            raise ValueError("effect_id, campaign_id and effect_type must be non-empty strings")
        if control_revision is not None and int(control_revision) < 0:
            raise ValueError("control_revision must not be negative")
        normalized_payload = dict(payload)
        payload_json = _canonical(normalized_payload)
        payload_hash = stable_hash(normalized_payload)
        raw_executions = normalized_payload.get("executions", [])
        if not isinstance(raw_executions, (list, tuple)):
            raise ValueError("committed effect executions must be an array")
        normalized_executions = [_normalize_execution(value) for value in raw_executions if isinstance(value, Mapping)]
        if len(normalized_executions) != len(raw_executions):
            raise ValueError("committed execution must be an object")
        scope = KnowledgeScopeIdentity.resolve(
            campaign_id=campaign_id,
            payload=normalized_payload,
            project_id=project_id,
            revision_id=revision_id,
            environment_id=environment_id,
        )
        created_at = _normalize_timestamp(observed_at or utc_now_iso())
        ingested_at = _normalize_timestamp(utc_now_iso())
        try:
            self._connection.execute("BEGIN IMMEDIATE")
            existing = self._connection.execute(
                "SELECT campaign_id, effect_type, payload_sha256, payload, control_revision, scope_id, "
                "project_id, revision_id, environment_id FROM knowledge_effects WHERE effect_id = ?",
                (effect_id,),
            ).fetchone()
            if existing is not None:
                same_payload = str(existing["payload_sha256"]) == payload_hash
                compatible_payload = allow_compatible_duplicate and _payload_contains(
                    json.loads(str(existing["payload"])), normalized_payload
                )
                existing_scope_id = str(existing["scope_id"] or "")
                weaker_compatible_scope = (
                    compatible_payload
                    and str(existing["project_id"] or "") == scope.project_id
                    and str(existing["revision_id"] or "") == scope.revision_id
                    and scope.environment_id == "legacy-environment:unknown"
                )
                metadata_conflicts = {
                    "campaign_id": (str(existing["campaign_id"]), campaign_id),
                    "effect_type": (str(existing["effect_type"]), effect_type),
                    "scope_id": (existing_scope_id, scope.scope_id),
                    "project_id": (str(existing["project_id"] or ""), scope.project_id),
                    "revision_id": (str(existing["revision_id"] or ""), scope.revision_id),
                    "environment_id": (str(existing["environment_id"] or ""), scope.environment_id),
                }
                conflicts = {key: values for key, values in metadata_conflicts.items() if values[0] != values[1]}
                if weaker_compatible_scope:
                    conflicts.pop("scope_id", None)
                    conflicts.pop("environment_id", None)
                existing_revision = existing["control_revision"]
                if existing_revision is not None and control_revision is not None and int(existing_revision) != int(control_revision):
                    conflicts["control_revision"] = (int(existing_revision), int(control_revision))
                if conflicts or not (same_payload or compatible_payload):
                    self._connection.rollback()
                    raise KnowledgeConflict(
                        "knowledge effect identity conflict: "
                        f"effect_id={effect_id}; conflicts={conflicts}; "
                        f"existing_payload_sha256={existing['payload_sha256']}; "
                        f"received_payload_sha256={payload_hash}; scope_id={scope.scope_id}"
                    )
                self._connection.commit()
                return KnowledgeIngestResult(
                    effect_id, 0, duplicate=True, scope_id=existing_scope_id
                )
            self._insert_scope(scope, created_at=created_at)
            duplicate_execution_ids: set[str] = set()
            new_executions: list[dict[str, Any]] = []
            for execution in normalized_executions:
                execution_id_value = str(execution["execution_id"])
                if execution_id_value in duplicate_execution_ids or any(
                    str(item["execution_id"]) == execution_id_value for item in new_executions
                ):
                    self._connection.rollback()
                    raise KnowledgeConflict(
                        "knowledge execution identity is duplicated in effect: "
                        f"effect_id={effect_id}; execution_id={execution_id_value}; scope_id={scope.scope_id}"
                    )
                existing_execution = self._connection.execute(
                    "SELECT campaign_id, execution_id, mutant_id, attempt, status, payload, scope_id, identity_sha256 "
                    "FROM knowledge_executions WHERE execution_id = ?",
                    (execution_id_value,),
                ).fetchone()
                if existing_execution is None:
                    new_executions.append(execution)
                    continue
                existing_identity = _execution_identity_payload(
                    campaign_id=str(existing_execution["campaign_id"]),
                    execution_id=str(existing_execution["execution_id"]),
                    mutant_id=str(existing_execution["mutant_id"]),
                    attempt=int(existing_execution["attempt"]),
                    status=str(existing_execution["status"]),
                    payload=json.loads(str(existing_execution["payload"])),
                )
                candidate_identity = _execution_identity_payload(
                    campaign_id=campaign_id,
                    execution_id=execution_id_value,
                    mutant_id=str(execution["mutant_id"]),
                    attempt=int(execution["attempt"]),
                    status=str(execution["status"]),
                    payload=execution["payload"],
                )
                if (
                    existing_identity != candidate_identity
                    or str(existing_execution["scope_id"] or "") != scope.scope_id
                    or str(existing_execution["identity_sha256"] or stable_hash(execution["payload"]))
                    != stable_hash(execution["payload"])
                ):
                    self._connection.rollback()
                    raise KnowledgeConflict(
                        "knowledge execution identity conflict: "
                        f"execution_id={execution_id_value}; scope_id={scope.scope_id}; "
                        f"existing_scope_id={existing_execution['scope_id']}; campaign_id={campaign_id}; "
                        f"mutant_id={execution['mutant_id']}; attempt={execution['attempt']}"
                    )
                duplicate_execution_ids.add(execution_id_value)
            self._connection.execute(
                "INSERT INTO knowledge_effects(effect_id, campaign_id, effect_type, payload_sha256, payload, control_revision, "
                "created_at, scope_id, project_id, revision_id, environment_id, identity_source, observed_at, ingested_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    effect_id,
                    campaign_id,
                    effect_type,
                    payload_hash,
                    payload_json,
                    control_revision,
                    created_at,
                    scope.scope_id,
                    scope.project_id,
                    scope.revision_id,
                    scope.environment_id,
                    scope.identity_source,
                    created_at,
                    ingested_at,
                ),
            )
            inserted_observations = 0
            for execution in new_executions:
                event_id = stable_hash(
                    {"effect_id": effect_id, "execution_id": execution["execution_id"], "payload": execution["payload"]}
                )[:32]
                identity_sha256 = self._bind_execution_identities(
                    scope_id=scope.scope_id, execution=execution, event_id=event_id, created_at=created_at
                )
                self._connection.execute(
                    "INSERT INTO knowledge_executions(event_id, effect_id, campaign_id, execution_id, mutant_id, attempt, "
                    "status, payload, created_at, scope_id, identity_sha256) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        event_id,
                        effect_id,
                        campaign_id,
                        execution["execution_id"],
                        execution["mutant_id"],
                        execution["attempt"],
                        execution["status"],
                        _canonical(execution["payload"]),
                        created_at,
                        scope.scope_id,
                        identity_sha256,
                    ),
                )
                self._connection.execute(
                    "INSERT INTO knowledge_execution_fingerprints(event_id, effect_id, campaign_id, execution_id, mutant_id, "
                    "function_id, function_fingerprint, mutant_fingerprint, test_fingerprint, conftest_fingerprint, environment_fingerprint, "
                    "selection_fingerprint, result_fingerprint, source_kind, evidence_quality, evidence_schema_version, retention_class, "
                    "knowledge_revision, created_at, scope_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        event_id,
                        effect_id,
                        campaign_id,
                        execution["execution_id"],
                        execution["mutant_id"],
                        execution["function_id"],
                        execution["function_fingerprint"],
                        execution["mutant_fingerprint"],
                        execution["test_fingerprint"],
                        execution["conftest_fingerprint"],
                        execution["environment_fingerprint"],
                        execution["selection_fingerprint"],
                        execution["result_fingerprint"],
                        execution["source_kind"],
                        execution["evidence_quality"],
                        execution["evidence_schema_version"],
                        execution["retention_class"],
                        execution["knowledge_revision"],
                        created_at,
                        scope.scope_id,
                    ),
                )
                self._connection.execute(
                    "INSERT OR IGNORE INTO knowledge_campaign_mutants(campaign_id, mutant_id) VALUES (?, ?)",
                    (campaign_id, execution["mutant_id"]),
                )
                for observation in execution["test_observations"]:
                    if self._insert_observation(
                        scope_id=scope.scope_id,
                        event_id=event_id,
                        effect_id=effect_id,
                        campaign_id=campaign_id,
                        execution_id=str(execution["execution_id"]),
                        mutant_id=str(execution["mutant_id"]),
                        observation=observation,
                        created_at=created_at,
                    ):
                        inserted_observations += 1
                self._project_execution_evidence(
                    scope_id=scope.scope_id,
                    project_id=scope.project_id,
                    revision_id=scope.revision_id,
                    environment_id=scope.environment_id,
                    effect_id=effect_id,
                    effect_type=effect_type,
                    campaign_id=campaign_id,
                    event_id=event_id,
                    execution_payload=execution["payload"],
                    effect_payload=normalized_payload,
                    created_at=created_at,
                )
            if new_executions:
                self._update_counters(campaign_id, new_executions, created_at)
            self._bump_revision()
            self._connection.commit()
            return KnowledgeIngestResult(
                effect_id,
                len(new_executions),
                inserted_observations=inserted_observations,
                scope_id=scope.scope_id,
            )
        except KnowledgeConflict as exc:
            self._connection.rollback()
            try:
                existing_payload = self._existing_conflict_payload(
                    effect_id=effect_id,
                    executions=normalized_executions,
                )
                self._record_conflict(
                    conflict_type="fact_ingest",
                    identity_type="effect",
                    identity_key=effect_id,
                    scope_id=scope.scope_id,
                    incoming_payload={
                        "effect_id": effect_id,
                        "campaign_id": campaign_id,
                        "effect_type": effect_type,
                        "control_revision": control_revision,
                        "scope": scope.to_dict(),
                        "payload": normalized_payload,
                    },
                    existing_payload=existing_payload,
                    reason=str(exc),
                )
            except BaseException as quarantine_error:
                raise KnowledgeConflict(
                    f"{exc}; conflict quarantine failed: {quarantine_error}"
                ) from exc
            raise
        except sqlite3.IntegrityError as exc:
            self._connection.rollback()
            conflict = KnowledgeConflict(
                f"knowledge append-only constraint failed: effect_id={effect_id}; scope_id={scope.scope_id}; error={exc}"
            )
            try:
                self._record_conflict(
                    conflict_type="append_only_constraint",
                    identity_type="effect",
                    identity_key=effect_id,
                    scope_id=scope.scope_id,
                    incoming_payload={
                        "effect_id": effect_id,
                        "campaign_id": campaign_id,
                        "effect_type": effect_type,
                        "control_revision": control_revision,
                        "scope": scope.to_dict(),
                        "payload": normalized_payload,
                    },
                    existing_payload=self._existing_conflict_payload(
                        effect_id=effect_id,
                        executions=normalized_executions,
                    ),
                    reason=str(conflict),
                )
            except BaseException as quarantine_error:
                raise KnowledgeConflict(
                    f"{conflict}; conflict quarantine failed: {quarantine_error}"
                ) from exc
            raise conflict from exc
        except BaseException:
            self._connection.rollback()
            raise
    def ingest_outbox(
        self,
        rows: Iterable[Mapping[str, Any]],
        *,
        namespace_effects: bool = False,
    ) -> tuple[KnowledgeIngestResult, ...]:
        # Replay durable Gallifrey outbox rows idempotently while preserving their original timestamps.
        results: list[KnowledgeIngestResult] = []
        for row in rows:
            effect_id = str(row.get("effect_id", ""))
            campaign_id = str(row.get("campaign_id", ""))
            if namespace_effects:
                effect_id = f"{campaign_id}:{effect_id}"
            payload_value = row.get("payload", {})
            if isinstance(payload_value, str):
                payload_value = json.loads(payload_value)
            if not isinstance(payload_value, Mapping):
                raise ValueError("outbox payload must be an object")
            nested_payload = payload_value.get("payload", payload_value)
            results.append(
                self.ingest_effect(
                    effect_id=effect_id,
                    campaign_id=campaign_id,
                    effect_type=str(row.get("event_type", "")),
                    payload=nested_payload if isinstance(nested_payload, Mapping) else {},
                    control_revision=None,
                    observed_at=str(row.get("created_at")) if row.get("created_at") else None,
                    allow_compatible_duplicate=namespace_effects,
                )
            )
        return tuple(results)
    def enrich_effect(
        self,
        *,
        effect_id: str,
        campaign_id: str,
        payload: Mapping[str, Any],
        observed_at: str | None = None,
    ) -> KnowledgeIngestResult:
        # Append late fingerprints and observations without changing committed effect or execution facts.
        normalized_payload = dict(payload)
        raw_executions = normalized_payload.get("executions", [])
        if not isinstance(raw_executions, (list, tuple)):
            raise ValueError("committed effect executions must be an array")
        normalized_executions = [_normalize_execution(value) for value in raw_executions]
        scope_id: str | None = None
        try:
            self._connection.execute("BEGIN IMMEDIATE")
            existing_effect = self._connection.execute(
                "SELECT payload, effect_type, scope_id, project_id, revision_id, environment_id, created_at FROM knowledge_effects "
                "WHERE effect_id = ? AND campaign_id = ?",
                (effect_id, campaign_id),
            ).fetchone()
            if existing_effect is None:
                self._connection.rollback()
                return self.ingest_effect(
                    effect_id=effect_id,
                    campaign_id=campaign_id,
                    effect_type="mutation.execute_shard",
                    payload=normalized_payload,
                    observed_at=observed_at,
                )
            if not _payload_contains(normalized_payload, json.loads(str(existing_effect["payload"]))):
                self._connection.rollback()
                raise KnowledgeConflict(
                    "knowledge effect enrichment conflict: "
                    f"effect_id={effect_id}; campaign_id={campaign_id}; scope_id={existing_effect['scope_id']}"
                )
            candidate_scope = KnowledgeScopeIdentity.resolve(
                campaign_id=campaign_id,
                payload=normalized_payload,
            )
            existing_scope_components = (
                str(existing_effect["project_id"] or ""),
                str(existing_effect["revision_id"] or ""),
                str(existing_effect["environment_id"] or ""),
            )
            candidate_components = (
                candidate_scope.project_id,
                candidate_scope.revision_id,
                candidate_scope.environment_id,
            )
            if candidate_scope.identity_source != "legacy" and existing_scope_components != candidate_components:
                self._connection.rollback()
                raise KnowledgeConflict(
                    "knowledge enrichment scope conflict: "
                    f"effect_id={effect_id}; existing={existing_scope_components}; received={candidate_components}"
                )
            scope_id = str(existing_effect["scope_id"] or "")
            if not scope_id:
                self._connection.rollback()
                raise KnowledgeConflict(f"knowledge effect enrichment has no canonical scope: effect_id={effect_id}")
            created_at = str(existing_effect["created_at"] or _normalize_timestamp(observed_at or utc_now_iso()))
            inserted_observations = 0
            changed = False
            for execution in normalized_executions:
                row = self._connection.execute(
                    "SELECT event_id, payload, scope_id FROM knowledge_executions "
                    "WHERE effect_id = ? AND execution_id = ?",
                    (effect_id, execution["execution_id"]),
                ).fetchone()
                if row is None:
                    self._connection.rollback()
                    raise KnowledgeConflict(
                        "knowledge execution enrichment is missing base row: "
                        f"effect_id={effect_id}; execution_id={execution['execution_id']}; scope_id={scope_id}"
                    )
                if str(row["scope_id"] or "") != scope_id:
                    self._connection.rollback()
                    raise KnowledgeConflict(
                        "knowledge execution enrichment crossed scopes: "
                        f"effect_id={effect_id}; execution_id={execution['execution_id']}; "
                        f"effect_scope_id={scope_id}; execution_scope_id={row['scope_id']}"
                    )
                event_id = str(row["event_id"])
                try:
                    base_execution_payload = json.loads(str(row["payload"]))
                except (TypeError, ValueError):
                    base_execution_payload = {}
                current = self._connection.execute(
                    "SELECT function_id, function_fingerprint, mutant_fingerprint, test_fingerprint, "
                    "conftest_fingerprint, environment_fingerprint, selection_fingerprint, result_fingerprint, "
                    "source_kind, evidence_quality, evidence_schema_version, retention_class, knowledge_revision, scope_id "
                    "FROM knowledge_execution_fingerprints WHERE event_id = ?",
                    (event_id,),
                ).fetchone()
                if current is None:
                    self._connection.execute(
                        "INSERT INTO knowledge_execution_fingerprints(event_id, effect_id, campaign_id, execution_id, mutant_id, "
                        "function_id, function_fingerprint, mutant_fingerprint, test_fingerprint, conftest_fingerprint, "
                        "environment_fingerprint, selection_fingerprint, result_fingerprint, source_kind, evidence_quality, "
                        "evidence_schema_version, retention_class, knowledge_revision, created_at, scope_id) "
                        "SELECT ?, effect_id, campaign_id, execution_id, mutant_id, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, created_at, scope_id "
                        "FROM knowledge_executions WHERE event_id = ?",
                        (
                            event_id,
                            execution["function_id"],
                            execution["function_fingerprint"],
                            execution["mutant_fingerprint"],
                            execution["test_fingerprint"],
                            execution["conftest_fingerprint"],
                            execution["environment_fingerprint"],
                            execution["selection_fingerprint"],
                            execution["result_fingerprint"],
                            execution["source_kind"],
                            execution["evidence_quality"],
                            execution["evidence_schema_version"],
                            execution["retention_class"],
                            execution["knowledge_revision"],
                            event_id,
                        ),
                    )
                    changed = True
                else:
                    columns = (
                        "function_id",
                        "function_fingerprint",
                        "mutant_fingerprint",
                        "test_fingerprint",
                        "conftest_fingerprint",
                        "environment_fingerprint",
                        "selection_fingerprint",
                        "result_fingerprint",
                        "evidence_quality",
                        "evidence_schema_version",
                    )
                    updates: dict[str, Any] = {}
                    for column in columns:
                        new_value = execution[column]
                        old_value = current[column]
                        if old_value is not None and new_value is not None and str(old_value) != str(new_value):
                            explicit_base_value = (
                                _nested_value(base_execution_payload, column)
                                if isinstance(base_execution_payload, Mapping)
                                else None
                            )
                            if explicit_base_value is None:
                                updates[column] = new_value
                                continue
                            self._connection.rollback()
                            raise KnowledgeConflict(
                                "knowledge fingerprint enrichment conflict: "
                                f"effect_id={effect_id}; execution_id={execution['execution_id']}; scope_id={scope_id}; "
                                f"column={column}; existing={old_value}; received={new_value}; base_value={explicit_base_value}"
                            )
                        if old_value is None and new_value is not None:
                            updates[column] = new_value
                    if updates:
                        self._connection.execute(
                            "UPDATE knowledge_execution_fingerprints SET "
                            + ", ".join(f"{column} = ?" for column in updates)
                            + " WHERE event_id = ?",
                            (*updates.values(), event_id),
                        )
                        changed = True
                if execution.get("function_id") and execution.get("function_fingerprint"):
                    self._bind_identity(
                        scope_id=scope_id,
                        identity_type="function",
                        identity_key=str(execution["function_id"]),
                        fingerprint=str(execution["function_fingerprint"]),
                        event_id=event_id,
                        payload_sha256=stable_hash(execution["payload"]),
                        created_at=created_at,
                    )
                if execution.get("mutant_fingerprint"):
                    self._bind_identity(
                        scope_id=scope_id,
                        identity_type="mutant",
                        identity_key=str(execution["mutant_id"]),
                        fingerprint=str(execution["mutant_fingerprint"]),
                        event_id=event_id,
                        payload_sha256=stable_hash(execution["payload"]),
                        created_at=created_at,
                    )
                for observation in execution["test_observations"]:
                    if self._insert_observation(
                        scope_id=scope_id,
                        event_id=event_id,
                        effect_id=effect_id,
                        campaign_id=campaign_id,
                        execution_id=str(execution["execution_id"]),
                        mutant_id=str(execution["mutant_id"]),
                        observation=observation,
                        created_at=created_at,
                    ):
                        inserted_observations += 1
                        changed = True
                graph_changes_before = self._connection.total_changes
                self._project_execution_evidence(
                    scope_id=scope_id,
                    project_id=str(existing_effect["project_id"] or ""),
                    revision_id=str(existing_effect["revision_id"] or ""),
                    environment_id=str(existing_effect["environment_id"] or ""),
                    effect_id=effect_id,
                    effect_type=str(existing_effect["effect_type"] or "mutation.execute_shard"),
                    campaign_id=campaign_id,
                    event_id=event_id,
                    execution_payload=execution["payload"],
                    effect_payload=normalized_payload,
                    created_at=created_at,
                )
                if self._connection.total_changes > graph_changes_before:
                    changed = True
            if changed:
                self._bump_revision()
            self._connection.commit()
            return KnowledgeIngestResult(
                effect_id,
                0,
                duplicate=True,
                inserted_observations=inserted_observations,
                scope_id=scope_id,
            )
        except KnowledgeConflict as exc:
            self._connection.rollback()
            try:
                self._record_conflict(
                    conflict_type="fact_enrichment",
                    identity_type="effect",
                    identity_key=effect_id,
                    scope_id=scope_id,
                    incoming_payload={
                        "effect_id": effect_id,
                        "campaign_id": campaign_id,
                        "payload": normalized_payload,
                    },
                    existing_payload=self._existing_conflict_payload(
                        effect_id=effect_id,
                        executions=normalized_executions,
                    ),
                    reason=str(exc),
                )
            except BaseException as quarantine_error:
                raise KnowledgeConflict(
                    f"{exc}; conflict quarantine failed: {quarantine_error}"
                ) from exc
            raise
        except sqlite3.IntegrityError as exc:
            self._connection.rollback()
            conflict = KnowledgeConflict(
                f"knowledge enrichment append-only constraint failed: effect_id={effect_id}; error={exc}"
            )
            try:
                self._record_conflict(
                    conflict_type="enrichment_constraint",
                    identity_type="effect",
                    identity_key=effect_id,
                    scope_id=scope_id,
                    incoming_payload={
                        "effect_id": effect_id,
                        "campaign_id": campaign_id,
                        "payload": normalized_payload,
                    },
                    existing_payload=self._existing_conflict_payload(
                        effect_id=effect_id,
                        executions=normalized_executions,
                    ),
                    reason=str(conflict),
                )
            except BaseException as quarantine_error:
                raise KnowledgeConflict(
                    f"{conflict}; conflict quarantine failed: {quarantine_error}"
                ) from exc
            raise conflict from exc
        except BaseException:
            self._connection.rollback()
            raise
    def summarize_campaign(self, campaign_id: str) -> KnowledgeSummary:
        # Read materialized campaign counters without replaying raw or compacted payloads.
        row = self._connection.execute(
            "SELECT executions, mutants, attempts, counts, revision FROM knowledge_campaign_counters WHERE campaign_id = ?",
            (campaign_id,),
        ).fetchone()
        if row is None:
            return KnowledgeSummary(campaign_id, 0, 0, 0, {}, self._current_revision())
        counts = json.loads(str(row["counts"]))
        return KnowledgeSummary(
            campaign_id,
            int(row["executions"]),
            int(row["mutants"]),
            int(row["attempts"]),
            counts if isinstance(counts, Mapping) else {},
            int(row["revision"]),
        )
    def _query_sql(
        self,
        *,
        include_compacted: bool,
        include_payload: bool,
        campaign_id: str | None,
        project_id: str | None,
        revision_id: str | None,
        environment_id: str | None,
        mutant_id: str | None,
        statuses: Sequence[str] | None,
        min_attempt: int | None,
        max_attempt: int | None,
        source_kind: str | None,
        result_fingerprint: str | None,
        cursor: tuple[str, str] | None,
        limit: int,
    ) -> tuple[str, list[Any]]:
        # Build a fixed-shape bounded query over raw evidence and optional historical rollups.
        payload_column = "items.payload" if include_payload else "NULL"
        raw_payload = "e.payload" if include_payload else "NULL"
        raw = (
            "SELECT e.event_id, e.effect_id, e.campaign_id, e.execution_id, e.mutant_id, e.attempt, e.status, "
            f"{raw_payload} AS payload, e.created_at, 0 AS compacted, f.function_id, f.function_fingerprint, "
            "f.mutant_fingerprint, f.test_fingerprint, f.conftest_fingerprint, f.environment_fingerprint, f.selection_fingerprint, "
            "f.result_fingerprint, f.source_kind, f.evidence_quality, f.evidence_schema_version, f.retention_class, "
            "e.scope_id, k.project_id, k.revision_id, k.environment_id, e.identity_sha256 FROM knowledge_executions e "
            "LEFT JOIN knowledge_execution_fingerprints f ON f.event_id = e.event_id "
            "JOIN knowledge_effects k ON k.effect_id = e.effect_id"
        )
        if include_compacted:
            rollup = (
                "SELECT r.event_id, r.effect_id, r.campaign_id, r.execution_id, r.mutant_id, r.attempt, r.status, "
                "NULL AS payload, r.created_at, 1 AS compacted, r.function_id, r.function_fingerprint, "
                "r.mutant_fingerprint, r.test_fingerprint, r.conftest_fingerprint, r.environment_fingerprint, r.selection_fingerprint, "
                "r.result_fingerprint, r.source_kind, r.evidence_quality, r.evidence_schema_version, r.retention_class, "
                "r.scope_id, r.project_id, r.revision_id, r.environment_id, r.identity_sha256 FROM knowledge_execution_rollups r"
            )
            source = f"({raw} UNION ALL {rollup}) AS items"
        else:
            source = f"({raw}) AS items"
        clauses: list[str] = []
        params: list[Any] = []
        if campaign_id is not None:
            clauses.append("items.campaign_id = ?")
            params.append(campaign_id)
        if project_id is not None:
            clauses.append("items.project_id = ?")
            params.append(project_id)
        if revision_id is not None:
            clauses.append("items.revision_id = ?")
            params.append(revision_id)
        if environment_id is not None:
            clauses.append("items.environment_id = ?")
            params.append(environment_id)
        if mutant_id is not None:
            clauses.append("items.mutant_id = ?")
            params.append(mutant_id)
        if statuses:
            placeholders = ", ".join("?" for _ in statuses)
            clauses.append(f"items.status IN ({placeholders})")
            params.extend(statuses)
        if min_attempt is not None:
            clauses.append("items.attempt >= ?")
            params.append(min_attempt)
        if max_attempt is not None:
            clauses.append("items.attempt <= ?")
            params.append(max_attempt)
        if source_kind is not None:
            clauses.append("items.source_kind = ?")
            params.append(source_kind)
        if result_fingerprint is not None:
            clauses.append("items.result_fingerprint = ?")
            params.append(result_fingerprint)
        if cursor is not None:
            clauses.append("(items.created_at > ? OR (items.created_at = ? AND items.event_id > ?))")
            params.extend((cursor[0], cursor[0], cursor[1]))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        return (
            f"SELECT items.event_id, items.effect_id, items.campaign_id, items.execution_id, items.mutant_id, "
            f"items.attempt, items.status, {payload_column} AS payload, items.created_at, items.compacted, "
            "items.function_id, items.function_fingerprint, items.mutant_fingerprint, items.test_fingerprint, "
            "items.conftest_fingerprint, items.environment_fingerprint, items.selection_fingerprint, items.result_fingerprint, items.source_kind, "
            "items.evidence_quality, items.evidence_schema_version, items.retention_class, items.scope_id, "
            "items.project_id, items.revision_id, items.environment_id, items.identity_sha256 "
            f"FROM {source}{where} ORDER BY items.created_at, items.event_id LIMIT ?",
            [*params, limit + 1],
        )
    @staticmethod
    def _row_to_dict(row: sqlite3.Row, *, include_payload: bool) -> dict[str, Any]:
        # Convert one SQL row into a portable query record with optional raw payload.
        value: dict[str, Any] = {
            "event_id": str(row["event_id"]),
            "effect_id": str(row["effect_id"]),
            "campaign_id": str(row["campaign_id"]),
            "execution_id": str(row["execution_id"]),
            "mutant_id": str(row["mutant_id"]),
            "attempt": int(row["attempt"]),
            "status": str(row["status"]),
            "created_at": str(row["created_at"]),
            "compacted": bool(row["compacted"]),
            "function_id": row["function_id"],
            "function_fingerprint": row["function_fingerprint"],
            "mutant_fingerprint": row["mutant_fingerprint"],
            "test_fingerprint": row["test_fingerprint"],
            "conftest_fingerprint": row["conftest_fingerprint"],
            "environment_fingerprint": row["environment_fingerprint"],
            "selection_fingerprint": row["selection_fingerprint"],
            "result_fingerprint": row["result_fingerprint"],
            "source_kind": row["source_kind"],
            "evidence_quality": row["evidence_quality"],
            "evidence_schema_version": int(row["evidence_schema_version"] or 0),
            "retention_class": row["retention_class"],
            "scope_id": str(row["scope_id"]),
            "project_id": str(row["project_id"]),
            "revision_id": str(row["revision_id"]),
            "environment_id": str(row["environment_id"]),
            "identity_sha256": str(row["identity_sha256"]),
        }
        if include_payload and row["payload"] is not None:
            value["payload"] = json.loads(str(row["payload"]))
        return value
    def query_executions(
        self,
        *,
        campaign_id: str | None = None,
        project_id: str | None = None,
        revision_id: str | None = None,
        environment_id: str | None = None,
        mutant_id: str | None = None,
        statuses: Sequence[str] | None = None,
        min_attempt: int | None = None,
        max_attempt: int | None = None,
        source_kind: str | None = None,
        result_fingerprint: str | None = None,
        cursor: str | None = None,
        limit: int = 100,
        include_payload: bool = False,
        include_compacted: bool = False,
    ) -> KnowledgePage:
        # Return a bounded, stable keyset page suitable for planner and UI consumers.
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        if min_attempt is not None and min_attempt < 0:
            raise ValueError("min_attempt must not be negative")
        if max_attempt is not None and max_attempt < 0:
            raise ValueError("max_attempt must not be negative")
        if min_attempt is not None and max_attempt is not None and min_attempt > max_attempt:
            raise ValueError("min_attempt must not exceed max_attempt")
        decoded_cursor = decode_cursor(cursor) if cursor else None
        sql, params = self._query_sql(
            include_compacted=include_compacted,
            include_payload=include_payload,
            campaign_id=campaign_id,
            project_id=project_id,
            revision_id=revision_id,
            environment_id=environment_id,
            mutant_id=mutant_id,
            statuses=tuple(str(item) for item in statuses) if statuses else None,
            min_attempt=min_attempt,
            max_attempt=max_attempt,
            source_kind=source_kind,
            result_fingerprint=result_fingerprint,
            cursor=decoded_cursor,
            limit=limit,
        )
        rows = self._connection.execute(sql, params).fetchall()
        has_more = len(rows) > limit
        visible = rows[:limit]
        records = tuple(self._row_to_dict(row, include_payload=include_payload) for row in visible)
        next_cursor = encode_cursor(str(visible[-1]["created_at"]), str(visible[-1]["event_id"])) if has_more else None
        return KnowledgePage(records, next_cursor, self._current_revision())

    def query_cost_observations(
        self,
        mutant_ids: Sequence[str],
        *,
        project_id: str | None = None,
        runtime_class: str | None = None,
        limit: int = 1000,
    ) -> dict[str, dict[str, object]]:
        # Aggregate bounded physical execution cost hints in one read without exposing semantic authority.
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1 or limit > 10000:
            raise ValueError("cost history limit must be between 1 and 10000")
        identifiers = tuple(sorted({str(item).strip() for item in mutant_ids if str(item).strip()}))
        if not identifiers:
            return {}
        placeholders = ", ".join("?" for _ in identifiers)
        clauses = [f"e.mutant_id IN ({placeholders})"]
        params: list[Any] = list(identifiers)
        if project_id is not None:
            clauses.append("k.project_id = ?")
            params.append(str(project_id))
        rows = self._connection.execute(
            "SELECT e.event_id, e.mutant_id, e.payload "
            "FROM knowledge_executions e "
            "JOIN knowledge_effects k ON k.effect_id = e.effect_id "
            f"WHERE {' AND '.join(clauses)} "
            "ORDER BY e.created_at DESC, e.event_id DESC LIMIT ?",
            (*params, limit),
        ).fetchall()
        buckets: dict[str, list[tuple[int, float, float, float, int, str | None]]] = {}
        for row in rows:
            mutant_id = str(row["mutant_id"])
            try:
                payload = json.loads(str(row["payload"]))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if not isinstance(payload, Mapping):
                continue
            source_kind = str(payload.get("source_kind", "observed")).strip().lower()
            if source_kind in {"exact_reuse", "partial_merged", "synthetic", "aggregate_projection"}:
                continue
            observed_runtime = _payload_runtime_class(payload)
            requested_runtime = str(runtime_class).strip() if runtime_class is not None else ""
            if requested_runtime and observed_runtime not in {None, requested_runtime}:
                continue
            mutation_seconds = _payload_duration_seconds(payload)
            subset_seconds, subset_count = _test_subset_cost(payload)
            cost_seconds = mutation_seconds if mutation_seconds is not None and mutation_seconds > 0.0 else subset_seconds
            if cost_seconds <= 0.0:
                continue
            runtime_rank = 0 if requested_runtime and observed_runtime == requested_runtime else 1
            buckets.setdefault(mutant_id, []).append(
                (
                    runtime_rank,
                    cost_seconds,
                    mutation_seconds if mutation_seconds is not None else 0.0,
                    subset_seconds,
                    subset_count,
                    observed_runtime,
                )
            )
        result: dict[str, dict[str, object]] = {}
        for mutant_id, bucket in buckets.items():
            exact = [item for item in bucket if item[0] == 0]
            selected = exact or bucket
            costs = [item[1] for item in selected]
            mutation_values = [item[2] for item in selected if item[2] > 0.0]
            subset_values = [item[3] for item in selected if item[3] > 0.0]
            runtime_values = [item[5] for item in selected if item[5]]
            runtime_value = runtime_values[0] if runtime_values and len(set(runtime_values)) == 1 else None
            result[mutant_id] = {
                "duration_seconds": sum(mutation_values) / len(mutation_values)
                if mutation_values
                else sum(costs) / len(costs),
                "duration_p95_seconds": _cost_p95(costs),
                "duration_sample_count": float(len(costs)),
                "test_subset_cost_seconds": (
                    sum(subset_values) / len(subset_values) if subset_values else 0.0
                ),
                "test_subset_count": (
                    sum(float(item[4]) for item in selected) / len(selected)
                    if selected
                    else 0.0
                ),
                "runtime_class": runtime_value,
                "runtime_class_sample_count": float(len(runtime_values)),
                "cost_observation_count": float(len(selected)),
            }
        return result

    def execution_history(
        self,
        campaign_id: str,
        mutant_id: str,
        *,
        include_compacted: bool = False,
    ) -> tuple[dict[str, Any], ...]:
        # Return all retry attempts for one mutant while optionally including compacted rollups.
        page = self.query_executions(
            campaign_id=campaign_id,
            mutant_id=mutant_id,
            limit=1000,
            include_payload=True,
            include_compacted=include_compacted,
        )
        rows = sorted(page.rows, key=lambda item: (int(item["attempt"]), str(item["created_at"]), str(item["event_id"])))
        return tuple(
            {
                "execution_id": str(row["execution_id"]),
                "attempt": int(row["attempt"]),
                "status": str(row["status"]),
                "payload": row.get("payload"),
                "created_at": str(row["created_at"]),
            }
            for row in rows
        )
    def get_execution_record(self, event_id: str) -> dict[str, Any] | None:
        # Load one raw execution payload for a verified exact-reuse decision.
        row = self._connection.execute(
            "SELECT e.event_id, e.effect_id, e.campaign_id, e.execution_id, e.mutant_id, e.attempt, e.status, "
            "e.payload, e.created_at, 0 AS compacted, f.function_id, f.function_fingerprint, f.mutant_fingerprint, "
            "f.test_fingerprint, f.conftest_fingerprint, f.environment_fingerprint, f.selection_fingerprint, f.result_fingerprint, "
            "f.source_kind, f.evidence_quality, f.evidence_schema_version, f.retention_class, e.scope_id, "
            "k.project_id, k.revision_id, k.environment_id, e.identity_sha256 FROM knowledge_executions e "
            "LEFT JOIN knowledge_execution_fingerprints f ON f.event_id = e.event_id "
            "JOIN knowledge_effects k ON k.effect_id = e.effect_id WHERE e.event_id = ?",
            (event_id,),
        ).fetchone()
        return self._row_to_dict(row, include_payload=True) if row is not None else None
    def _invalidated(
        self,
        candidate: Mapping[str, Any],
        test_ids: Sequence[str] = (),
        *,
        include_result: bool = True,
    ) -> bool:
        # Reject a candidate touched by a newer relevant invalidation edge.
        keys: list[tuple[str, str]] = []
        for scope_key in (candidate.get("mutant_id"), candidate.get("mutant_fingerprint")):
            if scope_key:
                keys.append(("mutant", str(scope_key)))
        for scope_key in (candidate.get("function_id"), candidate.get("function_fingerprint")):
            if scope_key:
                keys.append(("function", str(scope_key)))
        for scope_key, scope_type in (
            (candidate.get("conftest_fingerprint"), "conftest"),
            (candidate.get("environment_fingerprint"), "environment"),
            (candidate.get("selection_fingerprint"), "selection"),
        ):
            if scope_key:
                keys.append((scope_type, str(scope_key)))
        if include_result and candidate.get("result_fingerprint"):
            keys.append(("result", str(candidate["result_fingerprint"])))
        keys.extend(("test", str(item)) for item in test_ids)
        candidate_time = _normalize_timestamp(candidate["created_at"])
        candidate_datetime = datetime.fromisoformat(candidate_time.replace("Z", "+00:00"))
        for scope_type, scope_key in keys:
            row = self._connection.execute(
                "SELECT created_at FROM knowledge_invalidations WHERE scope_type = ? AND scope_key = ? "
                "AND (campaign_id IS NULL OR campaign_id = ?)",
                (scope_type, scope_key, candidate.get("campaign_id")),
            ).fetchall()
            for invalidation in row:
                invalidated_at = _normalize_timestamp(invalidation[0])
                invalidated_datetime = datetime.fromisoformat(invalidated_at.replace("Z", "+00:00"))
                if invalidated_datetime >= candidate_datetime:
                    return True
        return False
    def _candidate_observations(self, event_id: str) -> dict[str, str]:
        # Load the per-test fingerprint map required to form a partial reuse decision.
        rows = self._connection.execute(
            "SELECT test_id, test_fingerprint, outcome, payload FROM knowledge_test_observations WHERE event_id = ?",
            (event_id,),
        ).fetchall()
        observations: dict[str, str] = {}
        for row in rows:
            try:
                payload = json.loads(str(row["payload"]))
            except (TypeError, ValueError):
                continue
            if (
                not isinstance(payload, Mapping)
                or str(payload.get("evidence_kind", "")) != "pytest_test_event"
                or str(row["outcome"] or "") in {"", "unknown"}
            ):
                continue
            observations[str(row["test_id"])] = str(row["test_fingerprint"])
        return observations
    def invalidate_changed_inputs(
        self,
        requests: Iterable[Mapping[str, Any]],
        *,
        campaign_id: str | None = None,
        invalidated_at: str | None = None,
    ) -> tuple[str, ...]:
        # Append deterministic invalidation edges when current inputs differ from stored evidence.
        timestamp = _normalize_timestamp(invalidated_at or utc_now_iso())
        created: list[str] = []
        seen: set[tuple[str, str]] = set()
        for request in self._canonical_reuse_requests(requests):
            mutant_id = str(request["mutant_id"])
            candidates = self._reuse_candidate_rows(
                mutant_id,
                str(request["mutant_fingerprint"]) if request.get("mutant_fingerprint") else None,
            )
            current_tests = {
                str(key): str(value)
                for key, value in (request.get("test_fingerprints", {}) or {}).items()
            }
            for candidate in candidates:
                changes: list[tuple[str, str, str, str]] = []
                for field, scope_type in (
                    ("function_id", "function"),
                    ("function_fingerprint", "function"),
                    ("mutant_fingerprint", "mutant"),
                    ("environment_fingerprint", "environment"),
                    ("selection_fingerprint", "selection"),
                    ("result_fingerprint", "result"),
                    ("conftest_fingerprint", "conftest"),
                ):
                    old_value = candidate.get(field)
                    new_value = request.get(field)
                    if old_value and new_value and str(old_value) != str(new_value):
                        changes.append((scope_type, str(old_value), field, str(new_value)))
                observations = self._candidate_observations(str(candidate["event_id"]))
                for test_id, old_fingerprint in observations.items():
                    new_fingerprint = current_tests.get(test_id)
                    if new_fingerprint and new_fingerprint != old_fingerprint:
                        changes.append(("test", test_id, "test_fingerprint", new_fingerprint))
                for scope_type, scope_key, field, replacement in changes:
                    identity = (scope_type, scope_key)
                    if identity in seen:
                        continue
                    seen.add(identity)
                    idempotency_key = stable_hash(
                        {
                            "source_event_id": str(candidate["event_id"]),
                            "scope_type": scope_type,
                            "scope_key": scope_key,
                            "field": field,
                            "replacement": replacement,
                        }
                    )
                    created.append(
                        self.invalidate(
                            scope_type=scope_type,
                            scope_key=scope_key,
                            reason=f"automatic input change: {field}",
                            campaign_id=None,
                            fingerprint=replacement,
                            invalidated_at=timestamp,
                            idempotency_key=idempotency_key,
                        )
                    )
        return tuple(created)
    def _reuse_candidate_rows(self, mutant_id: str, mutant_fingerprint: str | None) -> list[dict[str, Any]]:
        # Collect raw and compacted candidates by identity or immutable mutant fingerprint.
        rows = self._connection.execute(
            "SELECT e.event_id, e.execution_id, e.mutant_id, e.attempt, e.status, e.campaign_id, e.created_at, "
            "0 AS compacted, f.function_id, f.function_fingerprint, f.mutant_fingerprint, f.test_fingerprint, "
            "f.conftest_fingerprint, f.environment_fingerprint, f.selection_fingerprint, f.result_fingerprint, f.source_kind, "
            "f.evidence_quality, f.evidence_schema_version "
            "FROM knowledge_executions e LEFT JOIN knowledge_execution_fingerprints f ON f.event_id = e.event_id "
            "WHERE e.mutant_id = ? OR (? IS NOT NULL AND f.mutant_fingerprint = ?) "
            "UNION ALL "
            "SELECT r.event_id, r.execution_id, r.mutant_id, r.attempt, r.status, r.campaign_id, r.created_at, "
            "1 AS compacted, r.function_id, r.function_fingerprint, r.mutant_fingerprint, r.test_fingerprint, "
            "r.conftest_fingerprint, r.environment_fingerprint, r.selection_fingerprint, r.result_fingerprint, r.source_kind, "
            "r.evidence_quality, r.evidence_schema_version "
            "FROM knowledge_execution_rollups r WHERE r.mutant_id = ? OR (? IS NOT NULL AND r.mutant_fingerprint = ?) "
            "ORDER BY attempt DESC, created_at DESC, event_id DESC",
            (mutant_id, mutant_fingerprint, mutant_fingerprint, mutant_id, mutant_fingerprint, mutant_fingerprint),
        ).fetchall()
        return [dict(row) for row in rows]
    def _reuse_mismatch_fields(
        self,
        candidate: Mapping[str, Any],
        *,
        mutant_fingerprint: str | None,
        environment_fingerprint: str | None,
        selection_fingerprint: str | None,
        test_fingerprint: str | None,
        test_fingerprints: Mapping[str, str],
        conftest_fingerprint: str | None,
        function_fingerprint: str | None,
        result_fingerprint: str | None,
    ) -> tuple[str, ...]:
        # Explain why one historical candidate cannot cross the current semantic reuse boundary.
        mismatches: list[str] = []
        if str(candidate.get("evidence_quality", EvidenceQuality.UNKNOWN.value)) != EvidenceQuality.VALIDATED.value:
            mismatches.append(f"evidence_quality={candidate.get('evidence_quality') or EvidenceQuality.UNKNOWN.value}")
        if str(candidate.get("status", "")) not in {"killed", "survived", "invalid_mutant"}:
            mismatches.append(f"status={candidate.get('status') or 'unknown'}")
        if str(candidate.get("source_kind", "")) not in {"observed", "validated"}:
            mismatches.append(f"source_kind={candidate.get('source_kind') or 'unknown'}")
        if bool(candidate.get("compacted")):
            mismatches.append("compacted")
        for field, expected in (
            ("mutant_fingerprint", mutant_fingerprint),
            ("function_fingerprint", function_fingerprint),
            ("environment_fingerprint", environment_fingerprint),
            ("selection_fingerprint", selection_fingerprint),
            ("test_fingerprint", test_fingerprint),
            ("conftest_fingerprint", conftest_fingerprint),
            ("result_fingerprint", result_fingerprint),
        ):
            if expected and str(candidate.get(field) or "") != str(expected):
                mismatches.append(field)
        observations = self._candidate_observations(str(candidate["event_id"]))
        if test_fingerprints and not observations:
            mismatches.append("test_observations_missing")
        elif test_fingerprints:
            changed = sorted(
                test_id
                for test_id, fingerprint in test_fingerprints.items()
                if observations.get(str(test_id)) != str(fingerprint)
            )
            if changed:
                mismatches.append("test_nodes=" + ",".join(changed[:8]))
        return tuple(dict.fromkeys(mismatches))
    def decide_reuse(
        self,
        *,
        mutant_id: str,
        mutant_fingerprint: str | None,
        environment_fingerprint: str | None,
        selection_fingerprint: str | None,
        test_fingerprint: str | None = None,
        test_fingerprints: Mapping[str, str] | None = None,
        conftest_fingerprint: str | None = None,
        function_id: str | None = None,
        function_fingerprint: str | None = None,
        result_fingerprint: str | None = None,
        reuse_blockers: Sequence[str] = (),
    ) -> ReuseDecision:
        # Decide exact, partial or historical reuse without allowing hints to skip execution.
        if not mutant_id:
            raise ValueError("mutant_id is required")
        blockers_set = {str(item) for item in reuse_blockers if str(item)}
        rule_key = str(function_id or function_fingerprint or mutant_fingerprint or mutant_id)
        quarantine_keys = (
            ("rule", rule_key),
            ("mutant", mutant_id),
            ("mutant_fingerprint", str(mutant_fingerprint) if mutant_fingerprint else ""),
            ("function", str(function_id) if function_id else ""),
            ("function_fingerprint", str(function_fingerprint) if function_fingerprint else ""),
        )
        for scope_type, scope_key in quarantine_keys:
            if scope_key and self.is_reuse_quarantined(scope_type=scope_type, scope_key=scope_key):
                blockers_set.add(f"reuse-quarantined:{scope_type}:{scope_key}")
        blockers = tuple(sorted(blockers_set))
        current_tests = {str(key): str(value) for key, value in (test_fingerprints or {}).items()}
        candidates = self._reuse_candidate_rows(mutant_id, mutant_fingerprint)
        exact_candidate: dict[str, Any] | None = None
        partial_candidate: tuple[dict[str, Any], tuple[str, ...], tuple[str, ...]] | None = None
        historical_candidate: dict[str, Any] | None = None
        historical_mismatches: tuple[str, ...] = ()
        for candidate in candidates:
            base_invalidated = self._invalidated(candidate, include_result=False)
            if base_invalidated:
                continue
            evidence_reusable = (
                str(candidate.get("evidence_quality", EvidenceQuality.UNKNOWN.value))
                == EvidenceQuality.VALIDATED.value
                and str(candidate.get("status", "")) in {"killed", "survived", "invalid_mutant"}
                and str(candidate.get("source_kind", "")) in {"observed", "validated"}
            )
            same_mutant = bool(mutant_fingerprint and candidate.get("mutant_fingerprint") == mutant_fingerprint)
            same_function = not function_fingerprint or candidate.get("function_fingerprint") == function_fingerprint
            same_environment = bool(environment_fingerprint and candidate.get("environment_fingerprint") == environment_fingerprint)
            same_selection = bool(selection_fingerprint and candidate.get("selection_fingerprint") == selection_fingerprint)
            same_test = bool(test_fingerprint and candidate.get("test_fingerprint") == test_fingerprint)
            same_conftest = not conftest_fingerprint or candidate.get("conftest_fingerprint") == conftest_fingerprint
            same_result = bool(result_fingerprint and candidate.get("result_fingerprint") == result_fingerprint)
            observations = self._candidate_observations(str(candidate["event_id"]))
            candidate_test_ids = tuple(observations)
            test_invalidated = self._invalidated(
                candidate,
                candidate_test_ids,
                include_result=True,
            )
            all_test_proof = bool(observations) and (not current_tests or set(current_tests).issubset(observations))
            if not blockers and evidence_reusable and not candidate.get("compacted") and not test_invalidated and all_test_proof and same_mutant and same_function and same_environment and same_selection and same_test and same_conftest and (same_result or not result_fingerprint):
                exact_candidate = candidate
                break
            if not blockers and evidence_reusable and str(candidate.get("status", "")) != "survived" and not candidate.get("compacted") and same_mutant and same_function and same_environment and same_selection and same_conftest and current_tests:
                matched = tuple(
                    sorted(
                        test_id
                        for test_id, value in current_tests.items()
                        if observations.get(test_id) == value
                        and not self._invalidated(
                            candidate,
                            (test_id,),
                            include_result=False,
                        )
                    )
                )
                missing = tuple(sorted(test_id for test_id in current_tests if test_id not in matched))
                if matched and missing:
                    current_rank = (len(matched), int(candidate.get("attempt", 0)), str(candidate.get("created_at", "")), str(candidate.get("event_id", "")))
                    previous_rank = (
                        len(partial_candidate[1]),
                        int(partial_candidate[0].get("attempt", 0)),
                        str(partial_candidate[0].get("created_at", "")),
                        str(partial_candidate[0].get("event_id", "")),
                    ) if partial_candidate is not None else None
                    if previous_rank is None or current_rank > previous_rank:
                        partial_candidate = (candidate, matched, missing)
            if same_mutant or (function_fingerprint and candidate.get("function_fingerprint") == function_fingerprint):
                if historical_candidate is None:
                    historical_candidate = candidate
                    historical_mismatches = self._reuse_mismatch_fields(
                        candidate,
                        mutant_fingerprint=mutant_fingerprint,
                        environment_fingerprint=environment_fingerprint,
                        selection_fingerprint=selection_fingerprint,
                        test_fingerprint=test_fingerprint,
                        test_fingerprints=current_tests,
                        conftest_fingerprint=conftest_fingerprint,
                        function_fingerprint=function_fingerprint,
                        result_fingerprint=result_fingerprint,
                    )
        if exact_candidate is not None:
            return ReuseDecision(
                mutant_id,
                ReuseKind.EXACT,
                True,
                "all result fingerprints match",
                source_event_id=str(exact_candidate["event_id"]),
                source_execution_id=str(exact_candidate["execution_id"]),
                result_status=str(exact_candidate["status"]),
                evidence_quality=str(exact_candidate.get("evidence_quality", EvidenceQuality.UNKNOWN.value)),
                audit_required=True,
                blockers=blockers,
            )
        if partial_candidate is not None:
            candidate, matched, missing = partial_candidate
            return ReuseDecision(
                mutant_id,
                ReuseKind.PARTIAL,
                True,
                "only a subset of test fingerprints match",
                source_event_id=str(candidate["event_id"]),
                source_execution_id=str(candidate["execution_id"]),
                result_status=str(candidate["status"]),
                evidence_quality=str(candidate.get("evidence_quality", EvidenceQuality.UNKNOWN.value)),
                matched_test_ids=matched,
                missing_test_ids=missing,
                audit_required=True,
                blockers=blockers,
            )
        if historical_candidate is not None:
            return ReuseDecision(
                mutant_id,
                ReuseKind.HISTORICAL_HINT,
                False,
                (
                    "reuse is blocked by unresolved dynamic dependencies: "
                    + ", ".join(blockers)
                    if blockers
                    else (
                        "historical result exists but the reuse boundary changed"
                        + (": " + ", ".join(historical_mismatches) if historical_mismatches else "")
                    )
                ),
                source_event_id=str(historical_candidate["event_id"]),
                source_execution_id=str(historical_candidate["execution_id"]),
                result_status=str(historical_candidate["status"]),
                evidence_quality=str(historical_candidate.get("evidence_quality", EvidenceQuality.UNKNOWN.value)),
                source_compacted=bool(historical_candidate.get("compacted")),
                blockers=blockers,
            )
        return ReuseDecision(
            mutant_id,
            ReuseKind.NONE,
            False,
            (
                "reuse is blocked by unresolved dynamic dependencies: " + ", ".join(blockers)
                if blockers
                else "no compatible execution evidence exists"
            ),
            blockers=blockers,
        )
    @staticmethod
    def _canonical_reuse_requests(requests: Iterable[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
        # Canonicalize request maps and reject duplicate mutant identities before planning.
        normalized: list[dict[str, Any]] = []
        seen: set[str] = set()
        for request in requests:
            if not isinstance(request, Mapping):
                raise ValueError("reuse request must be an object")
            row = dict(request)
            mutant_id = str(row.get("mutant_id", "")).strip()
            if not mutant_id:
                raise ValueError("reuse request mutant_id is required")
            if mutant_id in seen:
                raise ValueError(f"duplicate reuse request mutant_id: {mutant_id}")
            seen.add(mutant_id)
            raw_tests = row.get("test_fingerprints")
            if raw_tests is not None:
                if not isinstance(raw_tests, Mapping):
                    raise ValueError("test_fingerprints must be an object")
                row["test_fingerprints"] = {
                    str(key): str(value)
                    for key, value in sorted(raw_tests.items(), key=lambda item: str(item[0]))
                }
            raw_blockers = row.get("reuse_blockers")
            if isinstance(raw_blockers, (list, tuple, set, frozenset)):
                row["reuse_blockers"] = tuple(sorted(dict.fromkeys(str(item) for item in raw_blockers if str(item))))
            normalized.append(row)
        return tuple(sorted(normalized, key=lambda item: str(item["mutant_id"])))
    def plan_reuse(
        self,
        requests: Iterable[Mapping[str, Any]],
        *,
        reuse_mode: ReuseMode | str = ReuseMode.HINT,
    ) -> tuple[ReuseDecision, ...]:
        # Produce policy-authorized decisions in canonical mutant order.
        resolved_mode = normalize_reuse_mode(reuse_mode)
        decisions: list[ReuseDecision] = []
        for request in self._canonical_reuse_requests(requests):
            if not isinstance(request, Mapping):
                raise ValueError("reuse request must be an object")
            mutant_id = str(request.get("mutant_id", ""))
            if resolved_mode is ReuseMode.OFF:
                decisions.append(
                    ReuseDecision(
                        mutant_id=mutant_id,
                        kind=ReuseKind.NONE,
                        eligible=False,
                        reason="reuse disabled by off mode",
                        blockers=("reuse-mode:off",),
                        authorized=False,
                        reuse_mode=resolved_mode,
                    )
                )
                continue
            factual = self.decide_reuse(
                mutant_id=mutant_id,
                mutant_fingerprint=str(request["mutant_fingerprint"])
                if request.get("mutant_fingerprint")
                else None,
                environment_fingerprint=str(request["environment_fingerprint"])
                if request.get("environment_fingerprint")
                else None,
                selection_fingerprint=str(request["selection_fingerprint"])
                if request.get("selection_fingerprint")
                else None,
                test_fingerprint=str(request["test_fingerprint"])
                if request.get("test_fingerprint")
                else None,
                test_fingerprints=request.get("test_fingerprints")
                if isinstance(request.get("test_fingerprints"), Mapping)
                else None,
                conftest_fingerprint=str(request["conftest_fingerprint"])
                if request.get("conftest_fingerprint")
                else None,
                function_id=str(request["function_id"]) if request.get("function_id") else None,
                function_fingerprint=str(request["function_fingerprint"])
                if request.get("function_fingerprint")
                else None,
                result_fingerprint=str(request["result_fingerprint"])
                if request.get("result_fingerprint")
                else None,
                reuse_blockers=request.get("reuse_blockers", ())
                if isinstance(request.get("reuse_blockers", ()), (list, tuple, set, frozenset))
                else (),
            )
            decisions.append(authorize_reuse_decision(factual, resolved_mode))
        return tuple(decisions)
    def build_reuse_plan(
        self,
        requests: Iterable[Mapping[str, Any]],
        *,
        reuse_mode: ReuseMode | str = ReuseMode.HINT,
    ) -> ReusePlanArtifact:
        # Bind canonical decisions and their mode to the exact knowledge revision used.
        normalized_requests = self._canonical_reuse_requests(requests)
        resolved_mode = normalize_reuse_mode(reuse_mode)
        validation_started = time.perf_counter()
        decisions = self.plan_reuse(normalized_requests, reuse_mode=resolved_mode)
        validation_cost_seconds = max(0.0, time.perf_counter() - validation_started)
        estimated_costs: dict[str, float] = {}
        test_counts: dict[str, int] = {}
        for request in normalized_requests:
            mutant_id = str(request["mutant_id"])
            try:
                estimated_costs[mutant_id] = max(
                    0.0,
                    float(request.get("estimated_execution_seconds", request.get("estimated_seconds", 0.0))),
                )
            except (TypeError, ValueError):
                estimated_costs[mutant_id] = 0.0
            raw_tests = request.get("test_fingerprints")
            test_counts[mutant_id] = len(raw_tests) if isinstance(raw_tests, Mapping) else 0
        metrics = ReuseMetrics.from_decisions(
            decisions,
            estimated_costs=estimated_costs,
            test_counts=test_counts,
            validation_cost_seconds=validation_cost_seconds,
        )
        history_revision = self._current_revision()
        input_fingerprint = stable_hash(normalized_requests)
        plan_fingerprint = stable_hash(
            {
                "reuse_mode": resolved_mode.value,
                "history_revision": history_revision,
                "input_fingerprint": input_fingerprint,
                "decisions": [item.to_dict() for item in decisions],
            }
        )
        return ReusePlanArtifact(
            history_revision,
            input_fingerprint,
            plan_fingerprint,
            decisions,
            resolved_mode,
            metrics,
        )
    def write_reuse_plan(
        self,
        path: str | Path,
        requests: Iterable[Mapping[str, Any]],
        *,
        campaign_id: str | None = None,
        reuse_mode: ReuseMode | str = ReuseMode.HINT,
    ) -> ReusePlanArtifact:
        # Protect executable evidence before publishing one mode-authorized reuse plan artifact.
        plan = self.build_reuse_plan(requests, reuse_mode=reuse_mode)
        if campaign_id is not None:
            self.protect_reuse_plan(campaign_id, plan)
        atomic_write_json(Path(path), plan.to_dict(), durability="critical", category="report")
        return plan
    def invalidate(
        self,
        *,
        scope_type: str,
        scope_key: str,
        reason: str,
        campaign_id: str | None = None,
        fingerprint: str | None = None,
        invalidated_at: str | None = None,
        idempotency_key: str | None = None,
    ) -> str:
        # Append one idempotent invalidation marker that blocks only older matching evidence.
        if scope_type not in self._INVALIDATION_SCOPES:
            raise ValueError(f"unsupported invalidation scope: {scope_type}")
        if not scope_key or not reason:
            raise ValueError("scope_key and reason are required")
        created_at = _normalize_timestamp(invalidated_at or utc_now_iso())
        identity = str(idempotency_key or created_at).strip()
        if not identity:
            raise ValueError("idempotency_key must not be empty")
        invalidation_id = stable_hash(
            {
                "scope_type": scope_type,
                "scope_key": scope_key,
                "campaign_id": campaign_id,
                "reason": reason,
                "fingerprint": fingerprint,
                "idempotency_key": identity,
            }
        )[:32]
        try:
            self._connection.execute("BEGIN IMMEDIATE")
            cursor = self._connection.execute(
                "INSERT OR IGNORE INTO knowledge_invalidations(invalidation_id, scope_type, scope_key, campaign_id, reason, fingerprint, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (invalidation_id, scope_type, scope_key, campaign_id, reason, fingerprint, created_at),
            )
            if cursor.rowcount:
                self._bump_revision()
            self._connection.commit()
            return invalidation_id
        except BaseException:
            self._connection.rollback()
            raise
    def record_reuse_incident(
        self,
        *,
        campaign_id: str,
        mutant_id: str,
        rule_id: str,
        kind: str,
        mismatches: Sequence[str],
        expected_payload: Mapping[str, Any],
        actual_payload: Mapping[str, Any],
        created_at: str | None = None,
        status: str = "open",
    ) -> str:
        # Persist one content-addressed fresh-audit mismatch without duplicating retries or resumes.
        if not campaign_id or not mutant_id or not rule_id or not kind:
            raise ValueError("campaign_id, mutant_id, rule_id and kind are required")
        normalized_mismatches = tuple(sorted({str(item) for item in mismatches if str(item)}))
        if not normalized_mismatches:
            raise ValueError("mismatches must not be empty")
        timestamp = _normalize_timestamp(created_at or utc_now_iso())
        incident_id = stable_hash(
            {
                "campaign_id": campaign_id,
                "mutant_id": mutant_id,
                "rule_id": rule_id,
                "kind": kind,
                "mismatches": normalized_mismatches,
                "expected_payload": expected_payload,
                "actual_payload": actual_payload,
                "status": str(status),
            }
        )[:32]
        try:
            self._connection.execute("BEGIN IMMEDIATE")
            cursor = self._connection.execute(
                "INSERT OR IGNORE INTO knowledge_reuse_incidents(incident_id, campaign_id, mutant_id, rule_id, kind, "
                "mismatches, expected_payload, actual_payload, created_at, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    incident_id,
                    campaign_id,
                    mutant_id,
                    rule_id,
                    kind,
                    _canonical(normalized_mismatches),
                    _canonical(expected_payload),
                    _canonical(actual_payload),
                    timestamp,
                    str(status),
                ),
            )
            if cursor.rowcount:
                self._bump_revision()
            self._connection.commit()
            return incident_id
        except BaseException:
            self._connection.rollback()
            raise
    def quarantine_reuse_rule(
        self,
        *,
        scope_type: str,
        scope_key: str,
        reason: str,
        incident_id: str,
        created_at: str | None = None,
    ) -> None:
        # Activate one idempotent fail-closed quarantine bound to a durable audit incident.
        if not scope_type or not scope_key or not reason or not incident_id:
            raise ValueError("scope_type, scope_key, reason and incident_id are required")
        timestamp = _normalize_timestamp(created_at or utc_now_iso())
        try:
            self._connection.execute("BEGIN IMMEDIATE")
            incident = self._connection.execute(
                "SELECT 1 FROM knowledge_reuse_incidents WHERE incident_id = ?",
                (incident_id,),
            ).fetchone()
            if incident is None:
                self._connection.rollback()
                raise KnowledgeConflict(f"reuse quarantine references an unknown incident: incident_id={incident_id}")
            existing = self._connection.execute(
                "SELECT incident_id, reason, released_at FROM knowledge_reuse_quarantine "
                "WHERE scope_type = ? AND scope_key = ?",
                (scope_type, scope_key),
            ).fetchone()
            if (
                existing is not None
                and existing["released_at"] is None
                and str(existing["incident_id"]) == incident_id
                and str(existing["reason"]) == reason
            ):
                self._connection.commit()
                return
            self._connection.execute(
                "INSERT INTO knowledge_reuse_quarantine(scope_type, scope_key, incident_id, reason, created_at, released_at, success_count) "
                "VALUES (?, ?, ?, ?, ?, NULL, 0) "
                "ON CONFLICT(scope_type, scope_key) DO UPDATE SET incident_id=excluded.incident_id, reason=excluded.reason, "
                "created_at=excluded.created_at, released_at=NULL, success_count=0",
                (scope_type, scope_key, incident_id, reason, timestamp),
            )
            self._bump_revision()
            self._connection.commit()
        except BaseException:
            self._connection.rollback()
            raise
    def is_reuse_quarantined(self, *, scope_type: str, scope_key: str) -> bool:
        # Read only active quarantine state so planning can add a deterministic blocker.
        if not scope_type or not scope_key:
            return False
        row = self._connection.execute(
            "SELECT 1 FROM knowledge_reuse_quarantine WHERE scope_type = ? AND scope_key = ? AND released_at IS NULL",
            (scope_type, scope_key),
        ).fetchone()
        return row is not None
    def release_reuse_quarantine(
        self,
        *,
        scope_type: str,
        scope_key: str,
        released_at: str | None = None,
    ) -> bool:
        # Release one active quarantine explicitly while preserving its incident history.
        if not scope_type or not scope_key:
            raise ValueError("scope_type and scope_key are required")
        timestamp = _normalize_timestamp(released_at or utc_now_iso())
        try:
            self._connection.execute("BEGIN IMMEDIATE")
            cursor = self._connection.execute(
                "UPDATE knowledge_reuse_quarantine SET released_at = ? "
                "WHERE scope_type = ? AND scope_key = ? AND released_at IS NULL",
                (timestamp, scope_type, scope_key),
            )
            if cursor.rowcount:
                self._bump_revision()
            self._connection.commit()
            return bool(cursor.rowcount)
        except BaseException:
            self._connection.rollback()
            raise
    def apply_retention(
        self,
        policy: KnowledgeRetentionPolicy,
        *,
        now: str | None = None,
    ) -> KnowledgeRetentionResult:
        # Compact a bounded old batch atomically while preserving graph identity and executable evidence bundles.
        if not policy.preserve_rollups:
            raise ValueError("Knowledge Plane compaction requires preserve_rollups=True")
        cutoff = _cutoff_iso(policy.max_age_seconds, now)
        protected = tuple(str(item) for item in policy.protected_campaign_ids)
        protected_clause = "" if not protected else f" AND e.campaign_id NOT IN ({', '.join('?' for _ in protected)})"
        protected_params: tuple[Any, ...] = protected
        live_reference_clause = (
            " AND NOT EXISTS (SELECT 1 FROM knowledge_evidence_references r "
            "JOIN knowledge_reuse_plans p ON p.plan_id = r.plan_id "
            "WHERE r.event_id = e.event_id AND p.released_at IS NULL)"
        )
        try:
            self._connection.execute("BEGIN IMMEDIATE")
            rows = self._connection.execute(
                "SELECT e.event_id, e.effect_id, e.campaign_id, e.execution_id, e.mutant_id, e.attempt, e.status, "
                "e.payload, e.created_at, f.function_id, f.function_fingerprint, f.mutant_fingerprint, f.test_fingerprint, "
                "f.conftest_fingerprint, f.environment_fingerprint, f.selection_fingerprint, f.result_fingerprint, f.source_kind, "
                "f.evidence_quality, f.evidence_schema_version, f.retention_class, e.scope_id, e.identity_sha256, "
                "k.project_id, k.revision_id, k.environment_id "
                "FROM knowledge_executions e LEFT JOIN knowledge_execution_fingerprints f ON f.event_id = e.event_id "
                "JOIN knowledge_effects k ON k.effect_id = e.effect_id "
                "WHERE e.created_at < ? AND COALESCE(f.retention_class, 'campaign') != 'forever'"
                f"{protected_clause}{live_reference_clause} ORDER BY e.created_at, e.event_id LIMIT ?",
                (cutoff, *protected_params, policy.batch_size),
            ).fetchall()
            skipped_protected = 0
            if protected:
                placeholders = ", ".join("?" for _ in protected)
                skipped_row = self._connection.execute(
                    "SELECT COUNT(*) FROM knowledge_executions WHERE created_at < ? AND campaign_id IN ("
                    f"{placeholders})",
                    (cutoff, *protected),
                ).fetchone()
                skipped_protected = int(skipped_row[0] if skipped_row is not None else 0)
            referenced_row = self._connection.execute(
                "SELECT COUNT(DISTINCT e.event_id) FROM knowledge_executions e "
                "LEFT JOIN knowledge_execution_fingerprints f ON f.event_id = e.event_id "
                "JOIN knowledge_evidence_references r ON r.event_id = e.event_id "
                "JOIN knowledge_reuse_plans p ON p.plan_id = r.plan_id "
                "WHERE e.created_at < ? AND COALESCE(f.retention_class, 'campaign') != 'forever' "
                "AND p.released_at IS NULL",
                (cutoff,),
            ).fetchone()
            skipped_referenced = int(referenced_row[0] if referenced_row is not None else 0)
            if not rows:
                self._connection.commit()
                return KnowledgeRetentionResult(
                    cutoff,
                    0,
                    0,
                    skipped_protected,
                    skipped_referenced,
                    None,
                )
            source_revision = self._current_revision()
            event_ids = tuple(str(row["event_id"]) for row in rows)
            compaction_id = stable_hash(
                {
                    "source_revision": source_revision,
                    "cutoff": cutoff,
                    "events": event_ids,
                }
            )[:32]
            created_at = _normalize_timestamp(now or utc_now_iso())
            observation_count = 0
            manifest_rows: list[dict[str, Any]] = []
            rollup_columns = (
                "event_id",
                "effect_id",
                "campaign_id",
                "execution_id",
                "mutant_id",
                "attempt",
                "status",
                "function_id",
                "function_fingerprint",
                "mutant_fingerprint",
                "test_fingerprint",
                "conftest_fingerprint",
                "environment_fingerprint",
                "selection_fingerprint",
                "result_fingerprint",
                "source_kind",
                "evidence_quality",
                "evidence_schema_version",
                "retention_class",
                "payload_sha256",
                "created_at",
                "compacted_at",
                "scope_id",
                "project_id",
                "revision_id",
                "environment_id",
                "identity_sha256",
                "graph_fingerprint",
                "evidence_payload",
                "evidence_sha256",
                "observation_count",
                "compaction_id",
            )
            rollup_sql = (
                "INSERT INTO knowledge_execution_rollups("
                + ", ".join(rollup_columns)
                + ") VALUES ("
                + ", ".join("?" for _ in rollup_columns)
                + ")"
            )
            for compacted_events, row in enumerate(rows, start=1):
                event_id = str(row["event_id"])
                execution_id = str(row["execution_id"])
                raw_execution = json.loads(str(row["payload"]))
                observations = self._connection.execute(
                    "SELECT observation_id, test_id, test_fingerprint, outcome, payload, observation_sha256 "
                    "FROM knowledge_test_observations WHERE event_id = ? ORDER BY test_id, observation_id",
                    (event_id,),
                ).fetchall()
                observation_payloads = tuple(json.loads(str(item["payload"])) for item in observations)
                observation_count += len(observations)
                graph = self.evidence_graph(execution_id)
                evidence_bundle = {
                    "schema_version": 1,
                    "event_id": event_id,
                    "execution_id": execution_id,
                    "execution": raw_execution,
                    "observations": list(observation_payloads),
                    "graph_fingerprint": graph.graph_fingerprint,
                    "graph_complete": graph.complete,
                    "graph_missing_requirements": list(graph.missing_requirements),
                }
                payload_hash = stable_hash(raw_execution)
                evidence_sha256 = stable_hash(evidence_bundle)
                self._connection.execute(
                    rollup_sql,
                    (
                        event_id,
                        row["effect_id"],
                        row["campaign_id"],
                        execution_id,
                        row["mutant_id"],
                        row["attempt"],
                        row["status"],
                        row["function_id"],
                        row["function_fingerprint"],
                        row["mutant_fingerprint"],
                        row["test_fingerprint"],
                        row["conftest_fingerprint"],
                        row["environment_fingerprint"],
                        row["selection_fingerprint"],
                        row["result_fingerprint"],
                        row["source_kind"] or "observed",
                        row["evidence_quality"] or EvidenceQuality.UNKNOWN.value,
                        int(row["evidence_schema_version"] or 0),
                        row["retention_class"] or "campaign",
                        payload_hash,
                        row["created_at"],
                        created_at,
                        row["scope_id"],
                        row["project_id"],
                        row["revision_id"],
                        row["environment_id"],
                        row["identity_sha256"],
                        graph.graph_fingerprint,
                        _canonical(evidence_bundle),
                        evidence_sha256,
                        len(observations),
                        compaction_id,
                    ),
                )
                tombstone_payload = {
                    "event_id": event_id,
                    "execution_id": execution_id,
                    "compaction_id": compaction_id,
                    "evidence_sha256": evidence_sha256,
                    "graph_fingerprint": graph.graph_fingerprint,
                }
                tombstone_id = stable_hash(
                    {"entity_type": "execution", "entity_id": execution_id, "event_id": event_id}
                )[:32]
                self._connection.execute(
                    "INSERT INTO knowledge_tombstones(tombstone_id, entity_type, entity_id, source_event_id, "
                    "replacement_id, reason, payload_sha256, payload, created_at) "
                    "VALUES (?, 'execution', ?, ?, ?, ?, ?, ?, ?)",
                    (
                        tombstone_id,
                        execution_id,
                        event_id,
                        compaction_id,
                        "raw evidence compacted into immutable rollup",
                        stable_hash(tombstone_payload),
                        _canonical(tombstone_payload),
                        created_at,
                    ),
                )
                self._connection.execute(
                    "DELETE FROM knowledge_test_observations WHERE event_id = ?",
                    (event_id,),
                )
                self._connection.execute(
                    "DELETE FROM knowledge_execution_fingerprints WHERE event_id = ?",
                    (event_id,),
                )
                self._connection.execute(
                    "DELETE FROM knowledge_executions WHERE event_id = ?",
                    (event_id,),
                )
                graph_after = self.evidence_graph(execution_id)
                if graph_after.graph_fingerprint != graph.graph_fingerprint:
                    raise KnowledgeConflict(
                        "compaction changed provenance graph identity: "
                        f"event_id={event_id}; execution_id={execution_id}; "
                        f"before={graph.graph_fingerprint}; after={graph_after.graph_fingerprint}"
                    )
                manifest_rows.append(
                    {
                        "event_id": event_id,
                        "execution_id": execution_id,
                        "identity_sha256": str(row["identity_sha256"]),
                        "evidence_sha256": evidence_sha256,
                        "graph_fingerprint": graph.graph_fingerprint,
                        "observation_count": len(observations),
                        "tombstone_id": tombstone_id,
                    }
                )
                self._after_compaction_event(compaction_id, compacted_events)
            manifest = {
                "schema_version": 1,
                "compaction_id": compaction_id,
                "source_revision": source_revision,
                "cutoff": cutoff,
                "events": manifest_rows,
            }
            manifest_sha256 = stable_hash(manifest)
            committed_at = _normalize_timestamp(utc_now_iso())
            self._connection.execute(
                "INSERT INTO knowledge_compaction_runs(compaction_id, source_revision, cutoff, manifest_sha256, "
                "manifest, event_count, observation_count, created_at, committed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    compaction_id,
                    source_revision,
                    cutoff,
                    manifest_sha256,
                    _canonical(manifest),
                    len(rows),
                    observation_count,
                    created_at,
                    committed_at,
                ),
            )
            self._bump_revision()
            self._connection.commit()
            return KnowledgeRetentionResult(
                cutoff,
                len(rows),
                observation_count,
                skipped_protected,
                skipped_referenced,
                compaction_id,
            )
        except BaseException:
            self._connection.rollback()
            raise
__all__ = [
    "KNOWLEDGE_SCHEMA_VERSION",
    "KnowledgeCompactionRecord",
    "KnowledgeConflict",
    "KnowledgeConflictRecord",
    "KnowledgeIngestResult",
    "KnowledgeIntegrityReport",
    "KnowledgePlaneStore",
    "KnowledgeSchemaState",
    "KnowledgeScopeIdentity",
    "KnowledgeSummary",
]
