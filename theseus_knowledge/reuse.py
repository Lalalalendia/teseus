"""Deterministic fingerprints and reuse decisions for the E16 knowledge layer."""
from __future__ import annotations
import ast
import base64
import binascii
import hashlib
import json
import math
import platform
import sys
import textwrap
from dataclasses import dataclass, field as dataclass_field, replace
from enum import Enum
from typing import Any, Mapping, Sequence
from test_intelligence_unified_v1.io_utils import stable_hash
from theseus_contracts import ReuseMode, normalize_reuse_mode
from .schema import KNOWLEDGE_SCHEMA_VERSION
class ReuseKind(str, Enum):
    """The only planner-visible reuse outcomes."""
    EXACT = "exact"
    PARTIAL = "partial"
    HISTORICAL_HINT = "historical_hint"
    NONE = "none"
class EvidenceQuality(str, Enum):
    """Proof quality required before a historical execution can be reused."""
    VALIDATED = "validated"
    INELIGIBLE = "ineligible"
    UNKNOWN = "unknown"


EVIDENCE_IDENTITY_VERSION = "evidence-reuse-v2"


@dataclass(frozen=True, slots=True)
class EvidenceIdentity:
    """Content identity for reusable mutation evidence, independent of one execution attempt."""

    mutation_semantic_fingerprint: str
    test_evidence_fingerprint: str
    environment_fingerprint: str
    dependency_fingerprint: str
    configuration_fingerprint: str
    test_fingerprints: tuple[tuple[str, str], ...] = ()
    version: str = EVIDENCE_IDENTITY_VERSION

    def __post_init__(self) -> None:
        # Reject incomplete proof contexts before they can become executable reuse keys.
        required = (
            self.mutation_semantic_fingerprint,
            self.test_evidence_fingerprint,
            self.environment_fingerprint,
            self.dependency_fingerprint,
            self.configuration_fingerprint,
            self.version,
        )
        if any(not str(item).strip() for item in required):
            raise ValueError("evidence identity fields must be non-empty")
        normalized = tuple(
            sorted(
                (str(test_id), str(fingerprint))
                for test_id, fingerprint in self.test_fingerprints
                if str(test_id).strip() and str(fingerprint).strip()
            )
        )
        if len({test_id for test_id, _ in normalized}) != len(normalized):
            raise ValueError("evidence identity contains duplicate test identities")
        object.__setattr__(self, "test_fingerprints", normalized)

    @property
    def fingerprint(self) -> str:
        # Bind every semantic, test, dependency, runtime and configuration dimension together.
        return stable_hash(
            {
                "kind": "evidence-identity",
                "version": self.version,
                "mutation_semantic_identity": self.mutation_semantic_fingerprint,
                "test_evidence_identity": {
                    "aggregate": self.test_evidence_fingerprint,
                    "nodes": list(self.test_fingerprints),
                },
                "runtime_environment_compatibility": self.environment_fingerprint,
                "relevant_dependency_evidence": self.dependency_fingerprint,
                "semantic_configuration": self.configuration_fingerprint,
            }
        )

    def to_dict(self) -> dict[str, Any]:
        # Keep the complete proof boundary available for diagnostics and cache-key audits.
        return {
            "version": self.version,
            "fingerprint": self.fingerprint,
            "mutation_semantic_fingerprint": self.mutation_semantic_fingerprint,
            "test_evidence_fingerprint": self.test_evidence_fingerprint,
            "test_fingerprints": {test_id: fingerprint for test_id, fingerprint in self.test_fingerprints},
            "environment_fingerprint": self.environment_fingerprint,
            "dependency_fingerprint": self.dependency_fingerprint,
            "configuration_fingerprint": self.configuration_fingerprint,
        }


def fingerprint_evidence_identity(
    *,
    mutation_fingerprint: str,
    test_fingerprint: str,
    environment_fingerprint: str,
    function_fingerprint: str | None = None,
    conftest_fingerprint: str | None = None,
    test_fingerprints: Mapping[str, str] | None = None,
    dependency_fingerprint: str | None = None,
    configuration_fingerprint: str | None = None,
    semantic_configuration: Any = None,
) -> str:
    """Build one v2 key from the full proof context required for evidence reuse."""
    node_fingerprints = tuple(
        sorted(
            (str(test_id), str(fingerprint))
            for test_id, fingerprint in (test_fingerprints or {}).items()
            if str(test_id).strip() and str(fingerprint).strip()
        )
    )
    dependency = str(dependency_fingerprint or "").strip() or stable_hash(
        {
            "function_fingerprint": str(function_fingerprint or ""),
            "conftest_fingerprint": str(conftest_fingerprint or ""),
        }
    )
    configuration = str(configuration_fingerprint or "").strip() or stable_hash(
        _canonical(semantic_configuration if semantic_configuration is not None else {})
    )
    identity = EvidenceIdentity(
        mutation_semantic_fingerprint=str(mutation_fingerprint),
        test_evidence_fingerprint=str(test_fingerprint),
        environment_fingerprint=str(environment_fingerprint),
        dependency_fingerprint=dependency,
        configuration_fingerprint=configuration,
        test_fingerprints=node_fingerprints,
    )
    return identity.fingerprint


fingerprint_evidence = fingerprint_evidence_identity


@dataclass(frozen=True, slots=True)
class ReuseMetrics:
    """Measured planning/reuse counters; reuse is counted only when policy authorizes it."""

    candidate_count: int = 0
    eligible_hits: int = 0
    authorized_hits: int = 0
    exact_hits: int = 0
    partial_hits: int = 0
    historical_hints: int = 0
    misses: int = 0
    executions_avoided: int = 0
    tests_avoided: int = 0
    reuse_hit_rate: float = 0.0
    eligible_hit_rate: float = 0.0
    wall_saved_seconds: float = 0.0
    validation_cost_seconds: float = 0.0

    def __post_init__(self) -> None:
        # Keep metrics safe to persist and prevent negative counters from hiding accounting bugs.
        for name in (
            "candidate_count",
            "eligible_hits",
            "authorized_hits",
            "exact_hits",
            "partial_hits",
            "historical_hints",
            "misses",
            "executions_avoided",
            "tests_avoided",
        ):
            if int(getattr(self, name)) < 0:
                raise ValueError(f"reuse metric {name} must not be negative")
        for name in ("reuse_hit_rate", "eligible_hit_rate", "wall_saved_seconds", "validation_cost_seconds"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value < 0.0:
                raise ValueError(f"reuse metric {name} must be finite and non-negative")

    @classmethod
    def from_decisions(
        cls,
        decisions: Sequence[ReuseDecision],
        *,
        estimated_costs: Mapping[str, float] | None = None,
        test_counts: Mapping[str, int] | None = None,
        validation_cost_seconds: float = 0.0,
    ) -> "ReuseMetrics":
        # Derive actual avoided work from the immutable decision ledger, never from optimistic hints.
        candidate_count = len(decisions)
        eligible = tuple(
            item for item in decisions
            if bool(item.eligible) and item.kind in {ReuseKind.EXACT, ReuseKind.PARTIAL}
        )
        authorized = tuple(item for item in eligible if bool(item.authorized))
        exact = tuple(item for item in authorized if item.kind is ReuseKind.EXACT)
        partial = tuple(item for item in authorized if item.kind is ReuseKind.PARTIAL)
        historical = tuple(item for item in decisions if item.kind is ReuseKind.HISTORICAL_HINT)
        costs = estimated_costs or {}
        counts = test_counts or {}
        saved = 0.0
        tests_avoided = 0
        for item in exact:
            saved += max(0.0, float(costs.get(item.mutant_id, 0.0)))
            tests_avoided += max(0, int(counts.get(item.mutant_id, 0)))
        for item in partial:
            tests_avoided += len(item.matched_test_ids)
            total = max(1, len(item.matched_test_ids) + len(item.missing_test_ids))
            saved += max(0.0, float(costs.get(item.mutant_id, 0.0))) * len(item.matched_test_ids) / total
        eligible_hits = len(eligible)
        authorized_hits = len(authorized)
        return cls(
            candidate_count=candidate_count,
            eligible_hits=eligible_hits,
            authorized_hits=authorized_hits,
            exact_hits=len(exact),
            partial_hits=len(partial),
            historical_hints=len(historical),
            misses=max(0, candidate_count - eligible_hits - len(historical)),
            executions_avoided=len(exact),
            tests_avoided=tests_avoided,
            reuse_hit_rate=authorized_hits / candidate_count if candidate_count else 0.0,
            eligible_hit_rate=eligible_hits / candidate_count if candidate_count else 0.0,
            wall_saved_seconds=saved,
            validation_cost_seconds=max(0.0, float(validation_cost_seconds)),
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ReuseMetrics":
        # Restore optional metrics from a plan artifact while tolerating pre-PR76 artifacts.
        return cls(
            candidate_count=max(0, int(value.get("candidate_count", 0))),
            eligible_hits=max(0, int(value.get("eligible_hits", 0))),
            authorized_hits=max(0, int(value.get("authorized_hits", 0))),
            exact_hits=max(0, int(value.get("exact_hits", 0))),
            partial_hits=max(0, int(value.get("partial_hits", 0))),
            historical_hints=max(0, int(value.get("historical_hints", 0))),
            misses=max(0, int(value.get("misses", 0))),
            executions_avoided=max(0, int(value.get("executions_avoided", 0))),
            tests_avoided=max(0, int(value.get("tests_avoided", 0))),
            reuse_hit_rate=max(0.0, float(value.get("reuse_hit_rate", 0.0))),
            eligible_hit_rate=max(0.0, float(value.get("eligible_hit_rate", 0.0))),
            wall_saved_seconds=max(0.0, float(value.get("wall_saved_seconds", value.get("wall_saved", 0.0)))),
            validation_cost_seconds=max(0.0, float(value.get("validation_cost_seconds", value.get("reuse_validation_cost_seconds", 0.0)))),
        )

    def to_dict(self) -> dict[str, Any]:
        # Publish both hit rates so hint-only plans cannot be mistaken for physically reused work.
        return {
            "candidate_count": self.candidate_count,
            "eligible_hits": self.eligible_hits,
            "authorized_hits": self.authorized_hits,
            "exact_hits": self.exact_hits,
            "partial_hits": self.partial_hits,
            "historical_hints": self.historical_hints,
            "misses": self.misses,
            "executions_avoided": self.executions_avoided,
            "tests_avoided": self.tests_avoided,
            "reuse_hit_rate": self.reuse_hit_rate,
            "eligible_hit_rate": self.eligible_hit_rate,
            "wall_saved_seconds": self.wall_saved_seconds,
            "wall_saved": self.wall_saved_seconds,
            "validation_cost_seconds": self.validation_cost_seconds,
            "reuse_validation_cost_seconds": self.validation_cost_seconds,
        }
@dataclass(frozen=True, slots=True)
class ReuseDecision:
    """Explainable decision that never conflates a hint with executable reuse."""
    mutant_id: str
    kind: ReuseKind
    eligible: bool
    reason: str
    source_event_id: str | None = None
    source_execution_id: str | None = None
    result_status: str | None = None
    evidence_quality: str = EvidenceQuality.UNKNOWN.value
    matched_test_ids: tuple[str, ...] = ()
    missing_test_ids: tuple[str, ...] = ()
    audit_required: bool = False
    source_compacted: bool = False
    blockers: tuple[str, ...] = ()
    authorized: bool | None = None
    reuse_mode: ReuseMode = ReuseMode.PARTIAL
    def __post_init__(self) -> None:
        # Canonicalize test partitions and derive backward-compatible authority when policy is absent.
        matched = tuple(sorted(dict.fromkeys(str(item) for item in self.matched_test_ids if str(item))))
        missing = tuple(sorted(dict.fromkeys(str(item) for item in self.missing_test_ids if str(item))))
        overlap = set(matched).intersection(missing)
        if overlap:
            raise ValueError("reuse test partitions must not overlap: " + ", ".join(sorted(overlap)))
        if self.kind is ReuseKind.PARTIAL and (not matched or not missing):
            raise ValueError("partial reuse requires non-empty matched and missing test partitions")
        if self.kind is not ReuseKind.PARTIAL and (matched or missing):
            raise ValueError("only partial reuse may carry test partitions")
        object.__setattr__(self, "matched_test_ids", matched)
        object.__setattr__(self, "missing_test_ids", missing)
        object.__setattr__(self, "blockers", tuple(sorted(dict.fromkeys(str(item) for item in self.blockers if str(item)))))
        if self.authorized is None:
            object.__setattr__(
                self,
                "authorized",
                bool(self.eligible and self.kind in {ReuseKind.EXACT, ReuseKind.PARTIAL}),
            )
    def to_dict(self) -> dict[str, Any]:
        # Serialize the reuse proof as a stable plan-artifact record.
        return {
            "mutant_id": self.mutant_id,
            "kind": self.kind.value,
            "eligible": self.eligible,
            "reason": self.reason,
            "source_event_id": self.source_event_id,
            "source_execution_id": self.source_execution_id,
            "result_status": self.result_status,
            "evidence_quality": self.evidence_quality,
            "matched_test_ids": list(self.matched_test_ids),
            "missing_test_ids": list(self.missing_test_ids),
            "audit_required": self.audit_required,
            "source_compacted": self.source_compacted,
            "blockers": list(self.blockers),
            "authorized": bool(self.authorized),
            "reuse_mode": self.reuse_mode.value,
        }
def authorize_reuse_decision(decision: ReuseDecision, mode: ReuseMode | str) -> ReuseDecision:
    # Apply execution authority without rewriting the factual evidence classification.
    resolved = normalize_reuse_mode(mode)
    mode_blocker = f"reuse-mode:{resolved.value}"
    if resolved is ReuseMode.OFF:
        return replace(
            decision,
            kind=ReuseKind.NONE,
            eligible=False,
            reason="reuse disabled by off mode",
            source_event_id=None,
            source_execution_id=None,
            result_status=None,
            matched_test_ids=(),
            missing_test_ids=(),
            audit_required=False,
            blockers=tuple(dict.fromkeys((*decision.blockers, mode_blocker))),
            authorized=False,
            reuse_mode=resolved,
        )
    allowed = (
        resolved is ReuseMode.PARTIAL
        and decision.kind in {ReuseKind.EXACT, ReuseKind.PARTIAL}
    ) or (resolved is ReuseMode.EXACT and decision.kind is ReuseKind.EXACT)
    authorized = bool(decision.eligible and allowed)
    blockers = decision.blockers
    if not authorized and decision.kind in {ReuseKind.EXACT, ReuseKind.PARTIAL}:
        blockers = tuple(dict.fromkeys((*blockers, mode_blocker)))
    return replace(
        decision,
        authorized=authorized,
        audit_required=bool(authorized and decision.audit_required),
        blockers=blockers,
        reuse_mode=resolved,
    )
@dataclass(frozen=True, slots=True)
class ReusePlanArtifact:
    """Immutable explanation of reuse decisions and the history snapshot they used."""
    history_revision: int
    input_fingerprint: str
    plan_fingerprint: str
    decisions: tuple[ReuseDecision, ...]
    reuse_mode: ReuseMode = ReuseMode.HINT
    metrics: ReuseMetrics = dataclass_field(default_factory=ReuseMetrics)
    def to_dict(self) -> dict[str, Any]:
        # Serialize every decision so a planner can audit why a mutant was or was not skipped.
        return {
            "schema_version": 2,
            "reuse_mode": self.reuse_mode.value,
            "history_revision": self.history_revision,
            "input_fingerprint": self.input_fingerprint,
            "plan_fingerprint": self.plan_fingerprint,
            "decisions": [item.to_dict() for item in self.decisions],
            "metrics": self.metrics.to_dict(),
        }
@dataclass(frozen=True, slots=True)
class KnowledgePage:
    """Bounded keyset page returned by Knowledge Plane queries."""
    rows: tuple[Mapping[str, Any], ...]
    next_cursor: str | None
    snapshot_revision: int
    schema_version: int = KNOWLEDGE_SCHEMA_VERSION
    def to_dict(self) -> dict[str, Any]:
        # Keep query responses JSON-compatible without exposing SQLite rows.
        return {
            "rows": [dict(row) for row in self.rows],
            "next_cursor": self.next_cursor,
            "snapshot_revision": self.snapshot_revision,
            "schema_version": self.schema_version,
        }
@dataclass(frozen=True, slots=True)
class KnowledgeRetentionPolicy:
    """Explicit raw-evidence retention policy used by maintenance jobs."""
    max_age_seconds: float
    batch_size: int = 500
    protected_campaign_ids: tuple[str, ...] = ()
    preserve_rollups: bool = True
    def __post_init__(self) -> None:
        # Reject policies that could accidentally delete an unbounded or negative scope.
        if self.max_age_seconds < 0:
            raise ValueError("max_age_seconds must not be negative")
        if self.batch_size < 1 or self.batch_size > 10_000:
            raise ValueError("batch_size must be between 1 and 10000")
@dataclass(frozen=True, slots=True)
class KnowledgeRetentionResult:
    """Counters from one bounded, transactional retention pass."""
    cutoff: str
    compacted_events: int
    compacted_observations: int
    skipped_protected: int
    skipped_referenced: int = 0
    compaction_id: str | None = None
    def to_dict(self) -> dict[str, Any]:
        # Expose maintenance counters for diagnostics and retry decisions.
        return {
            "cutoff": self.cutoff,
            "compacted_events": self.compacted_events,
            "compacted_observations": self.compacted_observations,
            "skipped_protected": self.skipped_protected,
            "skipped_referenced": self.skipped_referenced,
            "compaction_id": self.compaction_id,
        }
@dataclass(frozen=True, slots=True)
class ReuseAuditPolicy:
    """Deterministic fresh-execution sampling policy for reuse correctness audits."""
    exact_sample_rate: float = 0.0
    partial_sample_rate: float = 0.0
    minimum_samples_per_rule: int = 0
    new_rule_warmup_samples: int = 0
    random_seed: str = "theseus-reuse-audit-v1"
    def __post_init__(self) -> None:
        # Reject non-reproducible or unbounded audit policies before they reach the planner.
        if not 0.0 <= float(self.exact_sample_rate) <= 1.0:
            raise ValueError("exact_sample_rate must be between 0 and 1")
        if not 0.0 <= float(self.partial_sample_rate) <= 1.0:
            raise ValueError("partial_sample_rate must be between 0 and 1")
        if int(self.minimum_samples_per_rule) < 0 or int(self.new_rule_warmup_samples) < 0:
            raise ValueError("audit sample counts must not be negative")
def should_sample_reuse_audit(
    *,
    campaign_id: str,
    mutant_id: str,
    rule_id: str,
    kind: ReuseKind | str,
    policy: ReuseAuditPolicy,
) -> bool:
    # Choose audit candidates from a stable SHA-256 fraction rather than process-local randomness.
    rate = policy.exact_sample_rate if str(kind) in {ReuseKind.EXACT.value, str(ReuseKind.EXACT)} else policy.partial_sample_rate
    if rate <= 0.0:
        return False
    digest = hashlib.sha256(
        "\x1f".join((str(campaign_id), str(mutant_id), str(rule_id), policy.random_seed)).encode("utf-8")
    ).digest()
    fraction = int.from_bytes(digest[:8], "big") / float(1 << 64)
    return fraction < float(rate)
def compare_reuse_audit(
    expected: Mapping[str, Any],
    actual: Mapping[str, Any],
) -> tuple[str, ...]:
    # Compare semantic and per-test fresh evidence while separating infrastructure inconclusive results.
    actual_status = str(actual.get("semantic_result") or actual.get("status") or "")
    if actual_status in {"error", "failed", "timeout", "cancelled", "restore_error", "baseline_failed"}:
        return ("infrastructure_inconclusive",)
    mismatches: list[str] = []
    expected_status = str(expected.get("semantic_result") or expected.get("status") or "")
    if expected_status != actual_status:
        mismatches.append("semantic_mismatch")
    expected_observations = {
        str(item.get("test_id")): str(item.get("outcome"))
        for item in expected.get("test_observations", ())
        if isinstance(item, Mapping) and item.get("test_id")
    }
    actual_observations = {
        str(item.get("test_id")): str(item.get("outcome"))
        for item in actual.get("test_observations", ())
        if isinstance(item, Mapping) and item.get("test_id")
    }
    if expected_observations != actual_observations:
        mismatches.append("test_outcome_mismatch")
    for field, mismatch in (
        ("environment_fingerprint", "environment_mismatch"),
        ("selection_fingerprint", "selection_mismatch"),
        ("test_fingerprint", "dependency_mismatch"),
        ("conftest_fingerprint", "dependency_mismatch"),
    ):
        if expected.get(field) and actual.get(field) and str(expected[field]) != str(actual[field]):
            mismatches.append(mismatch)
    if bool(expected.get("restore_verified", False)) != bool(actual.get("restore_verified", False)):
        mismatches.append("restore_mismatch")
    return tuple(dict.fromkeys(mismatches))
def _canonical(value: Any) -> Any:
    # Normalize nested JSON values so fingerprints do not depend on mapping order.
    if isinstance(value, Mapping):
        return {str(key): _canonical(item) for key, item in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, (set, frozenset)):
        values = [_canonical(item) for item in value]
        return sorted(values, key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True))
    return value
def _normalized_ast(value: Any) -> Any:
    # Strip source locations from AST input while accepting already-normalized JSON trees.
    if isinstance(value, ast.AST):
        return ast.dump(value, annotate_fields=True, include_attributes=False)
    if isinstance(value, str):
        try:
            return ast.dump(ast.parse(textwrap.dedent(value)), annotate_fields=True, include_attributes=False)
        except SyntaxError:
            return value
    return _canonical(value)
def fingerprint_function(
    *,
    normalized_ast: Any,
    signature: Any = None,
    decorators: Sequence[Any] = (),
    dependency_closure: Sequence[Any] = (),
    python_version: str | None = None,
) -> str:
    # Hash the function identity inputs required by the E16 invalidation contract.
    return stable_hash(
        {
            "kind": "function",
            "normalized_ast": _normalized_ast(normalized_ast),
            "signature": _canonical(signature),
            "decorators": _canonical(tuple(decorators)),
            "dependency_closure": _canonical(tuple(dependency_closure)),
            "python_version": python_version or platform.python_version(),
        }
    )
def fingerprint_test(
    *,
    test_code: Any,
    fixtures: Sequence[Any] = (),
    conftest_closure: Sequence[Any] = (),
    pytest_configuration: Any = None,
    plugins: Sequence[Any] = (),
    data_dependencies: Sequence[Any] = (),
    environment_dependencies: Sequence[Any] = (),
) -> str:
    # Hash a test together with every pytest input that can change its behavior.
    return stable_hash(
        {
            "kind": "test",
            "test_code": _normalized_ast(test_code),
            "fixtures": _canonical(tuple(fixtures)),
            "conftest_closure": _canonical(tuple(conftest_closure)),
            "pytest_configuration": _canonical(pytest_configuration),
            "plugins": _canonical(tuple(plugins)),
            "data_dependencies": _canonical(tuple(data_dependencies)),
            "environment_dependencies": _canonical(tuple(environment_dependencies)),
        }
    )
def fingerprint_conftest(*, closure: Sequence[Any]) -> str:
    # Hash the ordered conftest closure separately so a changed fixture scope invalidates all dependents.
    return stable_hash({"kind": "conftest", "closure": _canonical(tuple(closure))})
def fingerprint_mutant(
    *,
    function_fingerprint: str,
    operator_id: str,
    operator_version: str,
    position: Any,
    replacement: Any,
) -> str:
    # Bind a mutant to its function, operator implementation and exact replacement.
    return stable_hash(
        {
            "kind": "mutant",
            "function_fingerprint": function_fingerprint,
            "operator_id": operator_id,
            "operator_version": operator_version,
            "position": _canonical(position),
            "replacement": _canonical(replacement),
        }
    )
def fingerprint_environment(
    *,
    python_version: str | None = None,
    dependencies: Sequence[Any] = (),
    pytest_version: str | None = None,
    plugins: Sequence[Any] = (),
    platform_name: str | None = None,
    environment_profile: Any = None,
    test_command: Sequence[Any] = (),
) -> str:
    # Hash runtime and command inputs so an old execution cannot cross environment boundaries.
    return stable_hash(
        {
            "kind": "environment",
            "python_version": python_version or platform.python_version(),
            "dependencies": _canonical(tuple(dependencies)),
            "pytest_version": pytest_version,
            "plugins": _canonical(tuple(plugins)),
            "platform": platform_name or sys.platform,
            "environment_profile": _canonical(environment_profile),
            "test_command": _canonical(tuple(test_command)),
        }
    )
def fingerprint_result(
    *,
    mutant_fingerprint: str,
    test_fingerprint: str,
    environment_fingerprint: str,
    selection_configuration: Any = None,
) -> str:
    # Hash the complete reuse boundary rather than relying on a repository commit alone.
    return stable_hash(
        {
            "kind": "result",
            "mutant_fingerprint": mutant_fingerprint,
            "test_fingerprint": test_fingerprint,
            "environment_fingerprint": environment_fingerprint,
            "selection_configuration": _canonical(selection_configuration),
        }
    )
def encode_cursor(created_at: str, event_id: str) -> str:
    # Encode a stable keyset cursor without exposing SQL syntax to callers.
    raw = json.dumps([created_at, event_id], ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
def decode_cursor(value: str) -> tuple[str, str]:
    # Validate and decode a query cursor before it reaches a SQL predicate.
    if not value:
        raise ValueError("cursor must not be empty")
    try:
        padded = value + "=" * (-len(value) % 4)
        raw = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
    except (ValueError, UnicodeError, binascii.Error, json.JSONDecodeError) as exc:
        raise ValueError("invalid knowledge cursor") from exc
    if not isinstance(raw, list) or len(raw) != 2 or not all(isinstance(item, str) and item for item in raw):
        raise ValueError("invalid knowledge cursor")
    return raw[0], raw[1]
__all__ = [
    "KnowledgePage",
    "KnowledgeRetentionPolicy",
    "KnowledgeRetentionResult",
    "EVIDENCE_IDENTITY_VERSION",
    "EvidenceIdentity",
    "ReuseMetrics",
    "ReuseDecision",
    "ReuseKind",
    "ReuseMode",
    "ReusePlanArtifact",
    "authorize_reuse_decision",
    "decode_cursor",
    "encode_cursor",
    "fingerprint_environment",
    "fingerprint_evidence",
    "fingerprint_evidence_identity",
    "fingerprint_conftest",
    "fingerprint_function",
    "fingerprint_mutant",
    "fingerprint_result",
    "fingerprint_test",
    "normalize_reuse_mode",
]
