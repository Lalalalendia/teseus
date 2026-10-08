"""Immutable campaign-planning DTOs shared by the planner and workers."""
from __future__ import annotations
import hashlib
import json
import math
from dataclasses import dataclass, field, replace
from typing import Any, Mapping
from .campaign import CampaignBudget
from .ids import CampaignId
from .mutation import MutantDescriptor
from .serialization import WireModel, optional_string, required_string, sequence_of_strings
from .workers import ShardDescriptor
@dataclass(frozen=True, slots=True)
class PlannedMutant(WireModel):
    """One selected mutant with a stable rank and an explainable decision."""
    mutant: MutantDescriptor
    rank: int
    priority: int
    estimated_seconds: float = 1.0
    reasons: tuple[str, ...] = ()
    def __post_init__(self) -> None:
        # Reject malformed plan rows before they can become an execution input.
        if self.rank < 0 or self.priority < 0 or self.estimated_seconds <= 0:
            raise ValueError("planned mutant rank, priority and cost must be valid")
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "PlannedMutant":
        # Restore one planner decision without importing scheduling internals.
        raw_mutant = value.get("mutant")
        if not isinstance(raw_mutant, Mapping):
            raise ValueError("planned mutant must contain a mutant object")
        return cls(
            mutant=MutantDescriptor.from_dict(raw_mutant),
            rank=max(0, int(value.get("rank", 0))),
            priority=max(0, int(value.get("priority", 0))),
            estimated_seconds=float(value.get("estimated_seconds", 1.0)),
            reasons=sequence_of_strings(value, "reasons"),
        )
@dataclass(frozen=True, slots=True)
class PlanDecision(WireModel):
    """Explain one planner action, including work that is intentionally skipped."""
    mutant_id: str
    action: str
    reason: str
    priority: int = 0
    estimated_seconds: float = 0.0
    audit_selected: bool = False
    source_event_id: str | None = None
    matched_test_ids: tuple[str, ...] = ()
    missing_test_ids: tuple[str, ...] = ()
    def __post_init__(self) -> None:
        # Canonicalize test partitions and reject an ambiguous planner action before execution.
        if not self.mutant_id.strip() or not self.action.strip() or not self.reason.strip():
            raise ValueError("plan decision identity and explanation must be non-empty")
        allowed_actions = {
            "audit",
            "budget_excluded",
            "deduplicate",
            "execute",
            "outside_scope",
            "partial_reuse",
            "reuse",
        }
        if self.action not in allowed_actions:
            raise ValueError(f"unsupported plan decision action: {self.action}")
        if self.priority < 0 or self.estimated_seconds < 0:
            raise ValueError("plan decision metrics must not be negative")
        matched = tuple(sorted(dict.fromkeys(str(item) for item in self.matched_test_ids if str(item))))
        missing = tuple(sorted(dict.fromkeys(str(item) for item in self.missing_test_ids if str(item))))
        overlap = set(matched).intersection(missing)
        if overlap:
            raise ValueError("plan test partitions must not overlap: " + ", ".join(sorted(overlap)))
        if self.action == "partial_reuse" and (not matched or not missing):
            raise ValueError("partial_reuse requires non-empty matched and missing test partitions")
        if self.action not in {"partial_reuse", "audit"} and (matched or missing):
            raise ValueError("only partial_reuse or audit may carry test partitions")
        if self.action in {"reuse", "partial_reuse", "audit"} and not self.source_event_id:
            raise ValueError(f"{self.action} requires source_event_id evidence")
        object.__setattr__(self, "matched_test_ids", matched)
        object.__setattr__(self, "missing_test_ids", missing)
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "PlanDecision":
        # Restore one explainable action while preserving future decision metadata.
        return cls(
            mutant_id=required_string(value, "mutant_id"),
            action=required_string(value, "action"),
            reason=required_string(value, "reason"),
            priority=max(0, int(value.get("priority", 0))),
            estimated_seconds=max(0.0, float(value.get("estimated_seconds", 0.0))),
            audit_selected=bool(value.get("audit_selected", False)),
            source_event_id=optional_string(value, "source_event_id"),
            matched_test_ids=sequence_of_strings(value, "matched_test_ids"),
            missing_test_ids=sequence_of_strings(value, "missing_test_ids"),
        )
@dataclass(frozen=True, slots=True)
class CampaignPlan(WireModel):
    """Frozen, explainable execution plan produced before any shard is claimed."""
    plan_id: str
    campaign_id: CampaignId
    algorithm_version: str
    source_sha256: str
    prepared_snapshot_id: str | None
    input_fingerprint: str
    policy: CampaignBudget
    candidate_count: int
    eligible_count: int
    selected_count: int
    estimated_wall_seconds: float
    estimated_cpu_seconds: float
    selected: tuple[PlannedMutant, ...] = ()
    excluded: tuple[Mapping[str, Any], ...] = ()
    shards: tuple[ShardDescriptor, ...] = ()
    created_at: str = ""
    decisions: tuple[PlanDecision, ...] = ()
    planning_fingerprint: str = ""
    test_order: tuple[str, ...] = ()
    levels: tuple[Mapping[str, Any], ...] = ()
    audit_sample: tuple[str, ...] = ()
    cost_model: Mapping[str, float] = field(default_factory=dict)
    adaptive_policy: Mapping[str, Any] = field(default_factory=dict)
    adaptive_snapshot: Mapping[str, Mapping[str, float]] = field(default_factory=dict)
    artifact_sha256: str = ""
    @property
    def estimated_seconds(self) -> float:
        # Preserve the deprecated public alias while making wall-clock semantics unambiguous.
        return self.estimated_wall_seconds
    @property
    def worker_count(self) -> int:
        # Expose the immutable planner-owned execution width instead of reading campaign budget at runtime.
        return max(1, int(self.cost_model.get("worker_count", max(1, len(self.shards)))))

    @property
    def scheduler_worker_count(self) -> int:
        # Expose bounded runtime capacity separately from immutable shard-unit cardinality.
        return max(1, int(self.cost_model.get("scheduler_worker_count", self.worker_count)))
    def to_dict(self) -> dict[str, Any]:
        # Serialize explicit CPU/wall metrics and retain the legacy wall-clock alias on the wire.
        payload = WireModel.to_dict(self)
        payload["estimated_seconds"] = self.estimated_wall_seconds
        return payload
    def __post_init__(self) -> None:
        # Enforce plan arithmetic, cost dimensions and immutable membership at the wire boundary.
        if not self.plan_id.strip() or not self.algorithm_version.strip() or not self.source_sha256.strip():
            raise ValueError("plan identity fields must be non-empty")
        if not str(self.prepared_snapshot_id or "").strip():
            raise ValueError("campaign plan requires prepared_snapshot_id")
        if min(self.candidate_count, self.eligible_count, self.selected_count) < 0:
            raise ValueError("plan counters must not be negative")
        if self.eligible_count > self.candidate_count or self.selected_count > self.eligible_count:
            raise ValueError("plan counters are inconsistent")
        if self.selected_count != len(self.selected):
            raise ValueError("selected_count does not match selected mutants")
        if self.estimated_wall_seconds < 0 or self.estimated_cpu_seconds < 0:
            raise ValueError("plan wall and CPU estimates must not be negative")
        model_wall = self.cost_model.get("wall_seconds")
        if model_wall is not None and not math.isclose(
            self.estimated_wall_seconds,
            float(model_wall),
            rel_tol=1e-12,
            abs_tol=1e-12,
        ):
            raise ValueError("plan estimated_wall_seconds conflicts with cost_model.wall_seconds")
        model_cpu = self.cost_model.get("cpu_seconds")
        if model_cpu is not None and not math.isclose(
            self.estimated_cpu_seconds,
            float(model_cpu),
            rel_tol=1e-12,
            abs_tol=1e-12,
        ):
            raise ValueError("plan estimated_cpu_seconds conflicts with cost_model.cpu_seconds")
        selected_ids = [item.mutant.mutant_id.value for item in self.selected]
        if len(selected_ids) != len(set(selected_ids)):
            raise ValueError("plan contains duplicate selected mutants")
        shard_ids = [item.shard_id.value for item in self.shards]
        if len(shard_ids) != len(set(shard_ids)):
            raise ValueError("plan contains duplicate shard IDs")
        if any(item.plan_id != self.plan_id for item in self.shards):
            raise ValueError("every shard descriptor must be bound to the owning plan_id")
        expected_worker_count = max(1, len(self.shards))
        if self.worker_count != expected_worker_count:
            raise ValueError("plan worker_count must match immutable shard topology")
        scheduler_worker_count = self.scheduler_worker_count
        if scheduler_worker_count > max(1, int(self.policy.max_workers)):
            raise ValueError("plan scheduler_worker_count exceeds campaign worker budget")
        if scheduler_worker_count < 1:
            raise ValueError("plan scheduler_worker_count must be positive")
        planned_ids = [item for shard in self.shards for item in shard.mutant_ids]
        if len(planned_ids) != len(set(planned_ids)) or set(planned_ids) != set(selected_ids):
            raise ValueError("shard membership must cover selected mutants exactly once")
        decision_ids = [item.mutant_id for item in self.decisions]
        if len(decision_ids) != len(set(decision_ids)):
            raise ValueError("plan contains duplicate decision identities")
        if len(decision_ids) != self.candidate_count:
            raise ValueError("candidate_count must match the complete decision ledger")
        selected_set = set(selected_ids)
        selected_actions = {"audit", "execute", "partial_reuse", "reuse"}
        for item in self.decisions:
            if item.mutant_id in selected_set and item.action not in selected_actions:
                raise ValueError("selected mutant has a non-executable plan decision")
            if item.mutant_id not in selected_set and item.action in selected_actions:
                raise ValueError("unselected mutant has an executable plan decision")
        if any(item not in selected_set for item in self.audit_sample):
            raise ValueError("audit sample contains a mutant outside the selected plan")
        if any(not math.isfinite(float(value)) or float(value) < 0 for value in self.cost_model.values()):
            raise ValueError("plan cost model values must be finite and non-negative")
        policy = dict(self.adaptive_policy)
        if policy:
            if int(policy.get("schema_version", 0)) != 1:
                raise ValueError("unsupported adaptive plan policy schema_version")
            if int(policy.get("shard_count", -1)) != len(self.shards):
                raise ValueError("adaptive policy shard_count must match immutable topology")
            for name in ("exploration_rate", "random_audit_rate"):
                rate = float(policy.get(name, 0.0))
                if not math.isfinite(rate) or rate < 0.0 or rate > 1.0:
                    raise ValueError(f"adaptive policy {name} must be between zero and one")
            timeout_multiplier = float(policy.get("timeout_multiplier", 1.0))
            if not math.isfinite(timeout_multiplier) or timeout_multiplier < 1.0:
                raise ValueError("adaptive policy timeout_multiplier must be at least one")
            test_timeout = float(policy.get("test_timeout_seconds", 0.0))
            if not math.isfinite(test_timeout) or test_timeout <= 0.0:
                raise ValueError("adaptive policy test_timeout_seconds must be positive")
            if not str(policy.get("adaptive_seed", "")).strip():
                raise ValueError("adaptive policy adaptive_seed must be non-empty")
            if policy.get("survivor_first") is not True:
                raise ValueError("adaptive policy must preserve survivor-first prioritization")
            target = float(policy.get("target_shard_seconds", 0.0))
            if not math.isfinite(target) or target < 0.0:
                raise ValueError("adaptive policy target_shard_seconds must be non-negative")
            if policy.get("stop_on_kill") is not True:
                raise ValueError("adaptive policy must preserve correctness by stopping only after kill")
            raw_order = policy.get("escalation_order", ())
            if not isinstance(raw_order, (list, tuple)):
                raise ValueError("adaptive policy escalation_order must be an array")
            escalation_order = tuple(str(item) for item in raw_order)
            canonical_order = ("selected", "domain", "full")
            if escalation_order != canonical_order[:len(escalation_order)]:
                raise ValueError("adaptive policy escalation_order must be selected, domain, full")
        snapshot: dict[str, dict[str, float]] = {}
        for mutant_id, raw in self.adaptive_snapshot.items():
            if not str(mutant_id).strip() or not isinstance(raw, Mapping):
                raise ValueError("adaptive snapshot rows require mutant identities and objects")
            row: dict[str, float] = {}
            for name, value in raw.items():
                number = float(value)
                if not math.isfinite(number) or number < 0.0:
                    raise ValueError("adaptive snapshot values must be finite and non-negative")
                if (
                    name in {
                        "kill_probability",
                        "selection_confidence",
                        "operator_effectiveness",
                        "flaky_risk",
                        "infrastructure_risk",
                    }
                    and number > 1.0
                ):
                    raise ValueError(f"adaptive snapshot {name} must not exceed one")
                row[str(name)] = number
            snapshot[str(mutant_id)] = row
        if set(snapshot) - set(decision_ids):
            raise ValueError("adaptive snapshot contains a mutant outside the decision ledger")
        object.__setattr__(self, "adaptive_policy", policy)
        object.__setattr__(self, "adaptive_snapshot", snapshot)
        if self.artifact_sha256 and self.artifact_sha256 != self._computed_artifact_sha256():
            raise ValueError("campaign plan artifact integrity check failed")
    def _computed_artifact_sha256(self) -> str:
        # Hash the complete serialized plan except its self-referential integrity field.
        payload = self._canonical_json(self.to_dict())
        payload.pop("artifact_sha256", None)
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()
    @staticmethod
    def _canonical_json(value: Any) -> Any:
        # Normalize integer/float spellings so JSON round-trips preserve the artifact hash.
        if isinstance(value, Mapping):
            return {str(key): CampaignPlan._canonical_json(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [CampaignPlan._canonical_json(item) for item in value]
        if isinstance(value, float):
            return float(value)
        if isinstance(value, int) and not isinstance(value, bool):
            return float(value)
        return value
    @classmethod
    def _serialized_artifact_sha256(cls, value: Mapping[str, Any]) -> str:
        # Recompute integrity from the serialized shape so legacy plans can be verified before migration.
        payload = cls._canonical_json(dict(value))
        payload.pop("artifact_sha256", None)
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()
    def verify_integrity(self) -> None:
        # Require a content hash when a plan is loaded from durable storage.
        if not self.artifact_sha256 or self.artifact_sha256 != self._computed_artifact_sha256():
            raise ValueError("campaign plan artifact is missing or has invalid integrity hash")
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "CampaignPlan":
        # Verify the serialized artifact first, then migrate legacy estimated_seconds into explicit dimensions.
        serialized_hash = str(value.get("artifact_sha256", ""))
        if serialized_hash and serialized_hash != cls._serialized_artifact_sha256(value):
            raise ValueError("campaign plan artifact integrity check failed")
        raw_policy = value.get("policy", {})
        raw_selected = value.get("selected", [])
        raw_excluded = value.get("excluded", [])
        raw_shards = value.get("shards", [])
        raw_decisions = value.get("decisions", [])
        raw_levels = value.get("levels", [])
        raw_cost_model = value.get("cost_model", {})
        raw_adaptive_policy = value.get("adaptive_policy", {})
        raw_adaptive_snapshot = value.get("adaptive_snapshot", {})
        if not isinstance(raw_policy, Mapping) or not isinstance(raw_selected, (list, tuple)):
            raise ValueError("policy and selected must be objects/arrays")
        if (
            not isinstance(raw_excluded, (list, tuple))
            or not isinstance(raw_shards, (list, tuple))
            or not isinstance(raw_decisions, (list, tuple))
            or not isinstance(raw_levels, (list, tuple))
            or not isinstance(raw_cost_model, Mapping)
            or not isinstance(raw_adaptive_policy, Mapping)
            or not isinstance(raw_adaptive_snapshot, Mapping)
            or any(not isinstance(row, Mapping) for row in raw_adaptive_snapshot.values())
        ):
            raise ValueError("excluded, shards, decisions, levels, cost_model and adaptive fields have invalid types")
        legacy_seconds = max(0.0, float(value.get("estimated_seconds", 0.0)))
        wall_seconds = max(
            0.0,
            float(value.get("estimated_wall_seconds", raw_cost_model.get("wall_seconds", legacy_seconds))),
        )
        cpu_seconds = max(
            0.0,
            float(value.get("estimated_cpu_seconds", raw_cost_model.get("cpu_seconds", legacy_seconds))),
        )
        plan_id = required_string(value, "plan_id")
        parsed_shards = tuple(
            ShardDescriptor.from_dict(item)
            for item in raw_shards
            if isinstance(item, Mapping)
        )
        bound_shards = tuple(
            item if item.plan_id == plan_id else replace(item, plan_id=plan_id)
            for item in parsed_shards
        )
        normalized_cost_model = {str(key): float(item) for key, item in raw_cost_model.items()}
        normalized_cost_model["worker_count"] = float(max(1, len(bound_shards)))
        normalized_cost_model["scheduler_worker_count"] = float(
            max(1, int(normalized_cost_model.get("scheduler_worker_count", len(bound_shards))))
        )
        plan = cls(
            plan_id=plan_id,
            campaign_id=CampaignId(required_string(value, "campaign_id")),
            algorithm_version=required_string(value, "algorithm_version"),
            source_sha256=required_string(value, "source_sha256"),
            prepared_snapshot_id=optional_string(value, "prepared_snapshot_id"),
            input_fingerprint=required_string(value, "input_fingerprint"),
            policy=CampaignBudget.from_dict(raw_policy),
            candidate_count=max(0, int(value.get("candidate_count", 0))),
            eligible_count=max(0, int(value.get("eligible_count", 0))),
            selected_count=max(0, int(value.get("selected_count", 0))),
            estimated_wall_seconds=wall_seconds,
            estimated_cpu_seconds=cpu_seconds,
            selected=tuple(
                PlannedMutant.from_dict(item)
                for item in raw_selected
                if isinstance(item, Mapping)
            ),
            excluded=tuple(dict(item) for item in raw_excluded if isinstance(item, Mapping)),
            shards=bound_shards,
            created_at=str(value.get("created_at", "")),
            decisions=tuple(
                PlanDecision.from_dict(item)
                for item in raw_decisions
                if isinstance(item, Mapping)
            ),
            planning_fingerprint=str(value.get("planning_fingerprint", "")),
            test_order=sequence_of_strings(value, "test_order"),
            levels=tuple(dict(item) for item in raw_levels if isinstance(item, Mapping)),
            audit_sample=sequence_of_strings(value, "audit_sample"),
            cost_model=normalized_cost_model,
            adaptive_policy=dict(raw_adaptive_policy),
            adaptive_snapshot={
                str(mutant_id): {str(name): float(item) for name, item in row.items()}
                for mutant_id, row in raw_adaptive_snapshot.items()
                if isinstance(row, Mapping)
            },
            artifact_sha256="",
        )
        return replace(plan, artifact_sha256=plan._computed_artifact_sha256()) if serialized_hash else plan
__all__ = ["CampaignPlan", "PlanDecision", "PlannedMutant"]
