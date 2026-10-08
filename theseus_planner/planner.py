"""Pure deterministic campaign planning over immutable mutation descriptors."""
from __future__ import annotations
import hashlib
import json
import math
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignPlan,
    MutantDescriptor,
    PlanDecision,
    PlannedMutant,
    PreparedCampaign,
    ReuseMode,
    ShardDescriptor,
    ShardId,
    normalize_reuse_mode,
)
class PlanError(ValueError):
    """Raised when a campaign input cannot produce a safe immutable plan."""
@dataclass(frozen=True, slots=True)
class _Candidate:
    """Internal candidate row kept outside the public plan contract."""
    mutant: MutantDescriptor
    priority: int
    reasons: tuple[str, ...]
    function_key: str
    estimated_seconds: float = 0.0
    exploration_selected: bool = False
class CampaignPlanner:
    """Build deterministic budgeted mutant plans and balanced shard membership."""
    ALGORITHM_VERSION = "campaign-planner-v2"
    ESTIMATED_MUTANT_SECONDS = 1.0
    OPERATOR_COST_WEIGHTS = {
        "return_value_to_none": 1.35,
        "condition_to_not": 1.25,
        "remove_standalone_call": 1.45,
        "raise_to_pass": 1.10,
        "empty_list_to_none": 1.05,
        "empty_dict_to_sentinel": 1.05,
        "await_to_expression": 1.30,
    }
    def build(
        self,
        configuration: CampaignConfiguration,
        prepared: PreparedCampaign,
        *,
        mutants: Sequence[MutantDescriptor] | None = None,
        reuse_decisions: Mapping[str, Any] | None = None,
        audit_policy: Any | None = None,
        cost_observations: Mapping[str, float] | None = None,
        adaptive_observations: Mapping[str, Mapping[str, object]] | None = None,
    ) -> CampaignPlan:
        # Convert the immutable prepared catalog and frozen history snapshot into one adaptive plan.
        catalog = tuple(mutants if mutants is not None else prepared.mutants)
        if prepared.campaign_id != configuration.campaign_id:
            raise PlanError("prepared campaign does not match configuration")
        if not str(prepared.snapshot_id or "").strip():
            raise PlanError("prepared campaign requires immutable snapshot_id")
        if not catalog and configuration.scope.mutant_ids:
            raise PlanError("explicit mutant_ids were requested but the prepared catalog is empty")
        self._validate_budget(configuration.budget)
        reuse_mode = normalize_reuse_mode(configuration.reuse_mode)
        adaptive_snapshot = self._adaptive_snapshot(catalog, adaptive_observations)
        costs = self._cost_snapshot(catalog, cost_observations, adaptive_snapshot)
        candidates, scope_excluded = self._candidates(
            configuration,
            catalog,
            prepared_function_id=prepared.function_id,
            prepared_class_name=prepared.class_name,
        )
        candidates = self._adaptive_candidates(
            configuration,
            candidates,
            adaptive_snapshot=adaptive_snapshot,
            costs=costs,
        )
        ordered = sorted(candidates, key=self._ordering_key)
        selected, budget_excluded = self._apply_budget(
            configuration.budget,
            ordered,
            cost_observations=costs,
        )
        excluded = tuple((*scope_excluded, *budget_excluded))
        selected_rows = tuple(
            PlannedMutant(
                mutant=item.mutant,
                rank=index,
                priority=item.priority,
                estimated_seconds=self._estimated_cost(item, costs),
                reasons=item.reasons,
            )
            for index, item in enumerate(selected)
        )
        unbound_shards = self._shards(
            selected_rows,
            configuration.budget.max_workers,
            target_shard_seconds=configuration.budget.target_shard_seconds,
        )
        decisions = self._decisions(
            catalog,
            ordered,
            selected,
            excluded,
            reuse_decisions,
            campaign_id=configuration.campaign_id.value,
            reuse_mode=reuse_mode,
            audit_policy=audit_policy,
            estimated_costs=costs,
            random_audit_rate=configuration.budget.random_audit_rate,
            adaptive_seed=configuration.budget.adaptive_seed,
        )
        test_order, levels, escalation_order = self._test_plan(
            prepared,
            no_escalation=configuration.no_escalation,
            test_timeout_seconds=configuration.budget.max_test_seconds,
        )
        audit_sample = tuple(item.mutant_id for item in decisions if item.audit_selected)
        selected_cpu_seconds = sum(item.estimated_seconds for item in selected_rows)
        fallback_costs = tuple(
            item.estimated_seconds
            for item in selected_rows
            if item.mutant.mutant_id.value in audit_sample
        )
        fallback_cpu_seconds = sum(fallback_costs)
        worker_count = max(1, len(unbound_shards))
        scheduler_worker_count = max(
            1,
            min(max(1, int(configuration.budget.max_workers)), len(selected_rows)),
        )
        selected_wall_seconds = self._weighted_wall_seconds(
            tuple(item.estimated_seconds for item in selected_rows),
            scheduler_worker_count,
        )
        fallback_wall_seconds = self._weighted_wall_seconds(fallback_costs, scheduler_worker_count)
        estimated_cpu_seconds = selected_cpu_seconds + fallback_cpu_seconds
        estimated_wall_seconds = selected_wall_seconds + fallback_wall_seconds
        selected_ids = {item.mutant.mutant_id.value for item in selected_rows}
        expected_kills = sum(
            float(adaptive_snapshot.get(mutant_id, {}).get("kill_probability", 0.0))
            for mutant_id in selected_ids
        )
        exploration_count = sum(int(item.exploration_selected) for item in selected)
        cost_model = {
            "prepare_seconds": 0.0,
            "selected_test_seconds": selected_cpu_seconds,
            "selected_cpu_seconds": selected_cpu_seconds,
            "expected_fallback_seconds": fallback_cpu_seconds,
            "expected_fallback_cpu_seconds": fallback_cpu_seconds,
            "total_seconds": estimated_wall_seconds,
            "total_cpu_seconds": estimated_cpu_seconds,
            "total_wall_seconds": estimated_wall_seconds,
            "cpu_seconds": estimated_cpu_seconds,
            "wall_seconds": estimated_wall_seconds,
            "selected_wall_seconds": selected_wall_seconds,
            "fallback_wall_seconds": fallback_wall_seconds,
            "worker_count": float(worker_count),
            "scheduler_worker_count": float(scheduler_worker_count),
            "expected_kills": expected_kills,
            "historical_mutants": float(len(adaptive_snapshot)),
            "exploration_mutants": float(exploration_count),
        }
        adaptive_policy = {
            "schema_version": 1,
            "shard_count": len(unbound_shards),
            "target_shard_seconds": float(configuration.budget.target_shard_seconds or 0.0),
            "escalation_order": list(escalation_order),
            "test_timeout_seconds": float(configuration.budget.max_test_seconds or 120.0),
            "timeout_multiplier": float(configuration.budget.adaptive_timeout_multiplier),
            "stop_on_kill": True,
            "survivor_first": True,
            "exploration_rate": float(configuration.budget.exploration_rate),
            "random_audit_rate": float(configuration.budget.random_audit_rate),
            "scheduler_worker_count": scheduler_worker_count,
            "adaptive_seed": configuration.budget.adaptive_seed,
        }
        planning_payload = self._planning_payload(
            configuration,
            prepared,
            catalog,
            selected=selected_rows,
            excluded=excluded,
            test_order=test_order,
            levels=levels,
            audit_policy=audit_policy,
            adaptive_policy=adaptive_policy,
            adaptive_snapshot=adaptive_snapshot,
        )
        planning_fingerprint = self._digest(planning_payload)
        input_payload = {
            **planning_payload,
            "decisions": [item.to_dict() for item in decisions],
            "reuse_decisions": self._json_value(reuse_decisions or {}),
        }
        input_fingerprint = self._digest(input_payload)
        plan_id = f"plan-{input_fingerprint[:32]}"
        shards = tuple(replace(item, plan_id=plan_id) for item in unbound_shards)
        adaptive_policy = {**adaptive_policy, "shard_count": len(shards)}
        plan = CampaignPlan(
            plan_id=plan_id,
            campaign_id=configuration.campaign_id,
            algorithm_version=self.ALGORITHM_VERSION,
            source_sha256=prepared.source_sha256,
            prepared_snapshot_id=prepared.snapshot_id,
            input_fingerprint=input_fingerprint,
            policy=configuration.budget,
            candidate_count=len({item.mutant_id.value for item in catalog}),
            eligible_count=len(ordered),
            selected_count=len(selected_rows),
            estimated_wall_seconds=estimated_wall_seconds,
            estimated_cpu_seconds=estimated_cpu_seconds,
            selected=selected_rows,
            excluded=tuple(excluded),
            shards=shards,
            created_at="",
            decisions=decisions,
            planning_fingerprint=planning_fingerprint,
            test_order=test_order,
            levels=levels,
            audit_sample=audit_sample,
            cost_model=cost_model,
            adaptive_policy=adaptive_policy,
            adaptive_snapshot=adaptive_snapshot,
        )
        plan_content_hash = plan._computed_artifact_sha256()
        return replace(plan, artifact_sha256=plan_content_hash)
    def validate_resume(
        self,
        plan: CampaignPlan,
        configuration: CampaignConfiguration,
        prepared: PreparedCampaign,
        *,
        mutants: Sequence[MutantDescriptor] | None = None,
        reuse_decisions: Mapping[str, Any] | None = None,
        audit_policy: Any | None = None,
    ) -> CampaignPlan:
        # Validate current immutable inputs against the frozen plan without rereading mutable history.
        catalog = tuple(mutants if mutants is not None else prepared.mutants)
        if plan.algorithm_version != self.ALGORITHM_VERSION:
            raise PlanError(
                "stored campaign plan uses another planner algorithm version: "
                f"stored={plan.algorithm_version}; current={self.ALGORITHM_VERSION}"
            )
        if prepared.campaign_id != configuration.campaign_id or plan.campaign_id != configuration.campaign_id:
            raise PlanError("stored campaign plan belongs to another campaign")
        planning_payload = self._planning_payload(
            configuration,
            prepared,
            catalog,
            selected=plan.selected,
            excluded=plan.excluded,
            test_order=plan.test_order,
            levels=plan.levels,
            audit_policy=audit_policy,
            adaptive_policy=plan.adaptive_policy,
            adaptive_snapshot=plan.adaptive_snapshot,
        )
        planning_fingerprint = self._digest(planning_payload)
        if planning_fingerprint != plan.planning_fingerprint:
            raise PlanError(
                "stored campaign plan conflicts with immutable planning inputs: "
                f"stored={plan.planning_fingerprint}; current={planning_fingerprint}"
            )
        input_payload = {
            **planning_payload,
            "decisions": [item.to_dict() for item in plan.decisions],
            "reuse_decisions": self._json_value(reuse_decisions or {}),
        }
        input_fingerprint = self._digest(input_payload)
        if input_fingerprint != plan.input_fingerprint:
            raise PlanError(
                "stored campaign plan conflicts with immutable reuse inputs: "
                f"stored={plan.input_fingerprint}; current={input_fingerprint}"
            )
        return plan
    @classmethod
    def _planning_payload(
        cls,
        configuration: CampaignConfiguration,
        prepared: PreparedCampaign,
        catalog: Sequence[MutantDescriptor],
        *,
        selected: Sequence[PlannedMutant],
        excluded: Sequence[Mapping[str, Any]],
        test_order: Sequence[str],
        levels: Sequence[Mapping[str, Any]],
        audit_policy: Any | None,
        adaptive_policy: Mapping[str, Any],
        adaptive_snapshot: Mapping[str, Mapping[str, float]],
    ) -> dict[str, Any]:
        # Build the canonical planning identity from immutable inputs and frozen adaptive evidence.
        return {
            "algorithm_version": cls.ALGORITHM_VERSION,
            "campaign_id": configuration.campaign_id.value,
            "source_sha256": prepared.source_sha256,
            "prepared_snapshot_id": prepared.snapshot_id,
            "configuration": cls._semantic_configuration(configuration),
            "catalog": [
                item.to_dict()
                for item in sorted(
                    catalog,
                    key=lambda item: (
                        item.source_path,
                        item.line_no,
                        item.column_no,
                        item.mutation,
                        item.mutant_id.value,
                    ),
                )
            ],
            "selected_ids": [item.mutant.mutant_id.value for item in selected],
            "estimated_costs": {
                item.mutant.mutant_id.value: item.estimated_seconds for item in selected
            },
            "excluded": tuple(dict(item) for item in excluded),
            "test_order": list(test_order),
            "levels": [dict(item) for item in levels],
            "audit_policy": cls._json_value(audit_policy) if audit_policy is not None else None,
            "adaptive_policy": cls._json_value(adaptive_policy),
            "adaptive_snapshot": cls._json_value(adaptive_snapshot),
        }
    @staticmethod
    def _semantic_configuration(configuration: CampaignConfiguration) -> dict[str, Any]:
        # Exclude checkout and report locations so moving a project does not change plan identity.
        project = configuration.project
        return {
            "campaign_id": configuration.campaign_id.value,
            "project": {
                "project_id": project.project_id.value,
                "revision": project.revision.to_dict() if project.revision else None,
                "environment": project.environment.to_dict() if project.environment else None,
                "test_command": {
                    "argv": list(project.test_command.argv),
                    "shell": project.test_command.shell,
                } if project.test_command else None,
                "data_dependency_globs": list(project.data_dependency_globs),
                "data_dependency_exclude_globs": list(project.data_dependency_exclude_globs),
                "pytest_plugin_autoload": project.pytest_plugin_autoload,
            },
            "scope": configuration.scope.to_dict(),
            "budget": configuration.budget.to_dict(),
            "mode": str(configuration.mode),
            "configuration_fingerprint": configuration.configuration_fingerprint,
            "no_escalation": configuration.no_escalation,
            "reuse_mode": normalize_reuse_mode(configuration.reuse_mode).value,
        }
    @staticmethod
    def _validate_budget(budget: CampaignBudget) -> None:
        # Reject malformed adaptive limits instead of silently changing planner semantics.
        for name in (
            "max_mutants",
            "max_mutants_per_function",
            "max_mutants_per_operator",
        ):
            value = getattr(budget, name)
            if value is not None and value < 0:
                raise PlanError(f"{name} must be non-negative")
        for name in ("max_seconds", "max_cpu_seconds", "max_test_seconds"):
            value = getattr(budget, name)
            if value is not None and value < 0:
                raise PlanError(f"{name} must be non-negative")
        if budget.target_shard_seconds is not None and budget.target_shard_seconds <= 0.0:
            raise PlanError("target_shard_seconds must be positive")
        for name in ("exploration_rate", "random_audit_rate"):
            value = float(getattr(budget, name))
            if not math.isfinite(value) or value < 0.0 or value > 1.0:
                raise PlanError(f"{name} must be between zero and one")
        if not math.isfinite(float(budget.adaptive_timeout_multiplier)) or budget.adaptive_timeout_multiplier < 1.0:
            raise PlanError("adaptive_timeout_multiplier must be at least one")
        if not str(budget.adaptive_seed).strip():
            raise PlanError("adaptive_seed must be non-empty")
        if budget.max_workers < 1:
            raise PlanError("max_workers must be positive")
        if float(budget.lease_seconds) <= 0.0:
            raise PlanError("lease_seconds must be positive")
    @staticmethod
    def _canonical_path(root: Path, value: str) -> str:
        # Normalize absolute and relative source paths to one project-relative comparison value.
        candidate = Path(value)
        try:
            if candidate.is_absolute():
                candidate = candidate.resolve().relative_to(root.resolve())
        except (OSError, ValueError, RuntimeError):
            candidate = Path(value)
        return candidate.as_posix().lstrip("./")
    @staticmethod
    def _bounded_probability(value: Any, default: float) -> float:
        # Normalize one optional probability without accepting NaN, infinity or out-of-range values.
        if value is None or isinstance(value, bool):
            return default
        try:
            number = float(value)
        except (TypeError, ValueError):
            return default
        if not math.isfinite(number):
            return default
        return min(1.0, max(0.0, number))
    @staticmethod
    def _bounded_non_negative(value: Any, default: float = 0.0) -> float:
        # Treat corrupt or non-finite history as an absent performance hint.
        if value is None or isinstance(value, bool):
            return default
        try:
            number = float(value)
        except (TypeError, ValueError):
            return default
        return number if math.isfinite(number) and number >= 0.0 else default

    @classmethod
    def _adaptive_snapshot(
        cls,
        catalog: Sequence[MutantDescriptor],
        observations: Mapping[str, Mapping[str, object]] | None,
    ) -> dict[str, dict[str, object]]:
        # Freeze only bounded historical metrics for mutants present in the immutable catalog.
        source = observations or {}
        snapshot: dict[str, dict[str, object]] = {}
        for mutant_id in sorted({item.mutant_id.value for item in catalog}):
            raw = source.get(mutant_id)
            if not isinstance(raw, Mapping):
                continue
            killed = cls._bounded_non_negative(raw.get("killed_count"))
            survived = cls._bounded_non_negative(raw.get("survived_count"))
            invalid = cls._bounded_non_negative(raw.get("invalid_count"))
            sample_count = cls._bounded_non_negative(raw.get("sample_count"), killed + survived + invalid)
            terminal = killed + survived
            kill_probability = cls._bounded_probability(
                raw.get("kill_probability"),
                killed / terminal if terminal > 0.0 else 0.5,
            )
            duration_seconds = raw.get("duration_seconds")
            if duration_seconds is None and raw.get("duration_ms") is not None:
                duration_seconds = cls._bounded_non_negative(raw.get("duration_ms")) / 1000.0
            duration = cls._bounded_non_negative(duration_seconds)
            duration_p95 = cls._bounded_non_negative(raw.get("duration_p95_seconds"), duration)
            duration_sample_count = cls._bounded_non_negative(raw.get("duration_sample_count"))
            subset_cost = cls._bounded_non_negative(raw.get("test_subset_cost_seconds"))
            subset_count = cls._bounded_non_negative(raw.get("test_subset_count"))
            cost_observation_count = cls._bounded_non_negative(raw.get("cost_observation_count"))
            runtime_class_sample_count = cls._bounded_non_negative(raw.get("runtime_class_sample_count"))
            row = {
                "duration_p95_seconds": round(duration_p95, 9),
                "duration_sample_count": round(duration_sample_count, 9),
                "duration_seconds": round(duration, 9),
                "test_subset_cost_seconds": round(subset_cost, 9),
                "test_subset_count": round(subset_count, 9),
                "cost_observation_count": round(cost_observation_count, 9),
                "runtime_class_sample_count": round(runtime_class_sample_count, 9),
                "kill_probability": round(kill_probability, 9),
                "selection_confidence": round(
                    cls._bounded_probability(raw.get("selection_confidence"), min(1.0, sample_count / 5.0)),
                    9,
                ),
                "operator_effectiveness": round(
                    cls._bounded_probability(raw.get("operator_effectiveness"), kill_probability),
                    9,
                ),
                "flaky_risk": round(cls._bounded_probability(raw.get("flaky_risk"), 0.0), 9),
                "infrastructure_risk": round(
                    cls._bounded_probability(raw.get("infrastructure_risk"), 0.0),
                    9,
                ),
                "sample_count": round(sample_count, 9),
                "killed_count": round(killed, 9),
                "survived_count": round(survived, 9),
                "invalid_count": round(invalid, 9),
            }
            if any(isinstance(value, (int, float)) and value > 0.0 for name, value in row.items() if name != "kill_probability") or "kill_probability" in raw:
                snapshot[mutant_id] = row
        return snapshot
    @classmethod
    def _cost_snapshot(
        cls,
        catalog: Sequence[MutantDescriptor],
        observations: Mapping[str, float] | None,
        adaptive_snapshot: Mapping[str, Mapping[str, object]],
    ) -> dict[str, float]:
        # Prefer explicit costs and otherwise reuse the frozen historical duration for each catalog mutant.
        catalog_ids = {item.mutant_id.value for item in catalog}
        costs: dict[str, float] = {}
        for key, value in (observations or {}).items():
            if str(key) not in catalog_ids:
                continue
            number = cls._bounded_non_negative(value, -1.0)
            if number >= 0.0:
                costs[str(key)] = max(0.001, number)
        for mutant_id, row in adaptive_snapshot.items():
            duration = cls._bounded_non_negative(row.get("duration_seconds"))
            p95 = cls._bounded_non_negative(row.get("duration_p95_seconds"))
            subset_cost = cls._bounded_non_negative(row.get("test_subset_cost_seconds"))
            sample_count = cls._bounded_non_negative(row.get("cost_observation_count"))
            if sample_count > 0.0 and p95 > 0.0:
                duration = max(duration, p95)
            if duration <= 0.0 and subset_cost > 0.0:
                duration = subset_cost
            if mutant_id not in costs and duration > 0.0:
                costs[mutant_id] = max(0.001, duration)
        return costs
    @staticmethod
    def _deterministic_sample(
        campaign_id: str,
        mutant_id: str,
        *,
        purpose: str,
        rate: float,
        seed: str,
    ) -> bool:
        # Select one stable campaign-scoped sample without process randomness or input-order dependence.
        if rate <= 0.0:
            return False
        if rate >= 1.0:
            return True
        digest = hashlib.sha256(
            "\x1f".join((seed, purpose, campaign_id, mutant_id)).encode("utf-8")
        ).digest()
        return int.from_bytes(digest[:8], "big") / float(1 << 64) < rate
    def _adaptive_candidates(
        self,
        configuration: CampaignConfiguration,
        candidates: Sequence[_Candidate],
        *,
        adaptive_snapshot: Mapping[str, Mapping[str, float]],
        costs: Mapping[str, float],
    ) -> tuple[_Candidate, ...]:
        # Rank known survivors early while reserving deterministic exploration for mutants without history.
        rows: list[_Candidate] = []
        budget = configuration.budget
        for item in candidates:
            mutant_id = item.mutant.mutant_id.value
            observation = adaptive_snapshot.get(mutant_id)
            cost = float(costs.get(mutant_id, 0.0))
            reasons = list(item.reasons)
            priority = item.priority
            exploration_selected = False
            if observation is not None and float(observation.get("sample_count", 0.0)) > 0.0:
                sample_count = float(observation.get("sample_count", 0.0))
                history_weight = min(1.0, sample_count / 5.0)
                kill_probability = float(observation.get("kill_probability", 0.5))
                survivor_score = (1.0 - kill_probability) * history_weight
                confidence = float(observation.get("selection_confidence", 0.0))
                effectiveness = float(observation.get("operator_effectiveness", kill_probability))
                risk = max(
                    float(observation.get("flaky_risk", 0.0)),
                    float(observation.get("infrastructure_risk", 0.0)),
                )
                exploitation_score = min(
                    10.0,
                    kill_probability * effectiveness * confidence * (1.0 - risk) / max(0.001, cost or 1.0),
                )
                priority += int(round((survivor_score * 1000.0) + (exploitation_score * 100.0)))
                reasons.append("adaptive:historical-survivor" if survivor_score >= exploitation_score else "adaptive:expected-kill")
            else:
                exploration_selected = self._deterministic_sample(
                    configuration.campaign_id.value,
                    mutant_id,
                    purpose="exploration",
                    rate=float(budget.exploration_rate),
                    seed=budget.adaptive_seed,
                )
                if exploration_selected:
                    priority += 1500
                    reasons.append("adaptive:exploration")
            rows.append(
                replace(
                    item,
                    priority=priority,
                    reasons=tuple(reasons),
                    estimated_seconds=cost,
                    exploration_selected=exploration_selected,
                )
            )
        return tuple(rows)
    def _candidates(
        self,
        configuration: CampaignConfiguration,
        catalog: Iterable[MutantDescriptor],
        *,
        prepared_function_id: str | None,
        prepared_class_name: str | None,
    ) -> tuple[tuple[_Candidate, ...], tuple[dict[str, Any], ...]]:
        # Apply every scope predicate and retain a reason for each candidate rejected by the scope.
        root = Path(configuration.project.root_path).expanduser()
        scope = configuration.scope
        scope_kind = scope.scope_kind
        if scope_kind not in {"project", "directory", "file", "class", "function", "git_diff", "explicit"}:
            raise PlanError(f"unsupported scope_kind: {scope_kind}")
        if scope_kind == "function" and not scope.function:
            raise PlanError("function scope requires scope.function")
        if scope_kind == "class" and not scope.class_name:
            raise PlanError("class scope requires scope.class_name")
        if scope_kind == "explicit" and not scope.mutant_ids:
            raise PlanError("explicit scope requires mutant_ids")
        explicit_ids = set(scope.mutant_ids)
        operators = set(scope.operators)
        requested_path = self._canonical_path(root, scope.source_path)
        diff_paths = {
            self._canonical_path(root, item)
            for item in scope.git_diff_paths
        }
        seen: set[str] = set()
        catalog_ids = {mutant.mutant_id.value for mutant in catalog}
        missing_explicit = sorted(explicit_ids - catalog_ids)
        if missing_explicit:
            raise PlanError("explicit scope contains unknown mutant IDs: " + ", ".join(missing_explicit))
        candidates: list[_Candidate] = []
        excluded: list[dict[str, Any]] = []
        for mutant in catalog:
            mutant_id = mutant.mutant_id.value
            if mutant_id in seen:
                excluded.append({"mutant_id": mutant_id, "reason": "deduplicate"})
                continue
            seen.add(mutant_id)
            reasons: list[str] = []
            if explicit_ids:
                if mutant_id not in explicit_ids:
                    excluded.append({"mutant_id": mutant_id, "reason": "scope:explicit-mutant-id"})
                    continue
                reasons.append("explicit-mutant-id")
            mutant_path = self._canonical_path(root, mutant.source_path)
            if scope_kind in {"project", "explicit"}:
                source_matches = True
            elif scope_kind == "directory":
                directory_prefix = requested_path.rstrip("/")
                source_matches = mutant_path == directory_prefix or mutant_path.startswith(directory_prefix + "/")
            elif scope_kind == "git_diff":
                source_matches = mutant_path in (diff_paths or {requested_path})
            else:
                source_matches = mutant_path == requested_path
            if not source_matches:
                excluded.append({"mutant_id": mutant_id, "reason": "scope:source"})
                continue
            reasons.append("source-scope")
            if scope.function is not None and not self._function_matches(
                scope.function,
                mutant.function_id,
                prepared_function_id,
            ):
                excluded.append({"mutant_id": mutant_id, "reason": "scope:function"})
                continue
            if scope.function is not None:
                reasons.append("function-scope")
            if scope.class_name is not None:
                class_name = mutant.class_name or prepared_class_name
                if class_name != scope.class_name:
                    excluded.append({"mutant_id": mutant_id, "reason": "scope:class"})
                    continue
                reasons.append("class-scope")
            if scope.from_line is not None and mutant.line_no < scope.from_line:
                excluded.append({"mutant_id": mutant_id, "reason": "scope:from-line"})
                continue
            if scope.to_line is not None and mutant.line_no > scope.to_line:
                excluded.append({"mutant_id": mutant_id, "reason": "scope:to-line"})
                continue
            if scope.from_line is not None or scope.to_line is not None:
                reasons.append("line-scope")
            if operators and mutant.mutation not in operators:
                excluded.append({"mutant_id": mutant_id, "reason": "scope:operator"})
                continue
            if operators:
                reasons.append("operator-scope")
            priority = 100
            if explicit_ids:
                priority += 1000
            if scope.function is not None:
                priority += 100
            if scope.from_line is not None or scope.to_line is not None:
                priority += 10
            if operators:
                priority += 5
            candidates.append(
                _Candidate(
                    mutant,
                    priority,
                    tuple(reasons),
                    mutant.function_id or prepared_function_id or "<unknown>",
                )
            )
        return tuple(candidates), tuple(excluded)
    @staticmethod
    def _function_matches(
        requested: str,
        mutant_function_id: str | None,
        prepared_function_id: str | None,
    ) -> bool:
        # Accept the runner's qualified function identity while keeping an explicit mismatch fail-closed.
        if mutant_function_id:
            return mutant_function_id == requested or mutant_function_id.rsplit("::", 1)[-1] == requested
        if prepared_function_id:
            return prepared_function_id == requested or prepared_function_id.rsplit("::", 1)[-1] == requested
        return False
    @staticmethod
    def _ordering_key(candidate: _Candidate) -> tuple[Any, ...]:
        # Preserve source order for equal priorities while making ties independent of input iteration order.
        mutant = candidate.mutant
        return (
            -candidate.priority,
            mutant.source_path,
            mutant.line_no,
            mutant.column_no,
            mutant.mutation,
            mutant.mutant_id.value,
        )
    def _apply_budget(
        self,
        budget: CampaignBudget,
        ordered: Sequence[_Candidate],
        *,
        cost_observations: Mapping[str, float] | None = None,
    ) -> tuple[tuple[_Candidate, ...], tuple[dict[str, Any], ...]]:
        # Select candidates under every independent budget and explain each rejected boundary.
        selected: list[_Candidate] = []
        excluded: list[dict[str, Any]] = []
        function_counts: dict[str, int] = {}
        operator_counts: dict[str, int] = {}
        estimated_cpu_seconds = 0.0
        selected_costs: list[float] = []
        worker_count = max(1, int(budget.max_workers))
        for item in ordered:
            item_cost = self._estimated_cost(item, cost_observations)
            projected_cpu_seconds = estimated_cpu_seconds + item_cost
            projected_wall_seconds = self._weighted_wall_seconds((*selected_costs, item_cost), worker_count)
            reason: str | None = None
            if budget.max_mutants is not None and len(selected) >= budget.max_mutants:
                reason = "budget:max-mutants"
            elif budget.max_seconds is not None and projected_wall_seconds > budget.max_seconds:
                reason = "budget:max-seconds"
            elif budget.max_cpu_seconds is not None and projected_cpu_seconds > budget.max_cpu_seconds:
                reason = "budget:max-cpu-seconds"
            elif (
                budget.max_mutants_per_function is not None
                and function_counts.get(item.function_key, 0) >= budget.max_mutants_per_function
            ):
                reason = "budget:max-mutants-per-function"
            elif (
                budget.max_mutants_per_operator is not None
                and operator_counts.get(item.mutant.mutation, 0) >= budget.max_mutants_per_operator
            ):
                reason = "budget:max-mutants-per-operator"
            if reason is not None:
                excluded.append(
                    {
                        "mutant_id": item.mutant.mutant_id.value,
                        "reason": reason,
                        "priority": item.priority,
                        "estimated_seconds": item_cost,
                    }
                )
                continue
            selected.append(item)
            selected_costs.append(item_cost)
            estimated_cpu_seconds = projected_cpu_seconds
            function_counts[item.function_key] = function_counts.get(item.function_key, 0) + 1
            operator_counts[item.mutant.mutation] = operator_counts.get(item.mutant.mutation, 0) + 1
        return tuple(selected), tuple(excluded)
    @staticmethod
    def _weighted_wall_seconds(costs: Sequence[float], worker_count: int) -> float:
        # Estimate wall time through the same deterministic least-loaded placement used by shard planning.
        normalized = tuple(max(0.001, float(value)) for value in costs)
        if not normalized:
            return 0.0
        loads = [0.0] * min(max(1, int(worker_count)), len(normalized))
        for cost in sorted(normalized, reverse=True):
            target = min(range(len(loads)), key=lambda index: (loads[index], index))
            loads[target] += cost
        return max(loads, default=0.0)
    def _estimated_cost(self, item: _Candidate, observations: Mapping[str, float] | None = None) -> float:
        # Use the frozen candidate cost, then explicit history, then stable operator weights.
        if item.estimated_seconds > 0.0:
            return max(0.001, float(item.estimated_seconds))
        observed = (observations or {}).get(item.mutant.mutant_id.value)
        if observed is not None:
            return max(0.001, float(observed))
        weight = self.OPERATOR_COST_WEIGHTS.get(item.mutant.mutation, 1.0)
        return max(0.001, self.ESTIMATED_MUTANT_SECONDS * weight)
    def _decisions(
        self,
        catalog: Sequence[MutantDescriptor],
        eligible: Sequence[_Candidate],
        selected: Sequence[_Candidate],
        excluded: Sequence[Mapping[str, Any]],
        reuse_decisions: Mapping[str, Any] | None,
        *,
        campaign_id: str,
        reuse_mode: ReuseMode,
        audit_policy: Any | None = None,
        estimated_costs: Mapping[str, float] | None = None,
        random_audit_rate: float = 0.0,
        adaptive_seed: str = "theseus-adaptive-v2",
    ) -> tuple[PlanDecision, ...]:
        # Turn scope, budget and reuse evidence into one complete explainability ledger.
        eligible_ids = {item.mutant.mutant_id.value for item in eligible}
        selected_ids = {item.mutant.mutant_id.value for item in selected}
        priorities = {item.mutant.mutant_id.value: item.priority for item in eligible}
        exclusion_reasons = {
            str(item.get("mutant_id")): str(item.get("reason", "excluded"))
            for item in excluded
            if item.get("mutant_id")
        }
        decisions: list[PlanDecision] = []
        unique_catalog = {item.mutant_id.value: item for item in catalog}
        for mutant in sorted(unique_catalog.values(), key=lambda item: item.mutant_id.value):
            mutant_id = mutant.mutant_id.value
            if mutant_id not in eligible_ids:
                reason = exclusion_reasons.get(mutant_id, "outside_scope")
                action = "deduplicate" if reason == "deduplicate" else "outside_scope"
                decisions.append(PlanDecision(mutant_id, action, reason))
                continue
            if mutant_id not in selected_ids:
                decisions.append(
                    PlanDecision(
                        mutant_id,
                        "budget_excluded",
                        exclusion_reasons.get(mutant_id, "budget"),
                        priority=priorities.get(mutant_id, 0),
                        estimated_seconds=max(
                            0.001,
                            float((estimated_costs or {}).get(mutant_id, self.ESTIMATED_MUTANT_SECONDS)),
                        ),
                    )
                )
                continue
            raw = self._json_value((reuse_decisions or {}).get(mutant_id, {}))
            raw_mapping = raw if isinstance(raw, Mapping) else {}
            kind = str(raw_mapping.get("kind", "none"))
            decision_mode = normalize_reuse_mode(raw_mapping.get("reuse_mode", reuse_mode.value))
            if decision_mode is not reuse_mode:
                raise PlanError(
                    f"reuse decision mode conflicts with campaign policy: mutant_id={mutant_id}; "
                    f"campaign_mode={reuse_mode.value}; decision_mode={decision_mode.value}"
                )
            evidence_eligible = bool(raw_mapping.get("eligible", False))
            allowed = (
                reuse_mode is ReuseMode.PARTIAL and kind in {"exact", "partial"}
            ) or (reuse_mode is ReuseMode.EXACT and kind == "exact")
            authorized = bool(raw_mapping.get("authorized", evidence_eligible and allowed))
            if authorized and not evidence_eligible:
                raise PlanError(
                    f"reuse decision is authorized without eligible evidence: mutant_id={mutant_id}; "
                    f"mode={reuse_mode.value}; kind={kind}"
                )
            if authorized and not allowed:
                raise PlanError(
                    f"reuse decision exceeds campaign policy: mutant_id={mutant_id}; "
                    f"mode={reuse_mode.value}; kind={kind}"
                )
            audit_selected = authorized and self._audit_selected(
                campaign_id,
                mutant_id,
                raw_mapping,
                audit_policy,
                random_audit_rate=random_audit_rate,
                adaptive_seed=adaptive_seed,
            )
            if audit_selected and kind in {"exact", "partial"}:
                action = "audit"
                reason = "reuse-audit"
            elif authorized and kind == "exact":
                action = "reuse"
                reason = str(raw_mapping.get("reason", "exact-reuse"))
            elif authorized and kind == "partial":
                action = "partial_reuse"
                reason = str(raw_mapping.get("reason", "partial-reuse"))
            elif kind == "historical_hint":
                action = "execute"
                reason = "historical_hint_only"
            elif evidence_eligible and kind in {"exact", "partial"}:
                action = "execute"
                reason = f"reuse_not_authorized:{reuse_mode.value}"
            else:
                action = "execute"
                reason = str(raw_mapping.get("reason", "fresh-execution"))
            decisions.append(
                PlanDecision(
                    mutant_id,
                    action,
                    reason,
                    priority=priorities.get(mutant_id, 0),
                    estimated_seconds=max(
                        0.001,
                        float((estimated_costs or {}).get(mutant_id, self.ESTIMATED_MUTANT_SECONDS)),
                    ),
                    audit_selected=audit_selected,
                    source_event_id=(
                        str(raw_mapping["source_event_id"])
                        if raw_mapping.get("source_event_id")
                        else None
                    ),
                    matched_test_ids=tuple(sorted(dict.fromkeys(
                        str(item) for item in raw_mapping.get("matched_test_ids", ()) if str(item)
                    ))),
                    missing_test_ids=tuple(sorted(dict.fromkeys(
                        str(item) for item in raw_mapping.get("missing_test_ids", ()) if str(item)
                    ))),
                )
            )
        return tuple(decisions)
    @classmethod
    def _audit_selected(
        cls,
        campaign_id: str,
        mutant_id: str,
        raw: Mapping[str, Any],
        policy: Any | None,
        *,
        random_audit_rate: float = 0.0,
        adaptive_seed: str = "theseus-adaptive-v2",
    ) -> bool:
        # Preserve the shared reuse audit contract and add adaptive sampling independently.
        kind = str(raw.get("kind", ""))
        rule_id = str(
            raw.get("function_id")
            or raw.get("function_fingerprint")
            or raw.get("mutant_fingerprint")
            or "unknown-rule"
        )
        policy_selected = bool(raw.get("audit_required", False)) and policy is None
        if policy is not None and bool(raw.get("audit_required", False)):
            policy_rate = float(
                getattr(policy, "exact_sample_rate", 0.0)
                if kind == "exact"
                else getattr(policy, "partial_sample_rate", 0.0)
            )
            if policy_rate > 0.0:
                policy_seed = str(getattr(policy, "random_seed", "theseus-reuse-audit-v1"))
                digest = hashlib.sha256(
                    "\x1f".join((campaign_id, mutant_id, rule_id, policy_seed)).encode("utf-8")
                ).digest()
                policy_selected = int.from_bytes(digest[:8], "big") / float(1 << 64) < policy_rate
        adaptive_selected = cls._deterministic_sample(
            campaign_id,
            mutant_id,
            purpose=f"reuse-audit:{kind}:{rule_id}",
            rate=float(random_audit_rate),
            seed=str(adaptive_seed),
        )
        return policy_selected or adaptive_selected
    @staticmethod
    def _test_plan(
        prepared: PreparedCampaign,
        *,
        no_escalation: bool,
        test_timeout_seconds: float | None,
    ) -> tuple[tuple[str, ...], tuple[Mapping[str, Any], ...], tuple[str, ...]]:
        # Freeze the selected, domain and full ladder with timeout and correctness-preserving stop rules.
        selection = prepared.selection
        if selection is None:
            return (), (), ()
        order: list[str] = []
        seen: set[str] = set()
        for nodeid in (
            *selection.selected_tests,
            *(nodeid for level in selection.levels for nodeid in level.nodeids),
        ):
            if nodeid not in seen:
                seen.add(nodeid)
                order.append(nodeid)
        selected_levels = selection.levels[:1] if no_escalation else selection.levels[:3]
        stage_names = ("selected", "domain", "full")[:len(selected_levels)]
        base_timeout = max(1.0, float(test_timeout_seconds or 120.0))
        levels = tuple(
            {
                **level.to_dict(),
                "stage": stage_names[ordinal],
                "ordinal": ordinal,
                "timeout_seconds": base_timeout * float(2 ** ordinal),
                "stop_on_kill": True,
                "escalate_on": "survived",
            }
            for ordinal, level in enumerate(selected_levels)
        )
        return tuple(order), levels, stage_names
    @staticmethod
    def _json_value(value: Any) -> Any:
        # Convert optional reuse DTOs to canonical JSON without coupling the planner to Knowledge Plane classes.
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if hasattr(value, "to_dict"):
            return CampaignPlanner._json_value(value.to_dict())
        if isinstance(value, Mapping):
            return {str(key): CampaignPlanner._json_value(item) for key, item in value.items()}
        if isinstance(value, (list, tuple, set, frozenset)):
            return [CampaignPlanner._json_value(item) for item in value]
        return str(value)
    @staticmethod
    def _shards(
        selected: Sequence[PlannedMutant],
        max_workers: int,
        *,
        target_shard_seconds: float | None = None,
    ) -> tuple[ShardDescriptor, ...]:
        # Return no empty shard, then size weighted work within the configured worker ceiling.
        if not selected:
            return ()
        worker_limit = min(max(1, int(max_workers)), len(selected))
        if target_shard_seconds is None:
            shard_count = worker_limit
        else:
            total_cost = sum(max(0.001, float(item.estimated_seconds)) for item in selected)
            required = max(1, int(math.ceil(total_cost / float(target_shard_seconds))))
            shard_count = min(len(selected), required)
        rows: list[list[PlannedMutant]] = [[] for _ in range(shard_count)]
        costs = [0.0] * shard_count
        for item in sorted(selected, key=lambda row: (-row.estimated_seconds, row.mutant.mutant_id.value)):
            ordinal = min(range(shard_count), key=lambda index: (costs[index], index))
            rows[ordinal].append(item)
            costs[ordinal] += item.estimated_seconds
        shards: list[ShardDescriptor] = []
        for ordinal, bucket in enumerate(rows):
            bucket.sort(key=lambda row: row.rank)
            shards.append(
                ShardDescriptor(
                    shard_id=ShardId(f"shard-{ordinal:03d}"),
                    mutant_ids=tuple(item.mutant.mutant_id.value for item in bucket),
                    estimated_cost=costs[ordinal],
                )
            )
        return tuple(shards)
    @staticmethod
    def _digest(value: Any) -> str:
        # Hash only canonical JSON so the same immutable inputs always produce the same plan identity.
        payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()
__all__ = ["CampaignPlanner", "PlanError"]
