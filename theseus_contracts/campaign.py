"""Campaign request and summary DTOs for the Theseus public boundary."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Mapping
from .enums import CampaignMode, CampaignStatus
from .ids import CampaignId
from .project import ProjectDescriptor
from .reuse import normalize_reuse_mode
from .serialization import WireModel, optional_string, required_string, sequence_of_strings
@dataclass(frozen=True, slots=True)
class CampaignBudget(WireModel):
    """Explicit compute limits passed to a planner or engine."""
    max_mutants: int | None = None
    max_seconds: float | None = None
    max_workers: int = 1
    max_test_seconds: float | None = None
    max_mutants_per_function: int | None = None
    max_mutants_per_operator: int | None = None
    max_cpu_seconds: float | None = None
    lease_seconds: float = 60.0
    target_shard_seconds: float | None = None
    exploration_rate: float = 0.0
    random_audit_rate: float = 0.0
    adaptive_timeout_multiplier: float = 3.0
    adaptive_seed: str = "theseus-adaptive-v2"
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "CampaignBudget":
        # Normalize optional limits and retain conservative worker defaults.
        return cls(
            max_mutants=int(value["max_mutants"]) if value.get("max_mutants") is not None else None,
            max_mutants_per_function=(
                int(value["max_mutants_per_function"])
                if value.get("max_mutants_per_function") is not None
                else None
            ),
            max_mutants_per_operator=(
                int(value["max_mutants_per_operator"])
                if value.get("max_mutants_per_operator") is not None
                else None
            ),
            max_seconds=float(value["max_seconds"]) if value.get("max_seconds") is not None else None,
            max_cpu_seconds=float(value["max_cpu_seconds"]) if value.get("max_cpu_seconds") is not None else None,
            max_workers=max(1, int(value.get("max_workers", 1))),
            max_test_seconds=float(value["max_test_seconds"]) if value.get("max_test_seconds") is not None else None,
            lease_seconds=float(value.get("lease_seconds", 60.0)),
            target_shard_seconds=(
                float(value["target_shard_seconds"])
                if value.get("target_shard_seconds") is not None
                else None
            ),
            exploration_rate=float(value.get("exploration_rate", 0.0)),
            random_audit_rate=float(value.get("random_audit_rate", 0.0)),
            adaptive_timeout_multiplier=float(value.get("adaptive_timeout_multiplier", 3.0)),
            adaptive_seed=str(value.get("adaptive_seed", "theseus-adaptive-v2")),
        )
@dataclass(frozen=True, slots=True)
class MutationScope(WireModel):
    """Source and exact mutant filters for one campaign."""
    source_path: str
    function: str | None = None
    from_line: int | None = None
    to_line: int | None = None
    mutant_ids: tuple[str, ...] = ()
    operators: tuple[str, ...] = ()
    scope_kind: str = "file"
    class_name: str | None = None
    git_diff_paths: tuple[str, ...] = ()
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MutationScope":
        # Restore the composite scope as immutable filter inputs.
        return cls(
            source_path=required_string(value, "source_path"),
            scope_kind=str(value.get("scope_kind", "file")).strip().lower(),
            function=optional_string(value, "function"),
            class_name=optional_string(value, "class_name"),
            from_line=int(value["from_line"]) if value.get("from_line") is not None else None,
            to_line=int(value["to_line"]) if value.get("to_line") is not None else None,
            mutant_ids=sequence_of_strings(value, "mutant_ids"),
            operators=sequence_of_strings(value, "operators"),
            git_diff_paths=sequence_of_strings(value, "git_diff_paths"),
        )
@dataclass(frozen=True, slots=True)
class CampaignConfiguration(WireModel):
    """Complete engine input that remains independent from runner internals."""
    campaign_id: CampaignId
    project: ProjectDescriptor
    scope: MutationScope
    budget: CampaignBudget = CampaignBudget()
    mode: CampaignMode | str = CampaignMode.STANDARD
    configuration_fingerprint: str = ""
    no_escalation: bool = False
    reports_dir: str | None = None
    reuse_mode: str = "hint"
    def __post_init__(self) -> None:
        # Normalize canonical reuse authority at the public campaign boundary.
        object.__setattr__(self, "reuse_mode", normalize_reuse_mode(self.reuse_mode).value)
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "CampaignConfiguration":
        # Restore nested campaign inputs while ignoring future configuration fields.
        project = value.get("project")
        scope = value.get("scope")
        budget = value.get("budget")
        if not isinstance(project, Mapping) or not isinstance(scope, Mapping):
            raise ValueError("project and scope are required objects")
        mode_value = value.get("mode", CampaignMode.STANDARD.value)
        try:
            mode: CampaignMode | str = CampaignMode(str(mode_value))
        except ValueError:
            mode = str(mode_value)
        return cls(
            campaign_id=CampaignId(required_string(value, "campaign_id")),
            project=ProjectDescriptor.from_dict(project),
            scope=MutationScope.from_dict(scope),
            budget=CampaignBudget.from_dict(budget) if isinstance(budget, Mapping) else CampaignBudget(),
            mode=mode,
            configuration_fingerprint=str(value.get("configuration_fingerprint", "")),
            no_escalation=bool(value.get("no_escalation", False)),
            reports_dir=optional_string(value, "reports_dir"),
            reuse_mode=str(value.get("reuse_mode", "hint")).strip().lower(),
        )
@dataclass(frozen=True, slots=True)
class CampaignSummary(WireModel):
    """Stable read model returned after a campaign or replay."""
    campaign_id: CampaignId
    status: CampaignStatus | str
    total_mutants: int
    completed_mutants: int
    counts: Mapping[str, int] = field(default_factory=dict)
    report_path: str | None = None
    workers: int = 1
    error: str | None = None
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "CampaignSummary":
        # Restore bounded summary fields without loading the full result journal.
        status_value = required_string(value, "status")
        try:
            status: CampaignStatus | str = CampaignStatus(status_value)
        except ValueError:
            status = status_value
        raw_counts = value.get("counts", {})
        counts = {
            str(key): int(item)
            for key, item in raw_counts.items()
        } if isinstance(raw_counts, Mapping) else {}
        return cls(
            campaign_id=CampaignId(required_string(value, "campaign_id")),
            status=status,
            total_mutants=int(value.get("total_mutants", 0)),
            completed_mutants=int(value.get("completed_mutants", 0)),
            counts=counts,
            report_path=optional_string(value, "report_path"),
            workers=max(1, int(value.get("workers", 1))),
            error=optional_string(value, "error"),
        )
