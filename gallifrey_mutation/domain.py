"""Immutable mutation campaign aggregates kept separate from Gallifrey development runs."""
from __future__ import annotations
from dataclasses import dataclass, field, replace
from enum import Enum
import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
from theseus_contracts import (
    ArtifactId,
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    CampaignMode,
    ExecutionId,
    MutantId,
    MutationScope,
    ProjectId,
    RevisionId,
    ShardDescriptor,
    ShardId,
    ShardLease,
    WorkerCapabilities,
    WorkerHeartbeat,
    WorkerIdentity,
    WorkerId,
    deterministic_id,
)
from theseus_contracts.serialization import required_string, to_json_value, utc_now
from theseus_contracts.enums import WorkerStatus
from .outcomes import Outcome, Rejected, Success
class CampaignState(str, Enum):
    """Durable campaign lifecycle states owned by the mutation domain."""
    CREATED = "created"
    PREPARING = "preparing"
    COLLECTING = "collecting"
    INDEXING = "indexing"
    BASELINING = "baselining"
    DISCOVERING = "discovering"
    PLANNING = "planning"
    RUNNING = "running"
    AGGREGATING = "aggregating"
    MATERIALIZING = "materializing"
    READY_TO_COMMIT = "ready_to_commit"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
class OperatorActionStatus(str, Enum):
    """Durable lifecycle of one replay-safe operator command."""
    REQUESTED = "requested"
    RUNNING = "running"
    COMPLETED = "completed"
    REJECTED = "rejected"
    FAILED = "failed"
@dataclass(frozen=True, slots=True)
class OperatorAction:
    """Durable request identity and terminal result for one operator command."""
    action_id: str
    action_type: str
    campaign_id: CampaignId
    request_fingerprint: str
    expected_revision: int
    status: OperatorActionStatus = OperatorActionStatus.REQUESTED
    result: Mapping[str, Any] = field(default_factory=dict)
    error_code: str | None = None
    retriable: bool = False
    created_at: str = ""
    updated_at: str = ""
    def __post_init__(self) -> None:
        # Validate immutable request identity and freeze result data before persistence.
        if not isinstance(self.action_id, str) or not self.action_id.strip():
            raise ValueError("operator action_id must be a non-empty string")
        if not isinstance(self.action_type, str) or not self.action_type.strip():
            raise ValueError("operator action_type must be a non-empty string")
        if not isinstance(self.campaign_id, CampaignId):
            raise TypeError("operator campaign_id must be CampaignId")
        if not isinstance(self.request_fingerprint, str) or len(self.request_fingerprint) != 64:
            raise ValueError("operator request_fingerprint must be SHA-256")
        if isinstance(self.expected_revision, bool) or not isinstance(self.expected_revision, int) or self.expected_revision < 0:
            raise ValueError("operator expected_revision must be a non-negative integer")
        if not isinstance(self.status, OperatorActionStatus):
            raise TypeError("operator status must be OperatorActionStatus")
        if self.result is None:
            object.__setattr__(self, "result", {})
        elif not isinstance(self.result, Mapping):
            raise TypeError("operator result must be an object")
        else:
            object.__setattr__(self, "result", dict(self.result))
        if self.status in {OperatorActionStatus.REJECTED, OperatorActionStatus.FAILED} and not self.error_code:
            raise ValueError("terminal failed operator action requires error_code")
        if self.status not in {OperatorActionStatus.REJECTED, OperatorActionStatus.FAILED} and self.error_code is not None:
            raise ValueError("successful operator action cannot carry error_code")
    @classmethod
    def create(
        cls,
        action_id: str,
        action_type: str,
        campaign_id: CampaignId,
        expected_revision: int,
        *,
        parameters: Mapping[str, Any] | None = None,
        now: str | None = None,
    ) -> "OperatorAction":
        # Create one canonical request fingerprint independent of timestamps and storage paths.
        payload = {
            "action_type": str(action_type).strip(),
            "campaign_id": campaign_id.value,
            "expected_revision": expected_revision,
            "parameters": to_json_value(dict(parameters or {})),
        }
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        timestamp = now or utc_now()
        return cls(
            action_id=str(action_id).strip(),
            action_type=str(action_type).strip(),
            campaign_id=campaign_id,
            request_fingerprint=hashlib.sha256(encoded).hexdigest(),
            expected_revision=expected_revision,
            created_at=timestamp,
            updated_at=timestamp,
        )
    def start(self, *, now: str | None = None) -> "OperatorAction":
        # Advance a requested action while keeping running replay idempotent.
        if self.status == OperatorActionStatus.RUNNING:
            return self
        if self.status != OperatorActionStatus.REQUESTED:
            raise ValueError("only requested operator action can start")
        return replace(self, status=OperatorActionStatus.RUNNING, updated_at=now or utc_now())
    def update_running(self, result: Mapping[str, Any], *, now: str | None = None) -> "OperatorAction":
        # Replace only bounded running metadata without changing request or terminal authority.
        if self.status != OperatorActionStatus.RUNNING:
            raise ValueError("only running operator action can update metadata")
        return replace(
            self,
            result=dict(result),
            updated_at=now or utc_now(),
        )
    def complete(self, result: Mapping[str, Any], *, now: str | None = None) -> "OperatorAction":
        # Persist one sanitized terminal success result for exact replay.
        if self.status == OperatorActionStatus.COMPLETED:
            return self
        if self.status not in {OperatorActionStatus.REQUESTED, OperatorActionStatus.RUNNING}:
            raise ValueError("failed operator action cannot complete")
        return replace(
            self,
            status=OperatorActionStatus.COMPLETED,
            result=dict(result),
            error_code=None,
            retriable=False,
            updated_at=now or utc_now(),
        )
    def reject(self, code: str, result: Mapping[str, Any] | None = None, *, now: str | None = None) -> "OperatorAction":
        # Persist one expected terminal rejection without an exception message.
        if self.status == OperatorActionStatus.REJECTED:
            return self
        if self.status in {OperatorActionStatus.COMPLETED, OperatorActionStatus.FAILED}:
            raise ValueError("terminal operator action cannot be rejected again")
        return replace(
            self,
            status=OperatorActionStatus.REJECTED,
            result=dict(result or {}),
            error_code=str(code),
            retriable=False,
            updated_at=now or utc_now(),
        )
    def fail(
        self,
        code: str,
        *,
        retriable: bool,
        result: Mapping[str, Any] | None = None,
        now: str | None = None,
    ) -> "OperatorAction":
        # Persist one technical terminal failure without leaking infrastructure diagnostics.
        if self.status == OperatorActionStatus.FAILED:
            return self
        if self.status in {OperatorActionStatus.COMPLETED, OperatorActionStatus.REJECTED}:
            raise ValueError("terminal operator action cannot fail again")
        return replace(
            self,
            status=OperatorActionStatus.FAILED,
            result=dict(result or {}),
            error_code=str(code),
            retriable=bool(retriable),
            updated_at=now or utc_now(),
        )
    def to_dict(self) -> dict[str, Any]:
        # Serialize one durable operator action without implementation objects.
        return {
            "action_id": self.action_id,
            "action_type": self.action_type,
            "campaign_id": self.campaign_id.value,
            "request_fingerprint": self.request_fingerprint,
            "expected_revision": self.expected_revision,
            "status": self.status.value,
            "result": to_json_value(dict(self.result)),
            "error_code": self.error_code,
            "retriable": self.retriable,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "OperatorAction":
        # Restore one durable operator action and preserve future result fields.
        raw_result = value.get("result", {})
        if not isinstance(raw_result, Mapping):
            raise ValueError("operator result must be an object")
        return cls(
            action_id=required_string(value, "action_id"),
            action_type=required_string(value, "action_type"),
            campaign_id=CampaignId(required_string(value, "campaign_id")),
            request_fingerprint=required_string(value, "request_fingerprint"),
            expected_revision=int(value.get("expected_revision", 0)),
            status=OperatorActionStatus(required_string(value, "status")),
            result=dict(raw_result),
            error_code=str(value["error_code"]) if value.get("error_code") else None,
            retriable=bool(value.get("retriable", False)),
            created_at=str(value.get("created_at", "")),
            updated_at=str(value.get("updated_at", "")),
        )
class MutationShardState(str, Enum):
    """Shard states used by coordinator claim, retry and fan-in logic."""
    CREATED = "created"
    LEASED = "leased"
    RUNNING = "running"
    PARTIAL = "partial"
    COMPLETE = "complete"
    FAILED = "failed"
    CANCELLED = "cancelled"
    ORPHANED = "orphaned"
class MutationExecutionState(str, Enum):
    """Execution states independent from the semantic mutant result."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"
    CANCELLED = "cancelled"
class MutationResult(str, Enum):
    """Semantic result labels preserved separately from process execution status."""
    KILLED = "killed"
    SURVIVED = "survived"
    INVALID = "invalid_mutant"
    INFRASTRUCTURE_ERROR = "infrastructure_error"
    TIMEOUT = "timeout"
    ERROR = "error"
    NOT_RUN = "not_run"
_CAMPAIGN_TRANSITIONS: dict[CampaignState, frozenset[CampaignState]] = {
    CampaignState.CREATED: frozenset({CampaignState.PREPARING}),
    CampaignState.PREPARING: frozenset({CampaignState.COLLECTING}),
    CampaignState.COLLECTING: frozenset({CampaignState.INDEXING}),
    CampaignState.INDEXING: frozenset({CampaignState.BASELINING}),
    CampaignState.BASELINING: frozenset({CampaignState.DISCOVERING}),
    CampaignState.DISCOVERING: frozenset({CampaignState.PLANNING}),
    CampaignState.PLANNING: frozenset({CampaignState.RUNNING}),
    CampaignState.RUNNING: frozenset({CampaignState.AGGREGATING}),
    CampaignState.AGGREGATING: frozenset({CampaignState.MATERIALIZING}),
    CampaignState.MATERIALIZING: frozenset({CampaignState.READY_TO_COMMIT}),
    CampaignState.READY_TO_COMMIT: frozenset({CampaignState.COMPLETED}),
    CampaignState.COMPLETED: frozenset(),
    CampaignState.FAILED: frozenset(),
    CampaignState.CANCELLED: frozenset(),
}
_TERMINAL_CAMPAIGN_STATES = frozenset(
    {CampaignState.COMPLETED, CampaignState.FAILED, CampaignState.CANCELLED}
)
_CANCELLABLE_CAMPAIGN_STATES = frozenset(
    state for state in CampaignState if state not in _TERMINAL_CAMPAIGN_STATES
)
def _stale_revision(entity: str, expected: int, actual: int) -> Rejected:
    # Build one stable optimistic-concurrency rejection for all mutation aggregates.
    return Rejected(
        code="stale_revision",
        message=f"{entity} changed since it was loaded",
        details={"entity": entity, "expected_revision": expected, "actual_revision": actual},
    )
def _identifier(value: Any, identifier_type: type[Any], field_name: str) -> Any:
    # Restore a typed identifier without allowing an empty persistence key.
    if isinstance(value, identifier_type):
        return value
    return identifier_type(required_string({field_name: value}, field_name))
def _campaign_mode(value: Any) -> CampaignMode | str:
    # Preserve future campaign modes while normalizing known values to the public enum.
    try:
        return CampaignMode(str(value))
    except ValueError:
        return str(value)
@dataclass(frozen=True, slots=True)
class MutationCampaign:
    """Independent campaign aggregate for mutation execution and control-plane progress."""
    campaign_id: CampaignId
    project_id: ProjectId
    revision_id: RevisionId
    configuration: CampaignConfiguration
    scope: MutationScope
    mode: CampaignMode | str
    budget: CampaignBudget
    status: CampaignState = CampaignState.CREATED
    collection_snapshot_id: str | None = None
    index_snapshot_id: str | None = None
    baseline_snapshot_id: str | None = None
    plan_id: str | None = None
    prepared_snapshot_id: str | None = None
    total_mutants: int = 0
    completed_mutants: int = 0
    revision_number: int = 0
    def __post_init__(self) -> None:
        # Enforce aggregate identity and monotonic counters before persistence.
        if not isinstance(self.campaign_id, CampaignId):
            raise TypeError("campaign_id must be CampaignId")
        if not isinstance(self.project_id, ProjectId) or not isinstance(self.revision_id, RevisionId):
            raise TypeError("project_id and revision_id must be typed identifiers")
        if not isinstance(self.configuration, CampaignConfiguration):
            raise TypeError("configuration must be CampaignConfiguration")
        if not isinstance(self.status, CampaignState):
            raise TypeError("status must be CampaignState")
        if min(self.total_mutants, self.completed_mutants, self.revision_number) < 0:
            raise ValueError("campaign counters must not be negative")
        if self.completed_mutants > self.total_mutants:
            raise ValueError("completed_mutants cannot exceed total_mutants")
    @classmethod
    def create(
        cls,
        configuration: CampaignConfiguration,
        *,
        revision_id: RevisionId | None = None,
    ) -> "MutationCampaign":
        # Create a revision-bound aggregate without importing Gallifrey run models.
        project = configuration.project
        resolved_revision = revision_id
        if resolved_revision is None and project.revision is not None:
            resolved_revision = project.revision.revision_id
        if resolved_revision is None:
            resolved_revision = RevisionId(
                deterministic_id("rev", configuration.campaign_id.value, project.project_id.value)
            )
        return cls(
            campaign_id=configuration.campaign_id,
            project_id=project.project_id,
            revision_id=resolved_revision,
            configuration=configuration,
            scope=configuration.scope,
            mode=configuration.mode,
            budget=configuration.budget,
        )
    def transition(
        self,
        target: CampaignState,
        *,
        expected_revision: int,
    ) -> Outcome["MutationCampaign"]:
        # Apply exactly one state transition using optimistic concurrency control.
        if expected_revision != self.revision_number:
            return _stale_revision("campaign", expected_revision, self.revision_number)
        if target == self.status:
            return Success(self, message="Campaign transition already applied", duplicate=True)
        if target == CampaignState.CANCELLED and self.status in _CANCELLABLE_CAMPAIGN_STATES:
            next_campaign = replace(self, status=target, revision_number=self.revision_number + 1)
            return Success(next_campaign, message="Campaign cancelled")
        if target == CampaignState.FAILED and self.status not in _TERMINAL_CAMPAIGN_STATES:
            next_campaign = replace(self, status=target, revision_number=self.revision_number + 1)
            return Success(next_campaign, message="Campaign failed")
        if self.status in _TERMINAL_CAMPAIGN_STATES:
            return Rejected(
                code="campaign_immutable",
                message="Terminal campaign cannot transition",
                details={"status": self.status.value},
            )
        if target not in _CAMPAIGN_TRANSITIONS[self.status]:
            return Rejected(
                code="invalid_campaign_transition",
                message="Campaign transition is not allowed",
                details={"from_status": self.status.value, "to_status": target.value},
            )
        return Success(
            replace(self, status=target, revision_number=self.revision_number + 1),
            message="Campaign transitioned",
        )
    def attach_snapshots(
        self,
        *,
        expected_revision: int,
        collection_snapshot_id: str | None = None,
        index_snapshot_id: str | None = None,
        baseline_snapshot_id: str | None = None,
        plan_id: str | None = None,
        prepared_snapshot_id: str | None = None,
    ) -> Outcome["MutationCampaign"]:
        # Attach immutable preparation references without changing campaign status.
        if expected_revision != self.revision_number:
            return _stale_revision("campaign", expected_revision, self.revision_number)
        updates = {
            "collection_snapshot_id": collection_snapshot_id or self.collection_snapshot_id,
            "index_snapshot_id": index_snapshot_id or self.index_snapshot_id,
            "baseline_snapshot_id": baseline_snapshot_id or self.baseline_snapshot_id,
            "plan_id": plan_id or self.plan_id,
            "prepared_snapshot_id": prepared_snapshot_id or self.prepared_snapshot_id,
        }
        if all(getattr(self, key) == value for key, value in updates.items()):
            return Success(self, message="Campaign snapshots already attached", duplicate=True)
        return Success(replace(self, **updates, revision_number=self.revision_number + 1))
    def complete_stage(
        self,
        target: CampaignState,
        *,
        expected_revision: int,
        collection_snapshot_id: str | None = None,
        index_snapshot_id: str | None = None,
        baseline_snapshot_id: str | None = None,
        plan_id: str | None = None,
        prepared_snapshot_id: str | None = None,
    ) -> Outcome["MutationCampaign"]:
        # Advance one stage and attach its immutable artifacts in the same aggregate revision.
        if expected_revision != self.revision_number:
            return _stale_revision("campaign", expected_revision, self.revision_number)
        transitioned = self.transition(target, expected_revision=expected_revision)
        if not isinstance(transitioned, Success):
            return transitioned
        if transitioned.duplicate:
            candidate = self
            next_revision = self.revision_number
        else:
            candidate = transitioned.value
            next_revision = candidate.revision_number
        updates = {
            "collection_snapshot_id": collection_snapshot_id or candidate.collection_snapshot_id,
            "index_snapshot_id": index_snapshot_id or candidate.index_snapshot_id,
            "baseline_snapshot_id": baseline_snapshot_id or candidate.baseline_snapshot_id,
            "plan_id": plan_id or candidate.plan_id,
            "prepared_snapshot_id": prepared_snapshot_id or candidate.prepared_snapshot_id,
        }
        if all(getattr(candidate, key) == value for key, value in updates.items()):
            return Success(candidate, message="Campaign stage already completed", duplicate=True)
        if transitioned.duplicate:
            next_revision += 1
        return Success(replace(candidate, **updates, revision_number=next_revision))
    def record_discovery(
        self,
        total_mutants: int,
        *,
        expected_revision: int,
        plan_id: str | None = None,
    ) -> Outcome["MutationCampaign"]:
        # Publish the deterministic mutant cardinality before shard fan-out begins.
        if expected_revision != self.revision_number:
            return _stale_revision("campaign", expected_revision, self.revision_number)
        if total_mutants < 0 or total_mutants < self.completed_mutants:
            return Rejected(
                code="invalid_mutant_count",
                message="Total mutants cannot be below completed mutants",
            )
        resolved_plan = plan_id or self.plan_id
        if total_mutants == self.total_mutants and resolved_plan == self.plan_id:
            return Success(self, message="Discovery already recorded", duplicate=True)
        return Success(
            replace(
                self,
                total_mutants=total_mutants,
                plan_id=resolved_plan,
                revision_number=self.revision_number + 1,
            )
        )
    def commit_plan(
        self,
        plan_id: str,
        prepared_snapshot_id: str,
        selected_count: int,
        *,
        expected_revision: int,
    ) -> Outcome["MutationCampaign"]:
        # Bind one immutable plan and enter planning in the same aggregate revision.
        if expected_revision != self.revision_number:
            return _stale_revision("campaign", expected_revision, self.revision_number)
        normalized_plan_id = str(plan_id).strip()
        normalized_snapshot_id = str(prepared_snapshot_id).strip()
        if not normalized_plan_id or not normalized_snapshot_id:
            return Rejected(
                code="plan_identity_missing",
                message="Campaign plan and prepared snapshot identities must be non-empty",
            )
        if selected_count < 0 or selected_count < self.completed_mutants:
            return Rejected(
                code="invalid_mutant_count",
                message="Planned mutant count cannot be below completed mutants",
            )
        if self.plan_id not in {None, normalized_plan_id}:
            return Rejected(
                code="campaign_plan_conflict",
                message="Campaign is already bound to another immutable plan",
                details={"existing_plan_id": self.plan_id, "candidate_plan_id": normalized_plan_id},
            )
        if self.prepared_snapshot_id not in {None, normalized_snapshot_id}:
            return Rejected(
                code="prepared_snapshot_conflict",
                message="Campaign is already bound to another prepared snapshot",
                details={
                    "existing_prepared_snapshot_id": self.prepared_snapshot_id,
                    "candidate_prepared_snapshot_id": normalized_snapshot_id,
                },
            )
        if self.status == CampaignState.PLANNING:
            if (
                self.plan_id == normalized_plan_id
                and self.prepared_snapshot_id == normalized_snapshot_id
                and self.total_mutants == selected_count
            ):
                return Success(self, message="Campaign plan already committed", duplicate=True)
            return Rejected(
                code="campaign_plan_conflict",
                message="Planning campaign identity does not match the committed plan",
            )
        if self.status != CampaignState.DISCOVERING:
            return Rejected(
                code="plan_out_of_order",
                message="Campaign plan can be committed only from discovery",
                details={"status": self.status.value},
            )
        return Success(
            replace(
                self,
                status=CampaignState.PLANNING,
                plan_id=normalized_plan_id,
                prepared_snapshot_id=normalized_snapshot_id,
                total_mutants=selected_count,
                revision_number=self.revision_number + 1,
            )
        )
    def record_progress(
        self,
        completed_mutants: int,
        *,
        expected_revision: int,
        total_mutants: int | None = None,
    ) -> Outcome["MutationCampaign"]:
        # Advance fan-in progress monotonically while keeping terminal state immutable.
        if expected_revision != self.revision_number:
            return _stale_revision("campaign", expected_revision, self.revision_number)
        if self.status not in {
            CampaignState.RUNNING,
            CampaignState.AGGREGATING,
            CampaignState.MATERIALIZING,
        }:
            return Rejected(
                code="progress_out_of_order",
                message="Campaign progress is accepted only during execution or aggregation",
                details={"status": self.status.value},
            )
        resolved_total = self.total_mutants if total_mutants is None else total_mutants
        if resolved_total < 0 or completed_mutants < 0 or completed_mutants > resolved_total:
            return Rejected(code="invalid_progress", message="Campaign progress counters are invalid")
        if completed_mutants < self.completed_mutants or resolved_total < self.total_mutants:
            return Rejected(code="progress_not_monotonic", message="Campaign progress must be monotonic")
        if completed_mutants == self.completed_mutants and resolved_total == self.total_mutants:
            return Success(self, message="Campaign progress already recorded", duplicate=True)
        return Success(
            replace(
                self,
                total_mutants=resolved_total,
                completed_mutants=completed_mutants,
                revision_number=self.revision_number + 1,
            )
        )
    def to_dict(self) -> dict[str, Any]:
        # Serialize the full aggregate as JSON-only data for restart and effect receipts.
        return {
            "campaign_id": self.campaign_id.value,
            "project_id": self.project_id.value,
            "revision_id": self.revision_id.value,
            "configuration": self.configuration.to_dict(),
            "scope": self.scope.to_dict(),
            "mode": to_json_value(self.mode),
            "budget": self.budget.to_dict(),
            "status": self.status.value,
            "collection_snapshot_id": self.collection_snapshot_id,
            "index_snapshot_id": self.index_snapshot_id,
            "baseline_snapshot_id": self.baseline_snapshot_id,
            "plan_id": self.plan_id,
            "prepared_snapshot_id": self.prepared_snapshot_id,
            "total_mutants": self.total_mutants,
            "completed_mutants": self.completed_mutants,
            "revision_number": self.revision_number,
        }
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MutationCampaign":
        # Restore a campaign without reconstructing any AST or execution object.
        raw_configuration = value.get("configuration")
        if not isinstance(raw_configuration, Mapping):
            raise ValueError("configuration must be an object")
        configuration = CampaignConfiguration.from_dict(raw_configuration)
        raw_scope = value.get("scope")
        raw_budget = value.get("budget")
        if not isinstance(raw_scope, Mapping) or not isinstance(raw_budget, Mapping):
            raise ValueError("scope and budget must be objects")
        return cls(
            campaign_id=CampaignId(required_string(value, "campaign_id")),
            project_id=ProjectId(required_string(value, "project_id")),
            revision_id=RevisionId(required_string(value, "revision_id")),
            configuration=configuration,
            scope=MutationScope.from_dict(raw_scope),
            mode=_campaign_mode(value.get("mode", configuration.mode)),
            budget=CampaignBudget.from_dict(raw_budget),
            status=CampaignState(required_string(value, "status")),
            collection_snapshot_id=value.get("collection_snapshot_id"),
            index_snapshot_id=value.get("index_snapshot_id"),
            baseline_snapshot_id=value.get("baseline_snapshot_id"),
            plan_id=value.get("plan_id"),
            prepared_snapshot_id=value.get("prepared_snapshot_id"),
            total_mutants=int(value.get("total_mutants", 0)),
            completed_mutants=int(value.get("completed_mutants", 0)),
            revision_number=int(value.get("revision_number", 0)),
        )
@dataclass(frozen=True, slots=True)
class MutationShard:
    """Shard aggregate containing immutable membership and renewable worker ownership."""
    shard_id: ShardId
    campaign_id: CampaignId
    ordinal: int
    mutant_ids: tuple[MutantId, ...]
    estimated_cost: float = 0.0
    status: MutationShardState = MutationShardState.CREATED
    worker_id: WorkerId | None = None
    lease: ShardLease | None = None
    attempt: int = 0
    completed_count: int = 0
    mutant_list_artifact: ArtifactId | None = None
    revision_number: int = 0
    plan_id: str = ""
    def __post_init__(self) -> None:
        # Validate immutable shard membership and progress bounds before a lease is persisted.
        if not isinstance(self.shard_id, ShardId) or not isinstance(self.campaign_id, CampaignId):
            raise TypeError("shard and campaign identities must be typed")
        if self.ordinal < 0 or self.attempt < 0 or self.completed_count < 0 or self.revision_number < 0:
            raise ValueError("shard counters must not be negative")
        if self.completed_count > len(self.mutant_ids):
            raise ValueError("completed_count cannot exceed shard membership")
        if len(set(self.mutant_ids)) != len(self.mutant_ids):
            raise ValueError("shard mutant IDs must be unique")
        if self.estimated_cost < 0:
            raise ValueError("estimated_cost must not be negative")
        if self.plan_id and not self.plan_id.strip():
            raise ValueError("plan_id must be empty or non-blank")
    @classmethod
    def from_descriptor(cls, campaign_id: CampaignId, descriptor: ShardDescriptor, *, ordinal: int) -> "MutationShard":
        # Convert one shard descriptor while leaving plan authority to coordinator and recovery boundaries.
        return cls(
            shard_id=descriptor.shard_id,
            campaign_id=campaign_id,
            ordinal=ordinal,
            mutant_ids=tuple(MutantId(item) for item in descriptor.mutant_ids),
            estimated_cost=descriptor.estimated_cost,
            plan_id=descriptor.plan_id,
        )
    def matches_plan_descriptor(
        self,
        plan_id: str,
        descriptor: ShardDescriptor,
        *,
        ordinal: int,
    ) -> bool:
        # Compare only immutable topology fields while ignoring lease and execution progress.
        return (
            self.plan_id == str(plan_id)
            and descriptor.plan_id == str(plan_id)
            and self.shard_id == descriptor.shard_id
            and self.ordinal == int(ordinal)
            and tuple(item.value for item in self.mutant_ids) == tuple(descriptor.mutant_ids)
            and self.estimated_cost == descriptor.estimated_cost
        )
    def claim(
        self,
        lease: ShardLease,
        *,
        expected_revision: int,
    ) -> Outcome["MutationShard"]:
        # Claim a shard exactly once for the worker and lease supplied by the coordinator.
        if expected_revision != self.revision_number:
            return _stale_revision("shard", expected_revision, self.revision_number)
        if lease.worker_id != (self.worker_id or lease.worker_id):
            return Rejected(code="shard_worker_mismatch", message="Shard is owned by another worker")
        if lease.attempt < self.attempt:
            return Rejected(
                code="stale_attempt",
                message="Shard lease belongs to an older retry attempt",
                details={"expected_attempt": self.attempt, "received_attempt": lease.attempt},
            )
        if self.status == MutationShardState.COMPLETE:
            return Rejected(code="shard_immutable", message="Completed shard cannot be claimed")
        if self.status == MutationShardState.CANCELLED:
            return Rejected(code="shard_cancelled", message="Cancelled shard cannot be claimed")
        if self.status in {MutationShardState.LEASED, MutationShardState.RUNNING} and self.lease is not None:
            if self.lease.lease_id == lease.lease_id and self.lease.worker_id == lease.worker_id:
                return Success(self, message="Shard lease already applied", duplicate=True)
            return Rejected(code="duplicate_lease", message="Shard already has another active lease")
        return Success(
            replace(
                self,
                status=MutationShardState.LEASED,
                worker_id=lease.worker_id,
                lease=lease,
                attempt=max(self.attempt, lease.attempt),
                revision_number=self.revision_number + 1,
            )
        )
    def start(self, *, expected_revision: int) -> Outcome["MutationShard"]:
        # Move a claimed shard into execution while retaining its lease evidence.
        if expected_revision != self.revision_number:
            return _stale_revision("shard", expected_revision, self.revision_number)
        if self.status == MutationShardState.RUNNING:
            return Success(self, message="Shard already running", duplicate=True)
        if self.status != MutationShardState.LEASED:
            return Rejected(
                code="invalid_shard_transition",
                message="Only a leased shard may start",
                details={"status": self.status.value},
            )
        return Success(replace(self, status=MutationShardState.RUNNING, revision_number=self.revision_number + 1))
    def renew_lease(
        self,
        lease: ShardLease,
        *,
        expected_revision: int,
        now: str | None = None,
    ) -> Outcome["MutationShard"]:
        # Extend the current lease only when ownership matches and the current lease is not expired.
        if expected_revision != self.revision_number:
            return _stale_revision("shard", expected_revision, self.revision_number)
        if self.lease is None or self.worker_id != lease.worker_id:
            return Rejected(code="worker_mismatch", message="Shard is owned by another worker")
        if self.lease.lease_id != lease.lease_id:
            return Rejected(code="stale_lease", message="Lease token is no longer current")
        if self.lease.worker_instance_id and self.lease.worker_instance_id != lease.worker_instance_id:
            return Rejected(code="stale_worker_instance", message="Lease belongs to another worker instance")
        if self.lease.process_birth_token and self.lease.process_birth_token != lease.process_birth_token:
            return Rejected(code="stale_process_birth_token", message="Lease belongs to another process birth")
        if lease.attempt != self.attempt:
            return Rejected(code="stale_attempt", message="Lease attempt is no longer current")
        if now is not None and self.lease_expired(now):
            return Rejected(code="lease_expired", message="Expired lease cannot be renewed")
        if lease.heartbeat_seq < self.lease.heartbeat_seq:
            return Rejected(code="stale_heartbeat", message="Heartbeat sequence moved backwards")
        if lease == self.lease:
            return Success(self, message="Lease heartbeat already applied", duplicate=True)
        return Success(
            replace(
                self,
                status=MutationShardState.RUNNING,
                lease=lease,
                revision_number=self.revision_number + 1,
            )
        )
    def orphan(self, *, expected_revision: int) -> Outcome["MutationShard"]:
        # Invalidate an expired lease without deleting its evidence or shard membership.
        if expected_revision != self.revision_number:
            return _stale_revision("shard", expected_revision, self.revision_number)
        if self.status == MutationShardState.ORPHANED:
            return Success(self, message="Shard is already orphaned", duplicate=True)
        if self.status not in {MutationShardState.LEASED, MutationShardState.RUNNING} or self.lease is None:
            return Rejected(code="shard_not_active", message="Only an active leased shard can be orphaned")
        orphaned_lease = replace(self.lease, status=WorkerStatus.ORPHANED)
        return Success(
            replace(
                self,
                status=MutationShardState.ORPHANED,
                lease=orphaned_lease,
                revision_number=self.revision_number + 1,
            )
        )
    def close_lease(
        self,
        lease: ShardLease,
        *,
        expected_revision: int,
    ) -> Outcome["MutationShard"]:
        # Replace the active lease projection with its terminal generation state after fan-in.
        if expected_revision != self.revision_number:
            return _stale_revision("shard", expected_revision, self.revision_number)
        if self.lease is None or self.worker_id is None:
            return Rejected(code="lease_missing", message="Shard has no lease projection to close")
        if (
            self.lease.lease_id != lease.lease_id
            or self.worker_id != lease.worker_id
            or self.attempt != lease.attempt
        ):
            return Rejected(code="stale_lease", message="Terminal lease differs from shard ownership")
        if self.status not in {
            MutationShardState.COMPLETE,
            MutationShardState.FAILED,
            MutationShardState.CANCELLED,
            MutationShardState.PARTIAL,
            MutationShardState.ORPHANED,
        }:
            return Rejected(code="shard_not_terminal", message="Active shard lease cannot be closed before fan-in")
        if self.lease == lease:
            return Success(self, message="Shard terminal lease already projected", duplicate=True)
        return Success(replace(self, lease=lease, revision_number=self.revision_number + 1))
    def lease_expired(self, now: str) -> bool:
        # Determine expiration from the durable heartbeat and lease duration in UTC.
        if self.lease is None or self.status not in {MutationShardState.LEASED, MutationShardState.RUNNING}:
            return False
        heartbeat = str(self.lease.heartbeat_at)
        normalized_heartbeat = heartbeat[:-1] + "+00:00" if heartbeat.endswith("Z") else heartbeat
        normalized_now = str(now)
        normalized_now = normalized_now[:-1] + "+00:00" if normalized_now.endswith("Z") else normalized_now
        try:
            heartbeat_at = datetime.fromisoformat(normalized_heartbeat)
            now_at = datetime.fromisoformat(normalized_now)
        except ValueError:
            return True
        if heartbeat_at.tzinfo is None or now_at.tzinfo is None:
            return True
        return (now_at.astimezone(timezone.utc) - heartbeat_at.astimezone(timezone.utc)).total_seconds() > max(0.0, self.lease.lease_seconds)
    def record_completion(
        self,
        completed_count: int,
        *,
        expected_revision: int,
        failed: bool = False,
        cancelled: bool = False,
    ) -> Outcome["MutationShard"]:
        # Record one terminal or partial shard result without changing membership.
        if expected_revision != self.revision_number:
            return _stale_revision("shard", expected_revision, self.revision_number)
        if completed_count < 0 or completed_count > len(self.mutant_ids):
            return Rejected(code="invalid_shard_progress", message="Shard completed count is invalid")
        target = (
            MutationShardState.CANCELLED
            if cancelled
            else MutationShardState.FAILED
            if failed
            else MutationShardState.COMPLETE
            if completed_count == len(self.mutant_ids)
            else MutationShardState.PARTIAL
        )
        if self.status == target and self.completed_count == completed_count:
            return Success(self, message="Shard result already applied", duplicate=True)
        if self.status == MutationShardState.COMPLETE:
            return Rejected(code="shard_immutable", message="Completed shard cannot be rewritten")
        return Success(
            replace(
                self,
                status=target,
                completed_count=completed_count,
                revision_number=self.revision_number + 1,
            )
        )
    def retry(self, *, expected_revision: int) -> Outcome["MutationShard"]:
        # Requeue only an incomplete or failed shard and increment its attempt number.
        if expected_revision != self.revision_number:
            return _stale_revision("shard", expected_revision, self.revision_number)
        if self.status not in {
            MutationShardState.PARTIAL,
            MutationShardState.FAILED,
            MutationShardState.ORPHANED,
        }:
            return Rejected(code="shard_not_retryable", message="Shard is not eligible for retry")
        return Success(
            replace(
                self,
                status=MutationShardState.CREATED,
                worker_id=None,
                lease=None,
                attempt=self.attempt + 1,
                completed_count=0,
                revision_number=self.revision_number + 1,
            )
        )
    def reassign(
        self,
        lease: ShardLease,
        *,
        expected_revision: int,
    ) -> Outcome["MutationShard"]:
        # Replace one orphaned ownership generation with exactly one next-attempt lease.
        if expected_revision != self.revision_number:
            return _stale_revision("shard", expected_revision, self.revision_number)
        if self.status != MutationShardState.ORPHANED:
            return Rejected(
                code="shard_not_orphaned",
                message="Only an orphaned shard can be reassigned to a replacement PID",
            )
        if lease.attempt != self.attempt + 1:
            return Rejected(
                code="invalid_reassignment_attempt",
                message="Replacement lease must advance the shard attempt exactly once",
                details={"current_attempt": self.attempt, "received_attempt": lease.attempt},
            )
        if self.lease is None or self.lease.status != WorkerStatus.ORPHANED:
            return Rejected(
                code="orphan_evidence_missing",
                message="Shard reassignment requires a terminal orphaned lease projection",
            )
        if not lease.worker_instance_id:
            return Rejected(
                code="replacement_instance_missing",
                message="Replacement lease requires a new worker instance identity",
            )
        if self.lease.worker_instance_id == lease.worker_instance_id:
            return Rejected(
                code="worker_instance_reused",
                message="Reassignment cannot reuse the orphaned worker instance",
            )
        if self.lease.lease_id == lease.lease_id:
            return Rejected(
                code="lease_generation_reused",
                message="Replacement assignment requires a new lease generation",
            )
        return Success(
            replace(
                self,
                status=MutationShardState.LEASED,
                worker_id=lease.worker_id,
                lease=lease,
                attempt=lease.attempt,
                completed_count=0,
                revision_number=self.revision_number + 1,
            )
        )
    def cancel(self, *, expected_revision: int) -> Outcome["MutationShard"]:
        # Mark an owned shard cancelled while preserving the last lease for reconciliation.
        if expected_revision != self.revision_number:
            return _stale_revision("shard", expected_revision, self.revision_number)
        if self.status == MutationShardState.CANCELLED:
            return Success(self, message="Shard already cancelled", duplicate=True)
        if self.status == MutationShardState.COMPLETE:
            return Rejected(code="shard_immutable", message="Completed shard cannot be cancelled")
        return Success(replace(self, status=MutationShardState.CANCELLED, revision_number=self.revision_number + 1))
    def to_dict(self) -> dict[str, Any]:
        # Serialize shard ownership and membership without exposing mutable scheduler objects.
        return {
            "shard_id": self.shard_id.value,
            "campaign_id": self.campaign_id.value,
            "ordinal": self.ordinal,
            "mutant_ids": [item.value for item in self.mutant_ids],
            "estimated_cost": self.estimated_cost,
            "plan_id": self.plan_id,
            "status": self.status.value,
            "worker_id": self.worker_id.value if self.worker_id else None,
            "lease": to_json_value(self.lease) if self.lease else None,
            "attempt": self.attempt,
            "completed_count": self.completed_count,
            "mutant_list_artifact": self.mutant_list_artifact.value if self.mutant_list_artifact else None,
            "revision_number": self.revision_number,
        }
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MutationShard":
        # Restore shard state for a restarted coordinator without reading worker files.
        raw_ids = value.get("mutant_ids", [])
        if not isinstance(raw_ids, (list, tuple)):
            raise ValueError("mutant_ids must be an array")
        raw_lease = value.get("lease")
        return cls(
            shard_id=ShardId(required_string(value, "shard_id")),
            campaign_id=CampaignId(required_string(value, "campaign_id")),
            ordinal=int(value.get("ordinal", 0)),
            mutant_ids=tuple(MutantId(str(item)) for item in raw_ids),
            estimated_cost=float(value.get("estimated_cost", 0.0)),
            plan_id=str(value.get("plan_id", "")).strip(),
            status=MutationShardState(required_string(value, "status")),
            worker_id=WorkerId(str(value["worker_id"])) if value.get("worker_id") else None,
            lease=ShardLease.from_dict(raw_lease) if isinstance(raw_lease, Mapping) else None,
            attempt=int(value.get("attempt", 0)),
            completed_count=int(value.get("completed_count", 0)),
            mutant_list_artifact=ArtifactId(str(value["mutant_list_artifact"]))
            if value.get("mutant_list_artifact")
            else None,
            revision_number=int(value.get("revision_number", 0)),
        )
class MutationLeaseState(str, Enum):
    """Authoritative lifecycle of one shard ownership generation."""
    CLAIMED = "claimed"
    RUNNING = "running"
    DELIVERING = "delivering"
    RELEASED = "released"
    FAILED = "failed"
    CANCELLED = "cancelled"
    ORPHANED = "orphaned"
_TERMINAL_LEASE_STATES = frozenset(
    {
        MutationLeaseState.RELEASED,
        MutationLeaseState.FAILED,
        MutationLeaseState.CANCELLED,
        MutationLeaseState.ORPHANED,
    }
)
@dataclass(frozen=True, slots=True)
class MutationLease:
    """Single authoritative shard-to-worker ownership generation."""
    campaign_id: CampaignId
    shard_id: ShardId
    lease_id: str
    worker_id: WorkerId
    attempt: int
    lease_seconds: float
    heartbeat_at: str
    heartbeat_sequence: int = 0
    worker_heartbeat_sequence: int = 0
    status: MutationLeaseState = MutationLeaseState.CLAIMED
    worker_instance_id: str | None = None
    process_id: int | None = None
    process_birth_token: str | None = None
    revision_number: int = 0
    def __post_init__(self) -> None:
        # Validate lease identity and require complete process fencing once a process is bound.
        if not isinstance(self.campaign_id, CampaignId) or not isinstance(self.shard_id, ShardId):
            raise TypeError("campaign_id and shard_id must be typed identifiers")
        if not isinstance(self.worker_id, WorkerId):
            raise TypeError("worker_id must be WorkerId")
        if not self.lease_id:
            raise ValueError("lease_id must be non-empty")
        if int(self.attempt) < 0 or float(self.lease_seconds) <= 0:
            raise ValueError("lease attempt and duration are invalid")
        if min(int(self.heartbeat_sequence), int(self.worker_heartbeat_sequence), int(self.revision_number)) < 0:
            raise ValueError("lease counters must not be negative")
        if self.process_id is not None and self.process_birth_token is None:
            raise ValueError("lease process_id requires a process birth token")
        if self.process_id is not None and int(self.process_id) <= 0:
            raise ValueError("lease process_id must be positive")
        normalized = self.heartbeat_at[:-1] + "+00:00" if self.heartbeat_at.endswith("Z") else self.heartbeat_at
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            raise ValueError("lease heartbeat_at must be timezone-aware")
    @classmethod
    def claim(
        cls,
        campaign_id: CampaignId,
        shard_id: ShardId,
        lease: ShardLease,
    ) -> "MutationLease":
        # Create one ownership generation from the public claim contract.
        return cls(
            campaign_id=campaign_id,
            shard_id=shard_id,
            lease_id=lease.lease_id,
            worker_id=lease.worker_id,
            attempt=lease.attempt,
            lease_seconds=lease.lease_seconds,
            heartbeat_at=lease.heartbeat_at,
            heartbeat_sequence=lease.heartbeat_seq,
            worker_heartbeat_sequence=0,
            status=MutationLeaseState.CLAIMED,
            worker_instance_id=lease.worker_instance_id,
            process_birth_token=lease.process_birth_token,
        )
    @property
    def registry_key(self) -> str:
        # Use the immutable lease token as the durable ownership-generation key.
        return self.lease_id
    @property
    def expires_at(self) -> str:
        # Derive the receipt expiration from authoritative heartbeat time and duration.
        normalized = self.heartbeat_at[:-1] + "+00:00" if self.heartbeat_at.endswith("Z") else self.heartbeat_at
        heartbeat = datetime.fromisoformat(normalized).astimezone(timezone.utc)
        return (heartbeat + timedelta(seconds=float(self.lease_seconds))).isoformat().replace("+00:00", "Z")
    def expired(self, now: str) -> bool:
        # Compare one UTC instant against the derived authoritative expiration boundary.
        normalized_now = now[:-1] + "+00:00" if now.endswith("Z") else now
        normalized_expiry = self.expires_at[:-1] + "+00:00" if self.expires_at.endswith("Z") else self.expires_at
        try:
            current = datetime.fromisoformat(normalized_now)
            expiry = datetime.fromisoformat(normalized_expiry)
        except ValueError:
            return True
        if current.tzinfo is None or expiry.tzinfo is None:
            return True
        return current.astimezone(timezone.utc) > expiry.astimezone(timezone.utc)
    def to_shard_lease(self) -> ShardLease:
        # Project the authoritative generation into the existing public shard contract.
        status_map = {
            MutationLeaseState.CLAIMED: WorkerStatus.LEASED,
            MutationLeaseState.RUNNING: WorkerStatus.RUNNING,
            MutationLeaseState.DELIVERING: WorkerStatus.RUNNING,
            MutationLeaseState.RELEASED: WorkerStatus.COMPLETE,
            MutationLeaseState.FAILED: WorkerStatus.ERROR,
            MutationLeaseState.CANCELLED: WorkerStatus.CANCELLED,
            MutationLeaseState.ORPHANED: WorkerStatus.ORPHANED,
        }
        return ShardLease(
            worker_id=self.worker_id,
            lease_id=self.lease_id,
            status=status_map[self.status],
            lease_seconds=self.lease_seconds,
            heartbeat_at=self.heartbeat_at,
            heartbeat_seq=self.heartbeat_sequence,
            attempt=self.attempt,
            worker_instance_id=self.worker_instance_id,
            process_birth_token=self.process_birth_token,
        )
    def renew_claim(
        self,
        *,
        heartbeat_at: str,
        heartbeat_sequence: int,
        expected_revision: int,
        now: str | None = None,
    ) -> Outcome["MutationLease"]:
        # Renew a pre-registration claim without inventing a worker process identity.
        if expected_revision != self.revision_number:
            return _stale_revision("lease", expected_revision, self.revision_number)
        if self.process_id is not None or self.process_birth_token is not None:
            return Rejected(code="lease_already_bound", message="Bound lease requires a worker heartbeat")
        if self.status not in {MutationLeaseState.CLAIMED, MutationLeaseState.RUNNING}:
            return Rejected(code="lease_not_active", message="Only an active claim can be renewed")
        if now is not None and self.expired(now):
            return Rejected(code="lease_expired", message="Expired lease cannot be renewed")
        if int(heartbeat_sequence) <= self.heartbeat_sequence:
            return Rejected(code="stale_lease_heartbeat", message="Lease heartbeat sequence did not advance")
        return Success(
            replace(
                self,
                status=MutationLeaseState.RUNNING,
                heartbeat_at=str(heartbeat_at),
                heartbeat_sequence=int(heartbeat_sequence),
                revision_number=self.revision_number + 1,
            )
        )
    def bind(self, identity: WorkerIdentity, *, expected_revision: int) -> Outcome["MutationLease"]:
        # Bind the pre-engine ownership generation to one registered process instance.
        if expected_revision != self.revision_number:
            return _stale_revision("lease", expected_revision, self.revision_number)
        if self.status not in {MutationLeaseState.CLAIMED, MutationLeaseState.RUNNING}:
            return Rejected(code="lease_not_bindable", message="Only an active claimed lease can bind a worker")
        if identity.worker_id != self.worker_id.value:
            return Rejected(code="lease_worker_mismatch", message="Lease belongs to another worker slot")
        if self.worker_instance_id is not None and self.worker_instance_id != identity.instance_id:
            return Rejected(code="stale_worker_instance", message="Lease belongs to another worker instance")
        if self.process_birth_token is not None and self.process_birth_token != identity.process_birth_token:
            return Rejected(code="stale_process_birth_token", message="Lease belongs to another process birth")
        if (
            self.worker_instance_id == identity.instance_id
            and self.process_id == identity.process_id
            and self.process_birth_token == identity.process_birth_token
            and self.status == MutationLeaseState.RUNNING
        ):
            return Success(self, message="Lease process binding already applied", duplicate=True)
        return Success(
            replace(
                self,
                status=MutationLeaseState.RUNNING,
                worker_instance_id=identity.instance_id,
                process_id=identity.process_id,
                process_birth_token=identity.process_birth_token,
                revision_number=self.revision_number + 1,
            )
        )
    def renew(
        self,
        heartbeat: WorkerHeartbeat,
        *,
        expected_revision: int,
        now: str | None = None,
    ) -> Outcome["MutationLease"]:
        # Advance one bound lease only from its exact worker process and assignment heartbeat.
        if expected_revision != self.revision_number:
            return _stale_revision("lease", expected_revision, self.revision_number)
        if self.status not in {MutationLeaseState.RUNNING, MutationLeaseState.DELIVERING}:
            return Rejected(code="lease_not_active", message="Only a running lease can be renewed")
        identity = heartbeat.worker
        if (
            identity.worker_id != self.worker_id.value
            or identity.instance_id != self.worker_instance_id
            or identity.process_id != self.process_id
            or identity.process_birth_token != self.process_birth_token
        ):
            return Rejected(code="stale_worker_instance", message="Heartbeat belongs to another lease process")
        if (
            heartbeat.current_campaign_id != self.campaign_id.value
            or heartbeat.current_shard_id != self.shard_id.value
            or heartbeat.current_lease_id != self.lease_id
            or heartbeat.current_attempt != self.attempt
        ):
            return Rejected(code="lease_assignment_mismatch", message="Heartbeat assignment differs from lease ownership")
        if now is not None and self.expired(now):
            return Rejected(code="lease_expired", message="Expired lease cannot be renewed")
        if int(heartbeat.sequence) <= self.worker_heartbeat_sequence:
            return Rejected(
                code="stale_lease_heartbeat",
                message="Worker heartbeat sequence did not advance",
                details={"current": self.worker_heartbeat_sequence, "received": int(heartbeat.sequence)},
            )
        return Success(
            replace(
                self,
                heartbeat_at=heartbeat.sent_at,
                heartbeat_sequence=self.heartbeat_sequence + 1,
                worker_heartbeat_sequence=int(heartbeat.sequence),
                revision_number=self.revision_number + 1,
            )
        )
    def begin_delivery(self, *, expected_revision: int) -> Outcome["MutationLease"]:
        # Fence result delivery behind the current active ownership generation.
        if expected_revision != self.revision_number:
            return _stale_revision("lease", expected_revision, self.revision_number)
        if self.status == MutationLeaseState.DELIVERING:
            return Success(self, message="Lease already delivering", duplicate=True)
        if self.status != MutationLeaseState.RUNNING:
            return Rejected(code="lease_not_running", message="Only a running lease can begin delivery")
        return Success(replace(self, status=MutationLeaseState.DELIVERING, revision_number=self.revision_number + 1))
    def release(self, *, expected_revision: int) -> Outcome["MutationLease"]:
        # Close the ownership generation only after authoritative delivery acknowledgement.
        if expected_revision != self.revision_number:
            return _stale_revision("lease", expected_revision, self.revision_number)
        if self.status == MutationLeaseState.RELEASED:
            return Success(self, message="Lease already released", duplicate=True)
        if self.status not in {MutationLeaseState.RUNNING, MutationLeaseState.DELIVERING}:
            return Rejected(code="lease_not_releasable", message="Lease is not active or delivering")
        return Success(replace(self, status=MutationLeaseState.RELEASED, revision_number=self.revision_number + 1))
    def terminate(
        self,
        target: MutationLeaseState,
        *,
        expected_revision: int,
    ) -> Outcome["MutationLease"]:
        # End an active ownership generation as failed, cancelled or orphaned.
        if expected_revision != self.revision_number:
            return _stale_revision("lease", expected_revision, self.revision_number)
        if target not in {MutationLeaseState.FAILED, MutationLeaseState.CANCELLED, MutationLeaseState.ORPHANED}:
            return Rejected(code="invalid_lease_terminal_state", message="Unsupported lease terminal state")
        if self.status == target:
            return Success(self, message="Lease terminal transition already applied", duplicate=True)
        if self.status in _TERMINAL_LEASE_STATES:
            return Rejected(code="lease_terminal", message="Terminal lease cannot change state")
        return Success(replace(self, status=target, revision_number=self.revision_number + 1))
    def to_dict(self) -> dict[str, Any]:
        # Serialize one authoritative lease generation for SQLite restart recovery.
        return {
            "campaign_id": self.campaign_id.value,
            "shard_id": self.shard_id.value,
            "lease_id": self.lease_id,
            "worker_id": self.worker_id.value,
            "attempt": self.attempt,
            "lease_seconds": self.lease_seconds,
            "heartbeat_at": self.heartbeat_at,
            "heartbeat_sequence": self.heartbeat_sequence,
            "worker_heartbeat_sequence": self.worker_heartbeat_sequence,
            "status": self.status.value,
            "worker_instance_id": self.worker_instance_id,
            "process_id": self.process_id,
            "process_birth_token": self.process_birth_token,
            "revision_number": self.revision_number,
        }
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MutationLease":
        # Restore one authoritative lease without reading shard or worker projections.
        return cls(
            campaign_id=CampaignId(required_string(value, "campaign_id")),
            shard_id=ShardId(required_string(value, "shard_id")),
            lease_id=required_string(value, "lease_id"),
            worker_id=WorkerId(required_string(value, "worker_id")),
            attempt=int(value.get("attempt", 0)),
            lease_seconds=float(value.get("lease_seconds", 10.0)),
            heartbeat_at=required_string(value, "heartbeat_at"),
            heartbeat_sequence=int(value.get("heartbeat_sequence", 0)),
            worker_heartbeat_sequence=int(value.get("worker_heartbeat_sequence", 0)),
            status=MutationLeaseState(required_string(value, "status")),
            worker_instance_id=value.get("worker_instance_id"),
            process_id=int(value["process_id"]) if value.get("process_id") is not None else None,
            process_birth_token=value.get("process_birth_token"),
            revision_number=int(value.get("revision_number", 0)),
        )
class MutationWorkerState(str, Enum):
    """Authoritative lifecycle states for one registered worker process instance."""
    REGISTERING = "registering"
    IDLE = "idle"
    RUNNING = "running"
    DELIVERING = "delivering"
    DRAINING = "draining"
    STOPPED = "stopped"
    FAILED = "failed"
    ORPHANED = "orphaned"
_TERMINAL_WORKER_STATES = frozenset(
    {MutationWorkerState.STOPPED, MutationWorkerState.FAILED, MutationWorkerState.ORPHANED}
)
@dataclass(frozen=True, slots=True)
class MutationWorker:
    """Durable worker registration, liveness and assignment ownership aggregate."""
    campaign_id: CampaignId
    identity: WorkerIdentity
    capabilities: WorkerCapabilities
    workspace: str
    spool_path: str
    launcher_process_id: int | None = None
    status: MutationWorkerState = MutationWorkerState.IDLE
    heartbeat_sequence: int = 0
    last_heartbeat_at: str = ""
    current_shard_id: str | None = None
    current_lease_id: str | None = None
    current_attempt: int | None = None
    current_mutant_id: str | None = None
    child_process_id: int | None = None
    last_child_process_id: int | None = None
    completed_mutants: int = 0
    completed_assignments: int = 0
    workspace_healthy: bool = True
    revision_number: int = 0
    launcher_process_birth_token: str | None = None
    child_process_birth_token: str | None = None
    last_child_process_birth_token: str | None = None
    def __post_init__(self) -> None:
        # Validate authoritative worker identity, counters and active assignment coherence.
        if not isinstance(self.campaign_id, CampaignId):
            raise TypeError("campaign_id must be CampaignId")
        if not isinstance(self.identity, WorkerIdentity) or not isinstance(self.capabilities, WorkerCapabilities):
            raise TypeError("identity and capabilities must use public worker contracts")
        if not self.workspace or not self.spool_path:
            raise ValueError("worker workspace and spool_path must be non-empty")
        if self.launcher_process_id is not None and int(self.launcher_process_id) <= 0:
            raise ValueError("launcher_process_id must be positive when present")
        if self.launcher_process_birth_token is not None and self.launcher_process_id is None:
            raise ValueError("launcher_process_birth_token requires launcher_process_id")
        if self.child_process_id is not None and int(self.child_process_id) <= 0:
            raise ValueError("child_process_id must be positive when present")
        if self.last_child_process_id is not None and int(self.last_child_process_id) <= 0:
            raise ValueError("last_child_process_id must be positive when present")
        if self.child_process_birth_token is not None and self.child_process_id is None:
            raise ValueError("child_process_birth_token requires child_process_id")
        if self.last_child_process_birth_token is not None and self.last_child_process_id is None:
            raise ValueError("last_child_process_birth_token requires last_child_process_id")
        if min(
            int(self.heartbeat_sequence),
            int(self.completed_mutants),
            int(self.completed_assignments),
            int(self.revision_number),
        ) < 0:
            raise ValueError("worker counters must not be negative")
        active_fields = (self.current_shard_id, self.current_lease_id, self.current_attempt)
        if any(item is not None for item in active_fields) and not all(item is not None for item in active_fields):
            raise ValueError("worker assignment identity must be complete or empty")
        if self.current_attempt is not None and int(self.current_attempt) < 0:
            raise ValueError("current_attempt must not be negative")
    @property
    def worker_id(self) -> str:
        # Expose the reusable worker slot identity without discarding process-instance fencing.
        return self.identity.worker_id
    @property
    def registry_key(self) -> str:
        # Scope reusable local worker IDs by campaign inside the authoritative store.
        return f"{self.campaign_id.value}\x1f{self.identity.worker_id}"
    @classmethod
    def register(
        cls,
        campaign_id: CampaignId,
        identity: WorkerIdentity,
        capabilities: WorkerCapabilities,
        *,
        workspace: str,
        spool_path: str,
        launcher_process_id: int | None = None,
        launcher_process_birth_token: str | None = None,
    ) -> "MutationWorker":
        # Create an accepted idle registration after transport and capability validation.
        return cls(
            campaign_id=campaign_id,
            identity=identity,
            capabilities=capabilities,
            workspace=str(workspace),
            spool_path=str(spool_path),
            launcher_process_id=launcher_process_id,
            launcher_process_birth_token=launcher_process_birth_token,
            status=MutationWorkerState.IDLE,
        )
    def replace_registration(
        self,
        identity: WorkerIdentity,
        capabilities: WorkerCapabilities,
        *,
        workspace: str,
        spool_path: str,
        launcher_process_id: int | None,
        launcher_process_birth_token: str | None = None,
        expected_revision: int,
    ) -> Outcome["MutationWorker"]:
        # Fence active instances while allowing a terminal worker slot to register a new process instance.
        if expected_revision != self.revision_number:
            return _stale_revision("worker", expected_revision, self.revision_number)
        if self.status not in _TERMINAL_WORKER_STATES:
            if (
                self.identity == identity
                and self.capabilities == capabilities
                and self.workspace == str(workspace)
                and self.spool_path == str(spool_path)
                and self.launcher_process_id == launcher_process_id
                and self.launcher_process_birth_token == launcher_process_birth_token
            ):
                return Success(self, message="Worker registration already active", duplicate=True)
            return Rejected(
                code="worker_slot_in_use",
                message="Worker slot is already owned by another active process instance",
                details={"worker_id": self.worker_id, "instance_id": self.identity.instance_id},
            )
        return Success(
            MutationWorker.register(
                self.campaign_id,
                identity,
                capabilities,
                workspace=workspace,
                spool_path=spool_path,
                launcher_process_id=launcher_process_id,
                launcher_process_birth_token=launcher_process_birth_token,
            )._with_revision(self.revision_number + 1)
        )
    def _with_revision(self, revision_number: int) -> "MutationWorker":
        # Replace only the optimistic-concurrency revision for create-versus-reregister flows.
        return replace(self, revision_number=int(revision_number))
    def supports(
        self,
        *,
        engine_protocol_version: int,
        workspace_backend: str,
        python_version: str | None = None,
    ) -> bool:
        # Match only explicitly advertised execution capabilities before assignment.
        if int(engine_protocol_version) not in self.capabilities.engine_protocol_versions:
            return False
        if str(workspace_backend) not in self.capabilities.workspace_backends:
            return False
        return python_version is None or str(python_version) in self.capabilities.python_versions
    def assign(
        self,
        *,
        shard_id: str,
        lease_id: str,
        attempt: int,
        expected_revision: int,
        engine_protocol_version: int = 1,
        workspace_backend: str = "copy",
        python_version: str | None = None,
    ) -> Outcome["MutationWorker"]:
        # Bind exactly one active assignment after lifecycle and capability checks.
        if expected_revision != self.revision_number:
            return _stale_revision("worker", expected_revision, self.revision_number)
        if self.status != MutationWorkerState.IDLE:
            return Rejected(
                code="worker_not_idle",
                message="Only an idle worker can accept an assignment",
                details={"status": self.status.value},
            )
        if not self.supports(
            engine_protocol_version=engine_protocol_version,
            workspace_backend=workspace_backend,
            python_version=python_version,
        ):
            return Rejected(code="worker_capability_mismatch", message="Worker cannot execute this assignment")
        if not shard_id or not lease_id or int(attempt) < 0:
            return Rejected(code="invalid_worker_assignment", message="Worker assignment identity is invalid")
        return Success(
            replace(
                self,
                status=MutationWorkerState.RUNNING,
                current_shard_id=str(shard_id),
                current_lease_id=str(lease_id),
                current_attempt=int(attempt),
                current_mutant_id=None,
                revision_number=self.revision_number + 1,
            )
        )
    def heartbeat(
        self,
        message: WorkerHeartbeat,
        *,
        expected_revision: int,
        completed_assignments: int = 0,
    ) -> Outcome["MutationWorker"]:
        # Advance liveness only for the registered process instance and a strictly newer heartbeat sequence.
        if expected_revision != self.revision_number:
            return _stale_revision("worker", expected_revision, self.revision_number)
        if message.worker != self.identity:
            return Rejected(code="stale_worker_instance", message="Heartbeat belongs to another worker process instance")
        if self.status in _TERMINAL_WORKER_STATES:
            return Rejected(code="worker_terminal", message="Terminal worker cannot renew registration")
        if int(message.sequence) <= self.heartbeat_sequence:
            return Rejected(
                code="stale_worker_heartbeat",
                message="Worker heartbeat sequence did not advance",
                details={"current": self.heartbeat_sequence, "received": int(message.sequence)},
            )
        assignment_fields = (
            message.current_campaign_id,
            message.current_shard_id,
            message.current_lease_id,
            message.current_attempt,
        )
        if self.current_shard_id is None:
            if any(item is not None for item in assignment_fields):
                return Rejected(code="worker_assignment_mismatch", message="Idle worker heartbeat claims an assignment")
        elif any(item is not None for item in assignment_fields):
            if not all(item is not None for item in assignment_fields) or (
                message.current_campaign_id != self.campaign_id.value
                or message.current_shard_id != self.current_shard_id
                or message.current_lease_id != self.current_lease_id
                or message.current_attempt != self.current_attempt
            ):
                return Rejected(code="worker_assignment_mismatch", message="Heartbeat assignment differs from registry ownership")
        last_child = self.last_child_process_id
        last_child_birth = self.last_child_process_birth_token
        current_child_birth = message.child_process_birth_token
        if message.child_process_id is not None:
            if current_child_birth is None and message.child_process_id == self.child_process_id:
                current_child_birth = self.child_process_birth_token
            last_child = int(message.child_process_id)
            if current_child_birth is not None:
                last_child_birth = current_child_birth
        return Success(
            replace(
                self,
                heartbeat_sequence=int(message.sequence),
                last_heartbeat_at=message.sent_at,
                current_mutant_id=message.current_mutant_id,
                child_process_id=message.child_process_id,
                child_process_birth_token=current_child_birth,
                last_child_process_id=last_child,
                last_child_process_birth_token=last_child_birth,
                completed_mutants=max(self.completed_mutants, int(message.completed_mutants)),
                completed_assignments=max(self.completed_assignments, int(completed_assignments)),
                workspace_healthy=bool(message.workspace_healthy),
                revision_number=self.revision_number + 1,
            )
        )
    def mark_delivering(self, *, expected_revision: int) -> Outcome["MutationWorker"]:
        # Preserve assignment ownership while durable evidence waits for authoritative fan-in acknowledgement.
        if expected_revision != self.revision_number:
            return _stale_revision("worker", expected_revision, self.revision_number)
        if self.status != MutationWorkerState.RUNNING:
            return Rejected(code="worker_not_running", message="Only a running worker can deliver evidence")
        return Success(replace(self, status=MutationWorkerState.DELIVERING, revision_number=self.revision_number + 1))
    def release(self, *, expected_revision: int) -> Outcome["MutationWorker"]:
        # Clear one committed assignment and return the persistent process to the idle pool.
        if expected_revision != self.revision_number:
            return _stale_revision("worker", expected_revision, self.revision_number)
        if self.status not in {
            MutationWorkerState.RUNNING,
            MutationWorkerState.DELIVERING,
            MutationWorkerState.DRAINING,
        }:
            return Rejected(code="worker_not_assigned", message="Worker has no releasable assignment")
        next_status = (
            MutationWorkerState.DRAINING
            if self.status == MutationWorkerState.DRAINING
            else MutationWorkerState.IDLE
        )
        return Success(
            replace(
                self,
                status=next_status,
                current_shard_id=None,
                current_lease_id=None,
                current_attempt=None,
                current_mutant_id=None,
                child_process_id=None,
                child_process_birth_token=None,
                completed_assignments=self.completed_assignments + 1,
                revision_number=self.revision_number + 1,
            )
        )
    def drain(self, *, expected_revision: int) -> Outcome["MutationWorker"]:
        # Refuse new work while allowing an assigned worker to finish its current delivery.
        if expected_revision != self.revision_number:
            return _stale_revision("worker", expected_revision, self.revision_number)
        if self.status in _TERMINAL_WORKER_STATES:
            return Rejected(code="worker_terminal", message="Terminal worker cannot enter draining")
        if self.status == MutationWorkerState.DRAINING:
            return Success(self, message="Worker already draining", duplicate=True)
        return Success(replace(self, status=MutationWorkerState.DRAINING, revision_number=self.revision_number + 1))
    def stop(self, *, expected_revision: int) -> Outcome["MutationWorker"]:
        # Persist clean process termination without erasing the last observed engine identity.
        if expected_revision != self.revision_number:
            return _stale_revision("worker", expected_revision, self.revision_number)
        if self.status == MutationWorkerState.STOPPED:
            return Success(self, message="Worker already stopped", duplicate=True)
        if self.status in {MutationWorkerState.FAILED, MutationWorkerState.ORPHANED}:
            return Rejected(code="worker_terminal", message="Failed or orphaned worker cannot become stopped")
        return Success(
            replace(
                self,
                status=MutationWorkerState.STOPPED,
                current_shard_id=None,
                current_lease_id=None,
                current_attempt=None,
                current_mutant_id=None,
                child_process_id=None,
                child_process_birth_token=None,
                revision_number=self.revision_number + 1,
            )
        )
    def fail(self, *, expected_revision: int) -> Outcome["MutationWorker"]:
        # Fence a broken worker instance so no later heartbeat or assignment can revive it.
        if expected_revision != self.revision_number:
            return _stale_revision("worker", expected_revision, self.revision_number)
        if self.status == MutationWorkerState.FAILED:
            return Success(self, message="Worker already failed", duplicate=True)
        if self.status in {MutationWorkerState.STOPPED, MutationWorkerState.ORPHANED}:
            return Rejected(code="worker_terminal", message="Terminal worker cannot change failure state")
        return Success(replace(self, status=MutationWorkerState.FAILED, revision_number=self.revision_number + 1))
    def orphan(self, *, expected_revision: int) -> Outcome["MutationWorker"]:
        # Mark a vanished process instance unrecoverable while preserving its ownership evidence.
        if expected_revision != self.revision_number:
            return _stale_revision("worker", expected_revision, self.revision_number)
        if self.status == MutationWorkerState.ORPHANED:
            return Success(self, message="Worker already orphaned", duplicate=True)
        if self.status in {MutationWorkerState.STOPPED, MutationWorkerState.FAILED}:
            return Rejected(code="worker_terminal", message="Terminal worker cannot become orphaned")
        return Success(replace(self, status=MutationWorkerState.ORPHANED, revision_number=self.revision_number + 1))
    def to_dict(self) -> dict[str, Any]:
        # Serialize the complete worker projection with explicit stable identifier shapes.
        return {
            "campaign_id": self.campaign_id.value,
            "identity": self.identity.to_dict(),
            "capabilities": self.capabilities.to_dict(),
            "workspace": self.workspace,
            "spool_path": self.spool_path,
            "launcher_process_id": self.launcher_process_id,
            "launcher_process_birth_token": self.launcher_process_birth_token,
            "status": self.status.value,
            "heartbeat_sequence": self.heartbeat_sequence,
            "last_heartbeat_at": self.last_heartbeat_at,
            "current_shard_id": self.current_shard_id,
            "current_lease_id": self.current_lease_id,
            "current_attempt": self.current_attempt,
            "current_mutant_id": self.current_mutant_id,
            "child_process_id": self.child_process_id,
            "child_process_birth_token": self.child_process_birth_token,
            "last_child_process_id": self.last_child_process_id,
            "last_child_process_birth_token": self.last_child_process_birth_token,
            "completed_mutants": self.completed_mutants,
            "completed_assignments": self.completed_assignments,
            "workspace_healthy": self.workspace_healthy,
            "revision_number": self.revision_number,
        }
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MutationWorker":
        # Restore a worker aggregate without accepting implicit process identity defaults.
        raw_identity = value.get("identity")
        raw_capabilities = value.get("capabilities")
        if not isinstance(raw_identity, Mapping) or not isinstance(raw_capabilities, Mapping):
            raise ValueError("worker identity and capabilities must be objects")
        return cls(
            campaign_id=CampaignId(required_string(value, "campaign_id")),
            identity=WorkerIdentity.from_dict(raw_identity),
            capabilities=WorkerCapabilities.from_dict(raw_capabilities),
            workspace=required_string(value, "workspace"),
            spool_path=required_string(value, "spool_path"),
            launcher_process_id=(
                int(value["launcher_process_id"])
                if value.get("launcher_process_id") is not None
                else None
            ),
            launcher_process_birth_token=(
                str(value["launcher_process_birth_token"])
                if value.get("launcher_process_birth_token") is not None
                else None
            ),
            status=MutationWorkerState(required_string(value, "status")),
            heartbeat_sequence=max(0, int(value.get("heartbeat_sequence", 0))),
            last_heartbeat_at=str(value.get("last_heartbeat_at", "")),
            current_shard_id=str(value["current_shard_id"]) if value.get("current_shard_id") is not None else None,
            current_lease_id=str(value["current_lease_id"]) if value.get("current_lease_id") is not None else None,
            current_attempt=int(value["current_attempt"]) if value.get("current_attempt") is not None else None,
            current_mutant_id=str(value["current_mutant_id"]) if value.get("current_mutant_id") is not None else None,
            child_process_id=int(value["child_process_id"]) if value.get("child_process_id") is not None else None,
            child_process_birth_token=(
                str(value["child_process_birth_token"])
                if value.get("child_process_birth_token") is not None
                else None
            ),
            last_child_process_id=(
                int(value["last_child_process_id"])
                if value.get("last_child_process_id") is not None
                else None
            ),
            last_child_process_birth_token=(
                str(value["last_child_process_birth_token"])
                if value.get("last_child_process_birth_token") is not None
                else None
            ),
            completed_mutants=max(0, int(value.get("completed_mutants", 0))),
            completed_assignments=max(0, int(value.get("completed_assignments", 0))),
            workspace_healthy=bool(value.get("workspace_healthy", True)),
            revision_number=max(0, int(value.get("revision_number", 0))),
        )
@dataclass(frozen=True, slots=True)
class MutationExecution:
    """Immutable execution aggregate preserving semantic result and evidence references."""
    execution_id: ExecutionId
    campaign_id: CampaignId
    shard_id: ShardId
    mutant_id: MutantId
    attempt: int = 0
    status: MutationExecutionState = MutationExecutionState.PENDING
    semantic_result: MutationResult | str | None = None
    selected_tests: tuple[str, ...] = ()
    artifacts: tuple[ArtifactId, ...] = ()
    duration_seconds: float | None = None
    restore_verified: bool = False
    error: str | None = None
    revision_number: int = 0
    lease_id: str | None = None
    test_observations: tuple[Mapping[str, Any], ...] = ()
    def __post_init__(self) -> None:
        # Validate execution identity and immutable evidence counters before a result is stored.
        if not all(
            isinstance(item, expected)
            for item, expected in (
                (self.execution_id, ExecutionId),
                (self.campaign_id, CampaignId),
                (self.shard_id, ShardId),
                (self.mutant_id, MutantId),
            )
        ):
            raise TypeError("execution identities must use typed identifiers")
        if self.attempt < 0 or self.revision_number < 0:
            raise ValueError("execution counters must not be negative")
        if self.duration_seconds is not None and self.duration_seconds < 0:
            raise ValueError("duration_seconds must not be negative")
    def start(self, *, expected_revision: int) -> Outcome["MutationExecution"]:
        # Mark one execution active without changing its mutant identity or attempt.
        if expected_revision != self.revision_number:
            return _stale_revision("execution", expected_revision, self.revision_number)
        if self.status == MutationExecutionState.RUNNING:
            return Success(self, message="Execution already running", duplicate=True)
        if self.status != MutationExecutionState.PENDING:
            return Rejected(code="execution_immutable", message="Only a pending execution may start")
        return Success(
            replace(self, status=MutationExecutionState.RUNNING, revision_number=self.revision_number + 1)
        )
    def complete(
        self,
        semantic_result: MutationResult | str,
        *,
        expected_revision: int,
        selected_tests: tuple[str, ...] = (),
        artifacts: tuple[ArtifactId, ...] = (),
        duration_seconds: float | None = None,
        restore_verified: bool = False,
        error: str | None = None,
    ) -> Outcome["MutationExecution"]:
        # Publish one immutable semantic result together with selection and restoration evidence.
        if expected_revision != self.revision_number:
            return _stale_revision("execution", expected_revision, self.revision_number)
        if self.status == MutationExecutionState.COMPLETE:
            candidate = replace(
                self,
                semantic_result=semantic_result,
                selected_tests=tuple(selected_tests),
                artifacts=tuple(artifacts),
                duration_seconds=duration_seconds,
                restore_verified=restore_verified,
                error=error,
            )
            if candidate == self:
                return Success(self, message="Execution result already applied", duplicate=True)
            return Rejected(code="execution_immutable", message="Completed execution cannot be rewritten")
        if self.status == MutationExecutionState.CANCELLED:
            return Rejected(code="execution_cancelled", message="Cancelled execution cannot complete")
        return Success(
            replace(
                self,
                status=MutationExecutionState.COMPLETE,
                semantic_result=semantic_result,
                selected_tests=tuple(selected_tests),
                artifacts=tuple(artifacts),
                duration_seconds=duration_seconds,
                restore_verified=restore_verified,
                error=error,
                revision_number=self.revision_number + 1,
            )
        )
    def fail(self, *, expected_revision: int, error: str) -> Outcome["MutationExecution"]:
        # Preserve a technical execution failure as data for retry and reconciliation.
        if expected_revision != self.revision_number:
            return _stale_revision("execution", expected_revision, self.revision_number)
        if self.status == MutationExecutionState.FAILED and self.error == error:
            return Success(self, message="Execution failure already applied", duplicate=True)
        if self.status in {MutationExecutionState.COMPLETE, MutationExecutionState.CANCELLED}:
            return Rejected(code="execution_immutable", message="Terminal execution cannot fail")
        return Success(
            replace(
                self,
                status=MutationExecutionState.FAILED,
                semantic_result=MutationResult.ERROR,
                error=error,
                revision_number=self.revision_number + 1,
            )
        )
    def cancel(self, *, expected_revision: int) -> Outcome["MutationExecution"]:
        # Mark an unfinished execution cancelled without erasing its last known evidence.
        if expected_revision != self.revision_number:
            return _stale_revision("execution", expected_revision, self.revision_number)
        if self.status == MutationExecutionState.CANCELLED:
            return Success(self, message="Execution already cancelled", duplicate=True)
        if self.status == MutationExecutionState.COMPLETE:
            return Rejected(code="execution_immutable", message="Completed execution cannot cancel")
        return Success(
            replace(self, status=MutationExecutionState.CANCELLED, revision_number=self.revision_number + 1)
        )
    def to_dict(self) -> dict[str, Any]:
        # Serialize execution evidence as a compact restart-safe JSON projection.
        return {
            "execution_id": self.execution_id.value,
            "campaign_id": self.campaign_id.value,
            "shard_id": self.shard_id.value,
            "mutant_id": self.mutant_id.value,
            "attempt": self.attempt,
            "status": self.status.value,
            "semantic_result": to_json_value(self.semantic_result),
            "selected_tests": list(self.selected_tests),
            "artifacts": [item.value for item in self.artifacts],
            "duration_seconds": self.duration_seconds,
            "restore_verified": self.restore_verified,
            "error": self.error,
            "revision_number": self.revision_number,
            "lease_id": self.lease_id,
            "test_observations": [dict(item) for item in self.test_observations],
        }
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MutationExecution":
        # Restore one immutable execution row without loading output artifacts into memory.
        raw_tests = value.get("selected_tests", [])
        raw_artifacts = value.get("artifacts", [])
        raw_observations = value.get("test_observations", [])
        if (
            not isinstance(raw_tests, (list, tuple))
            or not isinstance(raw_artifacts, (list, tuple))
            or not isinstance(raw_observations, (list, tuple))
            or any(not isinstance(item, Mapping) for item in raw_observations)
        ):
            raise ValueError("selected_tests, artifacts and test_observations must be arrays")
        raw_result = value.get("semantic_result")
        try:
            semantic_result: MutationResult | str | None = (
                MutationResult(str(raw_result)) if raw_result is not None else None
            )
        except ValueError:
            semantic_result = str(raw_result)
        return cls(
            execution_id=ExecutionId(required_string(value, "execution_id")),
            campaign_id=CampaignId(required_string(value, "campaign_id")),
            shard_id=ShardId(required_string(value, "shard_id")),
            mutant_id=MutantId(required_string(value, "mutant_id")),
            attempt=int(value.get("attempt", 0)),
            status=MutationExecutionState(required_string(value, "status")),
            semantic_result=semantic_result,
            selected_tests=tuple(str(item) for item in raw_tests),
            artifacts=tuple(ArtifactId(str(item)) for item in raw_artifacts),
            duration_seconds=(
                float(value["duration_seconds"])
                if value.get("duration_seconds") is not None
                else None
            ),
            restore_verified=bool(value.get("restore_verified", False)),
            error=str(value["error"]) if value.get("error") is not None else None,
            revision_number=int(value.get("revision_number", 0)),
            lease_id=str(value["lease_id"]) if value.get("lease_id") is not None else None,
            test_observations=tuple(dict(item) for item in raw_observations),
        )
