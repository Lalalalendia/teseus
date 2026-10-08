"""In-memory and SQLite repositories for mutation aggregates and effect receipts."""
from __future__ import annotations
import sqlite3
from collections import Counter
from dataclasses import dataclass, replace
from pathlib import Path
from threading import RLock
from typing import Any, Mapping, Protocol
from theseus_contracts import (
    ArtifactRegistryEntry,
    CampaignId,
    ExecutionId,
    FinalizationIntent,
    ShardId,
)
from theseus_contracts.serialization import dumps, loads_object, utc_now
from .domain import (
    MutationCampaign,
    MutationExecution,
    MutationLease,
    MutationShard,
    MutationWorker,
    OperatorAction,
    OperatorActionStatus,
)
from .outcomes import Failed, Outcome, Rejected, Success, is_success
from theseus_performance.sqlite_metrics import SQLiteMetrics

_SQLITE_VARIABLE_CHUNK = 900

@dataclass(frozen=True, slots=True)
class EffectReceipt:
    """Durable idempotency record containing the exact successful domain projection."""
    effect_id: str
    effect_type: str
    campaign_id: CampaignId
    payload: Mapping[str, Any]
    def to_dict(self) -> dict[str, Any]:
        # Convert the receipt to JSON data while retaining future payload extensions.
        return {
            "effect_id": self.effect_id,
            "effect_type": self.effect_type,
            "campaign_id": self.campaign_id.value,
            "payload": dict(self.payload),
        }
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "EffectReceipt":
        # Restore one effect receipt without executing the original handler again.
        payload = value.get("payload", {})
        if not isinstance(payload, Mapping):
            raise ValueError("effect payload must be an object")
        return cls(
            effect_id=str(value["effect_id"]),
            effect_type=str(value["effect_type"]),
            campaign_id=CampaignId(str(value["campaign_id"])),
            payload=dict(payload),
        )
class MutationStore(Protocol):
    """Repository boundary consumed by Gallifrey mutation application services."""
    def get_campaign(self, campaign_id: CampaignId) -> Outcome[MutationCampaign | None]:
        # Load one campaign aggregate or an explicit empty result.
        ...
    def save_campaign(
        self,
        campaign: MutationCampaign,
        *,
        expected_revision: int | None,
    ) -> Outcome[MutationCampaign]:
        # Persist a campaign with optimistic concurrency and immutable terminal checks.
        ...
    def commit_campaign_creation(
        self,
        action: OperatorAction,
        campaign: MutationCampaign,
    ) -> Outcome[OperatorAction]:
        # Atomically create one campaign and its terminal create-action receipt.
        ...
    def get_shard(self, shard_id: ShardId) -> Outcome[MutationShard | None]:
        # Load one shard aggregate by its stable identity.
        ...
    def save_shard(self, shard: MutationShard, *, expected_revision: int | None) -> Outcome[MutationShard]:
        # Persist shard ownership and progress under optimistic concurrency.
        ...
    def list_shards(self, campaign_id: CampaignId) -> Outcome[tuple[MutationShard, ...]]:
        # Load all shard projections owned by one campaign.
        ...
    def get_lease(self, lease_id: str) -> Outcome[MutationLease | None]:
        # Load one authoritative lease generation by immutable token.
        ...
    def save_lease(
        self,
        lease: MutationLease,
        *,
        expected_revision: int | None,
    ) -> Outcome[MutationLease]:
        # Persist one authoritative lease generation with optimistic concurrency.
        ...
    def get_worker(self, campaign_id: CampaignId, worker_id: str) -> Outcome[MutationWorker | None]:
        # Load one authoritative worker registration scoped to a campaign.
        ...
    def save_worker(
        self,
        worker: MutationWorker,
        *,
        expected_revision: int | None,
    ) -> Outcome[MutationWorker]:
        # Persist one worker aggregate with optimistic concurrency.
        ...
    def list_workers(self, campaign_id: CampaignId) -> Outcome[tuple[MutationWorker, ...]]:
        # Load every worker projection for diagnostic projection and restart recovery.
        ...
    def get_execution(self, execution_id: ExecutionId) -> Outcome[MutationExecution | None]:
        # Load one immutable mutant execution projection.
        ...
    def get_executions(
        self,
        execution_ids: tuple[ExecutionId, ...],
    ) -> Outcome[tuple[MutationExecution, ...]]:
        # Load a bounded execution batch without issuing one query per mutant.
        ...
    def list_executions(
        self,
        campaign_id: CampaignId,
    ) -> Outcome[tuple[MutationExecution, ...]]:
        # Load every immutable execution needed for fan-in validation and progress reconciliation.
        ...
    def save_execution(
        self,
        execution: MutationExecution,
        *,
        expected_revision: int | None,
    ) -> Outcome[MutationExecution]:
        # Append or update an execution row without accepting conflicting duplicates.
        ...
    def get_effect(self, effect_id: str) -> Outcome[EffectReceipt | None]:
        # Load an idempotency receipt before running a retried effect.
        ...
    def save_effect(self, receipt: EffectReceipt) -> Outcome[EffectReceipt]:
        # Append one effect receipt or return the exact existing receipt.
        ...
    def commit_effect(
        self,
        *,
        receipt: EffectReceipt,
        campaign: MutationCampaign | None = None,
        campaign_expected_revision: int | None = None,
        shard: MutationShard | None = None,
        shard_expected_revision: int | None = None,
        lease: MutationLease | None = None,
        lease_expected_revision: int | None = None,
        worker: MutationWorker | None = None,
        worker_expected_revision: int | None = None,
        executions: tuple[MutationExecution, ...] = (),
    ) -> Outcome[EffectReceipt]:
        # Commit all aggregate projections, the effect receipt and the outbox row atomically.
        ...
    def commit_plan_topology(
        self,
        *,
        receipt: EffectReceipt,
        campaign: MutationCampaign | None,
        campaign_expected_revision: int | None,
        shards: tuple[MutationShard, ...],
    ) -> Outcome[EffectReceipt]:
        # Commit one campaign plan binding and its complete immutable shard topology atomically.
        ...
    def get_finalization_intent(self, campaign_id: CampaignId) -> Outcome[FinalizationIntent | None]:
        # Load the durable finalization intent for recovery or completion validation.
        ...
    def list_artifacts(self, campaign_id: CampaignId) -> Outcome[tuple[ArtifactRegistryEntry, ...]]:
        # Load the authoritative immutable artifact registry for one campaign.
        ...
    def commit_finalization(
        self,
        *,
        receipt: EffectReceipt,
        intent: FinalizationIntent,
        artifacts: tuple[ArtifactRegistryEntry, ...] = (),
        campaign: MutationCampaign | None = None,
        campaign_expected_revision: int | None = None,
    ) -> Outcome[FinalizationIntent]:
        # Commit finalization intent, immutable registry rows, campaign transition, effect and outbox atomically.
        ...
    def get_operator_action(self, action_id: str) -> Outcome[OperatorAction | None]:
        # Load one durable operator action by its stable request identity.
        ...
    def save_operator_action(
        self,
        action: OperatorAction,
        *,
        expected_status: OperatorActionStatus | None,
    ) -> Outcome[OperatorAction]:
        # Create or transition one durable operator action with status compare-and-swap.
        ...
    def commit_campaign_retry(
        self,
        action: OperatorAction,
        shards: tuple[MutationShard, ...],
        *,
        expected_revisions: Mapping[str, int],
    ) -> Outcome[OperatorAction]:
        # Commit all retried shards and the terminal operator action atomically.
        ...
    def acknowledge_outbox(self, effect_ids: tuple[str, ...]) -> Outcome[int]:
        # Mark projected effect notifications delivered without deleting their audit records.
        ...
def _plan_topology_error(
    campaign: MutationCampaign,
    shards: tuple[MutationShard, ...],
) -> Rejected | None:
    # Validate one complete planner-owned topology before any repository mutation begins.
    if campaign.status.value not in {"planning", "running"}:
        return Rejected(
            code="plan_topology_out_of_order",
            message="Plan topology requires a planning or running campaign projection",
            details={"status": campaign.status.value},
        )
    if not campaign.plan_id or not campaign.prepared_snapshot_id:
        return Rejected(
            code="plan_identity_missing",
            message="Plan topology requires campaign plan and prepared snapshot identities",
        )
    shard_ids = tuple(item.shard_id.value for item in shards)
    ordinals = tuple(item.ordinal for item in shards)
    mutant_ids = tuple(mutant.value for item in shards for mutant in item.mutant_ids)
    if len(shard_ids) != len(set(shard_ids)) or len(mutant_ids) != len(set(mutant_ids)):
        return Rejected(
            code="plan_topology_conflict",
            message="Plan topology contains duplicate shard or mutant identities",
        )
    if ordinals != tuple(range(len(shards))):
        return Rejected(
            code="plan_topology_conflict",
            message="Plan topology ordinals must be contiguous and deterministic",
        )
    if len(mutant_ids) != campaign.total_mutants:
        return Rejected(
            code="plan_topology_conflict",
            message="Plan topology membership does not match campaign mutant count",
            details={
                "campaign_total_mutants": campaign.total_mutants,
                "topology_mutants": len(mutant_ids),
            },
        )
    for shard in shards:
        if (
            shard.campaign_id != campaign.campaign_id
            or shard.plan_id != campaign.plan_id
            or shard.status.value != "created"
            or shard.revision_number != 0
        ):
            return Rejected(
                code="plan_topology_conflict",
                message="Shard topology is not an initial projection of the committed campaign plan",
                details={"shard_id": shard.shard_id.value},
            )
    return None
def _completed_shard_lease_projection_only(existing: Any, candidate: Any) -> bool:
    # Permit only terminal lease projection after completed shard fan-in and keep all semantic fields immutable.
    if not isinstance(existing, MutationShard) or not isinstance(candidate, MutationShard):
        return False
    existing_status = getattr(existing.status, "value", existing.status)
    candidate_status = getattr(candidate.status, "value", candidate.status)
    if existing_status != "complete" or candidate_status != "complete":
        return False
    if existing.lease is None or candidate.lease is None:
        return False
    existing_lease_status = getattr(existing.lease.status, "value", existing.lease.status)
    candidate_lease_status = getattr(candidate.lease.status, "value", candidate.lease.status)
    if existing_lease_status not in {"leased", "running"} or candidate_lease_status != "complete":
        return False
    if (
        existing.lease.lease_id != candidate.lease.lease_id
        or existing.lease.worker_id != candidate.lease.worker_id
        or existing.lease.attempt != candidate.lease.attempt
    ):
        return False
    projected = replace(
        existing,
        lease=candidate.lease,
        revision_number=candidate.revision_number,
    )
    return projected == candidate
_FINALIZATION_STATUS_ORDER = {"created": 0, "registered": 1, "completed": 2}
def _artifact_registry_key(campaign_id: CampaignId, logical_key: str) -> str:
    # Scope one logical artifact role by campaign so equal names never collide across runs.
    return f"{campaign_id.value}\x1f{logical_key}"
def _finalization_compatible(
    existing: FinalizationIntent | None,
    candidate: FinalizationIntent,
) -> Rejected | None:
    # Allow only monotonic status advancement over an otherwise immutable expected artifact set.
    if existing is None:
        return None
    immutable_existing = replace(existing, status=candidate.status, updated_at=candidate.updated_at)
    if immutable_existing != candidate:
        return Rejected(
            code="finalization_intent_conflict",
            message="Finalization intent identity or expected artifact set changed",
            details={
                "campaign_id": candidate.campaign_id.value,
                "intent_id": candidate.intent_id,
            },
        )
    if _FINALIZATION_STATUS_ORDER[candidate.status] < _FINALIZATION_STATUS_ORDER[existing.status]:
        return Rejected(
            code="finalization_status_regression",
            message="Finalization intent status cannot move backwards",
            details={"existing_status": existing.status, "candidate_status": candidate.status},
        )
    return None
def _finalization_input_error(
    receipt: EffectReceipt,
    intent: FinalizationIntent,
    artifacts: tuple[ArtifactRegistryEntry, ...],
    campaign: MutationCampaign | None,
) -> Rejected | None:
    # Reject cross-campaign, duplicate, or out-of-phase finalization bundles before persistence.
    if receipt.campaign_id != intent.campaign_id:
        return Rejected(
            code="finalization_campaign_mismatch",
            message="Finalization receipt and intent belong to different campaigns",
            details={
                "receipt_campaign_id": receipt.campaign_id.value,
                "intent_campaign_id": intent.campaign_id.value,
            },
        )
    if campaign is not None and campaign.campaign_id != intent.campaign_id:
        return Rejected(
            code="finalization_campaign_mismatch",
            message="Finalization campaign projection and intent belong to different campaigns",
            details={
                "campaign_id": campaign.campaign_id.value,
                "intent_campaign_id": intent.campaign_id.value,
            },
        )
    foreign = tuple(
        sorted(
            item.logical_key
            for item in artifacts
            if item.campaign_id != intent.campaign_id
        )
    )
    key_counts = Counter(item.logical_key for item in artifacts)
    duplicate = tuple(sorted(key for key, count in key_counts.items() if count > 1))
    if foreign or duplicate:
        return Rejected(
            code="invalid_artifact_registry_bundle",
            message="Finalization artifacts must be unique and owned by the intent campaign",
            details={
                "campaign_id": intent.campaign_id.value,
                "foreign_logical_keys": ",".join(foreign),
                "duplicate_logical_keys": ",".join(duplicate),
            },
        )
    if intent.status == "created" and artifacts:
        return Rejected(
            code="artifact_registration_out_of_order",
            message="Created finalization intent cannot persist registry rows",
            details={"campaign_id": intent.campaign_id.value},
        )
    return None
def _exact_registry_error(
    intent: FinalizationIntent,
    registry: Mapping[str, ArtifactRegistryEntry],
) -> Rejected | None:
    # Require the complete expected artifact map once finalization reaches registered or completed.
    if intent.status not in {"registered", "completed"}:
        return None
    expected = {item.logical_key: item for item in intent.artifacts}
    missing = tuple(sorted(set(expected) - set(registry)))
    unexpected = tuple(sorted(set(registry) - set(expected)))
    conflicting = tuple(
        sorted(
            key
            for key in set(expected) & set(registry)
            if expected[key] != registry[key]
        )
    )
    if not (missing or unexpected or conflicting):
        return None
    return Rejected(
        code="artifact_registry_incomplete",
        message="Finalization registry must exactly match the durable intent",
        details={
            "missing_logical_keys": ",".join(missing),
            "unexpected_logical_keys": ",".join(unexpected),
            "conflicting_logical_keys": ",".join(conflicting),
        },
    )
def _version_check(
    existing: Any,
    candidate: Any,
    *,
    expected_revision: int | None,
    entity: str,
) -> tuple[bool, bool, Rejected | None]:
    # Validate create/update version rules once for all aggregate repositories.
    if existing is None:
        if expected_revision is not None:
            return False, False, Rejected(
                code="aggregate_missing",
                message=f"Cannot update missing {entity}",
            )
        if candidate.revision_number != 0:
            return False, False, Rejected(
                code="invalid_initial_revision",
                message=f"New {entity} must start at revision zero",
            )
        return True, False, None
    if expected_revision is None:
        if existing == candidate:
            return False, True, None
        return False, False, Rejected(
            code="immutable_id_conflict",
            message=f"{entity} identity is already bound to another value",
        )
    if expected_revision != existing.revision_number:
        return False, False, Rejected(
            code="stale_revision",
            message=f"{entity} changed since it was loaded",
            details={
                "entity": entity,
                "expected_revision": expected_revision,
                "actual_revision": existing.revision_number,
            },
        )
    if candidate.revision_number != expected_revision + 1:
        return False, False, Rejected(
            code="invalid_revision_step",
            message=f"{entity} revision must advance by one",
        )
    terminal = getattr(existing.status, "value", existing.status)
    if (
        terminal in {"completed", "complete"}
        and existing != candidate
        and not _completed_shard_lease_projection_only(existing, candidate)
    ):
        return False, False, Rejected(
            code="aggregate_immutable",
            message=f"Completed {entity} cannot be rewritten",
        )
    return True, False, None
class InMemoryMutationStore:
    """Thread-safe repository useful for local control-plane tests and dry runs."""
    def __init__(self) -> None:
        # Keep all projections in private maps until a durable adapter is selected.
        self._campaigns: dict[str, MutationCampaign] = {}
        self._shards: dict[str, MutationShard] = {}
        self._leases: dict[str, MutationLease] = {}
        self._executions: dict[str, MutationExecution] = {}
        self._workers: dict[str, MutationWorker] = {}
        self._effects: dict[str, EffectReceipt] = {}
        self._artifacts: dict[str, ArtifactRegistryEntry] = {}
        self._finalizations: dict[str, FinalizationIntent] = {}
        self._operator_actions: dict[str, OperatorAction] = {}
        self._lock = RLock()
    def get_campaign(self, campaign_id: CampaignId) -> Outcome[MutationCampaign | None]:
        # Return a stable campaign snapshot under the store lock.
        with self._lock:
            return Success(self._campaigns.get(campaign_id.value))
    def save_campaign(
        self,
        campaign: MutationCampaign,
        *,
        expected_revision: int | None,
    ) -> Outcome[MutationCampaign]:
        # Apply a version-checked campaign write atomically in memory.
        with self._lock:
            existing = self._campaigns.get(campaign.campaign_id.value)
            allowed, duplicate, error = _version_check(
                existing, campaign, expected_revision=expected_revision, entity="campaign"
            )
            if error is not None:
                return error
            if duplicate:
                return Success(existing, message="Campaign write already applied", duplicate=True)
            if allowed:
                self._campaigns[campaign.campaign_id.value] = campaign
            return Success(campaign)
    def commit_campaign_creation(
        self,
        action: OperatorAction,
        campaign: MutationCampaign,
    ) -> Outcome[OperatorAction]:
        # Commit campaign identity and the replay-safe create receipt under one critical section.
        with self._lock:
            existing_action = self._operator_actions.get(action.action_id)
            if existing_action is not None:
                if (
                    existing_action.action_type == action.action_type
                    and existing_action.campaign_id == action.campaign_id
                    and existing_action.request_fingerprint == action.request_fingerprint
                ):
                    return Success(existing_action, duplicate=True)
                return Rejected(code="action_id_conflict", message="Action ID is bound to another request")
            existing_campaign = self._campaigns.get(campaign.campaign_id.value)
            if existing_campaign is not None:
                code = "campaign_already_exists" if existing_campaign == campaign else "campaign_id_conflict"
                rejected = action.reject(
                    code,
                    {"campaign_revision": existing_campaign.revision_number},
                )
                self._operator_actions[action.action_id] = rejected
                return Success(rejected)
            completed = action.complete(
                {
                    "campaign_revision": campaign.revision_number,
                    "campaign_status": campaign.status.value,
                }
            )
            self._campaigns[campaign.campaign_id.value] = campaign
            self._operator_actions[action.action_id] = completed
            return Success(completed)
    def get_shard(self, shard_id: ShardId) -> Outcome[MutationShard | None]:
        # Return a shard snapshot without exposing the mutable repository map.
        with self._lock:
            return Success(self._shards.get(shard_id.value))
    def save_shard(self, shard: MutationShard, *, expected_revision: int | None) -> Outcome[MutationShard]:
        # Apply a version-checked shard write atomically in memory.
        with self._lock:
            existing = self._shards.get(shard.shard_id.value)
            allowed, duplicate, error = _version_check(
                existing, shard, expected_revision=expected_revision, entity="shard"
            )
            if error is not None:
                return error
            if duplicate:
                return Success(existing, message="Shard write already applied", duplicate=True)
            if allowed:
                self._shards[shard.shard_id.value] = shard
            return Success(shard)
    def get_lease(self, lease_id: str) -> Outcome[MutationLease | None]:
        # Return one authoritative lease generation under the in-memory store lock.
        with self._lock:
            return Success(self._leases.get(str(lease_id)))
    def save_lease(
        self,
        lease: MutationLease,
        *,
        expected_revision: int | None,
    ) -> Outcome[MutationLease]:
        # Apply a version-checked lease write atomically in memory.
        with self._lock:
            existing = self._leases.get(lease.registry_key)
            allowed, duplicate, error = _version_check(
                existing, lease, expected_revision=expected_revision, entity="lease"
            )
            if error is not None:
                return error
            if duplicate:
                return Success(existing, message="Lease write already applied", duplicate=True)
            if allowed:
                self._leases[lease.registry_key] = lease
            return Success(lease)
    @staticmethod
    def _worker_key(campaign_id: CampaignId, worker_id: str) -> str:
        # Scope reusable worker slot names by campaign in the repository keyspace.
        return f"{campaign_id.value}\x1f{worker_id}"
    def get_worker(self, campaign_id: CampaignId, worker_id: str) -> Outcome[MutationWorker | None]:
        # Return one authoritative worker snapshot under the in-memory store lock.
        with self._lock:
            return Success(self._workers.get(self._worker_key(campaign_id, worker_id)))
    def save_worker(
        self,
        worker: MutationWorker,
        *,
        expected_revision: int | None,
    ) -> Outcome[MutationWorker]:
        # Apply a version-checked worker write atomically in memory.
        with self._lock:
            key = worker.registry_key
            existing = self._workers.get(key)
            allowed, duplicate, error = _version_check(
                existing, worker, expected_revision=expected_revision, entity="worker"
            )
            if error is not None:
                return error
            if duplicate:
                return Success(existing, message="Worker write already applied", duplicate=True)
            if allowed:
                self._workers[key] = worker
            return Success(worker)
    def list_workers(self, campaign_id: CampaignId) -> Outcome[tuple[MutationWorker, ...]]:
        # Return all worker registrations for one campaign in stable worker-id order.
        with self._lock:
            values = tuple(
                item for item in self._workers.values() if item.campaign_id == campaign_id
            )
            return Success(tuple(sorted(values, key=lambda item: item.worker_id)))
    def get_execution(self, execution_id: ExecutionId) -> Outcome[MutationExecution | None]:
        # Return one execution snapshot keyed by immutable execution identity.
        with self._lock:
            return Success(self._executions.get(execution_id.value))
    def get_executions(
        self,
        execution_ids: tuple[ExecutionId, ...],
    ) -> Outcome[tuple[MutationExecution, ...]]:
        # Return a bounded execution batch without changing the in-memory authority adapter.
        with self._lock:
            values = tuple(
                self._executions[item.value]
                for item in execution_ids
                if item.value in self._executions
            )
            return Success(values)
    def list_executions(
        self,
        campaign_id: CampaignId,
    ) -> Outcome[tuple[MutationExecution, ...]]:
        # Return all execution attempts for one campaign in deterministic identity order.
        with self._lock:
            return Success(
                tuple(
                    sorted(
                        (item for item in self._executions.values() if item.campaign_id == campaign_id),
                        key=lambda item: (item.mutant_id.value, item.attempt, item.execution_id.value),
                    )
                )
            )
    def save_execution(
        self,
        execution: MutationExecution,
        *,
        expected_revision: int | None,
    ) -> Outcome[MutationExecution]:
        # Append or version-check one execution without overwriting terminal evidence.
        with self._lock:
            existing = self._executions.get(execution.execution_id.value)
            allowed, duplicate, error = _version_check(
                existing, execution, expected_revision=expected_revision, entity="execution"
            )
            if error is not None:
                return error
            if duplicate:
                return Success(existing, message="Execution write already applied", duplicate=True)
            if allowed:
                self._executions[execution.execution_id.value] = execution
            return Success(execution)
    def get_effect(self, effect_id: str) -> Outcome[EffectReceipt | None]:
        # Read the exact effect receipt used for retry deduplication.
        with self._lock:
            return Success(self._effects.get(effect_id))
    def save_effect(self, receipt: EffectReceipt) -> Outcome[EffectReceipt]:
        # Append one effect receipt and reject an identity collision with different data.
        with self._lock:
            existing = self._effects.get(receipt.effect_id)
            if existing is None:
                self._effects[receipt.effect_id] = receipt
                return Success(receipt)
            if existing == receipt:
                return Success(existing, message="Effect already recorded", duplicate=True)
            return Rejected(
                code="effect_id_conflict",
                message="Effect ID is already bound to another receipt",
            )
    def commit_effect(
        self,
        *,
        receipt: EffectReceipt,
        campaign: MutationCampaign | None = None,
        campaign_expected_revision: int | None = None,
        shard: MutationShard | None = None,
        shard_expected_revision: int | None = None,
        lease: MutationLease | None = None,
        lease_expected_revision: int | None = None,
        worker: MutationWorker | None = None,
        worker_expected_revision: int | None = None,
        executions: tuple[MutationExecution, ...] = (),
    ) -> Outcome[EffectReceipt]:
        # Apply one complete effect bundle under the in-memory store lock.
        with self._lock:
            existing_effect = self._effects.get(receipt.effect_id)
            if existing_effect is not None:
                if existing_effect == receipt:
                    return Success(existing_effect, message="Effect already recorded", duplicate=True)
                return Rejected(code="effect_id_conflict", message="Effect ID is already bound to another receipt")
            checks: list[tuple[dict[str, Any], str, Any, Any, int | None]] = []
            if campaign is not None:
                checks.append((self._campaigns, campaign.campaign_id.value, campaign, campaign.campaign_id.value, campaign_expected_revision))
            if shard is not None:
                checks.append((self._shards, shard.shard_id.value, shard, shard.shard_id.value, shard_expected_revision))
            if lease is not None:
                checks.append((self._leases, lease.registry_key, lease, lease.registry_key, lease_expected_revision))
            if worker is not None:
                checks.append((self._workers, worker.registry_key, worker, worker.registry_key, worker_expected_revision))
            for execution in executions:
                checks.append((self._executions, execution.execution_id.value, execution, execution.execution_id.value, None))
            pending: list[tuple[dict[str, Any], str, Any]] = []
            for mapping, key, candidate, _identity, expected_revision in checks:
                existing = mapping.get(key)
                entity = (
                    "execution" if mapping is self._executions
                    else "worker" if mapping is self._workers
                    else "lease" if mapping is self._leases
                    else "shard" if mapping is self._shards
                    else "campaign"
                )
                allowed, duplicate, error = _version_check(
                    existing,
                    candidate,
                    expected_revision=expected_revision,
                    entity=entity,
                )
                if error is not None:
                    return error
                if allowed and not duplicate:
                    pending.append((mapping, key, candidate))
            for mapping, key, candidate in pending:
                mapping[key] = candidate
            self._effects[receipt.effect_id] = receipt
            return Success(receipt)
    def commit_plan_topology(
        self,
        *,
        receipt: EffectReceipt,
        campaign: MutationCampaign | None,
        campaign_expected_revision: int | None,
        shards: tuple[MutationShard, ...],
    ) -> Outcome[EffectReceipt]:
        # Apply the complete plan topology under one in-memory critical section.
        with self._lock:
            existing_effect = self._effects.get(receipt.effect_id)
            if existing_effect is not None:
                if existing_effect == receipt:
                    return Success(existing_effect, message="Plan topology already committed", duplicate=True)
                return Rejected(
                    code="effect_id_conflict",
                    message="Effect ID is already bound to another receipt",
                )
            raw_campaign = receipt.payload.get("campaign")
            if not isinstance(raw_campaign, Mapping):
                return Failed("effect_receipt_corrupted", "Plan topology receipt has no campaign projection")
            raw_shards = receipt.payload.get("shards")
            try:
                authority_campaign = MutationCampaign.from_dict(raw_campaign)
                receipt_shards = tuple(
                    MutationShard.from_dict(item)
                    for item in raw_shards
                ) if isinstance(raw_shards, (list, tuple)) else ()
            except (TypeError, ValueError) as exc:
                return Failed("effect_receipt_corrupted", f"Cannot decode plan topology receipt: {exc}")
            if receipt_shards != shards:
                return Rejected(
                    code="plan_topology_receipt_conflict",
                    message="Plan topology receipt does not match the committed shard set",
                )
            if receipt.campaign_id != authority_campaign.campaign_id:
                return Rejected(
                    code="plan_topology_campaign_mismatch",
                    message="Plan topology receipt belongs to another campaign",
                )
            input_error = _plan_topology_error(authority_campaign, shards)
            if input_error is not None:
                return input_error
            existing_campaign = self._campaigns.get(authority_campaign.campaign_id.value)
            if campaign is None:
                if existing_campaign != authority_campaign:
                    return Rejected(
                        code="campaign_plan_conflict",
                        message="Stored campaign projection conflicts with plan topology receipt",
                    )
            else:
                if campaign != authority_campaign:
                    return Rejected(
                        code="campaign_plan_conflict",
                        message="Candidate campaign projection conflicts with plan topology receipt",
                    )
                allowed, duplicate, error = _version_check(
                    existing_campaign,
                    campaign,
                    expected_revision=campaign_expected_revision,
                    entity="campaign",
                )
                if error is not None:
                    return error
                if duplicate or not allowed:
                    return Rejected(
                        code="campaign_plan_conflict",
                        message="Plan topology requires one new campaign revision",
                    )
            existing_campaign_shards = tuple(
                item for item in self._shards.values() if item.campaign_id == authority_campaign.campaign_id
            )
            if existing_campaign_shards:
                return Rejected(
                    code="partial_plan_topology",
                    message="Plan topology already contains shard rows without its atomic effect receipt",
                    details={
                        "existing_shards": ",".join(
                            sorted(item.shard_id.value for item in existing_campaign_shards)
                        )
                    },
                )
            conflicting_ids = tuple(
                sorted(item.shard_id.value for item in shards if item.shard_id.value in self._shards)
            )
            if conflicting_ids:
                return Rejected(
                    code="plan_topology_conflict",
                    message="Plan shard identity is already owned by another campaign",
                    details={"shard_ids": ",".join(conflicting_ids)},
                )
            if campaign is not None:
                self._campaigns[campaign.campaign_id.value] = campaign
            for shard in shards:
                self._shards[shard.shard_id.value] = shard
            self._effects[receipt.effect_id] = receipt
            return Success(receipt)
    def get_finalization_intent(self, campaign_id: CampaignId) -> Outcome[FinalizationIntent | None]:
        # Return the current finalization intent under the same repository lock as campaign state.
        with self._lock:
            return Success(self._finalizations.get(campaign_id.value))
    def list_artifacts(self, campaign_id: CampaignId) -> Outcome[tuple[ArtifactRegistryEntry, ...]]:
        # Return immutable registry rows in deterministic logical-key order.
        with self._lock:
            rows = tuple(
                item for item in self._artifacts.values() if item.campaign_id == campaign_id
            )
            return Success(tuple(sorted(rows, key=lambda item: item.logical_key)))
    def commit_finalization(
        self,
        *,
        receipt: EffectReceipt,
        intent: FinalizationIntent,
        artifacts: tuple[ArtifactRegistryEntry, ...] = (),
        campaign: MutationCampaign | None = None,
        campaign_expected_revision: int | None = None,
    ) -> Outcome[FinalizationIntent]:
        # Apply the complete finalization bundle under one in-memory critical section.
        with self._lock:
            existing_effect = self._effects.get(receipt.effect_id)
            if existing_effect is not None:
                if existing_effect != receipt:
                    return Rejected(code="effect_id_conflict", message="Effect ID is already bound to another receipt")
                raw_intent = existing_effect.payload.get("finalization_intent")
                if not isinstance(raw_intent, Mapping):
                    return Failed("effect_receipt_corrupted", "Finalization effect has no intent payload")
                return Success(FinalizationIntent.from_dict(raw_intent), duplicate=True)
            input_error = _finalization_input_error(receipt, intent, artifacts, campaign)
            if input_error is not None:
                return input_error
            existing_intent = self._finalizations.get(intent.campaign_id.value)
            conflict = _finalization_compatible(existing_intent, intent)
            if conflict is not None:
                return conflict
            pending_artifacts: list[tuple[str, ArtifactRegistryEntry]] = []
            for artifact in artifacts:
                key = _artifact_registry_key(artifact.campaign_id, artifact.logical_key)
                existing_artifact = self._artifacts.get(key)
                if existing_artifact is not None and existing_artifact != artifact:
                    return Rejected(
                        code="artifact_logical_key_conflict",
                        message="Artifact logical key is already bound to different content",
                        details={
                            "logical_key": artifact.logical_key,
                            "existing_sha256": existing_artifact.content_sha256,
                            "candidate_sha256": artifact.content_sha256,
                        },
                    )
                if existing_artifact is None:
                    pending_artifacts.append((key, artifact))
            registry = {
                item.logical_key: item
                for item in self._artifacts.values()
                if item.campaign_id == intent.campaign_id
            }
            registry.update({item.logical_key: item for _, item in pending_artifacts})
            registry_error = _exact_registry_error(intent, registry)
            if registry_error is not None:
                return registry_error
            if campaign is not None:
                existing_campaign = self._campaigns.get(campaign.campaign_id.value)
                allowed, duplicate, error = _version_check(
                    existing_campaign,
                    campaign,
                    expected_revision=campaign_expected_revision,
                    entity="campaign",
                )
                if error is not None:
                    return error
                if allowed and not duplicate:
                    self._campaigns[campaign.campaign_id.value] = campaign
            for key, artifact in pending_artifacts:
                self._artifacts[key] = artifact
            self._finalizations[intent.campaign_id.value] = intent
            self._effects[receipt.effect_id] = receipt
            return Success(intent)
    def get_operator_action(self, action_id: str) -> Outcome[OperatorAction | None]:
        # Return one durable operator command under the in-memory store lock.
        with self._lock:
            return Success(self._operator_actions.get(str(action_id)))
    def save_operator_action(
        self,
        action: OperatorAction,
        *,
        expected_status: OperatorActionStatus | None,
    ) -> Outcome[OperatorAction]:
        # Create or transition one operator action with exact request replay semantics.
        with self._lock:
            existing = self._operator_actions.get(action.action_id)
            if existing is None:
                if expected_status is not None:
                    return Rejected(code="operator_action_missing", message="Operator action does not exist")
                self._operator_actions[action.action_id] = action
                return Success(action)
            if (
                existing.action_type != action.action_type
                or existing.campaign_id != action.campaign_id
                or existing.request_fingerprint != action.request_fingerprint
            ):
                return Rejected(code="action_id_conflict", message="Action ID is bound to another request")
            if existing == action:
                return Success(existing, duplicate=True)
            if expected_status is None or existing.status != expected_status:
                return Rejected(
                    code="stale_operator_action",
                    message="Operator action status changed",
                    details={"expected_status": expected_status.value if expected_status else None, "actual_status": existing.status.value},
                )
            self._operator_actions[action.action_id] = action
            return Success(action)
    def commit_campaign_retry(
        self,
        action: OperatorAction,
        shards: tuple[MutationShard, ...],
        *,
        expected_revisions: Mapping[str, int],
    ) -> Outcome[OperatorAction]:
        # Commit every retried shard and the completed action under one critical section.
        with self._lock:
            existing_action = self._operator_actions.get(action.action_id)
            if existing_action is None:
                return Rejected(code="operator_action_missing", message="Operator action does not exist")
            if existing_action.status == OperatorActionStatus.COMPLETED and existing_action == action:
                return Success(existing_action, duplicate=True)
            if existing_action.status != OperatorActionStatus.RUNNING:
                return Rejected(code="stale_operator_action", message="Operator action is not running")
            if (
                existing_action.action_type != action.action_type
                or existing_action.campaign_id != action.campaign_id
                or existing_action.request_fingerprint != action.request_fingerprint
            ):
                return Rejected(code="action_id_conflict", message="Action ID is bound to another request")
            pending: list[MutationShard] = []
            for shard in shards:
                existing = self._shards.get(shard.shard_id.value)
                expected = expected_revisions.get(shard.shard_id.value)
                allowed, duplicate, error = _version_check(
                    existing,
                    shard,
                    expected_revision=expected,
                    entity="shard",
                )
                if error is not None:
                    return error
                if allowed and not duplicate:
                    pending.append(shard)
            for shard in pending:
                self._shards[shard.shard_id.value] = shard
            self._operator_actions[action.action_id] = action
            return Success(action)
    def list_campaigns(self) -> Outcome[tuple[MutationCampaign, ...]]:
        # Return all durable campaign projections for startup reconciliation.
        with self._lock:
            return Success(tuple(self._campaigns.values()))
    def list_shards(self, campaign_id: CampaignId) -> Outcome[tuple[MutationShard, ...]]:
        # Return the shard projections owned by one campaign for lease reconciliation.
        with self._lock:
            return Success(tuple(item for item in self._shards.values() if item.campaign_id == campaign_id))
    def acknowledge_outbox(self, effect_ids: tuple[str, ...]) -> Outcome[int]:
        # Keep the in-memory adapter compatible with the durable outbox acknowledgement port.
        return Success(len(effect_ids))
class SQLiteMutationStore:
    """Small SQLite implementation of the mutation repository and effect journal ports."""
    def __init__(
        self,
        path: str | Path,
        *,
        metrics: SQLiteMetrics | None = None,
    ) -> None:
        # Open one connection for the control-plane store and initialize only its own tables.
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        if metrics is not None:
            self._connection.set_trace_callback(metrics.trace)
        self._lock = RLock()
        self._initialize()
    def _initialize(self) -> None:
        # Create append-only aggregate projections and the idempotency table atomically.
        with self._connection:
            self._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS mutation_campaigns (
                    campaign_id TEXT PRIMARY KEY,
                    revision_number INTEGER NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mutation_shards (
                    shard_id TEXT PRIMARY KEY,
                    revision_number INTEGER NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mutation_workers (
                    worker_key TEXT PRIMARY KEY,
                    revision_number INTEGER NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mutation_leases (
                    lease_id TEXT PRIMARY KEY,
                    revision_number INTEGER NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mutation_executions (
                    execution_id TEXT PRIMARY KEY,
                    revision_number INTEGER NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mutation_artifacts (
                    artifact_key TEXT PRIMARY KEY,
                    campaign_id TEXT NOT NULL,
                    logical_key TEXT NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    UNIQUE(campaign_id, logical_key)
                );
                CREATE TABLE IF NOT EXISTS mutation_finalizations (
                    campaign_id TEXT PRIMARY KEY,
                    intent_id TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mutation_effects (
                    effect_id TEXT PRIMARY KEY,
                    effect_type TEXT NOT NULL,
                    campaign_id TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mutation_outbox (
                    effect_id TEXT PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    campaign_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    delivered_at TEXT
                );
                CREATE TABLE IF NOT EXISTS mutation_operator_actions (
                    action_id TEXT PRIMARY KEY,
                    action_type TEXT NOT NULL,
                    campaign_id TEXT NOT NULL,
                    request_fingerprint TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_mutation_operator_actions_campaign
                ON mutation_operator_actions(campaign_id, action_id);
                CREATE INDEX IF NOT EXISTS ix_mutation_shards_campaign
                ON mutation_shards(
                    json_extract(payload, '$.campaign_id'),
                    shard_id
                );
                CREATE INDEX IF NOT EXISTS ix_mutation_executions_campaign
                ON mutation_executions(
                    json_extract(payload, '$.campaign_id'),
                    json_extract(payload, '$.mutant_id'),
                    json_extract(payload, '$.attempt'),
                    execution_id
                );
                CREATE INDEX IF NOT EXISTS ix_mutation_outbox_delivery
                ON mutation_outbox(delivered_at, created_at, effect_id);
                """
            )
            self._connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_mutation_executions_execution_id "
                "ON mutation_executions(execution_id)"
            )
    def close(self) -> None:
        # Close the explicit SQLite connection at application shutdown.
        with self._lock:
            self._connection.close()
    def _load(self, table: str, key_column: str, key: str, parser: Any) -> Outcome[Any | None]:
        # Read and decode one JSON projection while converting corruption into a typed failure.
        try:
            row = self._connection.execute(
                f"SELECT payload FROM {table} WHERE {key_column} = ?", (key,)
            ).fetchone()
            if row is None:
                return Success(None)
            return Success(parser(loads_object(row["payload"])))
        except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
            return Failed("store_corrupted", f"Cannot load {table}: {exc}")
    def _save_aggregate(
        self,
        *,
        table: str,
        key_column: str,
        key: str,
        candidate: Any,
        existing: Any,
        expected_revision: int | None,
        entity: str,
    ) -> Outcome[Any]:
        # Write one aggregate using SQL compare-and-swap semantics.
        allowed, duplicate, error = _version_check(
            existing, candidate, expected_revision=expected_revision, entity=entity
        )
        if error is not None:
            return error
        if duplicate:
            return Success(existing, message=f"{entity} write already applied", duplicate=True)
        if not allowed:
            return Failed("store_write_rejected", f"Cannot write {entity}")
        try:
            payload = dumps(candidate.to_dict())
            with self._connection:
                if existing is None:
                    self._connection.execute(
                        f"INSERT INTO {table} ({key_column}, revision_number, payload) VALUES (?, ?, ?)",
                        (key, candidate.revision_number, payload),
                    )
                else:
                    cursor = self._connection.execute(
                        f"UPDATE {table} SET revision_number = ?, payload = ? WHERE {key_column} = ? AND revision_number = ?",
                        (candidate.revision_number, payload, key, expected_revision),
                    )
                    if cursor.rowcount != 1:
                        return Rejected(
                            code="stale_revision",
                            message=f"{entity} changed during persistence",
                        )
            return Success(candidate)
        except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
            return Failed("store_write_failed", f"Cannot save {entity}: {exc}", retriable=True)
    def get_campaign(self, campaign_id: CampaignId) -> Outcome[MutationCampaign | None]:
        # Load a campaign projection after a process restart.
        with self._lock:
            return self._load("mutation_campaigns", "campaign_id", campaign_id.value, MutationCampaign.from_dict)
    def save_campaign(
        self,
        campaign: MutationCampaign,
        *,
        expected_revision: int | None,
    ) -> Outcome[MutationCampaign]:
        # Persist a campaign with SQLite compare-and-swap semantics.
        with self._lock:
            current = self.get_campaign(campaign.campaign_id)
            if not is_success(current):
                return current
            return self._save_aggregate(
                table="mutation_campaigns",
                key_column="campaign_id",
                key=campaign.campaign_id.value,
                candidate=campaign,
                existing=current.value,
                expected_revision=expected_revision,
                entity="campaign",
            )
    def commit_campaign_creation(
        self,
        action: OperatorAction,
        campaign: MutationCampaign,
    ) -> Outcome[OperatorAction]:
        # Atomically insert a new campaign and its exact create-action result.
        with self._lock:
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                action_row = self._connection.execute(
                    "SELECT payload FROM mutation_operator_actions WHERE action_id = ?",
                    (action.action_id,),
                ).fetchone()
                if action_row is not None:
                    existing_action = OperatorAction.from_dict(loads_object(action_row["payload"]))
                    if (
                        existing_action.action_type == action.action_type
                        and existing_action.campaign_id == action.campaign_id
                        and existing_action.request_fingerprint == action.request_fingerprint
                    ):
                        self._connection.commit()
                        return Success(existing_action, duplicate=True)
                    self._connection.rollback()
                    return Rejected(code="action_id_conflict", message="Action ID is bound to another request")
                campaign_row = self._connection.execute(
                    "SELECT payload FROM mutation_campaigns WHERE campaign_id = ?",
                    (campaign.campaign_id.value,),
                ).fetchone()
                if campaign_row is not None:
                    existing_campaign = MutationCampaign.from_dict(loads_object(campaign_row["payload"]))
                    code = "campaign_already_exists" if existing_campaign == campaign else "campaign_id_conflict"
                    result = action.reject(
                        code,
                        {"campaign_revision": existing_campaign.revision_number},
                    )
                else:
                    result = action.complete(
                        {
                            "campaign_revision": campaign.revision_number,
                            "campaign_status": campaign.status.value,
                        }
                    )
                    self._connection.execute(
                        "INSERT INTO mutation_campaigns (campaign_id, revision_number, payload) VALUES (?, ?, ?)",
                        (
                            campaign.campaign_id.value,
                            campaign.revision_number,
                            dumps(campaign.to_dict()),
                        ),
                    )
                self._connection.execute(
                    "INSERT INTO mutation_operator_actions "
                    "(action_id, action_type, campaign_id, request_fingerprint, status, payload) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        result.action_id,
                        result.action_type,
                        result.campaign_id.value,
                        result.request_fingerprint,
                        result.status.value,
                        dumps(result.to_dict()),
                    ),
                )
                self._connection.commit()
                return Success(result)
            except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
                self._connection.rollback()
                return Failed(
                    "campaign_creation_commit_failed",
                    f"Cannot commit campaign creation: {exc}",
                    retriable=True,
                )
    def get_shard(self, shard_id: ShardId) -> Outcome[MutationShard | None]:
        # Load a shard projection after a coordinator restart.
        with self._lock:
            return self._load("mutation_shards", "shard_id", shard_id.value, MutationShard.from_dict)
    def save_shard(self, shard: MutationShard, *, expected_revision: int | None) -> Outcome[MutationShard]:
        # Persist a shard lease and progress projection with compare-and-swap.
        with self._lock:
            current = self.get_shard(shard.shard_id)
            if not is_success(current):
                return current
            return self._save_aggregate(
                table="mutation_shards",
                key_column="shard_id",
                key=shard.shard_id.value,
                candidate=shard,
                existing=current.value,
                expected_revision=expected_revision,
                entity="shard",
            )
    def get_lease(self, lease_id: str) -> Outcome[MutationLease | None]:
        # Load one authoritative lease generation after coordinator restart.
        with self._lock:
            return self._load("mutation_leases", "lease_id", str(lease_id), MutationLease.from_dict)
    def save_lease(
        self,
        lease: MutationLease,
        *,
        expected_revision: int | None,
    ) -> Outcome[MutationLease]:
        # Persist one lease generation with SQLite compare-and-swap semantics.
        with self._lock:
            current = self.get_lease(lease.lease_id)
            if not is_success(current):
                return current
            return self._save_aggregate(
                table="mutation_leases",
                key_column="lease_id",
                key=lease.registry_key,
                candidate=lease,
                existing=current.value,
                expected_revision=expected_revision,
                entity="lease",
            )
    @staticmethod
    def _worker_key(campaign_id: CampaignId, worker_id: str) -> str:
        # Scope reusable worker slot names by campaign in the SQLite primary key.
        return f"{campaign_id.value}\x1f{worker_id}"
    def get_worker(self, campaign_id: CampaignId, worker_id: str) -> Outcome[MutationWorker | None]:
        # Load one worker registration after coordinator restart.
        with self._lock:
            return self._load(
                "mutation_workers",
                "worker_key",
                self._worker_key(campaign_id, worker_id),
                MutationWorker.from_dict,
            )
    def save_worker(
        self,
        worker: MutationWorker,
        *,
        expected_revision: int | None,
    ) -> Outcome[MutationWorker]:
        # Persist worker registration and liveness with SQLite compare-and-swap.
        with self._lock:
            current = self.get_worker(worker.campaign_id, worker.worker_id)
            if not is_success(current):
                return current
            return self._save_aggregate(
                table="mutation_workers",
                key_column="worker_key",
                key=worker.registry_key,
                candidate=worker,
                existing=current.value,
                expected_revision=expected_revision,
                entity="worker",
            )
    def list_workers(self, campaign_id: CampaignId) -> Outcome[tuple[MutationWorker, ...]]:
        # Load every worker projection for one campaign from authoritative SQLite state.
        with self._lock:
            try:
                rows = self._connection.execute(
                    "SELECT payload FROM mutation_workers "
                    "WHERE json_extract(payload, '$.campaign_id') = ? "
                    "ORDER BY json_extract(payload, '$.identity.worker_id')",
                    (campaign_id.value,),
                ).fetchall()
                return Success(tuple(MutationWorker.from_dict(loads_object(row["payload"])) for row in rows))
            except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
                return Failed("store_corrupted", f"Cannot list workers: {exc}")
    def get_execution(self, execution_id: ExecutionId) -> Outcome[MutationExecution | None]:
        # Load one immutable mutant execution projection from SQLite.
        with self._lock:
            return self._load(
                "mutation_executions", "execution_id", execution_id.value, MutationExecution.from_dict
            )
    def get_executions(
        self,
        execution_ids: tuple[ExecutionId, ...],
    ) -> Outcome[tuple[MutationExecution, ...]]:
        # Load requested immutable execution identities through bounded indexed queries.
        if not execution_ids:
            return Success(())
        unique_ids = tuple(dict.fromkeys(item.value for item in execution_ids))
        with self._lock:
            try:
                values: list[MutationExecution] = []
                for offset in range(0, len(unique_ids), _SQLITE_VARIABLE_CHUNK):
                    batch = unique_ids[offset : offset + _SQLITE_VARIABLE_CHUNK]
                    placeholders = ", ".join("?" for _ in batch)
                    rows = self._connection.execute(
                        f"SELECT payload FROM mutation_executions WHERE execution_id IN ({placeholders})",
                        batch,
                    ).fetchall()
                    values.extend(MutationExecution.from_dict(loads_object(row["payload"])) for row in rows)
                return Success(tuple(values))
            except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
                return Failed("store_corrupted", f"Cannot load execution batch: {exc}")
    def list_executions(
        self,
        campaign_id: CampaignId,
    ) -> Outcome[tuple[MutationExecution, ...]]:
        # Load all immutable execution attempts for one campaign after a process restart.
        with self._lock:
            try:
                rows = self._connection.execute(
                    "SELECT payload FROM mutation_executions WHERE json_extract(payload, '$.campaign_id') = ? "
                    "ORDER BY json_extract(payload, '$.mutant_id'), json_extract(payload, '$.attempt'), execution_id",
                    (campaign_id.value,),
                ).fetchall()
                return Success(tuple(MutationExecution.from_dict(loads_object(row["payload"])) for row in rows))
            except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
                return Failed("store_corrupted", f"Cannot list executions: {exc}")
    def save_execution(
        self,
        execution: MutationExecution,
        *,
        expected_revision: int | None,
    ) -> Outcome[MutationExecution]:
        # Persist execution evidence without allowing terminal rewrites.
        with self._lock:
            current = self.get_execution(execution.execution_id)
            if not is_success(current):
                return current
            return self._save_aggregate(
                table="mutation_executions",
                key_column="execution_id",
                key=execution.execution_id.value,
                candidate=execution,
                existing=current.value,
                expected_revision=expected_revision,
                entity="execution",
            )
    def get_effect(self, effect_id: str) -> Outcome[EffectReceipt | None]:
        # Load one effect receipt before replaying a potentially retried command.
        with self._lock:
            try:
                row = self._connection.execute(
                    "SELECT effect_id, effect_type, campaign_id, payload FROM mutation_effects WHERE effect_id = ?",
                    (effect_id,),
                ).fetchone()
                if row is None:
                    return Success(None)
                return Success(
                    EffectReceipt.from_dict(
                        {
                            "effect_id": row["effect_id"],
                            "effect_type": row["effect_type"],
                            "campaign_id": row["campaign_id"],
                            "payload": loads_object(row["payload"]),
                        }
                    )
                )
            except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
                return Failed("store_corrupted", f"Cannot load effect: {exc}")
    def save_effect(self, receipt: EffectReceipt) -> Outcome[EffectReceipt]:
        # Insert an effect receipt exactly once and preserve the original response payload.
        with self._lock:
            existing = self.get_effect(receipt.effect_id)
            if not is_success(existing):
                return existing
            if existing.value is not None:
                if existing.value == receipt:
                    return Success(existing.value, message="Effect already recorded", duplicate=True)
                return Rejected(
                    code="effect_id_conflict",
                    message="Effect ID is already bound to another receipt",
                )
            try:
                with self._connection:
                    self._connection.execute(
                        "INSERT INTO mutation_effects (effect_id, effect_type, campaign_id, payload) VALUES (?, ?, ?, ?)",
                        (
                            receipt.effect_id,
                            receipt.effect_type,
                            receipt.campaign_id.value,
                            dumps(receipt.payload),
                        ),
                    )
                return Success(receipt)
            except sqlite3.IntegrityError:
                return self.get_effect(receipt.effect_id)
            except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
                return Failed("store_write_failed", f"Cannot save effect: {exc}", retriable=True)
    def commit_effect(
        self,
        *,
        receipt: EffectReceipt,
        campaign: MutationCampaign | None = None,
        campaign_expected_revision: int | None = None,
        shard: MutationShard | None = None,
        shard_expected_revision: int | None = None,
        lease: MutationLease | None = None,
        lease_expected_revision: int | None = None,
        worker: MutationWorker | None = None,
        worker_expected_revision: int | None = None,
        executions: tuple[MutationExecution, ...] = (),
    ) -> Outcome[EffectReceipt]:
        # Commit aggregate changes, effect receipt and outbox notification in one SQLite transaction.
        with self._lock:
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                existing_effect = self.get_effect(receipt.effect_id)
                if not is_success(existing_effect):
                    self._connection.rollback()
                    return existing_effect
                if existing_effect.value is not None:
                    self._connection.commit()
                    if existing_effect.value == receipt:
                        return Success(existing_effect.value, message="Effect already recorded", duplicate=True)
                    return Rejected(code="effect_id_conflict", message="Effect ID is already bound to another receipt")
                aggregate_rows = [
                    ("mutation_campaigns", "campaign_id", campaign.campaign_id.value, campaign, campaign_expected_revision, "campaign")
                    for campaign in (campaign,) if campaign is not None
                ]
                aggregate_rows.extend(
                    ("mutation_shards", "shard_id", shard.shard_id.value, shard, shard_expected_revision, "shard")
                    for shard in (shard,) if shard is not None
                )
                aggregate_rows.extend(
                    ("mutation_leases", "lease_id", lease.registry_key, lease, lease_expected_revision, "lease")
                    for lease in (lease,) if lease is not None
                )
                aggregate_rows.extend(
                    ("mutation_workers", "worker_key", worker.registry_key, worker, worker_expected_revision, "worker")
                    for worker in (worker,) if worker is not None
                )
                aggregate_rows.extend(
                    ("mutation_executions", "execution_id", execution.execution_id.value, execution, None, "execution")
                    for execution in executions
                )
                pending: list[tuple[str, str, str, Any, int | None]] = []
                execution_keys = tuple(
                    dict.fromkeys(
                        str(item[2])
                        for item in aggregate_rows
                        if item[0] == "mutation_executions"
                    )
                )
                execution_lookup: dict[str, Any] = {}
                if execution_keys:
                    for offset in range(0, len(execution_keys), _SQLITE_VARIABLE_CHUNK):
                        batch = execution_keys[offset : offset + _SQLITE_VARIABLE_CHUNK]
                        placeholders = ", ".join("?" for _ in batch)
                        rows = self._connection.execute(
                            f"SELECT execution_id, payload FROM mutation_executions "
                            f"WHERE execution_id IN ({placeholders})",
                            batch,
                        ).fetchall()
                        execution_lookup.update(
                            {str(row["execution_id"]): row for row in rows}
                        )
                for table, key_column, key, candidate, expected_revision, entity in aggregate_rows:
                    current_row = (
                        execution_lookup.get(key)
                        if table == "mutation_executions"
                        else self._connection.execute(
                            f"SELECT payload FROM {table} WHERE {key_column} = ?",
                            (key,),
                        ).fetchone()
                    )
                    current = None
                    if current_row is not None:
                        parser = {
                            "mutation_campaigns": MutationCampaign.from_dict,
                            "mutation_shards": MutationShard.from_dict,
                            "mutation_leases": MutationLease.from_dict,
                            "mutation_executions": MutationExecution.from_dict,
                            "mutation_workers": MutationWorker.from_dict,
                        }[table]
                        current = parser(loads_object(current_row["payload"]))
                    allowed, duplicate, error = _version_check(
                        current,
                        candidate,
                        expected_revision=expected_revision,
                        entity=entity,
                    )
                    if error is not None:
                        self._connection.rollback()
                        return error
                    if allowed and not duplicate:
                        pending.append((table, key_column, key, candidate, expected_revision))
                for table, key_column, key, candidate, expected_revision in pending:
                    payload = dumps(candidate.to_dict())
                    if expected_revision is None:
                        self._connection.execute(
                            f"INSERT INTO {table} ({key_column}, revision_number, payload) VALUES (?, ?, ?)",
                            (key, candidate.revision_number, payload),
                        )
                    else:
                        cursor = self._connection.execute(
                            f"UPDATE {table} SET revision_number = ?, payload = ? WHERE {key_column} = ? AND revision_number = ?",
                            (candidate.revision_number, payload, key, expected_revision),
                        )
                        if cursor.rowcount != 1:
                            self._connection.rollback()
                            return Rejected(code="stale_revision", message="Aggregate changed during effect commit")
                self._connection.execute(
                    "INSERT INTO mutation_effects (effect_id, effect_type, campaign_id, payload) VALUES (?, ?, ?, ?)",
                    (receipt.effect_id, receipt.effect_type, receipt.campaign_id.value, dumps(receipt.payload)),
                )
                self._connection.execute(
                    "INSERT INTO mutation_outbox (effect_id, event_type, campaign_id, payload, created_at) VALUES (?, ?, ?, ?, ?)",
                    (receipt.effect_id, receipt.effect_type, receipt.campaign_id.value, dumps(receipt.to_dict()), utc_now()),
                )
                self._connection.commit()
                return Success(receipt)
            except sqlite3.IntegrityError as exc:
                self._connection.rollback()
                return Failed("effect_commit_conflict", f"Cannot commit effect: {exc}", retriable=True)
            except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
                self._connection.rollback()
                return Failed("effect_commit_failed", f"Cannot commit effect: {exc}", retriable=True)
    def commit_plan_topology(
        self,
        *,
        receipt: EffectReceipt,
        campaign: MutationCampaign | None,
        campaign_expected_revision: int | None,
        shards: tuple[MutationShard, ...],
    ) -> Outcome[EffectReceipt]:
        # Commit the complete plan topology, receipt and outbox row in one SQLite transaction.
        with self._lock:
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                effect_row = self._connection.execute(
                    "SELECT effect_type, campaign_id, payload FROM mutation_effects WHERE effect_id = ?",
                    (receipt.effect_id,),
                ).fetchone()
                if effect_row is not None:
                    existing = EffectReceipt(
                        effect_id=receipt.effect_id,
                        effect_type=str(effect_row["effect_type"]),
                        campaign_id=CampaignId(str(effect_row["campaign_id"])),
                        payload=loads_object(effect_row["payload"]),
                    )
                    self._connection.commit()
                    if existing == receipt:
                        return Success(existing, message="Plan topology already committed", duplicate=True)
                    return Rejected(
                        code="effect_id_conflict",
                        message="Effect ID is already bound to another receipt",
                    )
                raw_campaign = receipt.payload.get("campaign")
                if not isinstance(raw_campaign, Mapping):
                    self._connection.rollback()
                    return Failed("effect_receipt_corrupted", "Plan topology receipt has no campaign projection")
                raw_shards = receipt.payload.get("shards")
                authority_campaign = MutationCampaign.from_dict(raw_campaign)
                receipt_shards = tuple(
                    MutationShard.from_dict(item)
                    for item in raw_shards
                ) if isinstance(raw_shards, (list, tuple)) else ()
                if receipt_shards != shards:
                    self._connection.rollback()
                    return Rejected(
                        code="plan_topology_receipt_conflict",
                        message="Plan topology receipt does not match the committed shard set",
                    )
                if receipt.campaign_id != authority_campaign.campaign_id:
                    self._connection.rollback()
                    return Rejected(
                        code="plan_topology_campaign_mismatch",
                        message="Plan topology receipt belongs to another campaign",
                    )
                input_error = _plan_topology_error(authority_campaign, shards)
                if input_error is not None:
                    self._connection.rollback()
                    return input_error
                campaign_row = self._connection.execute(
                    "SELECT revision_number, payload FROM mutation_campaigns WHERE campaign_id = ?",
                    (authority_campaign.campaign_id.value,),
                ).fetchone()
                existing_campaign = (
                    MutationCampaign.from_dict(loads_object(campaign_row["payload"]))
                    if campaign_row is not None
                    else None
                )
                if campaign is None:
                    if existing_campaign != authority_campaign:
                        self._connection.rollback()
                        return Rejected(
                            code="campaign_plan_conflict",
                            message="Stored campaign projection conflicts with plan topology receipt",
                        )
                else:
                    if campaign != authority_campaign:
                        self._connection.rollback()
                        return Rejected(
                            code="campaign_plan_conflict",
                            message="Candidate campaign projection conflicts with plan topology receipt",
                        )
                    allowed, duplicate, error = _version_check(
                        existing_campaign,
                        campaign,
                        expected_revision=campaign_expected_revision,
                        entity="campaign",
                    )
                    if error is not None:
                        self._connection.rollback()
                        return error
                    if duplicate or not allowed:
                        self._connection.rollback()
                        return Rejected(
                            code="campaign_plan_conflict",
                            message="Plan topology requires one new campaign revision",
                        )
                existing_rows = self._connection.execute(
                    "SELECT payload FROM mutation_shards ORDER BY shard_id"
                ).fetchall()
                existing_shards = tuple(
                    MutationShard.from_dict(loads_object(row["payload"]))
                    for row in existing_rows
                )
                campaign_shards = tuple(
                    item for item in existing_shards if item.campaign_id == authority_campaign.campaign_id
                )
                if campaign_shards:
                    self._connection.rollback()
                    return Rejected(
                        code="partial_plan_topology",
                        message="Plan topology already contains shard rows without its atomic effect receipt",
                        details={
                            "existing_shards": ",".join(
                                sorted(item.shard_id.value for item in campaign_shards)
                            )
                        },
                    )
                existing_ids = {item.shard_id.value for item in existing_shards}
                conflicting_ids = tuple(
                    sorted(item.shard_id.value for item in shards if item.shard_id.value in existing_ids)
                )
                if conflicting_ids:
                    self._connection.rollback()
                    return Rejected(
                        code="plan_topology_conflict",
                        message="Plan shard identity is already owned by another campaign",
                        details={"shard_ids": ",".join(conflicting_ids)},
                    )
                if campaign is not None:
                    cursor = self._connection.execute(
                        "UPDATE mutation_campaigns SET revision_number = ?, payload = ? "
                        "WHERE campaign_id = ? AND revision_number = ?",
                        (
                            campaign.revision_number,
                            dumps(campaign.to_dict()),
                            campaign.campaign_id.value,
                            campaign_expected_revision,
                        ),
                    )
                    if cursor.rowcount != 1:
                        self._connection.rollback()
                        return Rejected(
                            code="stale_revision",
                            message="Campaign changed during plan topology commit",
                        )
                for shard in shards:
                    self._connection.execute(
                        "INSERT INTO mutation_shards (shard_id, revision_number, payload) VALUES (?, ?, ?)",
                        (shard.shard_id.value, shard.revision_number, dumps(shard.to_dict())),
                    )
                self._connection.execute(
                    "INSERT INTO mutation_effects (effect_id, effect_type, campaign_id, payload) VALUES (?, ?, ?, ?)",
                    (receipt.effect_id, receipt.effect_type, receipt.campaign_id.value, dumps(receipt.payload)),
                )
                self._connection.execute(
                    "INSERT INTO mutation_outbox (effect_id, event_type, campaign_id, payload, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        receipt.effect_id,
                        receipt.effect_type,
                        receipt.campaign_id.value,
                        dumps(receipt.to_dict()),
                        utc_now(),
                    ),
                )
                self._connection.commit()
                return Success(receipt)
            except sqlite3.IntegrityError as exc:
                self._connection.rollback()
                return Failed(
                    "plan_topology_commit_conflict",
                    f"Cannot commit plan topology: {exc}",
                    retriable=True,
                )
            except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
                self._connection.rollback()
                return Failed(
                    "plan_topology_commit_failed",
                    f"Cannot commit plan topology: {exc}",
                    retriable=True,
                )
    def get_finalization_intent(self, campaign_id: CampaignId) -> Outcome[FinalizationIntent | None]:
        # Load one durable finalization intent by campaign identity.
        with self._lock:
            try:
                row = self._connection.execute(
                    "SELECT payload FROM mutation_finalizations WHERE campaign_id = ?",
                    (campaign_id.value,),
                ).fetchone()
                return Success(
                    FinalizationIntent.from_dict(loads_object(row["payload"]))
                    if row is not None
                    else None
                )
            except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
                return Failed("store_corrupted", f"Cannot load finalization intent: {exc}")
    def list_artifacts(self, campaign_id: CampaignId) -> Outcome[tuple[ArtifactRegistryEntry, ...]]:
        # Load immutable artifact registry rows in stable logical-key order.
        with self._lock:
            try:
                rows = self._connection.execute(
                    "SELECT payload FROM mutation_artifacts WHERE campaign_id = ? ORDER BY logical_key",
                    (campaign_id.value,),
                ).fetchall()
                return Success(
                    tuple(ArtifactRegistryEntry.from_dict(loads_object(row["payload"])) for row in rows)
                )
            except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
                return Failed("store_corrupted", f"Cannot load artifact registry: {exc}")
    def commit_finalization(
        self,
        *,
        receipt: EffectReceipt,
        intent: FinalizationIntent,
        artifacts: tuple[ArtifactRegistryEntry, ...] = (),
        campaign: MutationCampaign | None = None,
        campaign_expected_revision: int | None = None,
    ) -> Outcome[FinalizationIntent]:
        # Commit intent, content registry, campaign transition, effect receipt and outbox in one transaction.
        with self._lock:
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                effect_row = self._connection.execute(
                    "SELECT effect_type, campaign_id, payload FROM mutation_effects WHERE effect_id = ?",
                    (receipt.effect_id,),
                ).fetchone()
                if effect_row is not None:
                    existing_receipt = EffectReceipt(
                        receipt.effect_id,
                        str(effect_row["effect_type"]),
                        CampaignId(str(effect_row["campaign_id"])),
                        loads_object(effect_row["payload"]),
                    )
                    self._connection.commit()
                    if existing_receipt != receipt:
                        return Rejected(code="effect_id_conflict", message="Effect ID is already bound to another receipt")
                    raw_intent = existing_receipt.payload.get("finalization_intent")
                    if not isinstance(raw_intent, Mapping):
                        return Failed("effect_receipt_corrupted", "Finalization effect has no intent payload")
                    return Success(FinalizationIntent.from_dict(raw_intent), duplicate=True)
                input_error = _finalization_input_error(receipt, intent, artifacts, campaign)
                if input_error is not None:
                    self._connection.rollback()
                    return input_error
                intent_row = self._connection.execute(
                    "SELECT payload FROM mutation_finalizations WHERE campaign_id = ?",
                    (intent.campaign_id.value,),
                ).fetchone()
                existing_intent = (
                    FinalizationIntent.from_dict(loads_object(intent_row["payload"]))
                    if intent_row is not None
                    else None
                )
                conflict = _finalization_compatible(existing_intent, intent)
                if conflict is not None:
                    self._connection.rollback()
                    return conflict
                registry_rows = self._connection.execute(
                    "SELECT logical_key, payload FROM mutation_artifacts WHERE campaign_id = ?",
                    (intent.campaign_id.value,),
                ).fetchall()
                registry = {
                    str(row["logical_key"]): ArtifactRegistryEntry.from_dict(loads_object(row["payload"]))
                    for row in registry_rows
                }
                pending_artifacts: list[ArtifactRegistryEntry] = []
                for artifact in artifacts:
                    existing_artifact = registry.get(artifact.logical_key)
                    if existing_artifact is not None and existing_artifact != artifact:
                        self._connection.rollback()
                        return Rejected(
                            code="artifact_logical_key_conflict",
                            message="Artifact logical key is already bound to different content",
                            details={
                                "logical_key": artifact.logical_key,
                                "existing_sha256": existing_artifact.content_sha256,
                                "candidate_sha256": artifact.content_sha256,
                            },
                        )
                    if existing_artifact is None:
                        pending_artifacts.append(artifact)
                        registry[artifact.logical_key] = artifact
                registry_error = _exact_registry_error(intent, registry)
                if registry_error is not None:
                    self._connection.rollback()
                    return registry_error
                if campaign is not None:
                    campaign_row = self._connection.execute(
                        "SELECT payload FROM mutation_campaigns WHERE campaign_id = ?",
                        (campaign.campaign_id.value,),
                    ).fetchone()
                    existing_campaign = (
                        MutationCampaign.from_dict(loads_object(campaign_row["payload"]))
                        if campaign_row is not None
                        else None
                    )
                    allowed, duplicate, error = _version_check(
                        existing_campaign,
                        campaign,
                        expected_revision=campaign_expected_revision,
                        entity="campaign",
                    )
                    if error is not None:
                        self._connection.rollback()
                        return error
                    if allowed and not duplicate:
                        cursor = self._connection.execute(
                            "UPDATE mutation_campaigns SET revision_number = ?, payload = ? "
                            "WHERE campaign_id = ? AND revision_number = ?",
                            (
                                campaign.revision_number,
                                dumps(campaign.to_dict()),
                                campaign.campaign_id.value,
                                campaign_expected_revision,
                            ),
                        )
                        if cursor.rowcount != 1:
                            self._connection.rollback()
                            return Rejected(code="stale_revision", message="Campaign changed during finalization commit")
                for artifact in pending_artifacts:
                    self._connection.execute(
                        "INSERT INTO mutation_artifacts "
                        "(artifact_key, campaign_id, logical_key, content_sha256, payload) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (
                            _artifact_registry_key(artifact.campaign_id, artifact.logical_key),
                            artifact.campaign_id.value,
                            artifact.logical_key,
                            artifact.content_sha256,
                            dumps(artifact.to_dict()),
                        ),
                    )
                intent_payload = dumps(intent.to_dict())
                if existing_intent is None:
                    self._connection.execute(
                        "INSERT INTO mutation_finalizations (campaign_id, intent_id, status, payload) "
                        "VALUES (?, ?, ?, ?)",
                        (intent.campaign_id.value, intent.intent_id, intent.status, intent_payload),
                    )
                else:
                    self._connection.execute(
                        "UPDATE mutation_finalizations SET status = ?, payload = ? WHERE campaign_id = ?",
                        (intent.status, intent_payload, intent.campaign_id.value),
                    )
                self._connection.execute(
                    "INSERT INTO mutation_effects (effect_id, effect_type, campaign_id, payload) VALUES (?, ?, ?, ?)",
                    (receipt.effect_id, receipt.effect_type, receipt.campaign_id.value, dumps(receipt.payload)),
                )
                self._connection.execute(
                    "INSERT INTO mutation_outbox (effect_id, event_type, campaign_id, payload, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (receipt.effect_id, receipt.effect_type, receipt.campaign_id.value, dumps(receipt.to_dict()), utc_now()),
                )
                self._connection.commit()
                return Success(intent)
            except sqlite3.IntegrityError as exc:
                self._connection.rollback()
                return Failed("finalization_commit_conflict", f"Cannot commit finalization: {exc}", retriable=True)
            except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
                self._connection.rollback()
                return Failed("finalization_commit_failed", f"Cannot commit finalization: {exc}", retriable=True)
    def get_operator_action(self, action_id: str) -> Outcome[OperatorAction | None]:
        # Load one durable operator action before replaying a command.
        with self._lock:
            try:
                row = self._connection.execute(
                    "SELECT payload FROM mutation_operator_actions WHERE action_id = ?",
                    (str(action_id),),
                ).fetchone()
                if row is None:
                    return Success(None)
                return Success(OperatorAction.from_dict(loads_object(row["payload"])))
            except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
                return Failed("store_corrupted", f"Cannot load operator action: {exc}")
    def save_operator_action(
        self,
        action: OperatorAction,
        *,
        expected_status: OperatorActionStatus | None,
    ) -> Outcome[OperatorAction]:
        # Create or transition one operator action through a status compare-and-swap.
        with self._lock:
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                row = self._connection.execute(
                    "SELECT action_type, campaign_id, request_fingerprint, status, payload "
                    "FROM mutation_operator_actions WHERE action_id = ?",
                    (action.action_id,),
                ).fetchone()
                if row is None:
                    if expected_status is not None:
                        self._connection.rollback()
                        return Rejected(code="operator_action_missing", message="Operator action does not exist")
                    self._connection.execute(
                        "INSERT INTO mutation_operator_actions "
                        "(action_id, action_type, campaign_id, request_fingerprint, status, payload) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            action.action_id,
                            action.action_type,
                            action.campaign_id.value,
                            action.request_fingerprint,
                            action.status.value,
                            dumps(action.to_dict()),
                        ),
                    )
                    self._connection.commit()
                    return Success(action)
                existing = OperatorAction.from_dict(loads_object(row["payload"]))
                if (
                    existing.action_type != action.action_type
                    or existing.campaign_id != action.campaign_id
                    or existing.request_fingerprint != action.request_fingerprint
                ):
                    self._connection.rollback()
                    return Rejected(code="action_id_conflict", message="Action ID is bound to another request")
                if existing == action:
                    self._connection.commit()
                    return Success(existing, duplicate=True)
                if expected_status is None or existing.status != expected_status:
                    self._connection.rollback()
                    return Rejected(
                        code="stale_operator_action",
                        message="Operator action status changed",
                        details={"expected_status": expected_status.value if expected_status else None, "actual_status": existing.status.value},
                    )
                cursor = self._connection.execute(
                    "UPDATE mutation_operator_actions SET status = ?, payload = ? "
                    "WHERE action_id = ? AND status = ?",
                    (action.status.value, dumps(action.to_dict()), action.action_id, expected_status.value),
                )
                if cursor.rowcount != 1:
                    self._connection.rollback()
                    return Rejected(code="stale_operator_action", message="Operator action changed during persistence")
                self._connection.commit()
                return Success(action)
            except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
                self._connection.rollback()
                return Failed("operator_action_write_failed", f"Cannot save operator action: {exc}", retriable=True)
    def commit_campaign_retry(
        self,
        action: OperatorAction,
        shards: tuple[MutationShard, ...],
        *,
        expected_revisions: Mapping[str, int],
    ) -> Outcome[OperatorAction]:
        # Atomically persist all retried shards and the completed operator receipt.
        with self._lock:
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                row = self._connection.execute(
                    "SELECT payload FROM mutation_operator_actions WHERE action_id = ?",
                    (action.action_id,),
                ).fetchone()
                if row is None:
                    self._connection.rollback()
                    return Rejected(code="operator_action_missing", message="Operator action does not exist")
                existing_action = OperatorAction.from_dict(loads_object(row["payload"]))
                if existing_action.status == OperatorActionStatus.COMPLETED and existing_action == action:
                    self._connection.commit()
                    return Success(existing_action, duplicate=True)
                if existing_action.status != OperatorActionStatus.RUNNING:
                    self._connection.rollback()
                    return Rejected(code="stale_operator_action", message="Operator action is not running")
                if (
                    existing_action.action_type != action.action_type
                    or existing_action.campaign_id != action.campaign_id
                    or existing_action.request_fingerprint != action.request_fingerprint
                ):
                    self._connection.rollback()
                    return Rejected(code="action_id_conflict", message="Action ID is bound to another request")
                pending: list[tuple[MutationShard, int]] = []
                for shard in shards:
                    expected = expected_revisions.get(shard.shard_id.value)
                    row = self._connection.execute(
                        "SELECT revision_number, payload FROM mutation_shards WHERE shard_id = ?",
                        (shard.shard_id.value,),
                    ).fetchone()
                    existing = MutationShard.from_dict(loads_object(row["payload"])) if row is not None else None
                    allowed, duplicate, error = _version_check(
                        existing,
                        shard,
                        expected_revision=expected,
                        entity="shard",
                    )
                    if error is not None:
                        self._connection.rollback()
                        return error
                    if allowed and not duplicate:
                        pending.append((shard, int(expected)))
                for shard, expected in pending:
                    cursor = self._connection.execute(
                        "UPDATE mutation_shards SET revision_number = ?, payload = ? "
                        "WHERE shard_id = ? AND revision_number = ?",
                        (shard.revision_number, dumps(shard.to_dict()), shard.shard_id.value, expected),
                    )
                    if cursor.rowcount != 1:
                        self._connection.rollback()
                        return Rejected(code="stale_revision", message="Shard changed during campaign retry")
                cursor = self._connection.execute(
                    "UPDATE mutation_operator_actions SET status = ?, payload = ? "
                    "WHERE action_id = ? AND status = ?",
                    (
                        action.status.value,
                        dumps(action.to_dict()),
                        action.action_id,
                        OperatorActionStatus.RUNNING.value,
                    ),
                )
                if cursor.rowcount != 1:
                    self._connection.rollback()
                    return Rejected(code="stale_operator_action", message="Operator action changed during retry commit")
                self._connection.commit()
                return Success(action)
            except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
                self._connection.rollback()
                return Failed("campaign_retry_commit_failed", f"Cannot commit campaign retry: {exc}", retriable=True)
    def list_campaigns(self) -> Outcome[tuple[MutationCampaign, ...]]:
        # Load every campaign projection for coordinator startup reconciliation.
        with self._lock:
            try:
                rows = self._connection.execute("SELECT payload FROM mutation_campaigns ORDER BY campaign_id").fetchall()
                return Success(tuple(MutationCampaign.from_dict(loads_object(row["payload"])) for row in rows))
            except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
                return Failed("store_corrupted", f"Cannot list campaigns: {exc}")
    def list_shards(self, campaign_id: CampaignId) -> Outcome[tuple[MutationShard, ...]]:
        # Load all shard projections for one campaign's lease reconciliation.
        with self._lock:
            try:
                rows = self._connection.execute(
                    "SELECT payload FROM mutation_shards "
                    "WHERE json_extract(payload, '$.campaign_id') = ? ORDER BY shard_id",
                    (campaign_id.value,),
                ).fetchall()
                return Success(
                    tuple(MutationShard.from_dict(loads_object(row["payload"])) for row in rows)
                )
            except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
                return Failed("store_corrupted", f"Cannot list shards: {exc}")
    def list_outbox(self, *, undelivered_only: bool = True) -> Outcome[tuple[dict[str, Any], ...]]:
        # Expose durable outbox records for a future dispatcher without mutating delivery state.
        with self._lock:
            try:
                query = "SELECT effect_id, event_type, campaign_id, payload, created_at, delivered_at FROM mutation_outbox"
                if undelivered_only:
                    query += " WHERE delivered_at IS NULL"
                query += " ORDER BY created_at, effect_id"
                rows = self._connection.execute(query).fetchall()
                return Success(tuple(dict(row) for row in rows))
            except sqlite3.DatabaseError as exc:
                return Failed("store_corrupted", f"Cannot list outbox: {exc}")
    def acknowledge_outbox(self, effect_ids: tuple[str, ...]) -> Outcome[int]:
        # Mark successfully projected outbox effects delivered without deleting their audit records.
        if not effect_ids:
            return Success(0)
        with self._lock:
            try:
                placeholders = ", ".join("?" for _ in effect_ids)
                with self._connection:
                    cursor = self._connection.execute(
                        f"UPDATE mutation_outbox SET delivered_at = ? "
                        f"WHERE delivered_at IS NULL AND effect_id IN ({placeholders})",
                        (utc_now(), *effect_ids),
                    )
                return Success(int(cursor.rowcount))
            except sqlite3.DatabaseError as exc:
                return Failed("store_write_failed", f"Cannot acknowledge outbox: {exc}", retriable=True)
