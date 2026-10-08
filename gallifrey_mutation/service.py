"""Idempotent mutation effect handlers over the Gallifrey mutation repositories."""
from __future__ import annotations
from collections import Counter
from dataclasses import dataclass, replace
from typing import Any, Mapping
from theseus_contracts import (
    ArtifactId,
    ArtifactRegistryEntry,
    CampaignConfiguration,
    CampaignId,
    FinalizationIntent,
    MutantExecutionResult,
    PlanDecision,
    ShardDescriptor,
    ShardExecutionResult,
    ShardId,
    ShardLease,
    WorkerCapabilities,
    WorkerHeartbeat,
    WorkerIdentity,
    deterministic_id,
)
from theseus_contracts.serialization import utc_now
from .domain import (
    CampaignState,
    MutationCampaign,
    MutationExecution,
    MutationExecutionState,
    MutationLease,
    MutationLeaseState,
    MutationResult,
    MutationShard,
    MutationShardState,
    MutationWorker,
    OperatorAction,
    OperatorActionStatus,
)
from .outcomes import Failed, Outcome, Rejected, Success, is_success
from .store import EffectReceipt, MutationStore
@dataclass(frozen=True, slots=True)
class ShardResultReceipt:
    """Exact fan-in projection returned by an idempotent execute-shard effect."""
    campaign: MutationCampaign
    shard: MutationShard
    executions: tuple[MutationExecution, ...]
    duplicate: bool = False
@dataclass(frozen=True, slots=True)
class LeaseStateReceipt:
    """Atomic lease state plus its shard and optional worker projections."""
    lease: MutationLease
    shard: MutationShard
    worker: MutationWorker | None = None
    duplicate: bool = False
class MutationCampaignService:
    """Application service exposing named mutation effects without AST knowledge."""
    def __init__(self, store: MutationStore) -> None:
        # Keep the service dependent on repository ports rather than SQLite or runner internals.
        self.store = store
    def create_campaign(
        self,
        configuration: CampaignConfiguration,
        *,
        revision_id: Any | None = None,
    ) -> Outcome[MutationCampaign]:
        # Create a standalone mutation campaign aggregate at revision zero.
        campaign = MutationCampaign.create(configuration, revision_id=revision_id)
        return self.store.save_campaign(campaign, expected_revision=None)
    def create_campaign_action(
        self,
        action_id: str,
        configuration: CampaignConfiguration,
    ) -> Outcome[OperatorAction]:
        # Atomically create one campaign and a replay-safe terminal operator receipt.
        try:
            action = OperatorAction.create(
                action_id,
                "create_campaign",
                configuration.campaign_id,
                0,
                parameters={"configuration": configuration.to_dict()},
            )
            campaign = MutationCampaign.create(configuration)
        except (TypeError, ValueError) as exc:
            return Rejected(code="invalid_campaign_launch", message=str(exc))
        return self.store.commit_campaign_creation(action, campaign)
    def get_campaign(self, campaign_id: CampaignId) -> Outcome[MutationCampaign]:
        # Expose a read-only campaign lookup to coordinators without leaking repository details.
        return self._campaign(campaign_id)
    def _existing_effect(self, effect_id: str) -> Outcome[EffectReceipt | None]:
        # Read an idempotency receipt before touching any aggregate state.
        return self.store.get_effect(effect_id)
    def get_worker(self, campaign_id: CampaignId, worker_id: str) -> Outcome[MutationWorker]:
        # Expose one authoritative worker registration without leaking repository details.
        current = self.store.get_worker(campaign_id, worker_id)
        if not is_success(current):
            return current
        if current.value is None:
            return Rejected(
                code="worker_not_found",
                message="Worker registration does not exist",
                details={"campaign_id": campaign_id.value, "worker_id": worker_id},
            )
        return Success(current.value)
    def list_workers(self, campaign_id: CampaignId) -> Outcome[tuple[MutationWorker, ...]]:
        # Return all authoritative workers for projection and startup reconciliation.
        return self.store.list_workers(campaign_id)
    def get_operator_action(self, action_id: str) -> Outcome[OperatorAction | None]:
        # Expose one durable operator receipt for exact API replay.
        return self.store.get_operator_action(action_id)
    def request_operator_action(
        self,
        action_id: str,
        action_type: str,
        campaign_id: CampaignId,
        *,
        expected_revision: int,
        parameters: Mapping[str, Any] | None = None,
    ) -> Outcome[OperatorAction]:
        # Create one durable request after optimistic campaign fencing or return its exact replay.
        try:
            candidate = OperatorAction.create(
                action_id,
                action_type,
                campaign_id,
                expected_revision,
                parameters=parameters,
            )
        except (TypeError, ValueError) as exc:
            return Rejected(code="invalid_operator_action", message=str(exc))
        expected_effect_type = {
            "cancel": "mutation.cancel",
            "retry_shard": "mutation.retry_shard",
        }.get(candidate.action_type)
        existing_effect = self.store.get_effect(candidate.action_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None and existing_effect.value.effect_type != expected_effect_type:
            return Rejected(code="action_id_conflict", message="Action ID is bound to another mutation effect")
        existing = self.store.get_operator_action(candidate.action_id)
        if not is_success(existing):
            return existing
        if existing.value is not None:
            if (
                existing.value.action_type == candidate.action_type
                and existing.value.campaign_id == candidate.campaign_id
                and existing.value.request_fingerprint == candidate.request_fingerprint
            ):
                return Success(existing.value, duplicate=True)
            return Rejected(code="action_id_conflict", message="Action ID is bound to another request")
        campaign = self._campaign(campaign_id)
        if not is_success(campaign):
            if isinstance(campaign, Rejected):
                rejected = candidate.reject(campaign.code, campaign.details)
                return self.store.save_operator_action(rejected, expected_status=None)
            return campaign
        if campaign.value.revision_number != expected_revision:
            rejected = candidate.reject(
                "stale_revision",
                {
                    "expected_revision": expected_revision,
                    "actual_revision": campaign.value.revision_number,
                },
            )
            return self.store.save_operator_action(rejected, expected_status=None)
        return self.store.save_operator_action(candidate, expected_status=None)
    def start_operator_action(self, action_id: str) -> Outcome[OperatorAction]:
        # Advance one requested action while treating a running replay as idempotent.
        current = self.store.get_operator_action(action_id)
        if not is_success(current):
            return current
        if current.value is None:
            return Rejected(code="operator_action_missing", message="Operator action does not exist")
        if current.value.status == OperatorActionStatus.RUNNING:
            return Success(current.value, duplicate=True)
        if current.value.status != OperatorActionStatus.REQUESTED:
            return Success(current.value, duplicate=True)
        return self.store.save_operator_action(
            current.value.start(),
            expected_status=OperatorActionStatus.REQUESTED,
        )
    def update_running_operator_action(
        self,
        action_id: str,
        result: Mapping[str, Any],
    ) -> Outcome[OperatorAction]:
        # Persist bounded process metadata while an operator action remains running.
        current = self.store.get_operator_action(action_id)
        if not is_success(current):
            return current
        if current.value is None:
            return Rejected(code="operator_action_missing", message="Operator action does not exist")
        if current.value.status in {
            OperatorActionStatus.COMPLETED,
            OperatorActionStatus.REJECTED,
            OperatorActionStatus.FAILED,
        }:
            return Success(current.value, duplicate=True)
        if current.value.status != OperatorActionStatus.RUNNING:
            return Rejected(code="stale_operator_action", message="Operator action is not running")
        try:
            updated = current.value.update_running(result)
        except (TypeError, ValueError) as exc:
            return Rejected(code="invalid_operator_result", message=str(exc))
        return self.store.save_operator_action(
            updated,
            expected_status=OperatorActionStatus.RUNNING,
        )
    def complete_operator_action(
        self,
        action_id: str,
        result: Mapping[str, Any],
    ) -> Outcome[OperatorAction]:
        # Persist one terminal success for exact operator replay.
        current = self.store.get_operator_action(action_id)
        if not is_success(current):
            return current
        if current.value is None:
            return Rejected(code="operator_action_missing", message="Operator action does not exist")
        if current.value.status == OperatorActionStatus.COMPLETED:
            return Success(current.value, duplicate=True)
        if current.value.status not in {OperatorActionStatus.REQUESTED, OperatorActionStatus.RUNNING}:
            return Rejected(code="operator_action_terminal", message="Operator action already failed")
        expected = current.value.status
        try:
            completed = current.value.complete(result)
        except (TypeError, ValueError) as exc:
            return Rejected(code="invalid_operator_result", message=str(exc))
        return self.store.save_operator_action(completed, expected_status=expected)
    def reject_operator_action(
        self,
        action_id: str,
        code: str,
        *,
        result: Mapping[str, Any] | None = None,
    ) -> Outcome[OperatorAction]:
        # Persist one expected operator rejection without exposing exception text.
        current = self.store.get_operator_action(action_id)
        if not is_success(current):
            return current
        if current.value is None:
            return Rejected(code="operator_action_missing", message="Operator action does not exist")
        if current.value.status == OperatorActionStatus.REJECTED:
            return Success(current.value, duplicate=True)
        if current.value.status in {OperatorActionStatus.COMPLETED, OperatorActionStatus.FAILED}:
            return Rejected(code="operator_action_terminal", message="Operator action is already terminal")
        expected = current.value.status
        return self.store.save_operator_action(
            current.value.reject(code, result),
            expected_status=expected,
        )
    def fail_operator_action(
        self,
        action_id: str,
        code: str,
        *,
        retriable: bool,
        result: Mapping[str, Any] | None = None,
    ) -> Outcome[OperatorAction]:
        # Persist one sanitized technical failure for exact operator replay.
        current = self.store.get_operator_action(action_id)
        if not is_success(current):
            return current
        if current.value is None:
            return Rejected(code="operator_action_missing", message="Operator action does not exist")
        if current.value.status == OperatorActionStatus.FAILED:
            return Success(current.value, duplicate=True)
        if current.value.status in {OperatorActionStatus.COMPLETED, OperatorActionStatus.REJECTED}:
            return Rejected(code="operator_action_terminal", message="Operator action is already terminal")
        expected = current.value.status
        return self.store.save_operator_action(
            current.value.fail(code, retriable=retriable, result=result),
            expected_status=expected,
        )
    def retry_campaign(
        self,
        action_id: str,
        campaign_id: CampaignId,
        *,
        expected_revision: int,
    ) -> Outcome[OperatorAction]:
        # Requeue every retryable shard atomically while preserving complete execution attempts.
        requested = self.request_operator_action(
            action_id,
            "retry_campaign",
            campaign_id,
            expected_revision=expected_revision,
        )
        if not is_success(requested):
            return requested
        if requested.value.status in {
            OperatorActionStatus.COMPLETED,
            OperatorActionStatus.REJECTED,
            OperatorActionStatus.FAILED,
        }:
            return Success(requested.value, duplicate=True)
        started = self.start_operator_action(action_id)
        if not is_success(started):
            return started
        shards = self.store.list_shards(campaign_id)
        if not is_success(shards):
            return shards
        retried: list[MutationShard] = []
        expected_revisions: dict[str, int] = {}
        for shard in shards.value:
            if shard.status not in {
                MutationShardState.PARTIAL,
                MutationShardState.FAILED,
                MutationShardState.ORPHANED,
            }:
                continue
            candidate = shard.retry(expected_revision=shard.revision_number)
            if not is_success(candidate):
                return candidate
            retried.append(candidate.value)
            expected_revisions[shard.shard_id.value] = shard.revision_number
        result = {
            "campaign_revision": expected_revision,
            "shard_ids": [item.shard_id.value for item in retried],
            "shard_revisions": {
                item.shard_id.value: item.revision_number for item in retried
            },
            "retry_count": len(retried),
        }
        completed = started.value.complete(result)
        return self.store.commit_campaign_retry(
            completed,
            tuple(retried),
            expected_revisions=expected_revisions,
        )
    def get_lease(self, lease_id: str) -> Outcome[MutationLease]:
        # Resolve one required authoritative lease generation by immutable token.
        current = self.store.get_lease(lease_id)
        if not is_success(current):
            return current
        if current.value is None:
            return Rejected(code="lease_not_found", message="Authoritative lease does not exist")
        return Success(current.value)
    def _lease_state_from_receipt(self, receipt: EffectReceipt) -> Outcome[LeaseStateReceipt]:
        # Rehydrate the exact atomic lease, shard and worker projections for effect replay.
        raw_lease = receipt.payload.get("lease")
        raw_shard = receipt.payload.get("shard")
        raw_worker = receipt.payload.get("worker")
        if not isinstance(raw_lease, Mapping) or not isinstance(raw_shard, Mapping):
            return Failed("effect_receipt_corrupted", "Lease effect receipt is incomplete")
        try:
            worker = MutationWorker.from_dict(raw_worker) if isinstance(raw_worker, Mapping) else None
            return Success(
                LeaseStateReceipt(
                    MutationLease.from_dict(raw_lease),
                    MutationShard.from_dict(raw_shard),
                    worker,
                    duplicate=True,
                ),
                duplicate=True,
            )
        except (TypeError, ValueError) as exc:
            return Failed("effect_receipt_corrupted", f"Cannot decode lease receipt: {exc}")
    def _commit_lease_state(
        self,
        *,
        effect_id: str,
        effect_type: str,
        lease: MutationLease,
        shard: MutationShard,
        worker: MutationWorker | None,
        lease_expected_revision: int | None,
        shard_expected_revision: int | None,
        worker_expected_revision: int | None,
    ) -> Outcome[LeaseStateReceipt]:
        # Commit every lease-owned projection and its replay receipt in one store transaction.
        receipt = EffectReceipt(
            effect_id=effect_id,
            effect_type=effect_type,
            campaign_id=lease.campaign_id,
            payload={
                "lease": lease.to_dict(),
                "shard": shard.to_dict(),
                "worker": worker.to_dict() if worker is not None else None,
            },
        )
        committed = self._commit_effect(
            receipt,
            shard=shard,
            shard_expected_revision=shard_expected_revision,
            lease=lease,
            lease_expected_revision=lease_expected_revision,
            worker=worker,
            worker_expected_revision=worker_expected_revision,
        )
        if not is_success(committed):
            return committed
        if committed.duplicate:
            return self._lease_state_from_receipt(committed.value)
        return Success(LeaseStateReceipt(lease, shard, worker))
    def claim_shard_lease(
        self,
        effect_id: str,
        shard_id: ShardId,
        lease_contract: ShardLease,
        *,
        expected_shard_revision: int,
    ) -> Outcome[LeaseStateReceipt]:
        # Create the authoritative lease generation and shard projection atomically.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            return self._lease_state_from_receipt(existing_effect.value)
        stored_shard = self.store.get_shard(shard_id)
        if not is_success(stored_shard):
            return stored_shard
        if stored_shard.value is None:
            return Rejected(code="shard_not_found", message="Mutation shard does not exist")
        existing_lease = self.store.get_lease(lease_contract.lease_id)
        if not is_success(existing_lease):
            return existing_lease
        if existing_lease.value is not None:
            return Rejected(code="lease_id_conflict", message="Lease token already identifies another generation")
        lease = MutationLease.claim(stored_shard.value.campaign_id, shard_id, lease_contract)
        claimed = stored_shard.value.claim(lease.to_shard_lease(), expected_revision=expected_shard_revision)
        if not is_success(claimed):
            return claimed
        return self._commit_lease_state(
            effect_id=effect_id,
            effect_type="mutation.lease.claim",
            lease=lease,
            shard=claimed.value,
            worker=None,
            lease_expected_revision=None,
            shard_expected_revision=stored_shard.value.revision_number,
            worker_expected_revision=None,
        )
    def reassign_orphaned_shard(
        self,
        effect_id: str,
        shard_id: ShardId,
        lease_contract: ShardLease,
        *,
        expected_shard_revision: int,
    ) -> Outcome[LeaseStateReceipt]:
        # Create the next lease generation and advance an orphaned shard exactly once in one transaction.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            return self._lease_state_from_receipt(existing_effect.value)
        stored_shard = self.store.get_shard(shard_id)
        if not is_success(stored_shard):
            return stored_shard
        if stored_shard.value is None:
            return Rejected(code="shard_not_found", message="Mutation shard does not exist")
        if stored_shard.value.status != MutationShardState.ORPHANED:
            return Rejected(
                code="shard_not_orphaned",
                message="Only an orphaned shard can be reassigned to a replacement PID",
            )
        previous_projection = stored_shard.value.lease
        if previous_projection is None:
            return Rejected(
                code="orphan_evidence_missing",
                message="Reassignment requires the previous orphaned lease projection",
            )
        previous_lease = self.store.get_lease(previous_projection.lease_id)
        if not is_success(previous_lease):
            return previous_lease
        if previous_lease.value is None:
            return Rejected(
                code="orphan_lease_not_found",
                message="Reassignment requires the previous authoritative lease generation",
            )
        if (
            previous_lease.value.status != MutationLeaseState.ORPHANED
            or previous_lease.value.shard_id != shard_id
            or previous_lease.value.attempt != stored_shard.value.attempt
            or previous_lease.value.lease_id != previous_projection.lease_id
        ):
            return Rejected(
                code="orphan_lease_mismatch",
                message="Previous authoritative lease does not match orphaned shard ownership",
            )
        existing_lease = self.store.get_lease(lease_contract.lease_id)
        if not is_success(existing_lease):
            return existing_lease
        if existing_lease.value is not None:
            return Rejected(code="lease_id_conflict", message="Lease token already identifies another generation")
        lease = MutationLease.claim(stored_shard.value.campaign_id, shard_id, lease_contract)
        reassigned = stored_shard.value.reassign(
            lease.to_shard_lease(),
            expected_revision=expected_shard_revision,
        )
        if not is_success(reassigned):
            return reassigned
        return self._commit_lease_state(
            effect_id=effect_id,
            effect_type="mutation.lease.reassign",
            lease=lease,
            shard=reassigned.value,
            worker=None,
            lease_expected_revision=None,
            shard_expected_revision=stored_shard.value.revision_number,
            worker_expected_revision=None,
        )
    def renew_claimed_lease(
        self,
        effect_id: str,
        lease_id: str,
        *,
        heartbeat_at: str,
        heartbeat_sequence: int,
        expected_lease_revision: int,
        expected_shard_revision: int,
        now: str | None = None,
    ) -> Outcome[LeaseStateReceipt]:
        # Renew an unbound pre-engine claim and its shard projection in one transaction.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            return self._lease_state_from_receipt(existing_effect.value)
        current_lease = self.get_lease(lease_id)
        if not is_success(current_lease):
            return current_lease
        current_shard = self.store.get_shard(current_lease.value.shard_id)
        if not is_success(current_shard):
            return current_shard
        if current_shard.value is None:
            return Rejected(code="shard_not_found", message="Lease shard does not exist")
        renewed_lease = current_lease.value.renew_claim(
            heartbeat_at=heartbeat_at,
            heartbeat_sequence=heartbeat_sequence,
            expected_revision=expected_lease_revision,
            now=now,
        )
        if not is_success(renewed_lease):
            return renewed_lease
        renewed_shard = current_shard.value.renew_lease(
            renewed_lease.value.to_shard_lease(),
            expected_revision=expected_shard_revision,
            now=now,
        )
        if not is_success(renewed_shard):
            return renewed_shard
        return self._commit_lease_state(
            effect_id=effect_id,
            effect_type="mutation.lease.renew_claim",
            lease=renewed_lease.value,
            shard=renewed_shard.value,
            worker=None,
            lease_expected_revision=current_lease.value.revision_number,
            shard_expected_revision=current_shard.value.revision_number,
            worker_expected_revision=None,
        )
    def bind_worker_lease(
        self,
        effect_id: str,
        campaign_id: CampaignId,
        lease_id: str,
        identity: WorkerIdentity,
        *,
        expected_lease_revision: int,
        expected_shard_revision: int,
        expected_worker_revision: int,
        engine_protocol_version: int = 1,
        workspace_backend: str = "copy",
        python_version: str | None = None,
    ) -> Outcome[LeaseStateReceipt]:
        # Bind shard, lease and registered worker assignment through one compare-and-swap effect.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            return self._lease_state_from_receipt(existing_effect.value)
        current_lease = self.get_lease(lease_id)
        if not is_success(current_lease):
            return current_lease
        if current_lease.value.campaign_id != campaign_id:
            return Rejected(code="lease_campaign_mismatch", message="Lease belongs to another campaign")
        current_shard = self.store.get_shard(current_lease.value.shard_id)
        current_worker = self.get_worker(campaign_id, identity.worker_id)
        if not is_success(current_shard):
            return current_shard
        if not is_success(current_worker):
            return current_worker
        if current_shard.value is None:
            return Rejected(code="shard_not_found", message="Lease shard does not exist")
        if current_worker.value.identity != identity:
            return Rejected(code="stale_worker_instance", message="Lease binding uses another registered worker instance")
        bound_lease = current_lease.value.bind(identity, expected_revision=expected_lease_revision)
        if not is_success(bound_lease):
            return bound_lease
        bound_shard = current_shard.value.renew_lease(
            bound_lease.value.to_shard_lease(),
            expected_revision=expected_shard_revision,
        )
        if not is_success(bound_shard):
            return bound_shard
        assigned_worker = current_worker.value.assign(
            shard_id=current_lease.value.shard_id.value,
            lease_id=current_lease.value.lease_id,
            attempt=current_lease.value.attempt,
            expected_revision=expected_worker_revision,
            engine_protocol_version=engine_protocol_version,
            workspace_backend=workspace_backend,
            python_version=python_version,
        )
        if not is_success(assigned_worker):
            return assigned_worker
        return self._commit_lease_state(
            effect_id=effect_id,
            effect_type="mutation.lease.bind_worker",
            lease=bound_lease.value,
            shard=bound_shard.value,
            worker=assigned_worker.value,
            lease_expected_revision=current_lease.value.revision_number,
            shard_expected_revision=current_shard.value.revision_number,
            worker_expected_revision=current_worker.value.revision_number,
        )
    def renew_worker_lease(
        self,
        effect_id: str,
        campaign_id: CampaignId,
        heartbeat: WorkerHeartbeat,
        *,
        expected_lease_revision: int,
        expected_shard_revision: int,
        expected_worker_revision: int,
        completed_assignments: int = 0,
        now: str | None = None,
    ) -> Outcome[LeaseStateReceipt]:
        # Renew lease, shard and worker liveness as one authoritative heartbeat transaction.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            return self._lease_state_from_receipt(existing_effect.value)
        if not heartbeat.current_lease_id:
            return Rejected(code="lease_missing", message="Assigned worker heartbeat has no lease token")
        current_lease = self.get_lease(heartbeat.current_lease_id)
        if not is_success(current_lease):
            return current_lease
        current_shard = self.store.get_shard(current_lease.value.shard_id)
        current_worker = self.get_worker(campaign_id, heartbeat.worker.worker_id)
        if not is_success(current_shard):
            return current_shard
        if not is_success(current_worker):
            return current_worker
        if current_shard.value is None:
            return Rejected(code="shard_not_found", message="Lease shard does not exist")
        renewed_lease = current_lease.value.renew(
            heartbeat,
            expected_revision=expected_lease_revision,
            now=now,
        )
        if not is_success(renewed_lease):
            return renewed_lease
        renewed_shard = current_shard.value.renew_lease(
            renewed_lease.value.to_shard_lease(),
            expected_revision=expected_shard_revision,
            now=now,
        )
        if not is_success(renewed_shard):
            return renewed_shard
        renewed_worker = current_worker.value.heartbeat(
            heartbeat,
            expected_revision=expected_worker_revision,
            completed_assignments=completed_assignments,
        )
        if not is_success(renewed_worker):
            return renewed_worker
        return self._commit_lease_state(
            effect_id=effect_id,
            effect_type="mutation.lease.heartbeat",
            lease=renewed_lease.value,
            shard=renewed_shard.value,
            worker=renewed_worker.value,
            lease_expected_revision=current_lease.value.revision_number,
            shard_expected_revision=current_shard.value.revision_number,
            worker_expected_revision=current_worker.value.revision_number,
        )
    def begin_lease_delivery(
        self,
        effect_id: str,
        campaign_id: CampaignId,
        lease_id: str,
        *,
        expected_lease_revision: int,
        expected_worker_revision: int,
        now: str | None = None,
    ) -> Outcome[LeaseStateReceipt]:
        # Move lease and worker into delivering atomically before result fan-in.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            return self._lease_state_from_receipt(existing_effect.value)
        current_lease = self.get_lease(lease_id)
        if not is_success(current_lease):
            return current_lease
        current_shard = self.store.get_shard(current_lease.value.shard_id)
        current_worker = self.get_worker(campaign_id, current_lease.value.worker_id.value)
        if not is_success(current_shard):
            return current_shard
        if not is_success(current_worker):
            return current_worker
        if current_shard.value is None:
            return Rejected(code="shard_not_found", message="Lease shard does not exist")
        delivering_lease = current_lease.value.begin_delivery(
            expected_revision=expected_lease_revision
        )
        if not is_success(delivering_lease):
            return delivering_lease
        delivery_started_at = now or utc_now()
        if (
            current_lease.value.status == MutationLeaseState.RUNNING
            and current_lease.value.expired(delivery_started_at)
        ):
            return Rejected(
                code="lease_expired_before_delivery",
                message="Authoritative running lease expired before the durable delivery transition",
                details={
                    "campaign_id": campaign_id.value,
                    "shard_id": current_lease.value.shard_id.value,
                    "lease_id": current_lease.value.lease_id,
                    "attempt": current_lease.value.attempt,
                    "lease_status": getattr(
                        current_lease.value.status,
                        "value",
                        current_lease.value.status,
                    ),
                    "expires_at": current_lease.value.expires_at,
                    "delivery_started_at": delivery_started_at,
                },
            )
        delivering_worker = current_worker.value.mark_delivering(expected_revision=expected_worker_revision)
        if not is_success(delivering_worker):
            return delivering_worker
        return self._commit_lease_state(
            effect_id=effect_id,
            effect_type="mutation.lease.begin_delivery",
            lease=delivering_lease.value,
            shard=current_shard.value,
            worker=delivering_worker.value,
            lease_expected_revision=current_lease.value.revision_number,
            shard_expected_revision=None,
            worker_expected_revision=current_worker.value.revision_number,
        )
    def release_lease_assignment(
        self,
        effect_id: str,
        campaign_id: CampaignId,
        lease_id: str,
        *,
        expected_lease_revision: int,
        expected_worker_revision: int,
    ) -> Outcome[LeaseStateReceipt]:
        # Release lease and worker assignment together after durable delivery acknowledgement.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            return self._lease_state_from_receipt(existing_effect.value)
        current_lease = self.get_lease(lease_id)
        if not is_success(current_lease):
            return current_lease
        current_shard = self.store.get_shard(current_lease.value.shard_id)
        current_worker = self.get_worker(campaign_id, current_lease.value.worker_id.value)
        if not is_success(current_shard):
            return current_shard
        if not is_success(current_worker):
            return current_worker
        if current_shard.value is None:
            return Rejected(code="shard_not_found", message="Lease shard does not exist")
        released_lease = current_lease.value.release(expected_revision=expected_lease_revision)
        if not is_success(released_lease):
            return released_lease
        released_worker = current_worker.value.release(expected_revision=expected_worker_revision)
        if not is_success(released_worker):
            return released_worker
        closed_shard = current_shard.value.close_lease(
            released_lease.value.to_shard_lease(),
            expected_revision=current_shard.value.revision_number,
        )
        if not is_success(closed_shard):
            return closed_shard
        return self._commit_lease_state(
            effect_id=effect_id,
            effect_type="mutation.lease.release",
            lease=released_lease.value,
            shard=closed_shard.value,
            worker=released_worker.value,
            lease_expected_revision=current_lease.value.revision_number,
            shard_expected_revision=current_shard.value.revision_number,
            worker_expected_revision=current_worker.value.revision_number,
        )
    def fail_lease_assignment(
        self,
        effect_id: str,
        campaign_id: CampaignId,
        lease_id: str,
        *,
        expected_lease_revision: int,
        expected_worker_revision: int | None,
    ) -> Outcome[LeaseStateReceipt]:
        # Fail the lease and any registered worker together after durable infrastructure fan-in.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            return self._lease_state_from_receipt(existing_effect.value)
        current_lease = self.get_lease(lease_id)
        if not is_success(current_lease):
            return current_lease
        current_shard = self.store.get_shard(current_lease.value.shard_id)
        current_worker = self.store.get_worker(campaign_id, current_lease.value.worker_id.value)
        if not is_success(current_shard):
            return current_shard
        if not is_success(current_worker):
            return current_worker
        if current_shard.value is None:
            return Rejected(code="shard_not_found", message="Lease shard does not exist")
        failed_lease = current_lease.value.terminate(
            MutationLeaseState.FAILED,
            expected_revision=expected_lease_revision,
        )
        if not is_success(failed_lease):
            return failed_lease
        failed_worker = None
        worker_expected = None
        if current_worker.value is not None:
            if expected_worker_revision is None:
                return Rejected(code="worker_revision_required", message="Atomic lease failure requires worker revision")
            worker_outcome = current_worker.value.fail(expected_revision=expected_worker_revision)
            if not is_success(worker_outcome):
                return worker_outcome
            failed_worker = worker_outcome.value
            worker_expected = current_worker.value.revision_number
        failed_shard = current_shard.value.close_lease(
            failed_lease.value.to_shard_lease(),
            expected_revision=current_shard.value.revision_number,
        )
        if not is_success(failed_shard):
            return failed_shard
        return self._commit_lease_state(
            effect_id=effect_id,
            effect_type="mutation.lease.fail",
            lease=failed_lease.value,
            shard=failed_shard.value,
            worker=failed_worker,
            lease_expected_revision=current_lease.value.revision_number,
            shard_expected_revision=current_shard.value.revision_number,
            worker_expected_revision=worker_expected,
        )
    def orphan_lease_assignment(
        self,
        effect_id: str,
        lease_id: str,
        *,
        expected_lease_revision: int,
        expected_shard_revision: int,
        expected_worker_revision: int | None = None,
        now: str | None = None,
        require_expired: bool = True,
    ) -> Outcome[LeaseStateReceipt]:
        # Orphan lease, shard and any registered worker owner in one recovery transaction.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            return self._lease_state_from_receipt(existing_effect.value)
        current_lease = self.get_lease(lease_id)
        if not is_success(current_lease):
            return current_lease
        if require_expired and not current_lease.value.expired(now or utc_now()):
            return Rejected(code="lease_not_expired", message="Authoritative lease is still alive")
        current_shard = self.store.get_shard(current_lease.value.shard_id)
        current_worker = self.store.get_worker(current_lease.value.campaign_id, current_lease.value.worker_id.value)
        if not is_success(current_shard):
            return current_shard
        if not is_success(current_worker):
            return current_worker
        if current_shard.value is None:
            return Rejected(code="shard_not_found", message="Lease shard does not exist")
        orphaned_lease = current_lease.value.terminate(
            MutationLeaseState.ORPHANED,
            expected_revision=expected_lease_revision,
        )
        if not is_success(orphaned_lease):
            return orphaned_lease
        if current_shard.value.status in {MutationShardState.LEASED, MutationShardState.RUNNING}:
            orphaned_shard = current_shard.value.orphan(expected_revision=expected_shard_revision)
        else:
            orphaned_shard = current_shard.value.close_lease(
                orphaned_lease.value.to_shard_lease(),
                expected_revision=expected_shard_revision,
            )
        if not is_success(orphaned_shard):
            return orphaned_shard
        orphaned_worker = None
        worker_expected = None
        if current_worker.value is not None:
            if expected_worker_revision is None:
                return Rejected(code="worker_revision_required", message="Atomic orphaning requires worker revision")
            worker_outcome = current_worker.value.orphan(expected_revision=expected_worker_revision)
            if not is_success(worker_outcome):
                return worker_outcome
            orphaned_worker = worker_outcome.value
            worker_expected = current_worker.value.revision_number
        return self._commit_lease_state(
            effect_id=effect_id,
            effect_type="mutation.lease.orphan",
            lease=orphaned_lease.value,
            shard=orphaned_shard.value,
            worker=orphaned_worker,
            lease_expected_revision=current_lease.value.revision_number,
            shard_expected_revision=current_shard.value.revision_number,
            worker_expected_revision=worker_expected,
        )
    def _worker_from_receipt(self, receipt: EffectReceipt) -> Outcome[MutationWorker]:
        # Rehydrate the exact prior worker projection for idempotent effect replay.
        raw_worker = receipt.payload.get("worker")
        if not isinstance(raw_worker, Mapping):
            return Failed("effect_receipt_corrupted", "Effect receipt has no worker projection")
        try:
            return Success(MutationWorker.from_dict(raw_worker), duplicate=True)
        except (TypeError, ValueError) as exc:
            return Failed("effect_receipt_corrupted", f"Cannot decode worker receipt: {exc}")
    def _commit_worker_effect(
        self,
        *,
        effect_id: str,
        effect_type: str,
        worker: MutationWorker,
        expected_revision: int | None,
    ) -> Outcome[MutationWorker]:
        # Persist one worker transition and its exact idempotency receipt atomically.
        receipt = EffectReceipt(
            effect_id=effect_id,
            effect_type=effect_type,
            campaign_id=worker.campaign_id,
            payload={"worker": worker.to_dict()},
        )
        committed = self._commit_effect(
            receipt,
            worker=worker,
            worker_expected_revision=expected_revision,
        )
        if not is_success(committed):
            return committed
        if committed.duplicate:
            return self._worker_from_receipt(committed.value)
        return Success(worker)
    def register_worker(
        self,
        effect_id: str,
        campaign_id: CampaignId,
        identity: WorkerIdentity,
        capabilities: WorkerCapabilities,
        *,
        workspace: str,
        spool_path: str,
        launcher_process_id: int | None = None,
        launcher_process_birth_token: str | None = None,
    ) -> Outcome[MutationWorker]:
        # Accept registration only after active-instance fencing and durable compare-and-swap validation.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            return self._worker_from_receipt(existing_effect.value)
        current = self.store.get_worker(campaign_id, identity.worker_id)
        if not is_success(current):
            return current
        if current.value is None:
            candidate = MutationWorker.register(
                campaign_id,
                identity,
                capabilities,
                workspace=workspace,
                spool_path=spool_path,
                launcher_process_id=launcher_process_id,
                launcher_process_birth_token=launcher_process_birth_token,
            )
            expected_revision = None
        else:
            replaced = current.value.replace_registration(
                identity,
                capabilities,
                workspace=workspace,
                spool_path=spool_path,
                launcher_process_id=launcher_process_id,
                launcher_process_birth_token=launcher_process_birth_token,
                expected_revision=current.value.revision_number,
            )
            if not is_success(replaced):
                return replaced
            candidate = replaced.value
            expected_revision = current.value.revision_number if not replaced.duplicate else None
            if replaced.duplicate:
                receipt = EffectReceipt(
                    effect_id=effect_id,
                    effect_type="mutation.worker.register",
                    campaign_id=campaign_id,
                    payload={"worker": candidate.to_dict()},
                )
                recorded = self._commit_effect(receipt)
                if not is_success(recorded):
                    return recorded
                if recorded.duplicate:
                    return self._worker_from_receipt(recorded.value)
                return Success(candidate, duplicate=True)
        return self._commit_worker_effect(
            effect_id=effect_id,
            effect_type="mutation.worker.register",
            worker=candidate,
            expected_revision=expected_revision,
        )
    def heartbeat_worker(
        self,
        effect_id: str,
        campaign_id: CampaignId,
        heartbeat: WorkerHeartbeat,
        *,
        expected_revision: int,
        completed_assignments: int = 0,
    ) -> Outcome[MutationWorker]:
        # Fence stale instances and advance authoritative liveness through one idempotent effect.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            return self._worker_from_receipt(existing_effect.value)
        current = self.get_worker(campaign_id, heartbeat.worker.worker_id)
        if not is_success(current):
            return current
        updated = current.value.heartbeat(
            heartbeat,
            expected_revision=expected_revision,
            completed_assignments=completed_assignments,
        )
        if not is_success(updated):
            return updated
        return self._commit_worker_effect(
            effect_id=effect_id,
            effect_type="mutation.worker.heartbeat",
            worker=updated.value,
            expected_revision=current.value.revision_number,
        )
    def assign_worker(
        self,
        effect_id: str,
        campaign_id: CampaignId,
        worker_id: str,
        *,
        shard_id: str,
        lease_id: str,
        attempt: int,
        expected_revision: int,
        engine_protocol_version: int = 1,
        workspace_backend: str = "copy",
        python_version: str | None = None,
    ) -> Outcome[MutationWorker]:
        # Bind one idle capable worker to one active shard lease.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            return self._worker_from_receipt(existing_effect.value)
        current = self.get_worker(campaign_id, worker_id)
        if not is_success(current):
            return current
        assigned = current.value.assign(
            shard_id=shard_id,
            lease_id=lease_id,
            attempt=attempt,
            expected_revision=expected_revision,
            engine_protocol_version=engine_protocol_version,
            workspace_backend=workspace_backend,
            python_version=python_version,
        )
        if not is_success(assigned):
            return assigned
        return self._commit_worker_effect(
            effect_id=effect_id,
            effect_type="mutation.worker.assign",
            worker=assigned.value,
            expected_revision=current.value.revision_number,
        )
    def _transition_worker(
        self,
        effect_id: str,
        campaign_id: CampaignId,
        worker_id: str,
        *,
        expected_revision: int,
        effect_type: str,
        transition: str,
    ) -> Outcome[MutationWorker]:
        # Apply one named worker lifecycle transition through the same durable effect boundary.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            return self._worker_from_receipt(existing_effect.value)
        current = self.get_worker(campaign_id, worker_id)
        if not is_success(current):
            return current
        outcome = getattr(current.value, transition)(expected_revision=expected_revision)
        if not is_success(outcome):
            return outcome
        return self._commit_worker_effect(
            effect_id=effect_id,
            effect_type=effect_type,
            worker=outcome.value,
            expected_revision=current.value.revision_number,
        )
    def mark_worker_delivering(self, effect_id: str, campaign_id: CampaignId, worker_id: str, *, expected_revision: int) -> Outcome[MutationWorker]:
        # Mark durable delivery before authoritative result fan-in.
        return self._transition_worker(effect_id, campaign_id, worker_id, expected_revision=expected_revision, effect_type="mutation.worker.delivering", transition="mark_delivering")
    def release_worker(self, effect_id: str, campaign_id: CampaignId, worker_id: str, *, expected_revision: int) -> Outcome[MutationWorker]:
        # Return a committed worker assignment to the idle pool.
        return self._transition_worker(effect_id, campaign_id, worker_id, expected_revision=expected_revision, effect_type="mutation.worker.release", transition="release")
    def drain_worker(self, effect_id: str, campaign_id: CampaignId, worker_id: str, *, expected_revision: int) -> Outcome[MutationWorker]:
        # Prevent additional assignments while preserving current ownership.
        return self._transition_worker(effect_id, campaign_id, worker_id, expected_revision=expected_revision, effect_type="mutation.worker.drain", transition="drain")
    def stop_worker(self, effect_id: str, campaign_id: CampaignId, worker_id: str, *, expected_revision: int) -> Outcome[MutationWorker]:
        # Persist clean worker termination.
        return self._transition_worker(effect_id, campaign_id, worker_id, expected_revision=expected_revision, effect_type="mutation.worker.stop", transition="stop")
    def fail_worker(self, effect_id: str, campaign_id: CampaignId, worker_id: str, *, expected_revision: int) -> Outcome[MutationWorker]:
        # Fence a broken worker instance permanently.
        return self._transition_worker(effect_id, campaign_id, worker_id, expected_revision=expected_revision, effect_type="mutation.worker.fail", transition="fail")
    def orphan_worker(self, effect_id: str, campaign_id: CampaignId, worker_id: str, *, expected_revision: int) -> Outcome[MutationWorker]:
        # Mark a vanished process instance orphaned for restart reconciliation.
        return self._transition_worker(effect_id, campaign_id, worker_id, expected_revision=expected_revision, effect_type="mutation.worker.orphan", transition="orphan")
    def _campaign(self, campaign_id: CampaignId) -> Outcome[MutationCampaign]:
        # Resolve a required campaign and turn absence into a stable domain rejection.
        result = self.store.get_campaign(campaign_id)
        if not is_success(result):
            return result
        if result.value is None:
            return Rejected(
                code="campaign_not_found",
                message="Mutation campaign does not exist",
                details={"campaign_id": campaign_id.value},
            )
        return Success(result.value)
    def _campaign_from_receipt(self, receipt: EffectReceipt) -> Outcome[MutationCampaign]:
        # Rehydrate the exact prior campaign projection for a duplicate effect call.
        raw_campaign = receipt.payload.get("campaign")
        if not isinstance(raw_campaign, Mapping):
            return Failed("effect_receipt_corrupted", "Effect receipt has no campaign projection")
        try:
            return Success(MutationCampaign.from_dict(raw_campaign), duplicate=True)
        except (TypeError, ValueError) as exc:
            return Failed("effect_receipt_corrupted", f"Cannot decode campaign receipt: {exc}")
    def _finalization_intent_from_receipt(self, receipt: EffectReceipt) -> Outcome[FinalizationIntent]:
        # Rehydrate the exact prior finalization intent for idempotent stage replay.
        raw_intent = receipt.payload.get("finalization_intent")
        if not isinstance(raw_intent, Mapping):
            return Failed("effect_receipt_corrupted", "Finalization effect receipt has no intent projection")
        try:
            return Success(FinalizationIntent.from_dict(raw_intent), duplicate=True)
        except (TypeError, ValueError) as exc:
            return Failed("effect_receipt_corrupted", f"Cannot decode finalization intent receipt: {exc}")
    def _commit_effect(
        self,
        receipt: EffectReceipt,
        *,
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
        # Route every control-plane write through the repository's atomic effect boundary.
        return self.store.commit_effect(
            receipt=receipt,
            campaign=campaign,
            campaign_expected_revision=campaign_expected_revision,
            shard=shard,
            shard_expected_revision=shard_expected_revision,
            lease=lease,
            lease_expected_revision=lease_expected_revision,
            worker=worker,
            worker_expected_revision=worker_expected_revision,
            executions=executions,
        )
    def _transition(
        self,
        effect_id: str,
        campaign_id: CampaignId,
        target: CampaignState,
        *,
        expected_revision: int,
        effect_type: str,
    ) -> Outcome[MutationCampaign]:
        # Apply one named state effect and persist its exact result for replay.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            return self._campaign_from_receipt(existing_effect.value)
        current = self._campaign(campaign_id)
        if not is_success(current):
            return current
        transitioned = current.value.transition(target, expected_revision=expected_revision)
        if not is_success(transitioned):
            return transitioned
        candidate = None if transitioned.duplicate else transitioned.value
        receipt_campaign = transitioned.value
        receipt = EffectReceipt(
            effect_id=effect_id,
            effect_type=effect_type,
            campaign_id=campaign_id,
            payload={"campaign": receipt_campaign.to_dict()},
        )
        recorded = self._commit_effect(
            receipt,
            campaign=candidate,
            campaign_expected_revision=current.value.revision_number if candidate is not None else None,
        )
        if not is_success(recorded):
            return recorded
        if recorded.duplicate:
            return self._campaign_from_receipt(recorded.value)
        return Success(receipt_campaign, duplicate=transitioned.duplicate)
    def apply_transition(
        self,
        effect_id: str,
        campaign_id: CampaignId,
        target: CampaignState,
        *,
        expected_revision: int,
    ) -> Outcome[MutationCampaign]:
        # Provide one generic adapter for future effect runners and recovery tools.
        return self._transition(
            effect_id,
            campaign_id,
            target,
            expected_revision=expected_revision,
            effect_type=f"mutation.{target.value}",
        )
    def prepare(self, effect_id: str, campaign_id: CampaignId, *, expected_revision: int) -> Outcome[MutationCampaign]:
        # Enter preparation through the named mutation.prepare effect.
        return self._transition(effect_id, campaign_id, CampaignState.PREPARING, expected_revision=expected_revision, effect_type="mutation.prepare")
    def collect(self, effect_id: str, campaign_id: CampaignId, *, expected_revision: int) -> Outcome[MutationCampaign]:
        # Enter authoritative pytest collection through mutation.collect.
        return self._transition(effect_id, campaign_id, CampaignState.COLLECTING, expected_revision=expected_revision, effect_type="mutation.collect")
    def index(self, effect_id: str, campaign_id: CampaignId, *, expected_revision: int) -> Outcome[MutationCampaign]:
        # Enter immutable index construction through mutation.index.
        return self._transition(effect_id, campaign_id, CampaignState.INDEXING, expected_revision=expected_revision, effect_type="mutation.index")
    def baseline(self, effect_id: str, campaign_id: CampaignId, *, expected_revision: int) -> Outcome[MutationCampaign]:
        # Enter baseline escalation through mutation.baseline.
        return self._transition(effect_id, campaign_id, CampaignState.BASELINING, expected_revision=expected_revision, effect_type="mutation.baseline")
    def discover(self, effect_id: str, campaign_id: CampaignId, *, expected_revision: int) -> Outcome[MutationCampaign]:
        # Enter deterministic mutant discovery through mutation.discover.
        return self._transition(effect_id, campaign_id, CampaignState.DISCOVERING, expected_revision=expected_revision, effect_type="mutation.discover")
    def plan(self, effect_id: str, campaign_id: CampaignId, *, expected_revision: int) -> Outcome[MutationCampaign]:
        # Enter frozen selection and shard planning through mutation.plan.
        return self._transition(effect_id, campaign_id, CampaignState.PLANNING, expected_revision=expected_revision, effect_type="mutation.plan")
    def commit_plan_topology(
        self,
        effect_id: str,
        campaign_id: CampaignId,
        *,
        plan_id: str,
        prepared_snapshot_id: str,
        selected_count: int,
        shard_descriptors: tuple[ShardDescriptor, ...],
        expected_revision: int,
        plan_decisions: tuple[PlanDecision, ...] = (),
        plan_artifact_sha256: str = "",
    ) -> Outcome[MutationCampaign]:
        # Commit the campaign plan binding and every immutable shard row in one replay-safe transaction.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            raw_campaign = existing_effect.value.payload.get("campaign")
            raw_shards = existing_effect.value.payload.get("shards")
            raw_decisions = existing_effect.value.payload.get("plan_decisions", ())
            if (
                not isinstance(raw_campaign, Mapping)
                or not isinstance(raw_shards, (list, tuple))
                or not isinstance(raw_decisions, (list, tuple))
            ):
                return Failed("effect_receipt_corrupted", "Plan topology effect receipt is incomplete")
            try:
                prior_campaign = MutationCampaign.from_dict(raw_campaign)
                prior_shards = tuple(MutationShard.from_dict(item) for item in raw_shards)
                prior_decisions = tuple(PlanDecision.from_dict(item) for item in raw_decisions)
                requested_shards = tuple(
                    MutationShard.from_descriptor(campaign_id, descriptor, ordinal=ordinal)
                    for ordinal, descriptor in enumerate(shard_descriptors)
                )
            except (TypeError, ValueError) as exc:
                return Failed("effect_receipt_corrupted", f"Cannot decode plan topology receipt: {exc}")
            if (
                existing_effect.value.campaign_id != campaign_id
                or prior_campaign.plan_id != str(plan_id).strip()
                or prior_campaign.prepared_snapshot_id != str(prepared_snapshot_id).strip()
                or prior_campaign.total_mutants != selected_count
                or prior_shards != requested_shards
                or prior_decisions != plan_decisions
                or str(existing_effect.value.payload.get("plan_artifact_sha256", ""))
                != str(plan_artifact_sha256).strip()
            ):
                return Rejected(
                    code="effect_id_conflict",
                    message="Plan topology effect ID is already bound to another immutable payload",
                )
            return Success(prior_campaign, message="Campaign plan topology already committed", duplicate=True)
        current = self._campaign(campaign_id)
        if not is_success(current):
            return current
        normalized_plan_id = str(plan_id).strip()
        normalized_snapshot_id = str(prepared_snapshot_id).strip()
        normalized_artifact_sha256 = str(plan_artifact_sha256).strip()
        decision_ids = tuple(item.mutant_id for item in plan_decisions)
        if len(decision_ids) != len(set(decision_ids)):
            return Rejected(
                code="plan_decision_conflict",
                message="Plan decision ledger contains duplicate mutant identities",
            )
        if any(item.plan_id != normalized_plan_id for item in shard_descriptors):
            return Rejected(
                code="plan_topology_conflict",
                message="Every shard descriptor must be bound to the committed plan",
            )
        shard_ids = tuple(item.shard_id.value for item in shard_descriptors)
        mutant_ids = tuple(mutant_id for item in shard_descriptors for mutant_id in item.mutant_ids)
        if len(shard_ids) != len(set(shard_ids)) or len(mutant_ids) != len(set(mutant_ids)):
            return Rejected(
                code="plan_topology_conflict",
                message="Plan topology contains duplicate shard or mutant identities",
            )
        if plan_decisions:
            selected_actions = {"audit", "execute", "partial_reuse", "reuse"}
            selected_decision_ids = {
                item.mutant_id for item in plan_decisions if item.action in selected_actions
            }
            if selected_decision_ids != set(mutant_ids):
                return Rejected(
                    code="plan_decision_conflict",
                    message="Plan decision ledger does not match immutable shard membership",
                )
        if len(mutant_ids) != selected_count:
            return Rejected(
                code="plan_topology_conflict",
                message="Plan topology membership does not match selected mutant count",
                details={"selected_count": selected_count, "topology_count": len(mutant_ids)},
            )
        campaign_candidate: MutationCampaign | None = None
        authority_campaign = current.value
        if current.value.status == CampaignState.DISCOVERING:
            committed = current.value.commit_plan(
                normalized_plan_id,
                normalized_snapshot_id,
                selected_count,
                expected_revision=expected_revision,
            )
            if not is_success(committed):
                return committed
            authority_campaign = committed.value
            campaign_candidate = committed.value
        elif current.value.status in {CampaignState.PLANNING, CampaignState.RUNNING}:
            if expected_revision != current.value.revision_number:
                return Rejected(
                    code="stale_revision",
                    message="campaign changed since it was loaded",
                    details={
                        "entity": "campaign",
                        "expected_revision": expected_revision,
                        "actual_revision": current.value.revision_number,
                    },
                )
            if (
                current.value.plan_id != normalized_plan_id
                or current.value.prepared_snapshot_id != normalized_snapshot_id
                or current.value.total_mutants != selected_count
            ):
                return Rejected(
                    code="campaign_plan_conflict",
                    message="Existing campaign projection conflicts with immutable plan topology",
                )
        else:
            return Rejected(
                code="plan_out_of_order",
                message="Plan topology can be committed only before or during execution",
                details={"status": current.value.status.value},
            )
        shards = tuple(
            MutationShard.from_descriptor(campaign_id, descriptor, ordinal=ordinal)
            for ordinal, descriptor in enumerate(shard_descriptors)
        )
        receipt = EffectReceipt(
            effect_id=effect_id,
            effect_type="mutation.plan_topology",
            campaign_id=campaign_id,
            payload={
                "campaign": authority_campaign.to_dict(),
                "shards": [item.to_dict() for item in shards],
                "plan_id": normalized_plan_id,
                "plan_artifact_sha256": normalized_artifact_sha256,
                "plan_decisions": [item.to_dict() for item in plan_decisions],
            },
        )
        committed = self.store.commit_plan_topology(
            receipt=receipt,
            campaign=campaign_candidate,
            campaign_expected_revision=(
                current.value.revision_number if campaign_candidate is not None else None
            ),
            shards=shards,
        )
        if not is_success(committed):
            return committed
        if committed.duplicate:
            return self._campaign_from_receipt(committed.value)
        return Success(authority_campaign)
    def start(self, effect_id: str, campaign_id: CampaignId, *, expected_revision: int) -> Outcome[MutationCampaign]:
        # Start shard execution through the mutation coordinator boundary.
        return self._transition(effect_id, campaign_id, CampaignState.RUNNING, expected_revision=expected_revision, effect_type="mutation.execute")
    def aggregate(self, effect_id: str, campaign_id: CampaignId, *, expected_revision: int) -> Outcome[MutationCampaign]:
        # Enter deterministic fan-in after all available shard results are recorded.
        completion = self.validate_campaign_completion(campaign_id)
        if not is_success(completion):
            return completion
        return self._transition(effect_id, campaign_id, CampaignState.AGGREGATING, expected_revision=expected_revision, effect_type="mutation.aggregate")
    def validate_campaign_completion(self, campaign_id: CampaignId) -> Outcome[None]:
        # Prove that every immutable planned mutant has one terminal current-attempt execution before aggregation.
        campaign = self._campaign(campaign_id)
        if not is_success(campaign):
            return campaign
        shards = self.store.list_shards(campaign_id)
        if not is_success(shards):
            return shards
        executions = self.store.list_executions(campaign_id)
        if not is_success(executions):
            return executions
        planned: set[str] = set()
        for shard in shards.value:
            shard_mutants = {item.value for item in shard.mutant_ids}
            if planned.intersection(shard_mutants):
                return Rejected(
                    code="duplicate_planned_mutant",
                    message="Immutable campaign plan assigns one mutant to multiple shards",
                )
            planned.update(shard_mutants)
            if shard.status != MutationShardState.COMPLETE:
                return Rejected(
                    code="incomplete_shard",
                    message="Campaign cannot aggregate while a shard is not complete",
                    details={"shard_id": shard.shard_id.value, "status": shard.status.value},
                )
            if shard.completed_count != len(shard_mutants):
                return Rejected(
                    code="shard_progress_mismatch",
                    message="Completed shard count does not match immutable membership",
                    details={"shard_id": shard.shard_id.value},
                )
        if len(planned) != campaign.value.total_mutants:
            return Rejected(
                code="campaign_plan_mismatch",
                message="Shard membership does not match the discovered mutant plan",
                details={"planned": len(planned), "total_mutants": campaign.value.total_mutants},
            )
        current_attempts: dict[str, set[str]] = {item: set() for item in planned}
        execution_ids: set[str] = set()
        shard_by_id = {shard.shard_id.value: shard for shard in shards.value}
        for execution in executions.value:
            execution_key = execution.execution_id.value
            if execution_key in execution_ids:
                return Rejected(code="duplicate_execution_id", message="Execution identity is not globally unique")
            execution_ids.add(execution_key)
            shard = shard_by_id.get(execution.shard_id.value)
            if shard is None or execution.mutant_id.value not in planned:
                return Rejected(code="foreign_execution", message="Execution is outside the immutable campaign plan")
            if execution.mutant_id.value not in {item.value for item in shard.mutant_ids}:
                return Rejected(
                    code="foreign_execution",
                    message="Execution is assigned to a different shard than its immutable membership",
                    details={"execution_id": execution.execution_id.value, "shard_id": shard.shard_id.value},
                )
            if execution.attempt != shard.attempt:
                continue
            if execution.status != MutationExecutionState.COMPLETE:
                return Rejected(
                    code="non_terminal_execution",
                    message="Current shard attempt has no terminal execution evidence",
                    details={"execution_id": execution.execution_id.value},
                )
            current_attempts.setdefault(execution.mutant_id.value, set()).add(execution.execution_id.value)
        missing = sorted(mutant_id for mutant_id, ids in current_attempts.items() if len(ids) != 1)
        if missing:
            return Rejected(
                code="missing_authoritative_execution",
                message="Every planned mutant requires exactly one current-attempt execution",
                details={"mutant_ids": ",".join(missing)},
            )
        return Success(None)
    def materialize(self, effect_id: str, campaign_id: CampaignId, *, expected_revision: int) -> Outcome[MutationCampaign]:
        # Enter report and artifact materialization without committing user code.
        return self._transition(effect_id, campaign_id, CampaignState.MATERIALIZING, expected_revision=expected_revision, effect_type="mutation.materialize")
    def complete_stage(
        self,
        effect_id: str,
        campaign_id: CampaignId,
        target: CampaignState,
        *,
        expected_revision: int,
        collection_snapshot_id: str | None = None,
        index_snapshot_id: str | None = None,
        baseline_snapshot_id: str | None = None,
        plan_id: str | None = None,
        prepared_snapshot_id: str | None = None,
    ) -> Outcome[MutationCampaign]:
        # Persist a staged transition and its immutable artifact references as one effect commit.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            return self._campaign_from_receipt(existing_effect.value)
        current = self._campaign(campaign_id)
        if not is_success(current):
            return current
        updated = current.value.complete_stage(
            target,
            expected_revision=expected_revision,
            collection_snapshot_id=collection_snapshot_id,
            index_snapshot_id=index_snapshot_id,
            baseline_snapshot_id=baseline_snapshot_id,
            plan_id=plan_id,
            prepared_snapshot_id=prepared_snapshot_id,
        )
        if not is_success(updated):
            return updated
        receipt = EffectReceipt(
            effect_id=effect_id,
            effect_type=f"mutation.stage.{target.value}",
            campaign_id=campaign_id,
            payload={"campaign": updated.value.to_dict()},
        )
        recorded = self._commit_effect(
            receipt,
            campaign=None if updated.duplicate else updated.value,
            campaign_expected_revision=current.value.revision_number if not updated.duplicate else None,
        )
        if not is_success(recorded):
            return recorded
        if recorded.duplicate:
            return self._campaign_from_receipt(recorded.value)
        return Success(updated.value, duplicate=updated.duplicate)
    def record_discovery(
        self,
        effect_id: str,
        campaign_id: CampaignId,
        total_mutants: int,
        *,
        expected_revision: int,
        plan_id: str | None = None,
    ) -> Outcome[MutationCampaign]:
        # Persist deterministic discovery cardinality as an idempotent mutation.discover effect.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            return self._campaign_from_receipt(existing_effect.value)
        current = self._campaign(campaign_id)
        if not is_success(current):
            return current
        updated = current.value.record_discovery(
            total_mutants,
            expected_revision=expected_revision,
            plan_id=plan_id,
        )
        if not is_success(updated):
            return updated
        if updated.duplicate:
            receipt_campaign = updated.value
        else:
            receipt_campaign = updated.value
        receipt = EffectReceipt(
            effect_id=effect_id,
            effect_type="mutation.discover",
            campaign_id=campaign_id,
            payload={"campaign": receipt_campaign.to_dict()},
        )
        recorded = self._commit_effect(
            receipt,
            campaign=None if updated.duplicate else receipt_campaign,
            campaign_expected_revision=current.value.revision_number if not updated.duplicate else None,
        )
        if not is_success(recorded):
            return recorded
        if recorded.duplicate:
            return self._campaign_from_receipt(recorded.value)
        return Success(receipt_campaign, duplicate=updated.duplicate)
    def create_finalization_intent(
        self,
        effect_id: str,
        campaign_id: CampaignId,
        intent: FinalizationIntent,
    ) -> Outcome[FinalizationIntent]:
        # Persist the exact expected artifact set before any canonical publication starts.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            return self._finalization_intent_from_receipt(existing_effect.value)
        current = self._campaign(campaign_id)
        if not is_success(current):
            return current
        if current.value.status != CampaignState.MATERIALIZING:
            return Rejected(
                code="finalization_intent_out_of_order",
                message="Finalization intent requires a materializing campaign",
                details={"status": current.value.status.value},
            )
        if intent.campaign_id != campaign_id or intent.status != "created":
            return Rejected(
                code="invalid_finalization_intent",
                message="Finalization intent campaign or initial status is invalid",
                details={
                    "campaign_id": campaign_id.value,
                    "intent_campaign_id": intent.campaign_id.value,
                    "intent_status": intent.status,
                },
            )
        receipt = EffectReceipt(
            effect_id=effect_id,
            effect_type="mutation.finalization_intent",
            campaign_id=campaign_id,
            payload={"finalization_intent": intent.to_dict()},
        )
        return self.store.commit_finalization(receipt=receipt, intent=intent)
    def register_finalization(
        self,
        effect_id: str,
        campaign_id: CampaignId,
        intent: FinalizationIntent,
        artifacts: tuple[ArtifactRegistryEntry, ...],
        *,
        expected_revision: int,
    ) -> Outcome[MutationCampaign]:
        # Atomically register immutable content identities and move the campaign to ready_to_commit.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            return self._campaign_from_receipt(existing_effect.value)
        current = self._campaign(campaign_id)
        if not is_success(current):
            return current
        if current.value.status != CampaignState.MATERIALIZING:
            return Rejected(
                code="artifact_registration_out_of_order",
                message="Artifact registry commit requires a materializing campaign",
                details={"status": current.value.status.value},
            )
        stored_intent = self.store.get_finalization_intent(campaign_id)
        if not is_success(stored_intent):
            return stored_intent
        if stored_intent.value is None or stored_intent.value.intent_id != intent.intent_id:
            return Rejected(
                code="finalization_intent_missing",
                message="Artifact registration requires the matching durable finalization intent",
                details={"campaign_id": campaign_id.value, "intent_id": intent.intent_id},
            )
        expected_by_key = {item.logical_key: item for item in stored_intent.value.artifacts}
        actual_by_key = {item.logical_key: item for item in artifacts}
        artifact_key_counts = Counter(item.logical_key for item in artifacts)
        duplicate_keys = tuple(
            sorted(key for key, count in artifact_key_counts.items() if count > 1)
        )
        if duplicate_keys or expected_by_key != actual_by_key:
            missing = tuple(sorted(set(expected_by_key) - set(actual_by_key)))
            unexpected = tuple(sorted(set(actual_by_key) - set(expected_by_key)))
            conflicting = tuple(
                sorted(
                    key
                    for key in set(expected_by_key) & set(actual_by_key)
                    if expected_by_key[key] != actual_by_key[key]
                )
            )
            return Rejected(
                code="artifact_set_mismatch",
                message="Published artifact set does not match the durable finalization intent",
                details={
                    "duplicate": ",".join(duplicate_keys),
                    "missing": ",".join(missing),
                    "unexpected": ",".join(unexpected),
                    "conflicting": ",".join(conflicting),
                },
            )
        ready = current.value.transition(
            CampaignState.READY_TO_COMMIT,
            expected_revision=expected_revision,
        )
        if not is_success(ready):
            return ready
        registered_intent = replace(
            stored_intent.value,
            status="registered",
            updated_at=utc_now(),
        )
        receipt = EffectReceipt(
            effect_id=effect_id,
            effect_type="mutation.artifact_registry_committed",
            campaign_id=campaign_id,
            payload={
                "campaign": ready.value.to_dict(),
                "finalization_intent": registered_intent.to_dict(),
                "artifacts": [item.to_dict() for item in artifacts],
            },
        )
        committed = self.store.commit_finalization(
            receipt=receipt,
            intent=registered_intent,
            artifacts=artifacts,
            campaign=ready.value,
            campaign_expected_revision=current.value.revision_number,
        )
        if not is_success(committed):
            return committed
        return Success(ready.value, duplicate=committed.duplicate)
    def complete_finalization(
        self,
        effect_id: str,
        campaign_id: CampaignId,
        *,
        expected_revision: int,
    ) -> Outcome[MutationCampaign]:
        # Complete only after the durable intent and full required artifact registry are already committed.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            return self._campaign_from_receipt(existing_effect.value)
        current = self._campaign(campaign_id)
        if not is_success(current):
            return current
        if current.value.status == CampaignState.COMPLETED:
            completed_intent = self.store.get_finalization_intent(campaign_id)
            if not is_success(completed_intent):
                return completed_intent
            if completed_intent.value is None or completed_intent.value.status != "completed":
                return Rejected(
                    code="completed_finalization_receipt_missing",
                    message="Completed campaign has no completed finalization intent",
                    details={"campaign_id": campaign_id.value},
                )
            completed_registry = self.store.list_artifacts(campaign_id)
            if not is_success(completed_registry):
                return completed_registry
            expected_completed = {
                item.logical_key: item for item in completed_intent.value.artifacts
            }
            actual_completed = {
                item.logical_key: item for item in completed_registry.value
            }
            completed_counts = Counter(
                item.logical_key for item in completed_registry.value
            )
            duplicate_completed = tuple(
                sorted(key for key, count in completed_counts.items() if count > 1)
            )
            missing_completed = tuple(
                sorted(set(expected_completed) - set(actual_completed))
            )
            unexpected_completed = tuple(
                sorted(set(actual_completed) - set(expected_completed))
            )
            conflicting_completed = tuple(
                sorted(
                    key
                    for key in set(expected_completed) & set(actual_completed)
                    if expected_completed[key] != actual_completed[key]
                )
            )
            if (
                duplicate_completed
                or missing_completed
                or unexpected_completed
                or conflicting_completed
            ):
                return Rejected(
                    code="completed_artifact_registry_invalid",
                    message="Completed campaign artifact registry does not match its finalization intent",
                    details={
                        "duplicate_logical_keys": ",".join(duplicate_completed),
                        "missing_logical_keys": ",".join(missing_completed),
                        "unexpected_logical_keys": ",".join(unexpected_completed),
                        "conflicting_logical_keys": ",".join(conflicting_completed),
                    },
                )
            return Success(current.value, duplicate=True)
        if current.value.status != CampaignState.READY_TO_COMMIT:
            return Rejected(
                code="finalize_out_of_order",
                message="Campaign must have a verified artifact registry before completion",
                details={"status": current.value.status.value},
            )
        stored_intent = self.store.get_finalization_intent(campaign_id)
        if not is_success(stored_intent):
            return stored_intent
        if stored_intent.value is None or stored_intent.value.status not in {"registered", "completed"}:
            return Rejected(
                code="artifact_registry_required",
                message="Campaign completion requires a registered finalization intent",
                details={"campaign_id": campaign_id.value},
            )
        registry = self.store.list_artifacts(campaign_id)
        if not is_success(registry):
            return registry
        expected_by_key = {
            item.logical_key: item for item in stored_intent.value.artifacts
        }
        registry_by_key = {item.logical_key: item for item in registry.value}
        registry_key_counts = Counter(item.logical_key for item in registry.value)
        duplicate_keys = tuple(
            sorted(key for key, count in registry_key_counts.items() if count > 1)
        )
        missing = tuple(sorted(set(expected_by_key) - set(registry_by_key)))
        unexpected = tuple(sorted(set(registry_by_key) - set(expected_by_key)))
        conflicting = tuple(
            sorted(
                key
                for key in set(expected_by_key) & set(registry_by_key)
                if expected_by_key[key] != registry_by_key[key]
            )
        )
        canonical = registry_by_key.get(stored_intent.value.canonical_report_key)
        if duplicate_keys or missing or unexpected or conflicting or canonical is None:
            return Rejected(
                code="artifact_registry_incomplete",
                message="Campaign completion requires the exact immutable artifact set including canonical report",
                details={
                    "duplicate_logical_keys": ",".join(duplicate_keys),
                    "missing_logical_keys": ",".join(missing),
                    "unexpected_logical_keys": ",".join(unexpected),
                    "conflicting_logical_keys": ",".join(conflicting),
                    "canonical_report_key": stored_intent.value.canonical_report_key,
                },
            )
        completed = current.value.transition(
            CampaignState.COMPLETED,
            expected_revision=expected_revision,
        )
        if not is_success(completed):
            return completed
        completed_intent = replace(
            stored_intent.value,
            status="completed",
            updated_at=utc_now(),
        )
        receipt = EffectReceipt(
            effect_id=effect_id,
            effect_type="mutation.finalize",
            campaign_id=campaign_id,
            payload={
                "campaign": completed.value.to_dict(),
                "finalization_intent": completed_intent.to_dict(),
                "canonical_artifact": canonical.to_dict(),
            },
        )
        committed = self.store.commit_finalization(
            receipt=receipt,
            intent=completed_intent,
            campaign=completed.value,
            campaign_expected_revision=current.value.revision_number,
        )
        if not is_success(committed):
            return committed
        return Success(completed.value, duplicate=committed.duplicate)
    def finalize(self, effect_id: str, campaign_id: CampaignId, *, expected_revision: int) -> Outcome[MutationCampaign]:
        # Preserve the public method while forbidding completion without the authoritative artifact registry.
        current = self._campaign(campaign_id)
        if not is_success(current):
            return current
        if current.value.status not in {
            CampaignState.READY_TO_COMMIT,
            CampaignState.COMPLETED,
        }:
            return Rejected(
                code="artifact_registry_required",
                message="Use finalization intent and artifact registry before campaign completion",
                details={"status": current.value.status.value},
            )
        return self.complete_finalization(
            effect_id,
            campaign_id,
            expected_revision=expected_revision,
        )
    def create_shard(self, shard: MutationShard) -> Outcome[MutationShard]:
        # Persist one immutable shard plan before a worker lease is issued.
        campaign = self._campaign(shard.campaign_id)
        if not is_success(campaign):
            return campaign
        return self.store.save_shard(shard, expected_revision=None)
    def claim_shard(
        self,
        effect_id: str,
        shard_id: ShardId,
        lease: ShardLease,
        *,
        expected_revision: int,
    ) -> Outcome[MutationShard]:
        # Apply one durable lease claim with replay-safe effect identity.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            payload = existing_effect.value.payload.get("shard")
            if isinstance(payload, Mapping):
                return Success(MutationShard.from_dict(payload), duplicate=True)
            return Rejected(code="invalid_effect", message="Shard lease receipt has no shard projection")
        stored = self.store.get_shard(shard_id)
        if not is_success(stored):
            return stored
        if stored.value is None:
            return Rejected(code="shard_not_found", message="Mutation shard does not exist")
        claimed = stored.value.claim(lease, expected_revision=expected_revision)
        if not is_success(claimed):
            return claimed
        receipt = EffectReceipt(
            effect_id=effect_id,
            effect_type="mutation.claim_shard",
            campaign_id=claimed.value.campaign_id,
            payload={"shard": claimed.value.to_dict()},
        )
        recorded = self._commit_effect(
            receipt,
            shard=None if claimed.duplicate else claimed.value,
            shard_expected_revision=expected_revision if not claimed.duplicate else None,
        )
        if not is_success(recorded):
            return recorded
        return Success(claimed.value, duplicate=recorded.duplicate)
    def start_shard(
        self,
        effect_id: str,
        shard_id: ShardId,
        *,
        expected_revision: int,
    ) -> Outcome[MutationShard]:
        # Move a claimed shard to running through the same durable effect boundary.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            payload = existing_effect.value.payload.get("shard")
            if isinstance(payload, Mapping):
                return Success(MutationShard.from_dict(payload), duplicate=True)
            return Rejected(code="invalid_effect", message="Shard start receipt has no shard projection")
        stored = self.store.get_shard(shard_id)
        if not is_success(stored):
            return stored
        if stored.value is None:
            return Rejected(code="shard_not_found", message="Mutation shard does not exist")
        started = stored.value.start(expected_revision=expected_revision)
        if not is_success(started):
            return started
        receipt = EffectReceipt(
            effect_id=effect_id,
            effect_type="mutation.start_shard",
            campaign_id=started.value.campaign_id,
            payload={"shard": started.value.to_dict()},
        )
        recorded = self._commit_effect(
            receipt,
            shard=None if started.duplicate else started.value,
            shard_expected_revision=expected_revision if not started.duplicate else None,
        )
        if not is_success(recorded):
            return recorded
        return Success(started.value, duplicate=recorded.duplicate)
    def renew_shard_lease(
        self,
        effect_id: str,
        shard_id: ShardId,
        lease: ShardLease,
        *,
        expected_revision: int,
        now: str | None = None,
    ) -> Outcome[MutationShard]:
        # Renew one live worker lease atomically and make duplicate heartbeats replay-safe.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            payload = existing_effect.value.payload.get("shard")
            if isinstance(payload, Mapping):
                return Success(MutationShard.from_dict(payload), duplicate=True)
            return Rejected(code="invalid_effect", message="Lease receipt has no shard projection")
        stored = self.store.get_shard(shard_id)
        if not is_success(stored):
            return stored
        if stored.value is None:
            return Rejected(code="shard_not_found", message="Mutation shard does not exist")
        renewed = stored.value.renew_lease(lease, expected_revision=expected_revision, now=now)
        if not is_success(renewed):
            return renewed
        receipt = EffectReceipt(
            effect_id=effect_id,
            effect_type="mutation.renew_lease",
            campaign_id=renewed.value.campaign_id,
            payload={"shard": renewed.value.to_dict()},
        )
        recorded = self._commit_effect(
            receipt,
            shard=None if renewed.duplicate else renewed.value,
            shard_expected_revision=expected_revision if not renewed.duplicate else None,
        )
        if not is_success(recorded):
            return recorded
        return Success(renewed.value, duplicate=recorded.duplicate)
    def orphan_expired_shard(
        self,
        effect_id: str,
        shard_id: ShardId,
        *,
        expected_revision: int,
        now: str | None = None,
    ) -> Outcome[MutationShard]:
        # Convert an expired lease into an orphaned shard without accepting a live takeover.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            payload = existing_effect.value.payload.get("shard")
            if isinstance(payload, Mapping):
                return Success(MutationShard.from_dict(payload), duplicate=True)
            return Rejected(code="invalid_effect", message="Orphan receipt has no shard projection")
        stored = self.store.get_shard(shard_id)
        if not is_success(stored):
            return stored
        if stored.value is None:
            return Rejected(code="shard_not_found", message="Mutation shard does not exist")
        if not stored.value.lease_expired(now or utc_now()):
            return Rejected(code="lease_not_expired", message="Shard lease is still alive")
        orphaned = stored.value.orphan(expected_revision=expected_revision)
        if not is_success(orphaned):
            return orphaned
        receipt = EffectReceipt(
            effect_id=effect_id,
            effect_type="mutation.orphan_shard",
            campaign_id=orphaned.value.campaign_id,
            payload={"shard": orphaned.value.to_dict()},
        )
        recorded = self._commit_effect(
            receipt,
            shard=None if orphaned.duplicate else orphaned.value,
            shard_expected_revision=expected_revision if not orphaned.duplicate else None,
        )
        if not is_success(recorded):
            return recorded
        return Success(orphaned.value, duplicate=recorded.duplicate)
    def retry_shard(
        self,
        effect_id: str,
        shard_id: ShardId,
        *,
        expected_revision: int,
    ) -> Outcome[MutationShard]:
        # Requeue an orphaned or failed shard atomically and increment its attempt identity.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            payload = existing_effect.value.payload.get("shard")
            if isinstance(payload, Mapping):
                return Success(MutationShard.from_dict(payload), duplicate=True)
            return Rejected(code="invalid_effect", message="Retry receipt has no shard projection")
        stored = self.store.get_shard(shard_id)
        if not is_success(stored):
            return stored
        if stored.value is None:
            return Rejected(code="shard_not_found", message="Mutation shard does not exist")
        retried = stored.value.retry(expected_revision=expected_revision)
        if not is_success(retried):
            return retried
        receipt = EffectReceipt(
            effect_id=effect_id,
            effect_type="mutation.retry_shard",
            campaign_id=retried.value.campaign_id,
            payload={"shard": retried.value.to_dict()},
        )
        recorded = self._commit_effect(
            receipt,
            shard=None if retried.duplicate else retried.value,
            shard_expected_revision=expected_revision if not retried.duplicate else None,
        )
        if not is_success(recorded):
            return recorded
        return Success(retried.value, duplicate=recorded.duplicate)
    def record_shard_result(
        self,
        effect_id: str,
        campaign_id: CampaignId,
        shard_id: ShardId,
        result: ShardExecutionResult,
        *,
        expected_campaign_revision: int,
        now: str | None = None,
        durable_delivery_replay: bool = False,
        _shard_revision_retry: int = 0,
    ) -> Outcome[ShardResultReceipt]:
        # Record executions, shard fan-in and campaign progress while retrying only a concurrent heartbeat revision race.
        existing_effect = self._existing_effect(effect_id)
        if not is_success(existing_effect):
            return existing_effect
        if existing_effect.value is not None:
            return self._shard_receipt_from_effect(existing_effect.value)
        if result.shard_id != shard_id:
            return Rejected(code="shard_result_mismatch", message="Result belongs to another shard")
        campaign = self._campaign(campaign_id)
        if not is_success(campaign):
            return campaign
        shard_result = self.store.get_shard(shard_id)
        if not is_success(shard_result):
            return shard_result
        if shard_result.value is None:
            return Rejected(code="shard_not_found", message="Mutation shard does not exist")
        shard = shard_result.value
        if shard.campaign_id != campaign_id:
            return Rejected(code="shard_campaign_mismatch", message="Shard belongs to another campaign")
        if shard.worker_id is None or result.worker_id != shard.worker_id:
            return Rejected(
                code="worker_mismatch",
                message="Shard result was produced by another worker",
            )
        if shard.lease is None:
            return Rejected(code="lease_missing", message="Shard has no active lease projection")
        lease_reader = getattr(self.store, "get_lease", None)
        authoritative_lease = (
            lease_reader(shard.lease.lease_id)
            if callable(lease_reader)
            else Success(None)
        )
        if not is_success(authoritative_lease):
            return authoritative_lease
        if authoritative_lease.value is None:
            # Preserve legacy tests and pre-PR5 databases; production claims always create mutation_leases.
            lease = replace(
                MutationLease.claim(campaign_id, shard_id, shard.lease),
                status=MutationLeaseState.RUNNING,
            )
        else:
            lease = authoritative_lease.value
        if (
            lease.campaign_id != campaign_id
            or lease.shard_id != shard_id
            or lease.worker_id != result.worker_id
            or lease.attempt != shard.attempt
        ):
            return Rejected(code="lease_projection_mismatch", message="Shard and authoritative lease diverged")
        if lease.status not in {MutationLeaseState.RUNNING, MutationLeaseState.DELIVERING}:
            return Rejected(code="lease_not_active", message="Shard result lease is no longer active")
        executions: list[MutationExecution] = []
        seen_mutants: set[str] = set()
        for public_result in result.results:
            mutant_id = public_result.mutant_id.value
            if mutant_id not in {item.value for item in shard.mutant_ids}:
                return Rejected(
                    code="foreign_mutant",
                    message="Shard result contains a mutant outside immutable shard membership",
                    details={"mutant_id": mutant_id, "shard_id": shard_id.value},
                )
            if mutant_id in seen_mutants:
                return Rejected(
                    code="duplicate_mutant_result",
                    message="One shard result bundle contains the same mutant more than once",
                    details={"mutant_id": mutant_id},
                )
            seen_mutants.add(mutant_id)
            if public_result.lease_id != lease.lease_id:
                return Rejected(
                    code="stale_lease",
                    message="Shard result lease token is no longer current",
                    details={
                        "expected_lease_id": lease.lease_id,
                        "received_lease_id": public_result.lease_id,
                    },
                )
            if public_result.attempt != shard.attempt:
                return Rejected(
                    code="stale_attempt",
                    message="Shard result attempt is no longer current",
                    details={"expected_attempt": shard.attempt, "received_attempt": public_result.attempt},
                )
            execution = self._execution_from_result(campaign_id, shard_id, public_result)
            executions.append(execution)
        fan_in_at = now or utc_now()
        if (
            lease.status == MutationLeaseState.RUNNING
            and lease.expired(fan_in_at)
            and not durable_delivery_replay
        ):
            return Rejected(
                code="lease_expired",
                message="Authoritative running lease expired before result fan-in",
                details={
                    "campaign_id": campaign_id.value,
                    "shard_id": shard_id.value,
                    "lease_id": lease.lease_id,
                    "attempt": lease.attempt,
                    "lease_status": getattr(lease.status, "value", lease.status),
                    "expires_at": lease.expires_at,
                    "fan_in_at": fan_in_at,
                    "durable_delivery_replay": bool(durable_delivery_replay),
                },
            )
        result_status = getattr(result.status, "value", result.status)
        terminal_bundle = str(result_status) == "complete"
        if terminal_bundle and shard.mutant_ids and not seen_mutants:
            return Rejected(
                code="empty_terminal_bundle",
                message="A non-empty shard cannot complete without mutant executions",
            )
        if int(result.completed_mutants) != len(seen_mutants):
            return Rejected(
                code="result_progress_mismatch",
                message="Worker completed_mutants does not match validated result membership",
                details={"reported": int(result.completed_mutants), "validated": len(seen_mutants)},
            )
        expected_mutants = {item.value for item in shard.mutant_ids}
        if terminal_bundle and seen_mutants != expected_mutants:
            return Rejected(
                code="incomplete_terminal_bundle",
                message="A terminal shard result must contain exactly the immutable shard membership",
                details={"missing": ",".join(sorted(expected_mutants - seen_mutants))},
            )
        failed = str(result_status) in {"error", "failed", "baseline_failed", "restore_error"}
        cancelled = str(result_status) in {"cancelled", "canceled"}
        shard_update = shard.record_completion(
            min(max(0, result.completed_mutants), len(shard.mutant_ids)),
            expected_revision=shard.revision_number,
            failed=failed,
            cancelled=cancelled,
        )
        if not is_success(shard_update):
            return shard_update
        persisted_executions = self.store.list_executions(campaign_id)
        if not is_success(persisted_executions):
            return persisted_executions
        completed_mutants = {
            item.mutant_id.value
            for item in (*persisted_executions.value, *executions)
            if item.status == MutationExecutionState.COMPLETE
        }
        progress = campaign.value.record_progress(
            len(completed_mutants),
            expected_revision=expected_campaign_revision,
            total_mutants=max(campaign.value.total_mutants, len(shard.mutant_ids)),
        )
        if not is_success(progress):
            return progress
        saved_shard = shard_update.value
        saved_campaign = progress.value
        receipt = EffectReceipt(
            effect_id=effect_id,
            effect_type="mutation.execute_shard",
            campaign_id=campaign_id,
            payload={
                "campaign": saved_campaign.to_dict(),
                "shard": saved_shard.to_dict(),
                "executions": [item.to_dict() for item in executions],
            },
        )
        recorded = self._commit_effect(
            receipt,
            campaign=None if progress.duplicate else saved_campaign,
            campaign_expected_revision=expected_campaign_revision if not progress.duplicate else None,
            shard=None if shard_update.duplicate else saved_shard,
            shard_expected_revision=shard.revision_number if not shard_update.duplicate else None,
            executions=tuple(executions),
        )
        shard_revision_race = (
            isinstance(recorded, Rejected)
            and recorded.code == "stale_revision"
            and (
                str(recorded.details.get("entity", "")) == "shard"
                or str(recorded.message).lower().startswith("shard ")
            )
        )
        if shard_revision_race and _shard_revision_retry < 3:
            return self.record_shard_result(
                effect_id,
                campaign_id,
                shard_id,
                result,
                expected_campaign_revision=expected_campaign_revision,
                now=now,
                durable_delivery_replay=durable_delivery_replay,
                _shard_revision_retry=_shard_revision_retry + 1,
            )
        if not is_success(recorded):
            return recorded
        final_receipt = self._shard_receipt_from_effect(recorded.value)
        if not is_success(final_receipt):
            return final_receipt
        if recorded.duplicate:
            return Success(final_receipt.value, duplicate=True)
        return Success(final_receipt.value)
    def cancel(self, effect_id: str, campaign_id: CampaignId, *, expected_revision: int) -> Outcome[MutationCampaign]:
        # Cancel preparation or execution through the same replay-safe transition path.
        return self._transition(effect_id, campaign_id, CampaignState.CANCELLED, expected_revision=expected_revision, effect_type="mutation.cancel")
    def _execution_from_result(
        self,
        campaign_id: CampaignId,
        shard_id: ShardId,
        result: MutantExecutionResult,
    ) -> MutationExecution:
        # Adapt public result evidence into the domain execution aggregate.
        status = str(result.status)
        execution_state = (
            MutationExecutionState.CANCELLED
            if status in {"cancelled", "canceled"}
            else MutationExecutionState.FAILED
            if status in {"error", "failed", "baseline_failed", "restore_error"}
            else MutationExecutionState.COMPLETE
        )
        semantic: MutationResult | str
        try:
            semantic = MutationResult(status)
        except ValueError:
            semantic = status
        selected_tests = tuple(
            str(nodeid)
            for level in result.level_results
            for nodeid in level.get("nodeids", [])
            if isinstance(level.get("nodeids", []), (list, tuple))
        )
        artifacts = tuple(
            ArtifactId(deterministic_id("art", campaign_id.value, result.execution_id.value, path))
            for path in result.artifact_paths
        )
        return MutationExecution(
            execution_id=result.execution_id,
            campaign_id=campaign_id,
            shard_id=shard_id,
            mutant_id=result.mutant_id,
            attempt=result.attempt,
            status=execution_state,
            semantic_result=semantic,
            selected_tests=selected_tests,
            artifacts=artifacts,
            duration_seconds=result.duration_seconds,
            restore_verified=result.restore_verified,
            error=result.error,
            revision_number=0,
            lease_id=result.lease_id,
            test_observations=tuple(dict(item) for item in result.test_observations),
        )
    def _shard_receipt_from_effect(self, receipt: EffectReceipt) -> Outcome[ShardResultReceipt]:
        # Decode the exact fan-in projection saved by the original execute-shard effect.
        try:
            raw_campaign = receipt.payload["campaign"]
            raw_shard = receipt.payload["shard"]
            raw_executions = receipt.payload["executions"]
            if not isinstance(raw_campaign, Mapping) or not isinstance(raw_shard, Mapping) or not isinstance(raw_executions, (list, tuple)):
                raise ValueError("invalid shard receipt payload")
            return Success(
                ShardResultReceipt(
                    campaign=MutationCampaign.from_dict(raw_campaign),
                    shard=MutationShard.from_dict(raw_shard),
                    executions=tuple(MutationExecution.from_dict(item) for item in raw_executions),
                ),
                duplicate=True,
            )
        except (KeyError, TypeError, ValueError) as exc:
            return Failed("effect_receipt_corrupted", f"Cannot decode shard receipt: {exc}")
