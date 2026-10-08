"""Startup recovery primitives for durable local Theseus campaigns."""
from __future__ import annotations
import json
import os
import uuid
from pathlib import Path
from typing import Any, Mapping, Sequence
from gallifrey_mutation import CampaignState, MutationCampaign, MutationCampaignService, SQLiteMutationStore, Success
from theseus_contracts import CampaignId, CampaignPlan, ShardAssignment, ShardDescriptor, ShardExecutionResult
from theseus_contracts.serialization import utc_now
from test_intelligence_unified_v1.io_utils import atomic_write_json
from test_intelligence_unified_v1.recovery import current_process_birth_token
from .finalization import (
    cleanup_finalization_staging,
    publish_finalization_intent,
    validate_registered_artifacts,
)
from .worker_runtime import DurableExecutionSpool
class StartupRecoveryError(RuntimeError):
    """Raised when durable startup evidence is incomplete or contradictory."""
class StartupRecoveryBusy(StartupRecoveryError):
    """Raised when another live coordinator owns the startup recovery lock."""
def _process_exists(process_id: int) -> bool:
    # Conservatively detect a live PID when the platform cannot expose a stable birth token.
    if process_id <= 0:
        return False
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True
def _fsync_directory(path: Path) -> None:
    # Persist lock creation, replacement and removal directory entries on POSIX filesystems.
    if os.name == "nt":
        return
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)
class StartupRecoveryLock:
    """PID-and-birth-token fenced lock for one campaign control database."""
    def __init__(self, path: Path, *, database_path: Path) -> None:
        # Bind the lock to one canonical database and one concrete process incarnation.
        self.path = Path(path)
        self.database_path = Path(database_path).resolve()
        self.process_id = os.getpid()
        self.process_birth_token = (
            current_process_birth_token(self.process_id)
            or f"pid-{self.process_id}-unknown"
        )
        self.owner_id = uuid.uuid4().hex
        self._held = False
    @staticmethod
    def _read_owner(path: Path) -> Mapping[str, Any] | None:
        # Decode a prior owner defensively so malformed stale locks never look live.
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, TypeError, ValueError):
            return None
        return dict(value) if isinstance(value, Mapping) else None
    @staticmethod
    def _owner_is_alive(owner: Mapping[str, Any] | None) -> bool:
        # Trust a lock only when both PID and process birth token still identify the same process.
        if not isinstance(owner, Mapping):
            return False
        try:
            process_id = int(owner.get("process_id", 0))
        except (TypeError, ValueError):
            return False
        birth_token = str(owner.get("process_birth_token", ""))
        if process_id <= 0 or not birth_token:
            return False
        observed = current_process_birth_token(process_id)
        if observed is not None:
            return observed == birth_token
        return birth_token == f"pid-{process_id}-unknown" and _process_exists(process_id)
    def _payload(self) -> dict[str, Any]:
        # Persist enough ownership context to diagnose contention and fence PID reuse.
        return {
            "schema_version": 1,
            "owner_id": self.owner_id,
            "process_id": self.process_id,
            "process_birth_token": self.process_birth_token,
            "database_path": str(self.database_path),
            "acquired_at": utc_now(),
        }
    def __enter__(self) -> "StartupRecoveryLock":
        # Acquire exclusively, replacing only a lock whose exact process incarnation is gone.
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(
            self._payload(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        for _ in range(2):
            try:
                descriptor = os.open(
                    self.path,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                )
            except FileExistsError as exc:
                owner = self._read_owner(self.path)
                if self._owner_is_alive(owner):
                    raise StartupRecoveryBusy(
                        "startup recovery is already owned by a live coordinator: "
                        f"database={self.database_path}; "
                        f"process_id={owner.get('process_id')}; "
                        f"owner_id={owner.get('owner_id')}; "
                        f"lock={self.path}"
                    ) from exc
                stale_path = self.path.with_name(
                    f"{self.path.name}.stale.{uuid.uuid4().hex}"
                )
                try:
                    os.replace(self.path, stale_path)
                except FileNotFoundError:
                    continue
                try:
                    stale_path.unlink()
                except FileNotFoundError:
                    pass
                _fsync_directory(self.path.parent)
                continue
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            _fsync_directory(self.path.parent)
            self._held = True
            return self
        raise StartupRecoveryBusy(
            "startup recovery lock changed during acquisition: "
            f"database={self.database_path}; lock={self.path}"
        )
    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        # Delete only the lock still carrying this exact owner ID.
        del exc_type, exc, traceback
        if not self._held:
            return
        owner = self._read_owner(self.path)
        if isinstance(owner, Mapping) and owner.get("owner_id") == self.owner_id:
            try:
                self.path.unlink()
            except FileNotFoundError:
                pass
            else:
                _fsync_directory(self.path.parent)
        self._held = False
def scan_campaign_databases(root: Path) -> tuple[Path, ...]:
    # Discover campaign control databases deterministically while skipping tool and environment caches.
    resolved = Path(root).resolve()
    if resolved.is_file():
        return (resolved,) if resolved.name == "campaign.sqlite3" else ()
    if not resolved.exists():
        return ()
    ignored = {".git", ".pytest_cache", "__pycache__", ".venv", "venv"}
    databases = {
        path.resolve()
        for path in resolved.rglob("campaign.sqlite3")
        if not any(part in ignored for part in path.parts)
    }
    return tuple(sorted(databases, key=lambda item: item.as_posix()))
def recovery_report_path(database_path: Path) -> Path:
    # Keep the latest explicit startup decision beside the authoritative campaign database.
    return Path(database_path).resolve().parent / "startup-recovery.report.json"
def write_recovery_report(
    database_path: Path,
    *,
    started_at: str,
    completed_at: str,
    status: str,
    actions: Sequence[Mapping[str, Any]],
    error: str | None = None,
) -> Path:
    # Publish a bounded cumulative report so a second idempotent pass cannot erase the crash-recovery decision.
    path = recovery_report_path(database_path)
    previous_actions: list[dict[str, Any]] = []
    run_count = 0
    try:
        previous = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        previous = {}
    if isinstance(previous, Mapping):
        raw_previous_actions = previous.get("actions", [])
        if isinstance(raw_previous_actions, (list, tuple)):
            previous_actions = [
                dict(item) for item in raw_previous_actions if isinstance(item, Mapping)
            ]
        run_count = max(0, int(previous.get("run_count", 0)))
    combined: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in (*previous_actions, *(dict(row) for row in actions)):
        identity = json.dumps(
            item,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        if identity in seen:
            continue
        seen.add(identity)
        combined.append(item)
    combined = combined[-1000:]
    atomic_write_json(
        path,
        {
            "schema_version": 1,
            "database_path": str(Path(database_path).resolve()),
            "started_at": started_at,
            "completed_at": completed_at,
            "status": str(status),
            "run_count": run_count + 1,
            "run_action_count": len(actions),
            "action_count": len(combined),
            "actions": combined,
            "error": str(error) if error else None,
        },
        durability="critical",
        category="recovery_report",
    )
    return path
def _require(result: Any, *, operation: str) -> Any:
    # Convert typed domain outcomes into a diagnostic startup recovery failure.
    if not isinstance(result, Success):
        code = getattr(result, "code", "domain_failure")
        message = getattr(result, "message", str(result))
        details = dict(getattr(result, "details", {}) or {})
        raise StartupRecoveryError(
            f"{operation} failed: code={code}; message={message}; details={details}"
        )
    return result.value
def _delivery_identity(entry_payload: Mapping[str, Any]) -> tuple[ShardAssignment, Mapping[str, Any], ShardExecutionResult]:
    # Decode the immutable assignment, worker and shard result carried by one durable delivery.
    raw_assignment = entry_payload.get("assignment")
    raw_worker = entry_payload.get("worker")
    raw_result = entry_payload.get("result")
    if not isinstance(raw_assignment, Mapping):
        raise StartupRecoveryError("durable delivery has no assignment object")
    if not isinstance(raw_worker, Mapping):
        raise StartupRecoveryError("durable delivery has no worker object")
    if not isinstance(raw_result, Mapping):
        raise StartupRecoveryError("durable delivery has no result object")
    raw_shard_result = raw_result.get("shard_result")
    if not isinstance(raw_shard_result, Mapping):
        raise StartupRecoveryError("durable delivery has no shard_result object")
    return (
        ShardAssignment.from_dict(raw_assignment),
        dict(raw_worker),
        ShardExecutionResult.from_dict(raw_shard_result),
    )
def _validate_published_artifacts(
    database_path: Path,
    *,
    campaign_id: CampaignId,
    delivery_event_id: str,
    raw_result: Mapping[str, Any],
) -> tuple[str, ...]:
    # Verify every worker-published canonical artifact reference before replaying authoritative fan-in.
    raw_artifacts = raw_result.get("published_engine_artifacts", [])
    if not isinstance(raw_artifacts, (list, tuple)):
        raise StartupRecoveryError(
            "durable delivery published_engine_artifacts is not an array: "
            f"event_id={delivery_event_id}; type={type(raw_artifacts).__name__}"
        )
    canonical_root = (
        Path(database_path).resolve().parent.parent
        / "engine"
        / campaign_id.value
    ).resolve()
    validated: list[str] = []
    for item in raw_artifacts:
        relative = Path(str(item).replace("\\", "/"))
        if not str(item).strip() or relative.is_absolute() or ".." in relative.parts:
            raise StartupRecoveryError(
                "durable delivery contains an unsafe canonical artifact reference: "
                f"event_id={delivery_event_id}; artifact={item!r}; "
                f"canonical_root={canonical_root}"
            )
        candidate = (canonical_root / relative).resolve()
        if canonical_root != candidate and canonical_root not in candidate.parents:
            raise StartupRecoveryError(
                "durable delivery artifact escapes the canonical campaign root: "
                f"event_id={delivery_event_id}; artifact={item!r}; "
                f"resolved={candidate}; canonical_root={canonical_root}"
            )
        if not candidate.is_file():
            raise StartupRecoveryError(
                "durable delivery canonical artifact is missing before replay: "
                f"event_id={delivery_event_id}; artifact={relative.as_posix()}; "
                f"expected_path={candidate}; canonical_root={canonical_root}"
            )
        validated.append(relative.as_posix())
    return tuple(validated)
def _validate_mutant_events(
    spool: DurableExecutionSpool,
    *,
    delivery_event_id: str,
    shard_result: ShardExecutionResult,
) -> tuple[str, ...]:
    # Require one exact committed per-mutant event for every restored row before recovery fan-in.
    frames = spool.mutant_events(
        parent_event_id=delivery_event_id,
        include_quarantined=False,
    )
    expected = {
        (item.mutant_id.value, item.execution_id.value)
        for item in shard_result.results
        if item.restore_verified
    }
    actual = {
        (str(item.get("mutant_id", "")), str(item.get("execution_id", "")))
        for item in frames
    }
    if actual != expected:
        raise StartupRecoveryError(
            "durable delivery per-mutant evidence mismatch: "
            f"event_id={delivery_event_id}; expected={sorted(expected)}; "
            f"actual={sorted(actual)}; spool={spool.root}"
        )
    return tuple(str(item.get("event_id", "")) for item in frames)
def _load_campaign_plan_authority(
    database_path: Path,
    campaign: MutationCampaign,
) -> tuple[CampaignPlan, dict[str, tuple[int, ShardDescriptor]]]:
    # Load and validate the immutable planner topology before replaying any worker delivery.
    plan_path = Path(database_path).resolve().parent / "campaign.plan.json"
    try:
        raw = json.loads(plan_path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError) as exc:
        raise StartupRecoveryError(
            f"startup recovery requires a valid campaign plan: path={plan_path}; error={exc}"
        ) from exc
    if not isinstance(raw, Mapping):
        raise StartupRecoveryError(f"campaign plan is not an object: path={plan_path}")
    try:
        plan = CampaignPlan.from_dict(raw)
        plan.verify_integrity()
    except (TypeError, ValueError) as exc:
        raise StartupRecoveryError(f"campaign plan integrity failed: path={plan_path}; error={exc}") from exc
    if plan.campaign_id != campaign.campaign_id or plan.plan_id != campaign.plan_id:
        raise StartupRecoveryError(
            "campaign aggregate and immutable plan identity conflict: "
            f"campaign_id={campaign.campaign_id.value}; campaign_plan_id={campaign.plan_id}; "
            f"artifact_plan_id={plan.plan_id}; path={plan_path}"
        )
    if plan.prepared_snapshot_id != campaign.prepared_snapshot_id:
        raise StartupRecoveryError(
            "campaign prepared snapshot conflicts with immutable plan: "
            f"campaign_id={campaign.campaign_id.value}; "
            f"campaign_snapshot={campaign.prepared_snapshot_id}; "
            f"plan_snapshot={plan.prepared_snapshot_id}"
        )
    if campaign.total_mutants != plan.selected_count:
        raise StartupRecoveryError(
            "campaign mutant count conflicts with immutable plan: "
            f"campaign_id={campaign.campaign_id.value}; "
            f"campaign_total={campaign.total_mutants}; plan_selected={plan.selected_count}"
        )
    descriptors = {
        item.shard_id.value: (ordinal, item)
        for ordinal, item in enumerate(plan.shards)
    }
    return plan, descriptors
def validate_campaign_plan_topology(
    database_path: Path,
    store: SQLiteMutationStore,
    campaign: MutationCampaign,
    *,
    allow_unmaterialized: bool,
) -> CampaignPlan:
    # Validate the complete persisted shard topology or one explicit pre-materialization crash boundary.
    plan, descriptors = _load_campaign_plan_authority(database_path, campaign)
    shards = _require(
        store.list_shards(campaign.campaign_id),
        operation=f"list shards for {campaign.campaign_id.value}",
    )
    if not shards:
        unmaterialized_allowed = (
            allow_unmaterialized
            and campaign.status in {CampaignState.PLANNING, CampaignState.RUNNING}
        )
        if plan.shards and not unmaterialized_allowed:
            raise StartupRecoveryError(
                "campaign plan topology is not materialized: "
                f"campaign_id={campaign.campaign_id.value}; plan_id={plan.plan_id}; "
                f"status={campaign.status.value}"
            )
        return plan
    if len(shards) != len(plan.shards):
        raise StartupRecoveryError(
            "campaign plan topology is partially materialized: "
            f"campaign_id={campaign.campaign_id.value}; plan_id={plan.plan_id}; "
            f"expected_shards={len(plan.shards)}; actual_shards={len(shards)}"
        )
    actual_ids = {item.shard_id.value for item in shards}
    expected_ids = set(descriptors)
    if actual_ids != expected_ids:
        raise StartupRecoveryError(
            "campaign plan topology shard identities conflict: "
            f"campaign_id={campaign.campaign_id.value}; "
            f"missing={sorted(expected_ids - actual_ids)}; "
            f"unexpected={sorted(actual_ids - expected_ids)}"
        )
    for shard in shards:
        ordinal, descriptor = descriptors[shard.shard_id.value]
        if not shard.matches_plan_descriptor(plan.plan_id, descriptor, ordinal=ordinal):
            raise StartupRecoveryError(
                "campaign plan topology projection conflicts with immutable plan: "
                f"campaign_id={campaign.campaign_id.value}; shard_id={shard.shard_id.value}; "
                f"plan_id={plan.plan_id}"
            )
    return plan
def replay_pending_deliveries(
    *,
    database_path: Path,
    store: SQLiteMutationStore,
    service: MutationCampaignService,
    campaign_id: CampaignId,
    now: str,
) -> tuple[dict[str, Any], ...]:
    # Fan in unacknowledged durable deliveries before stale worker and lease ownership is orphaned.
    actions: list[dict[str, Any]] = []
    workers = _require(
        store.list_workers(campaign_id),
        operation=f"list workers for {campaign_id.value}",
    )
    if not workers:
        return ()
    campaign = _require(
        store.get_campaign(campaign_id),
        operation=f"load campaign {campaign_id.value} for delivery replay",
    )
    if campaign is None:
        raise StartupRecoveryError(f"delivery replay campaign is missing: {campaign_id.value}")
    campaign_plan, descriptors_by_id = _load_campaign_plan_authority(database_path, campaign)
    validate_campaign_plan_topology(
        database_path,
        store,
        campaign,
        allow_unmaterialized=False,
    )
    for worker in workers:
        spool = DurableExecutionSpool(Path(worker.spool_path))
        for entry in spool.pending():
            try:
                assignment, raw_worker, shard_result = _delivery_identity(entry.payload)
                if assignment.campaign_id != campaign_id.value:
                    raise StartupRecoveryError(
                        "durable delivery belongs to another campaign: "
                        f"expected={campaign_id.value}; actual={assignment.campaign_id}"
                    )
                if (
                    str(raw_worker.get("worker_id", "")) != worker.worker_id
                    or str(raw_worker.get("instance_id", "")) != worker.identity.instance_id
                    or int(raw_worker.get("process_id", 0)) != worker.identity.process_id
                    or str(raw_worker.get("process_birth_token", ""))
                    != worker.identity.process_birth_token
                ):
                    raise StartupRecoveryError(
                        "durable delivery worker identity conflicts with SQLite registry: "
                        f"event_id={entry.event_id}; worker_id={worker.worker_id}; "
                        f"registered_instance={worker.identity.instance_id}; "
                        f"delivery_instance={raw_worker.get('instance_id')}"
                    )
                shard = _require(
                    store.get_shard(shard_result.shard_id),
                    operation=f"load shard {shard_result.shard_id.value}",
                )
                if shard is None:
                    raise StartupRecoveryError(
                        f"durable delivery shard is missing: {shard_result.shard_id.value}"
                    )
                descriptor_row = descriptors_by_id.get(shard.shard_id.value)
                if descriptor_row is None:
                    raise StartupRecoveryError(
                        f"durable delivery references a shard outside campaign plan: {shard.shard_id.value}"
                    )
                ordinal, descriptor = descriptor_row
                if not shard.matches_plan_descriptor(campaign_plan.plan_id, descriptor, ordinal=ordinal):
                    raise StartupRecoveryError(
                        "durable delivery shard topology conflicts with campaign plan: "
                        f"shard_id={shard.shard_id.value}; plan_id={campaign_plan.plan_id}"
                    )
                if tuple(assignment.mutant_ids) != tuple(descriptor.mutant_ids):
                    raise StartupRecoveryError(
                        "durable delivery assignment membership conflicts with campaign plan: "
                        f"shard_id={shard.shard_id.value}; "
                        f"expected={list(descriptor.mutant_ids)}; actual={list(assignment.mutant_ids)}"
                    )
                if (
                    assignment.shard_id != shard.shard_id.value
                    or assignment.attempt != shard.attempt
                    or assignment.lease_id != (shard.lease.lease_id if shard.lease else None)
                    or shard_result.worker_id.value != worker.worker_id
                ):
                    raise StartupRecoveryError(
                        "durable delivery ownership conflicts with current shard generation: "
                        f"event_id={entry.event_id}; shard_id={shard.shard_id.value}; "
                        f"expected_attempt={shard.attempt}; actual_attempt={assignment.attempt}; "
                        f"expected_lease={shard.lease.lease_id if shard.lease else None}; "
                        f"actual_lease={assignment.lease_id}"
                    )
                raw_result = entry.payload.get("result")
                if not isinstance(raw_result, Mapping):
                    raise StartupRecoveryError(
                        f"durable delivery result is missing: event_id={entry.event_id}"
                    )
                published_artifacts = _validate_published_artifacts(
                    database_path,
                    campaign_id=campaign_id,
                    delivery_event_id=entry.event_id,
                    raw_result=raw_result,
                )
                mutant_event_ids = _validate_mutant_events(
                    spool,
                    delivery_event_id=entry.event_id,
                    shard_result=shard_result,
                )
                recorded = service.record_shard_result(
                    f"effect.execute-shard.{assignment.shard_id}.{assignment.attempt}",
                    campaign_id,
                    shard_result.shard_id,
                    shard_result,
                    expected_campaign_revision=campaign.revision_number,
                    now=now,
                    durable_delivery_replay=True,
                )
                receipt = _require(
                    recorded,
                    operation=f"replay delivery {entry.event_id}",
                )
                spool.acknowledge(entry.event_id)
                lease = _require(
                    store.get_lease(assignment.lease_id),
                    operation=f"load lease {assignment.lease_id}",
                )
                current_worker = _require(
                    store.get_worker(campaign_id, worker.worker_id),
                    operation=f"load worker {worker.worker_id}",
                )
                released = False
                if (
                    lease is not None
                    and current_worker is not None
                    and lease.status.value in {"running", "delivering"}
                    and current_worker.status.value in {"running", "delivering", "draining"}
                ):
                    release = service.release_lease_assignment(
                        (
                            f"effect.lease-release.{worker.worker_id}."
                            f"{assignment.shard_id}.{assignment.attempt}"
                        ),
                        campaign_id,
                        assignment.lease_id,
                        expected_lease_revision=lease.revision_number,
                        expected_worker_revision=current_worker.revision_number,
                    )
                    _require(
                        release,
                        operation=f"release replayed delivery {entry.event_id}",
                    )
                    released = True
                actions.append(
                    {
                        "campaign_id": campaign_id.value,
                        "shard_id": assignment.shard_id,
                        "lease_id": assignment.lease_id,
                        "attempt": assignment.attempt,
                        "worker_id": worker.worker_id,
                        "event_id": entry.event_id,
                        "status": "delivery_replayed",
                        "duplicate_fan_in": bool(getattr(recorded, "duplicate", False)),
                        "mutant_event_ids": list(mutant_event_ids),
                        "published_engine_artifacts": list(published_artifacts),
                        "completed_mutants": len(receipt.executions),
                        "lease_released": released,
                    }
                )
            except Exception as exc:
                actions.append(
                    {
                        "campaign_id": campaign_id.value,
                        "worker_id": worker.worker_id,
                        "event_id": entry.event_id,
                        "status": "delivery_unresolved",
                        "spool_path": str(spool.root),
                        "error": str(exc),
                    }
                )
                raise
    return tuple(actions)
def replay_finalization(
    *,
    database_path: Path,
    store: SQLiteMutationStore,
    service: MutationCampaignService,
    campaign_id: CampaignId,
) -> tuple[dict[str, Any], ...]:
    # Resume publication, registry commit, and completion from the durable finalization intent.
    campaign = _require(
        store.get_campaign(campaign_id),
        operation=f"load campaign {campaign_id.value} for finalization recovery",
    )
    if campaign is None:
        raise StartupRecoveryError(
            f"finalization recovery campaign is missing: campaign_id={campaign_id.value}"
        )
    intent = _require(
        store.get_finalization_intent(campaign_id),
        operation=f"load finalization intent for {campaign_id.value}",
    )
    actions: list[dict[str, Any]] = []
    if intent is None:
        removed = cleanup_finalization_staging(database_path, keep_intent_id=None)
        if campaign.status in {CampaignState.READY_TO_COMMIT, CampaignState.COMPLETED}:
            raise StartupRecoveryError(
                "terminal finalization state has no authoritative intent: "
                f"campaign_id={campaign_id.value}; status={campaign.status.value}; "
                f"database={Path(database_path).resolve()}"
            )
        if removed:
            actions.append(
                {
                    "campaign_id": campaign_id.value,
                    "status": "uncommitted_finalization_staging_removed",
                    "paths": list(removed),
                }
            )
        return tuple(actions)
    expected_intent_status = {
        CampaignState.MATERIALIZING: "created",
        CampaignState.READY_TO_COMMIT: "registered",
        CampaignState.COMPLETED: "completed",
    }.get(campaign.status)
    if expected_intent_status is None or intent.status != expected_intent_status:
        raise StartupRecoveryError(
            "campaign and finalization intent states are not an atomic recovery pair: "
            f"campaign_id={campaign_id.value}; campaign_status={campaign.status.value}; "
            f"intent_id={intent.intent_id}; intent_status={intent.status}; "
            f"expected_intent_status={expected_intent_status}; database={Path(database_path).resolve()}"
        )
    published = publish_finalization_intent(database_path, intent)
    actions.append(
        {
            "campaign_id": campaign_id.value,
            "intent_id": intent.intent_id,
            "status": "finalization_artifacts_verified",
            "artifact_count": len(published),
        }
    )
    if campaign.status == CampaignState.MATERIALIZING and intent.status == "created":
        campaign = _require(
            service.register_finalization(
                "effect.finalization.registry",
                campaign_id,
                intent,
                published,
                expected_revision=campaign.revision_number,
            ),
            operation=f"register finalization artifacts for {campaign_id.value}",
        )
        intent = _require(
            store.get_finalization_intent(campaign_id),
            operation=f"reload finalization intent for {campaign_id.value}",
        )
        actions.append(
            {
                "campaign_id": campaign_id.value,
                "intent_id": intent.intent_id,
                "status": "artifact_registry_replayed",
                "artifact_count": len(published),
            }
        )
    registry = _require(
        store.list_artifacts(campaign_id),
        operation=f"load artifact registry for {campaign_id.value}",
    )
    validate_registered_artifacts(database_path, intent, registry)
    if campaign.status == CampaignState.READY_TO_COMMIT:
        campaign = _require(
            service.complete_finalization(
                "effect.finalize",
                campaign_id,
                expected_revision=campaign.revision_number,
            ),
            operation=f"complete finalization for {campaign_id.value}",
        )
        intent = _require(
            store.get_finalization_intent(campaign_id),
            operation=f"reload completed finalization intent for {campaign_id.value}",
        )
        actions.append(
            {
                "campaign_id": campaign_id.value,
                "intent_id": intent.intent_id,
                "status": "finalization_completed",
                "campaign_revision": campaign.revision_number,
            }
        )
    if campaign.status == CampaignState.COMPLETED:
        if intent.status != "completed":
            raise StartupRecoveryError(
                "completed campaign has a non-completed finalization intent: "
                f"campaign_id={campaign_id.value}; intent_id={intent.intent_id}; "
                f"intent_status={intent.status}"
            )
        cleanup_finalization_staging(database_path, keep_intent_id=None)
    return tuple(actions)
def unfinished_campaign_configurations(database_path: Path) -> tuple[Any, ...]:
    # Load resumable campaign contracts without keeping the SQLite connection open during execution.
    store = SQLiteMutationStore(Path(database_path))
    try:
        campaigns = _require(
            store.list_campaigns(),
            operation=f"list campaigns in {database_path}",
        )
        return tuple(
            item.configuration
            for item in campaigns
            if item.status
            not in {CampaignState.COMPLETED, CampaignState.FAILED, CampaignState.CANCELLED}
        )
    finally:
        store.close()
