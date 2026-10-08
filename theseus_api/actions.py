"""Safe operator actions and bounded diagnostics for the transport-neutral local API."""
from __future__ import annotations
import base64
import hashlib
import sqlite3
from datetime import datetime, timezone
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any
from .contracts import (
    ActionReceiptDto,
    ApiError,
    ApiFailed,
    ApiOutcome,
    ApiRejected,
    ApiSuccess,
    ArtifactChunkDto,
    DiagnosticBlockerDto,
    DiagnosticsDto,
    HealthDto,
    ProgressDto,
    QuarantineInspectionDto,
    QuarantineRecordDto,
)
from .streaming import LocalEventStream
LOCAL_ACTION_MAX_CHUNK_BYTES = 1024 * 1024
LOCAL_DIAGNOSTICS_MAX_LIMIT = 200
class _ActionRejected(RuntimeError):
    """Internal expected action rejection."""
    def __init__(self, code: str, message: str, details: Mapping[str, str | int | bool | None] | None = None) -> None:
        # Retain stable scalar rejection details without preserving an exception object.
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = dict(details or {})
class _ActionFailed(RuntimeError):
    """Internal technical action failure."""
    def __init__(
        self,
        code: str,
        message: str,
        *,
        retriable: bool = False,
        details: Mapping[str, str | int | bool | None] | None = None,
    ) -> None:
        # Retain only stable failure data and bounded scalar diagnostics.
        super().__init__(message)
        self.code = code
        self.message = message
        self.retriable = retriable
        self.details = dict(details or {})
def _required_identifier(value: object, field_name: str) -> str:
    # Normalize one required action identity before opening durable state.
    if not isinstance(value, str) or not value.strip():
        raise _ActionRejected("invalid_request", f"{field_name} must be a non-empty string", {"field": field_name})
    return value.strip()
def _validate_limit(value: int, *, maximum: int, field_name: str = "limit") -> int:
    # Reject booleans and values outside one explicit action read bound.
    if isinstance(value, bool) or not isinstance(value, int) or value < 1 or value > maximum:
        raise _ActionRejected("invalid_limit", f"{field_name} must be between 1 and {maximum}")
    return value
def _identifier_value(value: object) -> str:
    # Convert one typed identifier or scalar identity to its stable string form.
    inner = getattr(value, "value", value)
    return str(inner)
def _enum_value(value: object) -> str:
    # Convert one enum-like domain status without returning the enum object itself.
    inner = getattr(value, "value", value)
    return str(inner)
def _revision(value: object) -> int:
    # Read one aggregate revision and reject malformed domain values fail closed.
    revision = getattr(value, "revision_number", None)
    if isinstance(revision, bool) or not isinstance(revision, int):
        raise _ActionFailed("boundary_corrupted", "authoritative boundary returned an invalid revision")
    return revision
def _outcome_kind(outcome: object) -> str:
    # Identify the stable Gallifrey outcome variant without importing it at module load time.
    return type(outcome).__name__.lower()
def _require_success(outcome: object) -> tuple[object, bool]:
    # Convert Gallifrey outcomes into internal action control flow with sanitized messages.
    kind = _outcome_kind(outcome)
    if kind == "success":
        return getattr(outcome, "value"), bool(getattr(outcome, "duplicate", False))
    code = str(getattr(outcome, "code", "action_rejected"))
    details = getattr(outcome, "details", {})
    safe_details = {
        str(key): item
        for key, item in (details.items() if isinstance(details, Mapping) else ())
        if item is None or isinstance(item, (str, int, bool))
    }
    if kind in {"rejected", "cancelled"}:
        raise _ActionRejected(code, "operator action was rejected", safe_details)
    if kind == "failed":
        raise _ActionFailed(code, "operator action failed", retriable=bool(getattr(outcome, "retriable", False)))
    raise _ActionFailed("boundary_corrupted", "authoritative boundary returned an unknown outcome")
def _action_receipt(value: object) -> ActionReceiptDto:
    # Convert one durable operator action into its stable public receipt or replay its failure.
    status = _enum_value(getattr(value, "status", ""))
    result = getattr(value, "result", {})
    result_mapping = dict(result) if isinstance(result, Mapping) else {}
    error_code = getattr(value, "error_code", None)
    safe_details = {
        str(key): item
        for key, item in result_mapping.items()
        if item is None or isinstance(item, (str, int, bool))
    }
    if status == "rejected":
        raise _ActionRejected(str(error_code or "action_rejected"), "operator action was rejected", safe_details)
    if status == "failed":
        raise _ActionFailed(
            str(error_code or "operator_action_failed"),
            "operator action failed",
            retriable=bool(getattr(value, "retriable", False)),
            details=safe_details,
        )
    campaign_revision = result_mapping.get("campaign_revision", getattr(value, "expected_revision", 0))
    shard_revisions = result_mapping.get("shard_revisions", {})
    shard_id = result_mapping.get("shard_id")
    shard_revision = None
    if isinstance(shard_revisions, Mapping) and isinstance(shard_id, str):
        raw_revision = shard_revisions.get(shard_id)
        shard_revision = int(raw_revision) if isinstance(raw_revision, int) and not isinstance(raw_revision, bool) else None
    raw_shard_ids = result_mapping.get("shard_ids", ())
    shard_ids = tuple(str(item) for item in raw_shard_ids) if isinstance(raw_shard_ids, (list, tuple)) else ()
    return ActionReceiptDto(
        action_id=str(getattr(value, "action_id", "")),
        action=str(getattr(value, "action_type", "")),
        campaign_id=_identifier_value(getattr(value, "campaign_id", "")),
        status=status,
        campaign_revision=int(campaign_revision),
        shard_id=str(shard_id) if isinstance(shard_id, str) else None,
        shard_revision=shard_revision,
        shard_ids=shard_ids,
    )
def _action_is_recent(value: object, *, seconds: float = 10.0) -> bool:
    # Treat a just-started action without PID metadata as an in-flight spawn race.
    raw = str(getattr(value, "updated_at", "") or "")
    if not raw:
        return False
    normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        updated = datetime.fromisoformat(normalized)
    except ValueError:
        return False
    if updated.tzinfo is None:
        return False
    return (datetime.now(timezone.utc) - updated.astimezone(timezone.utc)).total_seconds() < seconds
class LocalOperatorActions:
    """Operator boundary that delegates every mutation to existing authoritative services."""
    def __init__(
        self,
        campaign_database: str | Path,
        *,
        knowledge_database: str | Path | None = None,
        statistics_database: str | Path | None = None,
        mutation_context_factory: Callable[[Path], tuple[object, object]] | None = None,
        campaign_id_factory: Callable[[str], object] | None = None,
        shard_id_factory: Callable[[str], object] | None = None,
        cancel_signal: Callable[[object], object] | None = None,
        knowledge_store_factory: Callable[[Path], object] | None = None,
        statistics_event_store_factory: Callable[[Path], object] | None = None,
        statistics_projection_store_factory: Callable[[object], object] | None = None,
        artifact_validator: Callable[[Path, object, tuple[object, ...]], None] | None = None,
        recovery_path_resolver: Callable[[Path], Path] | None = None,
        resume_executor: Callable[[Path, str], object] | None = None,
        reconcile_executor: Callable[[Path, str], object] | None = None,
        recover_executor: Callable[[Path, str], object] | None = None,
        launch_configuration: object | None = None,
        launch_executor: Callable[[Path, str, str], Mapping[str, Any]] | None = None,
        launch_liveness: Callable[[Mapping[str, Any]], bool] | None = None,
    ) -> None:
        # Resolve durable roots and retain injectable authoritative adapters for isolated tests.
        self._campaign_database = Path(campaign_database).expanduser().resolve()
        self._knowledge_database = Path(knowledge_database).expanduser().resolve() if knowledge_database else None
        self._statistics_database = Path(statistics_database).expanduser().resolve() if statistics_database else None
        self._mutation_context_factory = mutation_context_factory
        self._campaign_id_factory = campaign_id_factory
        self._shard_id_factory = shard_id_factory
        self._cancel_signal = cancel_signal
        self._knowledge_store_factory = knowledge_store_factory
        self._statistics_event_store_factory = statistics_event_store_factory
        self._statistics_projection_store_factory = statistics_projection_store_factory
        self._artifact_validator = artifact_validator
        self._recovery_path_resolver = recovery_path_resolver
        self._resume_executor = resume_executor
        self._reconcile_executor = reconcile_executor
        self._recover_executor = recover_executor
        self._launch_configuration = launch_configuration
        self._launch_executor = launch_executor
        self._launch_liveness = launch_liveness
    @classmethod
    def for_configuration(
        cls,
        configuration: object,
        **kwargs: Any,
    ) -> "LocalOperatorActions":
        # Bind launch actions to the canonical database derived from one public configuration.
        from theseus_contracts import CampaignConfiguration
        from theseus_local.coordinator import LocalCampaignCoordinator
        resolved = (
            configuration
            if isinstance(configuration, CampaignConfiguration)
            else CampaignConfiguration.from_dict(configuration)
        )
        return cls(
            LocalCampaignCoordinator.campaign_database_path(resolved),
            launch_configuration=resolved,
            **kwargs,
        )
    @staticmethod
    def _configuration(value: object) -> object:
        # Restore one public campaign configuration without accepting private runtime objects.
        from theseus_contracts import CampaignConfiguration
        if isinstance(value, CampaignConfiguration):
            return value
        if isinstance(value, Mapping):
            return CampaignConfiguration.from_dict(value)
        raise _ActionRejected("invalid_request", "configuration must be a campaign object")
    def _spawn_campaign(self, campaign_id: str, action_id: str) -> Mapping[str, Any]:
        # Start the private detached worker through an injectable production boundary.
        if self._launch_executor is not None:
            return self._launch_executor(self._campaign_database, campaign_id, action_id)
        from theseus_local.launcher import spawn_campaign_action
        return spawn_campaign_action(self._campaign_database, campaign_id, action_id)
    def _launch_is_alive(self, metadata: Mapping[str, Any]) -> bool:
        # Verify one recorded launch process through the platform-specific process fence.
        if self._launch_liveness is not None:
            return bool(self._launch_liveness(metadata))
        from theseus_local.launcher import launch_process_is_alive
        return launch_process_is_alive(metadata)
    def _execute(self, operation: Callable[[], Any]) -> ApiOutcome[Any]:
        # Convert action, filesystem and persistence failures into stable API outcomes.
        try:
            return ApiSuccess(operation())
        except _ActionRejected as exc:
            return ApiRejected(ApiError(exc.code, exc.message, details=exc.details))
        except _ActionFailed as exc:
            return ApiFailed(ApiError(exc.code, exc.message, retriable=exc.retriable, details=exc.details))
        except sqlite3.DatabaseError:
            return ApiFailed(ApiError("store_action_failed", "campaign store operation failed", retriable=True))
        except (OSError, UnicodeError):
            return ApiFailed(ApiError("state_action_failed", "durable local state operation failed", retriable=True))
        except Exception:
            return ApiFailed(ApiError("operator_action_failed", "operator action failed"))
    def _open_mutation(self) -> tuple[object, object]:
        # Open the authoritative mutation store and application service without direct SQL writes.
        if self._mutation_context_factory is not None:
            return self._mutation_context_factory(self._campaign_database)
        from gallifrey_mutation import MutationCampaignService, SQLiteMutationStore
        store = SQLiteMutationStore(self._campaign_database)
        return store, MutationCampaignService(store)
    def _campaign_identifier(self, value: str) -> object:
        # Construct the public typed campaign identifier or its injected equivalent.
        if self._campaign_id_factory is not None:
            return self._campaign_id_factory(value)
        from theseus_contracts import CampaignId
        return CampaignId(value)
    def _shard_identifier(self, value: str) -> object:
        # Construct the public typed shard identifier or its injected equivalent.
        if self._shard_id_factory is not None:
            return self._shard_id_factory(value)
        from theseus_contracts import ShardId
        return ShardId(value)
    def _signal_cancel(self, configuration: object) -> None:
        # Publish the existing coordinator cancellation marker after the durable state transition.
        if self._cancel_signal is not None:
            self._cancel_signal(configuration)
            return
        from theseus_local.coordinator import LocalCampaignCoordinator
        LocalCampaignCoordinator.request_cancel(configuration)
    def _open_knowledge(self, path: Path) -> object:
        # Open knowledge through its public store boundary or an injected adapter.
        if self._knowledge_store_factory is not None:
            return self._knowledge_store_factory(path)
        from theseus_knowledge import KnowledgePlaneStore
        return KnowledgePlaneStore(path)
    def _open_statistics_event_store(self, path: Path) -> object:
        # Open canonical statistics through the existing public store boundary.
        if self._statistics_event_store_factory is not None:
            return self._statistics_event_store_factory(path)
        from theseus_statistics import StatisticsEventStore
        return StatisticsEventStore(path)
    def _open_statistics_projection_store(self, event_store: object) -> object:
        # Open statistics projections through their existing public boundary.
        if self._statistics_projection_store_factory is not None:
            return self._statistics_projection_store_factory(event_store)
        from theseus_statistics import StatisticsProjectionStore
        return StatisticsProjectionStore(event_store)
    def _validate_artifacts(self, intent: object, registry: tuple[object, ...]) -> None:
        # Validate the exact registry and content hashes through the existing finalization boundary.
        if self._artifact_validator is not None:
            self._artifact_validator(self._campaign_database, intent, registry)
            return
        from theseus_local.finalization import validate_registered_artifacts
        validate_registered_artifacts(self._campaign_database, intent, registry)
    def _resolve_knowledge_path(self, project_id: str) -> Path:
        # Resolve current and legacy knowledge locations without exposing either path.
        if self._knowledge_database is not None:
            return self._knowledge_database
        state_root = self._campaign_database.parent.parent.parent
        current = state_root / "knowledge" / f"{project_id}.sqlite3"
        legacy = self._campaign_database.parent / "knowledge.sqlite3"
        return current if current.is_file() or not legacy.is_file() else legacy
    def _record_action_failure(
        self,
        action_id: str,
        *,
        code: str,
        retriable: bool,
        rejected: bool,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        # Persist one sanitized terminal action failure without hiding the original API outcome.
        store, service = self._open_mutation()
        try:
            if rejected:
                service.reject_operator_action(action_id, code, result=details)
            else:
                service.fail_operator_action(action_id, code, retriable=retriable, result=details)
        finally:
            close = getattr(store, "close", None)
            if callable(close):
                close()
    def create_campaign(
        self,
        action_id: str,
        configuration: object | None = None,
    ) -> ApiOutcome[ActionReceiptDto]:
        # Create one durable campaign and terminal replay receipt without starting execution.
        def operation() -> ActionReceiptDto:
            # Commit campaign identity and create receipt atomically in its canonical control database.
            current_action = _required_identifier(action_id, "action_id")
            resolved = self._configuration(configuration or self._launch_configuration)
            from theseus_local.coordinator import LocalCampaignCoordinator
            expected_database = LocalCampaignCoordinator.campaign_database_path(resolved)
            if expected_database != self._campaign_database:
                raise _ActionRejected(
                    "campaign_database_mismatch",
                    "configuration resolves to another campaign database",
                )
            store, service = self._open_mutation()
            try:
                action, _ = _require_success(
                    service.create_campaign_action(current_action, resolved)
                )
                return _action_receipt(action)
            finally:
                close = getattr(store, "close", None)
                if callable(close):
                    close()
        return self._execute(operation)
    def start_campaign(
        self,
        action_id: str,
        campaign_id: str,
        *,
        expected_revision: int,
    ) -> ApiOutcome[ActionReceiptDto]:
        # Start one campaign in a detached coordinator process and return before completion.
        def operation() -> ActionReceiptDto:
            # Claim one durable start action and spawn at most one live process per exact request.
            current_action = _required_identifier(action_id, "action_id")
            current_campaign_id = _required_identifier(campaign_id, "campaign_id")
            if isinstance(expected_revision, bool) or not isinstance(expected_revision, int) or expected_revision < 0:
                raise _ActionRejected("invalid_revision", "expected_revision must be a non-negative integer")
            store, service = self._open_mutation()
            try:
                requested, _ = _require_success(
                    service.request_operator_action(
                        current_action,
                        "start_campaign",
                        self._campaign_identifier(current_campaign_id),
                        expected_revision=expected_revision,
                    )
                )
                status = _enum_value(getattr(requested, "status", ""))
                if status in {"completed", "rejected", "failed"}:
                    return _action_receipt(requested)
                started, duplicate = _require_success(service.start_operator_action(current_action))
                metadata = getattr(started, "result", {})
                safe_metadata = dict(metadata) if isinstance(metadata, Mapping) else {}
                if duplicate:
                    if self._launch_is_alive(safe_metadata) or (not safe_metadata and _action_is_recent(started)):
                        return _action_receipt(started)
            finally:
                close = getattr(store, "close", None)
                if callable(close):
                    close()
            try:
                launch_metadata = dict(self._spawn_campaign(current_campaign_id, current_action))
            except Exception as exc:
                details = {"stage": "spawn", "exception_type": type(exc).__name__}
                self._record_action_failure(
                    current_action,
                    code="campaign_launch_failed",
                    retriable=True,
                    rejected=False,
                    details=details,
                )
                raise _ActionFailed(
                    "campaign_launch_failed",
                    "campaign launch failed",
                    retriable=True,
                    details=details,
                ) from exc
            process_id = launch_metadata.get("process_id")
            process_birth_token = launch_metadata.get("process_birth_token")
            if (
                isinstance(process_id, bool)
                or not isinstance(process_id, int)
                or process_id <= 0
                or not isinstance(process_birth_token, str)
                or not process_birth_token
            ):
                self._record_action_failure(
                    current_action,
                    code="launch_boundary_corrupted",
                    retriable=True,
                    rejected=False,
                )
                raise _ActionFailed(
                    "launch_boundary_corrupted",
                    "campaign launch boundary returned invalid process identity",
                    retriable=True,
                )
            bounded_metadata = {
                "campaign_revision": expected_revision,
                "process_id": process_id,
                "process_birth_token": process_birth_token,
            }
            store, service = self._open_mutation()
            try:
                running, _ = _require_success(
                    service.update_running_operator_action(current_action, bounded_metadata)
                )
                return _action_receipt(running)
            finally:
                close = getattr(store, "close", None)
                if callable(close):
                    close()
        return self._execute(operation)
    def start(
        self,
        action_id: str,
        campaign_id: str,
        *,
        expected_revision: int,
    ) -> ApiOutcome[ActionReceiptDto]:
        # Expose the concise UI action name through the detached start boundary.
        return self.start_campaign(
            action_id,
            campaign_id,
            expected_revision=expected_revision,
        )
    def cancel_campaign(
        self,
        action_id: str,
        campaign_id: str,
        *,
        expected_revision: int,
    ) -> ApiOutcome[ActionReceiptDto]:
        # Cancel one campaign through a durable operator receipt and the existing mutation effect.
        def operation() -> ActionReceiptDto:
            # Replay the exact action result and repair a crash between cancellation and receipt completion.
            current_action = _required_identifier(action_id, "action_id")
            current_campaign_id = _required_identifier(campaign_id, "campaign_id")
            if isinstance(expected_revision, bool) or not isinstance(expected_revision, int) or expected_revision < 0:
                raise _ActionRejected("invalid_revision", "expected_revision must be a non-negative integer")
            store, service = self._open_mutation()
            try:
                requested, _ = _require_success(
                    service.request_operator_action(
                        current_action,
                        "cancel",
                        self._campaign_identifier(current_campaign_id),
                        expected_revision=expected_revision,
                    )
                )
                if _enum_value(getattr(requested, "status", "")) in {"completed", "rejected", "failed"}:
                    return _action_receipt(requested)
                _require_success(service.start_operator_action(current_action))
                try:
                    campaign, _ = _require_success(
                        service.cancel(
                            current_action,
                            self._campaign_identifier(current_campaign_id),
                            expected_revision=expected_revision,
                        )
                    )
                except _ActionRejected as exc:
                    rejected, _ = _require_success(
                        service.reject_operator_action(current_action, exc.code, result=exc.details)
                    )
                    return _action_receipt(rejected)
                except _ActionFailed as exc:
                    failed, _ = _require_success(
                        service.fail_operator_action(current_action, exc.code, retriable=exc.retriable)
                    )
                    return _action_receipt(failed)
                self._signal_cancel(getattr(campaign, "configuration"))
                completed, _ = _require_success(
                    service.complete_operator_action(
                        current_action,
                        {
                            "campaign_revision": _revision(campaign),
                            "campaign_status": _enum_value(getattr(campaign, "status", "cancelled")),
                        },
                    )
                )
                return _action_receipt(completed)
            finally:
                close = getattr(store, "close", None)
                if callable(close):
                    close()
        return self._execute(operation)
    def retry_shard(
        self,
        action_id: str,
        campaign_id: str,
        shard_id: str,
        *,
        expected_campaign_revision: int,
        expected_shard_revision: int,
    ) -> ApiOutcome[ActionReceiptDto]:
        # Requeue one failed or orphaned shard through a durable operator receipt.
        def operation() -> ActionReceiptDto:
            # Fence campaign and shard revisions before applying the existing atomic retry effect.
            current_action = _required_identifier(action_id, "action_id")
            current_campaign_id = _required_identifier(campaign_id, "campaign_id")
            current_shard_id = _required_identifier(shard_id, "shard_id")
            for name, value in (
                ("expected_campaign_revision", expected_campaign_revision),
                ("expected_shard_revision", expected_shard_revision),
            ):
                if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                    raise _ActionRejected("invalid_revision", f"{name} must be a non-negative integer")
            store, service = self._open_mutation()
            try:
                typed_campaign_id = self._campaign_identifier(current_campaign_id)
                typed_shard_id = self._shard_identifier(current_shard_id)
                requested, _ = _require_success(
                    service.request_operator_action(
                        current_action,
                        "retry_shard",
                        typed_campaign_id,
                        expected_revision=expected_campaign_revision,
                        parameters={
                            "shard_id": current_shard_id,
                            "expected_shard_revision": expected_shard_revision,
                        },
                    )
                )
                if _enum_value(getattr(requested, "status", "")) in {"completed", "rejected", "failed"}:
                    return _action_receipt(requested)
                _require_success(service.start_operator_action(current_action))
                shard, _ = _require_success(store.get_shard(typed_shard_id))
                if shard is None:
                    rejected, _ = _require_success(
                        service.reject_operator_action(
                            current_action,
                            "not_found",
                            result={"shard_id": current_shard_id},
                        )
                    )
                    return _action_receipt(rejected)
                if _identifier_value(getattr(shard, "campaign_id", "")) != current_campaign_id:
                    rejected, _ = _require_success(
                        service.reject_operator_action(current_action, "shard_campaign_mismatch")
                    )
                    return _action_receipt(rejected)
                try:
                    retried, _ = _require_success(
                        service.retry_shard(
                            current_action,
                            typed_shard_id,
                            expected_revision=expected_shard_revision,
                        )
                    )
                except _ActionRejected as exc:
                    rejected, _ = _require_success(
                        service.reject_operator_action(current_action, exc.code, result=exc.details)
                    )
                    return _action_receipt(rejected)
                completed, _ = _require_success(
                    service.complete_operator_action(
                        current_action,
                        {
                            "campaign_revision": expected_campaign_revision,
                            "shard_id": current_shard_id,
                            "shard_ids": [current_shard_id],
                            "shard_revisions": {current_shard_id: _revision(retried)},
                        },
                    )
                )
                return _action_receipt(completed)
            finally:
                close = getattr(store, "close", None)
                if callable(close):
                    close()
        return self._execute(operation)
    def retry_campaign(
        self,
        action_id: str,
        campaign_id: str,
        *,
        expected_revision: int,
    ) -> ApiOutcome[ActionReceiptDto]:
        # Atomically requeue every retryable shard while preserving complete executions and topology.
        def operation() -> ActionReceiptDto:
            # Delegate the multi-shard transaction to the authoritative mutation service.
            current_action = _required_identifier(action_id, "action_id")
            current_campaign_id = _required_identifier(campaign_id, "campaign_id")
            if isinstance(expected_revision, bool) or not isinstance(expected_revision, int) or expected_revision < 0:
                raise _ActionRejected("invalid_revision", "expected_revision must be a non-negative integer")
            store, service = self._open_mutation()
            try:
                action, _ = _require_success(
                    service.retry_campaign(
                        current_action,
                        self._campaign_identifier(current_campaign_id),
                        expected_revision=expected_revision,
                    )
                )
                return _action_receipt(action)
            finally:
                close = getattr(store, "close", None)
                if callable(close):
                    close()
        return self._execute(operation)
    def cancel(
        self,
        action_id: str,
        campaign_id: str,
        *,
        expected_revision: int,
    ) -> ApiOutcome[ActionReceiptDto]:
        # Expose the roadmap action name through the durable campaign cancellation boundary.
        return self.cancel_campaign(action_id, campaign_id, expected_revision=expected_revision)
    def retry(
        self,
        action_id: str,
        campaign_id: str,
        shard_id: str | None = None,
        *,
        expected_campaign_revision: int,
        expected_shard_revision: int | None = None,
    ) -> ApiOutcome[ActionReceiptDto]:
        # Route an explicit shard retry or the campaign-wide retry authority without ambiguity.
        if shard_id is None:
            return self.retry_campaign(
                action_id,
                campaign_id,
                expected_revision=expected_campaign_revision,
            )
        if expected_shard_revision is None:
            return ApiRejected(ApiError("invalid_revision", "expected_shard_revision is required for shard retry"))
        return self.retry_shard(
            action_id,
            campaign_id,
            shard_id,
            expected_campaign_revision=expected_campaign_revision,
            expected_shard_revision=expected_shard_revision,
        )
    def resume_campaign(
        self,
        action_id: str,
        campaign_id: str,
        *,
        expected_revision: int,
    ) -> ApiOutcome[ActionReceiptDto]:
        # Resume one exact campaign through a durable receipt and the existing coordinator authority.
        def operation() -> ActionReceiptDto:
            # Recover a requested or running action and never recalculate the immutable campaign plan.
            current_action = _required_identifier(action_id, "action_id")
            current_campaign_id = _required_identifier(campaign_id, "campaign_id")
            if isinstance(expected_revision, bool) or not isinstance(expected_revision, int) or expected_revision < 0:
                raise _ActionRejected("invalid_revision", "expected_revision must be a non-negative integer")
            store, service = self._open_mutation()
            try:
                requested, _ = _require_success(
                    service.request_operator_action(
                        current_action,
                        "resume",
                        self._campaign_identifier(current_campaign_id),
                        expected_revision=expected_revision,
                    )
                )
                if _enum_value(getattr(requested, "status", "")) in {"completed", "rejected", "failed"}:
                    return _action_receipt(requested)
                _require_success(service.start_operator_action(current_action))
            finally:
                close = getattr(store, "close", None)
                if callable(close):
                    close()
            try:
                if self._resume_executor is not None:
                    result = self._resume_executor(self._campaign_database, current_campaign_id)
                else:
                    from theseus_local.coordinator import LocalCampaignCoordinator
                    result = LocalCampaignCoordinator.resume_campaign(
                        self._campaign_database,
                        current_campaign_id,
                    )
            except Exception as exc:
                code = str(exc).split(":", 1)[0].strip()
                if type(exc).__name__ == "StartupRecoveryBusy":
                    raise _ActionRejected("action_in_progress", "campaign resume is already running") from exc
                if code in {"campaign_not_found", "campaign_not_resumable", "campaign_identity_mismatch"}:
                    self._record_action_failure(
                        current_action,
                        code=code,
                        retriable=False,
                        rejected=True,
                    )
                    raise _ActionRejected(code, "campaign resume was rejected") from exc
                self._record_action_failure(
                    current_action,
                    code="resume_failed",
                    retriable=True,
                    rejected=False,
                )
                raise _ActionFailed("resume_failed", "campaign resume failed", retriable=True) from exc
            campaign = getattr(result, "campaign", result)
            campaign_revision = _revision(campaign)
            campaign_status = _enum_value(getattr(campaign, "status", "unknown"))
            store, service = self._open_mutation()
            try:
                completed, _ = _require_success(
                    service.complete_operator_action(
                        current_action,
                        {
                            "campaign_revision": campaign_revision,
                            "campaign_status": campaign_status,
                        },
                    )
                )
                return _action_receipt(completed)
            finally:
                close = getattr(store, "close", None)
                if callable(close):
                    close()
        return self._execute(operation)
    def resume(
        self,
        action_id: str,
        campaign_id: str,
        *,
        expected_revision: int,
    ) -> ApiOutcome[ActionReceiptDto]:
        # Expose the roadmap action name through the durable single-campaign resume boundary.
        return self.resume_campaign(action_id, campaign_id, expected_revision=expected_revision)
    def get_recovery_action(
        self,
        campaign_id: str,
        action_id: str,
    ) -> ApiOutcome[ActionReceiptDto]:
        # Read one durable recovery receipt without changing its lifecycle or aggregate state.
        def operation() -> ActionReceiptDto:
            # Fence the receipt by campaign and recovery action type before public serialization.
            current_campaign_id = _required_identifier(campaign_id, "campaign_id")
            current_action = _required_identifier(action_id, "action_id")
            store, service = self._open_mutation()
            try:
                action, _ = _require_success(service.get_operator_action(current_action))
                if action is None:
                    raise _ActionRejected("action_not_found", "recovery action does not exist")
                action_campaign_id = _identifier_value(getattr(action, "campaign_id", ""))
                action_type = str(getattr(action, "action_type", ""))
                if (
                    action_campaign_id != current_campaign_id
                    or action_type not in {"reconcile_campaign", "recover_campaign"}
                ):
                    raise _ActionRejected("action_not_found", "recovery action does not exist")
                return _action_receipt(action)
            finally:
                close = getattr(store, "close", None)
                if callable(close):
                    close()
        return self._execute(operation)
    def reconcile_campaign(
        self,
        action_id: str,
        campaign_id: str,
        *,
        expected_revision: int,
    ) -> ApiOutcome[ActionReceiptDto]:
        # Reconcile one campaign through a durable receipt without resuming execution.
        def operation() -> ActionReceiptDto:
            # Persist requested and running states around the existing startup-recovery authority.
            current_action = _required_identifier(action_id, "action_id")
            current_campaign_id = _required_identifier(campaign_id, "campaign_id")
            if isinstance(expected_revision, bool) or not isinstance(expected_revision, int) or expected_revision < 0:
                raise _ActionRejected("invalid_revision", "expected_revision must be a non-negative integer")
            store, service = self._open_mutation()
            try:
                requested, _ = _require_success(
                    service.request_operator_action(
                        current_action,
                        "reconcile_campaign",
                        self._campaign_identifier(current_campaign_id),
                        expected_revision=expected_revision,
                    )
                )
                if _enum_value(getattr(requested, "status", "")) in {"completed", "rejected", "failed"}:
                    return _action_receipt(requested)
                _require_success(service.start_operator_action(current_action))
            finally:
                close = getattr(store, "close", None)
                if callable(close):
                    close()
            try:
                if self._reconcile_executor is not None:
                    result = self._reconcile_executor(self._campaign_database, current_campaign_id)
                else:
                    from theseus_local.coordinator import LocalCampaignCoordinator
                    result = LocalCampaignCoordinator.reconcile_campaign(self._campaign_database, current_campaign_id)
            except Exception as exc:
                code = str(exc).split(":", 1)[0].strip()
                if type(exc).__name__ == "StartupRecoveryBusy":
                    raise _ActionRejected("action_in_progress", "campaign reconciliation is already running") from exc
                rejected = code in {"campaign_not_found", "campaign_identity_mismatch", "campaign_not_reconcilable"}
                stable_code = code if rejected else "reconcile_failed"
                self._record_action_failure(
                    current_action,
                    code=stable_code,
                    retriable=not rejected,
                    rejected=rejected,
                )
                if rejected:
                    raise _ActionRejected(stable_code, "campaign reconciliation was rejected") from exc
                raise _ActionFailed("reconcile_failed", "campaign reconciliation failed", retriable=True) from exc
            actions = tuple(result) if isinstance(result, (list, tuple)) else ()
            store, service = self._open_mutation()
            try:
                campaign, _ = _require_success(service.get_campaign(self._campaign_identifier(current_campaign_id)))
                completed, _ = _require_success(
                    service.complete_operator_action(
                        current_action,
                        {
                            "campaign_revision": _revision(campaign),
                            "campaign_status": _enum_value(getattr(campaign, "status", "unknown")),
                            "action_count": len(actions),
                        },
                    )
                )
                return _action_receipt(completed)
            finally:
                close = getattr(store, "close", None)
                if callable(close):
                    close()
        return self._execute(operation)
    def recover_campaign(
        self,
        action_id: str,
        campaign_id: str,
        *,
        expected_revision: int,
    ) -> ApiOutcome[ActionReceiptDto]:
        # Reconcile and resume one campaign under a single durable operator identity.
        def operation() -> ActionReceiptDto:
            # Replay a running receipt safely after crashes and preserve immutable plan authority.
            current_action = _required_identifier(action_id, "action_id")
            current_campaign_id = _required_identifier(campaign_id, "campaign_id")
            if isinstance(expected_revision, bool) or not isinstance(expected_revision, int) or expected_revision < 0:
                raise _ActionRejected("invalid_revision", "expected_revision must be a non-negative integer")
            store, service = self._open_mutation()
            try:
                requested, _ = _require_success(
                    service.request_operator_action(
                        current_action,
                        "recover_campaign",
                        self._campaign_identifier(current_campaign_id),
                        expected_revision=expected_revision,
                    )
                )
                if _enum_value(getattr(requested, "status", "")) in {"completed", "rejected", "failed"}:
                    return _action_receipt(requested)
                _require_success(service.start_operator_action(current_action))
            finally:
                close = getattr(store, "close", None)
                if callable(close):
                    close()
            try:
                if self._recover_executor is not None:
                    result = self._recover_executor(self._campaign_database, current_campaign_id)
                else:
                    from theseus_local.coordinator import LocalCampaignCoordinator
                    result = LocalCampaignCoordinator.recover_campaign(self._campaign_database, current_campaign_id)
            except Exception as exc:
                code = str(exc).split(":", 1)[0].strip()
                if type(exc).__name__ == "StartupRecoveryBusy":
                    raise _ActionRejected("action_in_progress", "campaign recovery is already running") from exc
                rejected = code in {"campaign_not_found", "campaign_identity_mismatch", "campaign_not_resumable"}
                stable_code = code if rejected else "recover_failed"
                self._record_action_failure(
                    current_action,
                    code=stable_code,
                    retriable=not rejected,
                    rejected=rejected,
                )
                if rejected:
                    raise _ActionRejected(stable_code, "campaign recovery was rejected") from exc
                raise _ActionFailed("recover_failed", "campaign recovery failed", retriable=True) from exc
            campaign = getattr(result, "campaign", result)
            store, service = self._open_mutation()
            try:
                completed, _ = _require_success(
                    service.complete_operator_action(
                        current_action,
                        {
                            "campaign_revision": _revision(campaign),
                            "campaign_status": _enum_value(getattr(campaign, "status", "unknown")),
                        },
                    )
                )
                return _action_receipt(completed)
            finally:
                close = getattr(store, "close", None)
                if callable(close):
                    close()
        return self._execute(operation)
    def inspect_quarantine(
        self,
        campaign_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ApiOutcome[QuarantineInspectionDto]:
        # Return one bounded project-level quarantine snapshot through KnowledgePlaneStore.
        def operation() -> QuarantineInspectionDto:
            # Resolve project ownership from the campaign aggregate before opening knowledge state.
            current_campaign_id = _required_identifier(campaign_id, "campaign_id")
            page_limit = _validate_limit(limit, maximum=LOCAL_DIAGNOSTICS_MAX_LIMIT)
            store, service = self._open_mutation()
            try:
                campaign, _ = _require_success(service.get_campaign(self._campaign_identifier(current_campaign_id)))
                project_id = _identifier_value(getattr(campaign, "project_id", ""))
            finally:
                close = getattr(store, "close", None)
                if callable(close):
                    close()
            knowledge_path = self._resolve_knowledge_path(project_id)
            if not knowledge_path.is_file() and self._knowledge_store_factory is None:
                return QuarantineInspectionDto((), page_limit, False, None)
            knowledge = self._open_knowledge(knowledge_path)
            try:
                if hasattr(knowledge, "query_conflicts"):
                    try:
                        page = knowledge.query_conflicts(limit=page_limit, cursor=cursor)
                    except ValueError as exc:
                        raise _ActionRejected("invalid_cursor", "quarantine cursor is invalid") from exc
                    records = tuple(page.rows)
                    next_cursor = getattr(page, "next_cursor", None)
                    truncated = next_cursor is not None
                else:
                    if cursor is not None:
                        raise _ActionRejected("invalid_cursor", "quarantine cursor is not supported")
                    records = tuple(knowledge.list_conflicts(limit=page_limit + 1))
                    next_cursor = None
                    truncated = len(records) > page_limit
                def field(item: object, name: str, default: object = "") -> object:
                    # Read one conflict field from mapping or dataclass test adapters.
                    return item.get(name, default) if isinstance(item, Mapping) else getattr(item, name, default)
                items = tuple(
                    QuarantineRecordDto(
                        conflict_id=str(field(item, "conflict_id")),
                        conflict_type=str(field(item, "conflict_type")),
                        identity_type=str(field(item, "identity_type")),
                        identity_key=str(field(item, "identity_key")),
                        scope_id=(str(field(item, "scope_id")) if field(item, "scope_id", None) is not None else None),
                        reason=str(field(item, "reason")),
                        created_at=str(field(item, "created_at")),
                        status=str(field(item, "status", "quarantined")),
                    )
                    for item in records[:page_limit]
                )
                return QuarantineInspectionDto(items, page_limit, truncated, next_cursor)
            finally:
                close = getattr(knowledge, "close", None)
                if callable(close):
                    close()
        return self._execute(operation)
    def read_artifact(
        self,
        campaign_id: str,
        logical_key: str,
        *,
        offset: int,
        limit: int,
    ) -> ApiOutcome[ArtifactChunkDto]:
        # Read one verified content-addressed artifact chunk selected only by logical key.
        def operation() -> ArtifactChunkDto:
            # Validate registry identity and SHA-256 before reading a bounded byte range.
            current_campaign_id = _required_identifier(campaign_id, "campaign_id")
            current_logical_key = _required_identifier(logical_key, "logical_key")
            if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
                raise _ActionRejected("invalid_offset", "offset must be a non-negative integer")
            chunk_limit = _validate_limit(limit, maximum=LOCAL_ACTION_MAX_CHUNK_BYTES)
            store, service = self._open_mutation()
            try:
                typed_campaign_id = self._campaign_identifier(current_campaign_id)
                _require_success(service.get_campaign(typed_campaign_id))
                intent, _ = _require_success(store.get_finalization_intent(typed_campaign_id))
                if intent is None:
                    raise _ActionRejected("not_found", "finalization intent does not exist")
                registry_value, _ = _require_success(store.list_artifacts(typed_campaign_id))
                registry = tuple(registry_value)
                entry = next(
                    (item for item in registry if str(getattr(item, "logical_key", "")) == current_logical_key),
                    None,
                )
                if entry is None:
                    raise _ActionRejected("not_found", "artifact logical key does not exist", {"logical_key": current_logical_key})
                try:
                    self._validate_artifacts(intent, registry)
                except Exception as exc:
                    raise _ActionFailed("artifact_integrity_failed", "artifact integrity validation failed") from exc
                reports_root = self._campaign_database.parent.parent.resolve()
                serialized_path = str(getattr(entry, "content_path", ""))
                candidate = (reports_root / serialized_path.replace("\\", "/")).resolve()
                if candidate != reports_root and reports_root not in candidate.parents:
                    raise _ActionFailed("artifact_integrity_failed", "artifact registry path is unsafe")
                size_bytes = int(getattr(entry, "size_bytes", -1))
                expected_sha256 = str(getattr(entry, "content_sha256", ""))
                with candidate.open("rb") as handle:
                    digest = hashlib.sha256()
                    actual_size = 0
                    for block in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(block)
                        actual_size += len(block)
                    if digest.hexdigest() != expected_sha256 or actual_size != size_bytes:
                        raise _ActionFailed("artifact_integrity_failed", "artifact content identity does not match registry")
                    if offset > actual_size:
                        raise _ActionRejected("invalid_offset", "offset exceeds artifact size", {"size_bytes": actual_size})
                    handle.seek(offset)
                    data = handle.read(chunk_limit)
                next_offset = offset + len(data)
                return ArtifactChunkDto(
                    campaign_id=current_campaign_id,
                    logical_key=current_logical_key,
                    content_sha256=str(getattr(entry, "content_sha256", "")),
                    size_bytes=size_bytes,
                    offset=offset,
                    data_base64=base64.b64encode(data).decode("ascii"),
                    next_offset=next_offset if next_offset < size_bytes else None,
                    complete=next_offset >= size_bytes,
                )
            finally:
                close = getattr(store, "close", None)
                if callable(close):
                    close()
        return self._execute(operation)
    def health(self, *, max_rows: int = 10_000) -> ApiOutcome[HealthDto]:
        # Check bounded campaign, knowledge, statistics and recovery health without reading logs or environment values.
        def operation() -> HealthDto:
            # Build typed component blockers and never expose raw exception messages.
            row_limit = _validate_limit(max_rows, maximum=100_000, field_name="max_rows")
            blockers: list[DiagnosticBlockerDto] = []
            campaign_state = "unavailable"
            if self._campaign_database.is_file():
                connection = sqlite3.connect(self._campaign_database)
                try:
                    value = str(connection.execute("PRAGMA quick_check").fetchone()[0])
                    campaign_state = "healthy" if value == "ok" else "degraded"
                    if value != "ok":
                        blockers.append(DiagnosticBlockerDto("campaign_integrity", "campaign store integrity check failed", "error", "campaign"))
                finally:
                    connection.close()
            else:
                blockers.append(DiagnosticBlockerDto("campaign_unavailable", "campaign store is unavailable", "error", "campaign"))
            knowledge_state = "unavailable"
            knowledge_path = self._knowledge_database
            if knowledge_path is not None and (knowledge_path.is_file() or self._knowledge_store_factory is not None):
                knowledge = self._open_knowledge(knowledge_path)
                try:
                    report = knowledge.verify_integrity(max_rows=row_limit)
                    knowledge_state = "healthy" if bool(getattr(report, "healthy", False)) else "degraded"
                    if knowledge_state != "healthy":
                        blockers.append(DiagnosticBlockerDto("knowledge_integrity", "knowledge integrity check failed", "error", "knowledge"))
                finally:
                    close = getattr(knowledge, "close", None)
                    if callable(close):
                        close()
            statistics_state = "unavailable"
            statistics_path = self._statistics_database or self._campaign_database.parent / "statistics.sqlite3"
            if statistics_path.is_file() or self._statistics_event_store_factory is not None:
                event_store = self._open_statistics_event_store(statistics_path)
                try:
                    event_store.count()
                    projection = self._open_statistics_projection_store(event_store)
                    projection.checkpoint()
                    statistics_state = "healthy"
                finally:
                    close = getattr(event_store, "close", None)
                    if callable(close):
                        close()
            recovery_state = "unavailable"
            stream = LocalEventStream(
                self._campaign_database,
                statistics_database=self._statistics_database,
                knowledge_database=self._knowledge_database,
                statistics_event_store_factory=self._statistics_event_store_factory,
                statistics_projection_store_factory=self._statistics_projection_store_factory,
                knowledge_store_factory=self._knowledge_store_factory,
                recovery_path_resolver=self._recovery_path_resolver,
            )
            recovery_path = stream._resolve_recovery_path()
            if recovery_path.is_file():
                recovery_state = "available"
            overall = "healthy" if not blockers and campaign_state == "healthy" else "degraded"
            return HealthDto(
                status=overall,
                campaign_store=campaign_state,
                knowledge_store=knowledge_state,
                statistics_store=statistics_state,
                recovery=recovery_state,
                blockers=tuple(blockers),
            )
        return self._execute(operation)
    def diagnostics(
        self,
        *,
        campaign_id: str | None = None,
        limit: int,
    ) -> ApiOutcome[DiagnosticsDto]:
        # Assemble bounded health, progress and quarantine data without reading stdout, stderr or environment values.
        def operation() -> DiagnosticsDto:
            # Reuse public read boundaries and preserve typed blockers from partial component failures.
            page_limit = _validate_limit(limit, maximum=LOCAL_DIAGNOSTICS_MAX_LIMIT)
            health_outcome = self.health(max_rows=10_000)
            if not isinstance(health_outcome, ApiSuccess):
                raise _ActionFailed("health_unavailable", "health diagnostics are unavailable", retriable=True)
            progress: ProgressDto | None = None
            blockers = list(health_outcome.value.blockers)
            quarantine = QuarantineInspectionDto((), page_limit, False)
            if campaign_id is not None:
                current_campaign_id = _required_identifier(campaign_id, "campaign_id")
                stream = LocalEventStream(
                    self._campaign_database,
                    statistics_database=self._statistics_database,
                    knowledge_database=self._knowledge_database,
                    statistics_event_store_factory=self._statistics_event_store_factory,
                    statistics_projection_store_factory=self._statistics_projection_store_factory,
                    knowledge_store_factory=self._knowledge_store_factory,
                    recovery_path_resolver=self._recovery_path_resolver,
                )
                progress_outcome = stream.get_progress(current_campaign_id, shard_limit=page_limit)
                if isinstance(progress_outcome, ApiSuccess):
                    progress = progress_outcome.value
                else:
                    blockers.append(DiagnosticBlockerDto("progress_unavailable", "campaign progress is unavailable", "warning", "progress"))
                quarantine_outcome = self.inspect_quarantine(current_campaign_id, limit=page_limit)
                if isinstance(quarantine_outcome, ApiSuccess):
                    quarantine = quarantine_outcome.value
                else:
                    blockers.append(DiagnosticBlockerDto("quarantine_unavailable", "quarantine inspection is unavailable", "warning", "knowledge"))
            return DiagnosticsDto(
                health=health_outcome.value,
                progress=progress,
                quarantine=quarantine,
                blockers=tuple(blockers[:page_limit]),
            )
        return self._execute(operation)
