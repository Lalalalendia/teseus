from __future__ import annotations
import base64
import hashlib
import json
import sqlite3
from dataclasses import dataclass, replace
from pathlib import Path
from types import SimpleNamespace
from theseus_api import ApiFailed, ApiRejected, ApiSuccess, LocalOperatorActions
@dataclass(frozen=True)
class _Id:
    value: str
@dataclass(frozen=True)
class _Status:
    value: str
@dataclass(frozen=True)
class _Campaign:
    campaign_id: _Id
    project_id: _Id
    status: _Status
    revision_number: int
    configuration: object
@dataclass(frozen=True)
class _Shard:
    shard_id: _Id
    campaign_id: _Id
    status: _Status
    revision_number: int
    attempt: int
@dataclass(frozen=True)
class Success:
    value: object
    duplicate: bool = False
@dataclass(frozen=True)
class Rejected:
    code: str
    message: str
    details: dict[str, object]
@dataclass(frozen=True)
class _Action:
    action_id: str
    action_type: str
    campaign_id: _Id
    expected_revision: int
    request_fingerprint: str
    status: _Status = _Status("requested")
    result: dict[str, object] = None
    error_code: str | None = None
    retriable: bool = False
    def __post_init__(self) -> None:
        # Install an independent result mapping for the fake durable action.
        object.__setattr__(self, "result", dict(self.result or {}))
class _Store:
    def __init__(self, campaign: _Campaign, shard: _Shard, artifact: object | None = None, intent: object | None = None) -> None:
        # Keep mutable test state behind the same methods as the authoritative mutation store.
        self.campaign = campaign
        self.shard = shard
        self.effects: dict[str, object] = {}
        self.actions: dict[str, _Action] = {}
        self.artifact = artifact
        self.intent = intent
    def get_effect(self, action_id: str) -> Success:
        # Return a prior replay receipt marker when the fake action ID was already committed.
        value = self.effects.get(action_id)
        return Success(SimpleNamespace(effect_type=("mutation.cancel" if isinstance(value, _Campaign) else "mutation.retry_shard"), payload={}) if value is not None else None)
    def get_operator_action(self, action_id: str) -> Success:
        # Return one fake durable operator action for API replay tests.
        return Success(self.actions.get(action_id))
    def get_shard(self, shard_id: object) -> Success:
        # Return the exact authoritative shard for the requested test identity.
        return Success(self.shard if str(shard_id) == self.shard.shard_id.value else None)
    def get_finalization_intent(self, campaign_id: object) -> Success:
        # Return one immutable fake finalization intent.
        return Success(self.intent)
    def list_artifacts(self, campaign_id: object) -> Success:
        # Return one immutable fake registry row when configured.
        return Success((self.artifact,) if self.artifact is not None else ())
    def close(self) -> None:
        # Preserve fake state across action calls while matching the production lifecycle.
        return None
class _Service:
    def __init__(self, store: _Store) -> None:
        # Retain the fake authoritative store for replay-safe action effects.
        self.store = store
        self.cancel_calls = 0
        self.retry_calls = 0
    def request_operator_action(self, action_id: str, action_type: str, campaign_id: object, *, expected_revision: int, parameters=None) -> Success | Rejected:
        # Create or replay one fake action after campaign revision fencing.
        fingerprint = json.dumps({"action_type": action_type, "campaign_id": str(campaign_id), "expected_revision": expected_revision, "parameters": parameters or {}}, sort_keys=True)
        existing = self.store.actions.get(action_id)
        if existing is not None:
            if existing.request_fingerprint != fingerprint:
                return Rejected("action_id_conflict", "conflict", {})
            return Success(existing, duplicate=True)
        if self.store.campaign.revision_number != expected_revision:
            return Rejected("stale_revision", "stale", {"actual_revision": self.store.campaign.revision_number})
        action = _Action(action_id, action_type, _Id(str(campaign_id)), expected_revision, fingerprint)
        self.store.actions[action_id] = action
        return Success(action)
    def start_operator_action(self, action_id: str) -> Success:
        # Mark one fake action running while preserving exact replay.
        action = self.store.actions[action_id]
        if action.status.value == "requested":
            action = replace(action, status=_Status("running"))
            self.store.actions[action_id] = action
        return Success(action, duplicate=action.status.value == "running")
    def complete_operator_action(self, action_id: str, result: dict[str, object]) -> Success:
        # Persist one fake terminal success result.
        action = self.store.actions[action_id]
        if action.status.value != "completed":
            action = replace(action, status=_Status("completed"), result=dict(result))
            self.store.actions[action_id] = action
        return Success(action, duplicate=action.status.value == "completed")
    def reject_operator_action(self, action_id: str, code: str, *, result=None) -> Success:
        # Persist one fake terminal rejection.
        action = replace(self.store.actions[action_id], status=_Status("rejected"), result=dict(result or {}), error_code=code)
        self.store.actions[action_id] = action
        return Success(action)
    def fail_operator_action(self, action_id: str, code: str, *, retriable: bool, result=None) -> Success:
        # Persist one fake terminal technical failure.
        action = replace(self.store.actions[action_id], status=_Status("failed"), result=dict(result or {}), error_code=code, retriable=retriable)
        self.store.actions[action_id] = action
        return Success(action)
    def retry_campaign(self, action_id: str, campaign_id: object, *, expected_revision: int) -> Success | Rejected:
        # Requeue the one fake retryable shard through a campaign-level action.
        requested = self.request_operator_action(action_id, "retry_campaign", campaign_id, expected_revision=expected_revision)
        if isinstance(requested, Rejected):
            return requested
        if requested.value.status.value == "completed":
            return requested
        self.start_operator_action(action_id)
        if self.store.shard.status.value in {"failed", "partial", "orphaned"}:
            self.retry_calls += 1
            self.store.shard = replace(self.store.shard, status=_Status("created"), revision_number=self.store.shard.revision_number + 1, attempt=self.store.shard.attempt + 1)
            shard_ids = [self.store.shard.shard_id.value]
        else:
            shard_ids = []
        return self.complete_operator_action(action_id, {"campaign_revision": expected_revision, "shard_ids": shard_ids, "shard_revisions": {self.store.shard.shard_id.value: self.store.shard.revision_number} if shard_ids else {}})
    def get_campaign(self, campaign_id: object) -> Success | Rejected:
        # Return the required campaign or a stable not-found rejection.
        if str(campaign_id) != self.store.campaign.campaign_id.value:
            return Rejected("campaign_not_found", "missing", {"campaign_id": str(campaign_id)})
        return Success(self.store.campaign)
    def cancel(self, action_id: str, campaign_id: object, *, expected_revision: int) -> Success | Rejected:
        # Apply one replay-safe cancellation and return the same saved campaign on duplicate action IDs.
        if action_id in self.store.effects:
            return Success(self.store.effects[action_id], duplicate=True)
        if self.store.campaign.revision_number != expected_revision:
            return Rejected("stale_revision", "stale", {})
        self.cancel_calls += 1
        self.store.campaign = replace(
            self.store.campaign,
            status=_Status("cancelled"),
            revision_number=self.store.campaign.revision_number + 1,
        )
        self.store.effects[action_id] = self.store.campaign
        return Success(self.store.campaign)
    def retry_shard(self, action_id: str, shard_id: object, *, expected_revision: int) -> Success | Rejected:
        # Apply one replay-safe shard retry without creating another topology.
        if action_id in self.store.effects:
            return Success(self.store.effects[action_id], duplicate=True)
        if self.store.shard.revision_number != expected_revision:
            return Rejected("stale_revision", "stale", {})
        self.retry_calls += 1
        self.store.shard = replace(
            self.store.shard,
            status=_Status("created"),
            revision_number=self.store.shard.revision_number + 1,
            attempt=self.store.shard.attempt + 1,
        )
        self.store.effects[action_id] = self.store.shard
        return Success(self.store.shard)
class _KnowledgeStore:
    def __init__(self, path: Path) -> None:
        # Retain the configured path only to mirror the production constructor.
        self.path = path
    def list_conflicts(self, *, limit: int):
        # Return newest-first bounded conflicts without raw payloads.
        return tuple(
            SimpleNamespace(
                conflict_id=f"conflict-{index}",
                conflict_type="identity_conflict",
                identity_type="execution",
                identity_key=f"execution-{index}",
                scope_id="scope-1",
                reason="payload_mismatch",
                created_at=f"2026-08-05T12:00:0{index}Z",
                status="quarantined",
            )
            for index in range(min(3, limit))
        )
    def verify_integrity(self, *, max_rows: int) -> SimpleNamespace:
        # Return one healthy bounded integrity report.
        return SimpleNamespace(healthy=True, checked_rows=max_rows)
    def close(self) -> None:
        # Match the production resource lifecycle without side effects.
        return None
class _StatisticsStore:
    def __init__(self, path: Path) -> None:
        # Retain the configured path only to match the production factory.
        self.path = path
    def count(self) -> int:
        # Return one stable event count for health diagnostics.
        return 5
class _StatisticsProjection:
    def __init__(self, event_store: object) -> None:
        # Retain the event store only to match the production constructor.
        self.event_store = event_store
    def checkpoint(self) -> SimpleNamespace:
        # Return one stable projection checkpoint.
        return SimpleNamespace(sequence=5, event_id="event-5")
def _create_progress_database(path: Path, campaign: _Campaign, shard: _Shard) -> None:
    # Create the read tables required by health and diagnostics progress snapshots.
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE mutation_campaigns (campaign_id TEXT PRIMARY KEY, revision_number INTEGER NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE mutation_shards (shard_id TEXT PRIMARY KEY, revision_number INTEGER NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE mutation_workers (worker_key TEXT PRIMARY KEY, revision_number INTEGER NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE mutation_outbox (effect_id TEXT PRIMARY KEY, event_type TEXT NOT NULL, campaign_id TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL, delivered_at TEXT);
            """
        )
        campaign_payload = {
            "campaign_id": campaign.campaign_id.value,
            "project_id": campaign.project_id.value,
            "status": campaign.status.value,
            "total_mutants": 2,
            "completed_mutants": 1,
            "configuration": {"project": {"project_id": campaign.project_id.value}},
        }
        shard_payload = {
            "campaign_id": campaign.campaign_id.value,
            "status": shard.status.value,
            "attempt": shard.attempt,
            "completed_count": 0,
            "mutant_ids": ["mutant-1"],
        }
        connection.execute(
            "INSERT INTO mutation_campaigns(campaign_id, revision_number, payload) VALUES (?, ?, ?)",
            (campaign.campaign_id.value, campaign.revision_number, json.dumps(campaign_payload)),
        )
        connection.execute(
            "INSERT INTO mutation_shards(shard_id, revision_number, payload) VALUES (?, ?, ?)",
            (shard.shard_id.value, shard.revision_number, json.dumps(shard_payload)),
        )
        connection.execute(
            "INSERT INTO mutation_workers(worker_key, revision_number, payload) VALUES (?, ?, ?)",
            ("campaign-1\x1fworker-1", 1, json.dumps({"campaign_id": "campaign-1", "status": "running"})),
        )
        connection.execute(
            "INSERT INTO mutation_outbox(effect_id, event_type, campaign_id, payload, created_at, delivered_at) VALUES (?, ?, ?, ?, ?, NULL)",
            ("effect-1", "mutation.start", "campaign-1", json.dumps({"payload": {}}), "2026-08-05T12:00:00Z"),
        )
        connection.commit()
    finally:
        connection.close()
def _actions(tmp_path: Path, *, with_artifact: bool = False):
    # Build one operator boundary over persistent fake service state and real artifact bytes.
    campaign = _Campaign(_Id("campaign-1"), _Id("project-1"), _Status("running"), 4, SimpleNamespace(name="configuration"))
    shard = _Shard(_Id("shard-1"), _Id("campaign-1"), _Status("failed"), 2, 0)
    database = tmp_path / "reports" / "campaign-1" / "campaign.sqlite3"
    database.parent.mkdir(parents=True)
    _create_progress_database(database, campaign, shard)
    artifact = None
    intent = None
    payload = b"verified-artifact-content"
    if with_artifact:
        digest = hashlib.sha256(payload).hexdigest()
        content_path = f"artifacts/{digest[:2]}/{digest}"
        target = database.parent.parent / content_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
        artifact = SimpleNamespace(
            campaign_id=_Id("campaign-1"),
            logical_key="report",
            content_sha256=digest,
            size_bytes=len(payload),
            content_path=content_path,
        )
        intent = SimpleNamespace(campaign_id=_Id("campaign-1"))
    store = _Store(campaign, shard, artifact=artifact, intent=intent)
    service = _Service(store)
    signals: list[object] = []
    knowledge = tmp_path / "knowledge.sqlite3"
    knowledge.touch()
    statistics = tmp_path / "statistics.sqlite3"
    statistics.touch()
    recovery = tmp_path / "recovery.json"
    recovery.write_text(json.dumps({"status": "complete"}), encoding="utf-8")
    def validate_artifact(database_path: Path, current_intent: object, registry: tuple[object, ...]) -> None:
        # Verify the selected content bytes against the immutable fake registry identity.
        assert current_intent is intent
        assert registry == (artifact,)
        path = database_path.parent.parent / artifact.content_path
        data = path.read_bytes()
        assert hashlib.sha256(data).hexdigest() == artifact.content_sha256
        assert len(data) == artifact.size_bytes
    resume_calls: list[str] = []
    def resume_executor(database_path: Path, campaign_id: str) -> object:
        # Simulate one authoritative coordinator resume and durable campaign revision advance.
        resume_calls.append(campaign_id)
        store.campaign = replace(store.campaign, status=_Status("completed"), revision_number=store.campaign.revision_number + 1)
        return SimpleNamespace(campaign=store.campaign, database_path=database_path)
    actions = LocalOperatorActions(
        database,
        knowledge_database=knowledge,
        statistics_database=statistics,
        mutation_context_factory=lambda _: (store, service),
        campaign_id_factory=lambda value: value,
        shard_id_factory=lambda value: value,
        cancel_signal=signals.append,
        knowledge_store_factory=_KnowledgeStore,
        statistics_event_store_factory=_StatisticsStore,
        statistics_projection_store_factory=_StatisticsProjection,
        artifact_validator=validate_artifact if with_artifact else None,
        recovery_path_resolver=lambda _: recovery,
        resume_executor=resume_executor,
    )
    return actions, store, service, signals, payload, resume_calls
def test_cancel_and_retry_are_replay_safe_and_reject_stale_revisions(tmp_path: Path) -> None:
    # Delegate writes to authoritative replay-safe effects and keep repeated action outcomes identical.
    actions, store, service, signals, _, _ = _actions(tmp_path)
    stale = actions.cancel_campaign("cancel-stale", "campaign-1", expected_revision=3)
    assert isinstance(stale, ApiRejected)
    assert stale.error.code == "stale_revision"
    assert service.cancel_calls == 0
    first_cancel = actions.cancel("cancel-1", "campaign-1", expected_revision=4)
    repeated_cancel = actions.cancel_campaign("cancel-1", "campaign-1", expected_revision=4)
    assert isinstance(first_cancel, ApiSuccess)
    assert isinstance(repeated_cancel, ApiSuccess)
    assert repeated_cancel.to_dict() == first_cancel.to_dict()
    assert service.cancel_calls == 1
    assert len(signals) == 1
    store.campaign = replace(store.campaign, status=_Status("running"), revision_number=6)
    stale_retry = actions.retry_shard(
        "retry-stale",
        "campaign-1",
        "shard-1",
        expected_campaign_revision=5,
        expected_shard_revision=2,
    )
    assert isinstance(stale_retry, ApiRejected)
    assert service.retry_calls == 0
    first_retry = actions.retry(
        "retry-1",
        "campaign-1",
        "shard-1",
        expected_campaign_revision=6,
        expected_shard_revision=2,
    )
    repeated_retry = actions.retry_shard(
        "retry-1",
        "campaign-1",
        "shard-1",
        expected_campaign_revision=6,
        expected_shard_revision=2,
    )
    assert isinstance(first_retry, ApiSuccess)
    assert isinstance(repeated_retry, ApiSuccess)
    assert repeated_retry.to_dict() == first_retry.to_dict()
    assert service.retry_calls == 1
    assert store.shard.attempt == 1
def test_resume_is_durable_and_exact_replay_does_not_run_coordinator_twice(tmp_path: Path) -> None:
    # Persist one resume receipt and return it without invoking the coordinator on exact replay.
    actions, _, _, _, _, resume_calls = _actions(tmp_path)
    first = actions.resume("resume-1", "campaign-1", expected_revision=4)
    repeated = actions.resume_campaign("resume-1", "campaign-1", expected_revision=4)
    assert isinstance(first, ApiSuccess)
    assert isinstance(repeated, ApiSuccess)
    assert repeated.to_dict() == first.to_dict()
    assert resume_calls == ["campaign-1"]
    assert first.value.status == "completed"
    assert first.value.campaign_revision == 5
def test_artifact_content_is_verified_and_returned_in_bounded_chunks(tmp_path: Path) -> None:
    # Select content only by logical key and return verified bytes without exposing a physical path.
    actions, _, _, _, payload, _ = _actions(tmp_path, with_artifact=True)
    first = actions.read_artifact("campaign-1", "report", offset=0, limit=8)
    assert isinstance(first, ApiSuccess)
    assert base64.b64decode(first.value.data_base64) == payload[:8]
    assert first.value.next_offset == 8
    assert "content_path" not in json.dumps(first.to_dict(), sort_keys=True)
    second = actions.read_artifact("campaign-1", "report", offset=8, limit=1024)
    assert isinstance(second, ApiSuccess)
    assert base64.b64decode(second.value.data_base64) == payload[8:]
    assert second.value.complete is True
    missing = actions.read_artifact("campaign-1", "missing", offset=0, limit=8)
    assert isinstance(missing, ApiRejected)
    assert missing.error.code == "not_found"
def test_artifact_integrity_failure_is_sanitized(tmp_path: Path) -> None:
    # Convert validator failures into one stable API failure without returning filesystem details.
    actions, _, _, _, _, _ = _actions(tmp_path, with_artifact=True)
    actions._artifact_validator = lambda *args: (_ for _ in ()).throw(RuntimeError("D:/private/secret"))
    outcome = actions.read_artifact("campaign-1", "report", offset=0, limit=8)
    assert isinstance(outcome, ApiFailed)
    serialized = json.dumps(outcome.to_dict(), sort_keys=True)
    assert "D:/private/secret" not in serialized
def test_quarantine_health_and_diagnostics_are_bounded_and_secret_free(tmp_path: Path) -> None:
    # Return typed blockers and bounded operator data without stdout, stderr or environment values.
    actions, _, _, _, _, _ = _actions(tmp_path)
    quarantine = actions.inspect_quarantine("campaign-1", limit=2)
    assert isinstance(quarantine, ApiSuccess)
    assert len(quarantine.value.items) == 2
    assert quarantine.value.truncated is True
    health = actions.health(max_rows=100)
    assert isinstance(health, ApiSuccess)
    assert health.value.campaign_store == "healthy"
    diagnostics = actions.diagnostics(campaign_id="campaign-1", limit=2)
    assert isinstance(diagnostics, ApiSuccess)
    assert diagnostics.value.progress is not None
    serialized = json.dumps(diagnostics.to_dict(), sort_keys=True).lower()
    for forbidden in ("stdout", "stderr", "secret", "environment_value", "d:/"):
        assert forbidden not in serialized
def test_start_campaign_launch_failure_exposes_bounded_stage_without_private_exception_text(tmp_path: Path) -> None:
    # Preserve useful launch stage diagnostics while keeping the private exception message out of the public API.
    actions, _, _, _, _, _ = _actions(tmp_path)
    actions._launch_executor = lambda *_args: (_ for _ in ()).throw(OSError("D:/private/secret"))
    outcome = actions.start_campaign("launch-failure", "campaign-1", expected_revision=4)
    assert isinstance(outcome, ApiFailed)
    assert outcome.error.code == "campaign_launch_failed"
    assert outcome.error.details == {"stage": "spawn", "exception_type": "OSError"}
    assert "D:/private/secret" not in json.dumps(outcome.to_dict(), sort_keys=True)
