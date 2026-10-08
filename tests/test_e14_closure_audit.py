from __future__ import annotations

import ast
import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from gallifrey_mutation import (
    CampaignState,
    EffectReceipt,
    InMemoryMutationStore,
    MutationCampaign,
    MutationCampaignService,
    Rejected,
    SQLiteMutationStore,
    Success,
)
from theseus_contracts import (
    ArtifactRegistryEntry,
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    FinalizationIntent,
    MutationScope,
    ProjectDescriptor,
    ProjectId,
    WorkerCapabilities,
    WorkerIdentity,
)
from theseus_local import LocalCampaignCoordinator
from theseus_local.finalization import (
    ArtifactSource,
    FinalizationArtifactError,
    _copy_to_exclusive_target,
    build_finalization_intent,
    publish_finalization_intent,
    validate_registered_artifacts,
)
from theseus_local.startup_recovery import StartupRecoveryLock
from theseus_local.worker_runtime import DurableExecutionSpool, SpoolError
from test_intelligence_unified_v1.io_utils import build_mutant_spool_frame, stable_hash

_TIMESTAMP = "2026-08-05T00:00:00Z"


def _artifact_entry(
    campaign_id: CampaignId,
    logical_key: str,
    payload: bytes,
    *,
    logical_path: str | None = None,
    content_path: str | None = None,
    required: bool,
) -> ArtifactRegistryEntry:
    # Build one deterministic registry row with portable content and logical paths.
    digest = hashlib.sha256(payload).hexdigest()
    return ArtifactRegistryEntry(
        campaign_id=campaign_id,
        logical_key=logical_key,
        logical_role="canonical_report" if required else "supporting_evidence",
        content_sha256=digest,
        size_bytes=len(payload),
        schema_version=1,
        producer="e14-closure-audit",
        content_path=content_path or f"artifacts/sha256/{digest[:2]}/{digest}",
        logical_path=logical_path or f"campaign/{logical_key}",
        created_at=_TIMESTAMP,
        metadata={"required": required},
    )


def _intent(
    campaign_id: CampaignId,
    artifacts: tuple[ArtifactRegistryEntry, ...],
    *,
    status: str,
) -> FinalizationIntent:
    # Freeze one exact finalization artifact set for store and filesystem closure tests.
    return FinalizationIntent(
        intent_id="intent-e14-closure-audit",
        campaign_id=campaign_id,
        status=status,
        required_logical_keys=("canonical.report.json",),
        canonical_report_key="canonical.report.json",
        artifacts=artifacts,
        result_fingerprint="result-fingerprint-e14-closure-audit",
        created_at=_TIMESTAMP,
        updated_at=_TIMESTAMP,
    )


def _write_registered_artifact(root: Path, entry: ArtifactRegistryEntry, payload: bytes) -> None:
    # Materialize both immutable content and logical aliases exactly as the registry declares.
    for relative in (entry.content_path, entry.logical_path):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)


def _configuration(root: Path, campaign_id: str) -> CampaignConfiguration:
    # Build a minimal local campaign contract for startup process-ownership reconciliation.
    return CampaignConfiguration(
        campaign_id=CampaignId(campaign_id),
        project=ProjectDescriptor(
            project_id=ProjectId(f"project-{campaign_id}"),
            display_name="E14 closure audit fixture",
            root_path=str(root),
        ),
        scope=MutationScope(source_path="app.py"),
        budget=CampaignBudget(max_mutants=1, max_workers=1),
        reports_dir=str(root / "reports"),
    )


def test_finalization_validation_requires_the_exact_expected_registry(tmp_path: Path) -> None:
    # Reject a completed-looking registry that silently lost a non-required expected artifact.
    database_path = tmp_path / "reports" / "campaign" / "campaign.sqlite3"
    database_path.parent.mkdir(parents=True)
    campaign_id = CampaignId("campaign-e14-exact-registry")
    canonical_payload = b'{"status":"complete"}\n'
    plan_payload = b'{"plan":"immutable"}\n'
    canonical = _artifact_entry(
        campaign_id,
        "canonical.report.json",
        canonical_payload,
        required=True,
    )
    plan = _artifact_entry(
        campaign_id,
        "campaign.plan.json",
        plan_payload,
        required=False,
    )
    intent = _intent(campaign_id, (canonical, plan), status="registered")
    _write_registered_artifact(database_path.parent.parent, canonical, canonical_payload)
    with pytest.raises(FinalizationArtifactError) as captured:
        validate_registered_artifacts(database_path, intent, (canonical,))
    message = str(captured.value)
    assert "campaign.plan.json" in message and "missing" in message, (
        "E14 exact-registry gate did not identify the missing expected artifact; "
        f"campaign={campaign_id.value}; intent_id={intent.intent_id}; "
        f"expected_keys={[item.logical_key for item in intent.artifacts]}; "
        f"registered_keys={[canonical.logical_key]}; message={message!r}; "
        f"database={database_path}"
    )


def test_artifact_contract_rejects_noncanonical_hash_and_required_key_identity() -> None:
    # Refuse wire payload normalization that would hide uppercase hashes or duplicate required keys.
    campaign_id = CampaignId("campaign-e14-artifact-contract")
    entry = _artifact_entry(
        campaign_id,
        "canonical.report.json",
        b"canonical",
        required=True,
    )
    uppercase = entry.to_dict()
    uppercase["content_sha256"] = entry.content_sha256.upper()
    with pytest.raises(ValueError, match="lowercase SHA-256"):
        ArtifactRegistryEntry.from_dict(uppercase)
    with pytest.raises(ValueError, match="required logical keys must be unique"):
        FinalizationIntent(
            intent_id="intent-e14-duplicate-required",
            campaign_id=campaign_id,
            status="created",
            required_logical_keys=(entry.logical_key, entry.logical_key),
            canonical_report_key=entry.logical_key,
            artifacts=(entry,),
            result_fingerprint="result-fingerprint-duplicate-required",
            created_at=_TIMESTAMP,
            updated_at=_TIMESTAMP,
        )
    invalid_schema = _intent(campaign_id, (entry,), status="created").to_dict()
    invalid_schema["schema_version"] = 0
    with pytest.raises(ValueError, match="schema_version must be positive"):
        FinalizationIntent.from_dict(invalid_schema)


@pytest.mark.parametrize("store_kind", ("memory", "sqlite"))
def test_store_rejects_partial_or_duplicate_registered_artifact_sets(
    tmp_path: Path,
    store_kind: str,
) -> None:
    # Enforce the same exact registry contract in both authoritative store adapters.
    store = (
        InMemoryMutationStore()
        if store_kind == "memory"
        else SQLiteMutationStore(tmp_path / f"{store_kind}.sqlite3")
    )
    campaign_id = CampaignId(f"campaign-e14-store-{store_kind}")
    canonical = _artifact_entry(
        campaign_id,
        "canonical.report.json",
        b"canonical",
        required=True,
    )
    plan = _artifact_entry(
        campaign_id,
        "campaign.plan.json",
        b"plan",
        required=False,
    )
    created = _intent(campaign_id, (canonical, plan), status="created")
    created_receipt = EffectReceipt(
        "effect.e14.intent",
        "mutation.finalization_intent",
        campaign_id,
        {"finalization_intent": created.to_dict()},
    )
    try:
        created_outcome = store.commit_finalization(
            receipt=created_receipt,
            intent=created,
        )
        assert isinstance(created_outcome, Success), (
            "E14 fixture could not persist the durable finalization intent; "
            f"store={store_kind}; campaign={campaign_id.value}; outcome={created_outcome!r}"
        )
        registered = replace(created, status="registered")
        partial_receipt = EffectReceipt(
            "effect.e14.partial-registry",
            "mutation.artifact_registry_committed",
            campaign_id,
            {"finalization_intent": registered.to_dict()},
        )
        partial = store.commit_finalization(
            receipt=partial_receipt,
            intent=registered,
            artifacts=(canonical,),
        )
        assert isinstance(partial, Rejected), (
            "authoritative store accepted a partial registered artifact set; "
            f"store={store_kind}; campaign={campaign_id.value}; "
            f"expected={[canonical.logical_key, plan.logical_key]}; "
            f"actual={[canonical.logical_key]}; outcome={partial!r}"
        )
        assert "campaign.plan.json" in str(dict(partial.details)), (
            "partial registry rejection omitted the missing logical key; "
            f"store={store_kind}; campaign={campaign_id.value}; rejection={partial!r}"
        )
        duplicate_receipt = EffectReceipt(
            "effect.e14.duplicate-registry",
            "mutation.artifact_registry_committed",
            campaign_id,
            {"finalization_intent": registered.to_dict()},
        )
        duplicate = store.commit_finalization(
            receipt=duplicate_receipt,
            intent=registered,
            artifacts=(canonical, canonical, plan),
        )
        assert isinstance(duplicate, Rejected) and duplicate.code == "invalid_artifact_registry_bundle", (
            "authoritative store collapsed duplicate logical keys instead of failing closed; "
            f"store={store_kind}; campaign={campaign_id.value}; outcome={duplicate!r}"
        )
    finally:
        close = getattr(store, "close", None)
        if callable(close):
            close()


@pytest.mark.parametrize("store_kind", ("memory", "sqlite"))
def test_completed_campaign_without_finalization_receipt_is_not_idempotent_success(
    tmp_path: Path,
    store_kind: str,
) -> None:
    # Reject a corrupted completed projection instead of treating missing finalization evidence as a duplicate.
    store = (
        InMemoryMutationStore()
        if store_kind == "memory"
        else SQLiteMutationStore(tmp_path / f"completed-{store_kind}.sqlite3")
    )
    configuration = _configuration(
        tmp_path,
        f"campaign-e14-completed-without-receipt-{store_kind}",
    )
    corrupted = replace(
        MutationCampaign.create(configuration),
        status=CampaignState.COMPLETED,
    )
    try:
        saved = store.save_campaign(corrupted, expected_revision=None)
        assert isinstance(saved, Success), (
            "closure fixture could not install the intentionally corrupted completed projection; "
            f"store={store_kind}; campaign={configuration.campaign_id.value}; outcome={saved!r}"
        )
        outcome = MutationCampaignService(store).complete_finalization(
            "effect.e14.verify-corrupt-completion",
            configuration.campaign_id,
            expected_revision=corrupted.revision_number,
        )
        assert isinstance(outcome, Rejected) and outcome.code == "completed_finalization_receipt_missing", (
            "domain service accepted COMPLETED without the authoritative finalization receipt; "
            f"store={store_kind}; campaign={configuration.campaign_id.value}; "
            f"status={corrupted.status.value}; outcome={outcome!r}"
        )
    finally:
        close = getattr(store, "close", None)
        if callable(close):
            close()


def test_finalization_rejects_registry_paths_outside_reports_root(tmp_path: Path) -> None:
    # Fence a tampered registered intent before it can read or repair a path outside campaign state.
    database_path = tmp_path / "reports" / "campaign" / "campaign.sqlite3"
    database_path.parent.mkdir(parents=True)
    campaign_id = CampaignId("campaign-e14-path-fencing")
    entry = _artifact_entry(
        campaign_id,
        "canonical.report.json",
        b"canonical",
        content_path="../outside-content",
        logical_path="campaign/canonical.report.json",
        required=True,
    )
    intent = _intent(campaign_id, (entry,), status="registered")
    with pytest.raises(FinalizationArtifactError) as captured:
        publish_finalization_intent(database_path, intent)
    message = str(captured.value)
    assert "unsafe content_path" in message and entry.logical_key in message, (
        "path-fencing diagnostic omitted the unsafe field or logical artifact identity; "
        f"campaign={campaign_id.value}; intent_id={intent.intent_id}; "
        f"content_path={entry.content_path!r}; message={message!r}; database={database_path}"
    )
    assert not (tmp_path / "outside-content").exists(), (
        "finalization path traversal created or modified a file outside reports root; "
        f"campaign={campaign_id.value}; escaped_path={tmp_path / 'outside-content'}"
    )


def test_finalization_rejects_traversal_in_staged_logical_key(tmp_path: Path) -> None:
    # Prevent a generated payload logical key from escaping its immutable intent staging directory.
    database_path = tmp_path / "reports" / "campaign" / "campaign.sqlite3"
    database_path.parent.mkdir(parents=True)
    campaign_id = CampaignId("campaign-e14-staging-key-fencing")
    logical_path = database_path.parent / "canonical.report.json"
    source = ArtifactSource(
        logical_key="../../escaped.report.json",
        logical_role="canonical_report",
        logical_path=logical_path,
        producer="e14-closure-audit",
        schema_version=1,
        payload=b"canonical",
        required=True,
    )
    with pytest.raises(FinalizationArtifactError) as captured:
        build_finalization_intent(
            database_path,
            campaign_id,
            (source,),
            canonical_report_key=source.logical_key,
            result_fingerprint="result-fingerprint-staging-key",
        )
    message = str(captured.value)
    assert "unsafe logical_key staging path" in message and source.logical_key in message, (
        "staging-key fencing omitted the unsafe logical key and boundary name; "
        f"campaign={campaign_id.value}; logical_key={source.logical_key!r}; "
        f"message={message!r}; database={database_path}"
    )
    assert not (database_path.parent / "escaped.report.json").exists(), (
        "unsafe logical key created bytes outside its intent staging directory; "
        f"campaign={campaign_id.value}; database={database_path}"
    )


def test_content_publication_fsyncs_new_directory_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Require publication durability to include the directory entry, not only file contents.
    source = tmp_path / "source.bin"
    destination = tmp_path / "registry" / "content.bin"
    source.write_bytes(b"immutable-content")
    fsynced: list[Path] = []
    monkeypatch.setattr(
        "theseus_local.finalization._fsync_directory",
        lambda path: fsynced.append(Path(path).resolve()),
    )
    _copy_to_exclusive_target(source, destination)
    assert destination.read_bytes() == source.read_bytes(), (
        "content publication did not preserve exact source bytes; "
        f"source={source}; destination={destination}"
    )
    assert fsynced == [destination.parent.resolve()], (
        "content publication returned before persisting the new directory entry; "
        f"destination={destination}; expected_fsync={destination.parent.resolve()}; "
        f"actual_fsync={fsynced}"
    )


def test_terminal_worker_with_exact_live_pid_is_still_terminated_on_startup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Close the crash window where STOPPED was durable but the exact worker process had not exited yet.
    configuration = _configuration(tmp_path, "campaign-e14-terminal-process")
    database_path = tmp_path / "state" / "campaign.sqlite3"
    store = SQLiteMutationStore(database_path)
    service = MutationCampaignService(store)
    worker_pid = 424242
    worker_birth = "birth-terminal-worker"
    worker_workspace = tmp_path / "workers" / "worker-terminal" / "workspace"
    worker_workspace.mkdir(parents=True)
    registered = service.register_worker(
        "effect.e14.register-terminal-worker",
        configuration.campaign_id,
        WorkerIdentity("worker-terminal", "instance-terminal", worker_pid, worker_birth),
        WorkerCapabilities("win32", "AMD64", ("3.14",), (1,), ("copy",), 1),
        workspace=str(worker_workspace),
        spool_path=str(tmp_path / "spool"),
    )
    assert isinstance(registered, Success), registered
    stopped = service.stop_worker(
        "effect.e14.stop-terminal-worker",
        configuration.campaign_id,
        "worker-terminal",
        expected_revision=registered.value.revision_number,
    )
    assert isinstance(stopped, Success), stopped
    created = service.create_campaign(configuration)
    assert isinstance(created, Success), created
    failed = service.apply_transition(
        "effect.e14.fail-terminal-campaign",
        configuration.campaign_id,
        CampaignState.FAILED,
        expected_revision=created.value.revision_number,
    )
    assert isinstance(failed, Success), failed
    store.close()
    terminated: list[tuple[int | None, str | None]] = []
    monkeypatch.setattr(
        "theseus_local.coordinator.current_process_birth_token",
        lambda process_id: worker_birth if process_id == worker_pid else None,
    )
    monkeypatch.setattr(
        "theseus_local.coordinator.terminate_recorded_process",
        lambda process_id, birth_token: terminated.append((process_id, birth_token)) or True,
    )
    actions = LocalCampaignCoordinator.reconcile_startup(database_path)
    assert (worker_pid, worker_birth) in terminated, (
        "startup recovery trusted a terminal database status more than the exact live process identity; "
        f"campaign={configuration.campaign_id.value}; worker_id=worker-terminal; "
        f"worker_pid={worker_pid}; birth_token={worker_birth}; terminated={terminated}; "
        f"actions={actions}; database={database_path}"
    )
    worker_actions = [item for item in actions if item.get("worker_id") == "worker-terminal"]
    assert worker_actions and worker_actions[0]["process_identity"] == "exact_owner_terminated", (
        "startup recovery report did not expose termination of the exact terminal worker process; "
        f"campaign={configuration.campaign_id.value}; actions={worker_actions}; database={database_path}"
    )


def test_unknown_birth_token_lock_fails_closed_while_pid_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Prevent a second coordinator from stealing a live lock on platforms without birth-token support.
    process_id = 31337
    owner = {
        "process_id": process_id,
        "process_birth_token": f"pid-{process_id}-unknown",
    }
    monkeypatch.setattr(
        "theseus_local.startup_recovery.current_process_birth_token",
        lambda _process_id: None,
    )
    monkeypatch.setattr(
        "theseus_local.startup_recovery._process_exists",
        lambda _process_id: True,
    )
    assert StartupRecoveryLock._owner_is_alive(owner) is True, (
        "startup lock became stealable while its fallback PID was still alive; "
        f"owner={owner}"
    )
    monkeypatch.setattr(
        "theseus_local.startup_recovery._process_exists",
        lambda _process_id: False,
    )
    assert StartupRecoveryLock._owner_is_alive(owner) is False, (
        "startup lock remained live after both birth-token lookup and PID existence failed; "
        f"owner={owner}"
    )


def test_per_mutant_spool_rejects_negative_and_rewritten_identity(tmp_path: Path) -> None:
    # Reject corrupt lease generations and event IDs even when an attacker recomputes the payload hash.
    assignment = {
        "campaign_id": "campaign-e14-spool-identity",
        "shard_id": "shard-e14-spool-identity",
        "lease_id": "lease-e14-spool-identity",
        "attempt": 0,
    }
    worker = {
        "worker_id": "worker-e14-spool-identity",
        "instance_id": "instance-e14-spool-identity",
        "process_id": 1234,
        "process_birth_token": "birth-e14-spool-identity",
    }
    result = {
        "execution_id": "execution-e14-spool-identity",
        "mutant_id": "mutant-e14-spool-identity",
        "status": "killed",
        "classification_reason": "closure audit",
        "restore_verified": True,
        "level_results": [],
        "artifact_paths": [],
        "lease_id": assignment["lease_id"],
        "attempt": 0,
        "test_observations": [],
    }
    with pytest.raises(ValueError, match="must not be negative"):
        build_mutant_spool_frame(
            assignment={**assignment, "attempt": -1},
            worker=worker,
            source_sha256="source-e14-spool-identity",
            mutant_result={**result, "attempt": -1},
        )
    frame = build_mutant_spool_frame(
        assignment=assignment,
        worker=worker,
        source_sha256="source-e14-spool-identity",
        mutant_result=result,
    )
    frame["event_id"] = "0" * 32
    payload = dict(frame["payload"])
    frame["payload_sha256"] = stable_hash(payload)
    spool = DurableExecutionSpool(tmp_path / "spool")
    spool.mutant_events_path.write_text(
        json.dumps(frame, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(SpoolError) as captured:
        spool.mutant_events()
    message = str(captured.value)
    assert "event_id conflicts with deterministic identity" in message, (
        "per-mutant replay accepted a rewritten event ID with a self-consistent payload hash; "
        f"campaign={assignment['campaign_id']}; shard={assignment['shard_id']}; "
        f"lease={assignment['lease_id']}; attempt={assignment['attempt']}; "
        f"mutant={result['mutant_id']}; execution={result['execution_id']}; "
        f"message={message!r}; spool={spool.root}"
    )


def test_coordinator_has_one_recovery_import_and_deterministic_root_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Keep the closure-critical recovery entrypoint singular and its scan order stable.
    coordinator_path = Path(__file__).parents[1] / "theseus_local" / "coordinator.py"
    tree = ast.parse(coordinator_path.read_text(encoding="utf-8"))
    imported_replays = [
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "startup_recovery"
        for alias in node.names
        if alias.name == "replay_finalization"
    ]
    assert imported_replays == ["replay_finalization"], (
        "coordinator imports the finalization recovery boundary more than once; "
        f"path={coordinator_path}; imports={imported_replays}"
    )
    database_path = tmp_path / "state" / "campaign.sqlite3"
    SQLiteMutationStore(database_path).close()
    observed_roots: list[Path] = []
    monkeypatch.setattr(
        "theseus_local.coordinator.inspect_campaign_recovery",
        lambda root: observed_roots.append(Path(root).resolve()) or {"orphaned_campaigns": []},
    )
    LocalCampaignCoordinator.reconcile_startup(database_path)
    expected = [database_path.parent.resolve(), database_path.parent.parent.resolve()]
    assert observed_roots == expected, (
        "startup recovery scanned roots through an unordered collection; "
        f"database={database_path}; expected={expected}; actual={observed_roots}"
    )
