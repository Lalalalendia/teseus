from pathlib import Path
from theseus_contracts import (
    ArtifactRegistryEntry,
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    ExecutionId,
    FinalizationIntent,
    MutantId,
    MutantExecutionResult,
    MutationScope,
    ProjectDescriptor,
    ProjectId,
    ShardDescriptor,
    ShardExecutionResult,
    ShardId,
    ShardLease,
    WorkerId,
)
from theseus_contracts.enums import WorkerStatus
from gallifrey_mutation import (
    CampaignState,
    InMemoryMutationStore,
    MutationCampaign,
    MutationCampaignService,
    MutationShard,
    MutationShardState,
    SQLiteMutationStore,
    Success,
)
def _configuration(tmp_path: Path, campaign_id: str = "cmp_domain") -> CampaignConfiguration:
    # Build a dependency-free contract fixture with a safe argv test command.
    return CampaignConfiguration(
        campaign_id=CampaignId(campaign_id),
        project=ProjectDescriptor(
            project_id=ProjectId("project_domain"),
            display_name="domain fixture",
            root_path=str(tmp_path),
        ),
        scope=MutationScope(source_path="app.py"),
        budget=CampaignBudget(max_mutants=2, max_workers=1),
    )
def _advance_to_running(service: MutationCampaignService, campaign_id: CampaignId) -> MutationCampaign:
    # Walk the exact domain lifecycle so tests exercise legal transitions rather than private state edits.
    current = service._campaign(campaign_id)
    assert isinstance(current, Success)
    stages = [
        ("prepare", service.prepare),
        ("collect", service.collect),
        ("index", service.index),
        ("baseline", service.baseline),
        ("discover", service.discover),
        ("plan", service.plan),
        ("start", service.start),
    ]
    for index, (name, handler) in enumerate(stages):
        # Apply a unique effect identity so each state transition is independently replayable.
        result = handler(f"effect_{name}", campaign_id, expected_revision=current.value.revision_number)
        assert isinstance(result, Success), result
        current = result
    return current.value
def test_campaign_state_machine_rejects_stale_and_illegal_transitions(tmp_path: Path) -> None:
    # Verify the mutation campaign is its own aggregate with strict ordered lifecycle states.
    store = InMemoryMutationStore()
    service = MutationCampaignService(store)
    created = service.create_campaign(_configuration(tmp_path))
    assert isinstance(created, Success)
    illegal = service.start("effect_start_too_early", created.value.campaign_id, expected_revision=0)
    assert illegal.code == "invalid_campaign_transition"
    prepared = service.prepare("effect_prepare", created.value.campaign_id, expected_revision=0)
    assert isinstance(prepared, Success)
    stale = service.collect("effect_collect_stale", created.value.campaign_id, expected_revision=0)
    assert stale.code == "stale_revision"
def test_duplicate_effect_and_completed_campaign_are_immutable(tmp_path: Path) -> None:
    # Verify replay returns the original projection and cannot rewrite terminal campaign state.
    store = InMemoryMutationStore()
    service = MutationCampaignService(store)
    created = service.create_campaign(_configuration(tmp_path, "cmp_replay"))
    assert isinstance(created, Success)
    first = service.prepare("effect_prepare", created.value.campaign_id, expected_revision=0)
    replay = service.prepare("effect_prepare", created.value.campaign_id, expected_revision=999)
    assert isinstance(first, Success)
    assert isinstance(replay, Success) and replay.duplicate
    assert replay.value == first.value
    current = _advance_to_running(service, created.value.campaign_id)
    for name, handler in [("aggregate", service.aggregate), ("materialize", service.materialize)]:
        result = handler(f"effect_{name}_replay", created.value.campaign_id, expected_revision=current.revision_number)
        assert isinstance(result, Success)
        current = result.value
    artifact = ArtifactRegistryEntry(
        campaign_id=created.value.campaign_id,
        logical_key="canonical.report.json",
        logical_role="canonical_report",
        content_sha256="0" * 64,
        size_bytes=2,
        schema_version=1,
        producer="test_e10_mutation_domain",
        content_path="artifacts/sha256/00/" + ("0" * 64),
        logical_path="canonical.report.json",
        created_at="2026-01-01T00:00:00Z",
    )
    intent = FinalizationIntent(
        intent_id="intent-cmp-replay",
        campaign_id=created.value.campaign_id,
        status="created",
        required_logical_keys=(artifact.logical_key,),
        canonical_report_key=artifact.logical_key,
        artifacts=(artifact,),
        result_fingerprint="result-cmp-replay",
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
    )
    created_intent = service.create_finalization_intent(
        "effect_finalize_intent",
        created.value.campaign_id,
        intent,
    )
    assert isinstance(created_intent, Success), created_intent
    ready = service.register_finalization(
        "effect_finalize_registry",
        created.value.campaign_id,
        intent,
        (artifact,),
        expected_revision=current.revision_number,
    )
    assert isinstance(ready, Success), ready
    final = service.complete_finalization(
        "effect_finalize",
        created.value.campaign_id,
        expected_revision=ready.value.revision_number,
    )
    assert isinstance(final, Success), final
    assert final.value.status == CampaignState.COMPLETED
    blocked = service.prepare("effect_after_complete", created.value.campaign_id, expected_revision=final.value.revision_number)
    assert blocked.code == "campaign_immutable"
def test_cancel_during_preparation_and_execution_is_terminal(tmp_path: Path) -> None:
    # Verify cancellation is legal at both coordinator boundaries and never reopens a campaign.
    preparation_store = InMemoryMutationStore()
    preparation_service = MutationCampaignService(preparation_store)
    created = preparation_service.create_campaign(_configuration(tmp_path, "cmp_cancel_prepare"))
    assert isinstance(created, Success)
    cancelled = preparation_service.cancel("effect_cancel_prepare", created.value.campaign_id, expected_revision=0)
    assert isinstance(cancelled, Success)
    assert cancelled.value.status == CampaignState.CANCELLED
    reopened = preparation_service.prepare("effect_reopen", created.value.campaign_id, expected_revision=cancelled.value.revision_number)
    assert reopened.code == "campaign_immutable"
    execution_store = InMemoryMutationStore()
    execution_service = MutationCampaignService(execution_store)
    execution_created = execution_service.create_campaign(_configuration(tmp_path, "cmp_cancel_execute"))
    assert isinstance(execution_created, Success)
    running = _advance_to_running(execution_service, execution_created.value.campaign_id)
    cancelled_running = execution_service.cancel(
        "effect_cancel_execute",
        execution_created.value.campaign_id,
        expected_revision=running.revision_number,
    )
    assert isinstance(cancelled_running, Success)
    assert cancelled_running.value.status == CampaignState.CANCELLED
def test_shard_fan_in_records_partial_result_and_duplicate_execution(tmp_path: Path) -> None:
    # Verify shard progress, execution identity and duplicate result handling remain deterministic.
    store = InMemoryMutationStore()
    service = MutationCampaignService(store)
    created = service.create_campaign(_configuration(tmp_path, "cmp_shard"))
    assert isinstance(created, Success)
    running = _advance_to_running(service, created.value.campaign_id)
    discovered = running.record_discovery(2, expected_revision=running.revision_number)
    assert isinstance(discovered, Success)
    saved_campaign = store.save_campaign(discovered.value, expected_revision=running.revision_number)
    assert isinstance(saved_campaign, Success)
    shard = MutationShard.from_descriptor(
        created.value.campaign_id,
        ShardDescriptor(ShardId("shard_1"), ("mutant_1", "mutant_2"), estimated_cost=2.0),
        ordinal=0,
    )
    assert isinstance(service.create_shard(shard), Success)
    lease = ShardLease(WorkerId("worker_1"), "lease_1", WorkerStatus.RUNNING, 30.0, "2026-01-01T00:00:00Z")
    claimed = shard.claim(lease, expected_revision=0)
    assert isinstance(claimed, Success)
    started = claimed.value.start(expected_revision=claimed.value.revision_number)
    assert isinstance(started, Success)
    assert isinstance(store.save_shard(claimed.value, expected_revision=0), Success) is True
    assert isinstance(store.save_shard(started.value, expected_revision=claimed.value.revision_number), Success)
    result = ShardExecutionResult(
        shard_id=ShardId("shard_1"),
        worker_id=WorkerId("worker_1"),
        status=WorkerStatus.RUNNING,
        completed_mutants=1,
        results=(
            MutantExecutionResult(
                execution_id=ExecutionId("exec_1"),
                mutant_id=MutantId("mutant_1"),
                status="killed",
                classification_reason="assertion",
                restore_verified=True,
                level_results=({"nodeids": ["test_app.py::test_one"]},),
                lease_id="lease_1",
                duration_seconds=1.25,
            ),
        ),
    )
    recorded = service.record_shard_result(
        "effect_shard_result",
        created.value.campaign_id,
        ShardId("shard_1"),
        result,
        expected_campaign_revision=saved_campaign.value.revision_number,
        now="2026-01-01T00:00:01Z",
    )
    assert isinstance(recorded, Success)
    assert recorded.value.shard.status == MutationShardState.PARTIAL
    assert recorded.value.executions[0].duration_seconds == 1.25
    replay = service.record_shard_result(
        "effect_shard_result",
        created.value.campaign_id,
        ShardId("shard_1"),
        result,
        expected_campaign_revision=999,
    )
    assert isinstance(replay, Success) and replay.duplicate
    assert replay.value == recorded.value
def test_sqlite_store_survives_restart_and_enforces_compare_and_swap(tmp_path: Path) -> None:
    # Verify the first durable repository keeps aggregate and effect identity across connections.
    database = tmp_path / "theseus-mutation.sqlite3"
    first = SQLiteMutationStore(database)
    service = MutationCampaignService(first)
    created = service.create_campaign(_configuration(tmp_path, "cmp_sqlite"))
    assert isinstance(created, Success)
    prepared = service.prepare("effect_prepare", created.value.campaign_id, expected_revision=0)
    assert isinstance(prepared, Success)
    first.close()
    second = SQLiteMutationStore(database)
    loaded = second.get_campaign(created.value.campaign_id)
    assert isinstance(loaded, Success)
    assert loaded.value == prepared.value
    stale = second.save_campaign(prepared.value, expected_revision=0)
    assert stale.code == "stale_revision"
    replay = MutationCampaignService(second).prepare("effect_prepare", created.value.campaign_id, expected_revision=123)
    assert isinstance(replay, Success) and replay.duplicate
    second.close()
def test_sqlite_effect_bundle_rolls_back_when_outbox_boundary_fails(tmp_path: Path) -> None:
    # Keep aggregate, receipt and outbox changes all-or-nothing at the final durable boundary.
    database = tmp_path / "effect-boundary.sqlite3"
    store = SQLiteMutationStore(database)
    service = MutationCampaignService(store)
    created = service.create_campaign(_configuration(tmp_path, "cmp_atomic"))
    assert isinstance(created, Success)
    store._connection.execute(
        "INSERT INTO mutation_outbox(effect_id, event_type, campaign_id, payload, created_at, delivered_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ("effect_prepare_atomic", "injected", "cmp_atomic", "{}", "2026-01-01T00:00:00Z", None),
    )
    store._connection.commit()
    prepared = service.prepare("effect_prepare_atomic", created.value.campaign_id, expected_revision=0)
    assert not isinstance(prepared, Success)
    current = store.get_campaign(created.value.campaign_id)
    assert isinstance(current, Success)
    assert current.value == created.value
    effect = store.get_effect("effect_prepare_atomic")
    assert isinstance(effect, Success)
    assert effect.value is None
    store.close()
