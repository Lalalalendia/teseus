from __future__ import annotations
import inspect
from pathlib import Path
import pytest
from gallifrey_mutation import CampaignState, MutationCampaign, MutationCampaignService, Rejected, SQLiteMutationStore, Success
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    PlanDecision,
    ProjectDescriptor,
    ProjectId,
    ShardDescriptor,
    ShardId,
    StatisticsEventType,
)
from theseus_contracts.campaign import MutationScope
from theseus_contracts.errors import MissingFieldError
from theseus_contracts.events import StatisticsEvent
from theseus_local import LocalCampaignCoordinator
from theseus_statistics import (
    StatisticsEventStore,
    StatisticsProjectionStore,
    plan_decision_events_from_outbox,
    project_plan_decision_outbox,
)
def _configuration(root: Path, campaign_id: str = "campaign-pr21") -> CampaignConfiguration:
    # Build one deterministic domain configuration for planner statistics integration tests.
    return CampaignConfiguration(
        campaign_id=CampaignId(campaign_id),
        project=ProjectDescriptor(ProjectId("project-pr21"), "PR21", str(root)),
        scope=MutationScope("app.py", scope_kind="project"),
        budget=CampaignBudget(max_workers=2),
    )
def _discovering_campaign(
    service: MutationCampaignService,
    configuration: CampaignConfiguration,
) -> MutationCampaign:
    # Advance a campaign to the atomic plan-topology boundary without running the engine.
    created = service.create_campaign(configuration)
    assert isinstance(created, Success)
    campaign = created.value
    for ordinal, target in enumerate(
        (
            CampaignState.PREPARING,
            CampaignState.COLLECTING,
            CampaignState.INDEXING,
            CampaignState.BASELINING,
            CampaignState.DISCOVERING,
        )
    ):
        transitioned = service.apply_transition(
            f"effect.pr21-stage-{ordinal}",
            campaign.campaign_id,
            target,
            expected_revision=campaign.revision_number,
        )
        assert isinstance(transitioned, Success)
        campaign = transitioned.value
    attached = campaign.attach_snapshots(
        expected_revision=campaign.revision_number,
        prepared_snapshot_id="prepared-pr21",
    )
    assert isinstance(attached, Success)
    saved = service.store.save_campaign(
        attached.value,
        expected_revision=campaign.revision_number,
    )
    assert isinstance(saved, Success)
    return saved.value
def _decisions() -> tuple[PlanDecision, ...]:
    # Cover every selected action plus the aggregated exclusion counter in one immutable ledger.
    return (
        PlanDecision("m1", "execute", "fresh-execution", priority=4, estimated_seconds=1.0),
        PlanDecision("m2", "reuse", "exact-reuse", source_event_id="knowledge-event-2"),
        PlanDecision(
            "m3",
            "partial_reuse",
            "partial-reuse",
            source_event_id="knowledge-event-3",
            matched_test_ids=("tests/test_app.py::test_a",),
            missing_test_ids=("tests/test_app.py::test_b",),
        ),
        PlanDecision(
            "m4",
            "audit",
            "reuse-audit",
            audit_selected=True,
            source_event_id="knowledge-event-4",
        ),
        PlanDecision("m5", "budget_excluded", "max_mutants", priority=1, estimated_seconds=1.0),
    )
def _commit_plan_outbox(tmp_path: Path, *, campaign_id: str = "campaign-pr21") -> SQLiteMutationStore:
    # Commit one planner ledger and complete shard topology into the durable mutation outbox.
    configuration = _configuration(tmp_path, campaign_id)
    store = SQLiteMutationStore(tmp_path / f"{campaign_id}.sqlite3")
    service = MutationCampaignService(store)
    campaign = _discovering_campaign(service, configuration)
    committed = service.commit_plan_topology(
        "effect.plan-topology",
        campaign.campaign_id,
        plan_id="plan-pr21",
        prepared_snapshot_id="prepared-pr21",
        selected_count=4,
        shard_descriptors=(
            ShardDescriptor(ShardId("shard-000"), ("m1", "m2"), plan_id="plan-pr21"),
            ShardDescriptor(ShardId("shard-001"), ("m3", "m4"), plan_id="plan-pr21"),
        ),
        expected_revision=campaign.revision_number,
        plan_decisions=_decisions(),
        plan_artifact_sha256="artifact-pr21",
    )
    assert isinstance(committed, Success)
    return store
def test_plan_decision_event_requires_campaign_plan_and_mutant_identity() -> None:
    # Reject a canonical planner event that cannot identify the exact frozen mutant decision.
    with pytest.raises(MissingFieldError, match="mutant_id"):
        StatisticsEvent.create(
            StatisticsEventType.PLAN_DECIDED,
            "theseus.planner.decisions",
            0,
            {"action": "execute", "reason": "fresh-execution"},
            timestamp="2026-08-06T00:00:00Z",
            campaign_id="campaign-pr21",
            plan_id="plan-pr21",
        )
def test_plan_topology_effect_rejects_changed_decision_ledger_on_replay(tmp_path: Path) -> None:
    # Bind the idempotency receipt to the exact frozen decisions and artifact hash.
    configuration = _configuration(tmp_path, "campaign-pr21-conflict")
    store = SQLiteMutationStore(tmp_path / "campaign-pr21-conflict.sqlite3")
    service = MutationCampaignService(store)
    campaign = _discovering_campaign(service, configuration)
    descriptors = (
        ShardDescriptor(ShardId("shard-000"), ("m1", "m2"), plan_id="plan-pr21"),
        ShardDescriptor(ShardId("shard-001"), ("m3", "m4"), plan_id="plan-pr21"),
    )
    first = service.commit_plan_topology(
        "effect.plan-topology",
        campaign.campaign_id,
        plan_id="plan-pr21",
        prepared_snapshot_id="prepared-pr21",
        selected_count=4,
        shard_descriptors=descriptors,
        expected_revision=campaign.revision_number,
        plan_decisions=_decisions(),
        plan_artifact_sha256="artifact-pr21",
    )
    assert isinstance(first, Success)
    changed = (
        PlanDecision("m1", "execute", "changed-reason", priority=4, estimated_seconds=1.0),
        *_decisions()[1:],
    )
    conflict = service.commit_plan_topology(
        "effect.plan-topology",
        campaign.campaign_id,
        plan_id="plan-pr21",
        prepared_snapshot_id="prepared-pr21",
        selected_count=4,
        shard_descriptors=descriptors,
        expected_revision=campaign.revision_number,
        plan_decisions=changed,
        plan_artifact_sha256="artifact-pr21",
    )
    assert isinstance(conflict, Rejected)
    assert conflict.code == "effect_id_conflict"
    store.close()
def test_committed_plan_outbox_projects_exact_replay_safe_statistics(tmp_path: Path) -> None:
    # Derive planner summaries from the durable effect without reading campaign.plan.json.
    mutation_store = _commit_plan_outbox(tmp_path)
    try:
        rows = mutation_store.list_outbox(undelivered_only=True)
        assert isinstance(rows, Success)
        events = plan_decision_events_from_outbox(rows.value)
        assert tuple(item.payload["action"] for item in events) == (
            "execute",
            "reuse",
            "partial_reuse",
            "audit",
            "budget_excluded",
        )
        assert all(item.campaign_id == CampaignId("campaign-pr21") for item in events)
        assert all(item.plan_id is not None and item.plan_id.value == "plan-pr21" for item in events)
        assert tuple(item.producer_sequence for item in events) == tuple(range(5))
        event_store = StatisticsEventStore(tmp_path / "statistics.sqlite3")
        projection_store = StatisticsProjectionStore(event_store)
        assert project_plan_decision_outbox(
            rows.value,
            event_store=event_store,
            projection_store=projection_store,
        ) == (5, 0)
        plan = projection_store.get("plan", "plan-pr21")
        campaign = projection_store.get("campaign", "campaign-pr21")
        assert plan is not None and campaign is not None
        assert plan.plan_decision_count == 5
        assert plan.plan_execute_count == 1
        assert plan.plan_reuse_count == 1
        assert plan.plan_partial_reuse_count == 1
        assert plan.plan_audit_count == 1
        assert plan.plan_excluded_count == 1
        assert campaign.plan_decision_count == 5
        before = plan
        assert project_plan_decision_outbox(
            rows.value,
            event_store=event_store,
            projection_store=projection_store,
        ) == (0, 5)
        assert event_store.count() == 5
        assert projection_store.get("plan", "plan-pr21") == before
    finally:
        mutation_store.close()
def test_statistics_failure_leaves_plan_topology_outbox_pending(tmp_path: Path) -> None:
    # Preserve durable planner evidence when statistics persistence fails after the plan commit.
    mutation_store = _commit_plan_outbox(tmp_path, campaign_id="campaign-pr21-failure")
    calls: list[tuple[dict[str, object], ...]] = []
    class KnowledgeSink:
        def ingest_outbox(self, rows, *, namespace_effects):
            # Record downstream delivery only after canonical statistics succeeds.
            assert namespace_effects is True
            calls.append(tuple(rows))
    class FailingStatisticsStore:
        def append_many(self, events):
            # Simulate a statistics database failure after mutation topology is already durable.
            del events
            raise RuntimeError("statistics unavailable")
    try:
        with pytest.raises(RuntimeError, match="statistics unavailable"):
            LocalCampaignCoordinator._dispatch_outbox(
                mutation_store,
                KnowledgeSink(),
                KnowledgeSink(),
                FailingStatisticsStore(),
                object(),
            )
        pending = mutation_store.list_outbox(undelivered_only=True)
        assert isinstance(pending, Success)
        pending_effect_ids = tuple(item["effect_id"] for item in pending.value)
        assert pending_effect_ids == (
            "effect.pr21-stage-0",
            "effect.pr21-stage-1",
            "effect.pr21-stage-2",
            "effect.pr21-stage-3",
            "effect.pr21-stage-4",
            "effect.plan-topology",
        )
        assert calls == []
        event_store = StatisticsEventStore(tmp_path / "statistics-retry.sqlite3")
        projection_store = StatisticsProjectionStore(event_store)
        assert LocalCampaignCoordinator._dispatch_outbox(
            mutation_store,
            KnowledgeSink(),
            KnowledgeSink(),
            event_store,
            projection_store,
        ) == len(pending_effect_ids)
        assert tuple(item["effect_id"] for item in calls[0]) == pending_effect_ids
        assert tuple(item["effect_id"] for item in calls[1]) == pending_effect_ids
        after = mutation_store.list_outbox(undelivered_only=True)
        assert isinstance(after, Success) and after.value == ()
        assert event_store.count() == 5
    finally:
        mutation_store.close()
def test_coordinator_dispatches_planner_statistics_after_atomic_plan_commit() -> None:
    # Lock ordering so statistics failure cannot roll back or precede authoritative topology commit.
    run_source = inspect.getsource(LocalCampaignCoordinator.run)
    commit_position = run_source.index("service.commit_plan_topology(")
    dispatch_position = run_source.index("self._dispatch_outbox(", commit_position)
    assert commit_position < dispatch_position
    dispatch_source = inspect.getsource(LocalCampaignCoordinator._dispatch_outbox)
    assert dispatch_source.index("project_plan_decision_outbox(") < dispatch_source.index(
        "store.acknowledge_outbox"
    )
