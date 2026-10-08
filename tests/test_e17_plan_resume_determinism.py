from __future__ import annotations
import inspect
import json
from pathlib import Path
import pytest
from gallifrey_mutation import (
    CampaignState,
    Failed,
    InMemoryMutationStore,
    MutationCampaign,
    MutationCampaignService,
    MutationShard,
    Rejected,
    SQLiteMutationStore,
    Success,
)
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    MutantDescriptor,
    MutantId,
    MutationScope,
    PreparedCampaign,
    ProjectDescriptor,
    ProjectId,
)
from theseus_knowledge import KnowledgePlaneStore
from theseus_local import LocalCampaignCoordinator
from theseus_local.startup_recovery import (
    StartupRecoveryError,
    validate_campaign_plan_topology,
)
from theseus_planner import CampaignPlanner


def _configuration(root: Path, *, workers: int = 2) -> CampaignConfiguration:
    # Build one stable planner and domain request for resume tests.
    return CampaignConfiguration(
        campaign_id=CampaignId("campaign-e17-resume"),
        project=ProjectDescriptor(
            project_id=ProjectId("project-e17-resume"),
            display_name="E17 resume fixture",
            root_path=str(root),
        ),
        scope=MutationScope("app.py", scope_kind="project"),
        budget=CampaignBudget(max_workers=workers),
        no_escalation=True,
        reports_dir=str(root / "reports"),
    )


def _mutant(mutant_id: str, line_no: int) -> MutantDescriptor:
    # Create one deterministic planner candidate.
    return MutantDescriptor(
        MutantId(mutant_id),
        "condition_to_not",
        "app.py",
        line_no,
        1,
        "old",
        "new",
    )


def _prepared(mutants: tuple[MutantDescriptor, ...]) -> PreparedCampaign:
    # Bind one catalog to an immutable preparation identity.
    return PreparedCampaign(
        CampaignId("campaign-e17-resume"),
        "app.py",
        "source-resume",
        "index-resume",
        mutants,
        snapshot_id="prepared-resume",
    )


def _discovering_campaign(
    service: MutationCampaignService,
    configuration: CampaignConfiguration,
) -> MutationCampaign:
    # Advance a new campaign to the exact pre-plan state without engine dependencies.
    current = service.create_campaign(configuration)
    assert isinstance(current, Success)
    campaign = current.value
    for ordinal, target in enumerate(
        (
            CampaignState.PREPARING,
            CampaignState.COLLECTING,
            CampaignState.INDEXING,
            CampaignState.BASELINING,
            CampaignState.DISCOVERING,
        )
    ):
        outcome = service.apply_transition(
            f"effect.resume-stage-{ordinal}",
            campaign.campaign_id,
            target,
            expected_revision=campaign.revision_number,
        )
        assert isinstance(outcome, Success)
        campaign = outcome.value
    attached = campaign.attach_snapshots(
        expected_revision=campaign.revision_number,
        prepared_snapshot_id="prepared-resume",
    )
    assert isinstance(attached, Success)
    saved = service.store.save_campaign(
        attached.value,
        expected_revision=campaign.revision_number,
    )
    assert isinstance(saved, Success)
    return saved.value


def test_resume_loads_existing_plan_without_running_planner_or_rewriting_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Treat the persisted plan as authority before any planner algorithm can run again.
    configuration = _configuration(tmp_path)
    mutants = (_mutant("m1", 1), _mutant("m2", 2))
    prepared = _prepared(mutants)
    plan = CampaignPlanner().build(configuration, prepared, mutants=mutants)
    reports = tmp_path / "reports"
    reports.mkdir()
    plan_path = reports / "campaign.plan.json"
    plan_path.write_text(
        json.dumps(plan.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    original = plan_path.read_bytes()

    def forbidden_build(self, *args, **kwargs):
        # Prove resume validation cannot silently fall back to a second planning pass.
        raise AssertionError("CampaignPlanner.build must not run during resume")

    monkeypatch.setattr(CampaignPlanner, "build", forbidden_build)
    loaded = LocalCampaignCoordinator._load_or_build_campaign_plan(
        configuration,
        prepared,
        reports,
        mutants=mutants,
        existing_plan_id=plan.plan_id,
        reuse_decisions={},
        audit_policy=None,
    )
    assert loaded == plan
    assert plan_path.read_bytes() == original


def test_resume_uses_frozen_reuse_plan_after_knowledge_history_changes(tmp_path: Path) -> None:
    # Keep the campaign plan stable when unrelated knowledge is appended after planning.
    configuration = _configuration(tmp_path)
    mutants = (_mutant("m1", 1),)
    prepared = _prepared(mutants)
    reports = tmp_path / "reports"
    reports.mkdir()
    knowledge = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    try:
        reuse_plan = knowledge.write_reuse_plan(
            reports / "reuse.plan.json",
            ({"mutant_id": "m1"},),
            campaign_id=configuration.campaign_id.value,
            reuse_mode="hint",
        )
        decisions = {item.mutant_id: item for item in reuse_plan.decisions}
        campaign_plan = CampaignPlanner().build(
            configuration,
            prepared,
            mutants=mutants,
            reuse_decisions=decisions,
        )
        (reports / "campaign.plan.json").write_text(
            json.dumps(campaign_plan.to_dict(), ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        knowledge.invalidate(
            scope_type="mutant",
            scope_key="unrelated-mutant",
            reason="history changed after campaign planning",
        )
        frozen = LocalCampaignCoordinator._load_reuse_plan_artifact(
            reports / "reuse.plan.json"
        )
        loaded = LocalCampaignCoordinator._load_or_build_campaign_plan(
            configuration,
            prepared,
            reports,
            mutants=mutants,
            existing_plan_id=campaign_plan.plan_id,
            reuse_decisions={item.mutant_id: item for item in frozen.decisions},
            audit_policy=None,
        )
        assert frozen == reuse_plan
        assert loaded == campaign_plan
    finally:
        knowledge.close()


def test_resume_rejects_tampered_frozen_reuse_plan(tmp_path: Path) -> None:
    # Reject changed decision content before it can authorize execution under a stored campaign plan.
    path = tmp_path / "reuse.plan.json"
    payload = {
        "schema_version": 2,
        "reuse_mode": "hint",
        "history_revision": 0,
        "input_fingerprint": "input",
        "plan_fingerprint": "tampered",
        "decisions": [
            {
                "mutant_id": "m1",
                "kind": "none",
                "eligible": False,
                "reason": "fresh execution",
                "source_event_id": None,
                "source_execution_id": None,
                "result_status": None,
                "evidence_quality": "unknown",
                "matched_test_ids": [],
                "missing_test_ids": [],
                "audit_required": False,
                "source_compacted": False,
                "blockers": [],
                "authorized": False,
                "reuse_mode": "hint",
            }
        ],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="fingerprint mismatch"):
        LocalCampaignCoordinator._load_reuse_plan_artifact(path)


def test_resume_rejects_configuration_catalog_and_missing_plan_drift(tmp_path: Path) -> None:
    # Block every immutable input drift instead of creating a replacement plan.
    configuration = _configuration(tmp_path)
    mutants = (_mutant("m1", 1), _mutant("m2", 2))
    prepared = _prepared(mutants)
    plan = CampaignPlanner().build(configuration, prepared, mutants=mutants)
    reports = tmp_path / "reports"
    reports.mkdir()
    (reports / "campaign.plan.json").write_text(
        json.dumps(plan.to_dict(), ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="immutable planning inputs"):
        LocalCampaignCoordinator._load_or_build_campaign_plan(
            _configuration(tmp_path, workers=3),
            prepared,
            reports,
            mutants=mutants,
            existing_plan_id=plan.plan_id,
            reuse_decisions={},
            audit_policy=None,
        )
    changed_catalog = (*mutants, _mutant("m3", 3))
    with pytest.raises(RuntimeError, match="immutable planning inputs"):
        LocalCampaignCoordinator._load_or_build_campaign_plan(
            configuration,
            _prepared(changed_catalog),
            reports,
            mutants=changed_catalog,
            existing_plan_id=plan.plan_id,
            reuse_decisions={},
            audit_policy=None,
        )
    (reports / "campaign.plan.json").unlink()
    with pytest.raises(RuntimeError, match="missing immutable campaign plan"):
        LocalCampaignCoordinator._load_or_build_campaign_plan(
            configuration,
            prepared,
            reports,
            mutants=mutants,
            existing_plan_id=plan.plan_id,
            reuse_decisions={},
            audit_policy=None,
        )


def test_plan_topology_commit_is_atomic_and_idempotent_in_memory(tmp_path: Path) -> None:
    # Commit campaign binding and all shard rows once with exact replay semantics.
    configuration = _configuration(tmp_path)
    mutants = (_mutant("m1", 1), _mutant("m2", 2))
    plan = CampaignPlanner().build(configuration, _prepared(mutants), mutants=mutants)
    store = InMemoryMutationStore()
    service = MutationCampaignService(store)
    discovering = _discovering_campaign(service, configuration)
    first = service.commit_plan_topology(
        "effect.plan-topology",
        discovering.campaign_id,
        plan_id=plan.plan_id,
        prepared_snapshot_id=str(plan.prepared_snapshot_id),
        selected_count=plan.selected_count,
        shard_descriptors=plan.shards,
        expected_revision=discovering.revision_number,
    )
    assert isinstance(first, Success)
    assert first.value.status == CampaignState.PLANNING
    shards = store.list_shards(discovering.campaign_id)
    assert isinstance(shards, Success)
    assert tuple(item.shard_id.value for item in shards.value) == tuple(
        item.shard_id.value for item in plan.shards
    )
    revisions = (
        first.value.revision_number,
        tuple(item.revision_number for item in shards.value),
    )
    replay = service.commit_plan_topology(
        "effect.plan-topology",
        discovering.campaign_id,
        plan_id=plan.plan_id,
        prepared_snapshot_id=str(plan.prepared_snapshot_id),
        selected_count=plan.selected_count,
        shard_descriptors=plan.shards,
        expected_revision=discovering.revision_number,
    )
    assert isinstance(replay, Success) and replay.duplicate
    after = store.list_shards(discovering.campaign_id)
    assert isinstance(after, Success)
    assert revisions == (
        replay.value.revision_number,
        tuple(item.revision_number for item in after.value),
    )
    conflict = service.commit_plan_topology(
        "effect.plan-topology",
        discovering.campaign_id,
        plan_id="plan-foreign",
        prepared_snapshot_id=str(plan.prepared_snapshot_id),
        selected_count=plan.selected_count,
        shard_descriptors=plan.shards,
        expected_revision=discovering.revision_number,
    )
    assert isinstance(conflict, Rejected)
    assert conflict.code == "effect_id_conflict"


def test_partial_topology_is_rejected_instead_of_completed(tmp_path: Path) -> None:
    # Refuse to fill missing shard rows after any non-atomic topology write.
    configuration = _configuration(tmp_path)
    mutants = (_mutant("m1", 1), _mutant("m2", 2))
    plan = CampaignPlanner().build(configuration, _prepared(mutants), mutants=mutants)
    store = InMemoryMutationStore()
    service = MutationCampaignService(store)
    discovering = _discovering_campaign(service, configuration)
    partial = MutationShard.from_descriptor(
        discovering.campaign_id,
        plan.shards[0],
        ordinal=0,
    )
    created = service.create_shard(partial)
    assert isinstance(created, Success)
    outcome = service.commit_plan_topology(
        "effect.plan-topology",
        discovering.campaign_id,
        plan_id=plan.plan_id,
        prepared_snapshot_id=str(plan.prepared_snapshot_id),
        selected_count=plan.selected_count,
        shard_descriptors=plan.shards,
        expected_revision=discovering.revision_number,
    )
    assert isinstance(outcome, Rejected)
    assert outcome.code == "partial_plan_topology"
    campaign = store.get_campaign(discovering.campaign_id)
    assert isinstance(campaign, Success)
    assert campaign.value == discovering


def test_sqlite_plan_topology_failure_rolls_back_campaign_shards_and_effect(tmp_path: Path) -> None:
    # Leave either the complete topology or the old campaign state after an injected SQLite failure.
    configuration = _configuration(tmp_path)
    mutants = (_mutant("m1", 1), _mutant("m2", 2))
    plan = CampaignPlanner().build(configuration, _prepared(mutants), mutants=mutants)
    store = SQLiteMutationStore(tmp_path / "campaign.sqlite3")
    service = MutationCampaignService(store)
    discovering = _discovering_campaign(service, configuration)
    store._connection.execute(
        """
        CREATE TRIGGER fail_plan_topology
        BEFORE INSERT ON mutation_shards
        WHEN NEW.shard_id = 'shard-001'
        BEGIN
            SELECT RAISE(ABORT, 'injected plan topology failure');
        END;
        """
    )
    store._connection.commit()
    outcome = service.commit_plan_topology(
        "effect.plan-topology",
        discovering.campaign_id,
        plan_id=plan.plan_id,
        prepared_snapshot_id=str(plan.prepared_snapshot_id),
        selected_count=plan.selected_count,
        shard_descriptors=plan.shards,
        expected_revision=discovering.revision_number,
    )
    assert isinstance(outcome, Failed)
    campaign = store.get_campaign(discovering.campaign_id)
    shards = store.list_shards(discovering.campaign_id)
    effect = store.get_effect("effect.plan-topology")
    assert isinstance(campaign, Success) and campaign.value == discovering
    assert isinstance(shards, Success) and shards.value == ()
    assert isinstance(effect, Success) and effect.value is None
    store.close()


def test_runtime_cannot_replan_or_materialize_individual_missing_shards() -> None:
    # Lock the coordinator source against a return to per-shard recovery or resume planning.
    load_source = inspect.getsource(LocalCampaignCoordinator._load_or_build_campaign_plan)
    execute_source = inspect.getsource(LocalCampaignCoordinator._execute_parallel_shards)
    assert load_source.index("if path.is_file()") < load_source.index("planner.build(")
    run_source = inspect.getsource(LocalCampaignCoordinator.run)
    assert "service.create_shard" not in execute_source
    assert "stored shard topology is not the complete immutable campaign plan" in execute_source
    assert "_load_reuse_plan_artifact" in run_source
    assert run_source.index("if current.plan_id or campaign_plan_path.is_file()") < run_source.index(
        "knowledge.invalidate_changed_inputs"
    )
    assert run_source.index("campaign_plan = self._load_or_build_campaign_plan(") < run_source.index(
        "if protect_frozen_reuse:"
    )


def test_startup_recovery_rejects_missing_or_partial_plan_authority(tmp_path: Path) -> None:
    # Fail closed when restart sees a bound campaign without its full plan artifact and topology.
    configuration = _configuration(tmp_path)
    mutants = (_mutant("m1", 1), _mutant("m2", 2))
    plan = CampaignPlanner().build(configuration, _prepared(mutants), mutants=mutants)
    database_path = tmp_path / "state" / "campaign.sqlite3"
    store = SQLiteMutationStore(database_path)
    service = MutationCampaignService(store)
    discovering = _discovering_campaign(service, configuration)
    committed = discovering.commit_plan(
        plan.plan_id,
        str(plan.prepared_snapshot_id),
        plan.selected_count,
        expected_revision=discovering.revision_number,
    )
    assert isinstance(committed, Success)
    saved = store.save_campaign(
        committed.value,
        expected_revision=discovering.revision_number,
    )
    assert isinstance(saved, Success)
    plan_path = database_path.parent / "campaign.plan.json"
    plan_path.write_text(json.dumps(plan.to_dict(), ensure_ascii=False) + "\n", encoding="utf-8")
    partial = MutationShard.from_descriptor(
        committed.value.campaign_id,
        plan.shards[0],
        ordinal=0,
    )
    assert isinstance(store.save_shard(partial, expected_revision=None), Success)
    with pytest.raises(StartupRecoveryError, match="partially materialized"):
        validate_campaign_plan_topology(
            database_path,
            store,
            committed.value,
            allow_unmaterialized=True,
        )
    store.close()

    clean_path = tmp_path / "missing" / "campaign.sqlite3"
    clean_store = SQLiteMutationStore(clean_path)
    clean_service = MutationCampaignService(clean_store)
    clean_discovering = _discovering_campaign(clean_service, configuration)
    clean_committed = clean_discovering.commit_plan(
        plan.plan_id,
        str(plan.prepared_snapshot_id),
        plan.selected_count,
        expected_revision=clean_discovering.revision_number,
    )
    assert isinstance(clean_committed, Success)
    assert isinstance(
        clean_store.save_campaign(
            clean_committed.value,
            expected_revision=clean_discovering.revision_number,
        ),
        Success,
    )
    with pytest.raises(StartupRecoveryError, match="requires a valid campaign plan"):
        validate_campaign_plan_topology(
            clean_path,
            clean_store,
            clean_committed.value,
            allow_unmaterialized=True,
        )
    clean_store.close()
