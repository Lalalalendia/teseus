from __future__ import annotations
import json
from pathlib import Path
from types import SimpleNamespace
from theseus_contracts import ReuseMode
from theseus_knowledge import (
    KnowledgePlaneStore,
    ReuseAuditPolicy,
    ReuseDecision,
    ReuseKind,
    ReusePlanArtifact,
    compare_reuse_audit,
    should_sample_reuse_audit,
)
from theseus_local import LocalCampaignCoordinator
from theseus_planner import CampaignPlanner
def _execution(
    execution_id: str,
    *,
    environment: str = "environment-v1",
    result: str = "result-v1",
) -> dict[str, object]:
    # Build one fully validated execution for invalidation and quarantine closure tests.
    return {
        "execution_id": execution_id,
        "mutant_id": "mutant-1",
        "attempt": 0,
        "semantic_result": "killed",
        "status": "complete",
        "restore_verified": True,
        "evidence_schema_version": 2,
        "source_kind": "observed",
        "evidence_origin": "pytest_events",
        "function_id": "app.choose",
        "function_fingerprint": "function-v1",
        "mutant_fingerprint": "mutant-v1",
        "test_fingerprint": "tests-v1",
        "conftest_fingerprint": "conftest-v1",
        "environment_fingerprint": environment,
        "selection_fingerprint": "selection-v1",
        "result_fingerprint": result,
        "test_observations": [
            {
                "test_id": "test_app.py::test_choose",
                "test_fingerprint": "test-node-v1",
                "outcome": "failed",
                "evidence_kind": "pytest_test_event",
                "observation_schema_version": 1,
            }
        ],
    }
def _request(*, environment: str = "environment-v1", result: str = "result-v1") -> dict[str, object]:
    # Build the complete current reuse boundary corresponding to one execution fixture.
    return {
        "mutant_id": "mutant-1",
        "function_id": "app.choose",
        "function_fingerprint": "function-v1",
        "mutant_fingerprint": "mutant-v1",
        "test_fingerprint": "tests-v1",
        "test_fingerprints": {"test_app.py::test_choose": "test-node-v1"},
        "conftest_fingerprint": "conftest-v1",
        "environment_fingerprint": environment,
        "selection_fingerprint": "selection-v1",
        "result_fingerprint": result,
    }
def _ingest(
    store: KnowledgePlaneStore,
    execution_id: str,
    *,
    observed_at: str,
    environment: str = "environment-v1",
    result: str = "result-v1",
) -> None:
    # Commit one execution at an explicit timestamp so invalidation ordering remains deterministic.
    store.ingest_effect(
        effect_id=f"effect-{execution_id}",
        campaign_id="campaign-source",
        effect_type="mutation.execute_shard",
        payload={"executions": [_execution(execution_id, environment=environment, result=result)]},
        observed_at=observed_at,
    )
def test_automatic_invalidation_is_idempotent_and_newer_evidence_wins(tmp_path: Path) -> None:
    # Replaying the same input transition must not advance revision or block later matching evidence.
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    try:
        _ingest(store, "execution-old", observed_at="2026-01-01T00:00:00Z")
        request = _request(environment="environment-v2", result="result-v2")
        first = store.invalidate_changed_inputs(
            (request,),
            campaign_id="campaign-current",
            invalidated_at="2026-01-02T00:00:00Z",
        )
        revision_after_first = store.query_executions(campaign_id="campaign-source").snapshot_revision
        second = store.invalidate_changed_inputs(
            (request,),
            campaign_id="campaign-resume",
            invalidated_at="2026-01-03T00:00:00Z",
        )
        revision_after_second = store.query_executions(campaign_id="campaign-source").snapshot_revision
        blocked = store.decide_reuse(**request)
        _ingest(
            store,
            "execution-new",
            observed_at="2026-01-04T00:00:00Z",
            environment="environment-v2",
            result="result-v2",
        )
        allowed = store.decide_reuse(**request)
    finally:
        store.close()
    assert first == second
    assert revision_after_second == revision_after_first
    assert blocked.kind is ReuseKind.NONE
    assert allowed.kind is ReuseKind.EXACT and allowed.eligible
def test_incident_quarantine_and_release_are_idempotent_and_fail_closed(tmp_path: Path) -> None:
    # Deduplicate repeated audit mismatch handling and require explicit release before reuse resumes.
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    expected = _execution("execution-source")
    actual = dict(expected) | {"semantic_result": "survived"}
    try:
        _ingest(store, "execution-source", observed_at="2026-01-01T00:00:00Z")
        first_incident = store.record_reuse_incident(
            campaign_id="campaign-audit",
            mutant_id="mutant-1",
            rule_id="app.choose",
            kind="exact",
            mismatches=("semantic_mismatch",),
            expected_payload=expected,
            actual_payload=actual,
            created_at="2026-01-02T00:00:00Z",
        )
        first_revision = store.query_executions(campaign_id="campaign-source").snapshot_revision
        second_incident = store.record_reuse_incident(
            campaign_id="campaign-audit",
            mutant_id="mutant-1",
            rule_id="app.choose",
            kind="exact",
            mismatches=("semantic_mismatch",),
            expected_payload=expected,
            actual_payload=actual,
            created_at="2026-01-03T00:00:00Z",
        )
        second_revision = store.query_executions(campaign_id="campaign-source").snapshot_revision
        store.quarantine_reuse_rule(
            scope_type="mutant_fingerprint",
            scope_key="mutant-v1",
            reason="fresh reuse audit mismatch: semantic_mismatch",
            incident_id=first_incident,
            created_at="2026-01-02T00:00:00Z",
        )
        quarantine_revision = store.query_executions(campaign_id="campaign-source").snapshot_revision
        store.quarantine_reuse_rule(
            scope_type="mutant_fingerprint",
            scope_key="mutant-v1",
            reason="fresh reuse audit mismatch: semantic_mismatch",
            incident_id=second_incident,
            created_at="2026-01-03T00:00:00Z",
        )
        repeated_quarantine_revision = store.query_executions(campaign_id="campaign-source").snapshot_revision
        _ingest(store, "execution-new", observed_at="2026-01-04T00:00:00Z")
        blocked = store.decide_reuse(**_request())
        assert store.release_reuse_quarantine(
            scope_type="mutant_fingerprint",
            scope_key="mutant-v1",
            released_at="2026-01-05T00:00:00Z",
        )
        assert not store.release_reuse_quarantine(
            scope_type="mutant_fingerprint",
            scope_key="mutant-v1",
            released_at="2026-01-06T00:00:00Z",
        )
        allowed = store.decide_reuse(**_request())
        closure = store.closure_audit()
    finally:
        store.close()
    assert first_incident == second_incident
    assert second_revision == first_revision
    assert repeated_quarantine_revision == quarantine_revision
    assert blocked.kind is ReuseKind.HISTORICAL_HINT and not blocked.authorized
    assert any(item.startswith("reuse-quarantined:mutant_fingerprint:") for item in blocked.blockers)
    assert allowed.kind is ReuseKind.EXACT and allowed.eligible
    assert closure["e16_closed"]
    assert closure["reuse_invariants"]["incident_count"] == 1
    assert closure["reuse_invariants"]["quarantine_count"] == 1
    assert closure["reuse_invariants"]["active_quarantine_count"] == 0
def test_e16_closure_audit_detects_an_orphaned_quarantine(tmp_path: Path) -> None:
    # Fail the bounded closure diagnostic when quarantine provenance references no durable incident.
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    try:
        store._connection.execute(
            "INSERT INTO knowledge_reuse_quarantine(scope_type, scope_key, incident_id, reason, created_at, released_at, success_count) "
            "VALUES ('mutant', 'mutant-orphan', 'missing-incident', 'orphan', '2026-01-01T00:00:00.000000Z', NULL, 0)"
        )
        store._connection.commit()
        closure = store.closure_audit()
    finally:
        store.close()
    assert not closure["e16_closed"]
    assert "quarantine-incident:mutant:mutant-orphan" in closure["reuse_invariants"]["violations"]
def test_audit_progress_is_plan_bound_and_running_rows_are_resume_fenced(tmp_path: Path) -> None:
    # Bind audit progress to both plans and retain an interrupted marker instead of sampling twice.
    decision = ReuseDecision(
        mutant_id="mutant-1",
        kind=ReuseKind.EXACT,
        eligible=True,
        reason="exact",
        source_event_id="event-1",
        source_execution_id="execution-1",
        result_status="killed",
        evidence_quality="validated",
        audit_required=True,
        authorized=True,
        reuse_mode=ReuseMode.EXACT,
    )
    reuse_plan = ReusePlanArtifact(
        history_revision=7,
        input_fingerprint="input-1",
        plan_fingerprint="reuse-plan-1",
        decisions=(decision,),
        reuse_mode=ReuseMode.EXACT,
    )
    campaign_plan = SimpleNamespace(plan_id="campaign-plan-1", audit_sample=("mutant-1",))
    audit_id = LocalCampaignCoordinator._reuse_audit_id(reuse_plan.plan_fingerprint, decision)
    path = tmp_path / "reuse.audit.json"
    LocalCampaignCoordinator._write_reuse_audit_progress(
        path,
        campaign_id="campaign-1",
        reuse_plan=reuse_plan,
        campaign_plan=campaign_plan,
        rows={
            audit_id: {
                "audit_id": audit_id,
                "mutant_id": "mutant-1",
                "kind": "exact",
                "source_event_id": "event-1",
                "source_execution_id": "execution-1",
                "sampled": True,
                "status": "running",
                "passed": False,
                "failures": [],
            }
        },
    )
    loaded = LocalCampaignCoordinator._load_reuse_audit_progress(
        path,
        campaign_id="campaign-1",
        reuse_plan=reuse_plan,
        campaign_plan=campaign_plan,
    )
    wrong_history = SimpleNamespace(
        history_revision=8,
        input_fingerprint=reuse_plan.input_fingerprint,
        plan_fingerprint=reuse_plan.plan_fingerprint,
        decisions=reuse_plan.decisions,
        reuse_mode=reuse_plan.reuse_mode,
    )
    rejected = LocalCampaignCoordinator._load_reuse_audit_progress(
        path,
        campaign_id="campaign-1",
        reuse_plan=wrong_history,
        campaign_plan=campaign_plan,
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    coordinator_source = (Path(__file__).parents[1] / "theseus_local" / "coordinator.py").read_text(encoding="utf-8")
    assert loaded[audit_id]["status"] == "running"
    assert rejected == {}
    assert payload["plan_fingerprint"] == "reuse-plan-1"
    assert payload["history_revision"] == 7
    assert payload["decisions"][0]["source_event_id"] == "event-1"
    assert "audit_interrupted_before_commit" in coordinator_source
    assert coordinator_source.index("existing_audit = audit_progress.get(audit_id)") < coordinator_source.index(
        "fresh = self._fresh_reuse_audit_result("
    )
def test_audit_sampling_matches_the_shared_campaign_scoped_contract() -> None:
    # Keep planner sampling independent of input ordering and identical to the public audit helper.
    policy = ReuseAuditPolicy(exact_sample_rate=0.5, random_seed="seed-1")
    raw = {
        "kind": "exact",
        "function_id": "app.choose",
        "audit_required": True,
    }
    expected = should_sample_reuse_audit(
        campaign_id="campaign-1",
        mutant_id="mutant-1",
        rule_id="app.choose",
        kind=ReuseKind.EXACT,
        policy=policy,
    )
    first = CampaignPlanner._audit_selected("campaign-1", "mutant-1", raw, policy)
    second = CampaignPlanner._audit_selected("campaign-1", "mutant-1", dict(reversed(tuple(raw.items()))), policy)
    assert first == second == expected
def test_infrastructure_inconclusive_is_not_a_quarantine_mismatch() -> None:
    # Keep transient audit infrastructure failure separate from semantic evidence corruption.
    expected = _execution("execution-source")
    mismatches = compare_reuse_audit(
        expected,
        {
            "status": "error",
            "semantic_result": "error",
            "restore_verified": False,
            "test_observations": [],
        },
    )
    source = (Path(__file__).parents[1] / "theseus_local" / "coordinator.py").read_text(encoding="utf-8")
    inconclusive = source.index('elif mismatches == ("infrastructure_inconclusive",):')
    quarantine = source.index("incident_id, quarantine_keys = self._quarantine_reuse_mismatch(", inconclusive)
    assert mismatches == ("infrastructure_inconclusive",)
    assert inconclusive < quarantine
def test_coordinator_mismatch_creates_one_incident_and_one_rule_quarantine(tmp_path: Path) -> None:
    # Collapse all mismatch identities into one canonical rule quarantine across retries and resumes.
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    decision = SimpleNamespace(mutant_id="mutant-1", kind=ReuseKind.EXACT)
    request = {
        "function_id": "app.choose",
        "function_fingerprint": "function-v1",
        "mutant_fingerprint": "mutant-v1",
    }
    expected = _execution("execution-source")
    actual = dict(expected) | {"semantic_result": "survived"}
    try:
        first_incident, first_keys = LocalCampaignCoordinator._quarantine_reuse_mismatch(
            knowledge=store,
            campaign_id="campaign-audit",
            decision=decision,
            request=request,
            expected=expected,
            actual=actual,
            mismatches=("semantic_mismatch",),
        )
        first_revision = store.query_executions(campaign_id="campaign-audit").snapshot_revision
        second_incident, second_keys = LocalCampaignCoordinator._quarantine_reuse_mismatch(
            knowledge=store,
            campaign_id="campaign-audit",
            decision=decision,
            request=request,
            expected=expected,
            actual=actual,
            mismatches=("semantic_mismatch",),
        )
        second_revision = store.query_executions(campaign_id="campaign-audit").snapshot_revision
        closure = store.closure_audit()
    finally:
        store.close()
    assert first_incident == second_incident
    assert first_keys == second_keys == ("rule:app.choose",)
    assert second_revision == first_revision
    assert closure["reuse_invariants"]["incident_count"] == 1
    assert closure["reuse_invariants"]["quarantine_count"] == 1
