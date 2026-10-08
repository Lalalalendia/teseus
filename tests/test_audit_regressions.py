from __future__ import annotations
import json
from pathlib import Path
import pytest
from test_intelligence_unified_v1.io_utils import append_json_line
from test_intelligence_unified_v1.recovery import (
    JournalReplayError,
    merge_authoritative_results,
    replay_result_journal,
    repository_tree_fingerprint,
    result_event_id,
)
from theseus_knowledge import (
    KnowledgeConflict,
    KnowledgePlaneStore,
    KnowledgeRetentionPolicy,
    ReuseKind,
    fingerprint_conftest,
    fingerprint_environment,
    fingerprint_function,
    fingerprint_mutant,
    fingerprint_result,
    fingerprint_test,
)
def _result_event(mutant_id: str, status: str) -> dict[str, object]:
    # Build a v2 result event whose identity is independent of mutable semantic outcome fields.
    row: dict[str, object] = {
        "event_type": "mutant_completed",
        "event_schema_version": 2,
        "execution_id": f"execution-{mutant_id}",
        "attempt": 0,
        "lease_id": "lease-1",
        "worker_id": "worker-1",
        "status": status,
        "mutant": {"mutant_id": mutant_id, "mutation": "plus_to_minus"},
        "result": {"status": status, "score": 1 if status == "killed" else 0},
    }
    row["event_id"] = result_event_id("campaign-audit", row)
    return row
def _fingerprints() -> dict[str, str]:
    # Build one complete reuse boundary shared by the Knowledge Plane regression cases.
    function = fingerprint_function(
        normalized_ast="def choose(value): return value + 1",
        signature="choose(value)",
        dependency_closure=("math:stable",),
        python_version="3.12.0",
    )
    mutant = fingerprint_mutant(
        function_fingerprint=function,
        operator_id="plus_to_minus",
        operator_version="m3",
        position=(1, 35),
        replacement="-",
    )
    test = fingerprint_test(
        test_code="def test_choose(): assert choose(1) == 2",
        fixtures=("tmp_path",),
        conftest_closure=("conftest:stable",),
        pytest_configuration="pytest.ini:v1",
        plugins=("pytest-cov",),
    )
    environment = fingerprint_environment(
        python_version="3.12.0",
        dependencies=("pytest==9.1.1",),
        pytest_version="9.1.1",
        plugins=("pytest-cov",),
        platform_name="linux",
        test_command=("pytest", "-q"),
    )
    conftest = fingerprint_conftest(closure=("conftest:stable",))
    result = fingerprint_result(
        mutant_fingerprint=mutant,
        test_fingerprint=test,
        environment_fingerprint=environment,
        selection_configuration={"levels": ["L1", "L2"]},
    )
    return {
        "function": function,
        "mutant": mutant,
        "test": test,
        "environment": environment,
        "conftest": conftest,
        "selection": "selection-v1",
        "result": result,
    }
def _knowledge_payload(
    fingerprints: dict[str, str],
    *,
    execution_id: str = "execution-1",
    status: str = "killed",
    observations: bool = True,
    evidence_origin: str = "pytest_events",
) -> dict[str, object]:
    # Build a committed execution payload with optional real pytest evidence.
    execution: dict[str, object] = {
        "execution_id": execution_id,
        "mutant_id": "mutant-1",
        "attempt": 0,
        "semantic_result": status,
        "status": "complete",
        "restore_verified": True,
        "evidence_schema_version": 1,
        "source_kind": "observed",
        "evidence_origin": evidence_origin,
        "function_id": "app.choose",
        "function_fingerprint": fingerprints["function"],
        "mutant_fingerprint": fingerprints["mutant"],
        "test_fingerprint": fingerprints["test"],
        "conftest_fingerprint": fingerprints["conftest"],
        "environment_fingerprint": fingerprints["environment"],
        "selection_fingerprint": fingerprints["selection"],
        "result_fingerprint": fingerprints["result"],
    }
    if observations:
        execution["test_observations"] = [
            {
                "test_id": "tests/test_app.py::test_choose",
                "test_fingerprint": fingerprints["test"],
                "outcome": "failed",
                "evidence_kind": "pytest_test_event",
                "observation_schema_version": 1,
            }
        ]
    return {"executions": [execution]}
def test_result_event_identity_does_not_change_with_status_or_result_payload() -> None:
    # Keep one logical execution addressable even when its semantic result changes during a retry.
    killed = _result_event("mutant-1", "killed")
    survived = _result_event("mutant-1", "survived")
    assert killed["event_id"] == survived["event_id"]
def test_result_journal_replays_exact_duplicates_and_rejects_identity_conflicts(tmp_path: Path) -> None:
    # Deduplicate a durable replay but fail closed when the same execution identity carries different bytes.
    journal = tmp_path / "results.jsonl"
    first = _result_event("mutant-1", "killed")
    append_json_line(journal, first)
    append_json_line(journal, first)
    replay = replay_result_journal(journal)
    assert len(replay.rows) == 1
    assert replay.duplicate_events == 1
    append_json_line(journal, _result_event("mutant-1", "survived"))
    with pytest.raises(JournalReplayError, match="conflicting journal record identity"):
        replay_result_journal(journal)
def test_knowledge_effect_is_idempotent_but_payload_conflict_is_rejected(tmp_path: Path) -> None:
    # Preserve one committed effect exactly once and reject reuse of its ID for another payload.
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    try:
        payload = _knowledge_payload(_fingerprints())
        first = store.ingest_effect(
            effect_id="effect-1",
            campaign_id="campaign-1",
            effect_type="mutation.execute_shard",
            payload=payload,
            observed_at="2026-01-01T00:00:00Z",
        )
        duplicate = store.ingest_effect(
            effect_id="effect-1",
            campaign_id="campaign-1",
            effect_type="mutation.execute_shard",
            payload=payload,
            observed_at="2026-01-01T00:00:00Z",
        )
        changed = _knowledge_payload(_fingerprints(), status="survived")
        assert first.inserted_events == 1
        assert duplicate.duplicate is True
        assert store.summarize_campaign("campaign-1").executions == 1
        with pytest.raises(KnowledgeConflict, match="identity conflict"):
            store.ingest_effect(
                effect_id="effect-1",
                campaign_id="campaign-1",
                effect_type="mutation.execute_shard",
                payload=changed,
            )
        assert store.summarize_campaign("campaign-1").executions == 1
    finally:
        store.close()
def test_knowledge_enrichment_adds_evidence_without_double_counting(tmp_path: Path) -> None:
    # Attach late fingerprint and pytest evidence to an outbox projection without creating a second execution.
    fingerprints = _fingerprints()
    base = _knowledge_payload(fingerprints, observations=False, evidence_origin="aggregate_projection")
    enriched = _knowledge_payload(fingerprints, observations=True, evidence_origin="aggregate_projection")
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    try:
        store.ingest_effect(
            effect_id="effect-enrichment",
            campaign_id="campaign-enrichment",
            effect_type="mutation.execute_shard",
            payload=base,
        )
        result = store.enrich_effect(
            effect_id="effect-enrichment",
            campaign_id="campaign-enrichment",
            payload=enriched,
        )
        assert result.duplicate is True
        assert result.inserted_observations == 1
        assert store.summarize_campaign("campaign-enrichment").executions == 1
    finally:
        store.close()
def test_retention_preserves_rollup_but_demotes_exact_reuse_to_hint(tmp_path: Path) -> None:
    # Compact old evidence into a rollup while making it ineligible for executable reuse.
    fingerprints = _fingerprints()
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    try:
        store.ingest_effect(
            effect_id="effect-retention",
            campaign_id="campaign-retention",
            effect_type="mutation.execute_shard",
            payload=_knowledge_payload(fingerprints),
            observed_at="2026-01-01T00:00:00Z",
        )
        retained = store.apply_retention(
            KnowledgeRetentionPolicy(max_age_seconds=1),
            now="2026-01-02T00:00:00Z",
        )
        compacted = store.query_executions(campaign_id="campaign-retention", include_compacted=True)
        decision = store.decide_reuse(
            mutant_id="mutant-1",
            mutant_fingerprint=fingerprints["mutant"],
            function_fingerprint=fingerprints["function"],
            environment_fingerprint=fingerprints["environment"],
            selection_fingerprint=fingerprints["selection"],
            test_fingerprint=fingerprints["test"],
            conftest_fingerprint=fingerprints["conftest"],
            result_fingerprint=fingerprints["result"],
        )
        assert retained.compacted_events == 1
        assert len(compacted.rows) == 1
        assert compacted.rows[0]["compacted"] is True
        assert decision.kind is ReuseKind.HISTORICAL_HINT
        assert decision.eligible is False
    finally:
        store.close()
def test_reuse_plan_fingerprint_changes_after_input_invalidation(tmp_path: Path) -> None:
    # Make the immutable reuse plan reflect the knowledge revision and a newly invalidated input boundary.
    fingerprints = _fingerprints()
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    try:
        store.ingest_effect(
            effect_id="effect-plan",
            campaign_id="campaign-plan",
            effect_type="mutation.execute_shard",
            payload=_knowledge_payload(fingerprints),
            observed_at="2026-01-01T00:00:00Z",
        )
        request = {
            "mutant_id": "mutant-1",
            "mutant_fingerprint": fingerprints["mutant"],
            "function_fingerprint": fingerprints["function"],
            "environment_fingerprint": fingerprints["environment"],
            "selection_fingerprint": fingerprints["selection"],
            "test_fingerprint": fingerprints["test"],
            "conftest_fingerprint": fingerprints["conftest"],
            "result_fingerprint": fingerprints["result"],
        }
        first = store.build_reuse_plan([request])
        store.invalidate(
            scope_type="function",
            scope_key=fingerprints["function"],
            reason="function changed",
            invalidated_at="2026-01-02T00:00:00Z",
        )
        second = store.build_reuse_plan([request])
        assert first.plan_fingerprint != second.plan_fingerprint
        assert first.decisions[0].kind is ReuseKind.EXACT
        assert first.decisions[0].eligible is True
        assert first.decisions[0].authorized is False
        assert second.decisions[0].kind is ReuseKind.NONE
    finally:
        store.close()
def test_authoritative_merge_prefers_newer_semantic_retry_over_infrastructure_error() -> None:
    # Select the newest meaningful retry while retaining one result per mutant in the canonical projection.
    rows = [
        _result_event("mutant-1", "error") | {"attempt": 0, "execution_id": "execution-0"},
        _result_event("mutant-1", "killed") | {"attempt": 1, "execution_id": "execution-1"},
        _result_event("mutant-1", "timeout") | {"attempt": 1, "execution_id": "execution-2"},
    ]
    for row in rows:
        row["event_id"] = result_event_id("campaign-merge", row)
    merged = merge_authoritative_results(rows)
    assert len(merged) == 1
    assert merged[0]["status"] == "killed"
    assert merged[0]["attempt"] == 1
def test_repository_fingerprint_ignores_generated_reports_but_detects_source_change(tmp_path: Path) -> None:
    # Keep recovery compatibility stable across generated state while invalidating it on source edits.
    source = tmp_path / "app.py"
    source.write_text("value = 1\n", encoding="utf-8")
    reports = tmp_path / "reports"
    reports.mkdir()
    (reports / "generated.json").write_text(json.dumps({"run": 1}), encoding="utf-8")
    first = repository_tree_fingerprint(tmp_path)
    (reports / "generated.json").write_text(json.dumps({"run": 2}), encoding="utf-8")
    after_report = repository_tree_fingerprint(tmp_path)
    source.write_text("value = 2\n", encoding="utf-8")
    after_source = repository_tree_fingerprint(tmp_path)
    assert first == after_report
    assert after_source != first
