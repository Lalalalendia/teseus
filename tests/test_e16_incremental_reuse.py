from __future__ import annotations
import json
from pathlib import Path
from theseus_knowledge import (
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
def _fingerprints() -> dict[str, str]:
    # Build one complete E16 fingerprint boundary for the fixture executions.
    function = fingerprint_function(
        normalized_ast="def calculate(value): return value + 1",
        signature="calculate(value)",
        decorators=("@pure",),
        dependency_closure=("math:stable",),
        python_version="3.12.0",
    )
    mutant = fingerprint_mutant(
        function_fingerprint=function,
        operator_id="replace-add-with-subtract",
        operator_version="m3",
        position=(1, 34),
        replacement="-",
    )
    test_one = fingerprint_test(
        test_code="def test_one(): assert calculate(1) == 2",
        fixtures=("tmp_path",),
        conftest_closure=("conftest:stable",),
        pytest_configuration="pytest.ini:v1",
        plugins=("pytester",),
    )
    test_two = fingerprint_test(
        test_code="def test_two(): assert calculate(2) == 3",
        fixtures=("tmp_path",),
        conftest_closure=("conftest:stable",),
        pytest_configuration="pytest.ini:v1",
        plugins=("pytester",),
    )
    conftest = fingerprint_conftest(closure=("conftest:stable",))
    environment = fingerprint_environment(
        python_version="3.12.0",
        dependencies=("pytest==9.1.1",),
        pytest_version="9.1.1",
        plugins=("pytester",),
        platform_name="linux",
        environment_profile="default",
        test_command=("pytest", "-q"),
    )
    result = fingerprint_result(
        mutant_fingerprint=mutant,
        test_fingerprint=fingerprint_result(
            mutant_fingerprint="tests",
            test_fingerprint=test_one,
            environment_fingerprint=test_two,
        ),
        environment_fingerprint=environment,
        selection_configuration={"levels": ["L1", "L2"]},
    )
    return {
        "function": function,
        "mutant": mutant,
        "test_one": test_one,
        "test_two": test_two,
        "test_set": fingerprint_result(
            mutant_fingerprint="test-set",
            test_fingerprint=test_one,
            environment_fingerprint=test_two,
        ),
        "conftest": conftest,
        "environment": environment,
        "selection": "selection-v1",
        "result": result,
    }
def _payload(fingerprints: dict[str, str], *, execution_id: str, mutant_id: str = "m1") -> dict[str, object]:
    # Build one committed execution with per-test evidence for exact and partial reuse.
    return {
        "executions": [
            {
                "execution_id": execution_id,
                "mutant_id": mutant_id,
                "attempt": 0,
                "semantic_result": "killed",
                "status": "complete",
                "restore_verified": True,
                "evidence_schema_version": 1,
                "source_kind": "observed",
                "function_id": "module.calculate",
                "function_fingerprint": fingerprints["function"],
                "mutant_fingerprint": fingerprints["mutant"],
                "test_fingerprint": fingerprints["test_set"],
                "conftest_fingerprint": fingerprints["conftest"],
                "environment_fingerprint": fingerprints["environment"],
                "selection_fingerprint": fingerprints["selection"],
                "result_fingerprint": fingerprints["result"],
                "test_observations": [
                    {
                        "test_id": "test_one",
                        "test_fingerprint": fingerprints["test_one"],
                        "outcome": "failed",
                        "evidence_kind": "pytest_test_event",
                        "observation_schema_version": 1,
                    },
                    {
                        "test_id": "test_two",
                        "test_fingerprint": fingerprints["test_two"],
                        "outcome": "failed",
                        "evidence_kind": "pytest_test_event",
                        "observation_schema_version": 1,
                    },
                ],
            }
        ]
    }
def test_e16_fingerprints_are_stable_and_input_sensitive() -> None:
    # Verify normalized AST stability and sensitivity to every reuse-boundary category.
    assert fingerprint_function(normalized_ast="def f(value):\n    return value + 1") == fingerprint_function(
        normalized_ast="def f(value): return value + 1"
    )
    assert fingerprint_test(test_code="def test_value(): assert 1") != fingerprint_test(
        test_code="def test_value(): assert 2"
    )
    assert fingerprint_environment(python_version="3.12", pytest_version="9.1") != fingerprint_environment(
        python_version="3.12", pytest_version="9.2"
    )
    assert fingerprint_mutant(
        function_fingerprint="f",
        operator_id="op",
        operator_version="1",
        position=(1, 1),
        replacement="+",
    ) != fingerprint_mutant(
        function_fingerprint="f",
        operator_id="op",
        operator_version="1",
        position=(1, 1),
        replacement="-",
    )
def test_e16_query_is_bounded_and_keyset_stable(tmp_path: Path) -> None:
    # Page through the projection without offset scans or payload materialization by default.
    fingerprints = _fingerprints()
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    store.ingest_effect(
        effect_id="effect-1",
        campaign_id="campaign-1",
        effect_type="mutation.execute_shard",
        payload=_payload(fingerprints, execution_id="exec-1"),
        observed_at="2026-01-01T00:00:01+00:00",
    )
    store.ingest_effect(
        effect_id="effect-2",
        campaign_id="campaign-1",
        effect_type="mutation.execute_shard",
        payload=_payload(fingerprints, execution_id="exec-2"),
        observed_at="2026-01-01T00:00:02+00:00",
    )
    first = store.query_executions(campaign_id="campaign-1", limit=1)
    second = store.query_executions(campaign_id="campaign-1", limit=1, cursor=first.next_cursor)
    assert len(first.rows) == 1
    assert first.rows[0].get("payload") is None
    assert len(second.rows) == 1
    assert first.rows[0]["event_id"] != second.rows[0]["event_id"]
    assert store.summarize_campaign("campaign-1").executions == 2
    store.close()
def test_e16_exact_partial_and_historical_decisions_are_distinct(tmp_path: Path) -> None:
    # Prove that only exact and partial matches are executable while stale matches remain hints.
    fingerprints = _fingerprints()
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    store.ingest_effect(
        effect_id="effect-1",
        campaign_id="campaign-1",
        effect_type="mutation.execute_shard",
        payload=_payload(fingerprints, execution_id="exec-1"),
    )
    exact = store.decide_reuse(
        mutant_id="m1",
        mutant_fingerprint=fingerprints["mutant"],
        function_id="module.calculate",
        function_fingerprint=fingerprints["function"],
        environment_fingerprint=fingerprints["environment"],
        selection_fingerprint=fingerprints["selection"],
        test_fingerprint=fingerprints["test_set"],
        result_fingerprint=fingerprints["result"],
    )
    assert exact.kind is ReuseKind.EXACT
    assert exact.eligible is True
    partial = store.decide_reuse(
        mutant_id="m1",
        mutant_fingerprint=fingerprints["mutant"],
        function_id="module.calculate",
        function_fingerprint=fingerprints["function"],
        environment_fingerprint=fingerprints["environment"],
        selection_fingerprint=fingerprints["selection"],
        test_fingerprints={"test_one": fingerprints["test_one"], "test_two": "changed-test"},
    )
    assert partial.kind is ReuseKind.PARTIAL
    assert partial.eligible is True
    assert partial.matched_test_ids == ("test_one",)
    assert partial.missing_test_ids == ("test_two",)
    historical = store.decide_reuse(
        mutant_id="m1",
        mutant_fingerprint=fingerprints["mutant"],
        function_id="module.calculate",
        function_fingerprint=fingerprints["function"],
        environment_fingerprint="different-environment",
        selection_fingerprint=fingerprints["selection"],
        test_fingerprint=fingerprints["test_set"],
    )
    assert historical.kind is ReuseKind.HISTORICAL_HINT
    assert historical.eligible is False
    store.close()
def test_reuse_quarantine_records_incident_and_blocks_future_reuse(tmp_path: Path) -> None:
    # Make a sampled-audit mismatch durable and verify every later planner sees a fail-closed blocker.
    fingerprints = _fingerprints()
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    store.ingest_effect(
        effect_id="effect-quarantine",
        campaign_id="campaign-quarantine-source",
        effect_type="mutation.execute_shard",
        payload=_payload(fingerprints, execution_id="exec-quarantine"),
    )
    expected = _payload(fingerprints, execution_id="exec-quarantine")["executions"][0]
    actual = dict(expected)
    actual["semantic_result"] = "survived"
    incident_id = store.record_reuse_incident(
        campaign_id="campaign-quarantine-audit",
        mutant_id="m1",
        rule_id="module.calculate",
        kind="exact",
        mismatches=("semantic_mismatch",),
        expected_payload=expected,
        actual_payload=actual,
    )
    store.quarantine_reuse_rule(
        scope_type="mutant_fingerprint",
        scope_key=fingerprints["mutant"],
        reason="fresh reuse audit mismatch",
        incident_id=incident_id,
    )
    decision = store.decide_reuse(
        mutant_id="m1",
        mutant_fingerprint=fingerprints["mutant"],
        function_id="module.calculate",
        function_fingerprint=fingerprints["function"],
        environment_fingerprint=fingerprints["environment"],
        selection_fingerprint=fingerprints["selection"],
        test_fingerprint=fingerprints["test_set"],
        result_fingerprint=fingerprints["result"],
    )
    assert store.is_reuse_quarantined(scope_type="mutant_fingerprint", scope_key=fingerprints["mutant"])
    assert decision.kind is ReuseKind.HISTORICAL_HINT
    assert any(item.startswith("reuse-quarantined:mutant_fingerprint:") for item in decision.blockers)
    store.close()
def test_e16_invalidation_blocks_old_evidence_but_not_new_effect(tmp_path: Path) -> None:
    # Invalidate a function boundary and verify later observations can still become exact reuse.
    fingerprints = _fingerprints()
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    store.ingest_effect(
        effect_id="effect-1",
        campaign_id="campaign-1",
        effect_type="mutation.execute_shard",
        payload=_payload(fingerprints, execution_id="exec-1"),
        observed_at="2026-01-01T00:00:01+00:00",
    )
    store.invalidate(
        scope_type="function",
        scope_key=fingerprints["function"],
        reason="function changed",
        invalidated_at="2026-01-01T00:00:02+00:00",
    )
    blocked = store.decide_reuse(
        mutant_id="m1",
        mutant_fingerprint=fingerprints["mutant"],
        function_fingerprint=fingerprints["function"],
        environment_fingerprint=fingerprints["environment"],
        selection_fingerprint=fingerprints["selection"],
        test_fingerprint=fingerprints["test_set"],
        result_fingerprint=fingerprints["result"],
    )
    assert blocked.kind is ReuseKind.NONE
    store.ingest_effect(
        effect_id="effect-2",
        campaign_id="campaign-1",
        effect_type="mutation.execute_shard",
        payload=_payload(fingerprints, execution_id="exec-2"),
        observed_at="2026-01-01T00:00:03+00:00",
    )
    allowed = store.decide_reuse(
        mutant_id="m1",
        mutant_fingerprint=fingerprints["mutant"],
        function_fingerprint=fingerprints["function"],
        environment_fingerprint=fingerprints["environment"],
        selection_fingerprint=fingerprints["selection"],
        test_fingerprint=fingerprints["test_set"],
        result_fingerprint=fingerprints["result"],
    )
    assert allowed.kind is ReuseKind.EXACT
    store.close()
def test_e16_test_invalidation_preserves_partial_reuse_for_other_tests(tmp_path: Path) -> None:
    # Invalidate one test edge while retaining exact evidence for an unchanged independent test.
    fingerprints = _fingerprints()
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    store.ingest_effect(
        effect_id="effect-1",
        campaign_id="campaign-1",
        effect_type="mutation.execute_shard",
        payload=_payload(fingerprints, execution_id="exec-1"),
        observed_at="2026-01-01T00:00:01+00:00",
    )
    store.invalidate(
        scope_type="test",
        scope_key="test_two",
        reason="test changed",
        invalidated_at="2026-01-01T00:00:02+00:00",
    )
    decision = store.decide_reuse(
        mutant_id="m1",
        mutant_fingerprint=fingerprints["mutant"],
        function_fingerprint=fingerprints["function"],
        environment_fingerprint=fingerprints["environment"],
        selection_fingerprint=fingerprints["selection"],
        test_fingerprints={"test_one": fingerprints["test_one"], "test_two": "changed-test"},
    )
    assert decision.kind is ReuseKind.PARTIAL
    assert decision.matched_test_ids == ("test_one",)
    assert decision.missing_test_ids == ("test_two",)
    store.close()
def test_e16_reuse_plan_artifact_binds_history_revision(tmp_path: Path) -> None:
    # Persist an explainable plan whose fingerprint changes when knowledge history changes.
    fingerprints = _fingerprints()
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    store.ingest_effect(
        effect_id="effect-1",
        campaign_id="campaign-1",
        effect_type="mutation.execute_shard",
        payload=_payload(fingerprints, execution_id="exec-1"),
    )
    request = {
        "mutant_id": "m1",
        "mutant_fingerprint": fingerprints["mutant"],
        "function_fingerprint": fingerprints["function"],
        "environment_fingerprint": fingerprints["environment"],
        "selection_fingerprint": fingerprints["selection"],
        "test_fingerprint": fingerprints["test_set"],
        "result_fingerprint": fingerprints["result"],
    }
    path = tmp_path / "reuse-plan.json"
    first = store.write_reuse_plan(path, [request], reuse_mode="partial")
    persisted = json.loads(path.read_text(encoding="utf-8"))
    assert first.decisions[0].kind is ReuseKind.EXACT
    assert persisted["plan_fingerprint"] == first.plan_fingerprint
    store.invalidate(scope_type="environment", scope_key=fingerprints["environment"], reason="runtime changed")
    second = store.build_reuse_plan([request], reuse_mode="partial")
    assert second.history_revision > first.history_revision
    assert second.plan_fingerprint != first.plan_fingerprint
    store.close()
def test_e16_conftest_invalidation_blocks_the_whole_test_closure(tmp_path: Path) -> None:
    # Verify a conftest change invalidates the broad closure even when mutant and test code are unchanged.
    fingerprints = _fingerprints()
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    store.ingest_effect(
        effect_id="effect-1",
        campaign_id="campaign-1",
        effect_type="mutation.execute_shard",
        payload=_payload(fingerprints, execution_id="exec-1"),
        observed_at="2026-01-01T00:00:01+00:00",
    )
    store.invalidate(
        scope_type="conftest",
        scope_key=fingerprints["conftest"],
        reason="fixture closure changed",
        invalidated_at="2026-01-01T00:00:02+00:00",
    )
    decision = store.decide_reuse(
        mutant_id="m1",
        mutant_fingerprint=fingerprints["mutant"],
        function_fingerprint=fingerprints["function"],
        environment_fingerprint=fingerprints["environment"],
        selection_fingerprint=fingerprints["selection"],
        test_fingerprint=fingerprints["test_set"],
        conftest_fingerprint=fingerprints["conftest"],
        result_fingerprint=fingerprints["result"],
    )
    assert decision.kind is ReuseKind.NONE
    store.close()
def test_e16_retention_compacts_raw_rows_and_preserves_summary(tmp_path: Path) -> None:
    # Compact old raw evidence into historical rollups without allowing it to become exact reuse.
    fingerprints = _fingerprints()
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    store.ingest_effect(
        effect_id="effect-old",
        campaign_id="campaign-old",
        effect_type="mutation.execute_shard",
        payload=_payload(fingerprints, execution_id="exec-old"),
        observed_at="2020-01-01T00:00:00+00:00",
    )
    store.ingest_effect(
        effect_id="effect-protected",
        campaign_id="campaign-protected",
        effect_type="mutation.execute_shard",
        payload=_payload({**fingerprints, "mutant": "protected-mutant"}, execution_id="exec-protected", mutant_id="m-protected"),
        observed_at="2020-01-01T00:00:00+00:00",
    )
    result = store.apply_retention(
        KnowledgeRetentionPolicy(max_age_seconds=1, protected_campaign_ids=("campaign-protected",)),
        now="2026-01-01T00:00:00+00:00",
    )
    assert result.compacted_events == 1
    assert result.skipped_protected == 1
    assert store.query_executions(campaign_id="campaign-old").rows == ()
    assert len(store.query_executions(campaign_id="campaign-old", include_compacted=True).rows) == 1
    assert store.summarize_campaign("campaign-old").executions == 1
    hint = store.decide_reuse(
        mutant_id="m1",
        mutant_fingerprint=fingerprints["mutant"],
        function_fingerprint=fingerprints["function"],
        environment_fingerprint=fingerprints["environment"],
        selection_fingerprint=fingerprints["selection"],
        test_fingerprint=fingerprints["test_set"],
        result_fingerprint=fingerprints["result"],
    )
    assert hint.kind is ReuseKind.HISTORICAL_HINT
    assert hint.source_compacted is True
    store.close()
