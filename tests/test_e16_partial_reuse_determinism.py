from __future__ import annotations
from pathlib import Path
import pytest
from theseus_contracts import PlanDecision
from theseus_knowledge import KnowledgePlaneStore, ReuseDecision, ReuseKind

def _execution(
    execution_id: str,
    *,
    attempt: int,
    tests: tuple[tuple[str, str], ...],
    aggregate_test_fingerprint: str = "aggregate-old",
) -> dict[str, object]:
    # Build one validated execution with an explicit single-event test evidence partition.
    return {
        "execution_id": execution_id,
        "mutant_id": "mutant-partial",
        "attempt": attempt,
        "semantic_result": "killed",
        "status": "complete",
        "restore_verified": True,
        "evidence_schema_version": 2,
        "source_kind": "observed",
        "evidence_origin": "pytest_test_stats",
        "function_id": "app.choose",
        "function_fingerprint": "function-v1",
        "mutant_fingerprint": "mutant-v1",
        "test_fingerprint": aggregate_test_fingerprint,
        "conftest_fingerprint": "conftest-v1",
        "environment_fingerprint": "environment-v1",
        "selection_fingerprint": "selection-v1",
        "result_fingerprint": f"result-{execution_id}",
        "test_observations": [
            {
                "test_id": test_id,
                "test_fingerprint": fingerprint,
                "outcome": "failed",
                "evidence_kind": "pytest_test_event",
                "observation_schema_version": 1,
            }
            for test_id, fingerprint in tests
        ],
    }

def _ingest(
    store: KnowledgePlaneStore,
    effect_id: str,
    execution: dict[str, object],
    *,
    observed_at: str,
) -> None:
    # Persist one independent evidence source so selection cannot merge rows across events.
    store.ingest_effect(
        effect_id=effect_id,
        campaign_id=f"campaign-{effect_id}",
        effect_type="mutation.execute_shard",
        payload={"executions": [execution]},
        observed_at=observed_at,
    )

def _request(test_items: tuple[tuple[str, str], ...]) -> dict[str, object]:
    # Build one current boundary whose mapping order is intentionally caller-controlled.
    return {
        "mutant_id": "mutant-partial",
        "function_id": "app.choose",
        "function_fingerprint": "function-v1",
        "mutant_fingerprint": "mutant-v1",
        "test_fingerprint": "aggregate-current",
        "test_fingerprints": dict(test_items),
        "conftest_fingerprint": "conftest-v1",
        "environment_fingerprint": "environment-v1",
        "selection_fingerprint": "selection-v1",
        "result_fingerprint": "result-current",
    }

def test_partial_selection_uses_one_best_source_and_canonical_partitions(tmp_path: Path) -> None:
    # Prefer maximum coverage and then the newest deterministic source without merging observations.
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    try:
        _ingest(
            store,
            "effect-old-wide",
            _execution("execution-old-wide", attempt=0, tests=(("test_a", "a-v1"), ("test_b", "b-v1"))),
            observed_at="2026-01-01T00:00:00Z",
        )
        _ingest(
            store,
            "effect-new-narrow",
            _execution("execution-new-narrow", attempt=1, tests=(("test_a", "a-v1"),)),
            observed_at="2026-01-02T00:00:00Z",
        )
        decision = store.decide_reuse(**_request((("test_c", "c-v2"), ("test_b", "b-v1"), ("test_a", "a-v1"))))
    finally:
        store.close()
    assert decision.kind is ReuseKind.PARTIAL
    assert decision.source_execution_id == "execution-old-wide"
    assert decision.matched_test_ids == ("test_a", "test_b")
    assert decision.missing_test_ids == ("test_c",)

def test_equal_partial_coverage_prefers_attempt_then_timestamp(tmp_path: Path) -> None:
    # Resolve equal coverage by attempt, timestamp and event identity rather than query iteration side effects.
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    try:
        _ingest(
            store,
            "effect-attempt-zero",
            _execution("execution-attempt-zero", attempt=0, tests=(("test_a", "a-v1"),)),
            observed_at="2026-01-03T00:00:00Z",
        )
        _ingest(
            store,
            "effect-attempt-one",
            _execution("execution-attempt-one", attempt=1, tests=(("test_b", "b-v1"),)),
            observed_at="2026-01-01T00:00:00Z",
        )
        decision = store.decide_reuse(**_request((("test_a", "a-v1"), ("test_b", "b-v1"), ("test_c", "c-v2"))))
    finally:
        store.close()
    assert decision.source_execution_id == "execution-attempt-one"
    assert decision.matched_test_ids == ("test_b",)
    assert decision.missing_test_ids == ("test_a", "test_c")

def test_exact_evidence_has_priority_over_newer_partial_evidence(tmp_path: Path) -> None:
    # Select a complete exact proof even when a newer higher-attempt row offers only partial coverage.
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    request = _request((("test_b", "b-v1"), ("test_a", "a-v1")))
    request["test_fingerprint"] = "aggregate-current"
    request["result_fingerprint"] = "result-exact"
    try:
        exact = _execution(
            "execution-exact",
            attempt=0,
            tests=(("test_a", "a-v1"), ("test_b", "b-v1")),
            aggregate_test_fingerprint="aggregate-current",
        )
        exact["result_fingerprint"] = "result-exact"
        _ingest(store, "effect-exact", exact, observed_at="2026-01-01T00:00:00Z")
        _ingest(
            store,
            "effect-partial-new",
            _execution("execution-partial-new", attempt=2, tests=(("test_a", "a-v1"),)),
            observed_at="2026-01-03T00:00:00Z",
        )
        decision = store.decide_reuse(**request)
    finally:
        store.close()
    assert decision.kind is ReuseKind.EXACT
    assert decision.source_execution_id == "execution-exact"

def test_reuse_plan_identity_is_stable_under_request_and_mapping_permutation(tmp_path: Path) -> None:
    # Canonicalize request order and nested test maps before hashing or producing decisions.
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    first_request = _request((("test_b", "b-v1"), ("test_a", "a-v1"), ("test_c", "c-v2")))
    second_request = dict(first_request)
    second_request["mutant_id"] = "mutant-other"
    second_request["mutant_fingerprint"] = "mutant-other-v1"
    permuted_first = dict(first_request)
    permuted_first["test_fingerprints"] = {"test_c": "c-v2", "test_a": "a-v1", "test_b": "b-v1"}
    try:
        first = store.build_reuse_plan((second_request, first_request), reuse_mode="partial")
        second = store.build_reuse_plan((permuted_first, second_request), reuse_mode="partial")
    finally:
        store.close()
    assert first.input_fingerprint == second.input_fingerprint
    assert first.plan_fingerprint == second.plan_fingerprint
    assert tuple(item.mutant_id for item in first.decisions) == ("mutant-other", "mutant-partial")

def test_partial_partitions_are_disjoint_and_fail_closed_when_malformed() -> None:
    # Reject ambiguous partitions at both Knowledge Plane and public planner boundaries.
    with pytest.raises(ValueError, match="must not overlap"):
        ReuseDecision(
            mutant_id="m1",
            kind=ReuseKind.PARTIAL,
            eligible=True,
            reason="ambiguous",
            matched_test_ids=("test_a",),
            missing_test_ids=("test_a",),
        )
    with pytest.raises(ValueError, match="requires non-empty"):
        PlanDecision(
            mutant_id="m1",
            action="partial_reuse",
            reason="incomplete",
            matched_test_ids=("test_a",),
        )
