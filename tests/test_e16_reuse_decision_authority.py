from __future__ import annotations
from pathlib import Path
import pytest
from theseus_contracts import ReuseMode, normalize_reuse_mode
from theseus_knowledge import (
    KnowledgePlaneStore,
    ReuseDecision,
    ReuseKind,
    authorize_reuse_decision,
)
def _exact_decision() -> ReuseDecision:
    # Build one factual exact match before policy authorization is applied.
    return ReuseDecision(
        mutant_id="m1",
        kind=ReuseKind.EXACT,
        eligible=True,
        reason="all result fingerprints match",
        source_event_id="event-1",
        source_execution_id="execution-1",
        result_status="killed",
        evidence_quality="validated",
        audit_required=True,
    )
def _partial_decision() -> ReuseDecision:
    # Build one factual partial match with explicit reused and fresh test partitions.
    return ReuseDecision(
        mutant_id="m1",
        kind=ReuseKind.PARTIAL,
        eligible=True,
        reason="only a subset of test fingerprints match",
        source_event_id="event-1",
        source_execution_id="execution-1",
        result_status="killed",
        evidence_quality="validated",
        matched_test_ids=("test_one",),
        missing_test_ids=("test_two",),
        audit_required=True,
    )
def test_reuse_mode_normalization_has_one_public_contract() -> None:
    # Normalize all canonical modes and retain experimental only as a migration alias.
    assert tuple(item.value for item in ReuseMode) == ("off", "hint", "exact", "partial")
    assert normalize_reuse_mode("off") is ReuseMode.OFF
    assert normalize_reuse_mode("hint") is ReuseMode.HINT
    assert normalize_reuse_mode("exact") is ReuseMode.EXACT
    assert normalize_reuse_mode("partial") is ReuseMode.PARTIAL
    assert normalize_reuse_mode("disabled") is ReuseMode.OFF
    assert normalize_reuse_mode("experimental") is ReuseMode.PARTIAL
    with pytest.raises(ValueError, match="reuse_mode must be one of"):
        normalize_reuse_mode("unsafe")
def test_authority_matrix_is_fail_closed_and_deterministic() -> None:
    # Apply the same policy matrix used by Knowledge Plane, planner, and coordinator.
    exact = _exact_decision()
    partial = _partial_decision()
    off = authorize_reuse_decision(exact, ReuseMode.OFF)
    hint = authorize_reuse_decision(exact, ReuseMode.HINT)
    exact_allowed = authorize_reuse_decision(exact, ReuseMode.EXACT)
    exact_rejects_partial = authorize_reuse_decision(partial, ReuseMode.EXACT)
    partial_allows_exact = authorize_reuse_decision(exact, ReuseMode.PARTIAL)
    partial_allowed = authorize_reuse_decision(partial, ReuseMode.PARTIAL)
    assert off.kind is ReuseKind.NONE and not off.eligible and not off.authorized and off.source_event_id is None
    assert hint.kind is ReuseKind.EXACT and hint.eligible and not hint.authorized
    assert exact_allowed.kind is ReuseKind.EXACT and exact_allowed.eligible and exact_allowed.authorized
    assert exact_rejects_partial.kind is ReuseKind.PARTIAL and exact_rejects_partial.eligible and not exact_rejects_partial.authorized
    assert partial_allows_exact.kind is ReuseKind.EXACT and partial_allows_exact.eligible and partial_allows_exact.authorized
    assert partial_allowed.kind is ReuseKind.PARTIAL and partial_allowed.eligible and partial_allowed.authorized
def test_off_mode_does_not_query_historical_evidence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Keep disabled reuse independent from Knowledge Plane history and query cost.
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    monkeypatch.setattr(
        store,
        "decide_reuse",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("off mode queried history")),
    )
    try:
        decisions = store.plan_reuse(({"mutant_id": "m1"},), reuse_mode=ReuseMode.OFF)
    finally:
        store.close()
    assert len(decisions) == 1
    assert decisions[0].kind is ReuseKind.NONE
    assert decisions[0].reuse_mode is ReuseMode.OFF
def test_reuse_plan_artifact_binds_mode_into_identity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Prove equal historical facts produce distinct immutable plans under different authority modes.
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    monkeypatch.setattr(store, "plan_reuse", lambda _requests, *, reuse_mode: (authorize_reuse_decision(_exact_decision(), reuse_mode),))
    try:
        hint = store.build_reuse_plan(({"mutant_id": "m1"},), reuse_mode=ReuseMode.HINT)
        exact = store.build_reuse_plan(({"mutant_id": "m1"},), reuse_mode=ReuseMode.EXACT)
    finally:
        store.close()
    assert hint.reuse_mode is ReuseMode.HINT
    assert exact.reuse_mode is ReuseMode.EXACT
    assert hint.plan_fingerprint != exact.plan_fingerprint
    assert hint.to_dict()["decisions"][0]["eligible"] is True
    assert hint.to_dict()["decisions"][0]["authorized"] is False
    assert exact.to_dict()["decisions"][0]["eligible"] is True
    assert exact.to_dict()["decisions"][0]["authorized"] is True
def test_coordinator_and_planner_have_no_private_experimental_gate() -> None:
    # Prevent another hidden mode branch from bypassing the shared authority contract.
    root = Path(__file__).parents[1]
    coordinator = (root / "theseus_local" / "coordinator.py").read_text(encoding="utf-8")
    planner = (root / "theseus_planner" / "planner.py").read_text(encoding="utf-8")
    store = (root / "theseus_knowledge" / "store.py").read_text(encoding="utf-8")
    assert 'reuse_mode == "experimental"' not in coordinator
    assert 'reuse_mode != "experimental"' not in coordinator
    assert "normalize_reuse_mode(configuration.reuse_mode)" in coordinator
    assert "reuse decision exceeds campaign policy" in planner
    assert "authorize_reuse_decision" in store
