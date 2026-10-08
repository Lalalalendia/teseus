from __future__ import annotations

from pathlib import Path

import pytest

from theseus_knowledge import KnowledgeConflict, KnowledgePlaneStore, ReuseKind
from theseus_local.coordinator import LocalCampaignCoordinator
from test_intelligence_unified_v1.recovery import campaign_input_fingerprint


def _payload(
    *,
    execution_id: str,
    status: str = "killed",
    restore_verified: bool = True,
    evidence_schema_version: int = 1,
    function_fingerprint: str = "function-v1",
    mutant_fingerprint: str = "mutant-v1",
    test_fingerprint: str = "tests-v1",
    environment_fingerprint: str = "environment-v1",
    selection_fingerprint: str = "selection-v1",
    result_fingerprint: str = "result-v1",
) -> dict[str, object]:
    # Build one compact execution envelope for evidence eligibility and invalidation tests.
    return {
        "executions": [
            {
                "execution_id": execution_id,
                "mutant_id": "m1",
                "attempt": 0,
                "semantic_result": status,
                "status": "complete",
                "restore_verified": restore_verified,
                "evidence_schema_version": evidence_schema_version,
                "source_kind": "observed",
                "function_id": "module.calculate",
                "function_fingerprint": function_fingerprint,
                "mutant_fingerprint": mutant_fingerprint,
                "test_fingerprint": test_fingerprint,
                "environment_fingerprint": environment_fingerprint,
                "selection_fingerprint": selection_fingerprint,
                "result_fingerprint": result_fingerprint,
                "test_observations": [
                    {"test_id": "test_one", "test_fingerprint": "test-one-v1", "outcome": status},
                ],
            }
        ]
    }


def _decision(store: KnowledgePlaneStore):
    # Ask for the complete current boundary used by the fixture execution.
    return store.decide_reuse(
        mutant_id="m1",
        mutant_fingerprint="mutant-v1",
        function_id="module.calculate",
        function_fingerprint="function-v1",
        environment_fingerprint="environment-v1",
        selection_fingerprint="selection-v1",
        test_fingerprint="tests-v1",
        result_fingerprint="result-v1",
    )


@pytest.mark.parametrize(
    ("status", "restore_verified", "evidence_schema_version"),
    (
        ("infrastructure_error", True, 1),
        ("killed", False, 1),
        ("killed", True, 0),
    ),
)
def test_h3_non_deterministic_or_unproven_results_never_become_exact(
    tmp_path: Path,
    status: str,
    restore_verified: bool,
    evidence_schema_version: int,
) -> None:
    # Keep infrastructure failures and incomplete restoration proofs out of executable reuse.
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    store.ingest_effect(
        effect_id="effect-ineligible",
        campaign_id="campaign-1",
        effect_type="mutation.execute_shard",
        payload=_payload(
            execution_id="exec-ineligible",
            status=status,
            restore_verified=restore_verified,
            evidence_schema_version=evidence_schema_version,
        ),
    )

    decision = _decision(store)

    assert decision.kind is not ReuseKind.EXACT
    assert decision.eligible is False
    store.close()


def test_h3_execution_id_is_global_and_conflicting_replay_is_corruption(tmp_path: Path) -> None:
    # Preserve one immutable execution across effects while rejecting a second payload under its identity.
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    first = store.ingest_effect(
        effect_id="effect-one",
        campaign_id="campaign-1",
        effect_type="mutation.execute_shard",
        payload=_payload(execution_id="exec-global"),
    )
    duplicate = store.ingest_effect(
        effect_id="effect-two",
        campaign_id="campaign-1",
        effect_type="mutation.execute_shard",
        payload=_payload(execution_id="exec-global"),
    )

    assert first.inserted_events == 1
    assert duplicate.inserted_events == 0
    assert store.summarize_campaign("campaign-1").executions == 1
    assert store.summarize_campaign("campaign-2").executions == 0
    with pytest.raises(KnowledgeConflict, match="execution identity conflict"):
        store.ingest_effect(
            effect_id="effect-three",
            campaign_id="campaign-2",
            effect_type="mutation.execute_shard",
            payload=_payload(execution_id="exec-global", status="survived"),
        )
    store.close()


def test_h4_invalidation_compares_utc_instants_not_timestamp_strings(tmp_path: Path) -> None:
    # Treat equivalent offsets as real instants so an invalidation cannot be bypassed by timestamp formatting.
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    store.ingest_effect(
        effect_id="effect-time",
        campaign_id="campaign-1",
        effect_type="mutation.execute_shard",
        payload=_payload(execution_id="exec-time"),
        observed_at="2026-01-01T00:00:00+00:00",
    )
    store.invalidate(
        scope_type="function",
        scope_key="function-v1",
        reason="function changed",
        invalidated_at="2025-12-31T19:00:01-05:00",
    )

    assert _decision(store).kind is not ReuseKind.EXACT
    store.close()


def test_h4_test_fingerprint_is_node_level_not_whole_file(tmp_path: Path) -> None:
    # Keep an unchanged selected test reusable when a neighboring test in the same module changes.
    test_file = tmp_path / "tests" / "test_app.py"
    test_file.parent.mkdir()
    test_file.write_text(
        "def test_one():\n    assert 1 == 1\n\n\n"
        "def test_two():\n    assert 2 == 2\n",
        encoding="utf-8",
    )
    first = LocalCampaignCoordinator._test_fingerprints(
        tmp_path,
        {"nodeids": ["tests/test_app.py::test_one"]},
        (),
    )
    test_file.write_text(
        "def test_one():\n    assert 1 == 1\n\n\n"
        "def test_two():\n    assert 2 == 3\n",
        encoding="utf-8",
    )
    second = LocalCampaignCoordinator._test_fingerprints(
        tmp_path,
        {"nodeids": ["tests/test_app.py::test_one"]},
        (),
    )

    assert first == second


def test_h4_production_invalidation_marks_changed_fingerprint_edges(tmp_path: Path) -> None:
    # Generate invalidation edges from the current request boundary before a reuse plan is built.
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    store.ingest_effect(
        effect_id="effect-old",
        campaign_id="campaign-old",
        effect_type="mutation.execute_shard",
        payload=_payload(execution_id="exec-old"),
        observed_at="2025-12-31T00:00:00Z",
    )
    store.invalidate_changed_inputs(
        [
            {
                "mutant_id": "m1",
                "mutant_fingerprint": "mutant-v2",
                "function_id": "module.calculate",
                "function_fingerprint": "function-v2",
                "environment_fingerprint": "environment-v1",
                "selection_fingerprint": "selection-v1",
                "result_fingerprint": "result-v2",
                "test_fingerprints": {"test_one": "test-one-v1"},
            }
        ],
        invalidated_at="2026-01-01T00:00:02Z",
    )

    assert _decision(store).kind is not ReuseKind.EXACT
    store.close()


def test_h4_resume_fingerprint_includes_requirements_manifest(tmp_path: Path) -> None:
    # Prevent a resumed campaign from crossing a changed dependency declaration.
    source = tmp_path / "app.py"
    source.write_text("def value():\n    return 1\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text("pytest==1\n", encoding="utf-8")
    first = campaign_input_fingerprint(
        tmp_path,
        "app.py",
        "source-v1",
        {"nodeids": ["tests/test_app.py::test_value"]},
        {"command": ["python", "-m", "pytest"]},
    )
    (tmp_path / "requirements.txt").write_text("pytest==2\n", encoding="utf-8")
    second = campaign_input_fingerprint(
        tmp_path,
        "app.py",
        "source-v1",
        {"nodeids": ["tests/test_app.py::test_value"]},
        {"command": ["python", "-m", "pytest"]},
    )
    assert first != second
