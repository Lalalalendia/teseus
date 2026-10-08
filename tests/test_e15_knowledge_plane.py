from __future__ import annotations

from pathlib import Path

import pytest

from theseus_knowledge import KnowledgeConflict, KnowledgePlaneStore


def _payload(execution_id: str, mutant_id: str, attempt: int, status: str) -> dict[str, object]:
    # Build one control-plane execution projection suitable for knowledge ingestion.
    return {
        "executions": [
            {
                "execution_id": execution_id,
                "mutant_id": mutant_id,
                "attempt": attempt,
                "semantic_result": status,
                "status": "complete",
            }
        ]
    }


def test_knowledge_plane_is_idempotent_and_keeps_retry_history(tmp_path: Path) -> None:
    # Replay the same committed effect safely while retaining later attempts as separate evidence.
    path = tmp_path / "knowledge.sqlite3"
    store = KnowledgePlaneStore(path)
    first = store.ingest_effect(
        effect_id="effect-1",
        campaign_id="campaign-1",
        effect_type="mutation.execute_shard",
        payload=_payload("exec-1", "m1", 0, "error"),
        control_revision=4,
    )
    duplicate = store.ingest_effect(
        effect_id="effect-1",
        campaign_id="campaign-1",
        effect_type="mutation.execute_shard",
        payload=_payload("exec-1", "m1", 0, "error"),
        control_revision=4,
    )
    second = store.ingest_effect(
        effect_id="effect-2",
        campaign_id="campaign-1",
        effect_type="mutation.execute_shard",
        payload=_payload("exec-2", "m1", 1, "killed"),
        control_revision=5,
    )
    assert first.inserted_events == 1
    assert duplicate.duplicate is True
    assert second.inserted_events == 1
    summary = store.summarize_campaign("campaign-1")
    assert summary.executions == 2
    assert summary.mutants == 1
    assert summary.attempts == 2
    assert [row["attempt"] for row in store.execution_history("campaign-1", "m1")] == [0, 1]
    store.close()

    restarted = KnowledgePlaneStore(path)
    assert restarted.summarize_campaign("campaign-1").executions == 2
    restarted.close()


def test_knowledge_plane_rejects_conflicting_effect_payload(tmp_path: Path) -> None:
    # A reused effect ID with another payload is corruption, not a new observation.
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    store.ingest_effect(
        effect_id="effect-1",
        campaign_id="campaign-1",
        effect_type="mutation.execute_shard",
        payload=_payload("exec-1", "m1", 0, "error"),
    )
    with pytest.raises(KnowledgeConflict, match="identity conflict"):
        store.ingest_effect(
            effect_id="effect-1",
            campaign_id="campaign-1",
            effect_type="mutation.execute_shard",
            payload=_payload("exec-1", "m1", 0, "killed"),
        )
    store.close()
