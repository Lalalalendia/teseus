from __future__ import annotations
import os
import sqlite3
from pathlib import Path
import pytest
import theseus_knowledge.store as knowledge_store_module
from theseus_knowledge import (
    KNOWLEDGE_SCHEMA_VERSION,
    KnowledgeConflict,
    KnowledgePlaneStore,
    KnowledgeRetentionPolicy,
    ReuseDecision,
    ReuseKind,
    ReusePlanArtifact,
)
def _execution(execution_id: str, *, result: str = "killed", mutant_id: str = "mutant-closure") -> dict[str, object]:
    # Build one fully evidenced execution that remains valid through compaction and restore.
    return {
        "execution_id": execution_id,
        "campaign_id": "campaign-closure",
        "shard_id": "shard-closure",
        "mutant_id": mutant_id,
        "attempt": 0,
        "status": "complete",
        "semantic_result": result,
        "selected_tests": ["test_app.py::test_choose"],
        "artifacts": [f"artifact-{execution_id}"],
        "lease_id": "lease-closure",
        "restore_verified": True,
        "source_kind": "observed",
        "evidence_origin": "pytest_test_stats",
        "evidence_schema_version": 2,
        "function_id": "app.choose",
        "function_fingerprint": "function-closure-v1",
        "mutant_fingerprint": f"{mutant_id}-fingerprint-v1",
        "test_fingerprint": "test-set-closure-v1",
        "test_fingerprints": {"test_app.py::test_choose": "test-node-closure-v1"},
        "conftest_fingerprint": "conftest-closure-v1",
        "environment_fingerprint": "environment-closure-v1",
        "selection_fingerprint": "selection-closure-v1",
        "result_fingerprint": f"result-{execution_id}-{result}",
        "test_observations": [
            {
                "test_id": "test_app.py::test_choose",
                "test_fingerprint": "test-node-closure-v1",
                "outcome": "failed" if result == "killed" else "passed",
                "evidence_kind": "pytest_test_event",
                "observation_schema_version": 1,
            }
        ],
    }
def _payload(*executions: dict[str, object]) -> dict[str, object]:
    # Bind test executions to one canonical scope and concrete worker/lease provenance.
    return {
        "campaign": {
            "campaign_id": "campaign-closure",
            "project_id": "project-closure",
            "revision_id": "revision-closure",
        },
        "environment_id": "environment-closure-v1",
        "shard": {
            "shard_id": "shard-closure",
            "worker_id": "worker-closure",
            "lease": {
                "lease_id": "lease-closure",
                "worker_id": "worker-closure",
                "attempt": 0,
                "worker_instance_id": "worker-instance-closure",
                "process_id": 4242,
                "process_birth_token": "birth-closure",
            },
        },
        "executions": list(executions),
    }
def _ingest_old(store: KnowledgePlaneStore, execution_id: str, *, mutant_id: str = "mutant-closure") -> str:
    # Insert one old effect and return its immutable source event identity.
    store.ingest_effect(
        effect_id=f"effect-{execution_id}",
        campaign_id="campaign-closure",
        effect_type="mutation.execute_shard",
        payload=_payload(_execution(execution_id, mutant_id=mutant_id)),
        observed_at="2020-01-01T00:00:00Z",
    )
    rows = store.query_executions(campaign_id="campaign-closure", mutant_id=mutant_id).rows
    return str(next(item["event_id"] for item in rows if item["execution_id"] == execution_id))
def test_conflicting_fact_is_quarantined_without_rewriting_history(tmp_path: Path) -> None:
    # Keep exact replay idempotent while recording a conflicting reuse of the same effect identity.
    database = tmp_path / "knowledge.sqlite3"
    store = KnowledgePlaneStore(database)
    original = _payload(_execution("execution-conflict"))
    try:
        first = store.ingest_effect(
            effect_id="effect-conflict",
            campaign_id="campaign-closure",
            effect_type="mutation.execute_shard",
            payload=original,
        )
        revision_before_duplicate = store.query_executions(campaign_id="campaign-closure").snapshot_revision
        duplicate = store.ingest_effect(
            effect_id="effect-conflict",
            campaign_id="campaign-closure",
            effect_type="mutation.execute_shard",
            payload=original,
        )
        revision_after_duplicate = store.query_executions(campaign_id="campaign-closure").snapshot_revision
        with pytest.raises(KnowledgeConflict, match="identity conflict"):
            store.ingest_effect(
                effect_id="effect-conflict",
                campaign_id="campaign-closure",
                effect_type="mutation.execute_shard",
                payload=_payload(_execution("execution-conflict", result="survived")),
            )
        conflicts = store.list_conflicts()
        rows = store.query_executions(campaign_id="campaign-closure", include_payload=True).rows
        state = store.schema_state()
    finally:
        store.close()
    assert first.inserted_events == 1 and duplicate.duplicate
    assert revision_after_duplicate == revision_before_duplicate
    assert len(conflicts) == 1 and conflicts[0].status == "quarantined"
    assert conflicts[0].identity_key == "effect-conflict"
    assert state.quarantined_conflicts == 1
    assert len(rows) == 1 and rows[0]["status"] == "killed"
def test_compaction_preserves_graph_identity_and_executable_evidence(tmp_path: Path) -> None:
    # Replace raw rows with one complete evidence bundle, tombstone, and atomic manifest.
    database = tmp_path / "knowledge.sqlite3"
    store = KnowledgePlaneStore(database)
    try:
        event_id = _ingest_old(store, "execution-compacted")
        before = store.evidence_graph("execution-compacted")
        result = store.apply_retention(
            KnowledgeRetentionPolicy(max_age_seconds=1),
            now="2026-01-01T00:00:00Z",
        )
        after = store.evidence_graph("execution-compacted")
        compacted = store.compacted_evidence(event_id)
        tombstones = store.tombstones(event_id=event_id)
        compactions = store.compaction_records()
        integrity = store.require_integrity()
    finally:
        store.close()
    assert result.compacted_events == 1 and result.compacted_observations == 1
    assert result.compaction_id
    assert before.graph_fingerprint == after.graph_fingerprint
    assert compacted is not None
    assert compacted["graph_fingerprint"] == before.graph_fingerprint
    assert compacted["evidence"]["execution"]["execution_id"] == "execution-compacted"
    assert compacted["evidence"]["observations"][0]["test_id"] == "test_app.py::test_choose"
    assert len(tombstones) == 1 and tombstones[0]["replacement_id"] == result.compaction_id
    assert len(compactions) == 1 and compactions[0].compaction_id == result.compaction_id
    assert integrity.healthy
def test_retention_skips_evidence_referenced_by_live_reuse_plan(tmp_path: Path) -> None:
    # Protect an executable exact decision until its campaign releases the durable reuse plan.
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    try:
        event_id = _ingest_old(store, "execution-live")
        plan = ReusePlanArtifact(
            history_revision=store.query_executions(campaign_id="campaign-closure").snapshot_revision,
            input_fingerprint="input-live",
            plan_fingerprint="plan-live",
            decisions=(
                ReuseDecision(
                    mutant_id="mutant-closure",
                    kind=ReuseKind.EXACT,
                    eligible=True,
                    reason="validated exact evidence",
                    source_event_id=event_id,
                    source_execution_id="execution-live",
                    result_status="killed",
                    evidence_quality="validated",
                ),
            ),
        )
        plan_id = store.protect_reuse_plan("campaign-closure", plan)
        blocked = store.apply_retention(
            KnowledgeRetentionPolicy(max_age_seconds=1),
            now="2026-01-01T00:00:00Z",
        )
        references = store.active_evidence_references(campaign_id="campaign-closure")
        released = store.release_reuse_plans("campaign-closure")
        compacted = store.apply_retention(
            KnowledgeRetentionPolicy(max_age_seconds=1),
            now="2026-01-01T00:00:00Z",
        )
    finally:
        store.close()
    assert blocked.compacted_events == 0 and blocked.skipped_referenced == 1
    assert len(references) == 1 and references[0]["plan_id"] == plan_id
    assert released == 1 and compacted.compacted_events == 1
def test_compaction_fault_rolls_back_to_the_old_complete_version(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Inject a crash after the first event and require every raw row and graph to remain intact.
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    try:
        _ingest_old(store, "execution-crash-a", mutant_id="mutant-crash-a")
        _ingest_old(store, "execution-crash-b", mutant_id="mutant-crash-b")
        before = {
            execution_id: store.evidence_graph(execution_id).graph_fingerprint
            for execution_id in ("execution-crash-a", "execution-crash-b")
        }
        def fail_after_first(_compaction_id: str, compacted_events: int) -> None:
            # Model abrupt process loss while the SQLite transaction still owns partial writes.
            if compacted_events == 1:
                raise RuntimeError("injected compaction crash")
        monkeypatch.setattr(store, "_after_compaction_event", fail_after_first)
        with pytest.raises(RuntimeError, match="injected compaction crash"):
            store.apply_retention(
                KnowledgeRetentionPolicy(max_age_seconds=1),
                now="2026-01-01T00:00:00Z",
            )
        raw_after_failure = store.query_executions(campaign_id="campaign-closure", limit=10).rows
        compacted_after_failure = store.query_executions(
            campaign_id="campaign-closure",
            include_compacted=True,
            limit=10,
        ).rows
        assert len(raw_after_failure) == len(compacted_after_failure) == 2
        assert store.compaction_records() == () and store.tombstones() == ()
        assert before == {
            execution_id: store.evidence_graph(execution_id).graph_fingerprint
            for execution_id in before
        }
        monkeypatch.setattr(store, "_after_compaction_event", lambda _compaction_id, _count: None)
        completed = store.apply_retention(
            KnowledgeRetentionPolicy(max_age_seconds=1, batch_size=10),
            now="2026-01-01T00:00:00Z",
        )
    finally:
        store.close()
    assert completed.compacted_events == 2 and completed.compaction_id
def test_backup_restore_and_logical_corruption_detection(tmp_path: Path) -> None:
    # Restore one validated snapshot atomically and detect later payload tampering through stored hashes.
    database = tmp_path / "knowledge.sqlite3"
    backup = tmp_path / "knowledge.backup.sqlite3"
    restored = tmp_path / "restored.sqlite3"
    store = KnowledgePlaneStore(database)
    try:
        _ingest_old(store, "execution-backup")
        graph_fingerprint = store.evidence_graph("execution-backup").graph_fingerprint
        store.backup_to(backup)
    finally:
        store.close()
    KnowledgePlaneStore.restore_from(backup, restored)
    restored_store = KnowledgePlaneStore(restored)
    try:
        audit = restored_store.closure_audit()
        assert restored_store.evidence_graph("execution-backup").graph_fingerprint == graph_fingerprint
    finally:
        restored_store.close()
    assert audit["e15_closed"]
    assert audit["schema"]["version"] == audit["schema"]["supported_version"] == KNOWLEDGE_SCHEMA_VERSION
    with sqlite3.connect(restored) as connection:
        connection.execute("DROP TRIGGER knowledge_effects_append_only_update")
        connection.execute("UPDATE knowledge_effects SET payload = '{}' WHERE effect_id = 'effect-execution-backup'")
    corrupted = KnowledgePlaneStore(restored)
    try:
        report = corrupted.verify_integrity()
        with pytest.raises(KnowledgeConflict, match="integrity check failed"):
            corrupted.require_integrity()
    finally:
        corrupted.close()
    assert not report.healthy
    assert any(item.startswith("knowledge_effects:effect-execution-backup:") for item in report.hash_mismatches)

def test_backup_closes_destination_connection_before_atomic_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Require the SQLite destination handle to close before Windows-visible replacement begins.
    database = tmp_path / "knowledge.sqlite3"
    backup = tmp_path / "knowledge.backup.sqlite3"
    store = KnowledgePlaneStore(database)
    _ingest_old(store, "execution-close-order")
    real_connect = sqlite3.connect
    real_replace = os.replace
    state = {"closed": False}
    class TrackingConnection(sqlite3.Connection):
        # Track the actual close call while remaining a native SQLite connection accepted by backup().
        def close(self) -> None:
            # Mark the destination handle closed before delegating to sqlite3.
            state["closed"] = True
            super().close()
    def tracked_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        # Create the temporary backup database through a close-observable native subclass.
        return real_connect(*args, factory=TrackingConnection, **kwargs)
    def guarded_replace(source: str | bytes | os.PathLike[str] | os.PathLike[bytes], target: str | bytes | os.PathLike[str] | os.PathLike[bytes]) -> None:
        # Fail deterministically if atomic publication starts while SQLite still owns the file.
        assert state["closed"], "backup destination connection remained open before os.replace"
        real_replace(source, target)
    monkeypatch.setattr(knowledge_store_module.sqlite3, "connect", tracked_connect)
    monkeypatch.setattr(knowledge_store_module.os, "replace", guarded_replace)
    try:
        assert store.backup_to(backup) == backup
    finally:
        store.close()
    assert backup.is_file()

def test_coordinator_fences_live_reuse_evidence_until_terminal_state() -> None:
    # Keep the retention protection calls as explicit coordinator lifecycle invariants.
    source = (Path(__file__).parents[1] / "theseus_local" / "coordinator.py").read_text(encoding="utf-8")
    assert "campaign_id=current.campaign_id.value" in source
    assert "self.protect_reuse_plan(campaign_id, plan)" in (
        Path(__file__).parents[1] / "theseus_knowledge" / "store.py"
    ).read_text(encoding="utf-8")
    assert source.count("knowledge.release_reuse_plans(current.campaign_id.value)") >= 2
def test_schema_v4_backfills_pre_pr13_compacted_rollups(tmp_path: Path) -> None:
    # Rebuild a schema-v3 rollup into a manifest-backed evidence bundle exactly once during migration 4.
    database = tmp_path / "knowledge.sqlite3"
    store = KnowledgePlaneStore(database)
    try:
        event_id = _ingest_old(store, "execution-legacy-compaction")
        graph_fingerprint = store.evidence_graph("execution-legacy-compaction").graph_fingerprint
        store.apply_retention(
            KnowledgeRetentionPolicy(max_age_seconds=1),
            now="2026-01-01T00:00:00Z",
        )
    finally:
        store.close()
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            DROP TRIGGER knowledge_conflict_quarantine_append_only_update;
            DROP TRIGGER knowledge_conflict_quarantine_append_only_delete;
            DROP TRIGGER knowledge_compaction_runs_append_only_update;
            DROP TRIGGER knowledge_compaction_runs_append_only_delete;
            DROP TRIGGER knowledge_tombstones_append_only_update;
            DROP TRIGGER knowledge_tombstones_append_only_delete;
            DROP TRIGGER knowledge_evidence_references_append_only_update;
            DROP TRIGGER knowledge_evidence_references_append_only_delete;
            DROP TABLE knowledge_evidence_references;
            DROP TABLE knowledge_reuse_plans;
            DROP TABLE knowledge_conflict_quarantine;
            DROP TABLE knowledge_tombstones;
            DROP TABLE knowledge_compaction_runs;
            UPDATE knowledge_execution_rollups
            SET graph_fingerprint = NULL, evidence_payload = NULL, evidence_sha256 = NULL,
                observation_count = 0, compaction_id = NULL;
            DELETE FROM knowledge_schema_migrations WHERE version = 4;
            UPDATE knowledge_meta SET value = '3' WHERE key = 'schema_version';
            """
        )
    migrated = KnowledgePlaneStore(database)
    try:
        evidence = migrated.compacted_evidence(event_id)
        state = migrated.schema_state()
        compactions = migrated.compaction_records()
        tombstones = migrated.tombstones(event_id=event_id)
        integrity = migrated.require_integrity()
    finally:
        migrated.close()
    assert state.version == KNOWLEDGE_SCHEMA_VERSION
    assert evidence is not None and evidence["graph_fingerprint"] == graph_fingerprint
    assert evidence["evidence"]["raw_execution_available"] is False
    assert len(compactions) == len(tombstones) == 1
    assert integrity.healthy
