from __future__ import annotations
import json
import sqlite3
from pathlib import Path
import pytest
from theseus_knowledge import (
    KNOWLEDGE_SCHEMA_VERSION,
    KnowledgeConflict,
    KnowledgePlaneStore,
    KnowledgeRetentionPolicy,
)
def _execution(
    execution_id: str,
    *,
    mutant_id: str = "mutant-1",
    function_fingerprint: str = "function-v1",
    mutant_fingerprint: str = "mutant-v1",
    test_fingerprint: str = "test-v1",
    observations: bool = True,
) -> dict[str, object]:
    # Build one fully identified observed execution for canonical-schema tests.
    return {
        "execution_id": execution_id,
        "mutant_id": mutant_id,
        "attempt": 0,
        "semantic_result": "killed",
        "status": "complete",
        "restore_verified": True,
        "evidence_schema_version": 2,
        "source_kind": "observed",
        "evidence_origin": "pytest_test_stats",
        "function_id": "module.choose",
        "function_fingerprint": function_fingerprint,
        "mutant_fingerprint": mutant_fingerprint,
        "test_fingerprint": test_fingerprint,
        "environment_fingerprint": "environment-v1",
        "selection_fingerprint": "selection-v1",
        "result_fingerprint": f"result-{execution_id}",
        "test_observations": [
            {
                "test_id": "test_app.py::test_choose",
                "test_fingerprint": test_fingerprint,
                "outcome": "failed",
                "evidence_kind": "pytest_test_event",
                "observation_schema_version": 1,
            }
        ] if observations else [],
    }
def _payload(execution: dict[str, object]) -> dict[str, object]:
    # Bind one execution to explicit project, revision, and environment identity.
    return {
        "campaign": {
            "campaign_id": "campaign-schema",
            "project_id": "project-schema",
            "revision_id": "revision-schema",
        },
        "environment_id": "environment-v1",
        "executions": [execution],
    }
def test_fresh_database_records_exact_schema_migrations_and_scope_identity(tmp_path: Path) -> None:
    # Require one canonical schema version and checksummed migration receipts on a fresh database.
    database = tmp_path / "knowledge.sqlite3"
    store = KnowledgePlaneStore(database)
    try:
        result = store.ingest_effect(
            effect_id="effect-schema",
            campaign_id="campaign-schema",
            effect_type="mutation.execute_shard",
            payload=_payload(_execution("execution-schema")),
            control_revision=7,
        )
        state = store.schema_state()
        rows = store.query_executions(
            project_id="project-schema",
            revision_id="revision-schema",
            environment_id="environment-v1",
            include_payload=True,
        ).rows
        bindings = store.identity_bindings(scope_id=result.scope_id)
    finally:
        store.close()
    assert state.version == state.supported_version == KNOWLEDGE_SCHEMA_VERSION, (
        "canonical Knowledge Plane schema did not reach the runtime version; "
        f"database={database}; state={state.to_dict()}"
    )
    assert [item[0] for item in state.migrations] == list(range(1, KNOWLEDGE_SCHEMA_VERSION + 1)), (
        "schema migration receipts are missing or out of order; "
        f"database={database}; migrations={state.migrations}"
    )
    assert len(rows) == 1 and rows[0]["scope_id"] == result.scope_id, (
        "canonical scope was not queryable through project/revision/environment identity; "
        f"database={database}; result_scope={result.scope_id}; rows={rows}"
    )
    assert {item["identity_type"] for item in bindings} == {"execution", "function", "mutant", "test"}, (
        "canonical identity registry lost one or more entity domains; "
        f"database={database}; scope_id={result.scope_id}; bindings={bindings}"
    )
def test_scoped_function_mutant_and_test_identities_cannot_be_rebound(tmp_path: Path) -> None:
    # Reject every historical entity key that is reused with another fingerprint in the same scope.
    database = tmp_path / "knowledge.sqlite3"
    store = KnowledgePlaneStore(database)
    try:
        store.ingest_effect(
            effect_id="effect-source",
            campaign_id="campaign-schema",
            effect_type="mutation.execute_shard",
            payload=_payload(_execution("execution-source")),
        )
        cases = (
            ("function", _execution("execution-function-conflict", mutant_id="mutant-2", function_fingerprint="function-v2")),
            ("mutant", _execution("execution-mutant-conflict", mutant_fingerprint="mutant-v2")),
            ("test", _execution("execution-test-conflict", mutant_id="mutant-3", test_fingerprint="test-v2")),
        )
        for identity_type, execution in cases:
            with pytest.raises(KnowledgeConflict) as captured:
                store.ingest_effect(
                    effect_id=f"effect-{identity_type}-conflict",
                    campaign_id="campaign-schema",
                    effect_type="mutation.execute_shard",
                    payload=_payload(execution),
                )
            message = str(captured.value)
            assert f"identity_type={identity_type}" in message and "scope_id=" in message, (
                "identity conflict omitted the broken domain or canonical scope; "
                f"database={database}; identity_type={identity_type}; message={message!r}"
            )
        summary = store.summarize_campaign("campaign-schema")
    finally:
        store.close()
    assert summary.executions == 1, (
        "conflicting identity transaction changed historical execution cardinality; "
        f"database={database}; summary={summary}"
    )
def test_enrichment_rejects_conflicting_observation_instead_of_ignoring_it(tmp_path: Path) -> None:
    # Make a repeated test observation fail loudly when its fingerprint or outcome changes.
    database = tmp_path / "knowledge.sqlite3"
    base_execution = _execution("execution-enrichment", observations=False)
    base_execution.pop("test_fingerprint")
    base_execution.pop("result_fingerprint")
    base = _payload(base_execution)
    first = _payload(_execution("execution-enrichment", test_fingerprint="test-v1"))
    conflicting = _payload(_execution("execution-enrichment", test_fingerprint="test-v2"))
    store = KnowledgePlaneStore(database)
    try:
        store.ingest_effect(
            effect_id="effect-enrichment",
            campaign_id="campaign-schema",
            effect_type="mutation.execute_shard",
            payload=base,
        )
        inserted = store.enrich_effect(
            effect_id="effect-enrichment",
            campaign_id="campaign-schema",
            payload=first,
        )
        revision_before_duplicate = store.query_executions(campaign_id="campaign-schema").snapshot_revision
        duplicate = store.enrich_effect(
            effect_id="effect-enrichment",
            campaign_id="campaign-schema",
            payload=first,
        )
        revision_after_duplicate = store.query_executions(campaign_id="campaign-schema").snapshot_revision
        with pytest.raises(KnowledgeConflict) as captured:
            store.enrich_effect(
                effect_id="effect-enrichment",
                campaign_id="campaign-schema",
                payload=conflicting,
            )
        record = store.query_executions(campaign_id="campaign-schema", include_payload=True).rows[0]
    finally:
        store.close()
    message = str(captured.value)
    assert inserted.inserted_observations == 1, (
        "first enrichment did not append exactly one observation; "
        f"database={database}; result={inserted}"
    )
    assert duplicate.inserted_observations == 0 and revision_after_duplicate == revision_before_duplicate, (
        "identical enrichment appended a second observation or advanced projection revision; "
        f"database={database}; duplicate={duplicate}; before={revision_before_duplicate}; "
        f"after={revision_after_duplicate}"
    )
    assert "test_id=test_app.py::test_choose" in message and "existing_fingerprint=" in message, (
        "observation conflict omitted test identity and both fingerprint sides; "
        f"database={database}; message={message!r}"
    )
    assert record["test_fingerprint"] == "test-v1", (
        "conflicting enrichment rewrote the accepted historical fingerprint; "
        f"database={database}; record={record}"
    )
def test_effect_identity_includes_campaign_type_revision_and_scope(tmp_path: Path) -> None:
    # Reject an identical payload when the same effect ID is reused under different control metadata.
    database = tmp_path / "knowledge.sqlite3"
    payload = _payload(_execution("execution-effect"))
    store = KnowledgePlaneStore(database)
    try:
        store.ingest_effect(
            effect_id="effect-metadata",
            campaign_id="campaign-schema",
            effect_type="mutation.execute_shard",
            payload=payload,
            control_revision=10,
        )
        with pytest.raises(KnowledgeConflict) as captured:
            store.ingest_effect(
                effect_id="effect-metadata",
                campaign_id="campaign-other",
                effect_type="mutation.finalize",
                payload=payload,
                control_revision=11,
            )
    finally:
        store.close()
    message = str(captured.value)
    assert "campaign_id" in message and "effect_type" in message and "control_revision" in message, (
        "effect metadata conflict omitted one of its immutable identity components; "
        f"database={database}; message={message!r}"
    )
def test_append_only_triggers_reject_raw_history_updates(tmp_path: Path) -> None:
    # Prove raw effect, execution, and observation bytes cannot be updated behind the store API.
    database = tmp_path / "knowledge.sqlite3"
    store = KnowledgePlaneStore(database)
    try:
        store.ingest_effect(
            effect_id="effect-append-only",
            campaign_id="campaign-schema",
            effect_type="mutation.execute_shard",
            payload=_payload(_execution("execution-append-only")),
        )
    finally:
        store.close()
    with sqlite3.connect(database) as connection:
        with pytest.raises(sqlite3.IntegrityError) as captured:
            connection.execute(
                "INSERT INTO knowledge_effects(effect_id, campaign_id, effect_type, payload_sha256, payload, created_at) "
                "VALUES ('invalid-effect', 'campaign-schema', 'mutation.execute_shard', 'hash', '{}', '2026-01-01T00:00:00Z')"
            )
        assert "canonical scope identity" in str(captured.value), (
            "direct insert without canonical scope was not rejected diagnostically; "
            f"database={database}; error={captured.value!r}"
        )
    statements = (
        "UPDATE knowledge_effects SET payload = '{}' WHERE effect_id = 'effect-append-only'",
        "UPDATE knowledge_executions SET status = 'survived' WHERE execution_id = 'execution-append-only'",
        "UPDATE knowledge_test_observations SET outcome = 'passed' WHERE test_id = 'test_app.py::test_choose'",
    )
    with sqlite3.connect(database) as connection:
        for statement in statements:
            with pytest.raises(sqlite3.IntegrityError) as captured:
                connection.execute(statement)
            assert "append-only" in str(captured.value), (
                "raw historical update failed without naming the append-only invariant; "
                f"database={database}; statement={statement!r}; error={captured.value!r}"
            )
def test_legacy_database_migrates_once_and_backfills_deterministic_identities(tmp_path: Path) -> None:
    # Upgrade one pre-versioned E15 database without duplicating facts on repeated startup.
    database = tmp_path / "knowledge.sqlite3"
    payload = {"executions": [{"execution_id": "legacy-execution", "mutant_id": "legacy-mutant", "attempt": 0, "status": "killed"}]}
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            CREATE TABLE knowledge_effects (effect_id TEXT PRIMARY KEY, campaign_id TEXT NOT NULL, effect_type TEXT NOT NULL, payload_sha256 TEXT NOT NULL, payload TEXT NOT NULL, control_revision INTEGER, created_at TEXT NOT NULL);
            CREATE TABLE knowledge_executions (event_id TEXT PRIMARY KEY, effect_id TEXT NOT NULL, campaign_id TEXT NOT NULL, execution_id TEXT NOT NULL, mutant_id TEXT NOT NULL, attempt INTEGER NOT NULL, status TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(effect_id, execution_id));
            CREATE TABLE knowledge_execution_fingerprints (event_id TEXT PRIMARY KEY, effect_id TEXT NOT NULL, campaign_id TEXT NOT NULL, execution_id TEXT NOT NULL, mutant_id TEXT NOT NULL, function_id TEXT, function_fingerprint TEXT, mutant_fingerprint TEXT, test_fingerprint TEXT, environment_fingerprint TEXT, selection_fingerprint TEXT, result_fingerprint TEXT, source_kind TEXT NOT NULL, retention_class TEXT NOT NULL, knowledge_revision INTEGER NOT NULL, created_at TEXT NOT NULL);
            CREATE TABLE knowledge_test_observations (observation_id TEXT PRIMARY KEY, event_id TEXT NOT NULL, effect_id TEXT NOT NULL, campaign_id TEXT NOT NULL, execution_id TEXT NOT NULL, mutant_id TEXT NOT NULL, test_id TEXT NOT NULL, test_fingerprint TEXT NOT NULL, outcome TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(event_id, test_id));
            CREATE TABLE knowledge_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO knowledge_meta(key, value) VALUES ('projection_revision', '1');
            """
        )
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        execution_json = json.dumps(payload["executions"][0], ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        connection.execute(
            "INSERT INTO knowledge_effects VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("legacy-effect", "legacy-campaign", "mutation.execute_shard", "legacy-hash", payload_json, 1, "2026-01-01T00:00:00Z"),
        )
        connection.execute(
            "INSERT INTO knowledge_executions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("legacy-event", "legacy-effect", "legacy-campaign", "legacy-execution", "legacy-mutant", 0, "killed", execution_json, "2026-01-01T00:00:00Z"),
        )
        connection.execute(
            "INSERT INTO knowledge_execution_fingerprints(event_id, effect_id, campaign_id, execution_id, mutant_id, source_kind, retention_class, knowledge_revision, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("legacy-event", "legacy-effect", "legacy-campaign", "legacy-execution", "legacy-mutant", "observed", "campaign", 0, "2026-01-01T00:00:00Z"),
        )
    first = KnowledgePlaneStore(database)
    first_state = first.schema_state()
    first_rows = first.query_executions(campaign_id="legacy-campaign").rows
    first_bindings = first.identity_bindings(identity_type="execution")
    first.close()
    second = KnowledgePlaneStore(database)
    second_state = second.schema_state()
    second_bindings = second.identity_bindings(identity_type="execution")
    second.close()
    assert first_state == second_state and first_state.version == KNOWLEDGE_SCHEMA_VERSION, (
        "legacy migration was not idempotent across restart; "
        f"database={database}; first={first_state.to_dict()}; second={second_state.to_dict()}"
    )
    assert len(first_rows) == 1 and first_rows[0]["project_id"] == "legacy-project:legacy-campaign", (
        "legacy migration did not backfill deterministic project/revision/environment identity; "
        f"database={database}; rows={first_rows}"
    )
    assert first_bindings == second_bindings and len(first_bindings) == 1, (
        "legacy migration duplicated or changed execution identity bindings on restart; "
        f"database={database}; first={first_bindings}; second={second_bindings}"
    )

def test_compaction_preserves_canonical_scope_identity(tmp_path: Path) -> None:
    # Keep project, revision, environment, and execution identity queryable after raw-row compaction.
    database = tmp_path / "knowledge.sqlite3"
    store = KnowledgePlaneStore(database)
    try:
        inserted = store.ingest_effect(
            effect_id="effect-compaction",
            campaign_id="campaign-schema",
            effect_type="mutation.execute_shard",
            payload=_payload(_execution("execution-compaction")),
            observed_at="2020-01-01T00:00:00Z",
        )
        retained = store.apply_retention(
            KnowledgeRetentionPolicy(max_age_seconds=1),
            now="2026-01-01T00:00:00Z",
        )
        rows = store.query_executions(
            project_id="project-schema",
            revision_id="revision-schema",
            environment_id="environment-v1",
            include_compacted=True,
        ).rows
    finally:
        store.close()
    assert retained.compacted_events == 1 and len(rows) == 1, (
        "retention lost the compacted execution or its canonical scope; "
        f"database={database}; retained={retained}; rows={rows}"
    )
    assert rows[0]["scope_id"] == inserted.scope_id and rows[0]["identity_sha256"], (
        "compacted rollup lost scope or execution identity; "
        f"database={database}; inserted_scope={inserted.scope_id}; row={rows[0]}"
    )

def test_future_schema_version_is_rejected_with_database_diagnostics(tmp_path: Path) -> None:
    # Refuse to open a database whose canonical schema is newer than the running code.
    database = tmp_path / "knowledge.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE knowledge_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        connection.execute("INSERT INTO knowledge_meta(key, value) VALUES ('schema_version', '999')")
    with pytest.raises(KnowledgeConflict) as captured:
        KnowledgePlaneStore(database)
    message = str(captured.value)
    assert str(database) in message and "actual_version=999" in message, (
        "future-schema rejection omitted the database path or actual version; "
        f"database={database}; message={message!r}"
    )
def test_migration_checksum_tampering_is_detected_before_queries(tmp_path: Path) -> None:
    # Reject a schema version whose recorded migration meaning changed after application.
    database = tmp_path / "knowledge.sqlite3"
    store = KnowledgePlaneStore(database)
    store.close()
    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE knowledge_schema_migrations SET checksum = 'tampered' WHERE version = 1"
        )
    with pytest.raises(KnowledgeConflict) as captured:
        KnowledgePlaneStore(database)
    message = str(captured.value)
    assert str(database) in message and "version=1" in message and "actual_checksum=tampered" in message, (
        "migration checksum conflict omitted database, version, or actual checksum; "
        f"database={database}; message={message!r}"
    )
def test_schema_migration_failure_rolls_back_version_receipt_and_partial_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Prove one failed migration cannot publish its version receipt or partial canonical rows.
    database = tmp_path / "knowledge.sqlite3"
    original = KnowledgePlaneStore._migrate_identity_bindings
    def fail_second_migration(self: KnowledgePlaneStore) -> None:
        # Write one row inside migration two and then inject the crash boundary.
        self._connection.execute(
            "INSERT INTO knowledge_scopes(scope_id, project_id, revision_id, environment_id, identity_source, created_at) "
            "VALUES ('partial-scope', 'partial-project', 'partial-revision', 'partial-environment', 'test', '2026-01-01T00:00:00Z')"
        )
        raise RuntimeError("injected canonical schema migration failure")
    monkeypatch.setattr(KnowledgePlaneStore, "_migrate_identity_bindings", fail_second_migration)
    with pytest.raises(RuntimeError, match="injected canonical schema migration failure"):
        KnowledgePlaneStore(database)
    with sqlite3.connect(database) as connection:
        schema_version = int(
            connection.execute(
                "SELECT value FROM knowledge_meta WHERE key = 'schema_version'"
            ).fetchone()[0]
        )
        migration_versions = [
            int(row[0])
            for row in connection.execute(
                "SELECT version FROM knowledge_schema_migrations ORDER BY version"
            ).fetchall()
        ]
        partial_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM knowledge_scopes WHERE scope_id = 'partial-scope'"
            ).fetchone()[0]
        )
    assert schema_version == 1 and migration_versions == [1] and partial_count == 0, (
        "failed migration leaked a version receipt or partial canonical identity row; "
        f"database={database}; schema_version={schema_version}; "
        f"migration_versions={migration_versions}; partial_count={partial_count}"
    )
    monkeypatch.setattr(KnowledgePlaneStore, "_migrate_identity_bindings", original)
    recovered = KnowledgePlaneStore(database)
    recovered_state = recovered.schema_state()
    recovered.close()
    assert recovered_state.version == KNOWLEDGE_SCHEMA_VERSION, (
        "database could not resume the unapplied migration after the injected failure; "
        f"database={database}; state={recovered_state.to_dict()}"
    )
