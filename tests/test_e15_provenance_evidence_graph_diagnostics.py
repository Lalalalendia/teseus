from __future__ import annotations
import sqlite3
import sys
from pathlib import Path
import pytest
from theseus_knowledge import (
    EvidenceNodeType,
    EvidenceRelation,
    KNOWLEDGE_SCHEMA_VERSION,
    KnowledgeConflict,
    KnowledgePlaneStore,
)
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    MutationScope,
    ProjectDescriptor,
    ProjectId,
    TestCommandDescriptor,
)
from theseus_local import LocalCampaignCoordinator
from theseus_local.workspace import WorkspaceProvider
def _execution(execution_id: str, *, observations: bool = True) -> dict[str, object]:
    # Build one execution carrying every canonical provenance boundary required by PR12.
    return {
        "execution_id": execution_id,
        "campaign_id": "campaign-graph",
        "shard_id": "shard-graph",
        "mutant_id": "mutant-graph",
        "attempt": 2,
        "status": "complete",
        "semantic_result": "killed",
        "selected_tests": ["test_app.py::test_choose"],
        "artifacts": [f"artifact-{execution_id}"],
        "lease_id": "lease-graph",
        "restore_verified": True,
        "source_kind": "observed",
        "evidence_origin": "pytest_test_stats",
        "evidence_schema_version": 2,
        "function_id": "app.choose",
        "function_fingerprint": "function-v1",
        "mutant_fingerprint": "mutant-v1",
        "test_fingerprint": "tests-v1",
        "test_fingerprints": {"test_app.py::test_choose": "test-node-v1"},
        "conftest_fingerprint": "conftest-v1",
        "environment_fingerprint": "environment-v1",
        "selection_fingerprint": "selection-v1",
        "result_fingerprint": f"result-{execution_id}",
        "test_observations": [
            {
                "test_id": "test_app.py::test_choose",
                "test_fingerprint": "test-node-v1",
                "outcome": "failed",
                "evidence_kind": "pytest_test_event",
                "observation_schema_version": 1,
            }
        ] if observations else [],
    }
def _payload(execution: dict[str, object]) -> dict[str, object]:
    # Bind the execution to exact revision, environment, worker, and lease producer identity.
    return {
        "campaign": {
            "campaign_id": "campaign-graph",
            "project_id": "project-graph",
            "revision_id": "revision-graph",
        },
        "environment_id": "environment-v1",
        "shard": {
            "shard_id": "shard-graph",
            "worker_id": "worker-graph",
            "lease": {
                "lease_id": "lease-graph",
                "worker_id": "worker-graph",
                "attempt": 2,
                "worker_instance_id": "worker-instance-graph",
                "process_id": 4242,
                "process_birth_token": "birth-graph",
            },
        },
        "executions": [execution],
    }
def test_provenance_graph_materializes_the_full_revision_to_artifact_chain(tmp_path: Path) -> None:
    # Prove one committed execution answers who, when, revision, environment, tests, lease, result, and artifacts.
    database = tmp_path / "knowledge.sqlite3"
    store = KnowledgePlaneStore(database)
    try:
        store.ingest_effect(
            effect_id="effect-graph",
            campaign_id="campaign-graph",
            effect_type="mutation.execute_shard",
            payload=_payload(_execution("execution-graph")),
            observed_at="2026-08-05T10:00:00Z",
        )
        graph = store.evidence_graph("execution-graph")
    finally:
        store.close()
    node_types = {item.node_type for item in graph.nodes}
    relations = {item.relation for item in graph.edges}
    assert graph.complete, (
        "full committed execution did not produce a complete provenance graph; "
        f"database={database}; missing={graph.missing_requirements}; graph={graph.to_dict()}"
    )
    assert node_types == set(EvidenceNodeType), (
        "provenance graph omitted one or more canonical node domains; "
        f"database={database}; execution_id=execution-graph; node_types={sorted(item.value for item in node_types)}"
    )
    assert set(EvidenceRelation).issubset(relations), (
        "provenance graph omitted one or more required directed relations; "
        f"database={database}; execution_id=execution-graph; relations={sorted(item.value for item in relations)}"
    )
    assert all(item.producer_id == "worker-graph" for item in graph.nodes), (
        "graph nodes lost the concrete worker producer identity; "
        f"database={database}; producers={[(item.node_type.value, item.producer_id) for item in graph.nodes]}"
    )
    assert all(item.lease_id == "lease-graph" for item in graph.nodes), (
        "graph nodes lost the authoritative lease identity; "
        f"database={database}; leases={[(item.node_type.value, item.lease_id) for item in graph.nodes]}"
    )
    assert all(item.observed_at == "2026-08-05T10:00:00.000000Z" for item in graph.nodes), (
        "graph nodes did not preserve the committed observation timestamp; "
        f"database={database}; timestamps={sorted({item.observed_at for item in graph.nodes})}"
    )
def test_provenance_projection_is_idempotent_and_graph_fingerprint_is_stable(tmp_path: Path) -> None:
    # Replay one committed effect without duplicating graph nodes, edges, or changing graph identity.
    database = tmp_path / "knowledge.sqlite3"
    payload = _payload(_execution("execution-idempotent"))
    store = KnowledgePlaneStore(database)
    try:
        first = store.ingest_effect(
            effect_id="effect-idempotent",
            campaign_id="campaign-graph",
            effect_type="mutation.execute_shard",
            payload=payload,
        )
        graph_before = store.evidence_graph("execution-idempotent")
        counts_before = store.schema_state()
        duplicate = store.ingest_effect(
            effect_id="effect-idempotent",
            campaign_id="campaign-graph",
            effect_type="mutation.execute_shard",
            payload=payload,
        )
        graph_after = store.evidence_graph("execution-idempotent")
        counts_after = store.schema_state()
    finally:
        store.close()
    assert first.inserted_events == 1 and duplicate.duplicate, (
        "effect replay did not preserve the original idempotency contract; "
        f"database={database}; first={first}; duplicate={duplicate}"
    )
    assert graph_after.graph_fingerprint == graph_before.graph_fingerprint, (
        "idempotent replay changed the provenance graph fingerprint; "
        f"database={database}; before={graph_before.graph_fingerprint}; after={graph_after.graph_fingerprint}"
    )
    assert (counts_after.evidence_nodes, counts_after.evidence_edges) == (
        counts_before.evidence_nodes,
        counts_before.evidence_edges,
    ), (
        "idempotent replay appended duplicate graph rows; "
        f"database={database}; before={counts_before.to_dict()}; after={counts_after.to_dict()}"
    )
def test_same_scope_executions_keep_exact_event_worker_and_lease_provenance(tmp_path: Path) -> None:
    # Keep shared semantic identities while fencing every graph node to its producing committed event.
    database = tmp_path / "knowledge.sqlite3"
    first_payload = _payload(_execution("execution-worker-a"))
    second_payload = _payload(_execution("execution-worker-b"))
    second_payload["shard"]["worker_id"] = "worker-b"
    second_payload["shard"]["lease"] = {
        "lease_id": "lease-b",
        "worker_id": "worker-b",
        "attempt": 2,
        "worker_instance_id": "worker-instance-b",
        "process_id": 5252,
        "process_birth_token": "birth-b",
    }
    second_payload["executions"][0]["lease_id"] = "lease-b"
    store = KnowledgePlaneStore(database)
    try:
        store.ingest_effect(
            effect_id="effect-worker-a",
            campaign_id="campaign-graph",
            effect_type="mutation.execute_shard",
            payload=first_payload,
            observed_at="2026-08-05T10:00:00Z",
        )
        store.ingest_effect(
            effect_id="effect-worker-b",
            campaign_id="campaign-graph",
            effect_type="mutation.execute_shard",
            payload=second_payload,
            observed_at="2026-08-05T10:01:00Z",
        )
        first = store.evidence_graph("execution-worker-a")
        second = store.evidence_graph("execution-worker-b")
    finally:
        store.close()
    assert {item.event_id for item in first.nodes} == {first.edges[0].event_id}, (
        "first graph mixed nodes from another committed event; "
        f"database={database}; node_events={sorted({item.event_id for item in first.nodes})}; "
        f"edge_events={sorted({item.event_id for item in first.edges})}"
    )
    assert {item.event_id for item in second.nodes} == {second.edges[0].event_id}, (
        "second graph reused event-scoped nodes from the first execution; "
        f"database={database}; node_events={sorted({item.event_id for item in second.nodes})}; "
        f"edge_events={sorted({item.event_id for item in second.edges})}"
    )
    assert {item.producer_id for item in first.nodes} == {"worker-graph"}, (
        "first graph lost its concrete worker producer; "
        f"database={database}; producers={sorted({item.producer_id for item in first.nodes})}"
    )
    assert {item.producer_id for item in second.nodes} == {"worker-b"}, (
        "second graph inherited the first worker producer; "
        f"database={database}; producers={sorted({item.producer_id for item in second.nodes})}"
    )
    assert {item.lease_id for item in first.nodes} == {"lease-graph"}, (
        "first graph lost its authoritative lease; "
        f"database={database}; leases={sorted({item.lease_id for item in first.nodes})}"
    )
    assert {item.lease_id for item in second.nodes} == {"lease-b"}, (
        "second graph inherited the first lease; "
        f"database={database}; leases={sorted({item.lease_id for item in second.nodes})}"
    )
    assert {
        (item.node_type, item.identity_key, item.fingerprint)
        for item in first.nodes
        if item.node_type in {EvidenceNodeType.REVISION, EvidenceNodeType.FUNCTION, EvidenceNodeType.MUTANT}
    } == {
        (item.node_type, item.identity_key, item.fingerprint)
        for item in second.nodes
        if item.node_type in {EvidenceNodeType.REVISION, EvidenceNodeType.FUNCTION, EvidenceNodeType.MUTANT}
    }, (
        "event fencing changed shared semantic identities instead of only provenance identity; "
        f"database={database}; first={first.to_dict()}; second={second.to_dict()}"
    )
def test_enrichment_appends_missing_observation_provenance_without_rewriting_history(tmp_path: Path) -> None:
    # Add late pytest evidence as new graph facts while retaining the original execution node and revision scope.
    database = tmp_path / "knowledge.sqlite3"
    base_execution = _execution("execution-enriched", observations=False)
    base_execution["selected_tests"] = []
    base_execution.pop("test_fingerprints")
    base_execution.pop("test_fingerprint")
    base_execution.pop("result_fingerprint")
    base = _payload(base_execution)
    enriched = _payload(_execution("execution-enriched"))
    store = KnowledgePlaneStore(database)
    try:
        store.ingest_effect(
            effect_id="effect-enriched",
            campaign_id="campaign-graph",
            effect_type="mutation.execute_shard",
            payload=base,
        )
        before = store.evidence_graph("execution-enriched")
        result = store.enrich_effect(
            effect_id="effect-enriched",
            campaign_id="campaign-graph",
            payload=enriched,
        )
        after = store.evidence_graph("execution-enriched")
    finally:
        store.close()
    assert not before.complete and {"selected_test", "observation"}.issubset(before.missing_requirements), (
        "incomplete base effect was incorrectly reported as full provenance; "
        f"database={database}; graph={before.to_dict()}"
    )
    assert result.inserted_observations == 1 and after.complete, (
        "late enrichment did not append the missing selected-test and observation chain; "
        f"database={database}; result={result}; missing={after.missing_requirements}"
    )
    assert after.snapshot_revision > before.snapshot_revision, (
        "provenance enrichment did not advance the Knowledge Plane revision fence; "
        f"database={database}; before={before.snapshot_revision}; after={after.snapshot_revision}"
    )
def test_same_function_and_mutant_are_scoped_by_revision_and_environment(tmp_path: Path) -> None:
    # Prevent identical symbol names from collapsing across canonical revision or environment scopes.
    database = tmp_path / "knowledge.sqlite3"
    first_payload = _payload(_execution("execution-scope-a"))
    second_payload = _payload(_execution("execution-scope-b"))
    second_payload["campaign"] = {
        "campaign_id": "campaign-graph-b",
        "project_id": "project-graph",
        "revision_id": "revision-graph-b",
    }
    second_payload["environment_id"] = "environment-v2"
    second_payload["executions"][0]["campaign_id"] = "campaign-graph-b"
    store = KnowledgePlaneStore(database)
    try:
        store.ingest_effect(
            effect_id="effect-scope-a",
            campaign_id="campaign-graph",
            effect_type="mutation.execute_shard",
            payload=first_payload,
        )
        store.ingest_effect(
            effect_id="effect-scope-b",
            campaign_id="campaign-graph-b",
            effect_type="mutation.execute_shard",
            payload=second_payload,
        )
        first = store.evidence_graph("execution-scope-a")
        second = store.evidence_graph("execution-scope-b")
    finally:
        store.close()
    assert first.scope_id != second.scope_id, (
        "revision/environment changes collapsed two provenance graphs into one scope; "
        f"database={database}; first_scope={first.scope_id}; second_scope={second.scope_id}"
    )
    assert first.graph_fingerprint != second.graph_fingerprint, (
        "different canonical scopes produced the same graph fingerprint; "
        f"database={database}; graph_fingerprint={first.graph_fingerprint}"
    )
def test_graph_tables_are_append_only_and_conflicts_fail_closed(tmp_path: Path) -> None:
    # Reject direct mutation of accepted graph facts with an explicit append-only diagnostic.
    database = tmp_path / "knowledge.sqlite3"
    store = KnowledgePlaneStore(database)
    try:
        store.ingest_effect(
            effect_id="effect-append-only-graph",
            campaign_id="campaign-graph",
            effect_type="mutation.execute_shard",
            payload=_payload(_execution("execution-append-only-graph")),
        )
    finally:
        store.close()
    statements = (
        "UPDATE knowledge_evidence_nodes SET fingerprint = 'rewritten'",
        "DELETE FROM knowledge_evidence_edges",
    )
    with sqlite3.connect(database) as connection:
        for statement in statements:
            with pytest.raises(sqlite3.IntegrityError) as captured:
                connection.execute(statement)
            assert "append-only" in str(captured.value), (
                "raw provenance graph mutation failed without naming the invariant; "
                f"database={database}; statement={statement!r}; error={captured.value!r}"
            )
def test_schema_v3_backfills_existing_executions_into_the_evidence_graph(tmp_path: Path) -> None:
    # Simulate a schema-v2 database and require migration 3 to rebuild graph facts exactly once.
    database = tmp_path / "knowledge.sqlite3"
    first = KnowledgePlaneStore(database)
    first.ingest_effect(
        effect_id="effect-backfill",
        campaign_id="campaign-graph",
        effect_type="mutation.execute_shard",
        payload=_payload(_execution("execution-backfill")),
    )
    first.close()
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            DROP TRIGGER IF EXISTS knowledge_evidence_nodes_append_only_update;
            DROP TRIGGER IF EXISTS knowledge_evidence_nodes_append_only_delete;
            DROP TRIGGER IF EXISTS knowledge_evidence_edges_append_only_update;
            DROP TRIGGER IF EXISTS knowledge_evidence_edges_append_only_delete;
            DROP TABLE knowledge_evidence_edges;
            DROP TABLE knowledge_evidence_nodes;
            DELETE FROM knowledge_schema_migrations WHERE version >= 3;
            UPDATE knowledge_meta SET value = '2' WHERE key = 'schema_version';
            """
        )
    migrated = KnowledgePlaneStore(database)
    try:
        state = migrated.schema_state()
        graph = migrated.evidence_graph("execution-backfill")
        counts = (state.evidence_nodes, state.evidence_edges)
    finally:
        migrated.close()
    restarted = KnowledgePlaneStore(database)
    try:
        restarted_state = restarted.schema_state()
        restarted_graph = restarted.evidence_graph("execution-backfill")
    finally:
        restarted.close()
    assert state.version == KNOWLEDGE_SCHEMA_VERSION and graph.complete, (
        "schema-v3 migration did not backfill a complete provenance chain; "
        f"database={database}; state={state.to_dict()}; graph={graph.to_dict()}"
    )
    assert (restarted_state.evidence_nodes, restarted_state.evidence_edges) == counts, (
        "reopening a migrated database duplicated graph rows; "
        f"database={database}; first_counts={counts}; restarted={restarted_state.to_dict()}"
    )
    assert restarted_graph.graph_fingerprint == graph.graph_fingerprint, (
        "restarted graph fingerprint differs from the migration result; "
        f"database={database}; first={graph.graph_fingerprint}; restarted={restarted_graph.graph_fingerprint}"
    )
def test_incomplete_validated_provenance_names_exact_missing_boundaries(tmp_path: Path) -> None:
    # Report absent producer and test boundaries instead of silently treating partial history as complete evidence.
    database = tmp_path / "knowledge.sqlite3"
    execution = _execution("execution-incomplete")
    execution.pop("lease_id")
    payload = {
        "campaign": {
            "campaign_id": "campaign-incomplete",
            "project_id": "project-incomplete",
            "revision_id": "revision-incomplete",
        },
        "environment_id": "environment-v1",
        "executions": [execution],
    }
    store = KnowledgePlaneStore(database)
    try:
        store.ingest_effect(
            effect_id="effect-incomplete",
            campaign_id="campaign-incomplete",
            effect_type="mutation.execute_shard",
            payload=payload,
        )
        graph = store.evidence_graph("execution-incomplete")
    finally:
        store.close()
    assert not graph.complete, (
        "validated evidence without worker and lease ownership was marked complete; "
        f"database={database}; graph={graph.to_dict()}"
    )
    assert graph.missing_requirements == ("lease", "worker"), (
        "incomplete graph did not name the exact missing ownership boundaries; "
        f"database={database}; missing={graph.missing_requirements}"
    )
def test_evidence_graph_query_is_bounded_and_unknown_execution_fails_diagnostically(tmp_path: Path) -> None:
    # Keep graph reads bounded and distinguish missing execution evidence from an empty successful graph.
    database = tmp_path / "knowledge.sqlite3"
    store = KnowledgePlaneStore(database)
    try:
        store.ingest_effect(
            effect_id="effect-bounded",
            campaign_id="campaign-graph",
            effect_type="mutation.execute_shard",
            payload=_payload(_execution("execution-bounded")),
        )
        with pytest.raises(KnowledgeConflict, match="does not exist") as missing:
            store.evidence_graph("execution-unknown")
        with pytest.raises(KnowledgeConflict, match="bounded edge limit") as bounded:
            store.evidence_graph("execution-bounded", limit=1)
    finally:
        store.close()
    assert "execution_id=execution-unknown" in str(missing.value), (
        "missing graph diagnostic omitted the requested execution identity; "
        f"database={database}; message={missing.value!r}"
    )
    assert "execution_id=execution-bounded" in str(bounded.value), (
        "bounded graph diagnostic omitted the overflowing execution identity; "
        f"database={database}; message={bounded.value!r}"
    )
def test_local_coordinator_projects_real_worker_lease_and_pytest_provenance(tmp_path: Path) -> None:
    # Prove the production outbox and enrichment path materializes the same complete evidence graph.
    (tmp_path / "app.py").write_text(
        "def choose(value):\n    if value > 0:\n        return 1\n    return 0\n",
        encoding="utf-8",
    )
    (tmp_path / "test_app.py").write_text(
        "from app import choose\n\ndef test_choose():\n    assert choose(1) == 1\n",
        encoding="utf-8",
    )
    configuration = CampaignConfiguration(
        campaign_id=CampaignId("campaign-real-graph"),
        project=ProjectDescriptor(
            project_id=ProjectId("project-real-graph"),
            display_name="PR12 real provenance graph",
            root_path=str(tmp_path),
            test_command=TestCommandDescriptor((sys.executable, "-m", "pytest", "-q")),
            pytest_plugin_autoload=False,
        ),
        scope=MutationScope(source_path="app.py", function="choose", operators=("condition_to_not",)),
        budget=CampaignBudget(max_mutants=1, max_workers=1),
        no_escalation=True,
        reports_dir=str(tmp_path / "reports"),
        reuse_mode="hint",
    )
    result = LocalCampaignCoordinator().run(configuration)
    assert result.succeeded, (
        "production coordinator did not complete the provenance fixture campaign; "
        f"campaign_id={configuration.campaign_id.value}; database={result.database_path}; error={result.error}"
    )
    provider = WorkspaceProvider(configuration)
    database = provider.knowledge_root / "project-real-graph.sqlite3"
    store = KnowledgePlaneStore(database)
    try:
        rows = store.query_executions(
            campaign_id=configuration.campaign_id.value,
            include_payload=True,
            limit=10,
        ).rows
        assert len(rows) == 1, (
            "production campaign did not project exactly one execution into Knowledge Plane; "
            f"campaign_id={configuration.campaign_id.value}; database={database}; rows={rows}"
        )
        graph = store.evidence_graph(str(rows[0]["execution_id"]))
    finally:
        store.close()
    assert graph.complete, (
        "production outbox/enrichment path produced incomplete provenance; "
        f"campaign_id={configuration.campaign_id.value}; database={database}; "
        f"missing={graph.missing_requirements}; graph={graph.to_dict()}"
    )
    assert {item.node_type for item in graph.nodes}.issuperset(
        {
            EvidenceNodeType.REVISION,
            EvidenceNodeType.ENVIRONMENT,
            EvidenceNodeType.FUNCTION,
            EvidenceNodeType.MUTANT,
            EvidenceNodeType.WORKER,
            EvidenceNodeType.LEASE,
            EvidenceNodeType.SELECTED_TEST,
            EvidenceNodeType.OBSERVATION,
            EvidenceNodeType.RESULT,
        }
    ), (
        "production graph omitted one or more execution provenance domains; "
        f"campaign_id={configuration.campaign_id.value}; "
        f"node_types={sorted(item.node_type.value for item in graph.nodes)}"
    )
