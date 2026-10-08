from __future__ import annotations
import hashlib
from dataclasses import replace
from pathlib import Path
import pytest
from theseus_contracts.artifacts import ArtifactRegistryEntry
from theseus_contracts.engine import PreparedCampaignSnapshot
from theseus_contracts.ids import CampaignId, ExecutionId, MutantId, ProjectId, RevisionId
from theseus_contracts.mutation import MutantDescriptor, MutantExecutionResult
from theseus_contracts.project import EnvironmentDescriptor, ProjectDescriptor, RepositoryRevision
from theseus_knowledge.evidence import (
    EvidenceNodeType,
    EvidenceRelation,
    KnowledgeEvidenceEdge,
    KnowledgeEvidenceGraph,
    KnowledgeEvidenceNode,
)
from theseus_survivor_adapter import (
    AuthoritativeSurvivorEvidence,
    SurvivorEvidenceError,
    SurvivorRuntimeAdapter,
)
from theseus_survivor_lab.contracts import SelectionEvidence, SourceEvidence, TestEvidence as SurvivorTestEvidence

_SOURCE = "def choose(value):\n    if value > 0:\n        return 1\n    return 0\n"
_TEST_ID = "tests/test_app.py::test_choose"
_EVENT_ID = "event-survivor"
_SCOPE_ID = "scope-survivor"

def _sha(value: str) -> str:
    # Hash fixture text with the same UTF-8 content identity used by the adapter.
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
def _stable_hash(value: object) -> str:
    # Hash fixture JSON with the canonical Knowledge Plane encoding.
    import json
    return _sha(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")))

def _node(
    node_type: EvidenceNodeType,
    identity_key: str,
    *,
    payload: dict[str, object],
    event_id: str = _EVENT_ID,
    node_id: str | None = None,
) -> KnowledgeEvidenceNode:
    # Build one event-scoped provenance node with complete producer and lease identity.
    resolved_id = node_id or f"node-{node_type.value}"
    return KnowledgeEvidenceNode(
        node_id=resolved_id,
        scope_id=_SCOPE_ID,
        node_type=node_type,
        identity_key=identity_key,
        fingerprint=_sha(f"{node_type.value}:{identity_key}"),
        campaign_id="campaign-survivor",
        event_id=event_id,
        effect_id="effect-survivor",
        execution_id="execution-survivor",
        lease_id="lease-survivor",
        worker_id="worker-survivor",
        producer_type="worker",
        producer_id="worker-survivor",
        producer_version="runtime-test",
        observed_at="2026-08-05T00:00:00Z",
        payload_sha256=_stable_hash(payload),
        payload=payload,
    )

def _edge(
    source: KnowledgeEvidenceNode,
    relation: EvidenceRelation,
    target: KnowledgeEvidenceNode,
    *,
    event_id: str = _EVENT_ID,
) -> KnowledgeEvidenceEdge:
    # Build one immutable provenance edge for the same execution event.
    return KnowledgeEvidenceEdge(
        edge_id=f"edge-{relation.value}",
        scope_id=_SCOPE_ID,
        source_node_id=source.node_id,
        relation=relation,
        target_node_id=target.node_id,
        event_id=event_id,
        execution_id="execution-survivor",
        created_at="2026-08-05T00:00:00Z",
        payload_sha256=_stable_hash({}),
        payload={},
    )

def _graph() -> KnowledgeEvidenceGraph:
    # Build one complete authoritative revision-to-result chain for a surviving mutant.
    revision = _node(EvidenceNodeType.REVISION, "revision-survivor", payload={"revision_id": "revision-survivor"})
    environment = _node(EvidenceNodeType.ENVIRONMENT, "environment-survivor", payload={"environment_id": "environment-survivor"})
    function = _node(EvidenceNodeType.FUNCTION, "choose", payload={"function_id": "choose"})
    mutant = _node(EvidenceNodeType.MUTANT, "mutant-survivor", payload={"mutant_id": "mutant-survivor"})
    execution = _node(EvidenceNodeType.EXECUTION, "execution-survivor", payload={"execution_id": "execution-survivor"})
    worker = _node(EvidenceNodeType.WORKER, "worker-survivor", payload={"worker_id": "worker-survivor"})
    lease = _node(EvidenceNodeType.LEASE, "lease-survivor", payload={"lease_id": "lease-survivor"})
    selected = _node(EvidenceNodeType.SELECTED_TEST, f"execution-survivor\x1f{_TEST_ID}", payload={"test_id": _TEST_ID})
    observation = _node(EvidenceNodeType.OBSERVATION, f"{_EVENT_ID}\x1f{_TEST_ID}", payload={"test_id": _TEST_ID, "outcome": "passed"})
    result = _node(EvidenceNodeType.RESULT, "execution-survivor", payload={"semantic_result": "survived"})
    nodes = (revision, environment, function, mutant, execution, worker, lease, selected, observation, result)
    edges = (
        _edge(revision, EvidenceRelation.REVISION_CONTAINS_FUNCTION, function),
        _edge(environment, EvidenceRelation.ENVIRONMENT_GOVERNS_EXECUTION, execution),
        _edge(function, EvidenceRelation.FUNCTION_DEFINES_MUTANT, mutant),
        _edge(mutant, EvidenceRelation.MUTANT_EXECUTED_AS, execution),
        _edge(worker, EvidenceRelation.WORKER_PERFORMED_EXECUTION, execution),
        _edge(lease, EvidenceRelation.LEASE_AUTHORIZED_EXECUTION, execution),
        _edge(execution, EvidenceRelation.EXECUTION_SELECTED_TEST, selected),
        _edge(mutant, EvidenceRelation.MUTANT_SELECTED_TEST, selected),
        _edge(selected, EvidenceRelation.SELECTED_TEST_PRODUCED_OBSERVATION, observation),
        _edge(observation, EvidenceRelation.OBSERVATION_SUPPORTS_RESULT, result),
        _edge(execution, EvidenceRelation.EXECUTION_PRODUCED_RESULT, result),
    )
    graph_fingerprint = _stable_hash({
        "execution_id": "execution-survivor",
        "nodes": [item.to_dict() for item in sorted(nodes, key=lambda item: (item.node_type.value, item.identity_key, item.fingerprint, item.node_id))],
        "edges": [item.to_dict() for item in sorted(edges, key=lambda item: (item.relation.value, item.source_node_id, item.target_node_id, item.edge_id))],
    })
    return KnowledgeEvidenceGraph(
        scope_id=_SCOPE_ID,
        execution_id="execution-survivor",
        nodes=nodes,
        edges=edges,
        graph_fingerprint=graph_fingerprint,
        complete=True,
        missing_requirements=(),
        snapshot_revision=12,
    )

def _evidence() -> AuthoritativeSurvivorEvidence:
    # Build one runtime bundle whose every identity crosses contracts, graph, source, and artifact boundaries.
    source_sha256 = _sha(_SOURCE)
    mutant = MutantDescriptor(
        mutant_id=MutantId("mutant-survivor"),
        mutation="comparison_gt_to_ge",
        source_path="app.py",
        line_no=2,
        column_no=7,
        original="value > 0",
        replacement="value >= 0",
        operator_version="m3",
        function_id="choose",
    )
    execution = MutantExecutionResult(
        execution_id=ExecutionId("execution-survivor"),
        mutant_id=mutant.mutant_id,
        status="survived",
        classification_reason="selected tests passed",
        restore_verified=True,
        lease_id="lease-survivor",
        attempt=0,
        test_observations=(
            {
                "test_id": _TEST_ID,
                "test_fingerprint": _sha(_TEST_ID),
                "outcome": "passed",
                "evidence_kind": "pytest_test_event",
                "observation_schema_version": 1,
            },
        ),
    )
    project = ProjectDescriptor(
        project_id=ProjectId("project-survivor"),
        display_name="Survivor adapter fixture",
        root_path="D:/project",
        revision=RepositoryRevision(RevisionId("revision-survivor")),
        environment=EnvironmentDescriptor(
            fingerprint="environment-survivor",
            python_version="3.14.2",
            pytest_version="9.1.1",
            plugins=("pytest-xdist",),
        ),
    )
    prepared = PreparedCampaignSnapshot(
        campaign_id=CampaignId("campaign-survivor"),
        source_path="app.py",
        source_sha256=source_sha256,
        index_version="index-v1",
        index_payload={},
        mutants=(mutant,),
        function_id="choose",
        function_range=(1, 4),
        repository_fingerprint="repository-survivor",
        environment_fingerprint="environment-survivor",
        configuration_fingerprint="configuration-survivor",
        created_at="2026-08-05T00:00:00Z",
    )
    artifact = ArtifactRegistryEntry(
        campaign_id=CampaignId("campaign-survivor"),
        logical_key="execution/result.json",
        logical_role="execution_result",
        content_sha256=_sha("result"),
        size_bytes=6,
        schema_version=1,
        producer="worker-survivor",
        content_path="artifacts/sha256/result",
        logical_path="campaign/execution/result.json",
        created_at="2026-08-05T00:00:00Z",
        execution_id="execution-survivor",
    )
    return AuthoritativeSurvivorEvidence(
        campaign_id="campaign-survivor",
        project=project,
        prepared=prepared,
        mutant=mutant,
        execution=execution,
        graph=_graph(),
        source=SourceEvidence(
            source_path="app.py",
            source_sha256=source_sha256,
            source_text=_SOURCE,
            function_source=_SOURCE.rstrip("\n"),
            function_start_line=1,
            function_end_line=4,
        ),
        related_tests=(
            SurvivorTestEvidence(
                nodeid=_TEST_ID,
                source_path="tests/test_app.py",
                source_sha256=_sha("def test_choose(): pass\n"),
                source_text="def test_choose(): pass\n",
                selection_reasons=("static-direct",),
                executions=1,
                failures=0,
                median_duration_ms=4.5,
                killed_related_mutants=(),
            ),
        ),
        artifacts=(artifact,),
        selection=SelectionEvidence(
            runtime_location_reached=True,
            mutated_branch_reached=True,
            oracle_observed=True,
            selection_reasons=("static-direct",),
        ),
    )

def test_runtime_adapter_builds_offline_analysis_without_proposals_or_provider() -> None:
    # Prove PR38 performs only classification/context/hypothesis analysis over committed evidence.
    evidence = _evidence()
    result = SurvivorRuntimeAdapter().analyze(evidence)
    assert result.analysis.proposals == (), (
        "PR38 generated a test proposal reserved for PR39; "
        f"result_id={result.analysis.result_id}; proposals={result.analysis.proposals}"
    )
    assert result.analysis.provider.invoked is False, (
        "offline runtime adapter crossed the optional provider boundary; "
        f"provider={result.analysis.provider}; result_id={result.analysis.result_id}"
    )
    assert result.analysis.hypotheses, (
        "offline analysis did not preserve deterministic local hypotheses; "
        f"classification={result.analysis.classification}; blockers={result.analysis.blockers}"
    )
    assert result.source_event_id == _EVENT_ID
    assert result.artifact.content_sha256 == hashlib.sha256(result.artifact.content).hexdigest()
    assert result.artifact.metadata["execution_id"] == evidence.execution.execution_id.value

def test_runtime_adapter_is_deterministic_under_evidence_order_permutations() -> None:
    # Reordering graph rows, related tests, and artifacts must not alter request or artifact identity.
    evidence = _evidence()
    first = SurvivorRuntimeAdapter().analyze(evidence)
    reordered = replace(
        evidence,
        graph=replace(evidence.graph, nodes=tuple(reversed(evidence.graph.nodes)), edges=tuple(reversed(evidence.graph.edges))),
        related_tests=tuple(reversed(evidence.related_tests)),
        artifacts=tuple(reversed(evidence.artifacts)),
    )
    second = SurvivorRuntimeAdapter().analyze(reordered)
    assert second.request == first.request, (
        "runtime request changed after input reordering; "
        f"first={first.request}; second={second.request}"
    )
    assert second.analysis.result_id == first.analysis.result_id
    assert second.artifact.content == first.artifact.content
    assert second.artifact.content_sha256 == first.artifact.content_sha256

def test_runtime_adapter_rejects_cross_event_or_incomplete_provenance() -> None:
    # Fail closed when one analysis would mix source events or use an incomplete graph.
    evidence = _evidence()
    tampered = replace(evidence, graph=replace(evidence.graph, graph_fingerprint=_sha("tampered-graph")))
    with pytest.raises(SurvivorEvidenceError, match="graph fingerprint mismatch") as captured:
        SurvivorRuntimeAdapter().analyze(tampered)
    assert evidence.execution.execution_id.value in str(captured.value)
    changed_node = replace(evidence.graph.nodes[-1], event_id="foreign-event")
    changed_nodes = (*evidence.graph.nodes[:-1], changed_node)
    changed_fingerprint = _stable_hash({
        "execution_id": evidence.graph.execution_id,
        "nodes": [item.to_dict() for item in sorted(changed_nodes, key=lambda item: (item.node_type.value, item.identity_key, item.fingerprint, item.node_id))],
        "edges": [item.to_dict() for item in sorted(evidence.graph.edges, key=lambda item: (item.relation.value, item.source_node_id, item.target_node_id, item.edge_id))],
    })
    conflicting = replace(
        evidence,
        graph=replace(evidence.graph, nodes=changed_nodes, graph_fingerprint=changed_fingerprint),
    )
    with pytest.raises(SurvivorEvidenceError, match="mixes source events") as captured:
        SurvivorRuntimeAdapter().analyze(conflicting)
    assert "execution-survivor" in str(captured.value)
    incomplete = replace(evidence, graph=replace(evidence.graph, complete=False, missing_requirements=("lease",)))
    with pytest.raises(SurvivorEvidenceError, match="incomplete provenance graph") as captured:
        SurvivorRuntimeAdapter().analyze(incomplete)
    assert "lease" in str(captured.value) and evidence.graph.graph_fingerprint in str(captured.value)

def test_runtime_adapter_rejects_source_execution_and_observation_conflicts() -> None:
    # Fence source bytes, mutant identity, and per-test observations before Survivor Lab is invoked.
    evidence = _evidence()
    with pytest.raises(SurvivorEvidenceError, match="source hash mismatch"):
        SurvivorRuntimeAdapter().analyze(replace(evidence, source=replace(evidence.source, source_text=_SOURCE + "# changed\n")))
    foreign_execution = replace(evidence.execution, mutant_id=MutantId("foreign-mutant"))
    with pytest.raises(SurvivorEvidenceError, match="execution mutant mismatch"):
        SurvivorRuntimeAdapter().analyze(replace(evidence, execution=foreign_execution))
    conflicting_observation = dict(evidence.execution.test_observations[0])
    conflicting_observation["outcome"] = "failed"
    duplicated = replace(
        evidence.execution,
        test_observations=(*evidence.execution.test_observations, conflicting_observation),
    )
    with pytest.raises(SurvivorEvidenceError, match="conflicting observations"):
        SurvivorRuntimeAdapter().analyze(replace(evidence, execution=duplicated))

def test_runtime_adapter_does_not_write_workspace_or_production_source(tmp_path: Path) -> None:
    # The base adapter returns artifact bytes and leaves publication to a later explicit integration boundary.
    before = tuple(tmp_path.rglob("*"))
    result = SurvivorRuntimeAdapter().analyze(_evidence())
    after = tuple(tmp_path.rglob("*"))
    assert after == before == (), (
        "offline adapter wrote unexpected files; "
        f"before={before}; after={after}; logical_key={result.artifact.logical_key}"
    )
    assert b"NotImplementedError" not in result.artifact.content, (
        "PR38 artifact unexpectedly contains generated proposal code; "
        f"artifact_sha256={result.artifact.content_sha256}"
    )

def test_root_package_manifest_includes_isolated_survivor_adapter() -> None:
    # Keep clean installations from omitting the new adapter package.
    manifest = (Path(__file__).parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    assert '"theseus_survivor_adapter"' in manifest, (
        "root package list does not include the PR38 adapter; "
        f"manifest={Path(__file__).parents[1] / 'pyproject.toml'}"
    )
    assert 'theseus_survivor_adapter = "theseus_survivor_adapter"' in manifest
