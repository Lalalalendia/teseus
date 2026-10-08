"""Fail-closed offline adapter from committed runtime evidence to Survivor Lab."""
from __future__ import annotations
import hashlib
import json
from dataclasses import replace
from typing import Any, Mapping
from theseus_knowledge.evidence import EvidenceNodeType, EvidenceRelation, KnowledgeEvidenceNode
from theseus_survivor_lab.contracts import (
    EnvironmentEvidence,
    ExecutionEvidence,
    ExecutionStatus,
    MutantEvidence,
    MutantStatus,
    RequestedMode,
    SurvivorAnalysisRequest,
)
from theseus_survivor_lab.errors import SurvivorLabError
from theseus_survivor_lab.providers import NullRepairProposalProvider
from theseus_survivor_lab.serialization import canonical_json, result_to_dict
from theseus_survivor_lab.service import SurvivorAnalysisService
from theseus_survivor_lab.validation import canonical_relative_path, normalize_request, semantic_text_sha256, sha256_text
from .contracts import (
    AuthoritativeSurvivorEvidence,
    SurvivorAdapterError,
    SurvivorAdapterResult,
    SurvivorAnalysisArtifact,
    SurvivorEvidenceError,
)
_ALLOWED_STATUSES = frozenset({"survived", "timeout", "error", "infrastructure_error"})
_REQUIRED_NODES = frozenset({
    EvidenceNodeType.REVISION,
    EvidenceNodeType.ENVIRONMENT,
    EvidenceNodeType.MUTANT,
    EvidenceNodeType.EXECUTION,
    EvidenceNodeType.WORKER,
    EvidenceNodeType.LEASE,
    EvidenceNodeType.SELECTED_TEST,
    EvidenceNodeType.OBSERVATION,
    EvidenceNodeType.RESULT,
})
_REQUIRED_RELATIONS = frozenset({
    EvidenceRelation.ENVIRONMENT_GOVERNS_EXECUTION,
    EvidenceRelation.MUTANT_EXECUTED_AS,
    EvidenceRelation.WORKER_PERFORMED_EXECUTION,
    EvidenceRelation.LEASE_AUTHORIZED_EXECUTION,
    EvidenceRelation.EXECUTION_SELECTED_TEST,
    EvidenceRelation.MUTANT_SELECTED_TEST,
    EvidenceRelation.SELECTED_TEST_PRODUCED_OBSERVATION,
    EvidenceRelation.OBSERVATION_SUPPORTS_RESULT,
    EvidenceRelation.EXECUTION_PRODUCED_RESULT,
})
def _hash_bytes(value: bytes) -> str:
    # Hash one immutable artifact payload with SHA-256.
    return hashlib.sha256(value).hexdigest()
def _stable_hash(value: Any) -> str:
    # Hash JSON-compatible evidence with the canonical Knowledge Plane encoding.
    try:
        payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SurvivorEvidenceError(f"runtime evidence is not JSON-compatible: value={value!r}; error={exc}") from exc
    return _hash_bytes(payload)
def _graph_fingerprint(evidence: AuthoritativeSurvivorEvidence) -> str:
    # Recompute the Knowledge Plane graph fingerprint independent of input row order.
    nodes = sorted(
        evidence.graph.nodes,
        key=lambda item: (item.node_type.value, item.identity_key, item.fingerprint, item.node_id),
    )
    edges = sorted(
        evidence.graph.edges,
        key=lambda item: (item.relation.value, item.source_node_id, item.target_node_id, item.edge_id),
    )
    return _stable_hash({
        "execution_id": evidence.graph.execution_id,
        "nodes": [item.to_dict() for item in nodes],
        "edges": [item.to_dict() for item in edges],
    })
def _canonical_path(value: str, field: str) -> str:
    # Normalize one path through the shared cross-platform project-relative contract.
    try:
        normalized = canonical_relative_path(value)
    except SurvivorLabError as exc:
        raise SurvivorEvidenceError(f"{field} is not a valid project-relative identity: {value!r}; error={exc}") from exc
    if not normalized:
        raise SurvivorEvidenceError(f"{field} must identify a project file: {value!r}")
    return normalized
def _nodes(evidence: AuthoritativeSurvivorEvidence, kind: EvidenceNodeType) -> tuple[KnowledgeEvidenceNode, ...]:
    # Return one node type in stable identity order.
    return tuple(sorted((node for node in evidence.graph.nodes if node.node_type is kind), key=lambda node: node.node_id))
def _one(evidence: AuthoritativeSurvivorEvidence, kind: EvidenceNodeType) -> KnowledgeEvidenceNode:
    # Require exactly one node for a singleton identity domain.
    values = _nodes(evidence, kind)
    if len(values) != 1:
        raise SurvivorEvidenceError(f"graph requires exactly one {kind.value} node; actual={len(values)}")
    return values[0]
def _test_ids(evidence: AuthoritativeSurvivorEvidence, kind: EvidenceNodeType) -> tuple[str, ...]:
    # Extract canonical test identities from graph node payloads.
    values: set[str] = set()
    for node in _nodes(evidence, kind):
        test_id = str(node.payload.get("test_id") or node.payload.get("nodeid") or "").strip()
        if not test_id:
            raise SurvivorEvidenceError(f"{kind.value} node has no test identity: node_id={node.node_id}")
        values.add(test_id)
    return tuple(sorted(values))
def _canonical_observation(value: Mapping[str, Any], *, execution_id: str) -> str:
    # Encode one observation deterministically and reject non-JSON runtime evidence.
    try:
        return json.dumps(dict(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise SurvivorEvidenceError(
            f"observation is not JSON-compatible: execution_id={execution_id}; row={dict(value)!r}; error={exc}"
        ) from exc
def _observations(evidence: AuthoritativeSurvivorEvidence) -> dict[str, Mapping[str, Any]]:
    # Normalize committed observations and reject duplicate contradictory rows.
    rows: dict[str, Mapping[str, Any]] = {}
    for observation in evidence.execution.test_observations:
        test_id = str(observation.get("test_id") or observation.get("nodeid") or "").strip()
        fingerprint = str(observation.get("test_fingerprint") or observation.get("fingerprint") or "").strip()
        outcome = str(observation.get("outcome") or observation.get("status") or "").strip()
        if not test_id or not fingerprint or not outcome:
            raise SurvivorEvidenceError(
                f"observation lacks identity or outcome: execution_id={evidence.execution.execution_id.value}; row={dict(observation)!r}"
            )
        normalized = dict(observation)
        previous = rows.get(test_id)
        if previous is not None and _canonical_observation(previous, execution_id=evidence.execution.execution_id.value) != _canonical_observation(
            normalized,
            execution_id=evidence.execution.execution_id.value,
        ):
            raise SurvivorEvidenceError(
                f"conflicting observations share test_id={test_id}; execution_id={evidence.execution.execution_id.value}"
            )
        rows[test_id] = normalized
    return rows
def _graph_context(evidence: AuthoritativeSurvivorEvidence) -> tuple[str, str, tuple[str, ...], tuple[str, ...]]:
    # Validate one complete event-scoped graph and return its revision and test partitions.
    graph = evidence.graph
    execution_id = evidence.execution.execution_id.value
    if graph.execution_id != execution_id:
        raise SurvivorEvidenceError(
            "foreign provenance graph: "
            f"execution_id={execution_id}; graph_execution={graph.execution_id}; graph={graph.graph_fingerprint}"
        )
    if not graph.complete:
        raise SurvivorEvidenceError(
            "incomplete provenance graph: "
            f"execution_id={execution_id}; missing={graph.missing_requirements}; graph={graph.graph_fingerprint}"
        )
    expected_graph_fingerprint = _graph_fingerprint(evidence)
    if graph.graph_fingerprint != expected_graph_fingerprint:
        raise SurvivorEvidenceError(
            "provenance graph fingerprint mismatch: "
            f"execution_id={execution_id}; expected={expected_graph_fingerprint}; actual={graph.graph_fingerprint}"
        )
    invalid_node_hashes = tuple(
        sorted(node.node_id for node in graph.nodes if node.payload_sha256 != _stable_hash(dict(node.payload)))
    )
    invalid_edge_hashes = tuple(
        sorted(edge.edge_id for edge in graph.edges if edge.payload_sha256 != _stable_hash(dict(edge.payload)))
    )
    if invalid_node_hashes or invalid_edge_hashes:
        raise SurvivorEvidenceError(
            "provenance payload hash mismatch: "
            f"execution_id={execution_id}; nodes={invalid_node_hashes}; edges={invalid_edge_hashes}"
        )
    required_nodes = set(_REQUIRED_NODES)
    required_relations = set(_REQUIRED_RELATIONS)
    if evidence.mutant.function_id:
        required_nodes.add(EvidenceNodeType.FUNCTION)
        required_relations.update({EvidenceRelation.REVISION_CONTAINS_FUNCTION, EvidenceRelation.FUNCTION_DEFINES_MUTANT})
    missing_nodes = tuple(sorted(item.value for item in required_nodes - {node.node_type for node in graph.nodes}))
    missing_relations = tuple(sorted(item.value for item in required_relations - {edge.relation for edge in graph.edges}))
    if missing_nodes or missing_relations:
        raise SurvivorEvidenceError(
            f"provenance graph is incomplete: execution_id={execution_id}; nodes={missing_nodes}; relations={missing_relations}"
        )
    event_ids = {node.event_id for node in graph.nodes} | {edge.event_id for edge in graph.edges}
    scope_ids = {node.scope_id for node in graph.nodes} | {edge.scope_id for edge in graph.edges}
    if len(event_ids) != 1:
        raise SurvivorEvidenceError(
            f"graph mixes source events: execution_id={execution_id}; events={tuple(sorted(event_ids))}"
        )
    if scope_ids != {graph.scope_id}:
        raise SurvivorEvidenceError(
            f"graph mixes scope identity: execution_id={execution_id}; expected_scope={graph.scope_id}; scopes={tuple(sorted(scope_ids))}"
        )
    node_ids = {node.node_id for node in graph.nodes}
    broken = tuple(sorted(edge.edge_id for edge in graph.edges if edge.source_node_id not in node_ids or edge.target_node_id not in node_ids))
    if broken:
        raise SurvivorEvidenceError(f"graph edges reference missing nodes: execution_id={execution_id}; edges={broken}")
    foreign = tuple(sorted(node.node_id for node in graph.nodes if node.execution_id not in {None, execution_id}))
    foreign_edges = tuple(sorted(edge.edge_id for edge in graph.edges if edge.execution_id not in {None, execution_id}))
    if foreign or foreign_edges:
        raise SurvivorEvidenceError(
            f"graph contains foreign execution identity: execution_id={execution_id}; nodes={foreign}; edges={foreign_edges}"
        )
    campaign_ids = {node.campaign_id for node in graph.nodes}
    if campaign_ids != {evidence.campaign_id}:
        raise SurvivorEvidenceError(
            f"graph campaign identity conflict: execution_id={execution_id}; expected={evidence.campaign_id}; actual={tuple(sorted(campaign_ids))}"
        )
    lease_ids = {node.lease_id for node in graph.nodes if node.lease_id}
    worker_ids = {node.worker_id for node in graph.nodes if node.worker_id}
    lease_node = _one(evidence, EvidenceNodeType.LEASE)
    worker_node = _one(evidence, EvidenceNodeType.WORKER)
    if lease_ids != {lease_node.identity_key} or worker_ids != {worker_node.identity_key}:
        raise SurvivorEvidenceError(
            "graph producer identity conflict: "
            f"execution_id={execution_id}; lease_node={lease_node.identity_key}; lease_ids={tuple(sorted(lease_ids))}; "
            f"worker_node={worker_node.identity_key}; worker_ids={tuple(sorted(worker_ids))}"
        )
    if _one(evidence, EvidenceNodeType.EXECUTION).identity_key != execution_id:
        raise SurvivorEvidenceError(f"execution node identity mismatch: execution_id={execution_id}")
    if _one(evidence, EvidenceNodeType.MUTANT).identity_key != evidence.mutant.mutant_id.value:
        raise SurvivorEvidenceError(f"mutant node identity mismatch: mutant_id={evidence.mutant.mutant_id.value}")
    result_status = str(_one(evidence, EvidenceNodeType.RESULT).payload.get("semantic_result") or "")
    if result_status != evidence.execution.status:
        raise SurvivorEvidenceError(
            f"result status conflict: execution_id={execution_id}; graph={result_status!r}; committed={evidence.execution.status!r}"
        )
    lease = _one(evidence, EvidenceNodeType.LEASE).identity_key
    _one(evidence, EvidenceNodeType.WORKER)
    if not evidence.execution.lease_id or lease != evidence.execution.lease_id:
        raise SurvivorEvidenceError(
            f"lease identity conflict: execution_id={execution_id}; graph={lease!r}; committed={evidence.execution.lease_id!r}"
        )
    selected = _test_ids(evidence, EvidenceNodeType.SELECTED_TEST)
    observed = _test_ids(evidence, EvidenceNodeType.OBSERVATION)
    committed = tuple(sorted(_observations(evidence)))
    if committed != observed or not set(observed).issubset(selected):
        raise SurvivorEvidenceError(
            f"test evidence conflict: execution_id={execution_id}; selected={selected}; graph_observed={observed}; committed={committed}"
        )
    return next(iter(event_ids)), _one(evidence, EvidenceNodeType.REVISION).identity_key, selected, observed
def _validate_bundle(evidence: AuthoritativeSurvivorEvidence) -> tuple[str, str, tuple[str, ...], tuple[str, ...]]:
    # Validate campaign, source, snapshot, mutant, execution, environment, graph, and artifacts together.
    campaign_id = str(evidence.campaign_id).strip()
    execution_id = evidence.execution.execution_id.value
    if not campaign_id or evidence.prepared.campaign_id.value != campaign_id:
        raise SurvivorEvidenceError(
            f"campaign identity conflict: evidence={campaign_id!r}; prepared={evidence.prepared.campaign_id.value!r}"
        )
    if evidence.execution.mutant_id != evidence.mutant.mutant_id:
        raise SurvivorEvidenceError(
            f"execution mutant mismatch: execution_id={execution_id}; execution={evidence.execution.mutant_id.value}; mutant={evidence.mutant.mutant_id.value}"
        )
    if evidence.execution.status not in _ALLOWED_STATUSES or not evidence.execution.restore_verified:
        raise SurvivorEvidenceError(
            f"execution is not an authenticated survivor result: execution_id={execution_id}; status={evidence.execution.status!r}; restore={evidence.execution.restore_verified}"
        )
    source_path = _canonical_path(evidence.source.source_path, "source.source_path")
    paths = {
        _canonical_path(evidence.prepared.source_path, "prepared.source_path"),
        _canonical_path(evidence.mutant.source_path, "mutant.source_path"),
    }
    if paths != {source_path}:
        raise SurvivorEvidenceError(f"source path conflict: source={source_path}; prepared/mutant={tuple(sorted(paths))}")
    raw_source_hash = sha256_text(evidence.source.source_text)
    semantic_source_hash = semantic_text_sha256(evidence.source.source_text)
    accepted_source_hashes = {raw_source_hash, semantic_source_hash}
    if evidence.source.source_sha256.lower() not in accepted_source_hashes or evidence.prepared.source_sha256.lower() not in accepted_source_hashes:
        raise SurvivorEvidenceError(
            "source hash mismatch: "
            f"execution_id={execution_id}; source={evidence.source.source_sha256}; prepared={evidence.prepared.source_sha256}; "
            f"raw={raw_source_hash}; semantic={semantic_source_hash}"
        )
    matches = tuple(item for item in evidence.prepared.mutants if item.mutant_id == evidence.mutant.mutant_id)
    if matches != (evidence.mutant,):
        raise SurvivorEvidenceError(
            f"prepared snapshot lacks exact mutant descriptor: execution_id={execution_id}; matches={len(matches)}"
        )
    event_id, revision, selected, observed = _graph_context(evidence)
    if evidence.project.revision is not None and revision != evidence.project.revision.revision_id.value:
        raise SurvivorEvidenceError(
            f"revision identity conflict: project={evidence.project.revision.revision_id.value}; graph={revision}"
        )
    environment = _one(evidence, EvidenceNodeType.ENVIRONMENT).identity_key
    if evidence.project.environment is not None and environment != evidence.project.environment.fingerprint:
        raise SurvivorEvidenceError(
            f"environment identity conflict: project={evidence.project.environment.fingerprint}; graph={environment}"
        )
    keys: set[str] = set()
    for artifact in evidence.artifacts:
        if artifact.logical_key in keys or artifact.campaign_id.value != campaign_id or artifact.execution_id not in {None, execution_id}:
            raise SurvivorEvidenceError(
                f"artifact identity conflict: key={artifact.logical_key}; campaign={artifact.campaign_id.value}; execution={artifact.execution_id}"
            )
        keys.add(artifact.logical_key)
    return event_id, revision, selected, observed
def _request_id(evidence: AuthoritativeSurvivorEvidence, event_id: str) -> str:
    # Derive a relocation-safe request identity from committed semantic evidence.
    payload = {
        "campaign_id": evidence.campaign_id,
        "project_id": evidence.project.project_id.value,
        "execution_id": evidence.execution.execution_id.value,
        "mutant_id": evidence.mutant.mutant_id.value,
        "event_id": event_id,
        "graph": evidence.graph.graph_fingerprint,
        "source": semantic_text_sha256(evidence.source.source_text),
    }
    return "runtime-" + _hash_bytes(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"))[:24]
def _build_request(evidence: AuthoritativeSurvivorEvidence) -> tuple[SurvivorAnalysisRequest, str]:
    # Build the standalone request while forcing PR38 to exclude proposal and validation stages.
    event_id, revision, selected, observed = _validate_bundle(evidence)
    status = evidence.execution.status
    environment = evidence.environment
    if environment is None and evidence.project.environment is not None:
        descriptor = evidence.project.environment
        environment = EnvironmentEvidence(None, descriptor.python_version, tuple(sorted(descriptor.plugins)), (), None)
    selection = replace(
        evidence.selection,
        selected_tests=selected,
        related_test_nodeids=tuple(sorted(set((*evidence.selection.related_test_nodeids, *selected, *observed)))),
    )
    request = SurvivorAnalysisRequest(
        schema_version=1,
        request_id=_request_id(evidence, event_id),
        project_id=evidence.project.project_id.value,
        revision=revision,
        mutant=MutantEvidence(
            evidence.mutant.mutant_id.value,
            evidence.mutant.mutation,
            evidence.mutant.operator_version,
            _canonical_path(evidence.mutant.source_path, "mutant.source_path"),
            evidence.mutant.function_id,
            evidence.mutant.class_name,
            evidence.mutant.line_no,
            evidence.mutant.column_no,
            evidence.mutant.original,
            evidence.mutant.replacement,
            f"- {evidence.mutant.original}\n+ {evidence.mutant.replacement}",
            MutantStatus(status),
        ),
        source=replace(evidence.source, source_path=_canonical_path(evidence.source.source_path, "source.source_path")),
        selection=selection,
        executions=(ExecutionEvidence(
            evidence.execution.execution_id.value,
            evidence.execution_level,
            ExecutionStatus(status),
            0 if status == "survived" and evidence.exit_code is None else evidence.exit_code,
            bool(evidence.timed_out or status == "timeout"),
            bool(evidence.infrastructure_failure or status == "infrastructure_error"),
            True,
            selected,
            observed,
            None,
        ),),
        related_tests=tuple(sorted(evidence.related_tests, key=lambda item: item.nodeid)),
        related_dependencies=tuple(sorted(
            evidence.related_dependencies,
            key=lambda item: (item.name, item.kind, item.relationship, item.source_path or "", item.version or ""),
        )),
        environment=environment,
        requested_modes=(RequestedMode.CLASSIFY, RequestedMode.CONTEXT, RequestedMode.HYPOTHESES),
    )
    return normalize_request(request), event_id
class SurvivorRuntimeAdapter:
    """Invoke the standalone offline service without provider or filesystem side effects."""
    def __init__(self) -> None:
        # Pin PR38 to the explicit no-provider implementation.
        self._service = SurvivorAnalysisService(NullRepairProposalProvider())
    def build_request(self, evidence: AuthoritativeSurvivorEvidence) -> SurvivorAnalysisRequest:
        # Expose deterministic request construction for audit and integration tests.
        return _build_request(evidence)[0]
    def analyze(self, evidence: AuthoritativeSurvivorEvidence) -> SurvivorAdapterResult:
        # Run offline analysis and return content-addressed bytes without publishing them.
        request, event_id = _build_request(evidence)
        try:
            analysis = self._service.analyze(request)
        except SurvivorLabError as exc:
            raise SurvivorAdapterError(
                "Survivor Lab rejected the authoritative runtime request: "
                f"campaign_id={evidence.campaign_id}; execution_id={evidence.execution.execution_id.value}; error={exc}"
            ) from exc
        plan = analysis.validation_plan
        if analysis.proposals or analysis.provider.invoked or any((plan.original_checks, plan.mutant_checks, plan.regression_checks, plan.stability_checks)):
            raise SurvivorEvidenceError(
                f"PR38 crossed the proposal/provider boundary: result_id={analysis.result_id}; proposals={len(analysis.proposals)}; provider={analysis.provider}"
            )
        content = (canonical_json(result_to_dict(analysis)) + "\n").encode("utf-8")
        artifact = SurvivorAnalysisArtifact(
            logical_key=f"survivor-analysis/{evidence.execution.execution_id.value}.json",
            logical_role="survivor_analysis_result",
            content_sha256=_hash_bytes(content),
            size_bytes=len(content),
            schema_version=analysis.schema_version,
            producer="theseus_survivor_adapter.pr38",
            content=content,
            metadata={
                "campaign_id": evidence.campaign_id,
                "project_id": evidence.project.project_id.value,
                "mutant_id": evidence.mutant.mutant_id.value,
                "execution_id": evidence.execution.execution_id.value,
                "source_event_id": event_id,
                "graph_fingerprint": evidence.graph.graph_fingerprint,
                "analysis_id": analysis.analysis_id,
                "proposal_set_id": analysis.proposal_set_id,
                "result_id": analysis.result_id,
                "revision_id": request.revision or "",
                "lease_id": evidence.execution.lease_id or "",
                "worker_id": _one(evidence, EvidenceNodeType.WORKER).identity_key,
            },
        )
        return SurvivorAdapterResult(request, analysis, artifact, event_id, evidence.graph.graph_fingerprint)
__all__ = ["SurvivorRuntimeAdapter"]
