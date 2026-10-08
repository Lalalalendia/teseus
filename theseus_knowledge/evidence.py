"""Typed provenance nodes and edges for the canonical Knowledge Plane graph."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping

from .schema import KNOWLEDGE_SCHEMA_VERSION


class EvidenceNodeType(str, Enum):
    """Canonical entity and evidence node domains stored in the graph."""

    REVISION = "revision"
    ENVIRONMENT = "environment"
    FUNCTION = "function"
    MUTANT = "mutant"
    EXECUTION = "execution"
    WORKER = "worker"
    LEASE = "lease"
    SELECTED_TEST = "selected_test"
    OBSERVATION = "observation"
    RESULT = "result"
    ARTIFACT = "artifact"


class EvidenceRelation(str, Enum):
    """Directed relations that form one execution provenance chain."""

    REVISION_CONTAINS_FUNCTION = "revision_contains_function"
    ENVIRONMENT_GOVERNS_EXECUTION = "environment_governs_execution"
    FUNCTION_DEFINES_MUTANT = "function_defines_mutant"
    MUTANT_EXECUTED_AS = "mutant_executed_as"
    WORKER_PERFORMED_EXECUTION = "worker_performed_execution"
    LEASE_AUTHORIZED_EXECUTION = "lease_authorized_execution"
    EXECUTION_SELECTED_TEST = "execution_selected_test"
    MUTANT_SELECTED_TEST = "mutant_selected_test"
    SELECTED_TEST_PRODUCED_OBSERVATION = "selected_test_produced_observation"
    OBSERVATION_SUPPORTS_RESULT = "observation_supports_result"
    EXECUTION_PRODUCED_RESULT = "execution_produced_result"
    RESULT_PRODUCED_ARTIFACT = "result_produced_artifact"


@dataclass(frozen=True, slots=True)
class KnowledgeEvidenceNode:
    """One immutable provenance entity or observation in the Knowledge Plane graph."""

    node_id: str
    scope_id: str
    node_type: EvidenceNodeType
    identity_key: str
    fingerprint: str
    campaign_id: str
    event_id: str
    effect_id: str | None
    execution_id: str | None
    lease_id: str | None
    worker_id: str | None
    producer_type: str
    producer_id: str
    producer_version: str
    observed_at: str
    payload_sha256: str
    payload: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        # Serialize one node without exposing SQLite row or mapping implementations.
        return {
            "node_id": self.node_id,
            "scope_id": self.scope_id,
            "node_type": self.node_type.value,
            "identity_key": self.identity_key,
            "fingerprint": self.fingerprint,
            "campaign_id": self.campaign_id,
            "event_id": self.event_id,
            "effect_id": self.effect_id,
            "execution_id": self.execution_id,
            "lease_id": self.lease_id,
            "worker_id": self.worker_id,
            "producer_type": self.producer_type,
            "producer_id": self.producer_id,
            "producer_version": self.producer_version,
            "observed_at": self.observed_at,
            "payload_sha256": self.payload_sha256,
            "payload": dict(self.payload),
        }


@dataclass(frozen=True, slots=True)
class KnowledgeEvidenceEdge:
    """One immutable directed relation between two provenance nodes."""

    edge_id: str
    scope_id: str
    source_node_id: str
    relation: EvidenceRelation
    target_node_id: str
    event_id: str
    execution_id: str | None
    created_at: str
    payload_sha256: str
    payload: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        # Serialize one edge as deterministic JSON-compatible data.
        return {
            "edge_id": self.edge_id,
            "scope_id": self.scope_id,
            "source_node_id": self.source_node_id,
            "relation": self.relation.value,
            "target_node_id": self.target_node_id,
            "event_id": self.event_id,
            "execution_id": self.execution_id,
            "created_at": self.created_at,
            "payload_sha256": self.payload_sha256,
            "payload": dict(self.payload),
        }


@dataclass(frozen=True, slots=True)
class KnowledgeEvidenceGraph:
    """Bounded execution graph with a deterministic integrity fingerprint."""

    scope_id: str
    execution_id: str
    nodes: tuple[KnowledgeEvidenceNode, ...]
    edges: tuple[KnowledgeEvidenceEdge, ...]
    graph_fingerprint: str
    complete: bool
    missing_requirements: tuple[str, ...]
    snapshot_revision: int
    schema_version: int = KNOWLEDGE_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        # Publish graph evidence, completeness diagnostics, and revision fencing together.
        return {
            "schema_version": self.schema_version,
            "scope_id": self.scope_id,
            "execution_id": self.execution_id,
            "graph_fingerprint": self.graph_fingerprint,
            "complete": self.complete,
            "missing_requirements": list(self.missing_requirements),
            "snapshot_revision": self.snapshot_revision,
            "nodes": [item.to_dict() for item in self.nodes],
            "edges": [item.to_dict() for item in self.edges],
        }


__all__ = [
    "EvidenceNodeType",
    "EvidenceRelation",
    "KnowledgeEvidenceEdge",
    "KnowledgeEvidenceGraph",
    "KnowledgeEvidenceNode",
]
