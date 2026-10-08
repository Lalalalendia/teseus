"""Canonical schema and identity contracts for the Theseus Knowledge Plane."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Mapping, Sequence
from test_intelligence_unified_v1.io_utils import stable_hash
KNOWLEDGE_SCHEMA_VERSION = 4
KNOWLEDGE_SCHEMA_MIGRATIONS = (
    (1, "canonical-scope-columns"),
    (2, "append-only-identity-bindings"),
    (3, "provenance-evidence-graph"),
    (4, "conflict-quarantine-and-crash-safe-compaction"),
)
def _non_empty(value: Any) -> str | None:
    # Normalize one optional identity component without inventing whitespace-only keys.
    text = str(value).strip() if value is not None else ""
    return text or None
def _mapping(value: Any) -> Mapping[str, Any]:
    # Return a mapping view for defensive nested identity extraction.
    return value if isinstance(value, Mapping) else {}
def _configuration_environment_id(configuration: Mapping[str, Any]) -> str | None:
    # Derive a stable configured-environment identity available before execution enrichment.
    project = _mapping(configuration.get("project"))
    if not project:
        return None
    identity_payload = {
        "environment": project.get("environment"),
        "test_command": project.get("test_command"),
        "pytest_plugin_autoload": project.get("pytest_plugin_autoload"),
        "configuration_fingerprint": configuration.get("configuration_fingerprint"),
    }
    return f"configured-environment:{stable_hash(identity_payload)}"
def _execution_environment_ids(payload: Mapping[str, Any]) -> tuple[str, ...]:
    # Collect the distinct execution environment fingerprints carried by one committed effect.
    raw = payload.get("executions", ())
    if not isinstance(raw, (list, tuple)):
        return ()
    values = {
        str(item.get("environment_id") or item.get("environment_fingerprint") or "").strip()
        for item in raw
        if isinstance(item, Mapping)
    }
    return tuple(sorted(item for item in values if item))
@dataclass(frozen=True, slots=True)
class KnowledgeScopeIdentity:
    """Canonical project, revision, and environment identity for historical facts."""
    project_id: str
    revision_id: str
    environment_id: str
    identity_source: str = "explicit"
    def __post_init__(self) -> None:
        # Reject partial scope identities because they make historical facts ambiguous.
        for field_name in ("project_id", "revision_id", "environment_id", "identity_source"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
    @property
    def scope_id(self) -> str:
        # Derive a relocation-safe scope key from only canonical identity components.
        return stable_hash(
            {
                "project_id": self.project_id,
                "revision_id": self.revision_id,
                "environment_id": self.environment_id,
            }
        )[:32]
    def to_dict(self) -> dict[str, str]:
        # Serialize the canonical scope for diagnostics and migration reports.
        return {
            "scope_id": self.scope_id,
            "project_id": self.project_id,
            "revision_id": self.revision_id,
            "environment_id": self.environment_id,
            "identity_source": self.identity_source,
        }
    @classmethod
    def resolve(
        cls,
        *,
        campaign_id: str,
        payload: Mapping[str, Any],
        project_id: str | None = None,
        revision_id: str | None = None,
        environment_id: str | None = None,
    ) -> "KnowledgeScopeIdentity":
        # Resolve explicit or payload-bound scope identity with deterministic legacy fallbacks.
        campaign = _mapping(payload.get("campaign"))
        configuration = _mapping(campaign.get("configuration"))
        project = _mapping(configuration.get("project"))
        revision = _mapping(project.get("revision"))
        explicit_project = _non_empty(project_id)
        explicit_revision = _non_empty(revision_id)
        explicit_environment = _non_empty(environment_id)
        resolved_project = (
            explicit_project
            or _non_empty(payload.get("project_id"))
            or _non_empty(campaign.get("project_id"))
            or _non_empty(project.get("project_id"))
        )
        resolved_revision = (
            explicit_revision
            or _non_empty(payload.get("revision_id"))
            or _non_empty(campaign.get("revision_id"))
            or _non_empty(revision.get("revision_id"))
        )
        environments = _execution_environment_ids(payload)
        configured_environment = _configuration_environment_id(configuration)
        resolved_environment = (
            explicit_environment
            or _non_empty(payload.get("environment_id"))
            or configured_environment
            or (environments[0] if len(environments) == 1 else None)
            or _non_empty(configuration.get("configuration_fingerprint"))
        )
        source = "explicit" if explicit_project and explicit_revision and explicit_environment else "payload"
        if not resolved_project:
            resolved_project = f"legacy-project:{campaign_id}"
            source = "legacy"
        if not resolved_revision:
            resolved_revision = f"legacy-revision:{campaign_id}"
            source = "legacy"
        if not resolved_environment:
            resolved_environment = "legacy-environment:unknown"
            source = "legacy"
        return cls(resolved_project, resolved_revision, resolved_environment, source)
@dataclass(frozen=True, slots=True)
class KnowledgeSchemaState:
    """Read-only schema diagnostics returned after migration and integrity checks."""
    version: int
    supported_version: int
    migrations: tuple[tuple[int, str, str], ...]
    scopes: int
    identity_bindings: int
    evidence_nodes: int = 0
    evidence_edges: int = 0
    quarantined_conflicts: int = 0
    tombstones: int = 0
    compactions: int = 0
    live_evidence_references: int = 0
    def to_dict(self) -> dict[str, Any]:
        # Convert schema diagnostics into a stable JSON-compatible projection.
        return {
            "version": self.version,
            "supported_version": self.supported_version,
            "migrations": [
                {"version": version, "name": name, "checksum": checksum}
                for version, name, checksum in self.migrations
            ],
            "scopes": self.scopes,
            "identity_bindings": self.identity_bindings,
            "evidence_nodes": self.evidence_nodes,
            "evidence_edges": self.evidence_edges,
            "quarantined_conflicts": self.quarantined_conflicts,
            "tombstones": self.tombstones,
            "compactions": self.compactions,
            "live_evidence_references": self.live_evidence_references,
        }
def migration_checksum(version: int, name: str, statements: Sequence[str]) -> str:
    # Fingerprint migration intent so a reused version cannot silently change meaning.
    return stable_hash({"version": int(version), "name": str(name), "statements": tuple(statements)})
__all__ = [
    "KNOWLEDGE_SCHEMA_MIGRATIONS",
    "KNOWLEDGE_SCHEMA_VERSION",
    "KnowledgeSchemaState",
    "KnowledgeScopeIdentity",
    "migration_checksum",
]
