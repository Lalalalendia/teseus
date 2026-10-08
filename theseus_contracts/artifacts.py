"""Evidence artifact references without filesystem ownership in the contract layer."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Mapping
from .enums import ArtifactRetentionClass
from .ids import ArtifactId, CampaignId
from .serialization import (
    WireModel,
    optional_string,
    required_string,
    sequence_of_strings,
    validate_utc_timestamp,
)
@dataclass(frozen=True, slots=True)
class ArtifactRef(WireModel):
    """Portable reference to one report, output, recovery or trace artifact."""
    artifact_id: ArtifactId
    kind: str
    path: str
    sha256: str | None = None
    size_bytes: int | None = None
    retention: ArtifactRetentionClass | str = ArtifactRetentionClass.CAMPAIGN
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ArtifactRef":
        # Restore evidence metadata without opening or validating the referenced path.
        retention_value = str(value.get("retention", ArtifactRetentionClass.CAMPAIGN.value))
        try:
            retention: ArtifactRetentionClass | str = ArtifactRetentionClass(retention_value)
        except ValueError:
            retention = retention_value
        size = value.get("size_bytes")
        return cls(
            artifact_id=ArtifactId(required_string(value, "artifact_id")),
            kind=required_string(value, "kind"),
            path=required_string(value, "path"),
            sha256=optional_string(value, "sha256"),
            size_bytes=int(size) if size is not None else None,
            retention=retention,
        )
@dataclass(frozen=True, slots=True)
class ArtifactDescriptor(WireModel):
    """Artifact plus campaign ownership and bounded metadata."""
    campaign_id: CampaignId
    reference: ArtifactRef
    created_at: str
    metadata: Mapping[str, Any] = field(default_factory=dict)
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ArtifactDescriptor":
        # Restore the artifact projection while keeping metadata JSON-only.
        reference = value.get("reference")
        if not isinstance(reference, Mapping):
            raise ValueError("reference must be an object")
        metadata = value.get("metadata", {})
        if not isinstance(metadata, Mapping):
            raise ValueError("metadata must be an object")
        return cls(
            campaign_id=CampaignId(required_string(value, "campaign_id")),
            reference=ArtifactRef.from_dict(reference),
            created_at=required_string(value, "created_at"),
            metadata=dict(metadata),
        )
@dataclass(frozen=True, slots=True)
class ArtifactRegistryEntry(WireModel):
    """Immutable content-addressed artifact registered before campaign completion."""
    campaign_id: CampaignId
    logical_key: str
    logical_role: str
    content_sha256: str
    size_bytes: int
    schema_version: int
    producer: str
    content_path: str
    logical_path: str
    created_at: str
    shard_id: str | None = None
    execution_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    def __post_init__(self) -> None:
        # Reject incomplete or non-content-addressed registry rows before persistence.
        if not all((self.logical_key, self.logical_role, self.producer, self.content_path, self.logical_path)):
            raise ValueError("artifact registry identity fields must be non-empty")
        if (
            self.content_sha256 != self.content_sha256.lower()
            or len(self.content_sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.content_sha256)
        ):
            raise ValueError("content_sha256 must be a lowercase SHA-256 digest")
        if int(self.size_bytes) < 0:
            raise ValueError("artifact size_bytes must not be negative")
        if int(self.schema_version) < 1:
            raise ValueError("artifact schema_version must be positive")
        validate_utc_timestamp(self.created_at, field_name="created_at")
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ArtifactRegistryEntry":
        # Restore one immutable registry row without opening its content path.
        metadata = value.get("metadata", {})
        if not isinstance(metadata, Mapping):
            raise ValueError("artifact metadata must be an object")
        return cls(
            campaign_id=CampaignId(required_string(value, "campaign_id")),
            logical_key=required_string(value, "logical_key"),
            logical_role=required_string(value, "logical_role"),
            content_sha256=required_string(value, "content_sha256"),
            size_bytes=int(value.get("size_bytes", -1)),
            schema_version=int(value.get("schema_version", 0)),
            producer=required_string(value, "producer"),
            content_path=required_string(value, "content_path"),
            logical_path=required_string(value, "logical_path"),
            created_at=validate_utc_timestamp(required_string(value, "created_at"), field_name="created_at"),
            shard_id=optional_string(value, "shard_id"),
            execution_id=optional_string(value, "execution_id"),
            metadata=dict(metadata),
        )
@dataclass(frozen=True, slots=True)
class FinalizationIntent(WireModel):
    """Durable expected artifact set and replay state for one campaign finalization."""
    intent_id: str
    campaign_id: CampaignId
    status: str
    required_logical_keys: tuple[str, ...]
    canonical_report_key: str
    artifacts: tuple[ArtifactRegistryEntry, ...]
    result_fingerprint: str
    created_at: str
    updated_at: str
    schema_version: int = 1
    def __post_init__(self) -> None:
        # Validate immutable intent identity and exact expected artifact key coverage.
        if not all((self.intent_id, self.status, self.canonical_report_key, self.result_fingerprint)):
            raise ValueError("finalization intent identity fields must be non-empty")
        if self.status not in {"created", "registered", "completed"}:
            raise ValueError(f"unsupported finalization intent status: {self.status}")
        if int(self.schema_version) < 1:
            raise ValueError("finalization intent schema_version must be positive")
        validate_utc_timestamp(self.created_at, field_name="created_at")
        validate_utc_timestamp(self.updated_at, field_name="updated_at")
        artifact_keys = tuple(item.logical_key for item in self.artifacts)
        if len(set(artifact_keys)) != len(artifact_keys):
            raise ValueError("finalization intent artifact logical keys must be unique")
        if len(set(self.required_logical_keys)) != len(self.required_logical_keys):
            raise ValueError("finalization intent required logical keys must be unique")
        if any(item.campaign_id != self.campaign_id for item in self.artifacts):
            raise ValueError("finalization intent artifacts must belong to the same campaign")
        if self.canonical_report_key not in self.required_logical_keys:
            raise ValueError("canonical report key must be required")
        if not set(self.required_logical_keys).issubset(set(artifact_keys)):
            raise ValueError("finalization intent required artifacts are missing from the expected set")
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "FinalizationIntent":
        # Restore the complete expected artifact set used for idempotent finalization replay.
        raw_artifacts = value.get("artifacts", [])
        if not isinstance(raw_artifacts, (list, tuple)) or any(not isinstance(item, Mapping) for item in raw_artifacts):
            raise ValueError("finalization intent artifacts must be an array of objects")
        return cls(
            intent_id=required_string(value, "intent_id"),
            campaign_id=CampaignId(required_string(value, "campaign_id")),
            status=required_string(value, "status"),
            required_logical_keys=sequence_of_strings(value, "required_logical_keys"),
            canonical_report_key=required_string(value, "canonical_report_key"),
            artifacts=tuple(ArtifactRegistryEntry.from_dict(item) for item in raw_artifacts),
            result_fingerprint=required_string(value, "result_fingerprint"),
            created_at=validate_utc_timestamp(required_string(value, "created_at"), field_name="created_at"),
            updated_at=validate_utc_timestamp(required_string(value, "updated_at"), field_name="updated_at"),
            schema_version=int(value.get("schema_version", 1)),
        )