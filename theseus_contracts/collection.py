"""Authoritative pytest collection contract."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .serialization import SerializationError, WireModel, optional_string, required_string, sequence_of_strings


@dataclass(frozen=True, slots=True)
class CollectionSnapshot(WireModel):
    """Immutable nodeid inventory for one repository and environment fingerprint."""

    collection_snapshot_id: str
    repository_revision: str | None
    environment_fingerprint: str
    pytest_version: str | None
    plugin_fingerprint: str
    pytest_configuration_fingerprint: str
    nodeids: tuple[str, ...]
    collection_errors: tuple[str, ...] = ()
    created_at: str = ""
    collection_mode: str = "pytest"

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "CollectionSnapshot":
        # Restore the authoritative collection list and retain collection errors separately.
        raw_mode = value.get("collection_mode", "pytest")
        if not isinstance(raw_mode, str) or raw_mode not in {"pytest", "static_index"}:
            raise SerializationError("collection_mode must be pytest or static_index")
        return cls(
            collection_snapshot_id=required_string(value, "collection_snapshot_id"),
            repository_revision=optional_string(value, "repository_revision"),
            environment_fingerprint=required_string(value, "environment_fingerprint"),
            pytest_version=optional_string(value, "pytest_version"),
            plugin_fingerprint=required_string(value, "plugin_fingerprint"),
            pytest_configuration_fingerprint=required_string(value, "pytest_configuration_fingerprint"),
            nodeids=sequence_of_strings(value, "nodeids"),
            collection_errors=sequence_of_strings(value, "collection_errors"),
            created_at=str(value.get("created_at", "")),
            collection_mode=raw_mode,
        )
