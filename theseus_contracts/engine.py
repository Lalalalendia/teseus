"""Public engine lifecycle DTOs used by the first Theseus facade."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Mapping
from .campaign import CampaignConfiguration, CampaignSummary
from .ids import CampaignId, WorkerId
from .mutation import MutantDescriptor, PreparedMutant
from .selection import SelectionSnapshot
from .workers import ShardDescriptor
from .serialization import WireModel, required_string
from .serialization import optional_string
@dataclass(frozen=True, slots=True)
class PrepareCampaignRequest(WireModel):
    """Request to build immutable index, selection and mutant inputs."""
    configuration: CampaignConfiguration
    request_id: str | None = None
@dataclass(frozen=True, slots=True)
class PreparedCampaign(WireModel):
    """Read-only preparation result handed to planners and workers."""
    campaign_id: CampaignId
    source_path: str
    source_sha256: str
    index_version: str
    mutants: tuple[MutantDescriptor, ...] = ()
    selection: SelectionSnapshot | None = None
    snapshot_path: str | None = None
    snapshot_id: str | None = None
    function_id: str | None = None
    class_name: str | None = None
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "PreparedCampaign":
        # Restore the preparation snapshot without importing engine internals.
        raw_mutants = value.get("mutants", [])
        selection = value.get("selection")
        if not isinstance(raw_mutants, (list, tuple)) or any(not isinstance(item, Mapping) for item in raw_mutants):
            raise ValueError("mutants must be an array of objects")
        return cls(
            campaign_id=CampaignId(required_string(value, "campaign_id")),
            source_path=required_string(value, "source_path"),
            source_sha256=required_string(value, "source_sha256"),
            index_version=required_string(value, "index_version"),
            mutants=tuple(MutantDescriptor.from_dict(item) for item in raw_mutants),
            selection=SelectionSnapshot.from_dict(selection) if isinstance(selection, Mapping) else None,
            snapshot_path=optional_string(value, "snapshot_path"),
            snapshot_id=optional_string(value, "snapshot_id"),
            function_id=optional_string(value, "function_id"),
            class_name=optional_string(value, "class_name"),
        )
@dataclass(frozen=True, slots=True)
class PreparedCampaignSnapshot(WireModel):
    """Durable engine context needed to recreate a campaign after process restart."""
    campaign_id: CampaignId
    source_path: str
    source_sha256: str
    index_version: str
    index_payload: Mapping[str, Any]
    mutants: tuple[MutantDescriptor, ...] = ()
    internal_mutants: tuple[Mapping[str, Any], ...] = ()
    prepared_mutants: tuple[PreparedMutant, ...] = ()
    selection: SelectionSnapshot | None = None
    function_id: str | None = None
    function_range: tuple[int, int] | None = None
    function_info: Mapping[str, Any] | None = None
    repository_fingerprint: str = ""
    environment_fingerprint: str = ""
    configuration_fingerprint: str = ""
    created_at: str = ""
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "PreparedCampaignSnapshot":
        # Restore immutable preparation inputs without rebuilding AST or selection state.
        raw_index = value.get("index_payload", {})
        raw_mutants = value.get("mutants", [])
        raw_internal_mutants = value.get("internal_mutants", [])
        raw_prepared_mutants = value.get("prepared_mutants", [])
        raw_range = value.get("function_range")
        if (
            not isinstance(raw_index, Mapping)
            or not isinstance(raw_mutants, (list, tuple))
            or not isinstance(raw_internal_mutants, (list, tuple))
            or not isinstance(raw_prepared_mutants, (list, tuple))
        ):
            raise ValueError("index_payload, mutants, internal_mutants and prepared_mutants must be JSON arrays/objects")
        function_range = None
        if isinstance(raw_range, (list, tuple)) and len(raw_range) == 2:
            function_range = (int(raw_range[0]), int(raw_range[1]))
        selection = value.get("selection")
        raw_function_info = value.get("function_info")
        return cls(
            campaign_id=CampaignId(required_string(value, "campaign_id")),
            source_path=required_string(value, "source_path"),
            source_sha256=required_string(value, "source_sha256"),
            index_version=required_string(value, "index_version"),
            index_payload=dict(raw_index),
            mutants=tuple(MutantDescriptor.from_dict(item) for item in raw_mutants if isinstance(item, Mapping)),
            internal_mutants=tuple(dict(item) for item in raw_internal_mutants if isinstance(item, Mapping)),
            prepared_mutants=tuple(
                PreparedMutant.from_dict(item)
                for item in raw_prepared_mutants
                if isinstance(item, Mapping)
            ),
            selection=SelectionSnapshot.from_dict(selection) if isinstance(selection, Mapping) else None,
            function_id=str(value["function_id"]) if value.get("function_id") is not None else None,
            function_range=function_range,
            function_info=dict(raw_function_info) if isinstance(raw_function_info, Mapping) else None,
            repository_fingerprint=str(value.get("repository_fingerprint", "")),
            environment_fingerprint=str(value.get("environment_fingerprint", "")),
            configuration_fingerprint=str(value.get("configuration_fingerprint", "")),
            created_at=str(value.get("created_at", "")),
        )
@dataclass(frozen=True, slots=True)
class DiscoverMutantsRequest(WireModel):
    """Request to expose the deterministic mutant catalog from preparation."""
    campaign_id: CampaignId
@dataclass(frozen=True, slots=True)
class MutationDiscoveryResult(WireModel):
    """Stable mutant catalog projection."""
    campaign_id: CampaignId
    mutants: tuple[MutantDescriptor, ...] = ()
    total_mutants: int = 0
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MutationDiscoveryResult":
        # Restore the deterministic mutant catalog returned by a staged engine process.
        raw_mutants = value.get("mutants", [])
        if not isinstance(raw_mutants, (list, tuple)) or any(not isinstance(item, Mapping) for item in raw_mutants):
            raise ValueError("mutants must be an array of objects")
        mutants = tuple(MutantDescriptor.from_dict(item) for item in raw_mutants)
        return cls(
            campaign_id=CampaignId(required_string(value, "campaign_id")),
            mutants=mutants,
            total_mutants=max(0, int(value.get("total_mutants", len(mutants)))),
        )
@dataclass(frozen=True, slots=True)
class ExecuteShardRequest(WireModel):
    """Request to execute one prepared shard through the current backend."""
    campaign_id: CampaignId
    shard: ShardDescriptor
    attempt: int = 0
    worker_id: WorkerId | None = None
    lease_id: str | None = None
    test_overrides: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    worker_instance_id: str | None = None
    worker_process_id: int | None = None
    worker_process_birth_token: str | None = None
    mutant_spool_root: str | None = None
    expected_source_sha256: str | None = None
    test_fingerprints: Mapping[str, Mapping[str, str]] = field(default_factory=dict)
    prepared_snapshot_id: str | None = None
@dataclass(frozen=True, slots=True)
class FinalizeCampaignRequest(WireModel):
    """Request to materialize a campaign result after shard execution."""
    campaign_id: CampaignId
    status_override: str | None = None
@dataclass(frozen=True, slots=True)
class CampaignResult(WireModel):
    """Public result wrapper containing a summary and optional engine report."""
    summary: CampaignSummary
    report: Mapping[str, Any] = field(default_factory=dict)
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "CampaignResult":
        # Restore summary and keep report extensions as JSON data for future consumers.
        summary = value.get("summary")
        report = value.get("report", {})
        if not isinstance(summary, Mapping) or not isinstance(report, Mapping):
            raise ValueError("summary and report must be objects")
        return cls(summary=CampaignSummary.from_dict(summary), report=dict(report))
