"""Mutation discovery and execution DTOs."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Mapping
from .ids import CampaignId, ExecutionId, MutantId
from .serialization import WireModel, optional_string, required_string, sequence_of_strings
@dataclass(frozen=True, slots=True)
class MutantDescriptor(WireModel):
    """One first-order mutation independent of AST implementation details."""
    mutant_id: MutantId
    mutation: str
    source_path: str
    line_no: int
    column_no: int
    original: str
    replacement: str
    operator_version: str = "m2"
    function_id: str | None = None
    class_name: str | None = None
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MutantDescriptor":
        # Restore a deterministic mutant descriptor without importing mutation operators.
        return cls(
            mutant_id=MutantId(required_string(value, "mutant_id")),
            mutation=required_string(value, "mutation"),
            source_path=required_string(value, "source_path"),
            line_no=int(value.get("line_no", 0)),
            column_no=int(value.get("column_no", 0)),
            original=str(value.get("original", "")),
            replacement=str(value.get("replacement", "")),
            operator_version=str(value.get("operator_version", "m2")),
            function_id=optional_string(value, "function_id"),
            class_name=optional_string(value, "class_name"),
        )
@dataclass(frozen=True, slots=True)
class PreparedMutant(WireModel):
    """Prepared source image and hashes armed before an install."""
    mutant: MutantDescriptor
    source_sha256_before: str
    rendered_sha256: str
    compiled: bool
    source_b64: str = ""
    recovery_manifest: str | None = None
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "PreparedMutant":
        # Restore preparation evidence while keeping recovery storage opaque to consumers.
        descriptor = value.get("mutant")
        if not isinstance(descriptor, Mapping):
            raise ValueError("mutant must be an object")
        return cls(
            mutant=MutantDescriptor.from_dict(descriptor),
            source_sha256_before=required_string(value, "source_sha256_before"),
            rendered_sha256=required_string(value, "rendered_sha256"),
            compiled=bool(value.get("compiled", False)),
            source_b64=str(value.get("source_b64", "")),
            recovery_manifest=optional_string(value, "recovery_manifest"),
        )
@dataclass(frozen=True, slots=True)
class MutantExecutionRequest(WireModel):
    """Instruction to execute one mutant through the configured level ladder."""
    campaign_id: CampaignId
    execution_id: ExecutionId
    mutant: MutantDescriptor
    level_names: tuple[str, ...] = ()
    attempt: int = 0
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MutantExecutionRequest":
        # Restore one execution request with immutable level and retry inputs.
        descriptor = value.get("mutant")
        if not isinstance(descriptor, Mapping):
            raise ValueError("mutant must be an object")
        return cls(
            campaign_id=CampaignId(required_string(value, "campaign_id")),
            execution_id=ExecutionId(required_string(value, "execution_id")),
            mutant=MutantDescriptor.from_dict(descriptor),
            level_names=sequence_of_strings(value, "level_names"),
            attempt=max(0, int(value.get("attempt", 0))),
        )
@dataclass(frozen=True, slots=True)
class MutantExecutionResult(WireModel):
    """Portable result row with classification and restoration evidence."""
    execution_id: ExecutionId
    mutant_id: MutantId
    status: str
    classification_reason: str
    restore_verified: bool
    level_results: tuple[Mapping[str, Any], ...] = ()
    artifact_paths: tuple[str, ...] = ()
    error: str | None = None
    lease_id: str | None = None
    attempt: int = 0
    test_observations: tuple[Mapping[str, Any], ...] = ()
    duration_seconds: float | None = None
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MutantExecutionResult":
        # Restore result evidence without depending on the engine's internal result dataclass.
        raw_levels = value.get("level_results", [])
        if not isinstance(raw_levels, (list, tuple)) or any(not isinstance(item, Mapping) for item in raw_levels):
            raise ValueError("level_results must be an array of objects")
        raw_observations = value.get("test_observations", [])
        if not isinstance(raw_observations, (list, tuple)) or any(
            not isinstance(item, Mapping) for item in raw_observations
        ):
            raise ValueError("test_observations must be an array of objects")
        return cls(
            execution_id=ExecutionId(required_string(value, "execution_id")),
            mutant_id=MutantId(required_string(value, "mutant_id")),
            status=required_string(value, "status"),
            classification_reason=str(value.get("classification_reason", "")),
            restore_verified=bool(value.get("restore_verified", False)),
            level_results=tuple(dict(item) for item in raw_levels),
            artifact_paths=sequence_of_strings(value, "artifact_paths"),
            error=optional_string(value, "error"),
            lease_id=optional_string(value, "lease_id"),
            attempt=max(0, int(value.get("attempt", 0))),
            test_observations=tuple(dict(item) for item in raw_observations),
            duration_seconds=(
                float(value["duration_seconds"])
                if value.get("duration_seconds") is not None
                else None
            ),
        )
