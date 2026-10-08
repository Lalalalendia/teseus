"""Typed identifiers used by the Theseus wire protocol."""
from __future__ import annotations
import hashlib
from dataclasses import dataclass
from .serialization import dumps
@dataclass(frozen=True, slots=True)
class Identifier:
    """Base behavior shared by string-backed protocol identifiers."""
    value: str
    def __post_init__(self) -> None:
        # Keep identity values explicit and reject accidental empty or non-string keys.
        if not isinstance(self.value, str) or not self.value.strip():
            raise ValueError("identifier value must be a non-empty string")
    def __hash__(self) -> int:
        # Make identifiers usable as dictionary keys without exposing storage details.
        return hash((type(self), self.value))
    def __eq__(self, other: object) -> bool:
        # Keep different identifier domains from comparing equal by accident.
        return type(self) is type(other) and getattr(other, "value", None) == self.value
    def __repr__(self) -> str:
        # Render the domain type in diagnostics while keeping the wire value unchanged.
        return f"{type(self).__name__}({self.value!r})"
    def __str__(self) -> str:
        # Allow safe path and log formatting without implicit domain conversion.
        return self.value
    def to_wire_value(self) -> str:
        # Expose exactly the string representation used by JSON serialization.
        return self.value
class ProjectId(Identifier):
    """Stable project identifier."""
class RevisionId(Identifier):
    """Repository revision identifier."""
class CampaignId(Identifier):
    """Mutation campaign identifier."""
class PlanId(Identifier):
    """Stable execution-plan identifier."""
class MutantId(Identifier):
    """Deterministic mutation identifier."""
class ProducerId(Identifier):
    """Stable statistics-event producer identifier."""
class ProcessId(Identifier):
    """Stable process lifecycle identifier independent from an operating-system PID."""
class TestId(Identifier):
    """Canonical project-relative test identifier."""
class WorkerId(Identifier):
    """Worker process identifier."""
class ShardId(Identifier):
    """Mutation shard identifier."""
class ExecutionId(Identifier):
    """One mutant or shard execution identifier."""
class EventId(Identifier):
    """Append-only engine or statistics event identifier."""
class ArtifactId(Identifier):
    """Stored evidence artifact identifier."""
class MessageId(Identifier):
    """Generic wire-message identifier."""
def deterministic_id(prefix: str, *parts: object) -> str:
    # Derive a relocation-safe identifier from canonical values without absolute paths.
    if not isinstance(prefix, str) or not prefix.strip():
        raise ValueError("identifier prefix must be non-empty")
    digest = hashlib.sha256(dumps(parts).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}_{digest}"
