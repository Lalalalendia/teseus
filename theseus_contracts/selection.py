"""Selection plans and reasons at the public Theseus boundary."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from .serialization import WireModel, optional_string, required_string, sequence_of_strings


@dataclass(frozen=True, slots=True)
class SelectionEvidence(WireModel):
    """Versioned provenance for one selection decision."""

    source: str
    source_snapshot: str = ""
    revision: str = ""
    environment: str = ""
    confidence_class: str = "heuristic"
    detail: str = ""

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "SelectionEvidence":
        # Restore evidence fields while allowing future source kinds to pass through.
        return cls(
            source=required_string(value, "source"),
            source_snapshot=str(value.get("source_snapshot", "")),
            revision=str(value.get("revision", "")),
            environment=str(value.get("environment", "")),
            confidence_class=str(value.get("confidence_class", "heuristic")),
            detail=str(value.get("detail", "")),
        )


@dataclass(frozen=True, slots=True)
class SelectionReason(WireModel):
    """Explain why a test was selected without exposing ranking implementation classes."""

    source: str
    detail: str
    confidence: float | None = None

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "SelectionReason":
        # Restore a bounded human-readable reason for one selected test.
        confidence = value.get("confidence")
        return cls(
            source=required_string(value, "source"),
            detail=required_string(value, "detail"),
            confidence=float(confidence) if confidence is not None else None,
        )


@dataclass(frozen=True, slots=True)
class SelectedTest(WireModel):
    """One authoritative nodeid selected for a level or mutant."""

    nodeid: str
    rank: int
    reasons: tuple[SelectionReason, ...] = ()
    historical_health: str | None = None

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "SelectedTest":
        # Restore nested reasons while preserving nodeid text exactly as collected.
        raw_reasons = value.get("reasons", [])
        if not isinstance(raw_reasons, (list, tuple)) or any(not isinstance(item, Mapping) for item in raw_reasons):
            raise ValueError("reasons must be an array of objects")
        return cls(
            nodeid=required_string(value, "nodeid"),
            rank=max(0, int(value.get("rank", 0))),
            reasons=tuple(SelectionReason.from_dict(item) for item in raw_reasons),
            historical_health=optional_string(value, "historical_health"),
        )


@dataclass(frozen=True, slots=True)
class TestLevelPlan(WireModel):
    """One rung in the L1 → L2 → L3 test execution ladder."""

    name: str
    reason: str
    nodeids: tuple[str, ...] = ()
    files: tuple[str, ...] = ()
    command_argv: tuple[str, ...] = ()

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "TestLevelPlan":
        # Restore level commands as argv and never infer shell semantics at this boundary.
        return cls(
            name=required_string(value, "name"),
            reason=required_string(value, "reason"),
            nodeids=sequence_of_strings(value, "nodeids"),
            files=sequence_of_strings(value, "files"),
            command_argv=sequence_of_strings(value, "command_argv"),
        )


@dataclass(frozen=True, slots=True)
class SelectionSnapshot(WireModel):
    """Frozen selection proof reused by workers and later recovery."""

    snapshot_id: str
    source_path: str
    source_sha256: str
    algorithm_version: str
    levels: tuple[TestLevelPlan, ...]
    selected_tests: tuple[str, ...] = ()
    dropped_nodeids: tuple[str, ...] = ()
    index_version: str | None = None
    evidence: Mapping[str, tuple[SelectionEvidence, ...]] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "SelectionSnapshot":
        # Restore the frozen plan and keep dropped nodeids visible for diagnostics.
        raw_levels = value.get("levels", [])
        if not isinstance(raw_levels, (list, tuple)) or any(not isinstance(item, Mapping) for item in raw_levels):
            raise ValueError("levels must be an array of objects")
        raw_evidence = value.get("evidence", {})
        evidence: dict[str, tuple[SelectionEvidence, ...]] = {}
        if isinstance(raw_evidence, Mapping):
            for nodeid, rows in raw_evidence.items():
                if not isinstance(rows, (list, tuple)):
                    continue
                evidence[str(nodeid)] = tuple(
                    SelectionEvidence.from_dict(item)
                    for item in rows
                    if isinstance(item, Mapping)
                )
        return cls(
            snapshot_id=required_string(value, "snapshot_id"),
            source_path=required_string(value, "source_path"),
            source_sha256=required_string(value, "source_sha256"),
            algorithm_version=required_string(value, "algorithm_version"),
            levels=tuple(TestLevelPlan.from_dict(item) for item in raw_levels),
            selected_tests=sequence_of_strings(value, "selected_tests"),
            dropped_nodeids=sequence_of_strings(value, "dropped_nodeids"),
            index_version=optional_string(value, "index_version"),
            evidence=evidence,
        )
