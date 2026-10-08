"""Compact index snapshot contract."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .serialization import WireModel, required_string


@dataclass(frozen=True, slots=True)
class IndexSnapshot(WireModel):
    """Read-only index version shared by planner and execution workers."""

    index_version: str
    source_sha256: str
    file_count: int
    function_count: int
    test_count: int
    created_at: str = ""

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "IndexSnapshot":
        # Restore only the bounded index summary, not a storage-specific payload.
        return cls(
            index_version=required_string(value, "index_version"),
            source_sha256=required_string(value, "source_sha256"),
            file_count=int(value.get("file_count", 0)),
            function_count=int(value.get("function_count", 0)),
            test_count=int(value.get("test_count", 0)),
            created_at=str(value.get("created_at", "")),
        )
