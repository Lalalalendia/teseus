"""Baseline execution DTOs shared across engine and control-plane adapters."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .ids import CampaignId, ExecutionId
from .serialization import WireModel, optional_string, required_string


@dataclass(frozen=True, slots=True)
class BaselineObservation(WireModel):
    """One baseline level result and its evidence references."""

    campaign_id: CampaignId
    execution_id: ExecutionId
    level: str
    passed: bool
    exit_code: int | None = None
    artifact_path: str | None = None
    reason: str | None = None

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "BaselineObservation":
        # Restore a baseline row without importing the subprocess result model.
        exit_code = value.get("exit_code")
        return cls(
            campaign_id=CampaignId(required_string(value, "campaign_id")),
            execution_id=ExecutionId(required_string(value, "execution_id")),
            level=required_string(value, "level"),
            passed=bool(value.get("passed", False)),
            exit_code=int(exit_code) if exit_code is not None else None,
            artifact_path=optional_string(value, "artifact_path"),
            reason=optional_string(value, "reason"),
        )
