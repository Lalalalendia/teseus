"""Durable investigation trajectories used by interpretable experiment learning."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

INVESTIGATION_TRAJECTORY_SCHEMA_VERSION = 1


class InvestigationTrajectoryConflict(RuntimeError):
    """Raised when one trajectory identity is reused for different immutable evidence."""


@dataclass(frozen=True, slots=True)
class TrajectoryStepEvidence:
    """One verified or rejected experiment decision retained for later policy learning."""

    experiment: str
    outcome: str
    verified: bool
    expected_information_gain_bits: float
    expected_utility: float
    actual_cost: float

    def to_dict(self) -> dict[str, object]:
        # Serialize one trajectory step without runtime object references.
        return {
            "experiment": self.experiment,
            "outcome": self.outcome,
            "verified": self.verified,
            "expected_information_gain_bits": self.expected_information_gain_bits,
            "expected_utility": self.expected_utility,
            "actual_cost": self.actual_cost,
        }


@dataclass(frozen=True, slots=True)
class InvestigationTrajectory:
    """One completed active-investigation episode with outcome and cost evidence."""

    trajectory_id: str
    project_id: str
    signature: str
    resolved_category: str
    success: bool
    total_cost: float
    steps: tuple[TrajectoryStepEvidence, ...]

    def to_dict(self) -> dict[str, object]:
        # Serialize one immutable learning example for hashing and SQLite persistence.
        return {
            "schema_version": INVESTIGATION_TRAJECTORY_SCHEMA_VERSION,
            "trajectory_id": self.trajectory_id,
            "project_id": self.project_id,
            "signature": self.signature,
            "resolved_category": self.resolved_category,
            "success": self.success,
            "total_cost": self.total_cost,
            "steps": [item.to_dict() for item in self.steps],
        }


class InvestigationTrajectoryStore:
    """Small SQLite store for idempotent, queryable investigation trajectories."""

    def __init__(self, path: str | Path) -> None:
        # Open one local trajectory database and initialize its single logical schema object.
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.path)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS investigation_trajectories (
                trajectory_id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                signature TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        self._connection.execute(
            "CREATE INDEX IF NOT EXISTS ix_investigation_trajectories_project_signature ON investigation_trajectories(project_id, signature)"
        )
        self._connection.commit()

    def close(self) -> None:
        # Release the trajectory database connection at the owning workflow boundary.
        self._connection.close()

    @staticmethod
    def _payload(trajectory: InvestigationTrajectory) -> tuple[str, str]:
        # Encode one canonical trajectory and derive its immutable content digest.
        text = json.dumps(trajectory.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return text, hashlib.sha256(text.encode("utf-8")).hexdigest()

    def record(self, trajectory: InvestigationTrajectory) -> bool:
        # Insert one trajectory idempotently and reject identity reuse with different content.
        payload, digest = self._payload(trajectory)
        existing = self._connection.execute(
            "SELECT payload_sha256 FROM investigation_trajectories WHERE trajectory_id = ?",
            (trajectory.trajectory_id,),
        ).fetchone()
        if existing is not None:
            if str(existing["payload_sha256"]) != digest:
                raise InvestigationTrajectoryConflict("trajectory identity conflict")
            return False
        self._connection.execute(
            "INSERT INTO investigation_trajectories(trajectory_id, project_id, signature, payload_sha256, payload) VALUES (?, ?, ?, ?, ?)",
            (trajectory.trajectory_id, trajectory.project_id, trajectory.signature, digest, payload),
        )
        self._connection.commit()
        return True

    def list(self, *, project_id: str, signature: str | None = None, limit: int = 1000) -> tuple[InvestigationTrajectory, ...]:
        # Query bounded project trajectories in stable identity order for reproducible learning.
        bounded = max(1, min(10000, int(limit)))
        if signature is None:
            rows = self._connection.execute(
                "SELECT payload FROM investigation_trajectories WHERE project_id = ? ORDER BY trajectory_id ASC LIMIT ?",
                (project_id, bounded),
            ).fetchall()
        else:
            rows = self._connection.execute(
                "SELECT payload FROM investigation_trajectories WHERE project_id = ? AND signature = ? ORDER BY trajectory_id ASC LIMIT ?",
                (project_id, signature, bounded),
            ).fetchall()
        return tuple(self._from_payload(str(row["payload"])) for row in rows)

    @staticmethod
    def _from_payload(payload: str) -> InvestigationTrajectory:
        # Restore one persisted canonical trajectory while validating the expected schema shape.
        raw = json.loads(payload)
        if not isinstance(raw, Mapping) or raw.get("schema_version") != INVESTIGATION_TRAJECTORY_SCHEMA_VERSION:
            raise ValueError("investigation trajectory schema is unsupported")
        raw_steps = raw.get("steps", [])
        if not isinstance(raw_steps, list):
            raise ValueError("trajectory steps must be an array")
        steps = tuple(
            TrajectoryStepEvidence(
                experiment=str(item.get("experiment", "")),
                outcome=str(item.get("outcome", "")),
                verified=item.get("verified") is True,
                expected_information_gain_bits=float(item.get("expected_information_gain_bits", 0.0)),
                expected_utility=float(item.get("expected_utility", 0.0)),
                actual_cost=float(item.get("actual_cost", 0.0)),
            )
            for item in raw_steps
            if isinstance(item, Mapping)
        )
        return InvestigationTrajectory(
            trajectory_id=str(raw.get("trajectory_id", "")),
            project_id=str(raw.get("project_id", "")),
            signature=str(raw.get("signature", "")),
            resolved_category=str(raw.get("resolved_category", "")),
            success=raw.get("success") is True,
            total_cost=float(raw.get("total_cost", 0.0)),
            steps=steps,
        )


__all__ = [
    "INVESTIGATION_TRAJECTORY_SCHEMA_VERSION",
    "InvestigationTrajectory",
    "InvestigationTrajectoryConflict",
    "InvestigationTrajectoryStore",
    "TrajectoryStepEvidence",
]
