"""Durable per-project concurrency memory for repeated local runs."""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from threading import RLock
from typing import Mapping
from uuid import uuid4

from .concurrency import AutoConcurrencyDecision, ConcurrencySample, choose_auto_workers

TUNING_SCHEMA_VERSION = 1
MAX_PROJECT_SAMPLES = 128


@dataclass(frozen=True, slots=True)
class ProjectPerformanceSample:
    """One immutable completed-run throughput observation."""

    sample_id: str
    project_id: str
    workers: int
    completed_mutants: int
    wall_seconds: float

    @property
    def mutants_per_second(self) -> float:
        # Derive throughput without persisting a second potentially divergent numeric authority.
        return self.completed_mutants / self.wall_seconds if self.wall_seconds > 0.0 else 0.0

    def to_concurrency_sample(self) -> ConcurrencySample:
        # Adapt one durable project sample to the scheduler's compact history contract.
        return ConcurrencySample(self.workers, self.mutants_per_second, self.wall_seconds, self.completed_mutants)


class ProjectTuningStore:
    """Atomic JSON store retaining bounded project-specific concurrency evidence."""

    def __init__(self, path: str | Path) -> None:
        # Bind one tuning authority to a private state file without touching project sources.
        self.path = Path(path).expanduser().resolve()
        self._lock = RLock()

    def _load(self) -> dict[str, list[dict[str, object]]]:
        # Load only well-formed bounded sample arrays and fail closed on corrupted state.
        if not self.path.is_file():
            return {}
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            return {}
        if not isinstance(raw, Mapping) or raw.get("schema_version") != TUNING_SCHEMA_VERSION:
            return {}
        projects = raw.get("projects", {})
        if not isinstance(projects, Mapping):
            return {}
        result: dict[str, list[dict[str, object]]] = {}
        for project_id, rows in projects.items():
            if isinstance(project_id, str) and isinstance(rows, list):
                result[project_id] = [dict(item) for item in rows[-MAX_PROJECT_SAMPLES:] if isinstance(item, Mapping)]
        return result

    def _save(self, projects: Mapping[str, list[dict[str, object]]]) -> None:
        # Persist one bounded deterministic snapshot through an atomic replace.
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"schema_version": TUNING_SCHEMA_VERSION, "projects": {key: value[-MAX_PROJECT_SAMPLES:] for key, value in sorted(projects.items())}}
        temporary = self.path.with_name(f".{self.path.name}.{os.getpid()}-{uuid4().hex}.tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        os.replace(temporary, self.path)

    def record(self, sample: ProjectPerformanceSample) -> None:
        # Append one idempotent completed-run sample without allowing duplicate run identities.
        if not sample.sample_id or not sample.project_id or sample.workers < 1 or sample.completed_mutants < 1 or sample.wall_seconds <= 0.0:
            raise ValueError("project performance sample is invalid")
        with self._lock:
            projects = self._load()
            rows = projects.setdefault(sample.project_id, [])
            if any(str(item.get("sample_id", "")) == sample.sample_id for item in rows):
                return
            rows.append(asdict(sample))
            projects[sample.project_id] = rows[-MAX_PROJECT_SAMPLES:]
            self._save(projects)

    def samples(self, project_id: str) -> tuple[ProjectPerformanceSample, ...]:
        # Return valid project history in persistence order while ignoring malformed legacy rows.
        with self._lock:
            rows = tuple(self._load().get(project_id, ()))
        result: list[ProjectPerformanceSample] = []
        for row in rows:
            try:
                result.append(
                    ProjectPerformanceSample(
                        sample_id=str(row["sample_id"]),
                        project_id=str(row["project_id"]),
                        workers=int(row["workers"]),
                        completed_mutants=int(row["completed_mutants"]),
                        wall_seconds=float(row["wall_seconds"]),
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
        return tuple(result)

    def recommend(
        self,
        project_id: str,
        *,
        preferred_workers: int | None = None,
        workload_size: int | None = None,
        cpu_count: int | None = None,
        memory_available_bytes: int | None = None,
    ) -> AutoConcurrencyDecision:
        # Choose workers from project history first and deterministic machine capacity otherwise.
        history = tuple(item.to_concurrency_sample() for item in self.samples(project_id))
        return choose_auto_workers(
            cpu_count=cpu_count,
            memory_available_bytes=memory_available_bytes,
            preferred_workers=preferred_workers,
            workload_size=workload_size,
            history=history,
        )


__all__ = ["MAX_PROJECT_SAMPLES", "ProjectPerformanceSample", "ProjectTuningStore", "TUNING_SCHEMA_VERSION"]
