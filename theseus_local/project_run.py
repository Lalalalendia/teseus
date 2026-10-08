"""Deterministic project-level launch planning above file-scoped mutation campaigns."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Mapping
from theseus_contracts.project_tree import project_tree_path_is_excluded
from .project_profile import ProjectProfile
PROJECT_RUN_SCHEMA_VERSION = 4
MAX_PROJECT_RUN_SOURCE_FILES = 100_000
_TEST_NAMES = ("test_", "_test.py")
@dataclass(frozen=True, slots=True)
class ProjectRunEntry:
    """One file-scoped campaign launched as part of a project run."""
    source_path: str
    campaign_id: str
    launch_status: str
    error_code: str | None = None
    completed_mutants: int = 0
    total_mutants: int = 0
    error_stage: str | None = None
    failed_at: str | None = None
    @classmethod
    def from_dict(cls, raw: Mapping[str, object]) -> "ProjectRunEntry":
        # Restore one persisted run entry while rejecting malformed identities.
        source_path = raw.get("source_path")
        campaign_id = raw.get("campaign_id")
        launch_status = raw.get("launch_status", "running")
        error_code = raw.get("error_code")
        completed_mutants = raw.get("completed_mutants", 0)
        total_mutants = raw.get("total_mutants", 0)
        error_stage = raw.get("error_stage")
        failed_at = raw.get("failed_at")
        if error_stage is None and isinstance(error_code, str):
            if error_code.startswith("campaign_") and error_code.endswith("_failed"):
                error_stage = error_code[len("campaign_"):-len("_failed")] or "campaign"
            elif error_code in {"launch_exception", "launch_failed", "campaign_launch_failed"}:
                error_stage = "launch"
        if not isinstance(source_path, str) or not source_path:
            raise ValueError("source_path must be a non-empty string")
        if not isinstance(campaign_id, str) or not campaign_id:
            raise ValueError("campaign_id must be a non-empty string")
        if not isinstance(launch_status, str) or not launch_status:
            raise ValueError("launch_status must be a non-empty string")
        if error_code is not None and (not isinstance(error_code, str) or not error_code):
            raise ValueError("error_code must be text or null")
        if isinstance(completed_mutants, bool) or not isinstance(completed_mutants, int) or completed_mutants < 0:
            raise ValueError("completed_mutants must be a non-negative integer")
        if isinstance(total_mutants, bool) or not isinstance(total_mutants, int) or total_mutants < 0 or completed_mutants > total_mutants:
            raise ValueError("total_mutants must be a non-negative integer not below completed_mutants")
        if error_stage is not None and (not isinstance(error_stage, str) or not error_stage):
            raise ValueError("error_stage must be text or null")
        if failed_at is not None and (not isinstance(failed_at, str) or not failed_at):
            raise ValueError("failed_at must be text or null")
        return cls(source_path, campaign_id, launch_status, error_code, completed_mutants, total_mutants, error_stage, failed_at)
    def to_dict(self) -> dict[str, object]:
        # Serialize one project-run campaign entry without filesystem authority data.
        return {
            "source_path": self.source_path,
            "campaign_id": self.campaign_id,
            "launch_status": self.launch_status,
            "error_code": self.error_code,
            "completed_mutants": self.completed_mutants,
            "total_mutants": self.total_mutants,
            "error_stage": self.error_stage,
            "failed_at": self.failed_at,
        }
@dataclass(frozen=True, slots=True)
class ProjectRunRecord:
    """Persistent metadata linking one operator run to its file campaigns."""
    run_id: str
    project_id: str
    entries: tuple[ProjectRunEntry, ...]
    created_at: str | None = None
    completed_at: str | None = None
    worker_budget: int | None = None
    schema_version: int = PROJECT_RUN_SCHEMA_VERSION
    @classmethod
    def from_dict(cls, raw: Mapping[str, object]) -> "ProjectRunRecord":
        # Restore current project runs while migrating the A3 schema without a separate registry migration.
        schema_version = raw.get("schema_version", 1)
        run_id = raw.get("run_id")
        project_id = raw.get("project_id")
        entries = raw.get("entries", [])
        created_at = raw.get("created_at")
        completed_at = raw.get("completed_at")
        worker_budget = raw.get("worker_budget")
        if schema_version not in {1, 2, 3, PROJECT_RUN_SCHEMA_VERSION}:
            raise ValueError("project run schema_version is unsupported")
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("run_id must be a non-empty string")
        if not isinstance(project_id, str) or not project_id:
            raise ValueError("project_id must be a non-empty string")
        if not isinstance(entries, list) or len(entries) > MAX_PROJECT_RUN_SOURCE_FILES:
            raise ValueError("entries must be a bounded array")
        if created_at is not None and (not isinstance(created_at, str) or not created_at):
            raise ValueError("created_at must be text or null")
        if completed_at is not None and (not isinstance(completed_at, str) or not completed_at):
            raise ValueError("completed_at must be text or null")
        if worker_budget is not None and (isinstance(worker_budget, bool) or not isinstance(worker_budget, int) or worker_budget < 1):
            raise ValueError("worker_budget must be a positive integer or null")
        return cls(
            run_id,
            project_id,
            tuple(ProjectRunEntry.from_dict(item) for item in entries if isinstance(item, Mapping)),
            created_at=created_at,
            completed_at=completed_at,
            worker_budget=worker_budget,
        )
    def to_dict(self) -> dict[str, object]:
        # Serialize one project run with enough timing context for project-specific performance learning.
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "project_id": self.project_id,
            "entries": [entry.to_dict() for entry in self.entries],
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "worker_budget": self.worker_budget,
        }
def _is_production_python(path: Path) -> bool:
    # Keep Python implementation files while excluding conventional pytest-owned files.
    name = path.name.casefold()
    return path.suffix.casefold() == ".py" and name != "conftest.py" and not name.startswith(_TEST_NAMES[0]) and not name.endswith(_TEST_NAMES[1])
def project_test_cwd(profile: ProjectProfile, source_path: str) -> str:
    # Select the deepest test component that owns one source path while retaining root fallback.
    source = PurePosixPath(str(source_path).replace("\\", "/"))
    candidates: list[PurePosixPath] = []
    for raw_test_root in profile.test_roots:
        test_root = PurePosixPath(str(raw_test_root).replace("\\", "/"))
        component = test_root.parent
        if component == PurePosixPath(".") or component in source.parents:
            candidates.append(component)
    if not candidates:
        return "."
    selected = max(candidates, key=lambda item: len(item.parts))
    return "." if selected == PurePosixPath(".") else selected.as_posix()
def project_source_files(project_root: str | Path, profile: ProjectProfile) -> tuple[str, ...]:
    # Enumerate deterministic production Python files only under the profile-owned source roots.
    root = Path(project_root).expanduser().resolve()
    if not root.is_dir():
        raise ValueError("project root is not a directory")
    excluded = {item.casefold() for item in profile.excluded_dirs}
    test_roots = tuple(
        resolved
        for item in profile.test_roots
        for resolved in ((root / item).resolve(),)
        if resolved.is_relative_to(root) and resolved != root
    )
    discovered: set[str] = set()
    source_roots = (".",) if "." in profile.source_roots else profile.source_roots
    for source_root in source_roots:
        base = root if source_root == "." else (root / source_root).resolve()
        if not base.is_dir() or not base.is_relative_to(root):
            continue
        for candidate in base.rglob("*.py"):
            try:
                relative = candidate.resolve().relative_to(root)
            except (OSError, ValueError):
                continue
            resolved = candidate.resolve()
            if any(resolved == test_root or test_root in resolved.parents for test_root in test_roots):
                continue
            if project_tree_path_is_excluded(relative) or any(part.casefold() in excluded or part.casefold() in {"test", "tests"} for part in relative.parts[:-1]):
                continue
            if _is_production_python(candidate):
                discovered.add(relative.as_posix())
            if len(discovered) > MAX_PROJECT_RUN_SOURCE_FILES:
                raise ValueError("project source inventory exceeds the safety ceiling")
    return tuple(sorted(discovered))
__all__ = [
    "MAX_PROJECT_RUN_SOURCE_FILES",
    "PROJECT_RUN_SCHEMA_VERSION",
    "ProjectRunEntry",
    "ProjectRunRecord",
    "project_source_files",
    "project_test_cwd",
]
