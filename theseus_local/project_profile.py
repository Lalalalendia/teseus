"""Persistent project defaults derived from deterministic local discovery."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePath
from typing import Mapping

from theseus_contracts.project_tree import project_tree_path_is_excluded

from .project_discovery import ProjectDiscoveryResult

PROJECT_PROFILE_SCHEMA_VERSION = 2
PROJECT_PROFILE_REUSE_MODES = frozenset({"hint", "exact", "partial", "off"})


@dataclass(frozen=True, slots=True)
class ProjectProfile:
    """Persistent project execution defaults reused by browser-created campaigns."""

    python_interpreter: str
    python_interpreter_relative: str | None
    interpreter_origin: str
    source_roots: tuple[str, ...]
    test_roots: tuple[str, ...]
    test_command: tuple[str, ...]
    excluded_dirs: tuple[str, ...]
    pytest_config: str | None
    project_markers: tuple[str, ...]
    git_repository: bool
    discovery_fingerprint: str
    preferred_workers: int | None = None
    max_mutants: int = 100
    max_seconds: float = 600.0
    max_test_seconds: float = 120.0
    reuse_mode: str = "hint"
    no_escalation: bool = False
    schema_version: int = PROJECT_PROFILE_SCHEMA_VERSION

    @classmethod
    def from_discovery(cls, discovery: ProjectDiscoveryResult) -> "ProjectProfile":
        # Capture deterministic discovery facts as reusable execution defaults.
        return cls(
            python_interpreter=discovery.python_interpreter,
            python_interpreter_relative=discovery.python_interpreter_relative,
            interpreter_origin=discovery.interpreter_origin,
            source_roots=discovery.source_roots,
            test_roots=discovery.test_roots,
            test_command=discovery.test_command,
            excluded_dirs=discovery.excluded_dirs,
            pytest_config=discovery.pytest_config,
            project_markers=discovery.project_markers,
            git_repository=discovery.git_root is not None,
            discovery_fingerprint=discovery.fingerprint.sha256,
        )

    @classmethod
    def from_dict(cls, raw: Mapping[str, object]) -> "ProjectProfile":
        # Restore one bounded private profile while rejecting malformed persisted settings.
        def _strings(name: str) -> tuple[str, ...]:
            # Normalize one persisted string sequence without accepting mixed values.
            value = raw.get(name, ())
            if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
                raise ValueError(f"{name} must be an array of strings")
            return tuple(value)

        def _roots(name: str) -> tuple[str, ...]:
            # Drop generated paths and collapse redundant roots when the project root already owns production files.
            roots = tuple(item for item in _strings(name) if not project_tree_path_is_excluded(PurePath(item)))
            return (".",) if "." in roots else roots

        schema_version = raw.get("schema_version", PROJECT_PROFILE_SCHEMA_VERSION)
        interpreter = raw.get("python_interpreter")
        origin = raw.get("interpreter_origin")
        fingerprint = raw.get("discovery_fingerprint")
        relative = raw.get("python_interpreter_relative")
        pytest_config = raw.get("pytest_config")
        preferred_workers = raw.get("preferred_workers")
        max_mutants = raw.get("max_mutants", 100)
        max_seconds = raw.get("max_seconds", 600.0)
        max_test_seconds = raw.get("max_test_seconds", 120.0)
        reuse_mode = raw.get("reuse_mode", "hint")
        no_escalation = raw.get("no_escalation", False)
        git_repository = raw.get("git_repository", False)
        if schema_version != PROJECT_PROFILE_SCHEMA_VERSION:
            raise ValueError("project profile schema_version is unsupported")
        if not isinstance(interpreter, str) or not interpreter:
            raise ValueError("python_interpreter must be a non-empty string")
        if relative is not None and (not isinstance(relative, str) or not relative):
            raise ValueError("python_interpreter_relative must be text or null")
        if not isinstance(origin, str) or not origin:
            raise ValueError("interpreter_origin must be a non-empty string")
        if pytest_config is not None and (not isinstance(pytest_config, str) or not pytest_config):
            raise ValueError("pytest_config must be text or null")
        if not isinstance(fingerprint, str) or not fingerprint:
            raise ValueError("discovery_fingerprint must be a non-empty string")
        if preferred_workers is not None and (isinstance(preferred_workers, bool) or not isinstance(preferred_workers, int) or preferred_workers < 1):
            raise ValueError("preferred_workers must be a positive integer or null")
        if isinstance(max_mutants, bool) or not isinstance(max_mutants, int) or max_mutants < 1:
            raise ValueError("max_mutants must be a positive integer")
        if isinstance(max_seconds, bool) or not isinstance(max_seconds, (int, float)) or float(max_seconds) <= 0.0:
            raise ValueError("max_seconds must be greater than zero")
        if isinstance(max_test_seconds, bool) or not isinstance(max_test_seconds, (int, float)) or float(max_test_seconds) <= 0.0:
            raise ValueError("max_test_seconds must be greater than zero")
        if not isinstance(reuse_mode, str) or reuse_mode not in PROJECT_PROFILE_REUSE_MODES:
            raise ValueError("reuse_mode is invalid")
        if not isinstance(no_escalation, bool):
            raise ValueError("no_escalation must be boolean")
        if not isinstance(git_repository, bool):
            raise ValueError("git_repository must be boolean")
        test_command = _strings("test_command")
        if not test_command:
            raise ValueError("test_command must not be empty")
        return cls(
            python_interpreter=interpreter,
            python_interpreter_relative=relative,
            interpreter_origin=origin,
            source_roots=_roots("source_roots"),
            test_roots=_roots("test_roots"),
            test_command=test_command,
            excluded_dirs=_strings("excluded_dirs"),
            pytest_config=pytest_config,
            project_markers=_strings("project_markers"),
            git_repository=git_repository,
            discovery_fingerprint=fingerprint,
            preferred_workers=preferred_workers,
            max_mutants=max_mutants,
            max_seconds=float(max_seconds),
            max_test_seconds=float(max_test_seconds),
            reuse_mode=reuse_mode,
            no_escalation=no_escalation,
        )

    def to_dict(self) -> dict[str, object]:
        # Serialize the private profile including the executable path required for later launches.
        return {
            "schema_version": self.schema_version,
            "python_interpreter": self.python_interpreter,
            "python_interpreter_relative": self.python_interpreter_relative,
            "interpreter_origin": self.interpreter_origin,
            "source_roots": list(self.source_roots),
            "test_roots": list(self.test_roots),
            "test_command": list(self.test_command),
            "excluded_dirs": list(self.excluded_dirs),
            "pytest_config": self.pytest_config,
            "project_markers": list(self.project_markers),
            "git_repository": self.git_repository,
            "discovery_fingerprint": self.discovery_fingerprint,
            "preferred_workers": self.preferred_workers,
            "max_mutants": self.max_mutants,
            "max_seconds": self.max_seconds,
            "max_test_seconds": self.max_test_seconds,
            "reuse_mode": self.reuse_mode,
            "no_escalation": self.no_escalation,
        }

    def public_summary(self) -> dict[str, object]:
        # Expose reusable defaults without leaking absolute interpreter or checkout paths to the browser.
        interpreter = self.python_interpreter_relative or "Theseus runtime interpreter"
        return {
            "schema_version": self.schema_version,
            "python_interpreter": interpreter,
            "interpreter_origin": self.interpreter_origin,
            "source_roots": list(self.source_roots),
            "test_roots": list(self.test_roots),
            "test_command": [interpreter, *self.test_command[1:]],
            "excluded_dirs": list(self.excluded_dirs),
            "pytest_config": self.pytest_config,
            "project_markers": list(self.project_markers),
            "git_repository": self.git_repository,
            "discovery_fingerprint": self.discovery_fingerprint,
            "preferred_workers": self.preferred_workers,
            "max_mutants": self.max_mutants,
            "max_seconds": self.max_seconds,
            "max_test_seconds": self.max_test_seconds,
            "reuse_mode": self.reuse_mode,
            "no_escalation": self.no_escalation,
        }


__all__ = ["PROJECT_PROFILE_SCHEMA_VERSION", "PROJECT_PROFILE_REUSE_MODES", "ProjectProfile"]
