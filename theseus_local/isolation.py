"""Explicit trust and process/workspace policy for remote worker execution."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

from test_intelligence_unified_v1.models import ProcessResult


class IsolationPolicyError(RuntimeError):
    """Raised when an execution request would cross the worker boundary unsafely."""


@dataclass(frozen=True, slots=True)
class ExecutionPolicy:
    """Policy that is enforceable by the reference local process backend."""

    timeout_seconds: float = 120.0
    max_output_bytes: int = 8 * 1024 * 1024
    allowed_environment: tuple[str, ...] = (
        "PATH",
        "PATHEXT",
        "PYTHONUNBUFFERED",
        "SYSTEMROOT",
        "TEMP",
        "TMP",
        "USERPROFILE",
        "VIRTUAL_ENV",
        "WINDIR",
        "HOME",
        "LANG",
        "LC_ALL",
    )
    required_environment: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if float(self.timeout_seconds) <= 0:
            raise IsolationPolicyError("execution timeout must be positive")
        if int(self.max_output_bytes) < 1:
            raise IsolationPolicyError("max_output_bytes must be positive")

    def environment(self, supplied: Mapping[str, str] | None = None) -> dict[str, str]:
        """Build an explicit environment instead of forwarding coordinator secrets."""

        source = dict(os.environ)
        if supplied is not None:
            source.update({str(key): str(value) for key, value in supplied.items()})
        allowed = {item.upper() for item in self.allowed_environment}
        result = {
            key: value
            for key, value in source.items()
            if key.upper() in allowed
        }
        result.update({str(key): str(value) for key, value in self.required_environment.items()})
        return result

    def validate_result(self, result: ProcessResult) -> None:
        """Turn unbounded-output diagnostics or a leaked tree into infrastructure failure."""

        if int(result.output_bytes) > int(self.max_output_bytes):
            raise IsolationPolicyError(
                "worker output exceeded policy limit: "
                f"{result.output_bytes}>{self.max_output_bytes}"
            )
        if bool(result.process_tree_leak):
            raise IsolationPolicyError("worker process tree did not terminate cleanly")


def resolve_workspace(root: Path, relative: str) -> Path:
    """Resolve a worker workspace under its private root and reject traversal/absolute paths."""

    def canonical(path: Path) -> Path:
        resolved = str(path.resolve())
        # pathlib may return an extended-length ``\\?\`` spelling only after
        # a concurrently-created directory exists. Normalize both sides before
        # comparing containment so the security check is race-stable on Windows.
        if os.name == "nt" and resolved.startswith("\\\\?\\"):
            resolved = resolved[4:]
        return Path(resolved)

    base = canonical(Path(root))
    candidate = canonical(base / str(relative).replace("\\", "/"))
    if not candidate.is_relative_to(base):
        raise IsolationPolicyError(
            "workspace escaped the worker boundary: "
            f"base={base}; relative={relative!r}; candidate={candidate}"
        )
    return candidate


__all__ = ["ExecutionPolicy", "IsolationPolicyError", "resolve_workspace"]
