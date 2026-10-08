"""Versioned deterministic contracts for remote execution.

The protocol carries physical execution facts only.  Mutation classification and
authoritative evidence remain coordinator-owned concerns.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
from math import isfinite
from numbers import Real
from typing import Any, Mapping

from .serialization import dumps, loads_object, validate_utc_timestamp


REMOTE_PROTOCOL_VERSION = 1
REMOTE_SCHEMA_VERSION = 1


class RemoteProtocolError(ValueError):
    """Raised for malformed, incompatible, or identity-conflicting remote frames."""


def _required(value: Mapping[str, Any], name: str) -> str:
    item = value.get(name)
    if not isinstance(item, str) or not item.strip():
        raise RemoteProtocolError(f"remote field {name!r} must be a non-empty string")
    return item


def _required_integer(value: Mapping[str, Any], name: str) -> int:
    item = value.get(name)
    if isinstance(item, bool) or not isinstance(item, int):
        raise RemoteProtocolError(f"remote field {name!r} must be an integer")
    return item


def _required_boolean(value: Mapping[str, Any], name: str) -> bool:
    item = value.get(name)
    if not isinstance(item, bool):
        raise RemoteProtocolError(f"remote field {name!r} must be a boolean")
    return item


def _number(value: Any, name: str, *, default: Any = None) -> float:
    item = value.get(name, default)
    if isinstance(item, bool) or not isinstance(item, Real) or not isfinite(float(item)):
        raise RemoteProtocolError(f"remote field {name!r} must be a finite number")
    return float(item)


def _optional_string(value: Mapping[str, Any], name: str, *, default: str | None = None) -> str | None:
    item = value.get(name, default)
    if item is not None and not isinstance(item, str):
        raise RemoteProtocolError(f"remote field {name!r} must be a string or null")
    return item


def _reject_unknown(value: Mapping[str, Any], allowed: set[str]) -> None:
    unknown = sorted(str(key) for key in value if str(key) not in allowed)
    if unknown:
        raise RemoteProtocolError(f"unknown remote protocol fields: {unknown}")


def _relative_workspace(value: str) -> str:
    normalized = str(value).replace("\\", "/").strip() or "."
    path = PurePosixPath(normalized)
    if path.is_absolute() or ".." in path.parts:
        raise RemoteProtocolError("workspace path must be relative and traversal-free")
    return path.as_posix()


@dataclass(frozen=True, slots=True)
class RemoteExecutionRequest:
    """Immutable request bound by the coordinator before remote delivery."""

    execution_attempt_id: str
    evidence_identity: str
    mutation_identity: str
    runtime_identity: Mapping[str, Any]
    project_snapshot_id: str
    prepared_artifact_id: str
    test_plan_identity: str
    argv: tuple[str, ...]
    source_path: str
    expected_source_sha256: str
    workspace_relative: str = "."
    environment: Mapping[str, str] = field(default_factory=dict)
    timeout_seconds: float = 120.0
    deadline_utc: str | None = None
    cancellation_token: str = ""
    protocol_version: int = REMOTE_PROTOCOL_VERSION
    schema_version: int = REMOTE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if isinstance(self.protocol_version, bool) or not isinstance(self.protocol_version, int):
            raise RemoteProtocolError("protocol_version must be an integer")
        if isinstance(self.schema_version, bool) or not isinstance(self.schema_version, int):
            raise RemoteProtocolError("schema_version must be an integer")
        if self.protocol_version != REMOTE_PROTOCOL_VERSION:
            raise RemoteProtocolError("unsupported remote protocol version")
        if self.schema_version != REMOTE_SCHEMA_VERSION:
            raise RemoteProtocolError("unsupported remote schema version")
        for name, item in (
            ("execution_attempt_id", self.execution_attempt_id),
            ("evidence_identity", self.evidence_identity),
            ("mutation_identity", self.mutation_identity),
            ("project_snapshot_id", self.project_snapshot_id),
            ("prepared_artifact_id", self.prepared_artifact_id),
            ("test_plan_identity", self.test_plan_identity),
        ):
            if not str(item).strip():
                raise RemoteProtocolError(f"{name} must be non-empty")
        if not isinstance(self.runtime_identity, Mapping) or not self.runtime_identity:
            raise RemoteProtocolError("runtime_identity must be a non-empty object")
        if not isinstance(self.argv, (tuple, list)) or not self.argv or any(
            not isinstance(item, str) or not item for item in self.argv
        ):
            raise RemoteProtocolError("argv must be a non-empty array of strings")
        if isinstance(self.timeout_seconds, bool) or not isinstance(self.timeout_seconds, Real):
            raise RemoteProtocolError("timeout_seconds must be a finite positive number")
        if not isfinite(float(self.timeout_seconds)) or float(self.timeout_seconds) <= 0:
            raise RemoteProtocolError("timeout_seconds must be positive")
        if self.deadline_utc is not None:
            try:
                validate_utc_timestamp(self.deadline_utc, field_name="deadline_utc")
            except ValueError as exc:
                raise RemoteProtocolError(str(exc)) from exc
        _relative_workspace(self.workspace_relative)
        if not isinstance(self.source_path, str) or not self.source_path.strip():
            raise RemoteProtocolError("source_path must be non-empty")
        _relative_workspace(self.source_path)
        if not isinstance(self.expected_source_sha256, str) or len(self.expected_source_sha256) != 64:
            raise RemoteProtocolError("expected_source_sha256 must be a SHA-256 hex digest")
        if any(char not in "0123456789abcdefABCDEF" for char in self.expected_source_sha256):
            raise RemoteProtocolError("expected_source_sha256 must be a SHA-256 hex digest")
        if not isinstance(self.environment, Mapping):
            raise RemoteProtocolError("environment must be an object")
        if any(not isinstance(key, str) or not isinstance(item, str) for key, item in self.environment.items()):
            raise RemoteProtocolError("environment must contain only string keys and values")
        if not isinstance(self.cancellation_token, str):
            raise RemoteProtocolError("cancellation_token must be a string")

    def to_dict(self) -> dict[str, Any]:
        return {
            "protocol_version": int(self.protocol_version),
            "schema_version": int(self.schema_version),
            "execution_attempt_id": self.execution_attempt_id,
            "evidence_identity": self.evidence_identity,
            "mutation_identity": self.mutation_identity,
            "runtime_identity": dict(self.runtime_identity),
            "project_snapshot_id": self.project_snapshot_id,
            "prepared_artifact_id": self.prepared_artifact_id,
            "test_plan_identity": self.test_plan_identity,
            "argv": list(self.argv),
            "workspace_relative": _relative_workspace(self.workspace_relative),
            "environment": {str(key): str(item) for key, item in sorted(self.environment.items())},
            "timeout_seconds": float(self.timeout_seconds),
            "deadline_utc": self.deadline_utc,
            "cancellation_token": self.cancellation_token,
            "source_path": self.source_path,
            "expected_source_sha256": self.expected_source_sha256,
        }

    def to_json(self) -> str:
        return dumps(self.to_dict())

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "RemoteExecutionRequest":
        if not isinstance(value, Mapping):
            raise RemoteProtocolError("remote execution request must be an object")
        _reject_unknown(
            value,
            {
                "protocol_version",
                "schema_version",
                "execution_attempt_id",
                "evidence_identity",
                "mutation_identity",
                "runtime_identity",
                "project_snapshot_id",
                "prepared_artifact_id",
                "test_plan_identity",
                "argv",
                "workspace_relative",
                "environment",
                "timeout_seconds",
                "deadline_utc",
                "cancellation_token",
                "source_path",
                "expected_source_sha256",
            },
        )
        raw_runtime = value.get("runtime_identity")
        raw_argv = value.get("argv")
        raw_env = value.get("environment", {})
        if not isinstance(raw_runtime, Mapping) or not raw_runtime or not isinstance(raw_env, Mapping):
            raise RemoteProtocolError("runtime_identity and environment must be objects")
        if not isinstance(raw_argv, (list, tuple)):
            raise RemoteProtocolError("argv must be an array")
        if any(not isinstance(item, str) or not item for item in raw_argv):
            raise RemoteProtocolError("argv must contain non-empty strings")
        if any(not isinstance(key, str) or not isinstance(item, str) for key, item in raw_env.items()):
            raise RemoteProtocolError("environment must contain only string keys and values")
        raw_workspace = value.get("workspace_relative", ".")
        if not isinstance(raw_workspace, str):
            raise RemoteProtocolError("workspace_relative must be a string")
        return cls(
            protocol_version=_required_integer(value, "protocol_version"),
            schema_version=_required_integer(value, "schema_version"),
            execution_attempt_id=_required(value, "execution_attempt_id"),
            evidence_identity=_required(value, "evidence_identity"),
            mutation_identity=_required(value, "mutation_identity"),
            runtime_identity=dict(raw_runtime),
            project_snapshot_id=_required(value, "project_snapshot_id"),
            prepared_artifact_id=_required(value, "prepared_artifact_id"),
            test_plan_identity=_required(value, "test_plan_identity"),
            argv=tuple(raw_argv),
            workspace_relative=raw_workspace,
            environment=dict(raw_env),
            timeout_seconds=_number(value, "timeout_seconds", default=0.0),
            deadline_utc=_optional_string(value, "deadline_utc"),
            cancellation_token=_optional_string(value, "cancellation_token", default="") or "",
            source_path=_required(value, "source_path"),
            expected_source_sha256=_required(value, "expected_source_sha256"),
        )

    @classmethod
    def from_json(cls, raw: str | bytes) -> "RemoteExecutionRequest":
        return cls.from_dict(loads_object(raw))


@dataclass(frozen=True, slots=True)
class RemoteExecutionResult:
    """Physical facts returned by a worker; it intentionally has no mutation status."""

    execution_attempt_id: str
    evidence_identity: str
    mutation_identity: str
    started: bool
    exit_code: int | None
    timed_out: bool
    cancelled: bool
    elapsed_seconds: float
    worker_runtime_fingerprint: str
    workspace_integrity: str
    source_sha256: str | None = None
    prepared_artifact_sha256: str | None = None
    stdout_artifact_id: str | None = None
    stderr_artifact_id: str | None = None
    diagnostic_error: str | None = None
    protocol_version: int = REMOTE_PROTOCOL_VERSION
    schema_version: int = REMOTE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if isinstance(self.protocol_version, bool) or not isinstance(self.protocol_version, int):
            raise RemoteProtocolError("protocol_version must be an integer")
        if isinstance(self.schema_version, bool) or not isinstance(self.schema_version, int):
            raise RemoteProtocolError("schema_version must be an integer")
        if self.protocol_version != REMOTE_PROTOCOL_VERSION:
            raise RemoteProtocolError("unsupported remote protocol version")
        if self.schema_version != REMOTE_SCHEMA_VERSION:
            raise RemoteProtocolError("unsupported remote schema version")
        if not all(
            str(item).strip()
            for item in (
                self.execution_attempt_id,
                self.evidence_identity,
                self.mutation_identity,
                self.worker_runtime_fingerprint,
                self.workspace_integrity,
            )
        ):
            raise RemoteProtocolError("remote result identity fields must be non-empty")
        if not isinstance(self.started, bool) or not isinstance(self.timed_out, bool) or not isinstance(self.cancelled, bool):
            raise RemoteProtocolError("started, timed_out, and cancelled must be booleans")
        if self.exit_code is not None and (
            isinstance(self.exit_code, bool) or not isinstance(self.exit_code, int)
        ):
            raise RemoteProtocolError("exit_code must be an integer or null")
        if isinstance(self.elapsed_seconds, bool) or not isinstance(self.elapsed_seconds, Real):
            raise RemoteProtocolError("elapsed_seconds must be a finite non-negative number")
        if not isfinite(float(self.elapsed_seconds)) or float(self.elapsed_seconds) < 0:
            raise RemoteProtocolError("elapsed_seconds must not be negative")
        for name, value in (
            ("source_sha256", self.source_sha256),
            ("prepared_artifact_sha256", self.prepared_artifact_sha256),
        ):
            if value is not None and (
                not isinstance(value, str)
                or len(value) != 64
                or any(char not in "0123456789abcdefABCDEF" for char in value)
            ):
                raise RemoteProtocolError(f"{name} must be a SHA-256 hex digest when present")
        for name, value in (
            ("stdout_artifact_id", self.stdout_artifact_id),
            ("stderr_artifact_id", self.stderr_artifact_id),
            ("diagnostic_error", self.diagnostic_error),
        ):
            if value is not None and not isinstance(value, str):
                raise RemoteProtocolError(f"{name} must be a string or null")

    def to_dict(self) -> dict[str, Any]:
        return {
            "protocol_version": int(self.protocol_version),
            "schema_version": int(self.schema_version),
            "execution_attempt_id": self.execution_attempt_id,
            "evidence_identity": self.evidence_identity,
            "mutation_identity": self.mutation_identity,
            "started": bool(self.started),
            "exit_code": int(self.exit_code) if self.exit_code is not None else None,
            "timed_out": bool(self.timed_out),
            "cancelled": bool(self.cancelled),
            "elapsed_seconds": float(self.elapsed_seconds),
            "worker_runtime_fingerprint": self.worker_runtime_fingerprint,
            "workspace_integrity": self.workspace_integrity,
            "source_sha256": self.source_sha256,
            "prepared_artifact_sha256": self.prepared_artifact_sha256,
            "stdout_artifact_id": self.stdout_artifact_id,
            "stderr_artifact_id": self.stderr_artifact_id,
            "diagnostic_error": self.diagnostic_error,
        }

    def to_json(self) -> str:
        return dumps(self.to_dict())

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "RemoteExecutionResult":
        if not isinstance(value, Mapping):
            raise RemoteProtocolError("remote execution result must be an object")
        _reject_unknown(
            value,
            {
                "protocol_version",
                "schema_version",
                "execution_attempt_id",
                "evidence_identity",
                "mutation_identity",
                "started",
                "exit_code",
                "timed_out",
                "cancelled",
                "elapsed_seconds",
                "worker_runtime_fingerprint",
                "workspace_integrity",
                "source_sha256",
                "prepared_artifact_sha256",
                "stdout_artifact_id",
                "stderr_artifact_id",
                "diagnostic_error",
            },
        )
        return cls(
            protocol_version=_required_integer(value, "protocol_version"),
            schema_version=_required_integer(value, "schema_version"),
            execution_attempt_id=_required(value, "execution_attempt_id"),
            evidence_identity=_required(value, "evidence_identity"),
            mutation_identity=_required(value, "mutation_identity"),
            started=_required_boolean(value, "started"),
            exit_code=(
                value["exit_code"]
                if value.get("exit_code") is None
                else _required_integer(value, "exit_code")
            ),
            timed_out=_required_boolean(value, "timed_out"),
            cancelled=_required_boolean(value, "cancelled"),
            elapsed_seconds=_number(value, "elapsed_seconds", default=0.0),
            worker_runtime_fingerprint=_required(value, "worker_runtime_fingerprint"),
            workspace_integrity=_required(value, "workspace_integrity"),
            source_sha256=_optional_string(value, "source_sha256"),
            prepared_artifact_sha256=_optional_string(value, "prepared_artifact_sha256"),
            stdout_artifact_id=_optional_string(value, "stdout_artifact_id"),
            stderr_artifact_id=_optional_string(value, "stderr_artifact_id"),
            diagnostic_error=_optional_string(value, "diagnostic_error"),
        )

    @classmethod
    def from_json(cls, raw: str | bytes) -> "RemoteExecutionResult":
        return cls.from_dict(loads_object(raw))


__all__ = [
    "REMOTE_PROTOCOL_VERSION",
    "REMOTE_SCHEMA_VERSION",
    "RemoteExecutionRequest",
    "RemoteExecutionResult",
    "RemoteProtocolError",
]
