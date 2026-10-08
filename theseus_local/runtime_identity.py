"""Canonical runtime identity and compatibility checks for installed Theseus nodes.

The identity deliberately describes the installed/runtime environment rather than a
checkout path.  This makes coordinator and worker diagnostics comparable after a
wheel installation and gives incompatible state an explicit rejection path.
"""

from __future__ import annotations

import hashlib
import platform
import sys
from dataclasses import dataclass
from importlib import metadata
from typing import Any, Mapping

from test_intelligence_unified_v1 import __version__ as THESEUS_VERSION
from theseus_contracts import (
    PROTOCOL_VERSION,
    SCHEMA_VERSION,
    WORKER_PROTOCOL_VERSION,
    WORKER_SCHEMA_VERSION,
)
from theseus_contracts.serialization import dumps


RUNTIME_IDENTITY_SCHEMA_VERSION = 1


class RuntimeCompatibilityError(RuntimeError):
    """Raised when a node cannot safely consume another node's durable state."""


def _distribution_version() -> str:
    """Return the installed distribution version without making packaging mandatory."""

    try:
        return str(metadata.version("theseus-mutation-platform"))
    except metadata.PackageNotFoundError:
        return str(THESEUS_VERSION)


def _fingerprint_payload(
    *,
    theseus_version: str,
    python_version: str,
    python_implementation: str,
    platform_name: str,
    architecture: str,
    protocol_versions: Mapping[str, int],
    schema_versions: Mapping[str, int],
) -> dict[str, Any]:
    """Build the path-independent payload from which the runtime fingerprint is derived."""

    return {
        "schema_version": RUNTIME_IDENTITY_SCHEMA_VERSION,
        "theseus_version": str(theseus_version),
        "python_version": str(python_version),
        "python_implementation": str(python_implementation),
        "platform": str(platform_name),
        "architecture": str(architecture),
        "protocol_versions": {
            str(key): int(value) for key, value in sorted(protocol_versions.items())
        },
        "schema_versions": {
            str(key): int(value) for key, value in sorted(schema_versions.items())
        },
    }


def _fingerprint(payload: Mapping[str, Any]) -> str:
    """Hash canonical JSON bytes so equivalent installations get equivalent identities."""

    return hashlib.sha256(dumps(dict(payload)).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class RuntimeIdentity:
    """Immutable identity advertised by a coordinator or remote worker."""

    schema_version: int
    theseus_version: str
    python_version: str
    python_implementation: str
    platform: str
    architecture: str
    protocol_versions: Mapping[str, int]
    schema_versions: Mapping[str, int]
    runtime_fingerprint: str

    def __post_init__(self) -> None:
        if isinstance(self.schema_version, bool) or not isinstance(self.schema_version, int):
            raise RuntimeCompatibilityError("runtime identity schema_version must be an integer")
        if self.schema_version != RUNTIME_IDENTITY_SCHEMA_VERSION:
            raise RuntimeCompatibilityError(
                f"unsupported runtime identity schema: {self.schema_version}"
            )
        if not all(
            str(value).strip()
            for value in (
                self.theseus_version,
                self.python_version,
                self.python_implementation,
                self.platform,
                self.architecture,
                self.runtime_fingerprint,
            )
        ):
            raise ValueError("runtime identity fields must be non-empty")
        if not isinstance(self.protocol_versions, Mapping) or not isinstance(self.schema_versions, Mapping):
            raise ValueError("runtime identity protocol/schema versions must be objects")
        if not self.protocol_versions or not self.schema_versions:
            raise ValueError("runtime identity protocol/schema versions must be present")
        for label, versions in (("protocol", self.protocol_versions), ("schema", self.schema_versions)):
            if any(
                not isinstance(key, str)
                or not key.strip()
                or isinstance(value, bool)
                or not isinstance(value, int)
                or value < 1
                for key, value in versions.items()
            ):
                raise ValueError(f"runtime identity {label} versions must contain positive integer values")
        expected = _fingerprint_payload(
            theseus_version=self.theseus_version,
            python_version=self.python_version,
            python_implementation=self.python_implementation,
            platform_name=self.platform,
            architecture=self.architecture,
            protocol_versions=self.protocol_versions,
            schema_versions=self.schema_versions,
        )
        if self.runtime_fingerprint != _fingerprint(expected):
            raise RuntimeCompatibilityError("runtime fingerprint does not match identity fields")

    def to_dict(self) -> dict[str, Any]:
        """Return a deterministic JSON-safe runtime identity projection."""

        return {
            "schema_version": int(self.schema_version),
            "theseus_version": self.theseus_version,
            "python_version": self.python_version,
            "python_implementation": self.python_implementation,
            "platform": self.platform,
            "architecture": self.architecture,
            "protocol_versions": {
                str(key): int(value) for key, value in sorted(self.protocol_versions.items())
            },
            "schema_versions": {
                str(key): int(value) for key, value in sorted(self.schema_versions.items())
            },
            "runtime_fingerprint": self.runtime_fingerprint,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "RuntimeIdentity":
        """Decode and verify an identity before it is used for assignment."""

        if not isinstance(value, Mapping):
            raise RuntimeCompatibilityError("runtime identity must be an object")
        allowed = {
            "schema_version",
            "theseus_version",
            "python_version",
            "python_implementation",
            "platform",
            "architecture",
            "protocol_versions",
            "schema_versions",
            "runtime_fingerprint",
        }
        unknown = sorted(str(key) for key in value if str(key) not in allowed)
        if unknown:
            raise RuntimeCompatibilityError(f"unknown runtime identity fields: {unknown}")
        protocols = value.get("protocol_versions")
        schemas = value.get("schema_versions")
        if not isinstance(protocols, Mapping) or not isinstance(schemas, Mapping):
            raise RuntimeCompatibilityError("runtime identity versions must be objects")
        if isinstance(value.get("schema_version"), bool) or not isinstance(value.get("schema_version"), int):
            raise RuntimeCompatibilityError("runtime identity schema_version must be an integer")
        required_strings = (
            "theseus_version",
            "python_version",
            "python_implementation",
            "platform",
            "architecture",
            "runtime_fingerprint",
        )
        if any(not isinstance(value.get(name), str) or not value.get(name).strip() for name in required_strings):
            raise RuntimeCompatibilityError("runtime identity string fields must be non-empty strings")
        if any(
            not isinstance(key, str)
            or isinstance(item, bool)
            or not isinstance(item, int)
            for key, item in (*protocols.items(), *schemas.items())
        ):
            raise RuntimeCompatibilityError("runtime identity versions must contain integers")
        return cls(
            schema_version=value["schema_version"],
            theseus_version=value["theseus_version"],
            python_version=value["python_version"],
            python_implementation=value["python_implementation"],
            platform=value["platform"],
            architecture=value["architecture"],
            protocol_versions=dict(protocols),
            schema_versions=dict(schemas),
            runtime_fingerprint=value["runtime_fingerprint"],
        )


def current_runtime_identity(
    *,
    theseus_version: str | None = None,
    python_version: str | None = None,
    protocol_versions: Mapping[str, int] | None = None,
    schema_versions: Mapping[str, int] | None = None,
) -> RuntimeIdentity:
    """Build the identity for the currently imported installation."""

    protocols = dict(
        protocol_versions
        or {
            "core": PROTOCOL_VERSION,
            "worker": WORKER_PROTOCOL_VERSION,
            "remote_execution": 1,
        }
    )
    schemas = dict(
        schema_versions
        or {
            "core": SCHEMA_VERSION,
            "worker": WORKER_SCHEMA_VERSION,
            "remote_execution": 1,
        }
    )
    payload = _fingerprint_payload(
        theseus_version=str(theseus_version or _distribution_version()),
        python_version=str(python_version or platform.python_version()),
        python_implementation=platform.python_implementation(),
        platform_name=platform.system().lower() or sys.platform,
        architecture=platform.machine() or "unknown",
        protocol_versions=protocols,
        schema_versions=schemas,
    )
    return RuntimeIdentity(
        schema_version=RUNTIME_IDENTITY_SCHEMA_VERSION,
        theseus_version=payload["theseus_version"],
        python_version=payload["python_version"],
        python_implementation=payload["python_implementation"],
        platform=payload["platform"],
        architecture=payload["architecture"],
        protocol_versions=payload["protocol_versions"],
        schema_versions=payload["schema_versions"],
        runtime_fingerprint=_fingerprint(payload),
    )


def assert_runtime_compatible(
    expected: RuntimeIdentity,
    actual: RuntimeIdentity,
    *,
    require_exact_version: bool = True,
) -> None:
    """Reject incompatible coordinator/worker or persisted-state identities."""

    if require_exact_version and expected.theseus_version != actual.theseus_version:
        raise RuntimeCompatibilityError(
            "incompatible Theseus versions: "
            f"expected={expected.theseus_version}; actual={actual.theseus_version}"
        )
    if expected.protocol_versions != actual.protocol_versions:
        raise RuntimeCompatibilityError(
            "incompatible protocol versions: "
            f"expected={dict(expected.protocol_versions)!r}; actual={dict(actual.protocol_versions)!r}"
        )
    if expected.schema_versions != actual.schema_versions:
        raise RuntimeCompatibilityError(
            "incompatible schema versions: "
            f"expected={dict(expected.schema_versions)!r}; actual={dict(actual.schema_versions)!r}"
        )


__all__ = [
    "RUNTIME_IDENTITY_SCHEMA_VERSION",
    "RuntimeCompatibilityError",
    "RuntimeIdentity",
    "assert_runtime_compatible",
    "current_runtime_identity",
]
