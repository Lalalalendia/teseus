"""Release/install helpers for the PR54 version and upgrade contract."""

from __future__ import annotations

import hashlib
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from theseus_contracts.serialization import dumps, loads_object
from .runtime_identity import RuntimeIdentity, RuntimeCompatibilityError, current_runtime_identity


@dataclass(frozen=True, slots=True)
class DistributionArtifact:
    path: str
    sha256: str
    size_bytes: int
    runtime_identity: RuntimeIdentity

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "sha256": self.sha256,
            "size_bytes": int(self.size_bytes),
            "runtime_identity": self.runtime_identity.to_dict(),
        }


def sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def describe_artifact(path: Path, *, runtime_identity: RuntimeIdentity | None = None) -> DistributionArtifact:
    """Describe a built wheel/sdist without treating a checkout commit as its identity."""

    target = Path(path).resolve()
    if not target.is_file() or target.suffix.lower() not in {".whl", ".gz", ".zip"}:
        raise ValueError(f"unsupported or missing distribution artifact: {target}")
    digest, size = sha256_file(target)
    return DistributionArtifact(
        path=str(target),
        sha256=digest,
        size_bytes=size,
        runtime_identity=runtime_identity or current_runtime_identity(),
    )


def write_installation_identity(path: Path, identity: RuntimeIdentity | None = None) -> RuntimeIdentity:
    """Record the installed runtime identity atomically for future upgrade checks."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    resolved = identity or current_runtime_identity()
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(dumps(resolved.to_dict()) + "\n", encoding="utf-8", newline="\n")
    os.replace(temporary, target)
    return resolved


def read_installation_identity(path: Path) -> RuntimeIdentity:
    try:
        value = loads_object(Path(path).read_text(encoding="utf-8"))
        return RuntimeIdentity.from_dict(value)
    except (OSError, ValueError, TypeError) as exc:
        raise RuntimeCompatibilityError(f"installation identity is missing or corrupt: {path}") from exc


def require_installation_compatibility(path: Path, expected: RuntimeIdentity | None = None) -> RuntimeIdentity:
    """Fail explicitly when an upgrade leaves incompatible state behind."""

    actual = read_installation_identity(path)
    current = expected or current_runtime_identity()
    if actual.runtime_fingerprint != current.runtime_fingerprint:
        raise RuntimeCompatibilityError(
            "installed runtime identity differs from durable state: "
            f"expected={current.runtime_fingerprint}; actual={actual.runtime_fingerprint}"
        )
    return actual


__all__ = [
    "DistributionArtifact",
    "describe_artifact",
    "read_installation_identity",
    "require_installation_compatibility",
    "sha256_file",
    "write_installation_identity",
]
