"""Small mutation-gate laboratory for the control-plane invariants."""

from pathlib import PurePosixPath

SCHEMA_VERSION = 1


def require_schema(version: int) -> bool:
    if version != SCHEMA_VERSION:
        raise ValueError("unsupported schema")
    return True


def require_hash(actual: str, expected: str) -> bool:
    if actual != expected:
        raise ValueError("hash mismatch")
    return True


def require_live_lease(expires_at: float, now: float) -> bool:
    if expires_at <= now:
        raise ValueError("lease expired")
    return True


def require_identity(actual: str, expected: str) -> bool:
    if actual != expected:
        raise ValueError("identity mismatch")
    return True


def validate_candidate(path: str, root: str) -> bool:
    normalized = path.replace("\\", "/")
    prefix = root.rstrip("/") + "/"
    candidate = PurePosixPath(normalized)
    if (
        ".." in candidate.parts
        or not normalized.startswith(prefix)
        or not normalized.endswith(".py")
    ):
        raise ValueError("candidate escapes source boundary")
    return True
