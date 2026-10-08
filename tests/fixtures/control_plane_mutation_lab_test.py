from __future__ import annotations

import pytest

from control_plane_mutation_lab import (
    require_hash,
    require_identity,
    require_live_lease,
    require_schema,
    validate_candidate,
)


def test_schema_accepts_only_current_version() -> None:
    assert require_schema(1)
    with pytest.raises(ValueError):
        require_schema(2)


def test_hash_and_identity_reject_spoofed_bindings() -> None:
    assert require_hash("digest", "digest")
    with pytest.raises(ValueError):
        require_hash("digest", "spoofed")
    assert require_identity("attempt-1", "attempt-1")
    with pytest.raises(ValueError):
        require_identity("attempt-1", "attempt-2")


def test_lease_fence_rejects_expiry_boundary() -> None:
    assert require_live_lease(11.0, 10.0)
    with pytest.raises(ValueError):
        require_live_lease(10.0, 10.0)


def test_candidate_boundary_rejects_escape_and_non_python() -> None:
    assert validate_candidate("D:/project/app.py", "D:/project")
    for value in ("D:/project/../secret.py", "D:/other/app.py", "D:/project/app.txt"):
        with pytest.raises(ValueError):
            validate_candidate(value, "D:/project")
