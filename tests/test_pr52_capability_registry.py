from __future__ import annotations

from pathlib import Path

import pytest

from theseus_local.capabilities import (
    LocalBackendCapabilities,
    detect_local_capabilities,
    select_workspace_backend,
    validate_backend_selection,
)


def test_capability_probe_is_scoped_and_fails_closed_for_unproven_optimizations(tmp_path: Path) -> None:
    # Probe only a disposable state directory and keep native clone/import activation disabled without proof.
    capabilities = detect_local_capabilities(tmp_path / "capability-state")
    assert capabilities.supports_native_clone is False
    assert capabilities.import_time_activation == "no-go"
    assert capabilities.supports_immutable_mutant_artifacts is True
    assert not list((tmp_path / "capability-state").glob("*"))


def test_capability_selection_has_copy_fallback() -> None:
    # Select hardlink-COW only when the capability proof says it is available.
    unavailable = LocalBackendCapabilities(
        schema_version=1,
        platform="test",
        architecture="test",
        process_backend="local-process",
        supports_process_tree_kill=True,
        supports_hardlink_cow=False,
        supports_native_clone=False,
        supports_immutable_mutant_artifacts=True,
        import_time_activation="no-go",
        workspace_backends=("copy",),
    )
    assert select_workspace_backend(unavailable, "hardlink-cow") == "copy"
    assert select_workspace_backend(unavailable, "copy") == "copy"
    with pytest.raises(ValueError, match="unsupported workspace backend"):
        select_workspace_backend(unavailable, "native-clone")


def test_capability_validation_rejects_unavailable_persisted_backend() -> None:
    # Do not trust a stale manifest that claims an unavailable physical backend.
    capabilities = LocalBackendCapabilities(
        schema_version=1,
        platform="test",
        architecture="test",
        process_backend="local-process",
        supports_process_tree_kill=True,
        supports_hardlink_cow=False,
        supports_native_clone=False,
        supports_immutable_mutant_artifacts=True,
        import_time_activation="no-go",
        workspace_backends=("copy",),
    )
    validate_backend_selection(capabilities, "copy")
    with pytest.raises(RuntimeError, match="unavailable"):
        validate_backend_selection(capabilities, "hardlink-cow")
