"""Capability proof and safe fallback selection for local execution backends."""

from __future__ import annotations

import os
import platform
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class LocalBackendCapabilities:
    """Observed local capabilities; unsupported optimizations remain explicitly disabled."""

    schema_version: int
    platform: str
    architecture: str
    process_backend: str
    supports_process_tree_kill: bool
    supports_hardlink_cow: bool
    supports_native_clone: bool
    supports_immutable_mutant_artifacts: bool
    import_time_activation: str
    workspace_backends: tuple[str, ...]
    no_go_reasons: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        # Serialize one capability proof without exposing probe implementation state.
        return {
            "schema_version": self.schema_version,
            "platform": self.platform,
            "architecture": self.architecture,
            "process_backend": self.process_backend,
            "supports_process_tree_kill": self.supports_process_tree_kill,
            "supports_hardlink_cow": self.supports_hardlink_cow,
            "supports_native_clone": self.supports_native_clone,
            "supports_immutable_mutant_artifacts": self.supports_immutable_mutant_artifacts,
            "import_time_activation": self.import_time_activation,
            "workspace_backends": list(self.workspace_backends),
            "no_go_reasons": list(self.no_go_reasons),
        }


def _probe_hardlink(root: Path | None) -> bool:
    # Prove hardlink support in a dedicated disposable probe directory, never in the project checkout.
    temporary_root = None
    try:
        if root is None:
            temporary_root = tempfile.TemporaryDirectory(prefix="theseus-capability-")
            probe_root = Path(temporary_root.name)
        else:
            probe_root = Path(root).resolve()
            probe_root.mkdir(parents=True, exist_ok=True)
            temporary_root = tempfile.TemporaryDirectory(prefix=".theseus-capability-", dir=str(probe_root))
            probe_root = Path(temporary_root.name)
        source = probe_root / "source.bin"
        link = probe_root / "link.bin"
        source.write_bytes(b"theseus-capability-proof\n")
        os.link(source, link)
        return link.is_file() and os.path.samefile(source, link)
    except (OSError, ValueError):
        return False
    finally:
        if temporary_root is not None:
            temporary_root.cleanup()


def detect_local_capabilities(probe_root: Path | None = None) -> LocalBackendCapabilities:
    # Detect only capabilities with an observable proof and fail closed for unproven optimizations.
    hardlink = _probe_hardlink(probe_root)
    tree_kill = os.name != "nt" or shutil.which("taskkill") is not None
    reasons = (
        "native filesystem clone is not enabled without a platform-specific clone proof",
        "import-time activation is disabled because equivalence and cleanup cannot be proven here",
    )
    return LocalBackendCapabilities(
        schema_version=1,
        platform=platform.system().lower() or "unknown",
        architecture=platform.machine() or "unknown",
        process_backend="local-process",
        supports_process_tree_kill=tree_kill,
        supports_hardlink_cow=hardlink,
        supports_native_clone=False,
        supports_immutable_mutant_artifacts=True,
        import_time_activation="no-go",
        workspace_backends=("hardlink-cow", "copy") if hardlink else ("copy",),
        no_go_reasons=reasons,
    )


def select_workspace_backend(
    capabilities: LocalBackendCapabilities,
    preferred: str | None = None,
) -> str:
    # Select an observed backend or the conservative copy fallback without guessing host support.
    requested = str(preferred or "hardlink-cow").strip().lower()
    if requested == "hardlink-cow" and capabilities.supports_hardlink_cow:
        return "hardlink-cow"
    if requested == "copy" or requested == "hardlink-cow":
        return "copy"
    raise ValueError(f"unsupported workspace backend request: {preferred}")


def validate_backend_selection(
    capabilities: LocalBackendCapabilities,
    selected: str,
) -> None:
    # Refuse a persisted backend claim that is not supported by the current capability proof.
    normalized = str(selected).strip().lower()
    if normalized not in capabilities.workspace_backends:
        raise RuntimeError(
            "workspace backend is unavailable: "
            f"selected={selected}; available={capabilities.workspace_backends}"
        )


__all__ = [
    "LocalBackendCapabilities",
    "detect_local_capabilities",
    "select_workspace_backend",
    "validate_backend_selection",
]
