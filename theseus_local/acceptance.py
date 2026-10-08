"""Acceptance and quiescence helpers for the PR60 distributed gate."""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from test_intelligence_unified_v1.commands import active_child_process_ids
from .process import EngineProcessSession
from .remote_campaign import compare_execution_semantics
from .worker_runtime.process import PersistentWorkerProcess
from theseus_contracts import RemoteExecutionResult


class AcceptanceFailure(RuntimeError):
    """Raised when a distributed acceptance invariant is not satisfied."""


@dataclass(frozen=True, slots=True)
class ResourceQuiescence:
    child_process_ids: tuple[int, ...]
    engine_process_ids: tuple[int, ...]
    worker_process_ids: tuple[int, ...]
    worker_lifecycle_threads: tuple[str, ...]

    @property
    def clean(self) -> bool:
        return not any(
            (
                self.child_process_ids,
                self.engine_process_ids,
                self.worker_process_ids,
                self.worker_lifecycle_threads,
            )
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "child_process_ids": list(self.child_process_ids),
            "engine_process_ids": list(self.engine_process_ids),
            "worker_process_ids": list(self.worker_process_ids),
            "worker_lifecycle_threads": list(self.worker_lifecycle_threads),
            "clean": self.clean,
        }


def resource_quiescence() -> ResourceQuiescence:
    """Read in-process lifecycle registries used by the supported process backends."""

    return ResourceQuiescence(
        child_process_ids=tuple(sorted(int(item) for item in active_child_process_ids())),
        engine_process_ids=tuple(sorted(int(item) for item in EngineProcessSession.active_process_ids())),
        worker_process_ids=tuple(sorted(int(item) for item in PersistentWorkerProcess.active_process_ids())),
        worker_lifecycle_threads=PersistentWorkerProcess.active_lifecycle_thread_names(),
    )


def assert_resource_quiescence() -> None:
    snapshot = resource_quiescence()
    if not snapshot.clean:
        raise AcceptanceFailure(f"execution resources remain live: {snapshot.to_dict()}")


def process_resource_snapshot() -> dict[str, int | None]:
    """Capture bounded process-local resource counters for repeated-execution gates."""

    handles: int | None = None
    descriptors: int | None = None
    if os.name == "nt":
        try:
            import ctypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            count = ctypes.c_ulong()
            if kernel32.GetProcessHandleCount(kernel32.GetCurrentProcess(), ctypes.byref(count)):
                handles = int(count.value)
        except (AttributeError, OSError):
            handles = None
    else:
        fd_root = Path("/proc/self/fd")
        if fd_root.is_dir():
            try:
                descriptors = sum(1 for _ in fd_root.iterdir())
            except OSError:
                descriptors = None
    return {
        "open_handles": handles,
        "open_descriptors": descriptors,
        "threads": len(threading.enumerate()),
    }


def assert_resource_budget(
    before: Mapping[str, int | None],
    after: Mapping[str, int | None],
    *,
    handle_slack: int = 8,
    thread_slack: int = 2,
) -> None:
    """Reject material handle/descriptor/thread growth after a soak workload."""

    for key in ("open_handles", "open_descriptors"):
        initial = before.get(key)
        final = after.get(key)
        if initial is not None and final is not None:
            if int(final) > int(initial) + int(handle_slack):
                raise AcceptanceFailure(f"resource counter grew unexpectedly: {key} {initial}->{final}")
    if int(after.get("threads") or 0) > int(before.get("threads") or 0) + int(thread_slack):
        raise AcceptanceFailure(
            f"resource counter grew unexpectedly: threads {before.get('threads')}->{after.get('threads')}"
        )


def assert_semantic_equivalence(
    local: Iterable[RemoteExecutionResult],
    remote: Iterable[RemoteExecutionResult],
) -> None:
    equivalent, differences = compare_execution_semantics(local, remote)
    if not equivalent:
        raise AcceptanceFailure(f"local/remote semantic results differ: {differences}")


def summarize_acceptance(
    *,
    reports: Mapping[str, Mapping[str, Any]],
    resource_state: ResourceQuiescence | None = None,
) -> dict[str, Any]:
    """Build a bounded PR60 acceptance projection without introducing campaign authority."""

    resources = resource_state or resource_quiescence()
    return {
        "scenarios": {str(key): dict(value) for key, value in sorted(reports.items())},
        "resource_quiescence": resources.to_dict(),
        "accepted": resources.clean and all(bool(value.get("completed", False)) for value in reports.values()),
    }


__all__ = [
    "AcceptanceFailure",
    "ResourceQuiescence",
    "assert_resource_quiescence",
    "assert_resource_budget",
    "assert_semantic_equivalence",
    "process_resource_snapshot",
    "resource_quiescence",
    "summarize_acceptance",
]
