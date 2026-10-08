from __future__ import annotations

import errno
import json
import os
import shutil
from pathlib import Path

import pytest

import theseus_local.worker_pool as worker_pool_module
from theseus_local.capabilities import LocalBackendCapabilities
from theseus_local.worker_pool import prepare_worker_workspace, worker_workspace_backend
from theseus_local.workspace import WorkspaceProvider
from theseus_performance.project_benchmark import _physical_process_spawn_count, _physical_process_spawn_evidence


def _capabilities(*, hardlink: bool) -> LocalBackendCapabilities:
    # Build a deterministic capability proof for workspace selection tests.
    return LocalBackendCapabilities(
        schema_version=1,
        platform="test",
        architecture="test",
        process_backend="local-process",
        supports_process_tree_kill=True,
        supports_hardlink_cow=hardlink,
        supports_native_clone=False,
        supports_immutable_mutant_artifacts=True,
        import_time_activation="no-go",
        workspace_backends=("hardlink-cow", "copy") if hardlink else ("copy",),
    )


def _project(root: Path) -> None:
    # Keep two files so a mixed materialization can be distinguished from a proven hardlink tree.
    root.mkdir(parents=True)
    (root / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
    (root / "data.txt").write_text("baseline\n", encoding="utf-8")


def test_workspace_rebuilds_partial_hardlink_tree_as_copy_before_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Never label a partially linked tree as hardlink-COW; rebuild it before any external process can run.
    source = tmp_path / "campaign"
    destination = tmp_path / "worker" / "workspace"
    _project(source)

    def mixed_tree(source_path: Path, destination_path: Path) -> tuple[int, int]:
        # Simulate one file link succeeding while another file falls back to a copy.
        destination_path.mkdir(parents=True)
        os.link(source_path / "app.py", destination_path / "app.py")
        shutil.copy2(source_path / "data.txt", destination_path / "data.txt")
        return 1, 1

    monkeypatch.setattr(worker_pool_module, "detect_local_capabilities", lambda root: _capabilities(hardlink=True))
    monkeypatch.setattr(WorkspaceProvider, "_hardlink_tree", staticmethod(mixed_tree))
    prepare_worker_workspace(source, destination, worker_id="worker-000", campaign_id="campaign-1")

    assert worker_workspace_backend(destination) == "copy"
    assert not os.path.samefile(source / "app.py", destination / "app.py")
    assert not os.path.samefile(source / "data.txt", destination / "data.txt")
    manifest = json.loads((destination.parent / "workspace.ownership.json").read_text(encoding="utf-8"))
    assert manifest["linked_files"] == 0
    assert manifest["capability_proof"]["selected_workspace_backend"] == "copy"


def test_workspace_selects_copy_before_materialization_when_hardlink_is_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An unavailable capability must prevent the hardlink materializer from being called at all.
    source = tmp_path / "campaign-unavailable"
    destination = tmp_path / "worker-unavailable" / "workspace"
    _project(source)

    def unexpected_hardlink(*args: object, **kwargs: object) -> tuple[int, int]:
        # Fail if selection ever attempts an unproven optimization.
        del args, kwargs
        raise AssertionError("unavailable hardlink backend was materialized")

    monkeypatch.setattr(worker_pool_module, "detect_local_capabilities", lambda root: _capabilities(hardlink=False))
    monkeypatch.setattr(WorkspaceProvider, "_hardlink_tree", staticmethod(unexpected_hardlink))
    prepare_worker_workspace(source, destination, worker_id="worker-001", campaign_id="campaign-2")

    assert worker_workspace_backend(destination) == "copy"
    assert not os.path.samefile(source / "app.py", destination / "app.py")


def test_workspace_materialization_error_falls_back_before_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Treat a race between capability probing and linking as a pre-attempt copy fallback.
    source = tmp_path / "campaign-race"
    destination = tmp_path / "worker-race" / "workspace"
    _project(source)

    def materialization_error(source_path: Path, destination_path: Path) -> tuple[int, int]:
        # Leave a partial destination so cleanup is also exercised.
        destination_path.mkdir(parents=True)
        raise OSError(errno.EXDEV, "cross-device link")

    monkeypatch.setattr(worker_pool_module, "detect_local_capabilities", lambda root: _capabilities(hardlink=True))
    monkeypatch.setattr(WorkspaceProvider, "_hardlink_tree", staticmethod(materialization_error))
    prepare_worker_workspace(source, destination, worker_id="worker-002", campaign_id="campaign-3")

    assert worker_workspace_backend(destination) == "copy"
    assert (destination / "app.py").read_bytes() == (source / "app.py").read_bytes()
    assert not os.path.samefile(source / "app.py", destination / "app.py")


def test_benchmark_spawn_metric_aggregates_all_authoritative_shards(tmp_path: Path) -> None:
    # Count physical shard launches across workers while excluding per-process timing sidecars.
    campaign_root = tmp_path / "mutants-24" / "workers-4" / "state" / "workers" / "campaign-1"
    first = campaign_root / "local-worker-000" / "attempt-0" / "reports" / "engine" / "campaign-1"
    second = campaign_root / "local-worker-001" / "attempt-0" / "reports" / "engine" / "campaign-1"
    first.mkdir(parents=True)
    second.mkdir(parents=True)
    def payload(count: int) -> dict[str, dict[str, dict[str, int]]]:
        # Keep the fake report shape identical to the runner performance adapter contract.
        return {"metrics": {"performance": {"processes_started": count}}}
    first_report = first / "engine-shard-000-attempt-0.json"
    first_report.write_text(json.dumps(payload(12)), encoding="utf-8")
    (second / "engine-shard-001-attempt-0.json").write_text(json.dumps(payload(12)), encoding="utf-8")
    (first / "engine-shard-000-attempt-0.abc.performance.json").write_text(
        json.dumps(payload(999)),
        encoding="utf-8",
    )

    assert _physical_process_spawn_count(str(first_report)) == 24


def test_benchmark_spawn_metric_prefers_per_process_evidence_across_retries(tmp_path: Path) -> None:
    # Count every physical pytest launch across attempts, even when only the final shard report is retained.
    campaign_root = tmp_path / "mutants-100" / "workers-1" / "state" / "workers" / "campaign-2"
    attempt = campaign_root / "local-worker-000" / "attempt-0" / "reports" / "engine" / "campaign-2"
    events = attempt / "test_stats_events" / "engine-shard-000-attempt-0"
    events.mkdir(parents=True)
    for index in range(4):
        (events / f"pytest-{index}.performance.json").write_text("{}", encoding="utf-8")
    report = attempt / "engine-shard-000-attempt-0.json"
    report.write_text(
        json.dumps({"metrics": {"performance": {"processes_started": 1}}}),
        encoding="utf-8",
    )

    assert _physical_process_spawn_count(str(report)) == 4
    assert _physical_process_spawn_evidence(str(report)) == (
        4,
        "project-benchmark.aggregate-test-stats.processes_started",
    )
