from __future__ import annotations

import json
from pathlib import Path

import pytest

from theseus_contracts.collection import CollectionSnapshot
from theseus_contracts.serialization import SerializationError
from theseus_local.artifact_store import ArtifactIntegrityError, ContentAddressedArtifactStore
from theseus_local.distributed import DistributedScheduler, SchedulerError
from theseus_local.remote_worker import RemoteWorkerError, RemoteWorkerRuntime
from theseus_local.runtime_identity import current_runtime_identity

from post_pr63_helpers import remote_fixture


def _rewrite_json(path: Path, **changes: object) -> None:
    value = json.loads(path.read_text(encoding="utf-8"))
    value.update(changes)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_collection_snapshot_rejects_unknown_collection_mode() -> None:
    payload = {
        "collection_snapshot_id": "snapshot-1",
        "repository_revision": None,
        "environment_fingerprint": "env-1",
        "pytest_version": None,
        "plugin_fingerprint": "plugins-1",
        "pytest_configuration_fingerprint": "config-1",
        "nodeids": [],
        "collection_errors": [],
    }
    assert CollectionSnapshot.from_dict(payload).collection_mode == "pytest"
    assert CollectionSnapshot.from_dict(payload | {"collection_mode": "static_index"}).collection_mode == "static_index"
    with pytest.raises(SerializationError, match="collection_mode"):
        CollectionSnapshot.from_dict(payload | {"collection_mode": "spoofed"})


def test_scheduler_rejects_missing_future_and_invalid_persisted_schema(tmp_path: Path) -> None:
    state = tmp_path / "scheduler.json"
    scheduler = DistributedScheduler(
        state,
        expected_runtime_identity=current_runtime_identity(),
    )
    scheduler._save()

    _rewrite_json(state, schema_version=99)
    with pytest.raises(SchedulerError, match="corrupt"):
        DistributedScheduler(state)

    state.write_text(json.dumps({"workers": {}, "pending": {}}), encoding="utf-8")
    with pytest.raises(SchedulerError, match="corrupt"):
        DistributedScheduler(state)

    state.write_text(json.dumps({"schema_version": 1, "workers": []}), encoding="utf-8")
    with pytest.raises(SchedulerError, match="corrupt"):
        DistributedScheduler(state)

    scheduler._save()
    _rewrite_json(state, runtime_identity={"runtime_fingerprint": "spoofed"})
    with pytest.raises(SchedulerError, match="corrupt"):
        DistributedScheduler(state, expected_runtime_identity=current_runtime_identity())

    scheduler._save()
    _rewrite_json(
        state,
        runtime_identity=current_runtime_identity(theseus_version="99.0.0").to_dict(),
    )
    with pytest.raises(SchedulerError, match="corrupt"):
        DistributedScheduler(state, expected_runtime_identity=current_runtime_identity())


def test_worker_journal_rejects_schema_and_runtime_drift(tmp_path: Path) -> None:
    _, request, worker = remote_fixture(tmp_path)
    worker.execute(request)
    journal = tmp_path / "worker" / "worker-results.json"

    _rewrite_json(journal, schema_version=99)
    with pytest.raises(RemoteWorkerError, match="corrupt"):
        RemoteWorkerRuntime(worker_id="worker-0", root=tmp_path / "worker")

    worker.execute(request)
    _rewrite_json(journal, runtime_fingerprint="spoofed")
    with pytest.raises(RemoteWorkerError, match="corrupt"):
        RemoteWorkerRuntime(worker_id="worker-0", root=tmp_path / "worker")


def test_artifact_index_rejects_unknown_schema_version(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    store = ContentAddressedArtifactStore(root)
    store.put_bytes(b"stable")
    index = root / "index.json"
    _rewrite_json(index, schema_version=99)

    with pytest.raises(ArtifactIntegrityError, match="corrupt"):
        ContentAddressedArtifactStore(root)
