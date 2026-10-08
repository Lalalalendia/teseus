from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import subprocess
import sys
from threading import Barrier, Event

import pytest

from theseus_local.artifact_store import ArtifactIntegrityError, ContentAddressedArtifactStore, ProjectSnapshot

from post_pr63_helpers import assert_artifact_verified, remote_fixture


def test_content_identity_ignores_path_and_mtime(tmp_path: Path) -> None:
    first = ContentAddressedArtifactStore(tmp_path / "first")
    second = ContentAddressedArtifactStore(tmp_path / "second")
    record_a = first.put_bytes(b"same-content")
    record_b = second.put_bytes(b"same-content")
    assert record_a.artifact_id == record_b.artifact_id
    assert record_a.size_bytes == record_b.size_bytes
    assert_artifact_verified(first, record_a.artifact_id)
    assert_artifact_verified(second, record_b.artifact_id)


def test_corrupt_existing_cache_is_detected_and_never_counted_as_hit(tmp_path: Path) -> None:
    source = ContentAddressedArtifactStore(tmp_path / "source")
    destination = ContentAddressedArtifactStore(tmp_path / "destination")
    record = source.put_bytes(b"correct")
    destination.put_bytes(b"correct")
    destination.path_for(record.artifact_id, verify=False).write_bytes(b"wrong")
    stats = destination.transfer_from(source, (record.artifact_id,))
    assert stats.cache_hits == 0
    assert stats.cache_misses == 1
    assert destination.get_bytes(record.artifact_id) == b"correct"


def test_truncated_upload_is_not_published(tmp_path: Path) -> None:
    store = ContentAddressedArtifactStore(tmp_path / "cas")
    source = tmp_path / "source.bin"
    source.write_bytes(b"payload")
    with pytest.raises(OSError):
        from unittest.mock import patch

        with patch("theseus_local.artifact_store.os.replace", side_effect=OSError("rename failed")):
            store.put_file(source)
    assert len(store.records()) == 0
    assert not tuple(store.root.glob("*.part"))


def test_concurrent_publication_leaves_one_valid_immutable_artifact(tmp_path: Path) -> None:
    root = tmp_path / "shared-cas"
    stores = [ContentAddressedArtifactStore(root), ContentAddressedArtifactStore(root)]
    payload = b"x" * (1024 * 1024)
    with ThreadPoolExecutor(max_workers=8) as pool:
        records = tuple(pool.map(lambda index: stores[index % 2].put_bytes(payload), range(8)))
    artifact_id = records[0].artifact_id
    assert all(item.artifact_id == artifact_id for item in records)
    reopened = ContentAddressedArtifactStore(root)
    assert reopened.get_bytes(artifact_id) == payload
    assert len(reopened.records()) == 1


def test_cross_process_publication_merges_pins_without_lost_updates(tmp_path: Path) -> None:
    root = tmp_path / "cross-process-cas"
    script = (
        "from pathlib import Path; "
        "import sys; "
        "from theseus_local.artifact_store import ContentAddressedArtifactStore; "
        "ContentAddressedArtifactStore(Path(sys.argv[1])).put_bytes(b'process-shared', pin=True)"
    )
    environment = dict(os.environ)
    processes = [
        subprocess.Popen(
            (sys.executable, "-c", script, str(root)),
            cwd=str(Path(__file__).resolve().parents[1]),
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(2)
    ]
    results = [process.communicate(timeout=30) for process in processes]
    assert all(process.returncode == 0 for process in processes), results
    reopened = ContentAddressedArtifactStore(root)
    record = reopened.verify(__import__("hashlib").sha256(b"process-shared").hexdigest())
    assert record.pinned == 2
    assert reopened.get_bytes(record.artifact_id) == b"process-shared"


def test_readers_never_accept_partially_published_bytes(tmp_path: Path) -> None:
    store = ContentAddressedArtifactStore(tmp_path / "cas")
    payload = b"immutable" * 100_000
    artifact_id = __import__("hashlib").sha256(payload).hexdigest()
    started = Event()

    def publish() -> str:
        started.set()
        return store.put_bytes(payload).artifact_id

    with ThreadPoolExecutor(max_workers=8) as pool:
        future = pool.submit(publish)
        started.wait(timeout=1)
        observations = []
        while not future.done():
            try:
                observations.append(store.get_bytes(artifact_id))
            except ArtifactIntegrityError:
                observations.append(None)
        assert future.result() == artifact_id
    assert all(item in (None, payload) for item in observations)
    assert store.get_bytes(artifact_id) == payload


def test_pin_protects_artifact_until_terminal_release(tmp_path: Path) -> None:
    store = ContentAddressedArtifactStore(tmp_path / "cas")
    pinned = store.put_bytes(b"pinned", pin=True)
    removed = store.evict(max_bytes=0)
    assert pinned.artifact_id not in removed
    released = store.release(pinned.artifact_id)
    assert released.pinned == 0
    assert pinned.artifact_id in store.evict(max_bytes=0)


def test_concurrent_pin_and_evict_has_no_unpinned_delete_or_phantom_pin(tmp_path: Path) -> None:
    root = tmp_path / "pin-evict-race"
    seed = ContentAddressedArtifactStore(root)
    record = seed.put_bytes(b"race-payload")
    pinning = ContentAddressedArtifactStore(root)
    evicting = ContentAddressedArtifactStore(root)
    barrier = Barrier(2)

    def pin() -> str:
        barrier.wait()
        try:
            return f"pinned:{pinning.pin(record.artifact_id).pinned}"
        except ArtifactIntegrityError:
            return "removed-before-pin"

    def evict() -> str:
        barrier.wait()
        return "evicted" if record.artifact_id in evicting.evict(max_bytes=0) else "protected"

    with ThreadPoolExecutor(max_workers=2) as pool:
        pin_result, evict_result = tuple(pool.map(lambda fn: fn(), (pin, evict)))
    reopened = ContentAddressedArtifactStore(root)
    if pin_result.startswith("pinned"):
        assert evict_result == "protected"
        assert reopened.verify(record.artifact_id).pinned == 1
        reopened.release(record.artifact_id)
        assert record.artifact_id in reopened.evict(max_bytes=0)
    else:
        assert evict_result == "evicted"
        assert not reopened.has(record.artifact_id)


def test_persisted_pin_has_an_explicit_recovery_path(tmp_path: Path) -> None:
    root = tmp_path / "cas"
    store = ContentAddressedArtifactStore(root)
    record = store.put_bytes(b"recoverable", pin=True)
    restarted = ContentAddressedArtifactStore(root)
    assert restarted.verify(record.artifact_id).pinned == 1
    assert restarted.release(record.artifact_id).pinned == 0
    assert record.artifact_id in restarted.evict(max_bytes=0)


def test_snapshot_corruption_fails_before_remote_execution(tmp_path: Path) -> None:
    store, request, worker = remote_fixture(tmp_path)
    snapshot = store.load_snapshot(request.project_snapshot_id)
    file_id = next(iter(snapshot.files.values()))
    store.path_for(file_id, verify=False).write_bytes(b"corrupt snapshot file")
    result = worker.execute(request)
    assert result.started is False
    assert result.workspace_integrity == "failed"
    assert "snapshot" in (result.diagnostic_error or "").lower() or "artifact" in (result.diagnostic_error or "").lower()


def test_wrong_snapshot_with_valid_prepared_artifact_is_rejected(tmp_path: Path) -> None:
    store, request, worker = remote_fixture(tmp_path)
    original_snapshot = store.load_snapshot(request.project_snapshot_id)
    wrong_project = tmp_path / "wrong-project"
    wrong_project.mkdir()
    (wrong_project / "module.py").write_text("VALUE = 999\n", encoding="utf-8")
    wrong_snapshot = store.create_snapshot(wrong_project)
    store.persist_snapshot(wrong_snapshot)
    from dataclasses import replace

    mismatch = replace(
        request,
        project_snapshot_id=wrong_snapshot.snapshot_id,
        source_path="module.py",
        expected_source_sha256=original_snapshot.files["module.py"],
    )
    result = worker.execute(mismatch)
    assert result.started is False
    assert result.workspace_integrity == "failed"
    assert "source" in (result.diagnostic_error or "").lower()


def test_snapshot_path_traversal_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ArtifactIntegrityError):
        ProjectSnapshot.from_dict(
            {
                "snapshot_id": "0" * 64,
                "files": {"../escape.py": "0" * 64},
                "total_size_bytes": 1,
            }
        )
