from pathlib import Path
import pytest
from test_intelligence_unified_v1.io_utils import atomic_write_json
from theseus_local import worker_pool as worker_pool_module
from theseus_local.worker_pool import prepare_worker_workspace
from theseus_local.workspace import WorkspaceProvider
def _campaign_workspace(tmp_path: Path, campaign_id: str = "campaign-pr49") -> tuple[Path, str]:
    # Create one coordinator-shaped workspace and ownership manifest with a durable content identity.
    source = tmp_path / "state" / "workspaces" / campaign_id
    source.mkdir(parents=True)
    (source / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
    fingerprint = worker_pool_module._tree_fingerprint(source)
    atomic_write_json(
        source.parent / f"{campaign_id}.ownership.json",
        {
            "schema_version": 1,
            "campaign_id": campaign_id,
            "main_root": str(tmp_path / "checkout"),
            "source_fingerprint": "main-root-fixture",
            "workspace_fingerprint": fingerprint,
            "mode": "copy",
        },
        durability="critical",
        category="workspace_manifest",
    )
    return source, fingerprint
def test_worker_workspace_validates_campaign_identity_without_destination_rescan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Revalidate source identity once while avoiding a redundant content scan through the fresh hardlink destination.
    campaign_id = "campaign-pr49"
    source, expected_fingerprint = _campaign_workspace(tmp_path, campaign_id)
    destination = tmp_path / "workers" / "worker-000" / "workspace"
    original = worker_pool_module._tree_fingerprint
    calls: list[Path] = []
    def counting_fingerprint(path: Path) -> str:
        # Record every full-tree scan performed after the campaign identity has already been established.
        calls.append(Path(path).resolve())
        return original(path)
    monkeypatch.setattr(worker_pool_module, "_tree_fingerprint", counting_fingerprint)
    prepared = prepare_worker_workspace(
        source,
        destination,
        worker_id="worker-000",
        campaign_id=campaign_id,
    )
    assert prepared == destination.resolve()
    assert calls == [source.resolve()]
    assert original(destination) == expected_fingerprint
def test_worker_workspace_rejects_source_drift_against_campaign_identity(tmp_path: Path) -> None:
    # Fail closed when copied bytes no longer match the campaign workspace fingerprint already committed by the coordinator.
    campaign_id = "campaign-pr49"
    source, _ = _campaign_workspace(tmp_path, campaign_id)
    (source / "app.py").write_text("VALUE = 2\n", encoding="utf-8")
    destination = tmp_path / "workers" / "worker-000" / "workspace"
    with pytest.raises(ValueError, match="does not match campaign workspace identity"):
        prepare_worker_workspace(
            source,
            destination,
            worker_id="worker-000",
            campaign_id=campaign_id,
        )
    assert not destination.exists()
    assert not (destination.parent / f"{destination.name}.ownership.json").exists()
def test_worker_workspace_without_campaign_manifest_hashes_source_once_for_hardlink_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Establish one source identity for a fresh pure-hardlink tree without rereading the same bytes through destination.
    source = tmp_path / "plain-source"
    source.mkdir()
    (source / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
    destination = tmp_path / "workers" / "worker-000" / "workspace"
    original = worker_pool_module._tree_fingerprint
    calls: list[Path] = []
    def counting_fingerprint(path: Path) -> str:
        # Record legacy fallback scans without changing their fingerprint semantics.
        calls.append(Path(path).resolve())
        return original(path)
    monkeypatch.setattr(worker_pool_module, "_tree_fingerprint", counting_fingerprint)
    prepare_worker_workspace(
        source,
        destination,
        worker_id="worker-000",
        campaign_id="campaign-pr49",
    )
    assert calls == [source.resolve()]

def test_worker_workspace_copy_fallback_still_verifies_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Keep a destination content proof when the filesystem forces physical file copies instead of hardlinks.
    source = tmp_path / "copy-source"
    source.mkdir()
    (source / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
    destination = tmp_path / "workers" / "worker-copy" / "workspace"
    original = worker_pool_module._tree_fingerprint
    calls: list[Path] = []
    def counting_fingerprint(path: Path) -> str:
        # Record the source proof and the additional copied-destination proof.
        calls.append(Path(path).resolve())
        return original(path)
    def forced_copy(source_root: Path, destination_root: Path) -> tuple[int, int]:
        # Simulate a filesystem where hardlinks are unavailable while preserving real copy semantics.
        WorkspaceProvider._copy_tree(source_root, destination_root)
        return 0, 1
    monkeypatch.setattr(worker_pool_module, "_tree_fingerprint", counting_fingerprint)
    monkeypatch.setattr(WorkspaceProvider, "_hardlink_tree", staticmethod(forced_copy))
    prepare_worker_workspace(
        source,
        destination,
        worker_id="worker-copy",
        campaign_id="campaign-copy",
    )
    assert calls == [source.resolve(), destination.resolve()]
