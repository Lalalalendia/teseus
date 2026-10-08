from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from test_intelligence_unified_v1.io_utils import stable_hash
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    MutationScope,
    ProjectDescriptor,
    ProjectId,
    TestCommandDescriptor,
)
from theseus_local import LocalCampaignCoordinator
from theseus_local.worker_pool import materialize_prepared_snapshot, prepare_worker_workspace
from theseus_local.workspace import WorkspaceProvider


def _snapshot_payload() -> dict[str, object]:
    # Keep one compact prepared snapshot whose identity is independent of its destination path.
    return {
        "campaign_id": "campaign-pr79",
        "source_path": "app.py",
        "source_sha256": "source-pr79",
        "prepared_mutants": [],
    }


def test_prepared_snapshot_cache_reuses_verified_immutable_bytes(tmp_path: Path) -> None:
    # The first attempt publishes one cache object; later attempts link to it instead of rewriting the JSON.
    source = tmp_path / "prepared.snapshot.json"
    cache_root = tmp_path / "cache"
    first_destination = tmp_path / "worker-1" / "prepared.snapshot.json"
    second_destination = tmp_path / "worker-2" / "prepared.snapshot.json"
    payload = _snapshot_payload()
    source.write_text(json.dumps(payload), encoding="utf-8")
    expected_id = stable_hash(payload)[:32]

    first_metrics: dict[str, float] = {}
    second_metrics: dict[str, float] = {}
    materialize_prepared_snapshot(
        source,
        first_destination,
        expected_snapshot_id=expected_id,
        cache_root=cache_root,
        metrics=first_metrics,
    )
    materialize_prepared_snapshot(
        source,
        second_destination,
        expected_snapshot_id=expected_id,
        cache_root=cache_root,
        metrics=second_metrics,
    )

    cache_files = tuple(cache_root.glob("*.snapshot.json"))
    assert len(cache_files) == 1
    assert first_destination.read_bytes() == second_destination.read_bytes() == cache_files[0].read_bytes()
    assert first_metrics["prepared_snapshot_cache_misses"] == 1.0
    assert second_metrics["prepared_snapshot_cache_hits"] == 1.0
    assert second_metrics.get("prepared_snapshot_bytes_written", 0.0) == 0.0
    if os.name == "nt" or hasattr(os, "link"):
        try:
            assert os.path.samefile(cache_files[0], second_destination)
        except OSError:
            pass


def test_corrupt_prepared_snapshot_cache_is_rebuilt_from_authority(tmp_path: Path) -> None:
    # A damaged cache object is discarded and rebuilt from the coordinator source, never served as evidence.
    source = tmp_path / "prepared.snapshot.json"
    cache_root = tmp_path / "cache"
    payload = _snapshot_payload()
    source.write_text(json.dumps(payload), encoding="utf-8")
    expected_id = stable_hash(payload)[:32]
    first_destination = tmp_path / "worker-1" / "prepared.snapshot.json"
    second_destination = tmp_path / "worker-2" / "prepared.snapshot.json"

    materialize_prepared_snapshot(
        source,
        first_destination,
        expected_snapshot_id=expected_id,
        cache_root=cache_root,
    )
    cache_file = next(cache_root.glob("*.snapshot.json"))
    cache_file.chmod(0o666)
    cache_file.write_text("corrupt", encoding="utf-8")

    metrics: dict[str, float] = {}
    materialize_prepared_snapshot(
        source,
        second_destination,
        expected_snapshot_id=expected_id,
        cache_root=cache_root,
        metrics=metrics,
    )

    assert json.loads(second_destination.read_text(encoding="utf-8")) == payload
    assert metrics["prepared_snapshot_cache_misses"] == 1.0
    assert metrics.get("prepared_snapshot_cache_hits", 0.0) == 0.0


def test_workspace_materialization_exposes_backend_and_storage_cost(tmp_path: Path) -> None:
    # Preserve the existing hardlink-COW/copy fallback while making physical amplification measurable.
    source = tmp_path / "campaign"
    destination = tmp_path / "worker" / "workspace"
    source.mkdir()
    (source / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
    (source / "data.txt").write_text("payload\n", encoding="utf-8")
    metrics: dict[str, float] = {}

    prepare_worker_workspace(
        source,
        destination,
        worker_id="worker-pr79",
        campaign_id="campaign-pr79",
        metrics=metrics,
    )

    assert metrics["workspace_cache_misses"] == 1.0
    assert metrics["workspace_linked_files"] + metrics["workspace_copied_files"] == 2.0
    assert metrics["workspace_setup_seconds"] >= 0.0
    assert metrics["workspace_bytes_read"] >= metrics["workspace_bytes_written"]
    if WorkspaceProvider._hardlink_tree is not None:
        assert metrics["workspace_physical_bytes_allocated"] >= 0.0


def test_local_coordinator_publishes_pr79_data_plane_diagnostics(tmp_path: Path) -> None:
    # Exercise two real workers and prove the immutable snapshot cache is shared without changing execution authority.
    (tmp_path / "app.py").write_text(
        "def choose(value):\n"
        "    if value > 0:\n"
        "        return 1\n"
        "    if value == 0:\n"
        "        return 2\n"
        "    return 0\n",
        encoding="utf-8",
    )
    (tmp_path / "test_app.py").write_text(
        "from app import choose\n\n"
        "def test_choose():\n"
        "    assert choose(1) == 1\n",
        encoding="utf-8",
    )
    configuration = CampaignConfiguration(
        campaign_id=CampaignId("cmp_pr79_data_plane"),
        project=ProjectDescriptor(
            project_id=ProjectId("project_pr79_data_plane"),
            display_name="PR79 data plane fixture",
            root_path=str(tmp_path),
            test_command=TestCommandDescriptor(
                (sys.executable, "-m", "pytest", "-q", "test_app.py"),
            ),
        ),
        scope=MutationScope(
            source_path="app.py",
            function="choose",
            operators=("condition_to_not",),
        ),
        budget=CampaignBudget(max_mutants=2, max_workers=2, max_test_seconds=15.0),
        no_escalation=True,
        reports_dir=str(tmp_path / "reports"),
    )

    result = LocalCampaignCoordinator().run(configuration)

    assert result.succeeded, result.error
    timeline = json.loads(
        (result.database_path.parent / "coordinator.performance.json").read_text(encoding="utf-8")
    )
    diagnostics = timeline["diagnostics"]
    assert diagnostics["prepared_snapshot_cache_misses"] == 1.0
    assert diagnostics["prepared_snapshot_cache_hits"] == 1.0
    assert diagnostics["prepared_snapshot_destination_hits"] == 0.0
    assert diagnostics["workspace_cache_misses"] == 2.0
    assert diagnostics["workspace_linked_files"] + diagnostics["workspace_copied_files"] > 0.0
    assert diagnostics["workspace_physical_bytes_allocated"] >= 0.0

