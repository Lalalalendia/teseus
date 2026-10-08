from __future__ import annotations
import json
import sys
from pathlib import Path
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
from theseus_local.workspace import WorkspaceProvider
def test_coordinator_owns_distinct_os_workers_and_workspaces(tmp_path: Path) -> None:
    # Verify E13 uses independent child engine processes rather than logical labels over one session.
    (tmp_path / "app.py").write_text(
        "def choose(value):\n"
        "    if value > 0:\n        return 1\n"
        "    if value == 0:\n        return 2\n"
        "    return 0\n",
        encoding="utf-8",
    )
    (tmp_path / "test_app.py").write_text(
        "import time\n\n"
        "from app import choose\n\n"
        "def test_positive():\n    time.sleep(0.05)\n    assert choose(1) == 1\n\n"
        "def test_zero():\n    time.sleep(0.05)\n    assert choose(0) == 2\n",
        encoding="utf-8",
    )
    configuration = CampaignConfiguration(
        campaign_id=CampaignId("cmp_e13_os_pool"),
        project=ProjectDescriptor(
            project_id=ProjectId("project_e13_os_pool"),
            display_name="E13 OS worker pool",
            root_path=str(tmp_path),
            test_command=TestCommandDescriptor((sys.executable, "-m", "pytest", "-q")),
        ),
        scope=MutationScope(source_path="app.py", function="choose", operators=("condition_to_not",)),
        budget=CampaignBudget(max_mutants=2, max_workers=2),
        no_escalation=True,
        reports_dir=str(tmp_path / "reports"),
    )
    result = LocalCampaignCoordinator().run(configuration)
    assert result.succeeded, result.error
    registry_path = WorkspaceProvider(configuration).state_root / "workers" / configuration.campaign_id.value / "worker-registry.json"
    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    rows = registry["workers"]
    assert len(rows) == 2
    assert {row["state"] for row in rows} == {"complete"}
    assert len({row["pid"] for row in rows}) == 2
    assert len({row["child_process_id"] for row in rows}) == 2
    assert all(row["child_process_id"] != row["pid"] for row in rows)
    assert len({row["workspace"] for row in rows}) == 2
    for row in rows:
        worker_workspace = Path(row["workspace"])
        assert worker_workspace.is_dir()
        assert (worker_workspace.parent / "reports").is_dir()
        worker_engine = worker_workspace.parent / "reports" / "engine" / configuration.campaign_id.value
        assert list(worker_engine.glob("engine-shard-*.json"))
        snapshot = worker_engine / "prepared.snapshot.json"
        assert snapshot.is_file()
        assert snapshot.stat().st_mode & 0o222 == 0
        assert not (worker_engine / "collection.snapshot.json").exists()
def test_parallel_worker_heartbeat_renews_short_gallifrey_lease(tmp_path: Path) -> None:
    # Keep a slow shard alive across multiple short TTLs through the authoritative lease callback.
    (tmp_path / "app.py").write_text(
        "def choose(value):\n"
        "    if value > 0:\n        return 1\n"
        "    if value == 0:\n        return 2\n"
        "    return 0\n",
        encoding="utf-8",
    )
    (tmp_path / "test_app.py").write_text(
        "import time\n\n"
        "from app import choose\n\n"
        "def test_positive():\n    time.sleep(0.8)\n    assert choose(1) == 1\n\n"
        "def test_zero():\n    time.sleep(0.8)\n    assert choose(0) == 2\n",
        encoding="utf-8",
    )
    configuration = CampaignConfiguration(
        campaign_id=CampaignId("cmp_e13_short_lease"),
        project=ProjectDescriptor(
            project_id=ProjectId("project_e13_short_lease"),
            display_name="E13 short lease",
            root_path=str(tmp_path),
            test_command=TestCommandDescriptor((sys.executable, "-m", "pytest", "-q")),
        ),
        scope=MutationScope(source_path="app.py", function="choose", operators=("condition_to_not",)),
        budget=CampaignBudget(max_mutants=2, max_workers=2, max_test_seconds=5.0, lease_seconds=0.6),
        no_escalation=True,
        reports_dir=str(tmp_path / "reports"),
    )
    result = LocalCampaignCoordinator().run(configuration)
    assert result.succeeded, result.error
