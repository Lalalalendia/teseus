from __future__ import annotations
import json
import sqlite3
import sys
from pathlib import Path
import pytest
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
from theseus_local.worker_runtime import PersistentWorkerProcess, WorkerProcessError

def test_dead_worker_is_replaced_by_a_new_pid_in_the_same_campaign(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Kill the first registered agent and prove the coordinator completes the shard with attempt+1 on another PID.
    (tmp_path / "app.py").write_text(
        "def choose(value):\n    if value > 0:\n        return 1\n    return 0\n",
        encoding="utf-8",
    )
    (tmp_path / "test_app.py").write_text(
        "from app import choose\n\ndef test_choose():\n    assert choose(1) == 1\n",
        encoding="utf-8",
    )
    configuration = CampaignConfiguration(
        campaign_id=CampaignId("cmp-e14-real-pid-reassignment"),
        project=ProjectDescriptor(
            project_id=ProjectId("project-e14-real-pid-reassignment"),
            display_name="E14 real PID reassignment",
            root_path=str(tmp_path),
            test_command=TestCommandDescriptor((sys.executable, "-m", "pytest", "-q")),
        ),
        scope=MutationScope(source_path="app.py", function="choose", operators=("condition_to_not",)),
        budget=CampaignBudget(max_mutants=1, max_workers=1, lease_seconds=2.0),
        no_escalation=True,
        reports_dir=str(tmp_path / "reports"),
    )
    original = PersistentWorkerProcess.send_engine_assignment
    observed: list[tuple[int, str]] = []
    def kill_first(self: PersistentWorkerProcess, *args, **kwargs):
        # Inject one real OS-process death after registration and lease binding.
        observed.append((self.pid, str(self.instance_id)))
        if len(observed) == 1:
            self.terminate_tree()
            raise WorkerProcessError("injected worker PID death")
        return original(self, *args, **kwargs)
    monkeypatch.setattr(PersistentWorkerProcess, "send_engine_assignment", kill_first)
    result = LocalCampaignCoordinator().run(configuration)
    assert result.succeeded, result.error
    assert len(observed) == 2
    assert observed[0][0] != observed[1][0]
    assert observed[0][1] != observed[1][1]
    with sqlite3.connect(result.database_path) as connection:
        payloads = [
            json.loads(row[0])
            for row in connection.execute("SELECT payload FROM mutation_leases")
        ]
    payloads.sort(key=lambda item: (int(item["attempt"]), str(item["lease_id"])))
    assert [(item["attempt"], item["status"]) for item in payloads] == [
        (0, "orphaned"),
        (1, "released"),
    ]
