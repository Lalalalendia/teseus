from __future__ import annotations

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
from theseus_local.process import EngineProcessError, EngineProcessSession
from theseus_local.workspace import WorkspaceProvider
from gallifrey_mutation import SQLiteMutationStore, Success


def test_local_coordinator_materializes_one_lease_per_planned_shard(tmp_path: Path) -> None:
    # Prove max_workers creates distinct durable shards and workers rather than one hard-coded lease.
    (tmp_path / "app.py").write_text(
        "def choose(value):\n"
        "    if value > 0:\n"
        "        return 1\n"
        "    if value == 0:\n"
        "        return 2\n"
        "    return 0\n",
        encoding="utf-8",
    )
    configuration = CampaignConfiguration(
        campaign_id=CampaignId("cmp_e16_dynamic_shards"),
        project=ProjectDescriptor(
            project_id=ProjectId("project_e16_dynamic_shards"),
            display_name="E16 dynamic shards",
            root_path=str(tmp_path),
            test_command=TestCommandDescriptor(
                (sys.executable, "-c", "from app import choose; assert choose(1) == 1")
            ),
        ),
        scope=MutationScope(source_path="app.py", function="choose", operators=("condition_to_not",)),
        budget=CampaignBudget(max_mutants=2, max_workers=2),
        no_escalation=True,
        reports_dir=str(tmp_path / "reports"),
    )
    result = LocalCampaignCoordinator().run(configuration)
    assert result.succeeded, result.error

    store = SQLiteMutationStore(result.database_path)
    try:
        shards = store.list_shards(configuration.campaign_id)
        assert isinstance(shards, Success)
        assert len(shards.value) == 2
        assert {item.shard_id.value for item in shards.value} == {"shard-000", "shard-001"}
        assert {item.worker_id.value for item in shards.value if item.worker_id} == {
            "local-worker-000",
            "local-worker-001",
        }
        assert all(item.status.value == "complete" for item in shards.value)
        pending = store.list_outbox(undelivered_only=True)
        assert isinstance(pending, Success)
        assert pending.value == ()
    finally:
        store.close()

    reports_root = WorkspaceProvider(configuration).reports_root
    assert (reports_root / "cmp_e16_dynamic_shards" / "campaign.sqlite3").is_file()


def test_engine_request_cancellation_does_not_wait_on_blocking_read(tmp_path: Path) -> None:
    # Prove a silent child is interrupted by the coordinator polling loop instead of blocking on readline forever.
    calls = 0

    def cancel() -> bool:
        # Request cancellation after the first bounded poll interval.
        nonlocal calls
        calls += 1
        return calls >= 1

    session = EngineProcessSession(
        events_path=tmp_path / "events.jsonl",
        protocol_path=tmp_path / "responses.jsonl",
        stdout_path=tmp_path / "stdout.log",
        stderr_path=tmp_path / "stderr.log",
        cwd=tmp_path,
        command=(sys.executable, "-c", "import time; time.sleep(30)"),
        request_timeout_seconds=30.0,
        cancel_callback=cancel,
    )
    with pytest.raises(EngineProcessError, match="cancelled"):
        with session:
            session.request("hang")
    assert calls >= 1
