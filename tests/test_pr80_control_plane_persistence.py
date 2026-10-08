from __future__ import annotations

import json
import sys
from pathlib import Path

from gallifrey_mutation import (
    MutationCampaign,
    MutationExecution,
    MutationExecutionState,
    SQLiteMutationStore,
    Success,
)
from gallifrey_mutation.store import EffectReceipt
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    ExecutionId,
    MutantId,
    MutationScope,
    ProjectDescriptor,
    ProjectId,
    ShardId,
    TestCommandDescriptor,
)
from theseus_performance.sqlite_metrics import SQLiteMetrics
from theseus_local import LocalCampaignCoordinator

def _configuration(tmp_path: Path, campaign_id: str = "cmp_pr80") -> CampaignConfiguration:
    return CampaignConfiguration(
        campaign_id=CampaignId(campaign_id),
        project=ProjectDescriptor(
            project_id=ProjectId("project-pr80"),
            display_name="PR80 control plane fixture",
            root_path=str(tmp_path),
            test_command=TestCommandDescriptor(
                (sys.executable, "-m", "pytest", "-q", "test_app.py")
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


def test_sqlite_metrics_counts_queries_writes_and_atomic_boundaries() -> None:
    metrics = SQLiteMetrics()
    metrics.trace("SELECT 1")
    metrics.trace("INSERT INTO t VALUES (1)")
    metrics.trace("BEGIN IMMEDIATE")
    metrics.trace("COMMIT")
    metrics.trace("ROLLBACK")
    assert metrics.snapshot() == {
        "sqlite_statement_count": 5.0,
        "sqlite_query_count": 1.0,
        "sqlite_write_count": 1.0,
        "sqlite_transaction_count": 1.0,
        "sqlite_commit_count": 1.0,
        "sqlite_rollback_count": 1.0,
    }


def test_execution_batch_lookup_chunks_sqlite_variables(tmp_path: Path) -> None:
    metrics = SQLiteMetrics()
    store = SQLiteMutationStore(tmp_path / "chunk.sqlite3", metrics=metrics)
    metrics.reset()
    result = store.get_executions(
        tuple(ExecutionId(f"execution-{index}") for index in range(1001))
    )
    assert isinstance(result, Success)
    assert result.value == ()
    assert metrics.snapshot()["sqlite_query_count"] == 2.0
    store.close()


def test_commit_effect_batches_execution_existence_reads(tmp_path: Path) -> None:
    metrics = SQLiteMetrics()
    store = SQLiteMutationStore(tmp_path / "campaign.sqlite3", metrics=metrics)
    metrics.reset()
    campaign_id = CampaignId("cmp-batch")
    campaign = MutationCampaign.create(_configuration(tmp_path, campaign_id.value))
    executions = tuple(
        MutationExecution(
            execution_id=ExecutionId(f"execution-{index}"),
            campaign_id=campaign_id,
            shard_id=ShardId("shard-1"),
            mutant_id=MutantId(f"mutant-{index}"),
            attempt=0,
            status=MutationExecutionState.COMPLETE,
            semantic_result="killed",
            restore_verified=True,
            lease_id="lease-1",
        )
        for index in range(4)
    )
    receipt = EffectReceipt(
        effect_id="effect-batch",
        effect_type="test.batch",
        campaign_id=campaign_id,
        payload={"campaign": campaign.to_dict()},
    )
    result = store.commit_effect(receipt=receipt, executions=executions)
    assert isinstance(result, Success)
    snapshot = metrics.snapshot()
    assert snapshot["sqlite_query_count"] < 2 + len(executions)
    assert snapshot["sqlite_write_count"] >= len(executions) + 2
    store.close()


def test_batch_effect_rolls_back_after_injected_mid_batch_failure(tmp_path: Path) -> None:
    store = SQLiteMutationStore(tmp_path / "crash.sqlite3")
    store._connection.execute(
        """
        CREATE TRIGGER fail_second_execution
        BEFORE INSERT ON mutation_executions
        WHEN NEW.execution_id = 'execution-2'
        BEGIN
            SELECT RAISE(ABORT, 'injected batch failure');
        END
        """
    )
    store._connection.commit()
    campaign_id = CampaignId("cmp-crash")
    campaign = MutationCampaign.create(_configuration(tmp_path, campaign_id.value))
    executions = tuple(
        MutationExecution(
            execution_id=ExecutionId(f"execution-{index}"),
            campaign_id=campaign_id,
            shard_id=ShardId("shard-crash"),
            mutant_id=MutantId(f"mutant-{index}"),
            status=MutationExecutionState.COMPLETE,
            semantic_result="killed",
            restore_verified=True,
        )
        for index in range(4)
    )
    receipt = EffectReceipt(
        effect_id="effect-crash",
        effect_type="test.batch",
        campaign_id=campaign_id,
        payload={"campaign": campaign.to_dict()},
    )
    result = store.commit_effect(receipt=receipt, executions=executions)
    assert not isinstance(result, Success)
    assert store._connection.execute(
        "SELECT COUNT(*) FROM mutation_executions"
    ).fetchone()[0] == 0
    assert store._connection.execute(
        "SELECT COUNT(*) FROM mutation_effects"
    ).fetchone()[0] == 0
    assert store._connection.execute(
        "SELECT COUNT(*) FROM mutation_outbox"
    ).fetchone()[0] == 0


def test_sqlite_schema_has_campaign_indexes_and_batch_lookup(tmp_path: Path) -> None:
    store = SQLiteMutationStore(tmp_path / "schema.sqlite3")
    indexes = {
        str(row[1])
        for row in store._connection.execute(
            "SELECT type, name FROM sqlite_master WHERE type = 'index'"
        ).fetchall()
    }
    assert "ix_mutation_shards_campaign" in indexes
    assert "ix_mutation_executions_campaign" in indexes
    assert "ix_mutation_outbox_delivery" in indexes
    store.close()


def test_local_coordinator_publishes_control_plane_metrics(tmp_path: Path) -> None:
    (tmp_path / "app.py").write_text(
        "def choose(value):\n"
        "    if value > 0:\n"
        "        return 1\n"
        "    return 0\n",
        encoding="utf-8",
    )
    (tmp_path / "test_app.py").write_text(
        "from app import choose\n\n"
        "def test_choose():\n"
        "    assert choose(1) == 1\n",
        encoding="utf-8",
    )
    result = LocalCampaignCoordinator().run(
        _configuration(tmp_path, "cmp-pr80-integration")
    )
    assert result.succeeded, result.error
    timeline = json.loads(
        (result.database_path.parent / "coordinator.performance.json").read_text(
            encoding="utf-8"
        )
    )
    diagnostics = timeline["diagnostics"]
    assert diagnostics["sqlite_query_count"] > 0.0
    assert diagnostics["sqlite_write_count"] > 0.0
    assert diagnostics["sqlite_commit_count"] > 0.0
    assert diagnostics["sqlite_queries_per_mutant"] > 0.0
    assert diagnostics["sqlite_writes_per_mutant"] > 0.0
    assert diagnostics["sqlite_database_bytes"] > 0.0
    assert timeline["status"] == "completed"
