from __future__ import annotations
import json
import os
import sqlite3
import sys
from pathlib import Path
import pytest
from gallifrey_mutation import MutationCampaignService, SQLiteMutationStore, Success
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
from theseus_local.startup_recovery import (
    StartupRecoveryBusy,
    StartupRecoveryError,
    StartupRecoveryLock,
    recovery_report_path,
    scan_campaign_databases,
)
from theseus_local.worker_runtime import DurableExecutionSpool


def _configuration(root: Path, campaign_id: str) -> CampaignConfiguration:
    # Build one deterministic one-mutant campaign whose control database remains under the fixture root.
    return CampaignConfiguration(
        campaign_id=CampaignId(campaign_id),
        project=ProjectDescriptor(
            project_id=ProjectId(f"project-{campaign_id}"),
            display_name="E14 startup recovery diagnostics",
            root_path=str(root),
            test_command=TestCommandDescriptor(
                (sys.executable, "-m", "pytest", "-q")
            ),
        ),
        scope=MutationScope(
            source_path="app.py",
            function="choose",
            operators=("condition_to_not",),
        ),
        budget=CampaignBudget(
            max_mutants=1,
            max_workers=1,
            max_test_seconds=5.0,
            lease_seconds=0.6,
        ),
        no_escalation=True,
        reports_dir=str(root / "reports"),
    )


def _write_project(root: Path) -> None:
    # Create one observable branch mutation and a stable pytest oracle.
    (root / "app.py").write_text(
        "def choose(value):\n"
        "    if value > 0:\n"
        "        return 1\n"
        "    return 0\n",
        encoding="utf-8",
    )
    (root / "test_app.py").write_text(
        "from app import choose\n\n"
        "def test_choose_positive():\n"
        "    assert choose(1) == 1\n",
        encoding="utf-8",
    )


def test_startup_recovery_replays_delivery_then_resumes_without_duplicate_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Crash after worker fsync but before fan-in, then prove startup replay commits once and resumes automatically.
    _write_project(tmp_path)
    configuration = _configuration(
        tmp_path,
        "campaign-e14-startup-delivery-replay",
    )
    original = MutationCampaignService.record_shard_result
    injected = {"raised": False}

    def crash_before_fan_in(self, effect_id, campaign_id, shard_id, result, **kwargs):
        # Inject exactly one coordinator crash window while leaving worker delivery and mutant events durable.
        if (
            not injected["raised"]
            and str(effect_id).startswith("effect.execute-shard.")
            and not bool(kwargs.get("durable_delivery_replay", False))
        ):
            injected["raised"] = True
            raise RuntimeError(
                "injected crash after durable delivery fsync and before authoritative fan-in"
            )
        return original(
            self,
            effect_id,
            campaign_id,
            shard_id,
            result,
            **kwargs,
        )

    monkeypatch.setattr(
        MutationCampaignService,
        "record_shard_result",
        crash_before_fan_in,
    )
    interrupted = LocalCampaignCoordinator().run(configuration)
    assert injected["raised"] is True, (
        "E14 startup-recovery fixture did not enter the intended crash window; "
        f"campaign={configuration.campaign_id.value}; error={interrupted.error!r}; "
        f"database={interrupted.database_path}"
    )
    assert not interrupted.succeeded, (
        "campaign unexpectedly completed despite injected pre-fan-in crash; "
        f"campaign={configuration.campaign_id.value}; "
        f"status={interrupted.campaign.status}; database={interrupted.database_path}"
    )
    assert "after durable delivery fsync" in str(interrupted.error), (
        "coordinator failure lost the injected crash boundary; "
        f"campaign={configuration.campaign_id.value}; error={interrupted.error!r}"
    )

    store = SQLiteMutationStore(interrupted.database_path)
    try:
        workers = store.list_workers(configuration.campaign_id)
        assert isinstance(workers, Success) and len(workers.value) == 1, (
            "interrupted campaign did not persist exactly one worker registry row; "
            f"campaign={configuration.campaign_id.value}; outcome={workers!r}; "
            f"database={interrupted.database_path}"
        )
        spool_path = Path(workers.value[0].spool_path)
    finally:
        store.close()
    pending_before = DurableExecutionSpool(spool_path).pending()
    assert len(pending_before) == 1, (
        "crash window must leave one unacknowledged durable delivery; "
        f"campaign={configuration.campaign_id.value}; spool={spool_path}; "
        f"pending={[item.event_id for item in pending_before]}"
    )

    monkeypatch.setattr(
        MutationCampaignService,
        "record_shard_result",
        original,
    )
    raw_result = pending_before[0].payload.get("result", {})
    published = raw_result.get("published_engine_artifacts", []) if isinstance(raw_result, dict) else []
    assert published, (
        "real engine crash fixture did not publish canonical artifact references; "
        f"campaign={configuration.campaign_id.value}; event_id={pending_before[0].event_id}; "
        f"result={raw_result}; spool={spool_path}"
    )
    canonical_root = (
        interrupted.database_path.parent.parent
        / "engine"
        / configuration.campaign_id.value
    )
    missing_artifact = canonical_root / str(published[0])
    missing_bytes = missing_artifact.read_bytes()
    missing_artifact.unlink()
    with pytest.raises(StartupRecoveryError) as missing_captured:
        LocalCampaignCoordinator.reconcile_startup(interrupted.database_path)
    missing_message = str(missing_captured.value)
    assert pending_before[0].event_id in missing_message and str(missing_artifact) in missing_message, (
        "artifact reconciliation failure omitted delivery and expected-path identity; "
        f"campaign={configuration.campaign_id.value}; event_id={pending_before[0].event_id}; "
        f"expected_path={missing_artifact}; message={missing_message!r}"
    )
    assert len(DurableExecutionSpool(spool_path).pending()) == 1, (
        "failed artifact validation acknowledged or discarded durable delivery evidence; "
        f"campaign={configuration.campaign_id.value}; spool={spool_path}; "
        f"missing_artifact={missing_artifact}"
    )
    missing_artifact.parent.mkdir(parents=True, exist_ok=True)
    missing_artifact.write_bytes(missing_bytes)

    recovery_root = interrupted.database_path
    recovered = LocalCampaignCoordinator.recover_unfinished(recovery_root)
    assert len(recovered) == 1, (
        "startup scanner did not resume exactly one unfinished campaign; "
        f"root={tmp_path}; databases={LocalCampaignCoordinator.scan_startup(tmp_path)}; "
        f"results={[(item.campaign.campaign_id.value, item.error) for item in recovered]}"
    )
    final = recovered[0]
    assert final.succeeded, (
        "startup recovery failed to complete the interrupted campaign; "
        f"campaign={configuration.campaign_id.value}; status={final.campaign.status}; "
        f"error={final.error!r}; database={final.database_path}; "
        f"report={recovery_report_path(final.database_path)}"
    )
    assert DurableExecutionSpool(spool_path).pending() == (), (
        "replayed delivery remained pending after authoritative fan-in and local ACK; "
        f"campaign={configuration.campaign_id.value}; spool={spool_path}"
    )

    with sqlite3.connect(final.database_path) as connection:
        execution_count = int(
            connection.execute("SELECT COUNT(*) FROM mutation_executions").fetchone()[0]
        )
        effect_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM mutation_effects "
                "WHERE effect_type = 'mutation.execute_shard'"
            ).fetchone()[0]
        )
    assert execution_count == 1 and effect_count == 1, (
        "startup replay duplicated authoritative mutant execution or fan-in effect; "
        f"campaign={configuration.campaign_id.value}; "
        f"execution_count={execution_count}; effect_count={effect_count}; "
        f"database={final.database_path}"
    )

    second_actions = LocalCampaignCoordinator.reconcile_startup(final.database_path)
    assert not any(item.get("status") == "delivery_replayed" for item in second_actions), (
        "idempotent startup pass replayed an already acknowledged delivery; "
        f"campaign={configuration.campaign_id.value}; actions={second_actions}; "
        f"database={final.database_path}; spool={spool_path}"
    )
    with sqlite3.connect(final.database_path) as connection:
        second_execution_count = int(
            connection.execute("SELECT COUNT(*) FROM mutation_executions").fetchone()[0]
        )
        second_effect_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM mutation_effects "
                "WHERE effect_type = 'mutation.execute_shard'"
            ).fetchone()[0]
        )
    assert (second_execution_count, second_effect_count) == (1, 1), (
        "second startup reconciliation changed authoritative execution cardinality; "
        f"campaign={configuration.campaign_id.value}; "
        f"executions={second_execution_count}; effects={second_effect_count}; "
        f"database={final.database_path}"
    )

    report_path = recovery_report_path(final.database_path)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    replay_actions = [
        item for item in report["actions"] if item.get("status") == "delivery_replayed"
    ]
    assert len(replay_actions) == 1, (
        "explicit recovery report did not record the durable delivery replay; "
        f"campaign={configuration.campaign_id.value}; report={report_path}; "
        f"status={report.get('status')}; actions={report.get('actions')}"
    )
    assert replay_actions[0]["completed_mutants"] == 1, (
        "recovery report lost committed mutant cardinality; "
        f"campaign={configuration.campaign_id.value}; action={replay_actions[0]}"
    )
    assert replay_actions[0]["published_engine_artifacts"] == [str(item) for item in published], (
        "recovery report lost the canonical artifact set validated before fan-in; "
        f"campaign={configuration.campaign_id.value}; expected={published}; "
        f"action={replay_actions[0]}; report={report_path}"
    )


def test_startup_recovery_lock_rejects_second_live_coordinator(tmp_path: Path) -> None:
    # Hold the exact process-fenced lock and require a diagnostic rejection instead of concurrent reconciliation.
    database_path = tmp_path / "campaign.sqlite3"
    lock_path = tmp_path / "campaign.coordinator.lock"
    with StartupRecoveryLock(lock_path, database_path=database_path):
        with pytest.raises(StartupRecoveryBusy) as captured:
            LocalCampaignCoordinator.reconcile_startup(database_path)
    message = str(captured.value)
    assert str(database_path.resolve()) in message, (
        "recovery contention error omitted the authoritative database path; "
        f"message={message!r}; expected_database={database_path.resolve()}"
    )
    assert str(os.getpid()) in message and str(lock_path) in message, (
        "recovery contention error omitted owner PID or lock path; "
        f"message={message!r}; expected_pid={os.getpid()}; lock={lock_path}"
    )
    report = json.loads(recovery_report_path(database_path).read_text(encoding="utf-8"))
    assert report["status"] == "failed" and "already owned" in str(report["error"]), (
        "failed concurrent recovery was not materialized as an explicit diagnostic report; "
        f"report={report}; path={recovery_report_path(database_path)}"
    )


def test_startup_scanner_is_deterministic_and_ignores_environment_caches(
    tmp_path: Path,
) -> None:
    # Discover only campaign control databases in stable order while excluding virtual-environment copies.
    expected = (
        tmp_path / "state" / "campaign-a" / "campaign.sqlite3",
        tmp_path / "state" / "campaign-b" / "campaign.sqlite3",
    )
    for database_path in expected:
        database_path.parent.mkdir(parents=True, exist_ok=True)
        database_path.write_bytes(b"")
    ignored = tmp_path / ".venv" / "state" / "campaign.sqlite3"
    ignored.parent.mkdir(parents=True, exist_ok=True)
    ignored.write_bytes(b"")
    unrelated = tmp_path / "state" / "campaign-c" / "other.sqlite3"
    unrelated.parent.mkdir(parents=True, exist_ok=True)
    unrelated.write_bytes(b"")

    discovered = scan_campaign_databases(tmp_path)

    assert discovered == tuple(path.resolve() for path in expected), (
        "startup scanner returned a non-deterministic or unsafe database set; "
        f"root={tmp_path}; expected={[str(path.resolve()) for path in expected]}; "
        f"actual={[str(path) for path in discovered]}; ignored={ignored}; "
        f"unrelated={unrelated}"
    )
