from __future__ import annotations
import os
from pathlib import Path
from types import SimpleNamespace
import pytest
from gallifrey_mutation import (
    MutationCampaignService,
    OperatorActionStatus,
    SQLiteMutationStore,
    Success,
)
from theseus_api import ApiFailed, ApiRejected, ApiSuccess, LocalOperatorActions
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    MutationScope,
    ProjectDescriptor,
    ProjectId,
)
from theseus_local import LocalCampaignCoordinator
from theseus_local.launcher import run_campaign_action, spawn_campaign_action
from theseus_local.workspace import WorkspaceProvider


def _configuration(root: Path, campaign_id: str = "campaign-e21-launch") -> CampaignConfiguration:
    # Build one launch request whose durable state remains outside the project checkout.
    return CampaignConfiguration(
        campaign_id=CampaignId(campaign_id),
        project=ProjectDescriptor(
            ProjectId("project-e21-launch"),
            "E21 launch authority",
            str(root),
        ),
        scope=MutationScope("app.py", scope_kind="project"),
        budget=CampaignBudget(max_mutants=1, max_workers=1),
        reports_dir=str(root.parent / "theseus-e21-state" / "reports"),
    )


def test_campaign_creation_is_atomic_and_exact_replay_safe(tmp_path: Path) -> None:
    # Commit campaign identity and create receipt together without preparing a workspace.
    configuration = _configuration(tmp_path)
    actions = LocalOperatorActions.for_configuration(configuration)
    first = actions.create_campaign("action-e21-create")
    repeated = actions.create_campaign("action-e21-create")
    conflict = actions.create_campaign("action-e21-create-again")
    assert isinstance(first, ApiSuccess)
    assert isinstance(repeated, ApiSuccess)
    assert repeated.to_dict() == first.to_dict()
    assert isinstance(conflict, ApiRejected)
    assert conflict.error.code == "campaign_already_exists"
    database = LocalCampaignCoordinator.campaign_database_path(configuration)
    store = SQLiteMutationStore(database)
    try:
        campaign = store.get_campaign(configuration.campaign_id)
        action = store.get_operator_action("action-e21-create")
        rejected = store.get_operator_action("action-e21-create-again")
        assert isinstance(campaign, Success) and campaign.value is not None
        assert isinstance(action, Success) and action.value is not None
        assert action.value.status is OperatorActionStatus.COMPLETED
        assert isinstance(rejected, Success) and rejected.value is not None
        assert rejected.value.status is OperatorActionStatus.REJECTED
    finally:
        store.close()
    workspace = WorkspaceProvider(configuration).state_root / "workspaces" / configuration.campaign_id.value
    assert not workspace.exists()


def test_start_campaign_returns_running_and_reuses_one_live_process(tmp_path: Path) -> None:
    # Return immediately after spawn and avoid a second process for exact live replay.
    configuration = _configuration(tmp_path, "campaign-e21-live-start")
    launches: list[tuple[Path, str, str]] = []
    alive = {101: True, 202: True}
    process_ids = iter((101, 202))
    def launch(database: Path, campaign_id: str, action_id: str) -> dict[str, object]:
        # Record one detached launch and return bounded process identity metadata.
        process_id = next(process_ids)
        launches.append((database, campaign_id, action_id))
        return {"process_id": process_id, "process_birth_token": f"token-{process_id}"}
    actions = LocalOperatorActions.for_configuration(
        configuration,
        launch_executor=launch,
        launch_liveness=lambda metadata: bool(alive.get(int(metadata.get("process_id", 0)), False)),
    )
    assert isinstance(actions.create_campaign("action-e21-create-live"), ApiSuccess)
    first = actions.start_campaign(
        "action-e21-start-live",
        configuration.campaign_id.value,
        expected_revision=0,
    )
    repeated = actions.start_campaign(
        "action-e21-start-live",
        configuration.campaign_id.value,
        expected_revision=0,
    )
    assert isinstance(first, ApiSuccess) and first.value.status == "running"
    assert isinstance(repeated, ApiSuccess) and repeated.value.status == "running"
    assert len(launches) == 1
    alive[101] = False
    restarted = actions.start_campaign(
        "action-e21-start-live",
        configuration.campaign_id.value,
        expected_revision=0,
    )
    assert isinstance(restarted, ApiSuccess) and restarted.value.status == "running"
    assert len(launches) == 2
    store = SQLiteMutationStore(LocalCampaignCoordinator.campaign_database_path(configuration))
    try:
        action = store.get_operator_action("action-e21-start-live")
        assert isinstance(action, Success) and action.value is not None
        assert action.value.result["process_id"] == 202
    finally:
        store.close()


def test_stale_start_rejection_is_durable_across_campaign_changes(tmp_path: Path) -> None:
    # Preserve the first stale outcome even after the campaign revision later advances.
    configuration = _configuration(tmp_path, "campaign-e21-stale-start")
    actions = LocalOperatorActions.for_configuration(
        configuration,
        launch_executor=lambda *_: pytest.fail("stale start must not spawn a process"),
    )
    assert isinstance(actions.create_campaign("action-e21-create-stale"), ApiSuccess)
    first = actions.start_campaign(
        "action-e21-start-stale",
        configuration.campaign_id.value,
        expected_revision=1,
    )
    assert isinstance(first, ApiRejected)
    assert first.error.code == "stale_revision"
    database = LocalCampaignCoordinator.campaign_database_path(configuration)
    store = SQLiteMutationStore(database)
    try:
        service = MutationCampaignService(store)
        transitioned = service.prepare(
            "effect-e21-prepare",
            configuration.campaign_id,
            expected_revision=0,
        )
        assert isinstance(transitioned, Success)
    finally:
        store.close()
    repeated = actions.start_campaign(
        "action-e21-start-stale",
        configuration.campaign_id.value,
        expected_revision=1,
    )
    assert isinstance(repeated, ApiRejected)
    assert repeated.to_dict() == first.to_dict()


def test_launch_worker_completes_the_same_durable_start_action(tmp_path: Path) -> None:
    # Convert one running detached action into a terminal receipt without creating another campaign.
    configuration = _configuration(tmp_path, "campaign-e21-worker-complete")
    actions = LocalOperatorActions.for_configuration(
        configuration,
        launch_executor=lambda *_: {"process_id": 1001, "process_birth_token": "token-1001"},
        launch_liveness=lambda _: True,
    )
    assert isinstance(actions.create_campaign("action-e21-create-worker"), ApiSuccess)
    started = actions.start_campaign(
        "action-e21-start-worker",
        configuration.campaign_id.value,
        expected_revision=0,
    )
    assert isinstance(started, ApiSuccess) and started.value.status == "running"
    database = LocalCampaignCoordinator.campaign_database_path(configuration)
    result = SimpleNamespace(
        succeeded=True,
        campaign=SimpleNamespace(
            revision_number=7,
            status=SimpleNamespace(value="completed"),
        ),
    )
    exit_code = run_campaign_action(
        database,
        configuration.campaign_id.value,
        "action-e21-start-worker",
        resume_executor=lambda *_: result,
    )
    assert exit_code == 0
    replay = actions.start_campaign(
        "action-e21-start-worker",
        configuration.campaign_id.value,
        expected_revision=0,
    )
    assert isinstance(replay, ApiSuccess)
    assert replay.value.status == "completed"
    assert replay.value.campaign_revision == 7


def test_launch_worker_sanitizes_executor_failure(tmp_path: Path) -> None:
    # Persist one retriable typed failure without exposing the executor exception message.
    configuration = _configuration(tmp_path, "campaign-e21-worker-failure")
    actions = LocalOperatorActions.for_configuration(
        configuration,
        launch_executor=lambda *_: {"process_id": 1002, "process_birth_token": "token-1002"},
        launch_liveness=lambda _: True,
    )
    assert isinstance(actions.create_campaign("action-e21-create-failure"), ApiSuccess)
    assert isinstance(
        actions.start_campaign(
            "action-e21-start-failure",
            configuration.campaign_id.value,
            expected_revision=0,
        ),
        ApiSuccess,
    )
    database = LocalCampaignCoordinator.campaign_database_path(configuration)
    def explode(*_: object) -> object:
        # Simulate a private coordinator failure that must not cross the public boundary.
        raise RuntimeError("secret local checkout path")
    assert run_campaign_action(
        database,
        configuration.campaign_id.value,
        "action-e21-start-failure",
        resume_executor=explode,
    ) == 1
    replay = actions.start_campaign(
        "action-e21-start-failure",
        configuration.campaign_id.value,
        expected_revision=0,
    )
    assert isinstance(replay, ApiFailed)
    assert replay.error.code == "campaign_launch_failed"
    assert "secret" not in replay.error.message


def test_spawn_campaign_action_uses_private_module_without_shell(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Keep detached process construction argv-based and return no filesystem paths.
    captured: dict[str, object] = {}
    class Process:
        pid = os.getpid()
    def fake_popen(command: tuple[str, ...], **kwargs: object) -> Process:
        # Capture the production subprocess contract without starting another interpreter.
        captured["command"] = command
        captured["kwargs"] = kwargs
        return Process()
    monkeypatch.setattr("theseus_local.launcher.subprocess.Popen", fake_popen)
    metadata = spawn_campaign_action(
        tmp_path / "campaign.sqlite3",
        "campaign-e21-spawn",
        "action-e21-spawn",
    )
    command = captured["command"]
    kwargs = captured["kwargs"]
    assert isinstance(command, tuple)
    assert command[1:4] == ("-m", "theseus_local.launcher", "run")
    assert isinstance(kwargs, dict) and kwargs["shell"] is False
    assert set(metadata) == {"process_id", "process_birth_token"}
