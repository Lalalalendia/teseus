from __future__ import annotations

from theseus_performance import ProjectPerformanceSample, ProjectTuningStore


def test_a6_project_tuning_memory_survives_restart_and_drives_next_worker_choice(tmp_path) -> None:
    # Persist repeated-run throughput and choose the historically efficient worker count after restart.
    path = tmp_path / "tuning.json"
    store = ProjectTuningStore(path)
    store.record(ProjectPerformanceSample("run-1", "project-1", 2, 40, 10.0))
    store.record(ProjectPerformanceSample("run-2", "project-1", 4, 79, 10.0))
    store.record(ProjectPerformanceSample("run-3", "project-1", 8, 80, 10.0))
    restarted = ProjectTuningStore(path)
    decision = restarted.recommend("project-1", cpu_count=16)
    assert len(restarted.samples("project-1")) == 3
    assert decision.workers == 4
    assert decision.source == "history"


def test_a6_project_tuning_record_is_idempotent_by_run_identity(tmp_path) -> None:
    # Prevent repeated UI polling from teaching the same completed run more than once.
    store = ProjectTuningStore(tmp_path / "tuning.json")
    sample = ProjectPerformanceSample("run-1", "project-1", 4, 100, 20.0)
    store.record(sample)
    store.record(sample)
    assert len(store.samples("project-1")) == 1


def test_a6_one_click_run_uses_persisted_project_history_on_next_launch(tmp_path) -> None:
    # Feed prior project throughput into the actual browser authority and verify the next one-click campaign uses it.
    from theseus_api import ApiSuccess
    from theseus_performance import ProjectPerformanceSample
    from theseus_ui import LocalWorkspaceCampaignClient

    project = tmp_path / "project"
    project.mkdir()
    (project / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
    captured = []

    class Actions:
        def __init__(self, configuration):
            # Retain the configuration chosen from persisted tuning history.
            self.configuration = configuration

        def create_campaign(self, action_id, configuration):
            # Simulate successful campaign creation while capturing its worker budget.
            captured.append(configuration)
            return ApiSuccess({"action_id": action_id, "campaign_id": configuration.campaign_id.value, "status": "created", "campaign_revision": 0})

        def start_campaign(self, action_id, campaign_id, *, expected_revision):
            # Simulate launch without creating worker processes in the unit test.
            return ApiSuccess({"action_id": action_id, "campaign_id": campaign_id, "status": "running", "campaign_revision": expected_revision})

    client = LocalWorkspaceCampaignClient(tmp_path / "state", actions_factory=Actions)
    project_id = client.register_project(str(project))["value"]["project_id"]
    client.tuning_store.record(ProjectPerformanceSample("old-2", project_id, 2, 70, 10.0))
    client.tuning_store.record(ProjectPerformanceSample("old-4", project_id, 4, 100, 10.0))
    client.tuning_store.record(ProjectPerformanceSample("old-8", project_id, 8, 101, 10.0))
    result = client.create_project_run({"project_id": project_id})
    assert result["ok"] is True
    assert captured[-1].budget.max_workers == 4


def test_a6_completed_project_run_is_recorded_once_from_authoritative_campaign_progress(tmp_path, monkeypatch) -> None:
    # Teach project tuning automatically when aggregate child-campaign evidence first reaches complete.
    from datetime import datetime, timedelta, timezone
    from theseus_local.project_run import ProjectRunEntry, ProjectRunRecord
    from theseus_ui import LocalWorkspaceCampaignClient

    project = tmp_path / "project"
    project.mkdir()
    (project / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
    client = LocalWorkspaceCampaignClient(tmp_path / "state")
    project_id = client.register_project(str(project))["value"]["project_id"]
    created_at = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
    run = ProjectRunRecord(
        "run-complete",
        project_id,
        (ProjectRunEntry("app.py", "campaign-1", "running"),),
        created_at=created_at,
        worker_budget=4,
    )
    client.registry.register_project_run(run)
    campaign_root = tmp_path / "state" / "campaign-1"
    campaign_root.mkdir(parents=True)
    database = campaign_root / "campaign.sqlite3"
    database.write_bytes(b"")
    (campaign_root / "coordinator.performance.json").write_text(
        '{"status":"completed","total_wall_seconds":5.0,"accounting_error_seconds":0.0}',
        encoding="utf-8",
    )
    client.registry.register_campaign(project_id, "campaign-1", database)
    monkeypatch.setattr(
        client,
        "_campaign_rows",
        lambda project_id=None: [{"campaign_id": "campaign-1", "status": "complete", "total_mutants": 20, "completed_mutants": 20}],
    )
    first = client.get_project_run("run-complete")
    second = client.get_project_run("run-complete")
    samples = client.tuning_store.samples(project_id)
    assert first["value"]["status"] == "complete"
    assert second["value"]["status"] == "complete"
    assert len(samples) == 1
    assert samples[0].workers == 4
    assert samples[0].completed_mutants == 20
    assert client.registry.project_run("run-complete").completed_at is not None
