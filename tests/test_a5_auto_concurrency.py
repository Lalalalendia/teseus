from __future__ import annotations

from theseus_performance import ConcurrencySample, choose_auto_workers


def test_a5_auto_concurrency_uses_half_cpu_for_cold_project() -> None:
    # Start conservatively instead of consuming every logical CPU on an unknown project.
    decision = choose_auto_workers(cpu_count=16, workload_size=100)
    assert decision.workers == 8
    assert decision.source == "cpu_half"


def test_a5_auto_concurrency_prefers_fewer_workers_within_five_percent_of_best() -> None:
    # Avoid oversubscription when a smaller worker pool is effectively tied for best throughput.
    decision = choose_auto_workers(
        cpu_count=16,
        history=(
            ConcurrencySample(4, 9.7, 10.0, 97),
            ConcurrencySample(8, 10.0, 10.0, 100),
        ),
    )
    assert decision.workers == 4
    assert decision.source == "history"


def test_a5_auto_concurrency_respects_memory_ceiling() -> None:
    # Bound a cold project to two workers when available memory is below two gibibytes.
    decision = choose_auto_workers(cpu_count=32, memory_available_bytes=1024**3)
    assert decision.workers == 2
    assert decision.candidate_ceiling == 2


def test_a5_one_click_project_run_uses_auto_workers_but_manual_campaign_keeps_legacy_default(tmp_path, monkeypatch) -> None:
    # Apply AUTO only to the one-click path so expert manual launches retain their A2 semantics.
    from pathlib import Path
    from theseus_api import ApiSuccess
    from theseus_performance import concurrency as concurrency_module
    from theseus_ui import LocalWorkspaceCampaignClient

    project = tmp_path / "project"
    project.mkdir()
    (project / "app.py").write_text("def value():\n    return 1\n", encoding="utf-8")
    (project / "test_app.py").write_text("def test_value():\n    assert True\n", encoding="utf-8")
    captured = []

    class Actions:
        def __init__(self, configuration):
            # Retain the resolved configuration without starting subprocesses.
            self.configuration = configuration

        def create_campaign(self, action_id, configuration):
            # Simulate durable creation for AUTO-concurrency integration testing.
            captured.append(configuration)
            return ApiSuccess({"action_id": action_id, "campaign_id": configuration.campaign_id.value, "status": "created", "campaign_revision": 0})

        def start_campaign(self, action_id, campaign_id, *, expected_revision):
            # Simulate successful detached launch without executing mutation tests.
            return ApiSuccess({"action_id": action_id, "campaign_id": campaign_id, "status": "running", "campaign_revision": expected_revision})

    monkeypatch.setattr(concurrency_module.os, "cpu_count", lambda: 8)
    client = LocalWorkspaceCampaignClient(tmp_path / "state", actions_factory=Actions)
    project_id = client.register_project(str(project))["value"]["project_id"]
    manual = client.create_campaign({"project_id": project_id, "source_path": "app.py", "scope_kind": "file", "operators": []})
    assert manual["ok"] is True
    assert captured[-1].budget.max_workers == 1
    launched = client.create_project_run({"project_id": project_id})
    assert launched["ok"] is True
    assert captured[-1].budget.max_workers == 4
    assert launched["value"]["worker_budget"] == 4
