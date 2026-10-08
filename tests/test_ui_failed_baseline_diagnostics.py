from __future__ import annotations

import json
from pathlib import Path

from theseus_local.project_run import ProjectRunEntry, ProjectRunRecord
from theseus_ui.local_control import LocalWorkspaceCampaignClient


class _FakeDetailClient:
    def get_campaign(self, campaign_id: str, *, related_limit: int):
        # Return a pre-plan campaign detail with no source so the ProjectRun fallback is exercised.
        return {
            "ok": True,
            "kind": "success",
            "value": {
                "campaign": {
                    "campaign_id": campaign_id,
                    "project_id": "project",
                    "status": "indexing",
                    "source_path": "",
                    "revision_number": 1,
                    "completed_mutants": 0,
                    "total_mutants": 0,
                }
            },
        }


def test_campaign_detail_exposes_source_and_bounded_failed_baseline_facts(tmp_path: Path, monkeypatch) -> None:
    # Show enough failed-baseline evidence to diagnose a stuck pre-plan campaign without exposing raw output.
    project = tmp_path / "project"
    backend = project / "backend"
    (backend / "tests").mkdir(parents=True)
    (backend / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
    (backend / "tests" / "test_app.py").write_text("def test_value():\n    assert True\n", encoding="utf-8")
    (backend / "pytest.ini").write_text("[pytest]\npythonpath = .\n", encoding="utf-8")
    client = LocalWorkspaceCampaignClient(tmp_path / "ui-state")
    project_id = client.register_project(str(project), "project")["value"]["project_id"]
    campaign_id = "campaign-baseline-failed"
    database = tmp_path / "reports" / campaign_id / "campaign.sqlite3"
    database.parent.mkdir(parents=True)
    database.touch()
    client.registry.register_campaign(project_id, campaign_id, database)
    client.registry.register_project_run(
        ProjectRunRecord(
            "project-run",
            project_id,
            (ProjectRunEntry("backend/app.py", campaign_id, "running"),),
        )
    )
    baseline = database.parent.parent / "engine" / campaign_id / "baseline.json"
    baseline.parent.mkdir(parents=True)
    baseline.write_text(
        json.dumps(
            {
                "status": "baseline_failed",
                "rows": [
                    {
                        "level": "L1",
                        "passed": False,
                        "exit_code": 1,
                        "timed_out": False,
                        "elapsed_seconds": 0.25,
                        "diagnostic_excerpts": ["pytest exited with failing tests"],
                        "output_tail": "secret raw output must stay private",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(client, "_client", lambda _campaign_id: _FakeDetailClient())
    monkeypatch.setattr(client, "_campaign_attempt_failure_code", lambda _campaign_id: "campaign_baseline_failed")

    result = client.get_campaign(campaign_id, related_limit=10)

    assert result["ok"] is True
    assert result["value"]["campaign"]["source_path"] == "backend/app.py"
    assert result["value"]["execution_context"]["test_cwd"] == "backend"
    assert result["value"]["execution_context"]["baseline"] == {
        "status": "baseline_failed",
        "level": "L1",
        "exit_code": 1,
        "timed_out": False,
        "elapsed_seconds": 0.25,
        "diagnostic_excerpts": ["pytest exited with failing tests"],
    }
    assert "secret raw output" not in repr(result)
