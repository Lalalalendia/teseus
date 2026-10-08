import hashlib
import json
import sys
from pathlib import Path
from test_intelligence_unified_v1.runner import MutationConfig
from test_intelligence_unified_v1.io_utils import atomic_write_json
from test_intelligence_unified_v1.models import CampaignAccumulator
from test_intelligence_unified_v1.workers import (
    inspect_worker_workspaces,
    resume_parallel_campaign,
    run_parallel_campaign,
)
def _campaign_config(root: Path, reports: Path) -> MutationConfig:
    # Build the smallest deterministic two-worker compatibility campaign used by recovery tests.
    return MutationConfig(
        project_root=root,
        source="app.py",
        function="choose",
        test_command_argv=(sys.executable, "-c", "from app import choose; assert choose(1) == 1"),
        operators=("condition_to_not",),
        max_mutants=2,
        no_escalation=True,
        use_baseline_cache=False,
        reports_dir=reports,
        workers=2,
    )
def _write_project(root: Path) -> str:
    # Create a target with two independently killable condition mutants.
    source = (
        "def choose(value):\n"
        "    if value > 0:\n"
        "        result = 1\n"
        "    else:\n"
        "        result = 0\n"
        "    if value == 1:\n"
        "        return result\n"
        "    return 0\n"
    )
    (root / "app.py").write_text(source, encoding="utf-8")
    return source
def test_resume_restarts_only_missing_worker_results(tmp_path: Path) -> None:
    # Rebuild a truncated legacy campaign from worker journals without reopening runner parallel routing.
    source = _write_project(tmp_path)
    reports = tmp_path / "reports"
    initial = run_parallel_campaign(_campaign_config(tmp_path, reports))
    assert initial["status"] == "complete"
    manifest_path = next(reports.glob("*.workers.manifest.json"))
    report_path = Path(initial["report_path"])
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["status"] = "running"
    report["results"] = []
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["status"] = "active"
    manifest["coordinator_pid"] = 999999
    for worker in manifest["workers"]:
        worker["status"] = "active"
        worker_report_path = reports / worker["report_path"]
        worker_report = json.loads(worker_report_path.read_text(encoding="utf-8"))
        worker_report["status"] = "running"
        worker_report["results"] = []
        worker_report_path.write_text(
            json.dumps(worker_report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        journal_path = Path(worker_report["results_journal"])
        journal_path.write_text("", encoding="utf-8")
        state_path = Path(worker_report["state_path"])
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state.update(
            {
                "status": "running",
                "completed_mutants": 0,
                "counts": {},
                "journal_offset": 0,
                "journal_size": 0,
                "journal_sha256": hashlib.sha256(b"").hexdigest(),
                "last_sequence": 0,
                "completed_execution_ids": [],
                "completed_mutant_ids": [],
                "completed_identity_digests": {},
                "accumulator": CampaignAccumulator().checkpoint_dict(),
            }
        )
        atomic_write_json(state_path, state, durability="normal", category="state_checkpoint")
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    resumed = resume_parallel_campaign(manifest_path, force=True)
    assert resumed["status"] == "complete"
    assert len(resumed["results"]) == 2
    assert resumed["resumed_from"] == str(manifest_path)
    assert source == (tmp_path / "app.py").read_text(encoding="utf-8")
    assert not list(tmp_path.glob("*.test_intelligence.lock"))
    final_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert final_manifest["status"] == "resumed"
    assert Path(resumed["report_path"]).exists()
def test_worker_diagnostics_find_completed_campaign(tmp_path: Path) -> None:
    # Report cleaned compatibility worker projects while retaining the coordinator manifest inventory.
    _write_project(tmp_path)
    reports = tmp_path / "reports"
    report = run_parallel_campaign(_campaign_config(tmp_path, reports))
    assert report["status"] == "complete"
    diagnostics = inspect_worker_workspaces(reports)
    assert len(diagnostics["manifests"]) == 1
    assert diagnostics["active_workspaces"] == []
    assert diagnostics["stale_workspaces"] == []
