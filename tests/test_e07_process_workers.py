import os
import json
from pathlib import Path

from test_intelligence_unified_v1 import workers as workers_module
from test_intelligence_unified_v1.workers import StaticBaselineProvider, WorkerShard, reconcile_worker_leases


def test_reconcile_marks_expired_dead_worker_orphaned() -> None:
    # Preserve the worker workspace while making an expired dead lease actionable.
    manifest = {
        "workers": [
            {
                "worker_id": "worker-001",
                "status": "running",
                "pid": 999999,
                "lease_seconds": 1,
                "heartbeat_at": "2020-01-01T00:00:00+00:00",
            }
        ]
    }
    diagnostics = reconcile_worker_leases(manifest, now=1_577_836_802.0)
    assert diagnostics["orphaned_workers"] == ["worker-001"]
    assert manifest["workers"][0]["status"] == "orphaned"


def test_reconcile_keeps_live_worker_lease(monkeypatch) -> None:
    # Do not mark a live process orphaned even when its heartbeat is old.
    manifest = {
        "workers": [
            {
                "worker_id": "worker-002",
                "status": "running",
                "pid": os.getpid(),
                "lease_seconds": 1,
                "heartbeat_at": "2020-01-01T00:00:00+00:00",
            }
        ]
    }
    monkeypatch.setattr(workers_module, "_pid_alive", lambda pid: True)
    diagnostics = reconcile_worker_leases(manifest, now=1_577_836_801.0)
    assert diagnostics["live_workers"] == ["worker-002"]
    assert manifest["workers"][0]["status"] == "running"


def test_cancel_marks_leases_without_touching_completed_workers(tmp_path: Path, monkeypatch) -> None:
    # Publish cancellation intent and preserve the manifest as the recovery source of truth.
    manifest = tmp_path / "run.workers.manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "status": "active",
                "workers": [
                    {"worker_id": "worker-001", "status": "running", "pid": 1234},
                    {"worker_id": "worker-002", "status": "complete", "pid": 5678},
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(workers_module, "_terminate_worker_pid", lambda pid: int(pid) == 1234)
    result = workers_module.cancel_parallel_campaign(manifest, reason="operator request")
    saved = json.loads(manifest.read_text(encoding="utf-8"))
    assert result["cancelled_workers"] == ["worker-001"]
    assert result["unresolved_workers"] == []
    assert saved["status"] == "cancelled"
    assert saved["cancel_reason"] == "operator request"
    assert saved["workers"][0]["status"] == "cancelled"
    assert saved["workers"][1]["status"] == "complete"


def test_inspect_reports_lease_diagnostics_without_mutating_files(tmp_path: Path) -> None:
    # Surface orphaned leases through the read-only workspace diagnostic command.
    reports = tmp_path / "reports"
    worker_root = reports / "workers" / "run" / "worker-001"
    worker_root.mkdir(parents=True)
    (worker_root / "worker.json").write_text(
        json.dumps(
            {
                "worker_id": "worker-001",
                "status": "running",
                "pid": 999999,
                "lease_seconds": 1,
                "heartbeat_at": "2020-01-01T00:00:00+00:00",
                "project_root": str(worker_root / "project"),
                "reports_dir": str(worker_root / "reports"),
            }
        ),
        encoding="utf-8",
    )
    manifest = reports / "run.workers.manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "status": "active",
                "workers": [
                    {
                        "worker_id": "worker-001",
                        "status": "running",
                        "pid": 999999,
                        "lease_seconds": 1,
                        "heartbeat_at": "2020-01-01T00:00:00+00:00",
                        "workspace": {"project_root": str(worker_root / "project")},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    result = workers_module.inspect_worker_workspaces(reports)
    assert result["orphaned_workers"] == ["worker-001"]
    assert json.loads(manifest.read_text(encoding="utf-8"))["status"] == "active"


def test_failed_process_worker_gets_one_bounded_retry(tmp_path: Path, monkeypatch) -> None:
    # Retry a report-less process failure once while retaining the first failure diagnostic.
    manifest_path = tmp_path / "run.workers.manifest.json"
    manifest_path.write_text(json.dumps({"status": "active", "workers": []}), encoding="utf-8")
    shard = WorkerShard("worker-001", ("mutant-001",), 1.0)
    records = {
        "worker-001": {
            "worker_id": "worker-001",
            "status": "error",
            "error": "worker process disappeared",
            "report": None,
            "workspace": {},
        }
    }
    manifest = {"status": "active", "workers": []}
    calls: list[str] = []

    def fake_retry(*args, **kwargs):
        # Return a deterministic successful second attempt without starting an OS process in the unit test.
        calls.append(args[2].worker_id)
        return {
            "worker_id": args[2].worker_id,
            "status": "complete",
            "report": {"status": "complete", "results": []},
            "workspace": {},
        }

    monkeypatch.setattr(workers_module, "_retry_worker_once", fake_retry)
    workers_module._retry_failed_workers(
        None,
        "run",
        (shard,),
        {},
        {},
        StaticBaselineProvider([]),
        records,
        manifest,
        manifest_path,
        tmp_path,
    )
    saved = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert calls == ["worker-001"]
    assert records["worker-001"]["status"] == "complete"
    assert records["worker-001"]["retry_count"] == 1
    assert records["worker-001"]["initial_error"] == "worker process disappeared"
    assert saved["workers"][0]["retry_run_id"] == "run.retry1"
