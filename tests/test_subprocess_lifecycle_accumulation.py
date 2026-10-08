from __future__ import annotations
import subprocess
import sys
import time
from pathlib import Path
import pytest
from test_intelligence_unified_v1.commands import active_child_process_ids
from test_intelligence_unified_v1.recovery import current_process_birth_token
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
from theseus_local.process import EngineProcessSession
from theseus_local.worker_runtime import PersistentWorkerProcess, WorkerProcessError

def _wait_until_gone(process_id: int, timeout: float = 5.0) -> bool:
    # Poll one PID until the original process incarnation is no longer observable.
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if current_process_birth_token(process_id) is None:
            return True
        time.sleep(0.05)
    return current_process_birth_token(process_id) is None

def test_close_terminates_redirected_registered_worker_after_launcher_exit(tmp_path: Path) -> None:
    # Reproduce a Windows-style launcher redirect and require cleanup of the actual registered worker PID.
    pid_path = tmp_path / "redirected-worker.pid"
    launcher_script = tmp_path / "redirected_launcher.py"
    launcher_script.write_text(
        "import os, pathlib, subprocess, sys\n"
        "child = subprocess.Popen(\n"
        "    (sys.executable, '-c', 'import time; time.sleep(60)'),\n"
        "    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,\n"
        "    start_new_session=os.name != 'nt',\n"
        ")\n"
        "pathlib.Path(sys.argv[1]).write_text(str(child.pid), encoding='utf-8')\n",
        encoding="utf-8",
    )
    launcher = subprocess.Popen(
        (sys.executable, str(launcher_script), str(pid_path)),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    assert launcher.wait(timeout=5.0) == 0
    registered_worker_pid = int(pid_path.read_text(encoding="utf-8"))
    worker = PersistentWorkerProcess(
        worker_id="redirected-worker-cleanup",
        spool_root=tmp_path / "spool",
    )
    worker._process = launcher
    worker._worker_pid = registered_worker_pid
    worker._worker_process_birth_token = current_process_birth_token(registered_worker_pid)
    worker._register_process_id(launcher.pid)
    worker._register_process_id(registered_worker_pid)
    try:
        assert worker._worker_process_birth_token, (
            "redirected worker fixture has no birth token; "
            f"launcher_pid={launcher.pid}; worker_pid={registered_worker_pid}"
        )
        worker.close()
        assert _wait_until_gone(registered_worker_pid), (
            "registered worker survived parent-adapter close after its launcher exited; "
            f"launcher_pid={launcher.pid}; worker_pid={registered_worker_pid}; "
            f"birth_token={worker._worker_process_birth_token}; "
            f"active_worker_pids={sorted(PersistentWorkerProcess.active_process_ids())}"
        )
        assert registered_worker_pid not in PersistentWorkerProcess.active_process_ids(), (
            "registered worker PID remained in the parent lifecycle registry after exact termination; "
            f"worker_pid={registered_worker_pid}; "
            f"active_worker_pids={sorted(PersistentWorkerProcess.active_process_ids())}"
        )
    finally:
        if current_process_birth_token(registered_worker_pid) is not None:
            PersistentWorkerProcess._terminate_pid_tree(registered_worker_pid)
        PersistentWorkerProcess._unregister_process_id(launcher.pid)
        PersistentWorkerProcess._unregister_process_id(registered_worker_pid)

def test_runtime_lifecycle_registries_are_empty_after_repeated_worker_cleanup(tmp_path: Path) -> None:
    # Close repeated persistent workers and prove no PID or non-daemon transport thread accumulates.
    for ordinal in range(3):
        spool_root = tmp_path / f"spool-{ordinal}"
        with PersistentWorkerProcess(
            worker_id=f"worker-cleanup-{ordinal}",
            instance_id=f"instance-cleanup-{ordinal}",
            spool_root=spool_root,
            heartbeat_interval_seconds=0.01,
            cwd=Path(__file__).parents[1],
        ) as worker:
            registered = worker.wait_for("registered")
            worker_pid = int(registered["process_id"])
            worker.wait_for("acquire")
            worker.shutdown()
            worker.wait_for("terminated")
            assert worker.wait(timeout=10.0) == 0, (
                "persistent worker did not terminate cleanly in repeated cleanup fixture; "
                f"ordinal={ordinal}; worker_pid={worker_pid}; stderr={worker.stderr_text!r}"
            )
        assert not PersistentWorkerProcess.active_process_ids(), (
            "worker process registry accumulated a PID across sequential cleanup boundaries; "
            f"ordinal={ordinal}; active_worker_pids={sorted(PersistentWorkerProcess.active_process_ids())}"
        )
        assert not PersistentWorkerProcess.active_lifecycle_thread_names(), (
            "non-daemon worker transport thread survived a sequential cleanup boundary; "
            f"ordinal={ordinal}; threads={PersistentWorkerProcess.active_lifecycle_thread_names()}"
        )
        assert not EngineProcessSession.active_process_ids(), (
            "engine process registry was not empty after an idle worker shutdown; "
            f"ordinal={ordinal}; active_engine_pids={sorted(EngineProcessSession.active_process_ids())}"
        )
        assert not active_child_process_ids(), (
            "command subprocess registry was not empty after repeated worker cleanup; "
            f"ordinal={ordinal}; active_command_pids={sorted(active_child_process_ids())}"
        )

def test_coordinator_fails_closed_when_worker_cleanup_reports_a_leak(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Surface a post-execution cleanup failure instead of marking the campaign successful and leaking state.
    (tmp_path / "app.py").write_text(
        "def choose(value):\n"
        "    if value > 0:\n"
        "        return 1\n"
        "    return 0\n",
        encoding="utf-8",
    )
    (tmp_path / "test_app.py").write_text(
        "from app import choose\n\ndef test_choose():\n    assert choose(1) == 1\n",
        encoding="utf-8",
    )
    configuration = CampaignConfiguration(
        campaign_id=CampaignId("campaign-cleanup-fail-closed"),
        project=ProjectDescriptor(
            project_id=ProjectId("project-cleanup-fail-closed"),
            display_name="Subprocess cleanup fail-closed fixture",
            root_path=str(tmp_path),
            test_command=TestCommandDescriptor((sys.executable, "-m", "pytest", "-q")),
            pytest_plugin_autoload=False,
        ),
        scope=MutationScope(
            source_path="app.py",
            function="choose",
            operators=("condition_to_not",),
        ),
        budget=CampaignBudget(max_mutants=1, max_workers=1, max_test_seconds=5.0),
        no_escalation=True,
        reports_dir=str(tmp_path / "reports"),
    )
    original_close = PersistentWorkerProcess.close
    injected = {"raised": False}
    def close_with_injected_failure(self: PersistentWorkerProcess) -> None:
        # Complete real cleanup first, then model the diagnostic produced by a detected residual handle.
        original_close(self)
        if not injected["raised"]:
            injected["raised"] = True
            raise WorkerProcessError("injected post-cleanup lifecycle residue")
    monkeypatch.setattr(PersistentWorkerProcess, "close", close_with_injected_failure)
    result = LocalCampaignCoordinator().run(configuration)
    assert injected["raised"], (
        "cleanup failure fixture did not reach the worker close boundary; "
        f"campaign={configuration.campaign_id.value}; error={result.error!r}; "
        f"database={result.database_path}"
    )
    assert not result.succeeded and result.error is not None, (
        "coordinator hid a worker cleanup failure and reported campaign success; "
        f"campaign={configuration.campaign_id.value}; status={result.campaign.status}; "
        f"error={result.error!r}; database={result.database_path}"
    )
    assert "worker cleanup failed" in result.error and "injected post-cleanup lifecycle residue" in result.error, (
        "coordinator error lost the exact subprocess cleanup diagnostic; "
        f"campaign={configuration.campaign_id.value}; error={result.error!r}; "
        f"database={result.database_path}"
    )
