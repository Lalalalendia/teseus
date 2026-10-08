from __future__ import annotations
import os
from pathlib import Path
from theseus_local.worker_runtime import PersistentWorkerProcess
def test_standalone_worker_registers_heartbeats_acquires_and_shuts_down(tmp_path: Path) -> None:
    # Prove the worker lifecycle remains available as the stable parent of worker-owned engine children.
    with PersistentWorkerProcess(
        worker_id="persistent-worker-001",
        spool_root=tmp_path / "spool",
        heartbeat_interval_seconds=0.02,
        cwd=Path(__file__).parents[1],
    ) as worker:
        registered = worker.wait_for("registered")
        assert registered["process_id"] == worker.pid
        assert worker.pid != os.getpid()
        assert registered["payload"]["identity"]["worker_id"] == "persistent-worker-001"
        assert registered["payload"]["identity"]["process_birth_token"]
        worker.wait_for("heartbeat")
        first_acquire = worker.wait_for("acquire")
        assert first_acquire["state"] == "idle"
        worker.no_assignment(wait_seconds=0.01)
        second_acquire = worker.wait_for("acquire")
        assert second_acquire["process_id"] == worker.pid
        worker.shutdown()
        terminated = worker.wait_for("terminated")
        assert terminated["state"] == "terminated"
        assert worker.wait() == 0
def test_project_exposes_the_standalone_worker_command() -> None:
    # Keep the installed worker entrypoint distinct from the campaign CLI process.
    pyproject = (Path(__file__).parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    assert 'theseus-worker = "theseus_local.worker_runtime.entrypoint:main"' in pyproject
