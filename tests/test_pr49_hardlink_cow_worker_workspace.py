from __future__ import annotations
import errno
import os
import subprocess
import sys
from pathlib import Path
import pytest
from test_intelligence_unified_v1 import runner as runner_module
from test_intelligence_unified_v1.io_utils import atomic_write_bytes
from test_intelligence_unified_v1.runner import MutationConfig, MutationRunner
from test_intelligence_unified_v1.workspace_cow import copy_up_hardlink
from theseus_local.worker_pool import prepare_worker_workspace, worker_workspace_backend
from theseus_local.worker_runtime.agent import WorkerAgent
import theseus_local.worker_pool as worker_pool_module
def _project(root: Path) -> None:
    # Create a tiny project with multiple immutable inputs so link sharing and selective copy-up are visible.
    root.mkdir(parents=True)
    (root / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
    (root / "data.txt").write_text("baseline\n", encoding="utf-8")
def _worker_workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    # Materialize one worker while rejecting the old destination-wide content rehash on the creation path.
    source = tmp_path / "campaign"
    destination = tmp_path / "worker" / "workspace"
    _project(source)
    original_fingerprint = worker_pool_module._tree_fingerprint
    def tracked_fingerprint(path: Path) -> str:
        # Allow source identity hashing but fail if fresh materialization rereads every destination byte.
        if Path(path).resolve() == destination.resolve():
            raise AssertionError("fresh hardlink workspace performed a destination content rehash")
        return original_fingerprint(path)
    monkeypatch.setattr(worker_pool_module, "_tree_fingerprint", tracked_fingerprint)
    prepare_worker_workspace(source, destination, worker_id="worker-000", campaign_id="campaign-1")
    return source, destination
def test_worker_workspace_hardlinks_project_bytes_and_mutant_replace_is_copy_on_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Share immutable bytes across workers while letting the existing atomic mutant install privatize only the target.
    source, worker = _worker_workspace(tmp_path, monkeypatch)
    assert worker_workspace_backend(worker) == "hardlink-cow"
    assert os.path.samefile(source / "app.py", worker / "app.py")
    assert os.path.samefile(source / "data.txt", worker / "data.txt")
    atomic_write_bytes(worker / "app.py", b"VALUE = 2\n", durability="normal")
    assert (source / "app.py").read_text(encoding="utf-8") == "VALUE = 1\n"
    assert (worker / "app.py").read_text(encoding="utf-8") == "VALUE = 2\n"
    assert not os.path.samefile(source / "app.py", worker / "app.py")
    assert os.path.samefile(source / "data.txt", worker / "data.txt")
def test_worker_copy_on_write_does_not_cross_worker_boundaries(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Keep two worker directories byte-shared until one worker replaces its target and leaves the other untouched.
    source = tmp_path / "campaign-multi"
    first = tmp_path / "worker-first" / "workspace"
    second = tmp_path / "worker-second" / "workspace"
    _project(source)
    prepare_worker_workspace(source, first, worker_id="worker-first", campaign_id="campaign-multi")
    prepare_worker_workspace(source, second, worker_id="worker-second", campaign_id="campaign-multi")
    if worker_workspace_backend(first) != "hardlink-cow" or worker_workspace_backend(second) != "hardlink-cow":
        pytest.skip("filesystem does not support hardlinks for this test")
    assert os.path.samefile(source / "app.py", first / "app.py")
    assert os.path.samefile(source / "app.py", second / "app.py")
    atomic_write_bytes(first / "app.py", b"VALUE = 9\n", durability="normal")
    assert (source / "app.py").read_text(encoding="utf-8") == "VALUE = 1\n"
    assert (second / "app.py").read_text(encoding="utf-8") == "VALUE = 1\n"
    assert (first / "app.py").read_text(encoding="utf-8") == "VALUE = 9\n"
    assert not os.path.samefile(first / "app.py", second / "app.py")
def test_python_write_copies_up_one_existing_hardlink_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Break sharing for one test-written file without copying unrelated project inputs.
    source, worker = _worker_workspace(tmp_path, monkeypatch)
    assert copy_up_hardlink(worker / "data.txt", root=worker) is True
    (worker / "data.txt").write_text("worker\n", encoding="utf-8")
    assert (source / "data.txt").read_text(encoding="utf-8") == "baseline\n"
    assert (worker / "data.txt").read_text(encoding="utf-8") == "worker\n"
    assert os.path.samefile(source / "app.py", worker / "app.py")
def test_runner_privatizes_hardlink_workspace_once_before_external_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Detach one worker tree once so child processes need no process-global COW interception.
    source, worker = _worker_workspace(tmp_path, monkeypatch)
    if worker_workspace_backend(worker) != "hardlink-cow":
        pytest.skip("filesystem does not support hardlinks for this test")
    original_privatize = runner_module.privatize_hardlinked_tree
    calls = 0
    def tracked_privatize(root: Path) -> int:
        # Count the coarse-grained detach while preserving the production implementation.
        nonlocal calls
        calls += 1
        return original_privatize(root)
    monkeypatch.setattr(runner_module, "privatize_hardlinked_tree", tracked_privatize)
    runner = MutationRunner(MutationConfig(project_root=worker, source="app.py", workspace_backend="hardlink-cow"))
    runner._ensure_private_worker_workspace()
    runner._ensure_private_worker_workspace()
    assert calls == 1
    assert not os.path.samefile(source / "app.py", worker / "app.py")
    assert not os.path.samefile(source / "data.txt", worker / "data.txt")
    subprocess.run(
        [sys.executable, "-c", "from pathlib import Path; Path('data.txt').write_text('child\\n', encoding='utf-8')"],
        cwd=worker,
        check=True,
    )
    assert (source / "data.txt").read_text(encoding="utf-8") == "baseline\n"
    assert (worker / "data.txt").read_text(encoding="utf-8") == "child\n"
def test_runner_privatizes_before_pytest_launch_without_cow_audit_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Make the worker private before pytest starts and keep COW interception out of the child environment.
    source, worker = _worker_workspace(tmp_path, monkeypatch)
    if worker_workspace_backend(worker) != "hardlink-cow":
        pytest.skip("filesystem does not support hardlinks for this test")
    runner = MutationRunner(MutationConfig(project_root=worker, source="app.py", workspace_backend="hardlink-cow"))
    def passthrough_command(argv: object) -> tuple[str, ...]:
        # Preserve the fake pytest command without invoking instrumentation internals.
        return tuple(str(item) for item in argv)
    def pytest_command(argv: object) -> bool:
        # Force this unit test through the pytest-specific environment path.
        del argv
        return True
    def empty_test_env(*args: object, **kwargs: object) -> dict[str, str]:
        # Supply only the environment entries added directly by the runner boundary.
        del args, kwargs
        return {}
    monkeypatch.setattr(runner_module, "instrument_pytest_command", passthrough_command)
    monkeypatch.setattr(runner_module, "is_pytest_command", pytest_command)
    monkeypatch.setattr(runner_module, "build_test_stats_env", empty_test_env)
    def stop_after_boundary(command: object, **kwargs: object) -> object:
        # Assert the exact pre-subprocess authority boundary without starting a real pytest process.
        del command
        env = kwargs.get("env")
        assert isinstance(env, dict)
        assert "TI_COW_WORKSPACE_ROOT" not in env
        assert not os.path.samefile(source / "app.py", worker / "app.py")
        assert not os.path.samefile(source / "data.txt", worker / "data.txt")
        raise RuntimeError("process boundary reached")
    monkeypatch.setattr(runner_module, "run_argv", stop_after_boundary)
    with pytest.raises(RuntimeError, match="process boundary reached"):
        runner._run_test_command(
            (sys.executable, "-m", "pytest", "-q"),
            phase="mutant",
            level="L1",
            mutant_id="m1",
            target_sha256="abc",
            report_id="run-1",
            output_artifact=tmp_path / "pytest.out",
            timeout_seconds=1.0,
        )
def test_pytest_runtime_cow_audit_hook_is_not_on_the_execution_path() -> None:
    # Prevent the high-frequency process-global audit hook from returning to mutant pytest execution.
    root = Path(__file__).parents[1]
    runner_source = (root / "runner.py").read_text(encoding="utf-8")
    plugin_source = (root / "pytest_plugin.py").read_text(encoding="utf-8")
    assert "TI_COW_WORKSPACE_ROOT" not in runner_source
    assert "_cow_audit_event" not in plugin_source
    assert "workspace_cow" not in plugin_source
def test_worker_workspace_falls_back_to_copy_when_hardlinks_are_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Preserve correctness across filesystems that cannot hardlink the campaign image.
    source = tmp_path / "campaign-copy"
    destination = tmp_path / "worker-copy" / "workspace"
    _project(source)
    def unavailable_link(source_file: str, destination_file: str) -> None:
        # Simulate a cross-volume worker state root where hardlinks are impossible.
        del source_file, destination_file
        raise OSError(errno.EXDEV, "cross-device link")
    monkeypatch.setattr(os, "link", unavailable_link)
    prepare_worker_workspace(source, destination, worker_id="worker-copy", campaign_id="campaign-copy")
    assert worker_workspace_backend(destination) == "copy"
    assert not os.path.samefile(source / "app.py", destination / "app.py")
    assert (destination / "app.py").read_bytes() == (source / "app.py").read_bytes()
def test_worker_advertises_hardlink_cow_and_copy_fallback() -> None:
    # Keep scheduler capability checks aligned with the backend written into worker ownership manifests.
    assert WorkerAgent.local_capabilities().workspace_backends == ("hardlink-cow", "copy")
def test_coordinator_binds_the_materialized_workspace_backend() -> None:
    # Prevent scheduler authority from silently claiming copy after a hardlink-COW workspace was selected.
    root = Path(__file__).parents[1]
    source = (root / "theseus_local" / "coordinator.py").read_text(encoding="utf-8")
    assert "worker_workspace_backend(worker_workspace)" in source
    assert 'workspace_backend="copy"' not in source
