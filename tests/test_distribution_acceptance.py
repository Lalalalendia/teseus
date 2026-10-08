from __future__ import annotations

import json
import os
import subprocess
import venv
import zipfile
from pathlib import Path

import pytest


WHEEL_ENV = "THESEUS_RELEASE_WHEEL"


def _run(python: Path, *args: str, cwd: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(python), *args],
        cwd=str(cwd),
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )


@pytest.mark.release
@pytest.mark.skipif(not os.environ.get(WHEEL_ENV), reason="release acceptance requires THESEUS_RELEASE_WHEEL")
def test_clean_wheel_is_self_contained_and_runs_full_installed_acceptance(tmp_path: Path) -> None:
    wheel = Path(os.environ[WHEEL_ENV]).resolve()
    assert wheel.is_file()
    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
    for required in (
        "theseus_local/remote_worker.py",
        "theseus_local/distributed.py",
        "theseus_local/locking.py",
        "theseus_local/artifact_store.py",
        "theseus_contracts/remote_protocol.py",
        "theseus_local/network_transport.py",
        "theseus_local/network_security.py",
        "theseus_local/network_artifacts.py",
        "theseus_local/network_control.py",
        "theseus_local/coordinator_ha.py",
    ):
        assert required in names

    venv_dir = tmp_path / "clean-runtime"
    venv.EnvBuilder(with_pip=True, clear=True).create(venv_dir)
    installed_python = venv_dir / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    clean_env = dict(os.environ)
    clean_env.pop("PYTHONPATH", None)
    _run(
        installed_python,
        "-m",
        "pip",
        "install",
        "--no-index",
        "--no-deps",
        "--force-reinstall",
        str(wheel),
        cwd=tmp_path,
        env=clean_env,
    )

    runtime = json.loads(_run(installed_python, "-m", "theseus_local", "runtime", "--json", cwd=tmp_path, env=clean_env).stdout)
    assert runtime["protocol_versions"]["remote_execution"] == 1
    assert runtime["schema_versions"]["remote_execution"] == 1
    assert runtime["runtime_fingerprint"]

    for project_root in (tmp_path / "Teseus Test", tmp_path / "Тест Тесей"):
        project_root.mkdir()
        (project_root / "app.py").write_text("def classify(value):\n    return value + 1\n", encoding="utf-8")
        (project_root / "test_runner.py").write_text(
            "from app import classify\n\ndef test_classify():\n    assert classify(1) == 2\n\nif __name__ == '__main__':\n    test_classify()\n",
            encoding="utf-8",
        )
        _run(installed_python, "test_runner.py", cwd=project_root, env=clean_env)
        reports = tmp_path / f"reports-{len(list(tmp_path.iterdir()))}"
        summary = json.loads(
            _run(
                installed_python,
                "-m",
                "theseus_local",
                "run",
                str(project_root),
                "app.py",
                "--function",
                "classify",
                "--max-mutants",
                "1",
                "--workers",
                "1",
                "--no-escalation",
                "--reports-dir",
                str(reports),
                "--json",
                "--test-command",
                str(installed_python),
                "-B",
                "-c",
                "from app import classify; assert classify(1) == 2",
                cwd=project_root,
                env=clean_env,
            ).stdout
        )
        assert summary["status"] == "completed"
        report_path = Path(summary["report_path"])
        assert report_path.is_file()
        canonical_before = report_path.read_bytes()
        analysis = json.loads(
            _run(
                installed_python,
                "-m",
                "theseus_local",
                "analysis",
                "report",
                str(report_path),
                "--provider",
                "deterministic",
                "--json",
                cwd=tmp_path,
                env=clean_env,
            ).stdout
        )
        assert analysis["advisory"] is True
        operations = json.loads(
            _run(
                installed_python,
                "-m",
                "theseus_local",
                "operations",
                "list",
                str(reports),
                "--json",
                cwd=tmp_path,
                env=clean_env,
            ).stdout
        )
        assert operations[0]["campaign_id"] == summary["campaign_id"]
        assert report_path.read_bytes() == canonical_before

    remote_script = tmp_path / "remote_smoke.py"
    remote_script.write_text(
        """
import json
import subprocess
import sys
from pathlib import Path
from theseus_contracts import RemoteExecutionRequest
from theseus_local.artifact_store import ContentAddressedArtifactStore
from theseus_local.runtime_identity import current_runtime_identity

root = Path.cwd() / 'remote-project'
root.mkdir()
(root / 'module.py').write_text('VALUE = 1\\n', encoding='utf-8')
store = ContentAddressedArtifactStore(Path.cwd() / 'remote-cache')
snapshot = store.create_snapshot(root)
store.persist_snapshot(snapshot)
prepared = store.put_bytes(b'prepared')
request = RemoteExecutionRequest(
    execution_attempt_id='remote-attempt',
    evidence_identity='remote-evidence',
    mutation_identity='remote-mutation',
    runtime_identity=current_runtime_identity().to_dict(),
    project_snapshot_id=snapshot.snapshot_id,
    prepared_artifact_id=prepared.artifact_id,
    test_plan_identity='remote-plan',
    argv=(sys.executable, '-c', "print('ok')"),
    source_path='module.py',
    expected_source_sha256=snapshot.files['module.py'],
)
worker_root = Path.cwd() / 'jsonl-worker'
worker_store = ContentAddressedArtifactStore(worker_root / 'cache')
worker_store.transfer_snapshot(store, snapshot.snapshot_id, prepared_artifact_ids=(prepared.artifact_id,))
process = subprocess.Popen(
    [sys.executable, '-m', 'theseus_local.remote_worker', '--worker-id', 'jsonl', '--root', str(worker_root)],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    text=True,
)
assert process.stdin is not None and process.stdout is not None
registration = json.loads(process.stdout.readline())
assert registration['message_type'] == 'registration'
process.stdin.write(request.to_json() + '\\n{"message_type":"shutdown"}\\n')
process.stdin.close()
frames = [json.loads(line) for line in process.stdout if line.strip()]
assert process.wait(timeout=30) == 0
result = next(frame['result'] for frame in frames if frame.get('message_type') == 'result')
print(json.dumps(result, sort_keys=True))
""",
        encoding="utf-8",
    )
    remote = json.loads(_run(installed_python, str(remote_script), cwd=tmp_path, env=clean_env).stdout)
    assert remote["started"] is True
    assert remote["workspace_integrity"] == "verified"

    network_script = tmp_path / "network_smoke.py"
    network_script.write_text(
        """
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from theseus_contracts import RemoteExecutionRequest
from theseus_local.artifact_store import ContentAddressedArtifactStore
from theseus_local.network_control import NetworkCoordinator
from theseus_local.network_security import EnrollmentAuthority
from theseus_local.runtime_identity import current_runtime_identity

root = Path.cwd() / 'network-project'
root.mkdir()
(root / 'module.py').write_text('VALUE = 1\\n', encoding='utf-8')
store = ContentAddressedArtifactStore(Path.cwd() / 'network-cache')
snapshot = store.create_snapshot(root)
store.persist_snapshot(snapshot)
prepared = store.put_bytes(b'network-prepared')
runtime = current_runtime_identity()
request = RemoteExecutionRequest(
    execution_attempt_id='socket-attempt',
    evidence_identity='socket-evidence',
    mutation_identity='socket-mutation',
    runtime_identity=runtime.to_dict(),
    project_snapshot_id=snapshot.snapshot_id,
    prepared_artifact_id=prepared.artifact_id,
    test_plan_identity='socket-plan',
    argv=(sys.executable, '-c', "print('socket-ok')"),
    source_path='module.py',
    expected_source_sha256=snapshot.files['module.py'],
)
secret = 'clean-wheel-network-secret-0123456789'
enrollment = EnrollmentAuthority(Path.cwd() / 'enrollment.json', secret)
enrollment.authorize('clean-worker', runtime_fingerprint=runtime.runtime_fingerprint)
coordinator = NetworkCoordinator(
    state_path=Path.cwd() / 'network-scheduler.json',
    artifact_store=store,
    enrollment=enrollment,
)
address = coordinator.start()
worker_root = Path.cwd() / 'clean-worker'
worker = subprocess.Popen(
    [sys.executable, '-m', 'theseus_local', 'worker', 'start', '--host', address[0], '--port', str(address[1]), '--secret', secret, '--worker-id', 'clean-worker', '--root', str(worker_root), '--json'],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.PIPE,
    text=True,
)
try:
    assert coordinator.wait_for_workers(timeout_seconds=15.0) == ('clean-worker',)
    report = coordinator.run((request,), timeout_seconds=30.0, campaign_id='clean-wheel-network')
    assert report.semantic_outcomes == {'socket-evidence': 'survived'}
    assert report.results[0].workspace_integrity == 'verified'
finally:
    worker.terminate()
    try:
        worker.wait(timeout=10)
    except subprocess.TimeoutExpired:
        worker.kill()
        worker.wait(timeout=10)
    coordinator.stop()
print(json.dumps({'network': 'ok'}))
""",
        encoding="utf-8",
    )
    network = json.loads(_run(installed_python, str(network_script), cwd=tmp_path, env=clean_env).stdout)
    assert network["network"] == "ok"


@pytest.mark.release
@pytest.mark.skipif(not os.environ.get(WHEEL_ENV), reason="release acceptance requires THESEUS_RELEASE_WHEEL")
def test_incomplete_wheel_is_rejected_by_installed_runtime_gate(tmp_path: Path) -> None:
    wheel = Path(os.environ[WHEEL_ENV]).resolve()
    broken_wheel = tmp_path / wheel.name
    with zipfile.ZipFile(wheel) as source, zipfile.ZipFile(broken_wheel, "w", zipfile.ZIP_DEFLATED) as target:
        for info in source.infolist():
            if info.filename == "theseus_local/remote_worker.py":
                continue
            target.writestr(info, source.read(info.filename))

    venv_dir = tmp_path / "broken-runtime"
    venv.EnvBuilder(with_pip=True, clear=True).create(venv_dir)
    installed_python = venv_dir / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    clean_env = dict(os.environ)
    clean_env.pop("PYTHONPATH", None)
    _run(
        installed_python,
        "-m",
        "pip",
        "install",
        "--no-index",
        "--no-deps",
        "--force-reinstall",
        str(broken_wheel),
        cwd=tmp_path,
        env=clean_env,
    )
    result = subprocess.run(
        [str(installed_python), "-m", "theseus_local", "runtime", "--json"],
        cwd=str(tmp_path),
        env=clean_env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode != 0
    diagnostic = f"{result.stdout}\n{result.stderr}".lower()
    assert "remote_worker" in diagnostic
    assert "modulenotfounderror" in diagnostic or "no module named" in diagnostic
