from __future__ import annotations

import inspect
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


from test_intelligence_unified_v1.models import PerformanceMetrics
from test_intelligence_unified_v1.runner import MutationRunner
from test_intelligence_unified_v1.runner import MutationConfig
from theseus_local.execution_backend import (
    ExecutionRequest,
    LocalProcessBackend,
)


def _request(
    tmp_path: Path,
    execution_id: str,
    code: str,
    *,
    timeout_seconds: float = 5.0,
    environment: dict[str, str] | None = None,
    output_artifact: Path | None = None,
    cancellation=None,
) -> ExecutionRequest:
    # Build one shell-free backend request with explicit identity, cwd, environment and output facts.
    cwd = tmp_path / "проект с пробелом"
    cwd.mkdir(parents=True, exist_ok=True)
    return ExecutionRequest(
        execution_id=execution_id,
        argv=(sys.executable, "-c", code),
        cwd=cwd,
        environment=environment,
        timeout_seconds=timeout_seconds,
        output_artifact=output_artifact,
        cancellation=cancellation,
    )


def test_backend_preserves_exit_output_environment_cwd_and_artifact(tmp_path: Path) -> None:
    # Keep process facts intact across spaces, Unicode, explicit environment and durable output storage.
    artifact = tmp_path / "артефакты" / "stdout output.log"
    result = LocalProcessBackend().execute(
        _request(
            tmp_path,
            "execution-contract-basic",
            "import os; print(os.environ['THESEUS_CONTRACT_VALUE'])",
            environment={**os.environ, "THESEUS_CONTRACT_VALUE": "значение с пробелом"},
            output_artifact=artifact,
        )
    )
    assert result.exit_code == 0
    assert result.timed_out is False
    assert "значение с пробелом" in result.output
    assert "проект с пробелом" in result.cwd
    assert artifact.read_text(encoding="utf-8").strip() == "значение с пробелом"
    assert result.output_sha256


def test_backend_timeout_kills_process_tree_and_reports_timeout(tmp_path: Path) -> None:
    # Enforce the existing timeout/tree-kill guarantee through the extracted backend boundary.
    result = LocalProcessBackend().execute(
        _request(
            tmp_path,
            "execution-contract-timeout",
            "import time; time.sleep(30)",
            timeout_seconds=0.15,
        )
    )
    assert result.timed_out is True
    assert result.exit_code is None
    assert result.termination is not None
    assert result.process_tree_leak is False


def test_backend_cancellation_kills_running_process(tmp_path: Path) -> None:
    # Treat a cancellation request as a physical process stop rather than a semantic mutant result.
    started = time.monotonic()
    result = LocalProcessBackend().execute(
        _request(
            tmp_path,
            "execution-contract-cancel",
            "import time; time.sleep(30)",
            timeout_seconds=5.0,
            cancellation=lambda: time.monotonic() - started > 0.15,
        )
    )
    assert result.exit_code is None
    assert result.timed_out is False
    assert result.termination is not None
    assert result.termination.get("reason") == "cancelled"


def test_backend_reports_worker_crash_without_reclassifying_it(tmp_path: Path) -> None:
    # Return the child exit fact unchanged so the coordinator can apply its own infrastructure policy.
    result = LocalProcessBackend().execute(
        _request(tmp_path, "execution-contract-crash", "raise SystemExit(23)")
    )
    assert result.exit_code == 23
    assert result.timed_out is False


def test_backend_supports_concurrent_requests_with_distinct_facts(tmp_path: Path) -> None:
    # Prove the backend has no shared mutable process state across concurrent local requests.
    backend = LocalProcessBackend()
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(
            pool.map(
                lambda index: backend.execute(
                    _request(
                        tmp_path,
                        f"execution-contract-concurrent-{index}",
                        f"print({index})",
                    )
                ),
                range(4),
            )
        )
    assert [item.exit_code for item in results] == [0, 0, 0, 0]
    assert {item.output.strip() for item in results} == {"0", "1", "2", "3"}


def test_backend_recovery_replay_has_equivalent_physical_facts(tmp_path: Path) -> None:
    # Replaying the same durable request remains equivalent without depending on a previous interpreter.
    backend = LocalProcessBackend()
    first = backend.execute(_request(tmp_path, "execution-contract-replay", "print('stable')"))
    second = backend.execute(_request(tmp_path, "execution-contract-replay", "print('stable')"))
    assert (first.exit_code, first.timed_out, first.output) == (second.exit_code, second.timed_out, second.output)


def test_runner_routes_test_processes_through_backend(tmp_path: Path) -> None:
    # Keep subprocess construction out of the mutation runner and behind the physical execution SPI.
    runner = MutationRunner(MutationConfig(project_root=tmp_path, source="app.py"))
    source = inspect.getsource(runner._run_test_command)
    assert "self.execution_backend.execute" in source
    assert "subprocess" not in source


def test_backend_exposes_process_facts_without_mutation_policy(tmp_path: Path) -> None:
    # Confirm the backend API carries no selection, reuse or semantic mutant policy fields.
    request = _request(tmp_path, "execution-contract-policy", "print('ok')")
    facts = LocalProcessBackend().execute_facts(request, metrics=PerformanceMetrics())
    assert facts.execution_id == request.execution_id
    assert facts.process.exit_code == 0
    assert not hasattr(facts, "semantic_result")
