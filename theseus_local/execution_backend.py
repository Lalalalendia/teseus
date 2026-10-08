"""Local execution backend SPI for shell-free test and mutation processes."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Protocol

from test_intelligence_unified_v1.commands import run_argv
from test_intelligence_unified_v1.models import PerformanceMetrics, ProcessResult


@dataclass(frozen=True, slots=True)
class ExecutionRequest:
    """Physical process request independent from mutation semantics or selection policy."""

    execution_id: str
    argv: tuple[str, ...]
    cwd: Path
    environment: Mapping[str, str] | None = None
    timeout_seconds: float = 120.0
    output_artifact: Path | None = None
    retry: bool = False
    cancellation: Callable[[], bool] | None = None
    deadline_monotonic: float | None = None

    def __post_init__(self) -> None:
        # Validate only physical launch facts at the backend boundary.
        if not str(self.execution_id).strip():
            raise ValueError("execution_id must be non-empty")
        if not self.argv or any(not str(item) for item in self.argv):
            raise ValueError("argv must be non-empty")
        if not isinstance(self.cwd, Path):
            raise TypeError("cwd must be a Path")
        if float(self.timeout_seconds) <= 0.0:
            raise ValueError("timeout_seconds must be greater than zero")
        if self.deadline_monotonic is not None and float(self.deadline_monotonic) <= 0.0:
            raise ValueError("deadline_monotonic must be positive")


@dataclass(frozen=True, slots=True)
class ExecutionFacts:
    """Physical facts returned by one backend attempt."""

    execution_id: str
    process: ProcessResult

    def to_dict(self) -> dict[str, object]:
        # Keep backend facts JSON-safe without adding mutation result interpretation.
        return {
            "execution_id": self.execution_id,
            "process": self.process.to_dict(),
        }


class ExecutionBackend(Protocol):
    """Minimal process execution port consumed by the mutation runner."""

    def execute(
        self,
        request: ExecutionRequest,
        *,
        metrics: PerformanceMetrics | None = None,
    ) -> ProcessResult:
        # Execute one physical request and return process facts only.
        ...


Executor = Callable[..., ProcessResult]


class LocalProcessBackend:
    """Default local backend preserving the existing process-tree safety contract."""

    def __init__(self, *, executor: Executor = run_argv) -> None:
        # Allow compatibility tests and packaged launchers to replace only the physical executor.
        self._executor = executor

    def execute(
        self,
        request: ExecutionRequest,
        *,
        metrics: PerformanceMetrics | None = None,
    ) -> ProcessResult:
        # Route one request through the shell-free process implementation without interpreting its result.
        kwargs: dict[str, object] = {
            "cwd": request.cwd,
            "timeout_seconds": request.timeout_seconds,
            "env": request.environment,
            "retry": request.retry,
            "output_artifact": request.output_artifact,
            "metrics": metrics,
        }
        if request.cancellation is not None:
            kwargs["cancellation"] = request.cancellation
        if request.deadline_monotonic is not None:
            kwargs["deadline_monotonic"] = request.deadline_monotonic
        return self._executor(request.argv, **kwargs)

    def execute_facts(
        self,
        request: ExecutionRequest,
        *,
        metrics: PerformanceMetrics | None = None,
    ) -> ExecutionFacts:
        # Attach the request identity to process facts without changing the ProcessResult contract.
        return ExecutionFacts(request.execution_id, self.execute(request, metrics=metrics))


LocalExecutionBackend = LocalProcessBackend


__all__ = [
    "ExecutionBackend",
    "ExecutionFacts",
    "ExecutionRequest",
    "LocalExecutionBackend",
    "LocalProcessBackend",
]
