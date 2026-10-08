"""Reference distributed campaign loop and equivalence/acceptance projections."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from theseus_contracts import RemoteExecutionRequest, RemoteExecutionResult
from .artifact_store import ArtifactTransferStats, ContentAddressedArtifactStore
from .distributed import DistributedScheduler
from .remote_worker import RemoteWorkerRuntime


CANONICAL_SEMANTIC_STATUSES = (
    "killed",
    "survived",
    "invalid",
    "timeout",
    "infrastructure_error",
    "cancelled",
)


@dataclass(frozen=True, slots=True)
class DistributedRunReport:
    results: tuple[RemoteExecutionResult, ...]
    transfer_stats: Mapping[str, ArtifactTransferStats]
    scheduler_snapshot: Mapping[str, Any]
    wall_seconds: float
    workers: int
    worker_assignments: Mapping[str, int] = field(default_factory=dict)
    worker_utilization: Mapping[str, float] = field(default_factory=dict)
    lease_retries: int = 0
    journal_completed_attempts: Mapping[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "results": [item.to_dict() for item in self.results],
            "transfer_stats": {
                key: {
                    "artifact_ids": list(value.artifact_ids),
                    "bytes_transferred": value.bytes_transferred,
                    "cache_hits": value.cache_hits,
                    "cache_misses": value.cache_misses,
                    "cache_hit_rate": value.cache_hit_rate,
                }
                for key, value in sorted(self.transfer_stats.items())
            },
            "scheduler": dict(self.scheduler_snapshot),
            "wall_seconds": float(self.wall_seconds),
            "workers": int(self.workers),
            "worker_assignments": {key: int(value) for key, value in sorted(self.worker_assignments.items())},
            "worker_utilization": {key: float(value) for key, value in sorted(self.worker_utilization.items())},
            "lease_retries": int(self.lease_retries),
            "journal_completed_attempts": {
                key: int(value) for key, value in sorted(self.journal_completed_attempts.items())
            },
        }


def run_distributed_requests(
    requests: Sequence[RemoteExecutionRequest],
    *,
    coordinator_store: ContentAddressedArtifactStore,
    workers: Sequence[RemoteWorkerRuntime],
    scheduler: DistributedScheduler,
) -> DistributedRunReport:
    """Run at-least-once requests through the durable scheduler and remote workers."""

    started = time.perf_counter()
    if not workers:
        raise ValueError("at least one remote worker is required")
    transfer: dict[str, ArtifactTransferStats] = {}
    assignments: dict[str, int] = {worker.worker_id: 0 for worker in workers}
    for worker in workers:
        scheduler.register_worker(worker.registration())
        stats: list[ArtifactTransferStats] = []
        for request in requests:
            stats.append(
                worker.store.transfer_snapshot(
                    coordinator_store,
                    request.project_snapshot_id,
                    prepared_artifact_ids=(request.prepared_artifact_id,),
                )
            )
        transfer[worker.worker_id] = ArtifactTransferStats(
            artifact_ids=tuple(item for stat in stats for item in stat.artifact_ids),
            bytes_transferred=sum(item.bytes_transferred for item in stats),
            cache_hits=sum(item.cache_hits for item in stats),
            cache_misses=sum(item.cache_misses for item in stats),
        )
    for request in requests:
        scheduler.submit(request)
    while scheduler.pending_count() or scheduler.active_lease_count():
        progress = False
        for worker in workers:
            # Claim one per worker per round so a fast worker cannot drain the
            # entire queue before other advertised capacity gets a turn.
            for lease in scheduler.claim_available(worker.worker_id, limit=1):
                progress = True
                assignments[worker.worker_id] += 1
                result = worker.execute(lease.request)
                completion = scheduler.complete(worker.worker_id, lease.lease_id, result)
                if not completion.accepted:
                    # A failed/stale physical attempt is requeued only by lease expiry or
                    # worker disconnect; no last-writer-wins shortcut is permitted here.
                    scheduler.expire_leases()
        if not progress:
            expired = scheduler.expire_leases()
            if not expired:
                raise RuntimeError("distributed scheduler made no progress")
    results = tuple(
        result
        for request in sorted(requests, key=lambda item: item.evidence_identity)
        for result in (scheduler.authoritative(request.evidence_identity),)
        if result is not None
    )
    total_assignments = max(1, sum(assignments.values()))
    stale = scheduler.stale_records()
    return DistributedRunReport(
        results=results,
        transfer_stats=transfer,
        scheduler_snapshot=scheduler.snapshot(),
        wall_seconds=max(0.0, time.perf_counter() - started),
        workers=len(workers),
        worker_assignments=dict(assignments),
        worker_utilization={
            key: round(value / total_assignments, 6)
            for key, value in sorted(assignments.items())
        },
        lease_retries=sum(
            1
            for item in stale
            if str(item.get("reason", "")) in {"lease_expired", "worker_reconnected", "worker_disconnected", "worker_crash"}
        ),
        journal_completed_attempts={
            worker.worker_id: int(worker.completed_attempt_count)
            for worker in workers
        },
    )


def classify_physical_result(
    result: RemoteExecutionResult,
    *,
    coordinator_status: str | None = None,
) -> str:
    """Project worker facts into a public outcome under coordinator authority.

    Workers return facts only.  ``coordinator_status`` is the optional result of
    the mutation/test authority (for example, an invalid-mutant decision); it is
    deliberately not read from a worker frame.
    """

    if coordinator_status is not None:
        normalized = str(coordinator_status).strip().lower()
        if normalized in {"kill", "killed"}:
            return "killed"
        if normalized in {"survive", "survived"}:
            return "survived"
        if normalized in {"invalid", "invalid_mutant"}:
            return "invalid"
        if normalized in {"timeout", "timed_out"}:
            return "timeout"
        if normalized in {"cancelled", "canceled", "cancel_requested"}:
            return "cancelled"
        if normalized in {"infrastructure_error", "infrastructure_failed", "error", "failed"}:
            return "infrastructure_error"
        raise ValueError(f"unknown coordinator semantic status: {coordinator_status!r}")
    if result.cancelled:
        return "cancelled"
    if result.timed_out:
        return "timeout"
    if not result.started or result.workspace_integrity != "verified":
        return "infrastructure_error"
    return "survived" if result.exit_code == 0 else "killed"


def project_semantic_outcomes(
    rows: Iterable[RemoteExecutionResult],
    *,
    coordinator_statuses: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Return a stable evidence-to-status view owned by the coordinator."""

    overrides = coordinator_statuses or {}
    projected: dict[str, str] = {}
    for item in rows:
        if item.evidence_identity in projected:
            raise ValueError(f"duplicate semantic evidence: {item.evidence_identity}")
        projected[item.evidence_identity] = classify_physical_result(
            item,
            coordinator_status=overrides.get(item.evidence_identity),
        )
    return dict(sorted(projected.items()))


def compare_execution_semantics(
    left: Iterable[RemoteExecutionResult],
    right: Iterable[RemoteExecutionResult],
    *,
    left_coordinator_statuses: Mapping[str, str] | None = None,
    right_coordinator_statuses: Mapping[str, str] | None = None,
) -> tuple[bool, tuple[str, ...]]:
    """Compare coordinator semantic outcomes and physical terminal facts.

    Attempt IDs, elapsed time, and worker-local artifact IDs are transport
    details.  Mutation status remains an explicit coordinator projection so an
    untrusted worker cannot claim ``killed``/``survived``/``invalid`` itself.
    """

    # Materialize once so each side can use its own coordinator-owned overrides.
    left_rows = tuple(left)
    right_rows = tuple(right)

    def side_projection(
        rows: Iterable[RemoteExecutionResult],
        statuses: Mapping[str, str] | None,
    ) -> dict[str, tuple[Any, ...]]:
        projected: dict[str, tuple[Any, ...]] = {}
        for item in rows:
            if item.evidence_identity in projected:
                raise ValueError(f"duplicate semantic evidence: {item.evidence_identity}")
            projected[item.evidence_identity] = (
                item.mutation_identity,
                item.started,
                item.exit_code,
                item.timed_out,
                item.cancelled,
                item.workspace_integrity,
                classify_physical_result(item, coordinator_status=(statuses or {}).get(item.evidence_identity)),
            )
        return projected

    first = side_projection(left_rows, left_coordinator_statuses)
    second = side_projection(right_rows, right_coordinator_statuses)
    differences = tuple(
        sorted(
            {
                *[f"missing:{key}" for key in first.keys() - second.keys()],
                *[f"extra:{key}" for key in second.keys() - first.keys()],
                *[f"different:{key}" for key in first.keys() & second.keys() if first[key] != second[key]],
            }
        )
    )
    return not differences, differences


__all__ = [
    "CANONICAL_SEMANTIC_STATUSES",
    "DistributedRunReport",
    "classify_physical_result",
    "compare_execution_semantics",
    "project_semantic_outcomes",
    "run_distributed_requests",
]
