"""Deterministic canonical campaign reporting from durable execution authority."""

from __future__ import annotations

import hashlib
import json
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from gallifrey_mutation import (
    CampaignState,
    MutationCampaign,
    MutationExecution,
    MutationExecutionState,
    MutationResult,
    MutationShard,
)
from theseus_contracts import CampaignPlan


CANONICAL_REPORT_SCHEMA_VERSION = 2
CANONICAL_STATUSES = (
    "killed",
    "survived",
    "invalid",
    "timeout",
    "infrastructure_error",
    "cancelled",
)


class CanonicalReportError(RuntimeError):
    """Raised when durable execution evidence cannot form one unambiguous report."""


def _enum_value(value: Any) -> str | None:
    # Convert an enum or scalar status to one stable string without exposing implementation types.
    if value is None:
        return None
    if isinstance(value, Enum):
        return str(value.value)
    return str(value)


def _json_value(value: Any) -> Any:
    # Normalize nested durable values into JSON data with deterministic ordering.
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_value(value[key]) for key in sorted(value, key=lambda item: str(item))}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        converted = [_json_value(item) for item in value]
        return sorted(
            converted,
            key=lambda item: json.dumps(
                item,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
    return value


def _stable_digest(value: Any) -> str:
    # Build one content identity for evidence without using process-local object representations.
    payload = json.dumps(_json_value(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def canonicalize_status(
    execution_status: MutationExecutionState | str,
    semantic_result: MutationResult | str | None,
) -> str:
    # Project execution and semantic state into the six public terminal result labels.
    execution_value = _enum_value(execution_status) or ""
    semantic_value = (_enum_value(semantic_result) or "").strip().lower()
    if execution_value == MutationExecutionState.CANCELLED.value:
        return "cancelled"
    if execution_value == MutationExecutionState.FAILED.value:
        return "infrastructure_error"
    if semantic_value in {"killed", "kill"}:
        return "killed"
    if semantic_value in {"survived", "survive"}:
        return "survived"
    if semantic_value in {"invalid", "invalid_mutant"}:
        return "invalid"
    if semantic_value in {"timeout", "timed_out"}:
        return "timeout"
    if semantic_value in {"cancelled", "canceled", "cancel_requested"}:
        return "cancelled"
    if semantic_value in {"infrastructure_error", "infrastructure_failed", "error", "failed"}:
        return "infrastructure_error"
    return "infrastructure_error"


def _descriptor_payload(plan: CampaignPlan | None) -> dict[str, dict[str, Any]]:
    # Index only the immutable planner descriptors used to describe mutation identity.
    if plan is None:
        return {}
    return {
        item.mutant.mutant_id.value: _json_value(item.mutant.to_dict())
        for item in sorted(plan.selected, key=lambda row: row.mutant.mutant_id.value)
    }


def _mutation_identity(mutant: Mapping[str, Any], mutant_id: str) -> dict[str, Any]:
    # Keep source mutation identity independent from execution and evidence identity.
    fields = (
        "mutant_id",
        "mutation",
        "source_path",
        "line_no",
        "column_no",
        "original",
        "replacement",
        "operator_version",
    )
    identity = {key: _json_value(mutant[key]) for key in fields if key in mutant}
    identity.setdefault("mutant_id", mutant_id)
    return identity


def _current_terminal_executions(
    shards: Sequence[MutationShard],
    executions: Sequence[MutationExecution],
) -> tuple[tuple[MutationExecution, ...], int]:
    # Select only the current shard attempt and reject duplicate terminal evidence for one mutant.
    shard_attempts: dict[str, int] = {}
    for shard in shards:
        shard_id = shard.shard_id.value
        previous = shard_attempts.get(shard_id)
        if previous is not None and previous != shard.attempt:
            raise CanonicalReportError(
                f"durable shard attempts conflict: shard_id={shard_id}; "
                f"attempts={previous},{shard.attempt}"
            )
        shard_attempts[shard_id] = shard.attempt
    stale_count = 0
    selected: list[MutationExecution] = []
    by_mutant: dict[str, MutationExecution] = {}
    for execution in executions:
        expected_attempt = shard_attempts.get(execution.shard_id.value)
        if expected_attempt is None or execution.attempt != expected_attempt:
            stale_count += 1
            continue
        if execution.status in {MutationExecutionState.PENDING, MutationExecutionState.RUNNING}:
            continue
        mutant_id = execution.mutant_id.value
        previous = by_mutant.get(mutant_id)
        if previous is not None and previous.execution_id != execution.execution_id:
            raise CanonicalReportError(
                "durable terminal evidence is ambiguous: "
                f"mutant_id={mutant_id}; execution_ids="
                f"{previous.execution_id.value},{execution.execution_id.value}; "
                f"attempt={execution.attempt}"
            )
        by_mutant[mutant_id] = execution
        selected.append(execution)
    return tuple(sorted(selected, key=lambda item: item.mutant_id.value)), stale_count


def _configuration_payload(campaign: MutationCampaign) -> dict[str, Any]:
    # Expose stable execution configuration fields without deriving semantics from process output.
    configuration = campaign.configuration
    command = configuration.project.test_command
    return {
        "mode": _enum_value(configuration.mode),
        "no_escalation": bool(configuration.no_escalation),
        "reuse_mode": str(configuration.reuse_mode),
        "workers": int(campaign.budget.max_workers),
        "timeout_seconds": campaign.budget.max_test_seconds,
        "lease_seconds": campaign.budget.lease_seconds,
        "budget": _json_value(campaign.budget.to_dict()),
        "operators": list(campaign.scope.operators),
        "test_command": list(command.argv) if command is not None else None,
        "test_cwd": command.cwd if command is not None else None,
    }


def _scope_payload(campaign: MutationCampaign) -> dict[str, Any]:
    # Describe project and source scope as durable campaign inputs, not as mutable engine state.
    project = campaign.configuration.project
    return {
        "project_id": project.project_id.value,
        "display_name": project.display_name,
        "root_path": project.root_path,
        **_json_value(campaign.scope.to_dict()),
    }


def _row_for_execution(
    execution: MutationExecution,
    descriptor: Mapping[str, Any],
) -> dict[str, Any]:
    # Render one terminal result row with separate mutation, evidence and attempt identities.
    mutant_id = execution.mutant_id.value
    raw_status = _enum_value(execution.semantic_result) or execution.status.value
    status = canonicalize_status(execution.status, execution.semantic_result)
    evidence_payload = {
        "execution_id": execution.execution_id.value,
        "attempt": int(execution.attempt),
        "lease_id": execution.lease_id,
        "artifacts": sorted(item.value for item in execution.artifacts),
        "restore_verified": bool(execution.restore_verified),
    }
    evidence_id = f"evidence-{_stable_digest(evidence_payload)[:32]}"
    mutation = dict(descriptor) if descriptor else {"mutant_id": mutant_id}
    observations = sorted(
        (_json_value(dict(item)) for item in execution.test_observations),
        key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
    )
    return {
        "mutant": mutation,
        "mutation_identity": _mutation_identity(mutation, mutant_id),
        "status": status,
        "raw_status": raw_status,
        "execution_id": execution.execution_id.value,
        "attempt": int(execution.attempt),
        "lease_id": execution.lease_id,
        "execution_attempt_identity": {
            "execution_id": execution.execution_id.value,
            "attempt": int(execution.attempt),
        },
        "evidence_identity": {
            "evidence_id": evidence_id,
            "execution_id": execution.execution_id.value,
            "attempt": int(execution.attempt),
            "lease_id": execution.lease_id,
        },
        "restore_verified": bool(execution.restore_verified),
        "selected_tests": sorted(str(item) for item in execution.selected_tests),
        "test_observations": observations,
        "artifacts": sorted(item.value for item in execution.artifacts),
        "duration_seconds": execution.duration_seconds,
        "error": execution.error,
        "provenance": {
            "source": "gallifrey_execution_store",
            "campaign_id": execution.campaign_id.value,
            "shard_id": execution.shard_id.value,
            "execution_id": execution.execution_id.value,
            "attempt": int(execution.attempt),
            "lease_id": execution.lease_id,
        },
    }


def build_canonical_report(
    campaign: MutationCampaign,
    shards: Sequence[MutationShard],
    executions: Sequence[MutationExecution],
    *,
    campaign_plan: CampaignPlan | None = None,
    database_path: Path | None = None,
    report_path: Path | None = None,
    raw_engine_report_path: str | None = None,
    status_override: str | None = None,
) -> dict[str, Any]:
    # Build the public report exclusively from durable campaign, plan, shard and execution authority.
    terminal, stale_count = _current_terminal_executions(shards, executions)
    descriptors = _descriptor_payload(campaign_plan)
    rows = tuple(
        _row_for_execution(item, descriptors.get(item.mutant_id.value, {}))
        for item in terminal
    )
    counts = {status: 0 for status in CANONICAL_STATUSES}
    for row in rows:
        counts[str(row["status"])] += 1
    status = (
        str(status_override)
        if status_override is not None
        else "complete"
        if campaign.status == CampaignState.COMPLETED
        else campaign.status.value
    )
    selected_count = (
        int(campaign_plan.selected_count)
        if campaign_plan is not None
        else int(campaign.total_mutants)
    )
    completed_count = len(rows)
    duration_values = [
        float(row["duration_seconds"])
        for row in rows
        if row.get("duration_seconds") is not None
    ]
    durable_state = (
        CampaignState.COMPLETED.value
        if status == "complete"
        else campaign.status.value
    )
    campaign_section = {
        "campaign_id": campaign.campaign_id.value,
        "project_id": campaign.project_id.value,
        "status": status,
        "durable_state": durable_state,
        "plan_id": campaign_plan.plan_id if campaign_plan is not None else campaign.plan_id,
        "revision_id": campaign.revision_id.value,
        "revision_number": int(campaign.revision_number),
        "prepared_snapshot_id": campaign.prepared_snapshot_id,
        "collection_snapshot_id": campaign.collection_snapshot_id,
        "index_snapshot_id": campaign.index_snapshot_id,
        "baseline_snapshot_id": campaign.baseline_snapshot_id,
    }
    summary = {
        "selected": selected_count,
        "total": int(campaign.total_mutants),
        "completed": completed_count,
        "counts": counts,
        "terminal": completed_count,
    }
    report: dict[str, Any] = {
        "schema_version": CANONICAL_REPORT_SCHEMA_VERSION,
        "source": "gallifrey_authoritative",
        "authority": "durable_sqlite_execution_store",
        "status": status,
        "campaign_id": campaign.campaign_id.value,
        "plan_id": campaign_section["plan_id"],
        "plan_path": str(report_path.parent / "campaign.plan.json") if report_path is not None and campaign_plan is not None else None,
        "worker_count": int(campaign_plan.worker_count) if campaign_plan is not None else max(1, len(shards)),
        "total_mutants": int(campaign.total_mutants),
        "completed_mutants": completed_count,
        "counts": counts,
        "campaign": campaign_section,
        "scope": _scope_payload(campaign),
        "configuration": _configuration_payload(campaign),
        "baseline": {
            "status": "passed" if campaign.baseline_snapshot_id else "not_run",
            "snapshot_id": campaign.baseline_snapshot_id,
            "evidence": {
                "source": "durable_campaign",
                "snapshot_id": campaign.baseline_snapshot_id,
            },
        },
        "summary": summary,
        "results": list(rows),
        "performance": {
            "execution_count": completed_count,
            "execution_seconds": round(sum(duration_values), 9),
            "max_execution_seconds": round(max(duration_values), 9) if duration_values else 0.0,
            "average_execution_seconds": round(sum(duration_values) / len(duration_values), 9)
            if duration_values
            else 0.0,
            "wall_seconds": None,
        },
        "artifacts": {
            "canonical_report_json": str(report_path) if report_path is not None else None,
            "canonical_report_markdown": str(report_path.with_suffix(".md")) if report_path is not None else None,
            "database": str(database_path) if database_path is not None else None,
            "raw_engine_report": raw_engine_report_path,
        },
        "integrity": {
            "source_of_truth": "durable_sqlite_execution_store",
            "semantic_status_source": "mutation_executions.semantic_result",
            "stale_attempts_excluded": stale_count,
            "terminal_evidence_count": completed_count,
            "one_terminal_evidence_per_mutant": len({row["mutant"]["mutant_id"] for row in rows}) == completed_count,
            "publication": "atomic_report_files",
        },
    }
    report["raw_engine_report_path"] = raw_engine_report_path
    return _json_value(report)


def canonical_json_text(report: Mapping[str, Any]) -> str:
    # Serialize one canonical report with stable key ordering for publication and fingerprinting.
    return json.dumps(_json_value(report), ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def canonical_markdown(report: Mapping[str, Any]) -> str:
    # Render a compact deterministic human alias without changing JSON report authority.
    rows = [item for item in report.get("results", []) if isinstance(item, Mapping)]
    summary = report.get("summary", {})
    counts = summary.get("counts", {}) if isinstance(summary, Mapping) else {}
    lines = [
        f"# Theseus campaign `{report.get('campaign_id', '')}`",
        "",
        f"- Status: `{report.get('status', '')}`",
        f"- Mutants: `{int(report.get('completed_mutants', len(rows)))}/{int(report.get('total_mutants', len(rows)))}`",
        f"- Killed: `{int(counts.get('killed', 0))}`",
        f"- Survived: `{int(counts.get('survived', 0))}`",
        f"- Invalid: `{int(counts.get('invalid', 0))}`",
        f"- Timeout: `{int(counts.get('timeout', 0))}`",
        f"- Infrastructure error: `{int(counts.get('infrastructure_error', 0))}`",
        f"- Cancelled: `{int(counts.get('cancelled', 0))}`",
        "",
        "| Mutant | Result | Execution | Attempt |",
        "|---|---|---|---|",
    ]
    for row in rows:
        mutant = row.get("mutant", {})
        mutant_id = mutant.get("mutant_id", "") if isinstance(mutant, Mapping) else ""
        lines.append(
            f"| `{mutant_id}` | `{row.get('status', '')}` | "
            f"`{row.get('execution_id', '')}` | `{row.get('attempt', '')}` |"
        )
    return "\n".join(lines) + "\n"


__all__ = [
    "CANONICAL_REPORT_SCHEMA_VERSION",
    "CANONICAL_STATUSES",
    "CanonicalReportError",
    "build_canonical_report",
    "canonical_json_text",
    "canonical_markdown",
    "canonicalize_status",
]
