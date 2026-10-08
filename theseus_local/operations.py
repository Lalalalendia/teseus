"""Projection-only campaign operations view built from canonical reports."""

from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Mapping


class CampaignOperationalStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class CampaignOperationView:
    campaign_id: str
    status: CampaignOperationalStatus
    mutants_discovered: int
    mutants_executed: int
    killed: int
    survived: int
    infrastructure_failures: int
    report_path: str | None = None
    report_sha256: str | None = None
    worker_states: tuple[Mapping[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "status": self.status.value,
            "mutants_discovered": int(self.mutants_discovered),
            "mutants_executed": int(self.mutants_executed),
            "killed": int(self.killed),
            "survived": int(self.survived),
            "infrastructure_failures": int(self.infrastructure_failures),
            "report_path": self.report_path,
            "report_sha256": self.report_sha256,
            "worker_states": [dict(item) for item in sorted(self.worker_states, key=_worker_sort_key)],
        }


def _count(report: Mapping[str, Any], *names: str) -> int:
    counts = report.get("counts", {})
    if not isinstance(counts, Mapping):
        counts = {}
    return sum(max(0, int(counts.get(name, 0))) for name in names)


def _worker_sort_key(value: Mapping[str, Any]) -> str:
    # Worker advertisements are a projection, but their order must not depend on
    # registration or dictionary insertion order.
    return json.dumps(dict(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def project_campaign(
    report: Mapping[str, Any],
    *,
    report_path: Path | None = None,
    report_sha256: str | None = None,
    workers: tuple[Mapping[str, Any], ...] = (),
) -> CampaignOperationView:
    """Create an operational projection without a second campaign state database."""

    raw_status = str(report.get("status", "unknown")).lower()
    raw_status = {
        "complete": CampaignOperationalStatus.COMPLETED.value,
        "success": CampaignOperationalStatus.COMPLETED.value,
        "failed": CampaignOperationalStatus.FAILED.value,
        "cancel": CampaignOperationalStatus.CANCELLED.value,
    }.get(raw_status, raw_status)
    try:
        status = CampaignOperationalStatus(raw_status)
    except ValueError:
        status = CampaignOperationalStatus.UNKNOWN
    summary = report.get("summary", {})
    if not isinstance(summary, Mapping):
        summary = {}
    return CampaignOperationView(
        campaign_id=str(report.get("campaign_id", "unknown-campaign")),
        status=status,
        mutants_discovered=int(
            summary.get("total_mutants", report.get("total_mutants", report.get("mutants_discovered", 0))) or 0
        ),
        mutants_executed=int(
            summary.get("completed_mutants", report.get("completed_mutants", report.get("mutants_executed", 0))) or 0
        ),
        killed=_count(report, "killed", "kill"),
        survived=_count(report, "survived", "survive"),
        infrastructure_failures=_count(report, "infrastructure_error", "infrastructure_failed", "timeout", "cancelled"),
        report_path=str(report_path) if report_path is not None else None,
        report_sha256=report_sha256,
        worker_states=tuple(dict(item) for item in sorted(workers, key=_worker_sort_key)),
    )


def list_campaigns(
    report_root: Path,
    *,
    offset: int = 0,
    limit: int | None = None,
) -> tuple[CampaignOperationView, ...]:
    """List bounded report projections in stable path order; malformed files are skipped."""

    if int(offset) < 0:
        raise ValueError("operations offset must be non-negative")
    if limit is not None and int(limit) < 1:
        raise ValueError("operations limit must be positive")

    views: list[CampaignOperationView] = []
    skipped = 0
    for path in sorted(Path(report_root).rglob("*.report.json"), key=lambda item: item.as_posix()):
        try:
            raw = path.read_bytes()
            value = json.loads(raw.decode("utf-8"))
            if isinstance(value, Mapping) and str(value.get("campaign_id", "")).strip():
                if skipped < int(offset):
                    skipped += 1
                    continue
                views.append(
                    project_campaign(
                        value,
                        report_path=path,
                        report_sha256=hashlib.sha256(raw).hexdigest(),
                    )
                )
                if limit is not None and len(views) >= int(limit):
                    break
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
    return tuple(views)


__all__ = [
    "CampaignOperationView",
    "CampaignOperationalStatus",
    "list_campaigns",
    "project_campaign",
]
