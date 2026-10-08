"""Optional advisory analysis over canonical evidence.

This module has no write access to campaign authority.  Providers may be absent or
fail; the mutation campaign and its canonical report remain usable either way.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol

from theseus_contracts.serialization import dumps


AI_ANALYSIS_SCHEMA_VERSION = 1


class AnalysisProvider(Protocol):
    def analyze(self, report: Mapping[str, Any]) -> Mapping[str, Any]:
        """Return advisory observations without changing report status/counts."""


@dataclass(frozen=True, slots=True)
class AdvisoryAnalysis:
    campaign_id: str
    source_report_sha256: str
    provider: str
    status: str
    observations: tuple[Mapping[str, Any], ...] = ()
    error: str | None = None
    analysis_id: str = ""
    schema_version: int = AI_ANALYSIS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if int(self.schema_version) != AI_ANALYSIS_SCHEMA_VERSION:
            raise ValueError("unsupported advisory analysis schema")
        if not self.campaign_id or not self.source_report_sha256 or not self.provider:
            raise ValueError("analysis identity fields must be non-empty")
        if self.status not in {"available", "unavailable", "failed"}:
            raise ValueError(f"unsupported analysis status: {self.status}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": int(self.schema_version),
            "analysis_id": self.analysis_id,
            "campaign_id": self.campaign_id,
            "source_report_sha256": self.source_report_sha256,
            "provider": self.provider,
            "status": self.status,
            "advisory": True,
            "observations": [dict(item) for item in self.observations],
            "error": self.error,
        }


class UnavailableAnalysisProvider:
    """Explicit no-op provider used when AI is disabled or not installed."""

    name = "unavailable"

    def analyze(self, report: Mapping[str, Any]) -> Mapping[str, Any]:
        del report
        raise RuntimeError("AI analysis provider is unavailable")


class DeterministicAnalysisProvider:
    """Offline baseline provider useful for previews and deterministic contract tests."""

    name = "deterministic-advisory-v1"

    def analyze(self, report: Mapping[str, Any]) -> Mapping[str, Any]:
        rows = report.get("mutants", report.get("results", []))
        if not isinstance(rows, (list, tuple)):
            rows = []
        observations: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            status = str(row.get("status", "")).lower()
            mutant = row.get("mutant", {})
            if status == "survived":
                observations.append(
                    {
                        "kind": "survived_explanation",
                        "mutant_id": str(row.get("mutant_id") or (mutant.get("mutant_id") if isinstance(mutant, Mapping) else "")),
                        "summary": "Mutant survived the selected test plan; review missing behavioural coverage.",
                    }
                )
            elif status in {"infrastructure_error", "timeout", "cancelled"}:
                observations.append(
                    {
                        "kind": "infrastructure_explanation",
                        "mutant_id": str(row.get("mutant_id") or (mutant.get("mutant_id") if isinstance(mutant, Mapping) else "")),
                        "summary": "Execution did not produce a normal mutation verdict; inspect infrastructure evidence.",
                    }
                )
        return {"observations": observations}


def _report_digest(report: Mapping[str, Any]) -> str:
    return hashlib.sha256(dumps(dict(report)).encode("utf-8")).hexdigest()


def analyze_report(
    report: Mapping[str, Any],
    *,
    campaign_id: str | None = None,
    provider: AnalysisProvider | None = None,
) -> AdvisoryAnalysis:
    """Analyze a report while preserving the input mapping and all authoritative fields."""

    digest = _report_digest(report)
    resolved_campaign = str(campaign_id or report.get("campaign_id") or "unknown-campaign")
    selected = provider or UnavailableAnalysisProvider()
    provider_name = str(getattr(selected, "name", type(selected).__name__))
    try:
        raw = selected.analyze(dict(report))
        observations = raw.get("observations", []) if isinstance(raw, Mapping) else []
        if not isinstance(observations, (list, tuple)):
            raise ValueError("analysis observations must be an array")
        normalized = tuple(dict(item) for item in observations if isinstance(item, Mapping))
        status = "available"
        error = None
    except Exception as exc:  # advisory failures must never become campaign failures
        normalized = ()
        status = "unavailable" if isinstance(selected, UnavailableAnalysisProvider) else "failed"
        error = f"{type(exc).__name__}: {exc}"
    analysis_id = hashlib.sha256(
        dumps(
            {
                "campaign_id": resolved_campaign,
                "source_report_sha256": digest,
                "provider": provider_name,
                "status": status,
                "observations": list(normalized),
            }
        ).encode("utf-8")
    ).hexdigest()[:32]
    return AdvisoryAnalysis(
        campaign_id=resolved_campaign,
        source_report_sha256=digest,
        provider=provider_name,
        status=status,
        observations=normalized,
        error=error,
        analysis_id=analysis_id,
    )


def write_advisory_analysis(path: Path, analysis: AdvisoryAnalysis) -> None:
    """Publish advisory output atomically and never overwrite a conflicting identity."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = dumps(analysis.to_dict()) + "\n"
    if target.is_file() and target.read_text(encoding="utf-8") != payload:
        raise RuntimeError(f"advisory analysis path already contains a different result: {target}")
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(payload, encoding="utf-8", newline="\n")
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


__all__ = [
    "AI_ANALYSIS_SCHEMA_VERSION",
    "AdvisoryAnalysis",
    "AnalysisProvider",
    "DeterministicAnalysisProvider",
    "UnavailableAnalysisProvider",
    "analyze_report",
    "write_advisory_analysis",
]
