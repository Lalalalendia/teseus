"""Campaign report finalization port for the E-09 facade decomposition."""

from __future__ import annotations

from pathlib import Path
from typing import Any


class CampaignFinalizationService:
    """Compatibility service that owns the report/recovery finalization call shape."""

    def __init__(self, runner: Any) -> None:
        # Keep the durability implementation in the runner until the control-plane store is introduced.
        self.runner = runner

    def finalize(
        self,
        report_path: Path,
        report: dict[str, Any],
        manifest_path: Path,
        *,
        status: str,
    ) -> None:
        # Delegate the two-phase restore and report commit without changing its failure policy.
        self.runner._finish_report_impl(report_path, report, manifest_path, status=status)
