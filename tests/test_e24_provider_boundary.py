from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from theseus_survivor_lab.contracts import SourceEvidence
from theseus_survivor_lab.providers import (
    ProviderResponse,
    ProviderSuggestion,
)
from theseus_survivor_lab.service import SurvivorAnalysisService
from theseus_survivor_lab.validation import sha256_text


class SpyProvider:
    """Capture the sanitized provider request for boundary assertions."""

    def __init__(self) -> None:
        # Keep only the last immutable request for the test.
        self.request = None

    def propose(self, request):
        # Return a warning and no suggestion while recording the boundary payload.
        self.request = request
        return ProviderResponse((), ("spy_provider_used",))


class HostileProvider:
    """Try to influence proposal text but not the local classification."""

    def propose(self, request):
        # Return non-authoritative text that the service must treat as a suggestion only.
        del request
        return ProviderResponse(
            (
                ProviderSuggestion(
                    hypothesis_kind="boundary_input",
                    title="provider title",
                    arrangement=("provider input",),
                    action=("provider action",),
                    assertions=("provider assertion",),
                    rationale="provider rationale",
                    generated_code="provider code",
                ),
            ),
            ("provider_warning",),
        )


def test_provider_receives_only_sanitized_bounded_context(real_gap_request) -> None:
    # Ensure secrets and physical paths do not cross the optional provider boundary.
    source_text = "def decide(x):\n    if x > 3:\n        return 'C:\\\\Users\\\\Alice\\\\secret.txt token=abc123'\n    return False\n"
    request = replace(
        real_gap_request,
        source=SourceEvidence(
            source_path="C:/Users/Alice/project/src/decision.py",
            source_sha256=sha256_text(source_text),
            source_text=source_text,
            function_source=None,
            function_start_line=1,
                function_end_line=4,
        ),
    )
    provider = SpyProvider()
    result = SurvivorAnalysisService(provider).analyze(request)
    assert provider.request is not None
    assert "C:\\" not in provider.request.source_excerpt
    assert "/home/" not in provider.request.source_excerpt
    assert "abc123" not in provider.request.source_excerpt
    assert len(provider.request.source_excerpt) <= 2400
    assert result.warnings == ("spy_provider_used",)


def test_provider_cannot_change_authoritative_classification(real_gap_request) -> None:
    # Provider output may enrich proposals but cannot replace local category or plan.
    result = SurvivorAnalysisService(HostileProvider()).analyze(real_gap_request)
    assert result.classification.category.value == "real_test_gap"
    assert result.validation_plan.original_checks
    assert "provider_warning" in result.warnings
    assert result.proposals[0].generation_source.value == "external_provider"


def test_production_package_has_no_forbidden_runtime_imports() -> None:
    # Enforce the standalone package boundary as a source-level invariant.
    package_dir = Path(__file__).parents[1] / "theseus_survivor_lab"
    forbidden = (
        "theseus_local",
        "gallifrey_mutation",
        "test_intelligence_unified_v1.runner",
        "test_intelligence_unified_v1.engine",
        "test_intelligence_unified_v1.workers",
        "test_intelligence_unified_v1.coordinator",
    )
    for path in package_dir.glob("*.py"):
        content = path.read_text(encoding="utf-8")
        assert not any(token in content for token in forbidden), path.name
