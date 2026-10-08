from __future__ import annotations
import hashlib
from dataclasses import replace
from test_e24_survivor_runtime_adapter import _evidence
from theseus_survivor_adapter import SurvivorProposalPipeline, SurvivorProviderSubmission, SurvivorRuntimeAdapter
from theseus_survivor_lab.context import MAX_CAUSAL_CONTEXT_BYTES, MAX_CAUSAL_TESTS
from theseus_survivor_lab.contracts import DependencyEvidence, RequestedMode, TestEvidence as SurvivorTestEvidence
from theseus_survivor_lab.providers import ProviderResponse
from theseus_survivor_lab.service import SurvivorAnalysisService


def _sha(value: str) -> str:
    # Hash one test source with the contract's UTF-8 content identity.
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _unrelated_test(index: int, *, trusted: bool = False) -> SurvivorTestEvidence:
    # Build one unrelated test row that must not enter context unless explicitly trusted.
    source = f"def test_unrelated_{index}():\n    assert {index} == {index}\n"
    return SurvivorTestEvidence(
        nodeid=f"tests/test_unrelated.py::test_unrelated_{index}",
        source_path="D:/checkout/tests/test_unrelated.py",
        source_sha256=_sha(source),
        source_text=source,
        selection_reasons=("coverage",) if trusted else (),
        executions=1,
        failures=0,
        median_duration_ms=1.0,
        killed_related_mutants=(),
    )


def test_causal_context_identity_is_stable_across_checkout_relocation_and_input_order() -> None:
    # Physical checkout paths and row order must not alter the bounded causal identity.
    evidence = _evidence()
    first = SurvivorProposalPipeline().generate(SurvivorRuntimeAdapter().analyze(evidence))
    relocated = replace(
        evidence,
        project=replace(evidence.project, root_path="/home/runner/project", main_root_path="/home/runner/project"),
        related_tests=tuple(reversed(evidence.related_tests)),
        related_dependencies=tuple(reversed(evidence.related_dependencies)),
    )
    second = SurvivorProposalPipeline().generate(SurvivorRuntimeAdapter().analyze(relocated))
    assert first.causal_context is not None and second.causal_context is not None
    assert first.causal_context.context_id == second.causal_context.context_id, (
        "causal context identity changed after relocation or row reordering; "
        f"first={first.causal_context.context_id}; second={second.causal_context.context_id}"
    )
    assert first.causal_context.source_path == "app.py"


def test_unrelated_test_is_absent_from_provider_context() -> None:
    # Provider context must contain execution-backed tests and exclude unrelated history.
    evidence = replace(_evidence(), related_tests=(*_evidence().related_tests, _unrelated_test(99)))
    source = SurvivorRuntimeAdapter().analyze(evidence)
    class SpyProvider:
        provider_name = "spy"
        provider_version = "1"
        deterministic = True
        def __init__(self) -> None:
            # Keep the last bounded request for explicit context assertions.
            self.request = None
        def propose(self, request):
            # Capture context and return no provider suggestions.
            self.request = request
            return ProviderResponse((), ())
    provider = SpyProvider()
    request = replace(source.request, requested_modes=(*source.request.requested_modes, RequestedMode.PROPOSE))
    SurvivorAnalysisService(provider).analyze(request)
    assert provider.request is not None and provider.request.context is not None
    nodeids = tuple(item.nodeid for item in provider.request.context.related_tests)
    assert "tests/test_unrelated.py::test_unrelated_99" not in nodeids, (
        "unrelated test leaked into provider context; "
        f"context_id={provider.request.context.context_id}; tests={nodeids}"
    )
    assert tuple(nodeids) == tuple(sorted(nodeids))


def test_dynamic_or_missing_dependency_marks_context_incomplete_and_blocks_provider_submission() -> None:
    # Unresolved closure must create a fail-closed blocker instead of silent completeness.
    evidence = replace(
        _evidence(),
        related_dependencies=(
            DependencyEvidence("runtime_plugin", "dynamic_import", None, None, "unresolved"),
        ),
    )
    source = SurvivorRuntimeAdapter().analyze(evidence)
    result = SurvivorProposalPipeline().generate(
        source,
        SurvivorProviderSubmission("offline-provider", "1", False, ProviderResponse((), ())),
    )
    assert result.causal_context is not None and result.causal_context.complete is False
    assert "dependency_closure_incomplete:runtime_plugin" in result.causal_context.blockers
    assert result.accepted == (), (
        "incomplete causal context produced an accepted proposal; "
        f"context={result.causal_context.context_id}; accepted={result.accepted}"
    )
    reasons = {reason for row in result.rejected for reason in row.reasons}
    assert any("dependency_closure_incomplete:runtime_plugin" in reason for reason in reasons)


def test_causal_context_is_bounded_and_deterministically_sorted() -> None:
    # Trusted test rows must be capped by count and total canonical bytes.
    evidence = replace(
        _evidence(),
        related_tests=(*_evidence().related_tests, *tuple(_unrelated_test(index, trusted=True) for index in range(20))),
    )
    result = SurvivorProposalPipeline().generate(SurvivorRuntimeAdapter().analyze(evidence))
    context = result.causal_context
    assert context is not None
    assert len(context.related_tests) <= MAX_CAUSAL_TESTS
    assert context.total_bytes <= MAX_CAUSAL_CONTEXT_BYTES
    identities = tuple((item.nodeid, item.source_path or "") for item in context.related_tests)
    assert identities == tuple(sorted(identities)), (
        "causal tests are not deterministically ordered; "
        f"context_id={context.context_id}; identities={identities}"
    )


def test_provider_context_excludes_secrets_environment_values_and_execution_output() -> None:
    # Provider context may expose identities but never raw environment values or stdout/stderr excerpts.
    evidence = _evidence()
    source = SurvivorRuntimeAdapter().analyze(evidence)
    secret_request = replace(
        source.request,
        executions=(replace(source.request.executions[0], output_excerpt="token=TOPSECRET C:/Users/Alice/private.log"),),
        requested_modes=(*source.request.requested_modes, RequestedMode.PROPOSE),
    )
    class SpyProvider:
        provider_name = "spy"
        provider_version = "1"
        deterministic = True
        def __init__(self) -> None:
            # Preserve only the request object for boundary inspection.
            self.request = None
        def propose(self, request):
            # Return no suggestions after recording the sanitized context.
            self.request = request
            return ProviderResponse((), ())
    provider = SpyProvider()
    SurvivorAnalysisService(provider).analyze(secret_request)
    rendered = repr(provider.request.context)
    assert "TOPSECRET" not in rendered
    assert "private.log" not in rendered
    assert "output_excerpt" not in rendered


def test_causal_context_identity_ignores_windows_and_linux_checkout_prefixes(real_gap_request) -> None:
    # Canonical source identities must survive relocation between Windows and Linux checkouts.
    class SpyProvider:
        provider_name = "spy"
        provider_version = "1"
        deterministic = True
        def __init__(self) -> None:
            # Preserve the bounded request for context identity comparison.
            self.request = None
        def propose(self, request):
            # Record one provider request and return no external suggestions.
            self.request = request
            return ProviderResponse((), ())
    windows = replace(
        real_gap_request,
        mutant=replace(real_gap_request.mutant, source_path="C:/Users/A/project/src/decision.py"),
        source=replace(real_gap_request.source, source_path="C:/Users/A/project/src/decision.py"),
        requested_modes=real_gap_request.requested_modes,
    )
    linux = replace(
        real_gap_request,
        mutant=replace(real_gap_request.mutant, source_path="/home/b/project/src/decision.py"),
        source=replace(real_gap_request.source, source_path="/home/b/project/src/decision.py"),
        requested_modes=real_gap_request.requested_modes,
    )
    first_provider = SpyProvider()
    second_provider = SpyProvider()
    SurvivorAnalysisService(first_provider).analyze(windows)
    SurvivorAnalysisService(second_provider).analyze(linux)
    assert first_provider.request is not None and second_provider.request is not None
    assert first_provider.request.context.context_id == second_provider.request.context.context_id
    assert first_provider.request.context.source_path == second_provider.request.context.source_path == "src/decision.py"


def test_unrelated_dependency_is_not_included_in_provider_context() -> None:
    # Explicitly unrelated dependency history must not consume provider context budget.
    evidence = replace(
        _evidence(),
        related_dependencies=(
            DependencyEvidence("json", "module", "src/json_adapter.py", "1", "direct_import"),
            DependencyEvidence("old_debug_helper", "module", "tools/debug.py", "1", "unrelated_history-only"),
        ),
    )
    result = SurvivorProposalPipeline().generate(SurvivorRuntimeAdapter().analyze(evidence))
    assert result.causal_context is not None
    names = tuple(item.name for item in result.causal_context.dependencies)
    assert names == ("json",), (
        "unrelated dependency leaked into bounded causal context; "
        f"context_id={result.causal_context.context_id}; dependencies={names}"
    )
