from __future__ import annotations

from types import SimpleNamespace

from theseus_knowledge import InvestigationTrajectoryStore
from theseus_survivor_adapter import SurvivorInvestigationService
from theseus_survivor_lab import ExperimentObservation, SurvivorCategory
from theseus_survivor_lab.contracts import (
    EnvironmentEvidence,
    MutantEvidence,
    MutantStatus,
    RequestedMode,
    SelectionEvidence,
    SourceEvidence,
    SurvivorAnalysisRequest,
    SurvivorClassification,
)


def _request() -> SurvivorAnalysisRequest:
    # Build one authenticated-like survivor request for adapter integration without a target checkout.
    return SurvivorAnalysisRequest(
        schema_version=1,
        request_id="request-adapter",
        project_id="project-adapter",
        revision="rev",
        mutant=MutantEvidence("m1", "condition_to_not", "1", "app.py", "f", None, 1, 0, "x", "not x", None, MutantStatus.SURVIVED),
        source=SourceEvidence("app.py", "sha", "def f(x): return x", "def f(x): return x", 1, 1),
        selection=SelectionEvidence(selected_tests=("test_a",), related_test_nodeids=("test_b",)),
        executions=(),
        related_tests=(),
        related_dependencies=(),
        environment=EnvironmentEvidence(None, None, (), (), True),
        requested_modes=(RequestedMode.CLASSIFY,),
    )


class _Executor:
    def execute(self, experiment, request):
        # Return verified evidence through the active-investigation runtime port.
        return ExperimentObservation(experiment.kind, "supports", f"evidence-{experiment.kind.value}", True, 0.01)


def test_a10_adapter_runs_active_investigation_and_a11_persists_trajectory(tmp_path) -> None:
    # Connect an existing Survivor Lab result to Active Investigator and durable trajectory learning.
    classification = SurvivorClassification(SurvivorCategory.SELECTION_ESCAPE, "related test omitted", 0.65, (), ())
    source = SimpleNamespace(
        request=_request(),
        analysis=SimpleNamespace(classification=classification, analysis_id="analysis-1", result_id="result-1"),
    )
    store = InvestigationTrajectoryStore(tmp_path / "investigations.sqlite3")
    service = SurvivorInvestigationService(trajectory_store=store)
    result = service.run(source, _Executor())
    assert result.source_analysis_id == "analysis-1"
    assert result.trajectory_id is not None
    assert len(store.list(project_id="project-adapter")) == 1
    store.close()
