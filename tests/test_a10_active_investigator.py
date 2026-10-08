from __future__ import annotations

from theseus_survivor_lab import ActiveInvestigator, ExperimentKind, ExperimentObservation, SurvivorCategory
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
    # Build the smallest offline survivor request required by Active Investigator.
    return SurvivorAnalysisRequest(
        schema_version=1,
        request_id="req-1",
        project_id="project-1",
        revision="rev-1",
        mutant=MutantEvidence("m1", "condition_to_not", "1", "app.py", "f", None, 10, 1, "x >= 18", "not x >= 18", None, MutantStatus.SURVIVED),
        source=SourceEvidence("app.py", "sha", "def f(x): return x >= 18", "def f(x): return x >= 18", 1, 1),
        selection=SelectionEvidence(selected_tests=("tests/test_app.py::test_other",), related_test_nodeids=("tests/test_app.py::test_boundary",), boundary_values_observed=False),
        executions=(),
        related_tests=(),
        related_dependencies=(),
        environment=EnvironmentEvidence(None, None, (), (), True),
        requested_modes=(RequestedMode.CLASSIFY,),
    )


class _SelectionExecutor:
    def execute(self, experiment, request):
        # Return authoritative evidence that the omitted related test distinguishes the survivor.
        outcome = "supports" if experiment.kind == ExperimentKind.RELATED_TEST else "refutes"
        return ExperimentObservation(experiment.kind, outcome, f"evidence-{experiment.kind.value}", True, 0.01)


def test_a10_active_investigator_resolves_survivor_from_verified_experiment() -> None:
    # Run a real multi-step policy loop whose belief changes only after verified experiment evidence.
    classification = SurvivorClassification(SurvivorCategory.SELECTION_ESCAPE, "suspected", 0.65, (), (SurvivorCategory.TEST_DATA_GAP,))
    result = ActiveInvestigator(confidence_threshold=0.80, max_steps=3).run(_request(), classification, _SelectionExecutor())
    assert result.steps
    assert result.steps[0].observation.verified is True
    assert result.final_belief.revision >= 1
    assert result.diagnosis in {SurvivorCategory.SELECTION_ESCAPE, SurvivorCategory.TEST_DATA_GAP}


class _UnverifiedExecutor:
    def execute(self, experiment, request):
        # Simulate an untrusted experiment result that must not alter authoritative beliefs.
        return ExperimentObservation(experiment.kind, "supports", "unverified", False, 0.01)


def test_a10_unverified_observation_never_changes_belief_revision() -> None:
    # Keep reasoning suggestions non-authoritative until an external verifier confirms the evidence.
    classification = SurvivorClassification(SurvivorCategory.INSUFFICIENT_EVIDENCE, "unknown", 0.1, (), ())
    result = ActiveInvestigator(confidence_threshold=0.99, max_steps=2).run(_request(), classification, _UnverifiedExecutor())
    assert result.final_belief.revision == 0
    assert result.diagnosis == SurvivorCategory.INSUFFICIENT_EVIDENCE
