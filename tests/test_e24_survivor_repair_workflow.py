from __future__ import annotations
import hashlib
from dataclasses import replace
import pytest
from test_e24_survivor_runtime_adapter import _evidence
from theseus_survivor_adapter import (
    FreshProposalValidationEvidence,
    SurvivorEvidenceError,
    SurvivorRepairWorkflow,
    SurvivorRuntimeAdapter,
    SurvivorWorkflowState,
)

def _prepared_run(workflow: SurvivorRepairWorkflow | None = None):
    # Build one exact proposal-ready workflow from authoritative runtime evidence.
    resolved = workflow or SurvivorRepairWorkflow()
    source = SurvivorRuntimeAdapter().analyze(_evidence())
    return resolved, source, resolved.prepare(source)

def _validation(run, **changes) -> FreshProposalValidationEvidence:
    # Build successful fresh isolated evidence for the workflow-selected proposal.
    proposal = next(item.proposal for item in run.proposals.accepted if item.proposal_id == run.checkpoint.proposal_id)
    values = {
        "proposal_id": proposal.proposal_id,
        "project_id": run.source.request.project_id,
        "revision": run.source.request.revision,
        "mutant_id": run.source.request.mutant.mutant_id,
        "source_execution_id": run.source.request.executions[0].execution_id,
        "validation_execution_id": "validation-workflow-1",
        "candidate_test_nodeid": proposal.target_test_nodeid or "",
        "candidate_test_sha256": hashlib.sha256((proposal.generated_code or "").encode("utf-8")).hexdigest(),
        "fresh_run": True,
        "isolated_workspace": True,
        "original_passed": True,
        "mutant_killed": True,
        "regression_passed": True,
        "stable": True,
        "timed_out": False,
        "infrastructure_failure": False,
        "restore_verified": True,
    }
    values.update(changes)
    return FreshProposalValidationEvidence(**values)

def test_workflow_reuses_existing_classification_context_and_proposal_authorities() -> None:
    # Record explicit states without introducing a parallel classifier or causal-context builder.
    _, source, run = _prepared_run()
    assert run.checkpoint.state is SurvivorWorkflowState.PROPOSAL_READY
    assert run.checkpoint.classification == source.analysis.classification.category.value
    assert run.checkpoint.causal_context_id == run.proposals.causal_context.context_id
    assert run.checkpoint.proposal_set_id == run.proposals.proposal_set_id
    assert tuple(item.state for item in run.checkpoint.transitions) == (
        SurvivorWorkflowState.CREATED,
        SurvivorWorkflowState.CLASSIFIED,
        SurvivorWorkflowState.CONTEXT_READY,
        SurvivorWorkflowState.PROPOSAL_READY,
    )

def test_verified_evidence_advances_mutation_regression_and_human_review_states() -> None:
    # One verified PR35 receipt must prove both mutation kill and green regression before review.
    workflow, _, run = _prepared_run()
    validated = workflow.record_validation(run, _validation(run))
    assert validated.checkpoint.state is SurvivorWorkflowState.AWAITING_HUMAN_REVIEW
    assert validated.validation is not None
    assert tuple(item.state for item in validated.checkpoint.transitions[-3:]) == (
        SurvivorWorkflowState.MUTATION_VALIDATED,
        SurvivorWorkflowState.REGRESSION_VALIDATED,
        SurvivorWorkflowState.AWAITING_HUMAN_REVIEW,
    )

def test_provider_acceptance_without_fresh_validation_cannot_reach_review_or_export() -> None:
    # A proposal-ready workflow remains unverified until fresh isolated evidence is supplied.
    workflow, _, run = _prepared_run()
    with pytest.raises(SurvivorEvidenceError, match="human review requires"):
        workflow.record_review(run, object())
    assert run.checkpoint.state is SurvivorWorkflowState.PROPOSAL_READY

def test_timeout_stale_or_non_killing_validation_fails_closed() -> None:
    # Rejected fresh evidence becomes a bounded typed failure rather than a successful workflow.
    workflow, _, run = _prepared_run()
    failed = workflow.record_validation(
        run,
        _validation(run, revision="stale", timed_out=True, mutant_killed=False, regression_passed=False),
    )
    assert failed.checkpoint.state is SurvivorWorkflowState.FAILED
    assert failed.checkpoint.failure is not None
    assert failed.checkpoint.failure.code == "validation_rejected"
    assert "stale_identity:revision" in failed.checkpoint.failure.message
    assert "validation_timed_out" in failed.checkpoint.failure.message

def test_semantic_workflow_identity_ignores_checkout_relocation() -> None:
    # Windows and Linux checkout roots must not change workflow identity.
    workflow = SurvivorRepairWorkflow()
    source = SurvivorRuntimeAdapter().analyze(_evidence())
    first = workflow.prepare(source)
    relocated_evidence = replace(
        _evidence(),
        project=replace(_evidence().project, root_path="/home/runner/project", main_root_path="/home/runner/project"),
    )
    relocated = SurvivorRuntimeAdapter().analyze(relocated_evidence)
    second = workflow.prepare(relocated)
    assert first.checkpoint.workflow_id == second.checkpoint.workflow_id
    assert first.checkpoint.checkpoint_id == second.checkpoint.checkpoint_id

def test_exact_prepare_replay_rejects_different_provider_payload() -> None:
    # The same workflow identity cannot silently replace its durable proposal input.
    from theseus_survivor_adapter import SurvivorProposalConflict, SurvivorProviderSubmission
    from theseus_survivor_lab.providers import ProviderResponse
    workflow = SurvivorRepairWorkflow()
    source = SurvivorRuntimeAdapter().analyze(_evidence())
    first = SurvivorProviderSubmission("fixture", "1", True, ProviderResponse((), ("one",)))
    second = SurvivorProviderSubmission("fixture", "1", True, ProviderResponse((), ("two",)))
    workflow.prepare(source, first)
    with pytest.raises(SurvivorProposalConflict, match="provider input conflict"):
        workflow.prepare(source, second)

def test_exact_validation_replay_returns_existing_checkpoint() -> None:
    # Replaying the same already-obtained validation evidence must not advance or duplicate state.
    workflow, _, prepared = _prepared_run()
    evidence = _validation(prepared)
    first = workflow.record_validation(prepared, evidence)
    second = workflow.record_validation(first, evidence)
    assert second == first
