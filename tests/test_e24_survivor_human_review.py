from __future__ import annotations
import pytest
from test_e24_survivor_repair_workflow import _prepared_run, _validation
from theseus_survivor_adapter import (
    SurvivorExportFormat,
    SurvivorHumanReviewEvidence,
    SurvivorProposalConflict,
    SurvivorRepairWorkflow,
    SurvivorReviewDecision,
    SurvivorWorkflowState,
)

def _review(run, **changes) -> SurvivorHumanReviewEvidence:
    # Build one explicit human decision linked to the verified proposal and validation.
    values = {
        "review_id": "review-workflow-1",
        "reviewer_id": "reviewer-alice",
        "decision": SurvivorReviewDecision.APPROVE,
        "reviewed_at": "2026-08-06T03:00:00+03:00",
        "proposal_id": run.checkpoint.proposal_id,
        "validation_id": run.validation.validation_id,
        "note": "approved after mutation proof",
    }
    values.update(changes)
    return SurvivorHumanReviewEvidence(**values)

def _validated_run(workflow: SurvivorRepairWorkflow | None = None):
    # Build one workflow waiting for a human decision.
    resolved, _, prepared = _prepared_run(workflow)
    return resolved, resolved.record_validation(prepared, _validation(prepared))

def test_human_approval_is_required_and_timestamp_is_canonical_utc() -> None:
    # Approval must bind reviewer, proposal, validation, and canonical review time.
    workflow, run = _validated_run()
    approved = workflow.record_review(run, _review(run))
    assert approved.checkpoint.state is SurvivorWorkflowState.APPROVED
    assert approved.review.reviewed_at == "2026-08-06T00:00:00.000000Z"
    assert approved.review.proposal_id == run.checkpoint.proposal_id
    assert approved.review.validation_id == run.validation.validation_id

def test_exact_review_replay_is_idempotent() -> None:
    # Repeating the same review identity and payload must return the accepted decision.
    workflow, run = _validated_run()
    evidence = _review(run)
    first = workflow.record_review(run, evidence)
    second = workflow.record_review(first, evidence)
    assert first == second

def test_conflicting_decision_for_same_review_identity_is_rejected() -> None:
    # One review identity cannot change from approval to rejection across retries.
    workflow, run = _validated_run()
    approved = workflow.record_review(run, _review(run))
    with pytest.raises(SurvivorProposalConflict, match="human review identity conflict"):
        workflow.record_review(approved, _review(run, decision=SurvivorReviewDecision.REJECT))

def test_rejection_is_terminal_and_cannot_export() -> None:
    # Human rejection must never be promoted to approved or exported state.
    workflow, run = _validated_run()
    rejected = workflow.record_review(run, _review(run, decision=SurvivorReviewDecision.REJECT))
    assert rejected.checkpoint.state is SurvivorWorkflowState.REJECTED
    with pytest.raises(Exception, match="approved"):
        workflow.export(rejected, SurvivorExportFormat.OVERLAY_ZIP)
