from __future__ import annotations
from dataclasses import replace
import pytest
from test_e24_survivor_human_review import _review
from test_e24_survivor_repair_workflow import _prepared_run, _validation
from theseus_survivor_adapter import (
    SurvivorEvidenceError,
    SurvivorRepairWorkflow,
    SurvivorWorkflowState,
    checkpoint_bytes,
    checkpoint_from_bytes,
)

def test_checkpoint_round_trip_and_recovery_do_not_rerun_completed_validation() -> None:
    # Restore awaiting-review state from durable identities without provider or validation execution.
    workflow, _, prepared = _prepared_run()
    validated = workflow.record_validation(prepared, _validation(prepared))
    content = checkpoint_bytes(validated.checkpoint)
    restored_checkpoint = checkpoint_from_bytes(content)
    assert restored_checkpoint == validated.checkpoint
    recovered = SurvivorRepairWorkflow().restore(
        content,
        validated.source,
        proposals=validated.proposals,
        validation=validated.validation,
    )
    assert recovered.checkpoint.state is SurvivorWorkflowState.AWAITING_HUMAN_REVIEW
    assert recovered.validation == validated.validation

def test_recovered_workflow_continues_from_human_review_checkpoint() -> None:
    # Resume at the last durable state and continue without rebuilding proposals.
    workflow, _, prepared = _prepared_run()
    validated = workflow.record_validation(prepared, _validation(prepared))
    recovered_workflow = SurvivorRepairWorkflow()
    recovered = recovered_workflow.restore(
        checkpoint_bytes(validated.checkpoint),
        validated.source,
        proposals=validated.proposals,
        validation=validated.validation,
    )
    reviewed = recovered_workflow.record_review(recovered, _review(recovered))
    assert reviewed.checkpoint.state is SurvivorWorkflowState.APPROVED

def test_stale_revision_or_execution_cannot_restore_checkpoint() -> None:
    # Recovery must fail closed when supplied source identities drift from the checkpoint.
    _, _, prepared = _prepared_run()
    stale_request = replace(
        prepared.source.request,
        revision="stale-revision",
        executions=(replace(prepared.source.request.executions[0], execution_id="stale-execution"),),
    )
    stale_source = replace(prepared.source, request=stale_request)
    with pytest.raises(SurvivorEvidenceError, match="stale|metadata"):
        SurvivorRepairWorkflow().restore(checkpoint_bytes(prepared.checkpoint), stale_source, proposals=prepared.proposals)

def test_tampered_checkpoint_is_rejected_before_resume() -> None:
    # Checkpoint bytes cannot change state or identity without changing the content address.
    _, _, prepared = _prepared_run()
    content = checkpoint_bytes(prepared.checkpoint).replace(b'"proposal_ready"', b'"approved"')
    with pytest.raises(SurvivorEvidenceError, match="identity mismatch|transition history"):
        checkpoint_from_bytes(content)
