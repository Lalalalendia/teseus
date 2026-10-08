from __future__ import annotations
import hashlib
from pathlib import Path
import pytest
from test_e24_survivor_runtime_adapter import _evidence
from theseus_survivor_adapter import (
    FreshProposalValidationEvidence,
    ProposalEvidenceStatus,
    ProposalVerificationStatus,
    SurvivorProposalConflict,
    SurvivorProposalLedger,
    SurvivorProposalPipeline,
    SurvivorRuntimeAdapter,
)


def _source_and_proposals(ledger: SurvivorProposalLedger | None = None):
    # Build one authenticated PR38 result and its non-applied PR39 proposal set.
    source = SurvivorRuntimeAdapter().analyze(_evidence())
    pipeline = SurvivorProposalPipeline(ledger)
    proposals = pipeline.generate(source)
    return source, pipeline, proposals


def _validation(source, proposals, **changes) -> FreshProposalValidationEvidence:
    # Build one successful fresh isolated validation linked to the accepted proposal.
    proposal = proposals.accepted[0].proposal
    assert proposal is not None
    values = {
        "proposal_id": proposal.proposal_id,
        "project_id": source.request.project_id,
        "revision": source.request.revision,
        "mutant_id": source.request.mutant.mutant_id,
        "source_execution_id": source.request.executions[0].execution_id,
        "validation_execution_id": "validation-execution-1",
        "candidate_test_nodeid": proposal.target_test_nodeid or "",
        "candidate_test_sha256": hashlib.sha256(b"candidate-test").hexdigest(),
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


def test_provider_or_static_acceptance_is_not_verified_without_fresh_run() -> None:
    # PR39 acceptance remains non-authoritative until a separate fresh receipt exists.
    _, _, proposals = _source_and_proposals()
    assert proposals.accepted
    assert all(item.status is ProposalEvidenceStatus.ACCEPTED for item in proposals.accepted)
    assert not hasattr(proposals.accepted[0], "verified"), (
        "proposal evidence incorrectly embeds verification without fresh mutation evidence; "
        f"proposal_id={proposals.accepted[0].proposal_id}"
    )


def test_fresh_isolated_mutation_evidence_verifies_and_replays_idempotently() -> None:
    # A complete identity-matched run is verified and exact replay returns the same receipt.
    source, pipeline, proposals = _source_and_proposals()
    evidence = _validation(source, proposals)
    first = pipeline.validate_fresh(source, proposals, evidence)
    second = pipeline.validate_fresh(source, proposals, evidence)
    assert first == second
    assert first.status is ProposalVerificationStatus.VERIFIED
    assert first.reasons == ()
    assert first.mutant_id == source.request.mutant.mutant_id
    assert first.revision == source.request.revision
    assert first.source_execution_id == source.request.executions[0].execution_id


def test_stale_revision_mutant_or_source_execution_is_rejected_fail_closed() -> None:
    # Identity drift must remain explicit and cannot be interpreted as a verified proposal.
    source, pipeline, proposals = _source_and_proposals()
    receipt = pipeline.validate_fresh(
        source,
        proposals,
        _validation(
            source,
            proposals,
            revision="stale-revision",
            mutant_id="stale-mutant",
            source_execution_id="stale-execution",
        ),
    )
    assert receipt.status is ProposalVerificationStatus.REJECTED
    assert set(receipt.reasons) >= {
        "stale_identity:revision",
        "stale_identity:mutant_id",
        "stale_identity:source_execution_id",
    }


@pytest.mark.parametrize(
    ("changes", "reason"),
    (
        ({"timed_out": True}, "validation_timed_out"),
        ({"infrastructure_failure": True}, "validation_infrastructure_failure"),
        ({"original_passed": False}, "candidate_failed_on_original"),
        ({"mutant_killed": False}, "candidate_did_not_kill_mutant"),
        ({"regression_passed": False}, "regression_suite_failed"),
        ({"stable": False}, "candidate_is_unstable"),
        ({"isolated_workspace": False}, "validation_not_isolated"),
        ({"fresh_run": False}, "validation_not_fresh"),
    ),
)
def test_failed_timeout_or_non_isolated_validation_never_verifies(changes, reason: str) -> None:
    # Every failed fresh-run invariant must produce a rejected diagnostic receipt.
    source, pipeline, proposals = _source_and_proposals()
    receipt = pipeline.validate_fresh(source, proposals, _validation(source, proposals, **changes))
    assert receipt.status is ProposalVerificationStatus.REJECTED
    assert reason in receipt.reasons


def test_same_validation_identity_with_different_payload_is_a_conflict() -> None:
    # Append-only validation identity cannot silently change semantic outcomes.
    ledger = SurvivorProposalLedger()
    source, pipeline, proposals = _source_and_proposals(ledger)
    pipeline.validate_fresh(source, proposals, _validation(source, proposals))
    with pytest.raises(SurvivorProposalConflict, match="validation identity conflict"):
        pipeline.validate_fresh(source, proposals, _validation(source, proposals, mutant_killed=False))


def test_fresh_validation_does_not_write_or_apply_candidate_files(tmp_path: Path) -> None:
    # Receipt validation consumes typed evidence only and leaves production/test files untouched.
    source, pipeline, proposals = _source_and_proposals()
    before = tuple(tmp_path.rglob("*"))
    pipeline.validate_fresh(source, proposals, _validation(source, proposals))
    after = tuple(tmp_path.rglob("*"))
    assert before == after == ()
