from __future__ import annotations
import hashlib
from dataclasses import replace
from pathlib import Path
import pytest
from test_e24_survivor_runtime_adapter import _evidence
from theseus_survivor_adapter import (
    ProposalEvidenceStatus,
    SurvivorEvidenceError,
    SurvivorProposalConflict,
    SurvivorProposalLedger,
    SurvivorProposalPipeline,
    SurvivorProviderSubmission,
    SurvivorRuntimeAdapter,
)
from theseus_survivor_lab.providers import ProviderResponse, ProviderSuggestion

def _source_result():
    # Build one exact PR38 result from authoritative runtime evidence.
    return SurvivorRuntimeAdapter().analyze(_evidence())

def _submission(*, rationale: str, generated_code: str | None = None) -> SurvivorProviderSubmission:
    # Build one already-obtained external response without giving the pipeline a provider callback.
    return SurvivorProviderSubmission(
        provider_name="offline-fixture-provider",
        provider_version="1",
        deterministic=False,
        response=ProviderResponse(
            suggestions=(
                ProviderSuggestion(
                    hypothesis_kind="boundary_input",
                    title="provider candidate",
                    arrangement=(),
                    action=(),
                    assertions=(),
                    rationale=rationale,
                    generated_code=generated_code,
                ),
            ),
            warnings=(),
        ),
    )

def test_proposal_pipeline_accepts_only_exact_validated_pr38_result_and_is_deterministic(tmp_path: Path) -> None:
    # Generate a content-addressed proposal set without writing or executing candidate code.
    source = _source_result()
    before = tuple(tmp_path.rglob("*"))
    first = SurvivorProposalPipeline().generate(source)
    second = SurvivorProposalPipeline().generate(source)
    after = tuple(tmp_path.rglob("*"))
    assert first.accepted, (
        "deterministic local hypotheses did not produce accepted structured proposals; "
        f"analysis_id={source.analysis.analysis_id}; rejected={first.rejected}"
    )
    assert all(item.status is ProposalEvidenceStatus.ACCEPTED for item in first.accepted), (
        "accepted proposal set contains a non-accepted receipt; "
        f"proposal_set_id={first.proposal_set_id}; receipts={first.accepted}"
    )
    assert first.proposal_set_id == second.proposal_set_id, (
        "proposal identity changed across exact stateless replay; "
        f"first={first.proposal_set_id}; second={second.proposal_set_id}"
    )
    assert first.artifact.content == second.artifact.content
    assert first.artifact.content_sha256 == hashlib.sha256(first.artifact.content).hexdigest()
    assert before == after == (), (
        "proposal pipeline modified the filesystem; "
        f"before={before}; after={after}; proposal_set_id={first.proposal_set_id}"
    )

def test_exact_replay_is_idempotent_in_one_append_only_ledger() -> None:
    # Return duplicate receipts instead of appending a second proposal identity.
    source = _source_result()
    ledger = SurvivorProposalLedger()
    pipeline = SurvivorProposalPipeline(ledger)
    first = pipeline.generate(source)
    second = pipeline.generate(source)
    expected_duplicates = tuple(sorted(item.evidence_id for item in (*first.accepted, *first.rejected)))
    assert second.duplicate_evidence_ids == expected_duplicates, (
        "exact proposal replay was not recognized as duplicate evidence; "
        f"proposal_set_id={first.proposal_set_id}; expected={expected_duplicates}; actual={second.duplicate_evidence_ids}"
    )
    assert second.proposal_set_id == first.proposal_set_id
    assert second.artifact.content == first.artifact.content

def test_tampered_pr38_result_or_artifact_is_rejected_before_proposal_generation() -> None:
    # Re-authenticate request, result, and artifact bytes before entering proposal mode.
    source = _source_result()
    tampered_artifact = replace(source.artifact, content=source.artifact.content + b"{}\n")
    with pytest.raises(SurvivorEvidenceError, match="artifact identity conflict") as captured:
        SurvivorProposalPipeline().generate(replace(source, artifact=tampered_artifact))
    assert source.analysis.result_id in str(captured.value)
    tampered_analysis = replace(source.analysis, analysis_id="analysis-forged")
    with pytest.raises(SurvivorEvidenceError, match="result_id|deterministic PR38 replay|artifact"):
        SurvivorProposalPipeline().generate(replace(source, analysis=tampered_analysis))

def test_malformed_provider_response_is_retained_as_rejected_evidence_with_local_fallback() -> None:
    # Preserve malformed provider output as rejected evidence while retaining offline local proposals.
    source = _source_result()
    result = SurvivorProposalPipeline().generate(
        source,
        SurvivorProviderSubmission(
            provider_name="malformed-provider",
            provider_version="1",
            deterministic=False,
            response={"suggestions": "not-a-provider-response"},
        ),
    )
    assert result.accepted, (
        "malformed provider response disabled deterministic local proposal generation; "
        f"source_analysis_id={source.analysis.analysis_id}; rejected={result.rejected}"
    )
    assert len(result.rejected) == 1
    rejected = result.rejected[0]
    assert rejected.status is ProposalEvidenceStatus.REJECTED
    assert rejected.reasons == ("provider_response_schema_invalid",)
    assert rejected.provider_name == "malformed-provider"
    assert rejected.payload_sha256

def test_provider_candidate_with_file_write_is_rejected_and_never_executed(tmp_path: Path) -> None:
    # Reject dangerous generated code statically and prove no candidate side effect occurred.
    source = _source_result()
    marker = tmp_path / "provider-was-executed.txt"
    generated = (
        "def test_survivor_boundary_input_mutant_survivor():\n"
        f"    open({str(marker)!r}, 'w').write('executed')\n"
    )
    result = SurvivorProposalPipeline().generate(source, _submission(rationale="causal provider rationale", generated_code=generated))
    rejected = next(item for item in result.rejected if item.proposal_id is not None)
    assert any("generated_code_forbidden_call:open" in reason for reason in rejected.reasons), (
        "provider file-write candidate was not rejected by static validation; "
        f"proposal_id={rejected.proposal_id}; reasons={rejected.reasons}"
    )
    assert not marker.exists(), (
        "proposal pipeline executed or applied provider code; "
        f"proposal_id={rejected.proposal_id}; marker={marker}"
    )

def test_provider_candidate_without_local_causal_anchor_is_rejected() -> None:
    # Require every accepted provider proposal to retain a deterministic local hypothesis anchor.
    source = _source_result()
    submission = SurvivorProviderSubmission(
        provider_name="uncausal-provider",
        provider_version="1",
        deterministic=False,
        response=ProviderResponse(
            suggestions=(
                ProviderSuggestion(
                    hypothesis_kind="boundary_input",
                    title="unrelated",
                    arrangement=("unrelated arrangement",),
                    action=("unrelated action",),
                    assertions=("unrelated assertion",),
                    rationale="unrelated rationale",
                    generated_code=None,
                ),
            ),
            warnings=(),
        ),
    )
    result = SurvivorProposalPipeline().generate(source, submission)
    rejected = next(item for item in result.rejected if item.proposal_id is not None)
    assert "proposal_has_no_local_causal_anchor" in rejected.reasons, (
        "provider-only content replaced all local causal anchors; "
        f"proposal_id={rejected.proposal_id}; reasons={rejected.reasons}"
    )

def test_same_proposal_identity_with_different_payload_is_conflict() -> None:
    # Fence one proposal ID against provider content drift across retries.
    source = _source_result()
    pipeline = SurvivorProposalPipeline(SurvivorProposalLedger())
    first = pipeline.generate(source, _submission(rationale="first accepted rationale"))
    assert first.accepted, (
        "first external proposal was not accepted for conflict setup; "
        f"rejected={first.rejected}"
    )
    with pytest.raises(SurvivorProposalConflict, match="proposal identity conflict") as captured:
        pipeline.generate(source, _submission(rationale="second conflicting rationale"))
    proposal_id = first.accepted[0].proposal_id
    assert proposal_id is not None and proposal_id in str(captured.value), (
        "proposal conflict omitted the stable proposal identity; "
        f"proposal_id={proposal_id}; error={captured.value}"
    )

def test_unknown_and_duplicate_provider_kinds_are_rejected_individually() -> None:
    # Retain unknown and duplicate suggestions as separate rejected evidence without mixing identities.
    source = _source_result()
    base = ProviderSuggestion(
        hypothesis_kind="boundary_input",
        title="candidate",
        arrangement=(),
        action=(),
        assertions=(),
        rationale="accepted causal rationale",
        generated_code=None,
    )
    submission = SurvivorProviderSubmission(
        provider_name="multi-provider",
        provider_version="1",
        deterministic=False,
        response=ProviderResponse(
            suggestions=(
                replace(base, hypothesis_kind="invented_kind"),
                base,
                replace(base, rationale="duplicate payload"),
            ),
            warnings=(),
        ),
    )
    result = SurvivorProposalPipeline().generate(source, submission)
    reasons = {reason for item in result.rejected for reason in item.reasons}
    assert "provider_suggestion_unknown_hypothesis_kind" in reasons
    assert "provider_suggestion_duplicate_hypothesis_kind" in reasons
    assert len({item.evidence_id for item in result.rejected}) == len(result.rejected), (
        "rejected provider suggestions lost separate audit identities; "
        f"receipts={result.rejected}"
    )
