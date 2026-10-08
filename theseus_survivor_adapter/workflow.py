"""Durable deterministic state machine for the complete survivor repair workflow."""
from __future__ import annotations
import hashlib
import json
from dataclasses import replace
from threading import RLock
from typing import Any, Mapping
from theseus_survivor_lab.providers import ProviderResponse, provider_response_from_dict, provider_response_to_dict
from theseus_survivor_lab.serialization import canonical_json
from theseus_survivor_lab.validation import canonical_utc_timestamp, sanitize_text
from .contracts import (
    FreshProposalValidationEvidence,
    ProposalVerificationStatus,
    SurvivorAdapterResult,
    SurvivorExportBundle,
    SurvivorExportFormat,
    SurvivorHumanReviewEvidence,
    SurvivorHumanReviewReceipt,
    SurvivorProposalConflict,
    SurvivorProposalEvidence,
    SurvivorProposalPipelineResult,
    SurvivorProposalValidationReceipt,
    SurvivorProviderSubmission,
    SurvivorReviewDecision,
    SurvivorWorkflowCheckpoint,
    SurvivorWorkflowFailure,
    SurvivorWorkflowRun,
    SurvivorWorkflowState,
    SurvivorWorkflowTransition,
)
from .contracts import SurvivorEvidenceError
from .export import build_survivor_export, publish_survivor_export
from .proposal_pipeline import SurvivorProposalLedger, SurvivorProposalPipeline

_STATE_ORDER = {
    SurvivorWorkflowState.CREATED: 0,
    SurvivorWorkflowState.CLASSIFIED: 1,
    SurvivorWorkflowState.CONTEXT_READY: 2,
    SurvivorWorkflowState.PROPOSAL_READY: 3,
    SurvivorWorkflowState.MUTATION_VALIDATED: 4,
    SurvivorWorkflowState.REGRESSION_VALIDATED: 5,
    SurvivorWorkflowState.AWAITING_HUMAN_REVIEW: 6,
    SurvivorWorkflowState.APPROVED: 7,
    SurvivorWorkflowState.REJECTED: 7,
    SurvivorWorkflowState.EXPORTED: 8,
    SurvivorWorkflowState.FAILED: 9,
}

def _sha256(value: bytes) -> str:
    # Hash one canonical workflow payload.
    return hashlib.sha256(value).hexdigest()

def _stable_hash(value: Any) -> str:
    # Hash JSON-compatible workflow evidence with canonical JSON.
    try:
        return _sha256(canonical_json(value).encode("utf-8"))
    except (TypeError, ValueError) as exc:
        raise SurvivorEvidenceError(f"workflow evidence is not JSON-compatible: error={exc}") from exc

def _source_identities(source: SurvivorAdapterResult) -> dict[str, str | None]:
    # Extract the authoritative identities already authenticated by PR38.
    if len(source.request.executions) != 1:
        raise SurvivorEvidenceError(
            f"workflow requires one authoritative source execution: actual={len(source.request.executions)}"
        )
    metadata = source.artifact.metadata
    execution = source.request.executions[0]
    identities = {
        "campaign_id": str(metadata.get("campaign_id") or ""),
        "project_id": source.request.project_id,
        "revision": source.request.revision,
        "mutant_id": source.request.mutant.mutant_id,
        "source_execution_id": execution.execution_id,
        "source_analysis_id": source.analysis.analysis_id,
        "source_result_id": source.analysis.result_id,
        "source_event_id": source.source_event_id,
        "graph_fingerprint": source.graph_fingerprint,
    }
    missing = tuple(sorted(key for key, value in identities.items() if key != "revision" and not value))
    if missing:
        raise SurvivorEvidenceError(f"workflow source identities are incomplete: fields={missing}")
    expected_metadata = {
        "project_id": identities["project_id"],
        "revision_id": identities["revision"] or "",
        "mutant_id": identities["mutant_id"],
        "execution_id": identities["source_execution_id"],
        "analysis_id": identities["source_analysis_id"],
        "result_id": identities["source_result_id"],
        "source_event_id": identities["source_event_id"],
        "graph_fingerprint": identities["graph_fingerprint"],
    }
    conflicts = tuple(
        sorted(key for key, expected in expected_metadata.items() if str(metadata.get(key, "")) != str(expected or ""))
    )
    if conflicts:
        raise SurvivorEvidenceError(
            f"workflow source artifact metadata is stale or contradictory: fields={conflicts}; result_id={source.analysis.result_id}"
        )
    return identities

def _workflow_id(source: SurvivorAdapterResult) -> str:
    # Identify one semantic workflow independently of checkout, CWD, locale, or timestamps.
    identities = _source_identities(source)
    return "survivor-workflow-" + _stable_hash(identities)[:24]

def _provider_submission_fingerprint(submission: SurvivorProviderSubmission | None) -> str:
    # Identify an already-obtained provider submission without invoking a provider.
    if submission is None:
        return _stable_hash({"provider": None})
    response = submission.response
    try:
        if isinstance(response, ProviderResponse):
            payload = provider_response_to_dict(response)
        elif isinstance(response, Mapping):
            payload = provider_response_to_dict(provider_response_from_dict(dict(response)))
        else:
            payload = {"type": f"{type(response).__module__}.{type(response).__qualname__}", "value": sanitize_text(repr(response), 1200)}
    except Exception as exc:
        payload = {"malformed": True, "type": type(exc).__name__, "value": sanitize_text(repr(response), 1200)}
    return _stable_hash({
        "provider_name": sanitize_text(submission.provider_name, 200),
        "provider_version": sanitize_text(submission.provider_version, 100),
        "deterministic": submission.deterministic is True,
        "response": payload,
    })

def _transition(workflow_id: str, state: SurvivorWorkflowState, evidence_id: str | None) -> SurvivorWorkflowTransition:
    # Build one deterministic transition identity from state and authoritative evidence.
    transition_id = "workflow-transition-" + _stable_hash({
        "workflow_id": workflow_id,
        "state": state.value,
        "evidence_id": evidence_id,
    })[:24]
    return SurvivorWorkflowTransition(transition_id, state, evidence_id)

def _checkpoint_payload(checkpoint: SurvivorWorkflowCheckpoint, *, include_id: bool = True) -> dict[str, Any]:
    # Serialize one checkpoint for identity, storage, and recovery validation.
    payload = {
        "schema_version": 1,
        "workflow_id": checkpoint.workflow_id,
        "state": checkpoint.state.value,
        "campaign_id": checkpoint.campaign_id,
        "project_id": checkpoint.project_id,
        "revision": checkpoint.revision,
        "mutant_id": checkpoint.mutant_id,
        "source_execution_id": checkpoint.source_execution_id,
        "source_analysis_id": checkpoint.source_analysis_id,
        "source_result_id": checkpoint.source_result_id,
        "source_event_id": checkpoint.source_event_id,
        "graph_fingerprint": checkpoint.graph_fingerprint,
        "classification": checkpoint.classification,
        "causal_context_id": checkpoint.causal_context_id,
        "proposal_set_id": checkpoint.proposal_set_id,
        "proposal_id": checkpoint.proposal_id,
        "validation_id": checkpoint.validation_id,
        "review_id": checkpoint.review_id,
        "export_id": checkpoint.export_id,
        "transitions": [
            {"transition_id": item.transition_id, "state": item.state.value, "evidence_id": item.evidence_id}
            for item in checkpoint.transitions
        ],
        "failure": (
            {"code": checkpoint.failure.code, "message": checkpoint.failure.message}
            if checkpoint.failure is not None
            else None
        ),
    }
    if include_id:
        payload["checkpoint_id"] = checkpoint.checkpoint_id
    return payload

def _checkpoint(
    source: SurvivorAdapterResult,
    state: SurvivorWorkflowState,
    *,
    previous: SurvivorWorkflowCheckpoint | None = None,
    evidence_id: str | None = None,
    classification: str | None = None,
    causal_context_id: str | None = None,
    proposal_set_id: str | None = None,
    proposal_id: str | None = None,
    validation_id: str | None = None,
    review_id: str | None = None,
    export_id: str | None = None,
    failure: SurvivorWorkflowFailure | None = None,
) -> SurvivorWorkflowCheckpoint:
    # Create the next immutable checkpoint without allowing identity drift or state regression.
    identities = _source_identities(source)
    workflow_id = _workflow_id(source)
    if previous is not None:
        _validate_checkpoint_source(previous, source)
        if _STATE_ORDER[state] < _STATE_ORDER[previous.state] and state not in {SurvivorWorkflowState.FAILED}:
            raise SurvivorProposalConflict(
                f"workflow state regression: workflow_id={workflow_id}; previous={previous.state.value}; incoming={state.value}"
            )
    transition = _transition(workflow_id, state, evidence_id)
    transitions = previous.transitions if previous is not None else ()
    if not transitions or transitions[-1] != transition:
        transitions = (*transitions, transition)
    candidate = SurvivorWorkflowCheckpoint(
        checkpoint_id="",
        workflow_id=workflow_id,
        state=state,
        campaign_id=str(identities["campaign_id"]),
        project_id=str(identities["project_id"]),
        revision=identities["revision"],
        mutant_id=str(identities["mutant_id"]),
        source_execution_id=str(identities["source_execution_id"]),
        source_analysis_id=str(identities["source_analysis_id"]),
        source_result_id=str(identities["source_result_id"]),
        source_event_id=str(identities["source_event_id"]),
        graph_fingerprint=str(identities["graph_fingerprint"]),
        classification=classification if classification is not None else (previous.classification if previous else None),
        causal_context_id=causal_context_id if causal_context_id is not None else (previous.causal_context_id if previous else None),
        proposal_set_id=proposal_set_id if proposal_set_id is not None else (previous.proposal_set_id if previous else None),
        proposal_id=proposal_id if proposal_id is not None else (previous.proposal_id if previous else None),
        validation_id=validation_id if validation_id is not None else (previous.validation_id if previous else None),
        review_id=review_id if review_id is not None else (previous.review_id if previous else None),
        export_id=export_id if export_id is not None else (previous.export_id if previous else None),
        transitions=transitions,
        failure=failure,
    )
    checkpoint_id = "workflow-checkpoint-" + _stable_hash(_checkpoint_payload(candidate, include_id=False))[:24]
    return replace(candidate, checkpoint_id=checkpoint_id)

def checkpoint_bytes(checkpoint: SurvivorWorkflowCheckpoint) -> bytes:
    # Encode one durable checkpoint with deterministic UTF-8 JSON and final newline.
    return (canonical_json(_checkpoint_payload(checkpoint)) + "\n").encode("utf-8")

def checkpoint_from_bytes(content: bytes) -> SurvivorWorkflowCheckpoint:
    # Restore and authenticate one checkpoint without executing any workflow stage.
    try:
        data = json.loads(content.decode("utf-8-sig"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise SurvivorEvidenceError(f"workflow checkpoint is not valid UTF-8 JSON: error={exc}") from exc
    if not isinstance(data, Mapping) or int(data.get("schema_version", 0)) != 1:
        raise SurvivorEvidenceError("workflow checkpoint schema_version is unsupported")
    try:
        transitions = tuple(
            SurvivorWorkflowTransition(
                transition_id=str(item["transition_id"]),
                state=SurvivorWorkflowState(str(item["state"])),
                evidence_id=str(item["evidence_id"]) if item.get("evidence_id") is not None else None,
            )
            for item in data.get("transitions", [])
        )
        failure_data = data.get("failure")
        failure = (
            SurvivorWorkflowFailure(str(failure_data["code"]), str(failure_data["message"]))
            if isinstance(failure_data, Mapping)
            else None
        )
        checkpoint = SurvivorWorkflowCheckpoint(
            checkpoint_id=str(data["checkpoint_id"]),
            workflow_id=str(data["workflow_id"]),
            state=SurvivorWorkflowState(str(data["state"])),
            campaign_id=str(data["campaign_id"]),
            project_id=str(data["project_id"]),
            revision=str(data["revision"]) if data.get("revision") is not None else None,
            mutant_id=str(data["mutant_id"]),
            source_execution_id=str(data["source_execution_id"]),
            source_analysis_id=str(data["source_analysis_id"]),
            source_result_id=str(data["source_result_id"]),
            source_event_id=str(data["source_event_id"]),
            graph_fingerprint=str(data["graph_fingerprint"]),
            classification=str(data["classification"]) if data.get("classification") is not None else None,
            causal_context_id=str(data["causal_context_id"]) if data.get("causal_context_id") is not None else None,
            proposal_set_id=str(data["proposal_set_id"]) if data.get("proposal_set_id") is not None else None,
            proposal_id=str(data["proposal_id"]) if data.get("proposal_id") is not None else None,
            validation_id=str(data["validation_id"]) if data.get("validation_id") is not None else None,
            review_id=str(data["review_id"]) if data.get("review_id") is not None else None,
            export_id=str(data["export_id"]) if data.get("export_id") is not None else None,
            transitions=transitions,
            failure=failure,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise SurvivorEvidenceError(f"workflow checkpoint fields are malformed: error={exc}") from exc
    expected_id = "workflow-checkpoint-" + _stable_hash(_checkpoint_payload(replace(checkpoint, checkpoint_id=""), include_id=False))[:24]
    if checkpoint.checkpoint_id != expected_id:
        raise SurvivorEvidenceError(
            f"workflow checkpoint identity mismatch: expected={expected_id}; actual={checkpoint.checkpoint_id}"
        )
    if not checkpoint.transitions or checkpoint.transitions[-1].state is not checkpoint.state:
        raise SurvivorEvidenceError(
            f"workflow checkpoint transition history is inconsistent: checkpoint_id={checkpoint.checkpoint_id}"
        )
    return checkpoint

def _validate_checkpoint_source(checkpoint: SurvivorWorkflowCheckpoint, source: SurvivorAdapterResult) -> None:
    # Fence every resumed operation against stale campaign, revision, mutant, and execution evidence.
    identities = _source_identities(source)
    expected = {
        "workflow_id": _workflow_id(source),
        "campaign_id": identities["campaign_id"],
        "project_id": identities["project_id"],
        "revision": identities["revision"],
        "mutant_id": identities["mutant_id"],
        "source_execution_id": identities["source_execution_id"],
        "source_analysis_id": identities["source_analysis_id"],
        "source_result_id": identities["source_result_id"],
        "source_event_id": identities["source_event_id"],
        "graph_fingerprint": identities["graph_fingerprint"],
    }
    conflicts = tuple(sorted(key for key, value in expected.items() if getattr(checkpoint, key) != value))
    if conflicts:
        raise SurvivorEvidenceError(
            f"stale workflow source evidence: checkpoint_id={checkpoint.checkpoint_id}; fields={conflicts}"
        )

def _selected_proposal(proposals: SurvivorProposalPipelineResult, proposal_id: str | None = None) -> SurvivorProposalEvidence:
    # Select one accepted proposal deterministically or require an explicit identity when ambiguous.
    accepted = tuple(item for item in proposals.accepted if item.proposal_id is not None)
    if proposal_id is not None:
        matches = tuple(item for item in accepted if item.proposal_id == proposal_id)
        if len(matches) != 1:
            raise SurvivorEvidenceError(
                f"workflow proposal identity is unavailable: proposal_id={proposal_id}; accepted={tuple(item.proposal_id for item in accepted)}"
            )
        return matches[0]
    if len(accepted) != 1:
        raise SurvivorEvidenceError(
            f"workflow requires one selected proposal or explicit proposal_id: accepted={tuple(item.proposal_id for item in accepted)}"
        )
    return accepted[0]

def _review_payload(evidence: SurvivorHumanReviewEvidence) -> dict[str, Any]:
    # Normalize one human review while keeping secrets and physical paths out of the receipt.
    reviewer_id = sanitize_text(evidence.reviewer_id, 200) or ""
    review_id = sanitize_text(evidence.review_id, 200) or ""
    note = sanitize_text(evidence.note, 1200) or ""
    if not reviewer_id or not review_id:
        raise SurvivorEvidenceError("human review requires non-empty review_id and reviewer_id")
    return {
        "review_id": review_id,
        "reviewer_id": reviewer_id,
        "decision": SurvivorReviewDecision(evidence.decision).value,
        "reviewed_at": canonical_utc_timestamp(evidence.reviewed_at),
        "proposal_id": evidence.proposal_id,
        "validation_id": evidence.validation_id,
        "note": note,
    }

def _review_receipt(run: SurvivorWorkflowRun, evidence: SurvivorHumanReviewEvidence) -> SurvivorHumanReviewReceipt:
    # Authenticate one human decision against the exact verified proposal and validation receipt.
    if run.validation is None or run.validation.status is not ProposalVerificationStatus.VERIFIED:
        raise SurvivorEvidenceError("human review requires verified fresh validation")
    payload = _review_payload(evidence)
    if payload["proposal_id"] != run.checkpoint.proposal_id or payload["validation_id"] != run.validation.validation_id:
        raise SurvivorEvidenceError(
            f"human review identity is stale: proposal_id={payload['proposal_id']}; validation_id={payload['validation_id']}"
        )
    return SurvivorHumanReviewReceipt(
        review_id=str(payload["review_id"]),
        reviewer_id=str(payload["reviewer_id"]),
        decision=SurvivorReviewDecision(str(payload["decision"])),
        reviewed_at=str(payload["reviewed_at"]),
        proposal_id=str(payload["proposal_id"]),
        validation_id=str(payload["validation_id"]),
        payload_sha256=_stable_hash(payload),
        note=str(payload["note"]),
    )

class SurvivorWorkflowLedger:
    """Thread-safe append-only checkpoints, reviews, and exports for replay fencing."""
    def __init__(self) -> None:
        # Keep immutable hashes and latest checkpoints without invoking any external stage.
        self._checkpoints: dict[str, str] = {}
        self._latest: dict[str, SurvivorWorkflowCheckpoint] = {}
        self._reviews: dict[str, str] = {}
        self._exports: dict[str, str] = {}
        self._lock = RLock()
    def commit_checkpoint(self, checkpoint: SurvivorWorkflowCheckpoint) -> bool:
        # Append one checkpoint, return exact duplicate, or reject conflicting/regressive state.
        digest = _sha256(checkpoint_bytes(checkpoint))
        with self._lock:
            existing = self._checkpoints.get(checkpoint.checkpoint_id)
            if existing is not None:
                if existing != digest:
                    raise SurvivorProposalConflict(
                        f"workflow checkpoint identity conflict: checkpoint_id={checkpoint.checkpoint_id}"
                    )
                return True
            latest = self._latest.get(checkpoint.workflow_id)
            if latest is not None and _STATE_ORDER[checkpoint.state] < _STATE_ORDER[latest.state]:
                raise SurvivorProposalConflict(
                    f"workflow checkpoint regression: workflow_id={checkpoint.workflow_id}; latest={latest.state.value}; incoming={checkpoint.state.value}"
                )
            self._checkpoints[checkpoint.checkpoint_id] = digest
            self._latest[checkpoint.workflow_id] = checkpoint
            return False
    def latest(self, workflow_id: str) -> SurvivorWorkflowCheckpoint | None:
        # Return the latest immutable checkpoint without mutating replay state.
        with self._lock:
            return self._latest.get(workflow_id)
    def commit_review(self, receipt: SurvivorHumanReviewReceipt) -> bool:
        # Append one review decision or reject the same review identity with different content.
        digest = _stable_hash({
            "review_id": receipt.review_id,
            "reviewer_id": receipt.reviewer_id,
            "decision": receipt.decision.value,
            "reviewed_at": receipt.reviewed_at,
            "proposal_id": receipt.proposal_id,
            "validation_id": receipt.validation_id,
            "payload_sha256": receipt.payload_sha256,
            "note": receipt.note,
        })
        with self._lock:
            existing = self._reviews.get(receipt.review_id)
            if existing is not None:
                if existing != digest:
                    raise SurvivorProposalConflict(
                        f"human review identity conflict: review_id={receipt.review_id}"
                    )
                return True
            self._reviews[receipt.review_id] = digest
            return False
    def commit_export(self, bundle: SurvivorExportBundle) -> bool:
        # Append one export identity or reject conflicting immutable manifest content.
        digest = _stable_hash({
            "export_id": bundle.export_id,
            "format": bundle.export_format.value,
            "manifest_sha256": bundle.manifest_sha256,
            "files": [(item.path, item.content_sha256) for item in bundle.files],
        })
        with self._lock:
            existing = self._exports.get(bundle.export_id)
            if existing is not None:
                if existing != digest:
                    raise SurvivorProposalConflict(f"survivor export identity conflict: export_id={bundle.export_id}")
                return True
            self._exports[bundle.export_id] = digest
            return False

class SurvivorRepairWorkflow:
    """Compose existing survivor analysis, proposal, validation, review, and export authorities."""
    def __init__(
        self,
        proposal_pipeline: SurvivorProposalPipeline | None = None,
        ledger: SurvivorWorkflowLedger | None = None,
    ) -> None:
        # Reuse one proposal authority and one append-only workflow ledger.
        self._proposal_pipeline = proposal_pipeline if proposal_pipeline is not None else SurvivorProposalPipeline(SurvivorProposalLedger())
        self._ledger = ledger if ledger is not None else SurvivorWorkflowLedger()
        self._prepared_cache: dict[str, SurvivorWorkflowRun] = {}
        self._prepare_inputs: dict[str, str | None] = {}
    def prepare(
        self,
        source: SurvivorAdapterResult,
        provider_submission: SurvivorProviderSubmission | None = None,
        *,
        proposal_id: str | None = None,
    ) -> SurvivorWorkflowRun:
        # Advance authenticated PR38 evidence through existing classification, context, and proposal authorities.
        workflow_id = _workflow_id(source)
        input_fingerprint = _provider_submission_fingerprint(provider_submission)
        cached = self._prepared_cache.get(workflow_id)
        if cached is not None:
            _validate_checkpoint_source(cached.checkpoint, source)
            previous_input = self._prepare_inputs.get(workflow_id)
            if previous_input is None:
                if provider_submission is not None:
                    raise SurvivorProposalConflict(
                        f"recovered workflow cannot replace durable proposal input: workflow_id={workflow_id}"
                    )
            elif previous_input != input_fingerprint:
                raise SurvivorProposalConflict(
                    f"workflow provider input conflict: workflow_id={workflow_id}; existing={previous_input}; incoming={input_fingerprint}"
                )
            selected = _selected_proposal(cached.proposals, proposal_id) if cached.proposals is not None else None
            if selected is not None and selected.proposal_id != cached.checkpoint.proposal_id:
                raise SurvivorProposalConflict(
                    f"workflow proposal selection conflict: workflow_id={workflow_id}; existing={cached.checkpoint.proposal_id}; incoming={selected.proposal_id}"
                )
            return cached
        proposals = self._proposal_pipeline.generate(source, provider_submission)
        selected = _selected_proposal(proposals, proposal_id)
        created = _checkpoint(source, SurvivorWorkflowState.CREATED, evidence_id=source.analysis.result_id)
        classified = _checkpoint(
            source,
            SurvivorWorkflowState.CLASSIFIED,
            previous=created,
            evidence_id=source.analysis.analysis_id,
            classification=source.analysis.classification.category.value,
        )
        if proposals.causal_context is None:
            raise SurvivorEvidenceError(f"proposal pipeline returned no causal context: workflow_id={workflow_id}")
        context_ready = _checkpoint(
            source,
            SurvivorWorkflowState.CONTEXT_READY,
            previous=classified,
            evidence_id=proposals.causal_context.context_id,
            causal_context_id=proposals.causal_context.context_id,
        )
        proposal_ready = _checkpoint(
            source,
            SurvivorWorkflowState.PROPOSAL_READY,
            previous=context_ready,
            evidence_id=selected.evidence_id,
            proposal_set_id=proposals.proposal_set_id,
            proposal_id=selected.proposal_id,
        )
        for checkpoint in (created, classified, context_ready, proposal_ready):
            self._ledger.commit_checkpoint(checkpoint)
        run = SurvivorWorkflowRun(proposal_ready, source, proposals=proposals)
        self._prepared_cache[workflow_id] = run
        self._prepare_inputs[workflow_id] = input_fingerprint
        return run
    def record_validation(
        self,
        run: SurvivorWorkflowRun,
        evidence: FreshProposalValidationEvidence,
    ) -> SurvivorWorkflowRun:
        # Record existing isolated fresh-run evidence without invoking provider, pytest, or mutation execution.
        _validate_checkpoint_source(run.checkpoint, run.source)
        if run.proposals is None:
            raise SurvivorEvidenceError(f"validation requires durable proposals: workflow_id={run.checkpoint.workflow_id}")
        if run.validation is not None:
            incoming = self._proposal_pipeline.validate_fresh(run.source, run.proposals, evidence)
            if incoming == run.validation:
                return run
            raise SurvivorProposalConflict(f"workflow already has different validation: workflow_id={run.checkpoint.workflow_id}")
        if run.checkpoint.state is not SurvivorWorkflowState.PROPOSAL_READY:
            raise SurvivorEvidenceError(f"validation requires proposal_ready workflow: state={run.checkpoint.state.value}")
        receipt = self._proposal_pipeline.validate_fresh(run.source, run.proposals, evidence)
        if receipt.status is not ProposalVerificationStatus.VERIFIED:
            failure = SurvivorWorkflowFailure(
                "validation_rejected",
                sanitize_text(",".join(receipt.reasons), 1200) or "validation rejected",
            )
            failed = _checkpoint(
                run.source,
                SurvivorWorkflowState.FAILED,
                previous=run.checkpoint,
                evidence_id=receipt.validation_id,
                validation_id=receipt.validation_id,
                failure=failure,
            )
            self._ledger.commit_checkpoint(failed)
            return replace(run, checkpoint=failed, validation=receipt)
        mutation_validated = _checkpoint(
            run.source,
            SurvivorWorkflowState.MUTATION_VALIDATED,
            previous=run.checkpoint,
            evidence_id=receipt.validation_id,
            validation_id=receipt.validation_id,
        )
        regression_validated = _checkpoint(
            run.source,
            SurvivorWorkflowState.REGRESSION_VALIDATED,
            previous=mutation_validated,
            evidence_id=receipt.validation_id,
        )
        awaiting = _checkpoint(
            run.source,
            SurvivorWorkflowState.AWAITING_HUMAN_REVIEW,
            previous=regression_validated,
            evidence_id=receipt.validation_id,
        )
        for checkpoint in (mutation_validated, regression_validated, awaiting):
            self._ledger.commit_checkpoint(checkpoint)
        return replace(run, checkpoint=awaiting, validation=receipt)
    def record_review(
        self,
        run: SurvivorWorkflowRun,
        evidence: SurvivorHumanReviewEvidence,
    ) -> SurvivorWorkflowRun:
        # Record an idempotent human approval or rejection after verified validation.
        _validate_checkpoint_source(run.checkpoint, run.source)
        if run.checkpoint.state is not SurvivorWorkflowState.AWAITING_HUMAN_REVIEW:
            if run.review is not None:
                incoming = _review_receipt(run, evidence)
                self._ledger.commit_review(incoming)
                if incoming == run.review:
                    return run
            raise SurvivorEvidenceError(f"human review requires awaiting_human_review state: state={run.checkpoint.state.value}")
        receipt = _review_receipt(run, evidence)
        self._ledger.commit_review(receipt)
        state = SurvivorWorkflowState.APPROVED if receipt.decision is SurvivorReviewDecision.APPROVE else SurvivorWorkflowState.REJECTED
        checkpoint = _checkpoint(
            run.source,
            state,
            previous=run.checkpoint,
            evidence_id=receipt.review_id,
            review_id=receipt.review_id,
        )
        self._ledger.commit_checkpoint(checkpoint)
        return replace(run, checkpoint=checkpoint, review=receipt)
    def export(
        self,
        run: SurvivorWorkflowRun,
        export_format: SurvivorExportFormat,
        *,
        destination: str | None = None,
    ) -> SurvivorWorkflowRun:
        # Build and optionally atomically publish a test-only export after human approval.
        _validate_checkpoint_source(run.checkpoint, run.source)
        if run.checkpoint.state is SurvivorWorkflowState.EXPORTED and run.export is not None:
            if run.export.export_format is not export_format:
                raise SurvivorProposalConflict(
                    f"workflow export format conflict: export_id={run.export.export_id}; existing={run.export.export_format.value}; incoming={export_format.value}"
                )
            if destination is not None:
                publish_survivor_export(run.export, destination)
            return run
        if run.checkpoint.state is not SurvivorWorkflowState.APPROVED:
            raise SurvivorEvidenceError(f"export requires approved workflow: state={run.checkpoint.state.value}")
        if run.proposals is None or run.validation is None or run.review is None:
            raise SurvivorEvidenceError("export requires proposal, validation, and review evidence")
        proposal = _selected_proposal(run.proposals, run.checkpoint.proposal_id)
        bundle = build_survivor_export(run.checkpoint, proposal, run.validation, run.review, export_format)
        self._ledger.commit_export(bundle)
        if destination is not None:
            publish_survivor_export(bundle, destination)
        checkpoint = _checkpoint(
            run.source,
            SurvivorWorkflowState.EXPORTED,
            previous=run.checkpoint,
            evidence_id=bundle.export_id,
            export_id=bundle.export_id,
        )
        self._ledger.commit_checkpoint(checkpoint)
        return replace(run, checkpoint=checkpoint, export=bundle)
    def restore(
        self,
        content: bytes,
        source: SurvivorAdapterResult,
        *,
        proposals: SurvivorProposalPipelineResult | None = None,
        validation: SurvivorProposalValidationReceipt | None = None,
        review: SurvivorHumanReviewReceipt | None = None,
        export: SurvivorExportBundle | None = None,
    ) -> SurvivorWorkflowRun:
        # Restore the last checkpoint and supplied immutable evidence without rerunning completed stages.
        checkpoint = checkpoint_from_bytes(content)
        _validate_checkpoint_source(checkpoint, source)
        if checkpoint.proposal_set_id is not None:
            if proposals is None or proposals.proposal_set_id != checkpoint.proposal_set_id:
                raise SurvivorEvidenceError("recovery requires the exact durable proposal result")
            _selected_proposal(proposals, checkpoint.proposal_id)
        if checkpoint.validation_id is not None:
            if validation is None or validation.validation_id != checkpoint.validation_id:
                raise SurvivorEvidenceError("recovery requires the exact durable validation receipt")
        if checkpoint.review_id is not None:
            if review is None or review.review_id != checkpoint.review_id:
                raise SurvivorEvidenceError("recovery requires the exact durable human review receipt")
        if checkpoint.export_id is not None:
            if export is None or export.export_id != checkpoint.export_id:
                raise SurvivorEvidenceError("recovery requires the exact durable export bundle")
        self._ledger.commit_checkpoint(checkpoint)
        run = SurvivorWorkflowRun(checkpoint, source, proposals, validation, review, export)
        if _STATE_ORDER[checkpoint.state] >= _STATE_ORDER[SurvivorWorkflowState.PROPOSAL_READY]:
            self._prepared_cache[checkpoint.workflow_id] = run
            self._prepare_inputs[checkpoint.workflow_id] = None
        return run

__all__ = [
    "SurvivorRepairWorkflow",
    "SurvivorWorkflowLedger",
    "checkpoint_bytes",
    "checkpoint_from_bytes",
]
