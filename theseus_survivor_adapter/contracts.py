"""Typed runtime boundary between authoritative Theseus evidence and Survivor Lab."""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping
from theseus_contracts.artifacts import ArtifactRegistryEntry
from theseus_contracts.engine import PreparedCampaignSnapshot
from theseus_contracts.mutation import MutantDescriptor, MutantExecutionResult
from theseus_contracts.project import ProjectDescriptor
from theseus_knowledge.evidence import KnowledgeEvidenceGraph
from theseus_survivor_lab.contracts import (
    DependencyEvidence,
    EnvironmentEvidence,
    SelectionEvidence,
    SourceEvidence,
    SurvivorAnalysisRequest,
    SurvivorAnalysisResult,
    SurvivorCausalContext,
    TestEvidence,
    TestProposal,
    ValidationPlan,
)
class SurvivorAdapterError(RuntimeError):
    """Base error for deterministic Survivor Lab runtime integration failures."""
class SurvivorEvidenceError(SurvivorAdapterError):
    """Raised when runtime evidence is missing, stale, or contradictory."""
class SurvivorProposalConflict(SurvivorAdapterError):
    """Raised when one proposal identity is replayed with different immutable content."""
class ProposalEvidenceStatus(StrEnum):
    """Disposition assigned by the offline PR39 validation boundary."""
    ACCEPTED = "accepted"
    REJECTED = "rejected"
class ProposalVerificationStatus(StrEnum):
    """Outcome of a separately supplied fresh isolated mutation validation."""
    VERIFIED = "verified"
    REJECTED = "rejected"

class SurvivorWorkflowState(StrEnum):
    """Durable states of one survivor repair workflow."""
    CREATED = "created"
    CLASSIFIED = "classified"
    CONTEXT_READY = "context_ready"
    PROPOSAL_READY = "proposal_ready"
    MUTATION_VALIDATED = "mutation_validated"
    REGRESSION_VALIDATED = "regression_validated"
    AWAITING_HUMAN_REVIEW = "awaiting_human_review"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPORTED = "exported"
    FAILED = "failed"
class SurvivorReviewDecision(StrEnum):
    """Explicit human disposition for one verified proposal."""
    APPROVE = "approve"
    REJECT = "reject"
class SurvivorExportFormat(StrEnum):
    """Deterministic export representations supported without Git integration."""
    UNIFIED_DIFF = "unified_diff"
    OVERLAY_ZIP = "overlay_zip"
    OVERLAY_DIRECTORY = "overlay_directory"
@dataclass(frozen=True, slots=True)
class SurvivorWorkflowTransition:
    """One content-addressed state transition in workflow order."""
    transition_id: str
    state: SurvivorWorkflowState
    evidence_id: str | None = None
@dataclass(frozen=True, slots=True)
class SurvivorWorkflowFailure:
    """Bounded typed workflow failure without a public traceback."""
    code: str
    message: str
@dataclass(frozen=True, slots=True)
class SurvivorHumanReviewEvidence:
    """Externally supplied human decision linked to verified proposal evidence."""
    review_id: str
    reviewer_id: str
    decision: SurvivorReviewDecision
    reviewed_at: str
    proposal_id: str
    validation_id: str
    note: str = ""
@dataclass(frozen=True, slots=True)
class SurvivorHumanReviewReceipt:
    """Canonical accepted human-review decision retained for replay."""
    review_id: str
    reviewer_id: str
    decision: SurvivorReviewDecision
    reviewed_at: str
    proposal_id: str
    validation_id: str
    payload_sha256: str
    note: str = ""
@dataclass(frozen=True, slots=True)
class SurvivorExportFile:
    """One project-relative test-only file in an export bundle."""
    path: str
    content_sha256: str
    content: bytes = field(repr=False)
@dataclass(frozen=True, slots=True)
class SurvivorExportBundle:
    """Deterministic test-only export with manifest, diff, and optional ZIP bytes."""
    export_id: str
    export_format: SurvivorExportFormat
    manifest_sha256: str
    manifest: Mapping[str, Any]
    files: tuple[SurvivorExportFile, ...]
    unified_diff: bytes = field(repr=False)
    archive: bytes | None = field(default=None, repr=False)
@dataclass(frozen=True, slots=True)
class SurvivorWorkflowCheckpoint:
    """Content-addressed durable checkpoint sufficient for fail-closed recovery."""
    checkpoint_id: str
    workflow_id: str
    state: SurvivorWorkflowState
    campaign_id: str
    project_id: str
    revision: str | None
    mutant_id: str
    source_execution_id: str
    source_analysis_id: str
    source_result_id: str
    source_event_id: str
    graph_fingerprint: str
    classification: str | None = None
    causal_context_id: str | None = None
    proposal_set_id: str | None = None
    proposal_id: str | None = None
    validation_id: str | None = None
    review_id: str | None = None
    export_id: str | None = None
    transitions: tuple[SurvivorWorkflowTransition, ...] = ()
    failure: SurvivorWorkflowFailure | None = None
@dataclass(frozen=True, slots=True)
class SurvivorWorkflowRun:
    """In-memory workflow view backed by a durable checkpoint artifact."""
    checkpoint: SurvivorWorkflowCheckpoint
    source: SurvivorAdapterResult
    proposals: SurvivorProposalPipelineResult | None = None
    validation: SurvivorProposalValidationReceipt | None = None
    review: SurvivorHumanReviewReceipt | None = None
    export: SurvivorExportBundle | None = None

@dataclass(frozen=True, slots=True)
class AuthoritativeSurvivorEvidence:
    """Complete committed runtime evidence required for one offline analysis."""
    campaign_id: str
    project: ProjectDescriptor
    prepared: PreparedCampaignSnapshot
    mutant: MutantDescriptor
    execution: MutantExecutionResult
    graph: KnowledgeEvidenceGraph
    source: SourceEvidence
    selection: SelectionEvidence = SelectionEvidence()
    related_tests: tuple[TestEvidence, ...] = ()
    related_dependencies: tuple[DependencyEvidence, ...] = ()
    environment: EnvironmentEvidence | None = None
    artifacts: tuple[ArtifactRegistryEntry, ...] = ()
    execution_level: str = "selected"
    exit_code: int | None = None
    timed_out: bool = False
    infrastructure_failure: bool = False
@dataclass(frozen=True, slots=True)
class SurvivorAnalysisArtifact:
    """Content-addressed artifact draft returned without touching the filesystem."""
    logical_key: str
    logical_role: str
    content_sha256: str
    size_bytes: int
    schema_version: int
    producer: str
    content: bytes = field(repr=False)
    metadata: Mapping[str, Any] = field(default_factory=dict)
@dataclass(frozen=True, slots=True)
class SurvivorAdapterResult:
    """Offline analysis plus immutable artifact bytes and provenance fencing."""
    request: SurvivorAnalysisRequest
    analysis: SurvivorAnalysisResult
    artifact: SurvivorAnalysisArtifact
    source_event_id: str
    graph_fingerprint: str
@dataclass(frozen=True, slots=True)
class SurvivorProviderSubmission:
    """Already-obtained untrusted provider response accepted without performing network I/O."""
    provider_name: str
    provider_version: str
    deterministic: bool
    response: object = field(repr=False)
@dataclass(frozen=True, slots=True)
class SurvivorProposalEvidence:
    """Immutable accepted or rejected proposal evidence retained for audit and replay."""
    evidence_id: str
    proposal_id: str | None
    hypothesis_id: str | None
    status: ProposalEvidenceStatus
    payload_sha256: str
    reasons: tuple[str, ...]
    source_analysis_id: str
    source_result_id: str
    source_event_id: str
    graph_fingerprint: str
    provider_name: str
    provider_version: str
    proposal: TestProposal | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)
@dataclass(frozen=True, slots=True)
class SurvivorProposalPipelineResult:
    """Validated proposal set and rejected evidence produced without applying or executing code."""
    source_analysis_id: str
    proposal_set_id: str
    accepted: tuple[SurvivorProposalEvidence, ...]
    rejected: tuple[SurvivorProposalEvidence, ...]
    validation_plan: ValidationPlan
    artifact: SurvivorAnalysisArtifact
    duplicate_evidence_ids: tuple[str, ...] = ()
    causal_context: SurvivorCausalContext | None = None
@dataclass(frozen=True, slots=True)
class FreshProposalValidationEvidence:
    """Already-obtained fresh isolated mutation-run evidence for one accepted proposal."""
    proposal_id: str
    project_id: str
    revision: str | None
    mutant_id: str
    source_execution_id: str
    validation_execution_id: str
    candidate_test_nodeid: str
    candidate_test_sha256: str
    fresh_run: bool
    isolated_workspace: bool
    original_passed: bool
    mutant_killed: bool
    regression_passed: bool
    stable: bool
    timed_out: bool
    infrastructure_failure: bool
    restore_verified: bool
@dataclass(frozen=True, slots=True)
class SurvivorProposalValidationReceipt:
    """Content-addressed verified or rejected result for fresh proposal validation."""
    validation_id: str
    proposal_id: str
    status: ProposalVerificationStatus
    reasons: tuple[str, ...]
    payload_sha256: str
    source_analysis_id: str
    source_result_id: str
    source_event_id: str
    graph_fingerprint: str
    project_id: str
    revision: str | None
    mutant_id: str
    source_execution_id: str
    validation_execution_id: str
    candidate_test_nodeid: str
    candidate_test_sha256: str
__all__ = [
    "AuthoritativeSurvivorEvidence",
    "FreshProposalValidationEvidence",
    "ProposalEvidenceStatus",
    "ProposalVerificationStatus",
    "SurvivorAdapterError",
    "SurvivorAdapterResult",
    "SurvivorAnalysisArtifact",
    "SurvivorEvidenceError",
    "SurvivorProposalConflict",
    "SurvivorProposalEvidence",
    "SurvivorProposalPipelineResult",
    "SurvivorProposalValidationReceipt",
    "SurvivorProviderSubmission",
    "SurvivorExportBundle",
    "SurvivorExportFile",
    "SurvivorExportFormat",
    "SurvivorHumanReviewEvidence",
    "SurvivorHumanReviewReceipt",
    "SurvivorReviewDecision",
    "SurvivorWorkflowCheckpoint",
    "SurvivorWorkflowFailure",
    "SurvivorWorkflowRun",
    "SurvivorWorkflowState",
    "SurvivorWorkflowTransition",
]
