"""Versioned, dependency-free contracts for Theseus Survivor Lab."""
from dataclasses import dataclass
from enum import StrEnum
SCHEMA_VERSION = 1
class MutantStatus(StrEnum):
    """Statuses accepted by the offline survivor-analysis input boundary."""
    SURVIVED = "survived"
    TIMEOUT = "timeout"
    ERROR = "error"
    INFRASTRUCTURE_ERROR = "infrastructure_error"
class ExecutionStatus(StrEnum):
    """Semantic status values emitted by a mutation execution adapter."""
    SURVIVED = "survived"
    KILLED = "killed"
    TIMEOUT = "timeout"
    ERROR = "error"
    INFRASTRUCTURE_ERROR = "infrastructure_error"
    CANCELLED = "cancelled"
class RequestedMode(StrEnum):
    """Explicit pieces of the offline pipeline requested by the caller."""
    CLASSIFY = "classify"
    CONTEXT = "context"
    HYPOTHESES = "hypotheses"
    PROPOSE = "propose"
    VALIDATION_PLAN = "validation_plan"
class SurvivorCategory(StrEnum):
    """Primary explanations that can account for a surviving mutant."""
    REAL_TEST_GAP = "real_test_gap"
    WEAK_ORACLE = "weak_oracle"
    SELECTION_ESCAPE = "selection_escape"
    EQUIVALENT_SUSPECTED = "equivalent_suspected"
    UNREACHABLE_CODE = "unreachable_code"
    ENVIRONMENT_DEPENDENT = "environment_dependent"
    TEST_DATA_GAP = "test_data_gap"
    ASSERTION_TOO_BROAD = "assertion_too_broad"
    INFRASTRUCTURE_AMBIGUITY = "infrastructure_ambiguity"
    TIMEOUT_AMBIGUITY = "timeout_ambiguity"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
class GenerationSource(StrEnum):
    """Origin of a candidate test proposal."""
    DETERMINISTIC_TEMPLATE = "deterministic_template"
    EXTERNAL_PROVIDER = "external_provider"
    MANUAL_ADAPTER = "manual_adapter"
@dataclass(frozen=True)
class ProviderMetadata:
    """Identity and invocation facts for the optional proposal provider."""
    name: str
    version: str
    deterministic: bool
    invoked: bool
    request_sha256: str | None
    response_sha256: str | None
@dataclass(frozen=True)
class MutantEvidence:
    """Describe the mutation and its source location."""
    mutant_id: str
    operator: str
    operator_version: str
    source_path: str
    function_id: str | None
    class_name: str | None
    line_no: int
    column_no: int
    original: str
    replacement: str
    diff: str | None
    status: MutantStatus
@dataclass(frozen=True)
class SourceEvidence:
    """Carry bounded source evidence without importing the analyzed project."""
    source_path: str
    source_sha256: str
    source_text: str
    function_source: str | None
    function_start_line: int | None
    function_end_line: int | None
@dataclass(frozen=True)
class SelectionEvidence:
    """Describe reachability, selection, oracle, and equivalence observations."""
    selected_tests: tuple[str, ...] = ()
    related_test_nodeids: tuple[str, ...] = ()
    similar_mutant_ids: tuple[str, ...] = ()
    runtime_location_reached: bool | None = None
    mutated_branch_reached: bool | None = None
    boundary_values_observed: bool | None = None
    truth_table_observed: bool | None = None
    oracle_observed: bool | None = None
    assertions_observed: bool | None = None
    equivalent_observation: bool | None = None
    reachable_paths_proven: bool = False
    static_reachability: str | None = None
    observed_effects_changed: bool | None = None
    selection_reasons: tuple[str, ...] = ()
@dataclass(frozen=True)
class ExecutionEvidence:
    """Record one selected/domain/full execution without raw process state."""
    execution_id: str
    level: str
    status: ExecutionStatus
    exit_code: int | None
    timed_out: bool
    infrastructure_failure: bool
    restore_verified: bool
    selected_tests: tuple[str, ...]
    observed_tests: tuple[str, ...]
    output_excerpt: str | None
@dataclass(frozen=True)
class TestEvidence:
    """Describe a related test and the observations available for it."""
    nodeid: str
    source_path: str | None
    source_sha256: str | None
    source_text: str | None
    selection_reasons: tuple[str, ...]
    executions: int
    failures: int
    median_duration_ms: float | None
    killed_related_mutants: tuple[str, ...]
@dataclass(frozen=True)
class DependencyEvidence:
    """Describe a dependency symbol without loading or executing it."""
    name: str
    kind: str
    source_path: str | None
    version: str | None
    relationship: str
@dataclass(frozen=True)
class EnvironmentEvidence:
    """Carry only non-secret environment descriptors relevant to analysis."""
    platform: str | None
    python_version: str | None
    markers: tuple[str, ...]
    differences: tuple[str, ...]
    stable: bool | None
@dataclass(frozen=True)
class SurvivorAnalysisRequest:
    """Top-level versioned input bundle for survivor analysis."""
    schema_version: int
    request_id: str
    project_id: str
    revision: str | None
    mutant: MutantEvidence
    source: SourceEvidence
    selection: SelectionEvidence
    executions: tuple[ExecutionEvidence, ...]
    related_tests: tuple[TestEvidence, ...]
    related_dependencies: tuple[DependencyEvidence, ...]
    environment: EnvironmentEvidence | None
    requested_modes: tuple[RequestedMode, ...]
@dataclass(frozen=True)
class TestFragment:
    """Bounded source fragment from a related test."""
    nodeid: str
    source_path: str | None
    excerpt: str
    referenced_names: tuple[str, ...]
@dataclass(frozen=True)
class CausalContext:
    """Bounded AST-derived context around a mutation."""
    function_source: str | None
    mutation_line: str
    lines_before: tuple[str, ...]
    lines_after: tuple[str, ...]
    referenced_names: tuple[str, ...]
    nearby_conditions: tuple[str, ...]
    return_expressions: tuple[str, ...]
    related_test_fragments: tuple[TestFragment, ...]
    warnings: tuple[str, ...] = ()
@dataclass(frozen=True)
class CausalDependencyEvidence:
    """One confirmed dependency edge included in bounded provider context."""
    name: str
    kind: str
    source_path: str | None
    version: str | None
    relationship: str

@dataclass(frozen=True)
class CausalExecutionEvidence:
    """One execution identity without raw output, environment values, or process state."""
    execution_id: str
    level: str
    status: ExecutionStatus
    restore_verified: bool
    selected_tests: tuple[str, ...]
    observed_tests: tuple[str, ...]

@dataclass(frozen=True)
class SurvivorCausalContext:
    """Minimal content-addressed context allowed to support survivor proposals."""
    context_id: str
    complete: bool
    project_id: str
    revision: str | None
    mutant_id: str
    execution_ids: tuple[str, ...]
    source_path: str
    source_sha256: str
    mutation_diff: str
    function_source: str | None
    mutation_line: str
    related_tests: tuple[TestFragment, ...]
    dependencies: tuple[CausalDependencyEvidence, ...]
    executions: tuple[CausalExecutionEvidence, ...]
    classification: SurvivorCategory
    classification_reason: str
    authoritative_contracts: tuple[str, ...]
    blockers: tuple[str, ...]
    uncertainty: tuple[str, ...]
    total_bytes: int

@dataclass(frozen=True)
class AnalysisFinding:
    """Explain one observed signal and the evidence supporting it."""
    finding_id: str
    kind: str
    title: str
    description: str
    evidence_ids: tuple[str, ...]
    severity: str
@dataclass(frozen=True)
class SurvivorClassification:
    """Authoritative local classification plus explainability metadata."""
    category: SurvivorCategory
    reason: str
    confidence: float
    evidence_ids: tuple[str, ...]
    secondary_categories: tuple[SurvivorCategory, ...]
@dataclass(frozen=True)
class RepairHypothesis:
    """Structured, non-authoritative hypothesis for strengthening tests."""
    hypothesis_id: str
    kind: str
    title: str
    explanation: str
    target_behavior: str
    suggested_inputs: tuple[str, ...]
    suggested_assertions: tuple[str, ...]
    related_tests: tuple[str, ...]
    confidence: float
    evidence_ids: tuple[str, ...]
@dataclass(frozen=True)
class TestProposal:
    """A structured candidate test proposal, not an applied patch."""
    proposal_id: str
    target_test_file: str | None
    target_test_nodeid: str | None
    proposed_test_name: str
    arrangement: tuple[str, ...]
    action: tuple[str, ...]
    assertions: tuple[str, ...]
    rationale: str
    expected_original_outcome: str
    expected_mutant_outcome: str
    imports_needed: tuple[str, ...]
    fixtures_needed: tuple[str, ...]
    generated_code: str | None
    generation_source: GenerationSource
@dataclass(frozen=True)
class ValidationStep:
    """One check in the offline validation plan."""
    step_id: str
    title: str
    command_hint: str
    purpose: str
@dataclass(frozen=True)
class ValidationPlan:
    """Ordered checks another orchestrator may execute later."""
    original_checks: tuple[ValidationStep, ...]
    mutant_checks: tuple[ValidationStep, ...]
    regression_checks: tuple[ValidationStep, ...]
    stability_checks: tuple[ValidationStep, ...]
@dataclass(frozen=True)
class SurvivorAnalysisResult:
    """Deterministic JSON/Markdown projection of a survivor analysis."""
    schema_version: int
    request_id: str
    analysis_id: str
    proposal_set_id: str
    result_id: str
    classification: SurvivorClassification
    confidence: float
    findings: tuple[AnalysisFinding, ...]
    hypotheses: tuple[RepairHypothesis, ...]
    proposals: tuple[TestProposal, ...]
    validation_plan: ValidationPlan
    provider: ProviderMetadata
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
