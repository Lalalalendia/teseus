"""Offline generation, validation, and append-only evidence receipts for survivor proposals."""
from __future__ import annotations
import ast
import hashlib
import re
from dataclasses import replace
from pathlib import PurePosixPath
from threading import RLock
from typing import Any, Mapping
from theseus_survivor_lab.context import (
    build_survivor_causal_context,
    causal_context_to_dict,
    extract_causal_context,
    relevant_tests,
)
from theseus_survivor_lab.contracts import RequestedMode, SurvivorCausalContext, TestProposal
from theseus_survivor_lab.errors import ContractError, SurvivorLabError
from theseus_survivor_lab.proposals import proposal_identity
from theseus_survivor_lab.providers import ProviderResponse, ProviderSuggestion, provider_response_from_dict, provider_response_to_dict
from theseus_survivor_lab.serialization import canonical_json, result_to_dict
from theseus_survivor_lab.service import SurvivorAnalysisService
from theseus_survivor_lab.validation import (
    MAX_PROVIDER_SUGGESTIONS,
    MAX_PROVIDER_WARNINGS,
    canonical_relative_path,
    sanitize_text,
    validate_request,
    validate_result,
)
from .contracts import (
    FreshProposalValidationEvidence,
    ProposalEvidenceStatus,
    ProposalVerificationStatus,
    SurvivorAdapterError,
    SurvivorAdapterResult,
    SurvivorAnalysisArtifact,
    SurvivorEvidenceError,
    SurvivorProposalConflict,
    SurvivorProposalEvidence,
    SurvivorProposalPipelineResult,
    SurvivorProposalValidationReceipt,
    SurvivorProviderSubmission,
)
_TEST_NAME = re.compile(r"^test_[A-Za-z0-9_]+$")
_FORBIDDEN_IMPORT_ROOTS = frozenset({"httpx", "requests", "socket", "subprocess", "urllib"})
_FORBIDDEN_CALL_NAMES = frozenset({"__import__", "compile", "eval", "exec", "open"})
_FORBIDDEN_CALL_PATHS = frozenset({
    "os.remove", "os.rename", "os.replace", "os.system", "os.unlink",
    "pathlib.Path.rename", "pathlib.Path.replace", "pathlib.Path.unlink", "pathlib.Path.write_bytes", "pathlib.Path.write_text",
    "shutil.copy", "shutil.copy2", "shutil.copyfile", "shutil.copytree", "shutil.move", "shutil.rmtree",
    "subprocess.call", "subprocess.check_call", "subprocess.check_output", "subprocess.Popen", "subprocess.run",
})
def _hash_bytes(value: bytes) -> str:
    # Hash one immutable proposal artifact or receipt payload.
    return hashlib.sha256(value).hexdigest()
def _stable_hash(value: Any) -> str:
    # Hash JSON-compatible proposal evidence with canonical UTF-8 JSON.
    try:
        return _hash_bytes(canonical_json(value).encode("utf-8"))
    except (TypeError, ValueError) as exc:
        raise SurvivorEvidenceError(f"proposal evidence is not JSON-compatible: value={value!r}; error={exc}") from exc
def _proposal_payload(proposal: TestProposal) -> dict[str, Any]:
    # Serialize one proposal independently from private Survivor Lab codecs.
    return {
        "proposal_id": proposal.proposal_id,
        "target_test_file": proposal.target_test_file,
        "target_test_nodeid": proposal.target_test_nodeid,
        "proposed_test_name": proposal.proposed_test_name,
        "arrangement": list(proposal.arrangement),
        "action": list(proposal.action),
        "assertions": list(proposal.assertions),
        "rationale": proposal.rationale,
        "expected_original_outcome": proposal.expected_original_outcome,
        "expected_mutant_outcome": proposal.expected_mutant_outcome,
        "imports_needed": list(proposal.imports_needed),
        "fixtures_needed": list(proposal.fixtures_needed),
        "generated_code": proposal.generated_code,
        "generation_source": proposal.generation_source.value,
    }
def _evidence_payload(value: SurvivorProposalEvidence) -> dict[str, Any]:
    # Serialize one accepted or rejected receipt for stable artifact and replay identity.
    return {
        "evidence_id": value.evidence_id,
        "proposal_id": value.proposal_id,
        "hypothesis_id": value.hypothesis_id,
        "status": value.status.value,
        "payload_sha256": value.payload_sha256,
        "reasons": list(value.reasons),
        "source_analysis_id": value.source_analysis_id,
        "source_result_id": value.source_result_id,
        "source_event_id": value.source_event_id,
        "graph_fingerprint": value.graph_fingerprint,
        "provider_name": value.provider_name,
        "provider_version": value.provider_version,
        "proposal": _proposal_payload(value.proposal) if value.proposal is not None else None,
        "payload": dict(value.payload),
    }
def _receipt_hash(value: SurvivorProposalEvidence) -> str:
    # Hash the full immutable receipt rather than only provider-controlled proposal text.
    return _stable_hash(_evidence_payload(value))
def _validate_adapter_result(source: SurvivorAdapterResult) -> None:
    # Re-authenticate the PR38 request, result, content-addressed artifact, and no-proposal boundary.
    if not isinstance(source, SurvivorAdapterResult):
        raise SurvivorEvidenceError("proposal pipeline requires a SurvivorAdapterResult")
    try:
        validate_request(source.request)
        validate_result(source.analysis)
    except ContractError as exc:
        raise SurvivorEvidenceError(f"source SurvivorAdapterResult violates Survivor Lab contracts: {exc}") from exc
    if source.analysis.request_id != source.request.request_id:
        raise SurvivorEvidenceError(
            f"source request/result identity conflict: request_id={source.request.request_id}; result_request_id={source.analysis.request_id}"
        )
    if source.analysis.proposals or source.analysis.provider.invoked:
        raise SurvivorEvidenceError(
            f"source result is not a PR38 analysis: result_id={source.analysis.result_id}; proposals={len(source.analysis.proposals)}; provider={source.analysis.provider}"
        )
    plan = source.analysis.validation_plan
    if any((plan.original_checks, plan.mutant_checks, plan.regression_checks, plan.stability_checks)):
        raise SurvivorEvidenceError(f"source PR38 result unexpectedly contains a validation plan: result_id={source.analysis.result_id}")
    expected_content = (canonical_json(result_to_dict(source.analysis)) + "\n").encode("utf-8")
    actual_hash = _hash_bytes(source.artifact.content)
    if source.artifact.content != expected_content or source.artifact.content_sha256 != actual_hash or source.artifact.size_bytes != len(source.artifact.content):
        raise SurvivorEvidenceError(
            "source analysis artifact identity conflict: "
            f"result_id={source.analysis.result_id}; expected_sha256={_hash_bytes(expected_content)}; "
            f"declared_sha256={source.artifact.content_sha256}; actual_sha256={actual_hash}; "
            f"declared_size={source.artifact.size_bytes}; actual_size={len(source.artifact.content)}"
        )
    if len(source.request.executions) != 1:
        raise SurvivorEvidenceError(
            f"source request must contain exactly one authoritative execution: actual={len(source.request.executions)}"
        )
    execution = source.request.executions[0]
    metadata = source.artifact.metadata
    expected_metadata = {
        "source_event_id": source.source_event_id,
        "graph_fingerprint": source.graph_fingerprint,
        "analysis_id": source.analysis.analysis_id,
        "result_id": source.analysis.result_id,
        "project_id": source.request.project_id,
        "mutant_id": source.request.mutant.mutant_id,
        "execution_id": execution.execution_id,
        "revision_id": source.request.revision or "",
    }
    conflicts = tuple(sorted(key for key, expected in expected_metadata.items() if str(metadata.get(key, "")) != expected))
    if conflicts:
        raise SurvivorEvidenceError(
            f"source analysis artifact metadata conflict: result_id={source.analysis.result_id}; fields={conflicts}; metadata={dict(metadata)}"
        )
    replay = SurvivorAnalysisService().analyze(source.request)
    if replay != source.analysis:
        raise SurvivorEvidenceError(
            f"source analysis is not an exact deterministic PR38 replay: expected_result_id={replay.result_id}; actual_result_id={source.analysis.result_id}"
        )
def _causal_context(source: SurvivorAdapterResult) -> SurvivorCausalContext:
    # Rebuild the minimal causal context from the exact authenticated PR38 request and result.
    syntax = extract_causal_context(
        source.request.source,
        source.request.mutant.line_no,
        relevant_tests(source.request),
    )
    return build_survivor_causal_context(
        source.request,
        syntax,
        source.analysis.classification,
        source.analysis.blockers,
        source.analysis.warnings,
    )
def _suggestion_payload(value: object) -> dict[str, Any]:
    # Convert untrusted provider data to a bounded redacted audit payload without executing it.
    if isinstance(value, ProviderSuggestion):
        return {
            "hypothesis_kind": sanitize_text(value.hypothesis_kind, 200),
            "title": sanitize_text(value.title, 500),
            "arrangement": [sanitize_text(item, 500) for item in value.arrangement[:12]] if isinstance(value.arrangement, tuple) else [],
            "action": [sanitize_text(item, 800) for item in value.action[:12]] if isinstance(value.action, tuple) else [],
            "assertions": [sanitize_text(item, 800) for item in value.assertions[:12]] if isinstance(value.assertions, tuple) else [],
            "rationale": sanitize_text(value.rationale, 1200),
            "generated_code": sanitize_text(value.generated_code, 3000),
        }
    return {"type": f"{type(value).__module__}.{type(value).__qualname__}", "value": sanitize_text(repr(value), 1200)}
def _rejected_evidence(
    source: SurvivorAdapterResult,
    *,
    provider_name: str,
    provider_version: str,
    reason: str,
    payload: Mapping[str, Any],
    proposal_id: str | None = None,
    hypothesis_id: str | None = None,
    index: int = 0,
) -> SurvivorProposalEvidence:
    # Preserve one invalid provider or proposal payload as deterministic rejected evidence.
    normalized = dict(payload)
    payload_sha256 = _stable_hash(normalized)
    evidence_id = "proposal-evidence-" + _stable_hash({
        "source_analysis_id": source.analysis.analysis_id,
        "proposal_id": proposal_id,
        "hypothesis_id": hypothesis_id,
        "provider_name": provider_name,
        "provider_version": provider_version,
        "reason": reason,
        "payload_sha256": payload_sha256,
        "index": index,
    })[:24]
    return SurvivorProposalEvidence(
        evidence_id=evidence_id,
        proposal_id=proposal_id,
        hypothesis_id=hypothesis_id,
        status=ProposalEvidenceStatus.REJECTED,
        payload_sha256=payload_sha256,
        reasons=(reason,),
        source_analysis_id=source.analysis.analysis_id,
        source_result_id=source.analysis.result_id,
        source_event_id=source.source_event_id,
        graph_fingerprint=source.graph_fingerprint,
        provider_name=provider_name,
        provider_version=provider_version,
        payload=normalized,
    )
def _validate_provider_suggestion(value: object) -> ProviderSuggestion:
    # Validate and sanitize one untrusted suggestion before proposal generation.
    if not isinstance(value, ProviderSuggestion):
        raise ValueError("provider suggestion is not a ProviderSuggestion")
    if not isinstance(value.hypothesis_kind, str) or not value.hypothesis_kind.strip():
        raise ValueError("provider suggestion hypothesis_kind must be non-empty text")
    if not isinstance(value.title, str) or not isinstance(value.rationale, str):
        raise ValueError("provider suggestion title and rationale must be text")
    for field in ("arrangement", "action", "assertions"):
        items = getattr(value, field)
        if not isinstance(items, tuple) or len(items) > 12 or any(not isinstance(item, str) for item in items):
            raise ValueError(f"provider suggestion {field} must be a bounded tuple of strings")
    if value.generated_code is not None and not isinstance(value.generated_code, str):
        raise ValueError("provider suggestion generated_code must be text or null")
    return ProviderSuggestion(
        hypothesis_kind=sanitize_text(value.hypothesis_kind, 200) or "",
        title=sanitize_text(value.title, 500) or "",
        arrangement=tuple(sanitize_text(item, 500) or "" for item in value.arrangement),
        action=tuple(sanitize_text(item, 800) or "" for item in value.action),
        assertions=tuple(sanitize_text(item, 800) or "" for item in value.assertions),
        rationale=sanitize_text(value.rationale, 1200) or "",
        generated_code=sanitize_text(value.generated_code, 3000),
    )
def _provider_boundary(
    source: SurvivorAdapterResult,
    submission: SurvivorProviderSubmission | None,
) -> tuple[str, str, bool, tuple[ProviderSuggestion, ...], tuple[SurvivorProposalEvidence, ...], tuple[str, ...]]:
    # Validate an already-obtained provider response and retain every discarded value as rejected evidence.
    if submission is None:
        return "null", "1", True, (), (), ()
    provider_name = sanitize_text(submission.provider_name, 200) or ""
    provider_version = sanitize_text(submission.provider_version, 100) or ""
    if not provider_name or not provider_version or not isinstance(submission.deterministic, bool):
        raise SurvivorEvidenceError(
            f"provider submission identity is invalid: name={submission.provider_name!r}; version={submission.provider_version!r}; deterministic={submission.deterministic!r}"
        )
    response = submission.response
    try:
        if isinstance(response, ProviderResponse):
            response = provider_response_from_dict(provider_response_to_dict(response))
        elif isinstance(response, Mapping):
            response = provider_response_from_dict(dict(response))
        else:
            raise ValueError("provider response is neither typed nor serialized")
    except (ContractError, ValueError, TypeError) as exc:
        rejected = _rejected_evidence(
            source,
            provider_name=provider_name,
            provider_version=provider_version,
            reason="provider_response_schema_invalid",
            payload={**_suggestion_payload(response), "error": sanitize_text(str(exc), 800)},
        )
        return provider_name, provider_version, submission.deterministic, (), (rejected,), ("provider_response_schema_invalid",)
    rejected_rows: list[SurvivorProposalEvidence] = []
    warnings: list[str] = []
    if not isinstance(response.suggestions, tuple) or len(response.suggestions) > MAX_PROVIDER_SUGGESTIONS:
        rejected = _rejected_evidence(
            source,
            provider_name=provider_name,
            provider_version=provider_version,
            reason="provider_suggestions_unbounded",
            payload={"type": type(response.suggestions).__name__, "count": len(response.suggestions) if hasattr(response.suggestions, "__len__") else None},
        )
        return provider_name, provider_version, submission.deterministic, (), (rejected,), ("provider_suggestions_unbounded",)
    if not isinstance(response.warnings, tuple) or len(response.warnings) > MAX_PROVIDER_WARNINGS or any(not isinstance(item, str) for item in response.warnings):
        warnings.append("provider_warnings_invalid")
    else:
        warnings.extend(sanitize_text(item, 800) or "" for item in response.warnings)
    hypotheses = {item.kind: item for item in source.analysis.hypotheses}
    accepted: list[ProviderSuggestion] = []
    seen: set[str] = set()
    for index, raw in enumerate(response.suggestions):
        payload = _suggestion_payload(raw)
        try:
            suggestion = _validate_provider_suggestion(raw)
        except ValueError as exc:
            rejected_rows.append(_rejected_evidence(
                source,
                provider_name=provider_name,
                provider_version=provider_version,
                reason=f"provider_suggestion_schema_invalid:{exc}",
                payload=payload,
                index=index,
            ))
            continue
        hypothesis = hypotheses.get(suggestion.hypothesis_kind)
        if hypothesis is None:
            rejected_rows.append(_rejected_evidence(
                source,
                provider_name=provider_name,
                provider_version=provider_version,
                reason="provider_suggestion_unknown_hypothesis_kind",
                payload=payload,
                index=index,
            ))
            continue
        expected_id = proposal_identity(source.request, hypothesis)
        if suggestion.hypothesis_kind in seen:
            rejected_rows.append(_rejected_evidence(
                source,
                provider_name=provider_name,
                provider_version=provider_version,
                reason="provider_suggestion_duplicate_hypothesis_kind",
                payload=payload,
                proposal_id=expected_id,
                hypothesis_id=hypothesis.hypothesis_id,
                index=index,
            ))
            continue
        seen.add(suggestion.hypothesis_kind)
        accepted.append(suggestion)
    return (
        provider_name,
        provider_version,
        submission.deterministic,
        tuple(accepted),
        tuple(rejected_rows),
        tuple(sorted(set(item for item in warnings if item))),
    )
class _StaticResponseProvider:
    """In-memory provider facade that cannot perform network or filesystem I/O."""
    def __init__(self, name: str, version: str, deterministic: bool, response: ProviderResponse) -> None:
        # Store an already-validated response and explicit provider identity.
        self.provider_name = name
        self.provider_version = version
        self.deterministic = deterministic
        self._response = response
    def propose(self, request: object) -> ProviderResponse:
        # Return the prevalidated immutable response without invoking an external provider.
        del request
        return self._response
def _attribute_path(node: ast.AST) -> str | None:
    # Resolve a dotted name used by static side-effect checks.
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _attribute_path(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    return None
def _validate_generated_code(code: str, proposal: TestProposal) -> tuple[str, ...]:
    # Parse candidate code and reject top-level execution, process/network access, writes, and physical paths.
    reasons: list[str] = []
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        return (f"generated_code_syntax_error:{exc.msg}",)
    allowed_top_level = (ast.Import, ast.ImportFrom, ast.FunctionDef, ast.AsyncFunctionDef)
    if any(not isinstance(node, allowed_top_level) for node in tree.body):
        reasons.append("generated_code_has_top_level_execution")
    functions = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    if len(functions) != 1 or functions[0].name != proposal.proposed_test_name:
        reasons.append("generated_code_test_function_identity_mismatch")
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(alias.name.split(".", 1)[0] in _FORBIDDEN_IMPORT_ROOTS for alias in node.names):
                reasons.append("generated_code_imports_process_or_network_module")
        elif isinstance(node, ast.ImportFrom):
            if (node.module or "").split(".", 1)[0] in _FORBIDDEN_IMPORT_ROOTS:
                reasons.append("generated_code_imports_process_or_network_module")
        elif isinstance(node, ast.Call):
            path = _attribute_path(node.func)
            leaf = path.rsplit(".", 1)[-1] if path else ""
            if (
                path in _FORBIDDEN_CALL_NAMES
                or path in _FORBIDDEN_CALL_PATHS
                or leaf in {"Popen", "remove", "rename", "replace", "rmtree", "run", "system", "unlink", "write_bytes", "write_text"}
            ):
                reasons.append(f"generated_code_forbidden_call:{path or leaf}")
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            text = node.value.replace("\\", "/")
            if text.startswith("/") or re.match(r"^[A-Za-z]:/", text):
                reasons.append("generated_code_contains_absolute_path")
    return tuple(sorted(set(reasons)))
def _validate_proposal(
    source: SurvivorAdapterResult,
    proposal: TestProposal,
    hypothesis: Any,
) -> tuple[str, ...]:
    # Validate proposal schema, target test identity, static source safety, and causal anchoring.
    reasons: list[str] = []
    expected_id = proposal_identity(source.request, hypothesis)
    if proposal.proposal_id != expected_id:
        reasons.append("proposal_identity_mismatch")
    if not _TEST_NAME.fullmatch(proposal.proposed_test_name):
        reasons.append("proposed_test_name_invalid")
    related_by_nodeid = {item.nodeid: item for item in source.request.related_tests}
    related_paths = {
        canonical_relative_path(item.source_path)
        for item in source.request.related_tests
        if item.source_path is not None
    }
    try:
        target_path = canonical_relative_path(proposal.target_test_file)
    except ContractError:
        target_path = None
        reasons.append("target_test_file_invalid")
    if target_path is None or target_path not in related_paths:
        reasons.append("target_test_file_not_authoritative_related_test")
    elif not (target_path.startswith("tests/") or PurePosixPath(target_path).name.startswith("test_")):
        reasons.append("target_test_file_is_not_a_test_module")
    if proposal.target_test_nodeid not in related_by_nodeid:
        reasons.append("target_test_nodeid_not_authoritative_related_test")
    if hypothesis.related_tests and proposal.target_test_nodeid not in set(hypothesis.related_tests):
        reasons.append("proposal_target_not_linked_to_hypothesis")
    if not proposal.arrangement or not proposal.action or not proposal.assertions:
        reasons.append("proposal_behavior_sections_incomplete")
    causal_anchors = (
        set(proposal.arrangement) & set(hypothesis.suggested_inputs),
        set(proposal.assertions) & set(hypothesis.suggested_assertions),
        {hypothesis.target_behavior} & set(proposal.action),
    )
    if not any(causal_anchors):
        reasons.append("proposal_has_no_local_causal_anchor")
    if proposal.generated_code is not None:
        reasons.extend(_validate_generated_code(proposal.generated_code, proposal))
    return tuple(sorted(set(reasons)))
def _accepted_evidence(
    source: SurvivorAdapterResult,
    proposal: TestProposal,
    hypothesis: Any,
    provider_name: str,
    provider_version: str,
) -> SurvivorProposalEvidence:
    # Wrap one validated proposal in an immutable accepted evidence receipt.
    payload = _proposal_payload(proposal)
    payload_sha256 = _stable_hash(payload)
    evidence_id = "proposal-evidence-" + _stable_hash({
        "source_analysis_id": source.analysis.analysis_id,
        "proposal_id": proposal.proposal_id,
        "payload_sha256": payload_sha256,
        "status": ProposalEvidenceStatus.ACCEPTED.value,
    })[:24]
    return SurvivorProposalEvidence(
        evidence_id=evidence_id,
        proposal_id=proposal.proposal_id,
        hypothesis_id=hypothesis.hypothesis_id,
        status=ProposalEvidenceStatus.ACCEPTED,
        payload_sha256=payload_sha256,
        reasons=(),
        source_analysis_id=source.analysis.analysis_id,
        source_result_id=source.analysis.result_id,
        source_event_id=source.source_event_id,
        graph_fingerprint=source.graph_fingerprint,
        provider_name=provider_name,
        provider_version=provider_version,
        proposal=proposal,
        payload=payload,
    )
def _fresh_validation_payload(evidence: FreshProposalValidationEvidence) -> dict[str, Any]:
    # Serialize externally obtained validation evidence without executing or opening candidate files.
    return {
        "proposal_id": evidence.proposal_id,
        "project_id": evidence.project_id,
        "revision": evidence.revision,
        "mutant_id": evidence.mutant_id,
        "source_execution_id": evidence.source_execution_id,
        "validation_execution_id": evidence.validation_execution_id,
        "candidate_test_nodeid": evidence.candidate_test_nodeid,
        "candidate_test_sha256": evidence.candidate_test_sha256.lower() if isinstance(evidence.candidate_test_sha256, str) else "",
        "fresh_run": evidence.fresh_run,
        "isolated_workspace": evidence.isolated_workspace,
        "original_passed": evidence.original_passed,
        "mutant_killed": evidence.mutant_killed,
        "regression_passed": evidence.regression_passed,
        "stable": evidence.stable,
        "timed_out": evidence.timed_out,
        "infrastructure_failure": evidence.infrastructure_failure,
        "restore_verified": evidence.restore_verified,
    }
def _validation_receipt_payload(receipt: SurvivorProposalValidationReceipt) -> dict[str, Any]:
    # Serialize one validation receipt for append-only replay and conflict checks.
    return {
        "validation_id": receipt.validation_id,
        "proposal_id": receipt.proposal_id,
        "status": receipt.status.value,
        "reasons": list(receipt.reasons),
        "payload_sha256": receipt.payload_sha256,
        "source_analysis_id": receipt.source_analysis_id,
        "source_result_id": receipt.source_result_id,
        "source_event_id": receipt.source_event_id,
        "graph_fingerprint": receipt.graph_fingerprint,
        "project_id": receipt.project_id,
        "revision": receipt.revision,
        "mutant_id": receipt.mutant_id,
        "source_execution_id": receipt.source_execution_id,
        "validation_execution_id": receipt.validation_execution_id,
        "candidate_test_nodeid": receipt.candidate_test_nodeid,
        "candidate_test_sha256": receipt.candidate_test_sha256,
    }
def _fresh_validation_receipt(
    source: SurvivorAdapterResult,
    proposals: SurvivorProposalPipelineResult,
    evidence: FreshProposalValidationEvidence,
) -> SurvivorProposalValidationReceipt:
    # Validate fresh isolated mutation evidence against the exact accepted proposal and source identities.
    if proposals.source_analysis_id != source.analysis.analysis_id:
        raise SurvivorEvidenceError(
            "proposal result belongs to another analysis: "
            f"expected={source.analysis.analysis_id}; actual={proposals.source_analysis_id}"
        )
    accepted = {item.proposal_id: item for item in proposals.accepted if item.proposal_id is not None}
    accepted_evidence = accepted.get(evidence.proposal_id)
    if accepted_evidence is None or accepted_evidence.proposal is None:
        raise SurvivorEvidenceError(
            f"fresh validation requires an accepted proposal: proposal_id={evidence.proposal_id}; accepted={tuple(sorted(accepted))}"
        )
    execution = source.request.executions[0]
    reasons: list[str] = []
    expected = {
        "project_id": source.request.project_id,
        "revision": source.request.revision,
        "mutant_id": source.request.mutant.mutant_id,
        "source_execution_id": execution.execution_id,
        "candidate_test_nodeid": accepted_evidence.proposal.target_test_nodeid,
    }
    actual = {
        "project_id": evidence.project_id,
        "revision": evidence.revision,
        "mutant_id": evidence.mutant_id,
        "source_execution_id": evidence.source_execution_id,
        "candidate_test_nodeid": evidence.candidate_test_nodeid,
    }
    for field, expected_value in expected.items():
        if actual[field] != expected_value:
            reasons.append(f"stale_identity:{field}")
    digest = evidence.candidate_test_sha256.lower() if isinstance(evidence.candidate_test_sha256, str) else ""
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        reasons.append("candidate_test_sha256_invalid")
    for field in (
        "fresh_run",
        "isolated_workspace",
        "original_passed",
        "mutant_killed",
        "regression_passed",
        "stable",
        "timed_out",
        "infrastructure_failure",
        "restore_verified",
    ):
        if not isinstance(getattr(evidence, field), bool):
            reasons.append(f"validation_field_not_boolean:{field}")
    if not evidence.validation_execution_id.strip() or evidence.validation_execution_id == evidence.source_execution_id:
        reasons.append("validation_execution_not_fresh")
    if evidence.fresh_run is not True:
        reasons.append("validation_not_fresh")
    if evidence.isolated_workspace is not True:
        reasons.append("validation_not_isolated")
    if evidence.timed_out is True:
        reasons.append("validation_timed_out")
    if evidence.infrastructure_failure is True:
        reasons.append("validation_infrastructure_failure")
    if evidence.restore_verified is not True:
        reasons.append("validation_restore_unverified")
    if evidence.original_passed is not True:
        reasons.append("candidate_failed_on_original")
    if evidence.mutant_killed is not True:
        reasons.append("candidate_did_not_kill_mutant")
    if evidence.regression_passed is not True:
        reasons.append("regression_suite_failed")
    if evidence.stable is not True:
        reasons.append("candidate_is_unstable")
    payload = _fresh_validation_payload(evidence)
    payload_sha256 = _stable_hash(payload)
    validation_id = "proposal-validation-" + _stable_hash({
        "source_analysis_id": source.analysis.analysis_id,
        "proposal_id": evidence.proposal_id,
        "validation_execution_id": evidence.validation_execution_id,
    })[:24]
    return SurvivorProposalValidationReceipt(
        validation_id=validation_id,
        proposal_id=evidence.proposal_id,
        status=ProposalVerificationStatus.VERIFIED if not reasons else ProposalVerificationStatus.REJECTED,
        reasons=tuple(sorted(set(reasons))),
        payload_sha256=payload_sha256,
        source_analysis_id=source.analysis.analysis_id,
        source_result_id=source.analysis.result_id,
        source_event_id=source.source_event_id,
        graph_fingerprint=source.graph_fingerprint,
        project_id=evidence.project_id,
        revision=evidence.revision,
        mutant_id=evidence.mutant_id,
        source_execution_id=evidence.source_execution_id,
        validation_execution_id=evidence.validation_execution_id,
        candidate_test_nodeid=evidence.candidate_test_nodeid,
        candidate_test_sha256=digest,
    )
class SurvivorProposalLedger:
    """Thread-safe append-only proposal identity registry for exact replay and conflict fencing."""
    def __init__(self) -> None:
        # Keep evidence and accepted proposal identities separate so rejected attempts remain auditable.
        self._evidence: dict[str, str] = {}
        self._proposals: dict[str, str] = {}
        self._validations: dict[str, str] = {}
        self._lock = RLock()
    def commit(self, evidence: SurvivorProposalEvidence) -> bool:
        # Append one receipt, return duplicate replay, or reject conflicting accepted proposal content.
        receipt_sha256 = _receipt_hash(evidence)
        with self._lock:
            existing_receipt = self._evidence.get(evidence.evidence_id)
            if existing_receipt is not None:
                if existing_receipt != receipt_sha256:
                    raise SurvivorProposalConflict(
                        f"proposal evidence identity conflict: evidence_id={evidence.evidence_id}; expected_sha256={existing_receipt}; actual_sha256={receipt_sha256}"
                    )
                return True
            if evidence.status is ProposalEvidenceStatus.ACCEPTED and evidence.proposal_id is not None:
                existing_payload = self._proposals.get(evidence.proposal_id)
                if existing_payload is not None and existing_payload != evidence.payload_sha256:
                    raise SurvivorProposalConflict(
                        "proposal identity conflict: "
                        f"proposal_id={evidence.proposal_id}; existing_sha256={existing_payload}; incoming_sha256={evidence.payload_sha256}; "
                        f"source_analysis_id={evidence.source_analysis_id}"
                    )
                self._proposals[evidence.proposal_id] = evidence.payload_sha256
            self._evidence[evidence.evidence_id] = receipt_sha256
            return False
    def commit_validation(self, receipt: SurvivorProposalValidationReceipt) -> bool:
        # Append one fresh-validation receipt or reject the same identity with different content.
        receipt_sha256 = _stable_hash(_validation_receipt_payload(receipt))
        with self._lock:
            existing = self._validations.get(receipt.validation_id)
            if existing is not None:
                if existing != receipt_sha256:
                    raise SurvivorProposalConflict(
                        "proposal validation identity conflict: "
                        f"validation_id={receipt.validation_id}; expected_sha256={existing}; actual_sha256={receipt_sha256}"
                    )
                return True
            self._validations[receipt.validation_id] = receipt_sha256
            return False
class SurvivorProposalPipeline:
    """Generate and statically validate proposals without applying files, executing tests, or calling a network provider."""
    def __init__(self, ledger: SurvivorProposalLedger | None = None) -> None:
        # Use an explicit append-only ledger or a private in-memory registry for replay fencing.
        self._ledger = ledger if ledger is not None else SurvivorProposalLedger()
    def validate_fresh(
        self,
        source: SurvivorAdapterResult,
        proposals: SurvivorProposalPipelineResult,
        evidence: FreshProposalValidationEvidence,
    ) -> SurvivorProposalValidationReceipt:
        # Authenticate externally obtained fresh-run evidence without executing or applying the proposal.
        _validate_adapter_result(source)
        receipt = _fresh_validation_receipt(source, proposals, evidence)
        self._ledger.commit_validation(receipt)
        return receipt
    def generate(
        self,
        source: SurvivorAdapterResult,
        provider_submission: SurvivorProviderSubmission | None = None,
    ) -> SurvivorProposalPipelineResult:
        # Build proposals only from an exact validated PR38 result and retain every rejected payload.
        _validate_adapter_result(source)
        causal_context = _causal_context(source)
        if provider_submission is not None and not causal_context.complete:
            provider_name = sanitize_text(provider_submission.provider_name, 200) or "invalid-provider"
            provider_version = sanitize_text(provider_submission.provider_version, 100) or "unknown"
            provider_deterministic = provider_submission.deterministic is True
            suggestions = ()
            rejected = (
                _rejected_evidence(
                    source,
                    provider_name=provider_name,
                    provider_version=provider_version,
                    reason="provider_blocked_by_incomplete_causal_context",
                    payload={
                        "context_id": causal_context.context_id,
                        "blockers": list(causal_context.blockers),
                    },
                ),
            )
            provider_warnings = ("provider_blocked_by_incomplete_causal_context",)
        else:
            provider_name, provider_version, provider_deterministic, suggestions, rejected, provider_warnings = _provider_boundary(
                source,
                provider_submission,
            )
        request = replace(
            source.request,
            requested_modes=(
                RequestedMode.CLASSIFY,
                RequestedMode.CONTEXT,
                RequestedMode.HYPOTHESES,
                RequestedMode.PROPOSE,
                RequestedMode.VALIDATION_PLAN,
            ),
        )
        provider = _StaticResponseProvider(
            provider_name,
            provider_version,
            provider_deterministic,
            ProviderResponse(suggestions=suggestions, warnings=provider_warnings),
        )
        try:
            generated = SurvivorAnalysisService(provider).analyze(request)
            validate_result(generated)
        except SurvivorLabError as exc:
            raise SurvivorAdapterError(
                f"Survivor Lab proposal generation failed: source_result_id={source.analysis.result_id}; error={exc}"
            ) from exc
        if (
            generated.classification != source.analysis.classification
            or generated.findings != source.analysis.findings
            or generated.hypotheses != source.analysis.hypotheses
        ):
            raise SurvivorEvidenceError(
                "proposal mode changed authoritative local analysis: "
                f"source_analysis_id={source.analysis.analysis_id}; generated_analysis_id={generated.analysis_id}"
            )
        hypotheses = {item.kind: item for item in source.analysis.hypotheses}
        accepted_rows: list[SurvivorProposalEvidence] = []
        rejected_rows = list(rejected)
        for index, proposal in enumerate(generated.proposals):
            hypothesis = hypotheses.get(source.analysis.hypotheses[index].kind) if index < len(source.analysis.hypotheses) else None
            if hypothesis is None:
                rejected_rows.append(_rejected_evidence(
                    source,
                    provider_name=provider_name,
                    provider_version=provider_version,
                    reason="proposal_has_no_matching_hypothesis",
                    payload=_proposal_payload(proposal),
                    proposal_id=proposal.proposal_id,
                    index=index,
                ))
                continue
            context_reasons = (
                tuple(f"causal_context_incomplete:{item}" for item in causal_context.blockers)
                if not causal_context.complete
                else ()
            )
            reasons = tuple(sorted(set((*context_reasons, *_validate_proposal(source, proposal, hypothesis)))))
            if reasons:
                payload = _proposal_payload(proposal)
                payload_sha256 = _stable_hash(payload)
                evidence_id = "proposal-evidence-" + _stable_hash({
                    "source_analysis_id": source.analysis.analysis_id,
                    "proposal_id": proposal.proposal_id,
                    "payload_sha256": payload_sha256,
                    "reasons": reasons,
                    "status": ProposalEvidenceStatus.REJECTED.value,
                })[:24]
                rejected_rows.append(SurvivorProposalEvidence(
                    evidence_id=evidence_id,
                    proposal_id=proposal.proposal_id,
                    hypothesis_id=hypothesis.hypothesis_id,
                    status=ProposalEvidenceStatus.REJECTED,
                    payload_sha256=payload_sha256,
                    reasons=reasons,
                    source_analysis_id=source.analysis.analysis_id,
                    source_result_id=source.analysis.result_id,
                    source_event_id=source.source_event_id,
                    graph_fingerprint=source.graph_fingerprint,
                    provider_name=provider_name,
                    provider_version=provider_version,
                    proposal=proposal,
                    payload=payload,
                ))
                continue
            accepted_rows.append(_accepted_evidence(source, proposal, hypothesis, provider_name, provider_version))
        accepted_tuple = tuple(sorted(accepted_rows, key=lambda item: item.proposal_id or ""))
        rejected_tuple = tuple(sorted(rejected_rows, key=lambda item: item.evidence_id))
        proposal_set_payload = {
            "schema_version": 1,
            "source_analysis_id": source.analysis.analysis_id,
            "source_result_id": source.analysis.result_id,
            "source_event_id": source.source_event_id,
            "graph_fingerprint": source.graph_fingerprint,
            "causal_context": causal_context_to_dict(causal_context),
            "provider": {
                "name": provider_name,
                "version": provider_version,
                "deterministic": provider_deterministic,
            },
            "generated_result_id": generated.result_id,
            "accepted": [_evidence_payload(item) for item in accepted_tuple],
            "rejected": [_evidence_payload(item) for item in rejected_tuple],
            "validation_plan": result_to_dict(generated)["validation_plan"],
        }
        proposal_set_id = "runtime-proposal-set-" + _stable_hash(proposal_set_payload)[:24]
        artifact_payload = {**proposal_set_payload, "proposal_set_id": proposal_set_id}
        content = (canonical_json(artifact_payload) + "\n").encode("utf-8")
        artifact = SurvivorAnalysisArtifact(
            logical_key=f"survivor-proposals/{source.request.executions[0].execution_id}.json",
            logical_role="survivor_proposal_evidence",
            content_sha256=_hash_bytes(content),
            size_bytes=len(content),
            schema_version=1,
            producer="theseus_survivor_adapter.pr39",
            content=content,
            metadata={
                "source_analysis_id": source.analysis.analysis_id,
                "source_result_id": source.analysis.result_id,
                "source_event_id": source.source_event_id,
                "graph_fingerprint": source.graph_fingerprint,
                "proposal_set_id": proposal_set_id,
                "accepted_count": len(accepted_tuple),
                "rejected_count": len(rejected_tuple),
                "causal_context_id": causal_context.context_id,
                "causal_context_complete": causal_context.complete,
            },
        )
        duplicates: list[str] = []
        for evidence in (*accepted_tuple, *rejected_tuple):
            if self._ledger.commit(evidence):
                duplicates.append(evidence.evidence_id)
        return SurvivorProposalPipelineResult(
            source_analysis_id=source.analysis.analysis_id,
            proposal_set_id=proposal_set_id,
            accepted=accepted_tuple,
            rejected=rejected_tuple,
            validation_plan=generated.validation_plan,
            artifact=artifact,
            duplicate_evidence_ids=tuple(sorted(duplicates)),
            causal_context=causal_context,
        )
__all__ = ["SurvivorProposalLedger", "SurvivorProposalPipeline"]
