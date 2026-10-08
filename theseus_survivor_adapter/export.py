"""Deterministic test-only export bundles for approved survivor repairs."""
from __future__ import annotations
import hashlib
import io
import json
import os
import shutil
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any
from theseus_survivor_lab.serialization import canonical_json
from theseus_survivor_lab.validation import canonical_relative_path, canonical_text
from .contracts import (
    SurvivorExportBundle,
    SurvivorExportFile,
    SurvivorExportFormat,
    SurvivorHumanReviewReceipt,
    SurvivorProposalEvidence,
    SurvivorProposalValidationReceipt,
    SurvivorWorkflowCheckpoint,
)
from .contracts import ProposalEvidenceStatus, ProposalVerificationStatus, SurvivorAdapterError, SurvivorEvidenceError
def _sha256(value: bytes) -> str:
    # Hash one immutable export payload.
    return hashlib.sha256(value).hexdigest()
def _test_path(value: str | None) -> str:
    # Normalize an export target and reject production or escaping paths.
    try:
        path = canonical_relative_path(value)
    except Exception as exc:
        raise SurvivorEvidenceError(f"export target path is invalid: path={value!r}; error={exc}") from exc
    if not path or not path.startswith("tests/") or not path.endswith(".py"):
        raise SurvivorEvidenceError(f"export target must be a Python test file: path={path!r}")
    return path
def _candidate_file(proposal: SurvivorProposalEvidence) -> SurvivorExportFile:
    # Convert one accepted proposal into canonical test-only file bytes.
    if proposal.status is not ProposalEvidenceStatus.ACCEPTED or proposal.proposal is None:
        raise SurvivorEvidenceError(f"export requires accepted proposal evidence: evidence_id={proposal.evidence_id}")
    if not proposal.proposal.generated_code:
        raise SurvivorEvidenceError(f"export requires generated test code: proposal_id={proposal.proposal_id}")
    target = _test_path(proposal.proposal.target_test_file)
    target_path = PurePosixPath(target)
    proposal_suffix = "".join(character for character in (proposal.proposal_id or "proposal") if character.isalnum())[-16:]
    path = (target_path.parent / f"{target_path.stem}__survivor_{proposal_suffix}.py").as_posix()
    content = canonical_text(proposal.proposal.generated_code).encode("utf-8")
    return SurvivorExportFile(path=path, content_sha256=_sha256(content), content=content)
def _unified_diff(file: SurvivorExportFile) -> bytes:
    # Render a deterministic new-file unified diff for one test candidate.
    lines = file.content.decode("utf-8").splitlines()
    body = ["--- /dev/null", f"+++ b/{file.path}", f"@@ -0,0 +1,{len(lines)} @@"]
    body.extend(f"+{line}" for line in lines)
    return ("\n".join(body) + "\n").encode("utf-8")
def _manifest(
    checkpoint: SurvivorWorkflowCheckpoint,
    proposal: SurvivorProposalEvidence,
    validation: SurvivorProposalValidationReceipt,
    review: SurvivorHumanReviewReceipt,
    file: SurvivorExportFile,
) -> dict[str, Any]:
    # Build the path-independent export manifest from verified workflow identities.
    return {
        "schema_version": 1,
        "workflow_id": checkpoint.workflow_id,
        "campaign_id": checkpoint.campaign_id,
        "project_id": checkpoint.project_id,
        "revision": checkpoint.revision,
        "mutant_id": checkpoint.mutant_id,
        "source_execution_id": checkpoint.source_execution_id,
        "source_analysis_id": checkpoint.source_analysis_id,
        "source_result_id": checkpoint.source_result_id,
        "source_event_id": checkpoint.source_event_id,
        "graph_fingerprint": checkpoint.graph_fingerprint,
        "proposal_id": proposal.proposal_id,
        "proposal_evidence_id": proposal.evidence_id,
        "validation_id": validation.validation_id,
        "validation_execution_id": validation.validation_execution_id,
        "review_id": review.review_id,
        "reviewer_id": review.reviewer_id,
        "reviewed_at": review.reviewed_at,
        "review_decision": review.decision.value,
        "proposal_target_test_file": proposal.proposal.target_test_file if proposal.proposal is not None else None,
        "files": [{"path": file.path, "content_sha256": file.content_sha256, "size_bytes": len(file.content)}],
    }
def _zip_bytes(manifest_bytes: bytes, file: SurvivorExportFile) -> bytes:
    # Create a reproducible ZIP with fixed timestamps, permissions, and member order.
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name, content in (("manifest.json", manifest_bytes), (file.path, file.content)):
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            archive.writestr(info, content)
    return stream.getvalue()
def build_survivor_export(
    checkpoint: SurvivorWorkflowCheckpoint,
    proposal: SurvivorProposalEvidence,
    validation: SurvivorProposalValidationReceipt,
    review: SurvivorHumanReviewReceipt,
    export_format: SurvivorExportFormat,
) -> SurvivorExportBundle:
    # Build a deterministic export only from approved, verified, identity-matched evidence.
    if checkpoint.state.value != "approved":
        raise SurvivorEvidenceError(f"export requires approved workflow: state={checkpoint.state.value}")
    if validation.status is not ProposalVerificationStatus.VERIFIED:
        raise SurvivorEvidenceError(f"export requires verified validation: validation_id={validation.validation_id}")
    if review.decision.value != "approve":
        raise SurvivorEvidenceError(f"export requires human approval: review_id={review.review_id}")
    if proposal.proposal_id != checkpoint.proposal_id or validation.proposal_id != checkpoint.proposal_id:
        raise SurvivorEvidenceError("export proposal identity does not match workflow checkpoint")
    if validation.validation_id != checkpoint.validation_id or review.validation_id != checkpoint.validation_id:
        raise SurvivorEvidenceError("export validation identity does not match workflow checkpoint")
    if review.review_id != checkpoint.review_id or review.proposal_id != checkpoint.proposal_id:
        raise SurvivorEvidenceError("export review identity does not match workflow checkpoint")
    file = _candidate_file(proposal)
    manifest = _manifest(checkpoint, proposal, validation, review, file)
    identity_manifest = {**manifest, "export_format": export_format.value}
    identity_bytes = (canonical_json(identity_manifest) + "\n").encode("utf-8")
    diff = _unified_diff(file)
    export_id = "survivor-export-" + _sha256(identity_bytes + b"\0" + file.content)[:24]
    final_manifest = {**identity_manifest, "export_id": export_id}
    final_manifest_bytes = (canonical_json(final_manifest) + "\n").encode("utf-8")
    archive = _zip_bytes(final_manifest_bytes, file) if export_format is SurvivorExportFormat.OVERLAY_ZIP else None
    return SurvivorExportBundle(
        export_id=export_id,
        export_format=export_format,
        manifest_sha256=_sha256(final_manifest_bytes),
        manifest=final_manifest,
        files=(file,),
        unified_diff=diff,
        archive=archive,
    )
def _fsync_directory(path: Path) -> None:
    # Persist directory entries on POSIX while avoiding unsupported Windows directory descriptors.
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
def _write_durable_file(path: Path, content: bytes) -> None:
    # Write and flush one complete file through the same writable descriptor used by fsync.
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
def _fsync_tree_directories(root: Path) -> None:
    # Persist nested overlay directory entries before publishing the complete tree.
    if os.name == "nt":
        return
    directories = [root, *(item for item in root.rglob("*") if item.is_dir())]
    for directory in sorted(directories, key=lambda item: len(item.parts), reverse=True):
        _fsync_directory(directory)
def _write_atomic_file(path: Path, content: bytes) -> None:
    # Publish one file through a sibling temporary file and atomic replace.
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)
def _existing_directory_export_id(path: Path) -> str | None:
    # Read only the existing manifest identity for exact idempotent replay.
    manifest = path / "manifest.json"
    if not manifest.is_file():
        return None
    try:
        data = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return str(data.get("export_id") or "") or None
def publish_survivor_export(bundle: SurvivorExportBundle, destination: str | Path) -> Path:
    # Atomically publish a complete diff, ZIP, or test-only overlay directory.
    path = Path(destination)
    if bundle.export_format is SurvivorExportFormat.UNIFIED_DIFF:
        _write_atomic_file(path, bundle.unified_diff)
        return path
    if bundle.export_format is SurvivorExportFormat.OVERLAY_ZIP:
        if bundle.archive is None:
            raise SurvivorAdapterError(f"ZIP export has no archive bytes: export_id={bundle.export_id}")
        _write_atomic_file(path, bundle.archive)
        return path
    if path.exists():
        if path.is_dir() and _existing_directory_export_id(path) == bundle.export_id:
            return path
        raise SurvivorAdapterError(f"overlay destination already exists with different content: path={path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{path.name}.", dir=path.parent))
    try:
        manifest_bytes = (canonical_json(dict(bundle.manifest)) + "\n").encode("utf-8")
        _write_durable_file(temporary / "manifest.json", manifest_bytes)
        for item in bundle.files:
            _write_durable_file(temporary / item.path, item.content)
        _fsync_tree_directories(temporary)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary, ignore_errors=True)
    return path
__all__ = ["build_survivor_export", "publish_survivor_export"]