"""Validate AI mutation proposals through the normal immutable mutation pipeline."""

from __future__ import annotations

import ast
import base64
import hashlib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable

from theseus_contracts import MutantDescriptor, MutantId, PreparedMutant
from theseus_contracts.serialization import dumps


class MutationCandidateError(ValueError):
    """Raised when an advisory mutation cannot enter deterministic execution."""


@dataclass(frozen=True, slots=True)
class MutationCandidate:
    source_path: str
    line_no: int
    column_no: int
    original: str
    replacement: str
    operator_version: str = "ai-v1"
    rationale: str = ""

    def normalized(self) -> "MutationCandidate":
        path = PurePosixPath(str(self.source_path).replace("\\", "/"))
        if path.is_absolute() or ".." in path.parts or not path.parts:
            raise MutationCandidateError("AI mutation source path must be relative and traversal-free")
        if int(self.line_no) < 1 or int(self.column_no) < 0:
            raise MutationCandidateError("AI mutation location is invalid")
        if not str(self.operator_version).strip():
            raise MutationCandidateError("AI mutation operator_version must be non-empty")
        if not str(self.original):
            raise MutationCandidateError("AI mutation original text must be non-empty")
        return MutationCandidate(
            source_path=path.as_posix(),
            line_no=int(self.line_no),
            column_no=int(self.column_no),
            original=str(self.original),
            replacement=str(self.replacement),
            operator_version=str(self.operator_version).strip(),
            rationale=str(self.rationale),
        )


@dataclass(frozen=True, slots=True)
class PreparedAIMutation:
    """Immutable prepared artifact that is safe to hand to the existing execution path."""

    candidate: MutationCandidate
    mutation_identity: str
    source_sha256_before: str
    rendered_sha256: str
    rendered_source: bytes
    prepared_mutant: PreparedMutant

    def to_dict(self) -> dict[str, object]:
        return {
            "candidate": self.candidate.__dict__ if hasattr(self.candidate, "__dict__") else {
                "source_path": self.candidate.source_path,
                "line_no": self.candidate.line_no,
                "column_no": self.candidate.column_no,
                "original": self.candidate.original,
                "replacement": self.candidate.replacement,
                "operator_version": self.candidate.operator_version,
            },
            "mutation_identity": self.mutation_identity,
            "source_sha256_before": self.source_sha256_before,
            "rendered_sha256": self.rendered_sha256,
            "prepared_mutant": self.prepared_mutant.to_dict(),
        }


def prepare_candidate(
    candidate: MutationCandidate,
    project_root: Path,
    *,
    allowed_source_paths: Iterable[str] | None = None,
) -> PreparedAIMutation:
    """Validate, normalize, render, syntax-check, and identify one candidate."""

    normalized = candidate.normalized()
    if allowed_source_paths is not None:
        allowed = {PurePosixPath(str(item).replace("\\", "/")).as_posix() for item in allowed_source_paths}
        if normalized.source_path not in allowed:
            raise MutationCandidateError("AI mutation is outside the selected source scope")
    root = Path(project_root).resolve()
    source = (root / normalized.source_path).resolve()
    if not source.is_relative_to(root) or not source.is_file() or source.suffix.lower() != ".py":
        raise MutationCandidateError("AI mutation source file is unavailable or outside project root")
    original_bytes = source.read_bytes()
    try:
        source_text = original_bytes.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")
    except UnicodeDecodeError as exc:
        raise MutationCandidateError("AI mutation source must be UTF-8") from exc
    lines = source_text.splitlines(keepends=True)
    if normalized.line_no > len(lines):
        raise MutationCandidateError("AI mutation line is outside source")
    line = lines[normalized.line_no - 1]
    column = max(0, int(normalized.column_no))
    line_offset = sum(len(item) for item in lines[: normalized.line_no - 1])
    local_offset = line.find(normalized.original, column)
    if local_offset < 0:
        raise MutationCandidateError("AI mutation original text does not match source")
    absolute_offset = line_offset + local_offset
    rendered_text = (
        source_text[:absolute_offset]
        + normalized.replacement
        + source_text[absolute_offset + len(normalized.original) :]
    )
    try:
        ast.parse(rendered_text, filename=str(source))
    except SyntaxError as exc:
        raise MutationCandidateError(f"AI mutation is not syntactically valid: {exc}") from exc
    rendered_bytes = rendered_text.encode("utf-8")
    source_sha256 = hashlib.sha256(original_bytes).hexdigest()
    rendered_sha256 = hashlib.sha256(rendered_bytes).hexdigest()
    identity_payload = {
        "source_path": normalized.source_path,
        "line_no": normalized.line_no,
        "column_no": normalized.column_no,
        "original": normalized.original,
        "replacement": normalized.replacement,
        "operator_version": normalized.operator_version,
        "source_sha256_before": source_sha256,
    }
    mutation_identity = "ai-" + hashlib.sha256(dumps(identity_payload).encode("utf-8")).hexdigest()[:32]
    descriptor = MutantDescriptor(
        mutant_id=MutantId(mutation_identity),
        mutation=f"{normalized.original}->{normalized.replacement}",
        source_path=normalized.source_path,
        line_no=normalized.line_no,
        column_no=normalized.column_no,
        original=normalized.original,
        replacement=normalized.replacement,
        operator_version=normalized.operator_version,
    )
    prepared = PreparedMutant(
        mutant=descriptor,
        source_sha256_before=source_sha256,
        rendered_sha256=rendered_sha256,
        compiled=True,
        source_b64=base64.b64encode(rendered_bytes).decode("ascii"),
    )
    return PreparedAIMutation(
        candidate=normalized,
        mutation_identity=mutation_identity,
        source_sha256_before=source_sha256,
        rendered_sha256=rendered_sha256,
        rendered_source=rendered_bytes,
        prepared_mutant=prepared,
    )


def deduplicate_candidates(candidates: Iterable[PreparedAIMutation]) -> tuple[PreparedAIMutation, ...]:
    """Collapse semantic duplicates by immutable MutationIdentity, preserving stable order."""

    unique: dict[str, PreparedAIMutation] = {}
    for item in candidates:
        existing = unique.get(item.mutation_identity)
        if existing is not None and existing.rendered_sha256 != item.rendered_sha256:
            raise MutationCandidateError("one mutation identity maps to conflicting rendered bytes")
        unique.setdefault(item.mutation_identity, item)
    return tuple(unique[key] for key in sorted(unique))


__all__ = [
    "MutationCandidate",
    "MutationCandidateError",
    "PreparedAIMutation",
    "deduplicate_candidates",
    "prepare_candidate",
]
