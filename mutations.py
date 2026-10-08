"""AST/token-aware mutation generation and recoverable source snapshots."""
from __future__ import annotations
import ast
import heapq
import io
import json
import tokenize
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import Iterable, Iterator, Sequence
from .io_utils import atomic_write_bytes, atomic_write_json, ensure_dir, read_json, sha256_bytes, stable_hash, utc_now_iso
from .models import Mutant, PerformanceMetrics
MUTATION_REPLACEMENTS: tuple[tuple[str, str, str], ...] = (
    ("true_to_false", "True", "False"),
    ("false_to_true", "False", "True"),
    ("none_to_empty_string", "None", "\"\""),
    ("eq_to_ne", "==", "!="),
    ("ne_to_eq", "!=", "=="),
    ("lt_to_le", "<", "<="),
    ("le_to_lt", "<=", "<"),
    ("gt_to_ge", ">", ">="),
    ("ge_to_gt", ">=", ">"),
    ("plus_to_minus", "+", "-"),
    ("minus_to_plus", "-", "+"),
    ("and_to_or", "and", "or"),
    ("or_to_and", "or", "and"),
)
TOKEN_MUTATION_OPERATOR_VERSION = "m2"
MUTATION_OPERATOR_VERSION = "m3"
AWAIT_MUTATION_OPERATOR_VERSION = "m4"
AST_MUTATION_OPERATORS: tuple[str, ...] = (
    "return_value_to_none",
    "condition_to_not",
    "remove_standalone_call",
    "raise_to_pass",
    "empty_list_to_none",
    "empty_dict_to_sentinel",
    "await_to_expression",
)
class RestoreError(RuntimeError):
    """Raised when the target changed unexpectedly and must not be overwritten."""
class InvalidMutantError(ValueError):
    """Raised when a token replacement does not produce valid Python."""
@dataclass(frozen=True)
class SourceSnapshot:
    target_path: Path
    original_bytes: bytes
    original_sha256: str
    original_size: int
    original_mtime_ns: int
    mode: int
    artifact_path: Path
    @property
    def text(self) -> str:
        return self.original_bytes.decode("utf-8")
    def to_dict(self) -> dict[str, object]:
        return {
            "target_path": str(self.target_path),
            "original_sha256": self.original_sha256,
            "original_size": self.original_size,
            "original_mtime_ns": self.original_mtime_ns,
            "mode": self.mode,
            "artifact_path": str(self.artifact_path),
            "created_at": utc_now_iso(),
        }
@dataclass(frozen=True)
class PreparedMutant:
    """Compiled bytes held for one mutant immediately before installation."""
    mutant_id: str
    original_sha256: str
    data: bytes
    sha256: str
    def to_manifest(self) -> dict[str, object]:
        # Persist provenance without copying the prepared source bytes into recovery metadata.
        return {
            "mutant_id": self.mutant_id,
            "original_sha256": self.original_sha256,
            "sha256": self.sha256,
            "size": len(self.data),
        }
def _line_offsets(text: str) -> list[int]:
    offsets = [0]
    for index, char in enumerate(text):
        if char == "\n":
            offsets.append(index + 1)
    return offsets
def _canonical_source_text(text: str) -> str:
    # Normalize line endings only for mutant identity while preserving raw patch offsets.
    return text.replace("\r\n", "\n").replace("\r", "\n")
def _offset(offsets: Sequence[int], row: int, column: int) -> int:
    return offsets[max(0, row - 1)] + column
def _protected_spans(tokens: Iterable[tokenize.TokenInfo], offsets: Sequence[int]) -> list[tuple[int, int]]:
    # Build protected string/comment spans from the already-tokenized source.
    spans: list[tuple[int, int]] = []
    for token in tokens:
        if token.type in {tokenize.STRING, tokenize.COMMENT, tokenize.ENCODING}:
            spans.append((_offset(offsets, *token.start), _offset(offsets, *token.end)))
    return spans
def _in_spans(start: int, end: int, spans: Iterable[tuple[int, int]]) -> bool:
    return any(start < span_end and end > span_start for span_start, span_end in spans)
def _node_range(node: ast.AST) -> tuple[int, int] | None:
    start = getattr(node, "lineno", None)
    end = getattr(node, "end_lineno", None)
    if start is None or end is None:
        return None
    return int(start), int(end)
def _matches_range(line_no: int, function_range: tuple[int, int] | None, from_line: int | None, to_line: int | None) -> bool:
    if function_range and not (function_range[0] <= line_no <= function_range[1]):
        return False
    if from_line is not None and line_no < from_line:
        return False
    if to_line is not None and line_no > to_line:
        return False
    return True
def available_mutation_operators() -> tuple[str, ...]:
    # Return the stable operator catalog exposed by the CLI and reports.
    return tuple(dict.fromkeys([item[0] for item in MUTATION_REPLACEMENTS] + list(AST_MUTATION_OPERATORS)))
def _ast_offset(lines: Sequence[str], offsets: Sequence[int], row: int, byte_column: int) -> int:
    # Convert AST UTF-8 byte columns using one precomputed line table.
    line = lines[max(0, row - 1)] if row - 1 < len(lines) else ""
    prefix = line.encode("utf-8")[:byte_column].decode("utf-8", errors="ignore")
    return offsets[max(0, row - 1)] + len(prefix)
def _ast_span(lines: Sequence[str], offsets: Sequence[int], node: ast.AST) -> tuple[int, int] | None:
    # Resolve an AST node to character offsets usable by the patch renderer.
    start = getattr(node, "lineno", None)
    end = getattr(node, "end_lineno", None)
    start_column = getattr(node, "col_offset", None)
    end_column = getattr(node, "end_col_offset", None)
    if None in {start, end, start_column, end_column}:
        return None
    return (
        _ast_offset(lines, offsets, int(start), int(start_column)),
        _ast_offset(lines, offsets, int(end), int(end_column)),
    )
def _ast_mutant(
    text: str,
    lines: Sequence[str],
    offsets: Sequence[int],
    identity_text: str,
    identity_lines: Sequence[str],
    identity_offsets: Sequence[int],
    node: ast.AST,
    *,
    operator: str,
    replacement: str,
    function_range: tuple[int, int] | None,
    from_line: int | None,
    to_line: int | None,
    mutant_ids: set[str] | None,
    operator_version: str = MUTATION_OPERATOR_VERSION,
) -> Mutant | None:
    # Build a raw patch whose stable identity is independent from LF or CRLF storage.
    line_no = getattr(node, "lineno", None)
    column_no = getattr(node, "col_offset", None)
    if (
        line_no is None
        or column_no is None
        or not _matches_range(
            int(line_no),
            function_range,
            from_line,
            to_line,
        )
    ):
        return None
    span = _ast_span(lines, offsets, node)
    identity_span = _ast_span(
        identity_lines,
        identity_offsets,
        node,
    )
    if (
        span is None
        or identity_span is None
        or span[0] >= span[1]
        or identity_span[0] >= identity_span[1]
    ):
        return None
    start, end = span
    identity_start, identity_end = identity_span
    original = text[start:end]
    identity_original = identity_text[identity_start:identity_end]
    if original == replacement:
        return None
    context_hash = stable_hash(
        {
            "operator_version": operator_version,
            "operator": operator,
            "original": identity_original,
            "before": identity_text[
                max(0, identity_start - 32) : identity_start
            ],
            "after": identity_text[
                identity_end : identity_end + 32
            ],
            "start": identity_start,
        }
    )[:12]
    mutant_id = (
        f"{operator_version}:{operator}:"
        f"L{int(line_no)}:C{int(column_no) + 1}:"
        f"{context_hash}"
    )
    if mutant_ids and mutant_id not in mutant_ids:
        return None
    return Mutant(
        mutant_id=mutant_id,
        mutation=operator,
        line_no=int(line_no),
        column_no=int(column_no) + 1,
        original=original,
        replacement=replacement,
        start=start,
        end=end,
        operator_version=operator_version,
    )
def _iter_ast_mutants(
    text: str,
    lines: Sequence[str],
    offsets: Sequence[int],
    identity_text: str,
    identity_lines: Sequence[str],
    identity_offsets: Sequence[int],
    *,
    function_range: tuple[int, int] | None,
    from_line: int | None,
    to_line: int | None,
    mutant_ids: set[str] | None,
    enabled: set[str],
) -> Iterator[Mutant]:
    # Generate AST mutations with raw offsets and newline-independent identities.
    tree = ast.parse(text)
    candidates: list[
        tuple[str, ast.AST, str, str]
    ] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Return) and node.value is not None:
            candidates.append(
                (
                    "return_value_to_none",
                    node.value,
                    "None",
                    MUTATION_OPERATOR_VERSION,
                )
            )
        elif isinstance(node, (ast.If, ast.While)):
            condition_span = _ast_span(
                lines,
                offsets,
                node.test,
            )
            if condition_span is not None:
                condition = text[
                    condition_span[0] : condition_span[1]
                ]
                candidates.append(
                    (
                        "condition_to_not",
                        node.test,
                        f"not ({condition})",
                        MUTATION_OPERATOR_VERSION,
                    )
                )
        elif (
            isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Call)
        ):
            candidates.append(
                (
                    "remove_standalone_call",
                    node,
                    "pass",
                    MUTATION_OPERATOR_VERSION,
                )
            )
        elif isinstance(node, ast.Raise):
            candidates.append(
                (
                    "raise_to_pass",
                    node,
                    "pass",
                    MUTATION_OPERATOR_VERSION,
                )
            )
        elif isinstance(node, ast.List) and not node.elts:
            candidates.append(
                (
                    "empty_list_to_none",
                    node,
                    "[None]",
                    MUTATION_OPERATOR_VERSION,
                )
            )
        elif isinstance(node, ast.Dict) and not node.keys:
            candidates.append(
                (
                    "empty_dict_to_sentinel",
                    node,
                    '{"mutant": None}',
                    MUTATION_OPERATOR_VERSION,
                )
            )
        elif isinstance(node, ast.Await):
            value_span = _ast_span(
                lines,
                offsets,
                node.value,
            )
            if value_span is not None:
                candidates.append(
                    (
                        "await_to_expression",
                        node,
                        text[value_span[0] : value_span[1]],
                        AWAIT_MUTATION_OPERATOR_VERSION,
                    )
                )
    candidates = [
        candidate
        for candidate in candidates
        if candidate[0] in enabled
    ]
    for (
        operator,
        target,
        replacement,
        operator_version,
    ) in sorted(
        candidates,
        key=lambda item: (
            int(getattr(item[1], "lineno", 0) or 0),
            int(getattr(item[1], "col_offset", 0) or 0),
            item[0],
            type(item[1]).__name__,
        ),
    ):
        mutant = _ast_mutant(
            text,
            lines,
            offsets,
            identity_text,
            identity_lines,
            identity_offsets,
            target,
            operator=operator,
            replacement=replacement,
            function_range=function_range,
            from_line=from_line,
            to_line=to_line,
            mutant_ids=mutant_ids,
            operator_version=operator_version,
        )
        if mutant is not None:
            yield mutant
def _iter_token_mutants(
    text: str,
    offsets: Sequence[int],
    identity_text: str,
    identity_offsets: Sequence[int],
    tokens: Sequence[tokenize.TokenInfo],
    protected: Sequence[tuple[int, int]],
    *,
    function_range: tuple[int, int] | None,
    from_line: int | None,
    to_line: int | None,
    mutant_ids: set[str] | None,
    enabled: set[str],
) -> Iterator[Mutant]:
    # Generate raw token patches with identities based on canonical line endings.
    for token in tokens:
        original = token.string
        matches = [
            (name, replacement)
            for name, value, replacement
            in MUTATION_REPLACEMENTS
            if (
                value == original
                and name in enabled
            )
        ]
        if (
            not matches
            or token.type not in {
                tokenize.NAME,
                tokenize.OP,
            }
        ):
            continue
        start = _offset(
            offsets,
            *token.start,
        )
        end = _offset(
            offsets,
            *token.end,
        )
        if _in_spans(start, end, protected):
            continue
        line_no = int(token.start[0])
        if not _matches_range(
            line_no,
            function_range,
            from_line,
            to_line,
        ):
            continue
        identity_start = _offset(
            identity_offsets,
            *token.start,
        )
        identity_end = _offset(
            identity_offsets,
            *token.end,
        )
        for mutation_name, replacement in matches:
            context_hash = stable_hash(
                {
                    "operator": mutation_name,
                    "original": original,
                    "before": identity_text[
                        max(0, identity_start - 32)
                        : identity_start
                    ],
                    "after": identity_text[
                        identity_end
                        : identity_end + 32
                    ],
                    "start": identity_start,
                }
            )[:12]
            mutant_id = (
                f"m2:{mutation_name}:"
                f"L{line_no}:C{token.start[1] + 1}:"
                f"{context_hash}"
            )
            if mutant_ids and mutant_id not in mutant_ids:
                continue
            yield Mutant(
                mutant_id=mutant_id,
                mutation=mutation_name,
                line_no=line_no,
                column_no=token.start[1] + 1,
                original=original,
                replacement=replacement,
                start=start,
                end=end,
                operator_version=TOKEN_MUTATION_OPERATOR_VERSION,
            )
def generate_mutants(
    text: str,
    *,
    function_range: tuple[int, int] | None = None,
    from_line: int | None = None,
    to_line: int | None = None,
    mutant_ids: set[str] | None = None,
    max_mutants: int | None = None,
    operators: Iterable[str] | None = None,
) -> list[Mutant]:
    """Create one-token mutants while preserving strings and comments.
    Filters are deliberately applied together: function + line range + exact
    mutant IDs form an AND expression. This makes a rerun reproducible.
    """
    # Generate raw patches while deriving stable identities from canonical line endings.
    offsets = _line_offsets(text)
    lines = text.splitlines(keepends=True)
    identity_text = _canonical_source_text(text)
    identity_offsets = _line_offsets(identity_text)
    identity_lines = identity_text.splitlines(
        keepends=True
    )
    enabled = set(
        available_mutation_operators()
        if operators is None
        else operators
    )
    if max_mutants == 0:
        return []
    try:
        tokens = tuple(
            tokenize.generate_tokens(
                io.StringIO(text).readline
            )
        )
    except (
        IndentationError,
        tokenize.TokenError,
    ) as exc:
        raise SyntaxError(
            f"cannot tokenize target source: {exc}"
        ) from exc
    protected = _protected_spans(
        tokens,
        offsets,
    )
    token_mutants = _iter_token_mutants(
        text,
        offsets,
        identity_text,
        identity_offsets,
        tokens,
        protected,
        function_range=function_range,
        from_line=from_line,
        to_line=to_line,
        mutant_ids=mutant_ids,
        enabled=enabled,
    )
    ast_mutants = _iter_ast_mutants(
        text,
        lines,
        offsets,
        identity_text,
        identity_lines,
        identity_offsets,
        function_range=function_range,
        from_line=from_line,
        to_line=to_line,
        mutant_ids=mutant_ids,
        enabled=enabled,
    )
    ordered = heapq.merge(
        token_mutants,
        ast_mutants,
        key=lambda item: (
            item.line_no,
            item.column_no,
            item.mutation,
            item.mutant_id,
        ),
    )
    if (
        max_mutants is not None
        and max_mutants >= 0
    ):
        return list(
            islice(
                ordered,
                max_mutants,
            )
        )
    return list(ordered)
def find_function_range(source_text: str, function_name: str | None) -> tuple[int, int] | None:
    if not function_name:
        return None
    tree = ast.parse(source_text)
    matches: list[ast.AST] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function_name:
            matches.append(node)
    if not matches:
        raise LookupError(f"function not found in source: {function_name}")
    if len(matches) > 1:
        raise LookupError(f"function name is ambiguous in source: {function_name}")
    value = _node_range(matches[0])
    if value is None:
        raise LookupError(f"function has no source range: {function_name}")
    return value
def create_snapshot(
    target_path: Path,
    recovery_dir: Path,
    *,
    metrics: PerformanceMetrics | None = None,
) -> SourceSnapshot:
    target_path = target_path.resolve()
    stat = target_path.stat()
    original = target_path.read_bytes()
    if metrics is not None:
        metrics.source_bytes_read += len(original)
    artifact = ensure_dir(recovery_dir) / f"original_{target_path.name}.bin"
    atomic_write_bytes(
        artifact,
        original,
        mode=stat.st_mode,
        category="recovery",
        metrics=metrics,
    )
    return SourceSnapshot(
        target_path=target_path,
        original_bytes=original,
        original_sha256=sha256_bytes(original),
        original_size=len(original),
        original_mtime_ns=stat.st_mtime_ns,
        mode=stat.st_mode,
        artifact_path=artifact,
    )
def current_sha256(snapshot: SourceSnapshot) -> str:
    return sha256_bytes(snapshot.target_path.read_bytes())
def apply_mutant(
    snapshot: SourceSnapshot,
    mutant: Mutant,
    *,
    durability: str = "normal",
    metrics: PerformanceMetrics | None = None,
) -> str:
    # Preserve the compatibility API while using the cheaper intermediate write policy.
    current = current_sha256(snapshot)
    if current != snapshot.original_sha256:
        raise RestoreError(
            f"refusing to mutate changed target: expected {snapshot.original_sha256}, got {current}"
        )
    data = render_mutant(snapshot, mutant)
    atomic_write_bytes(
        snapshot.target_path,
        data,
        mode=snapshot.mode,
        durability=durability,
        category="source_mutation",
        metrics=metrics,
    )
    return sha256_bytes(data)
def _canonical_offset_map(text: str) -> tuple[str, tuple[int, ...]]:
    # Build canonical LF text and map every canonical character boundary to the physical source offset.
    canonical: list[str] = []
    physical_offsets = [0]
    index = 0
    while index < len(text):
        if text[index] == "\r":
            index += 2 if index + 1 < len(text) and text[index + 1] == "\n" else 1
            canonical.append("\n")
            physical_offsets.append(index)
            continue
        canonical.append(text[index])
        index += 1
        physical_offsets.append(index)
    return "".join(canonical), tuple(physical_offsets)
def _line_endings(text: str) -> tuple[str, ...]:
    # Return physical newline sequences in source order without normalizing mixed line endings.
    endings: list[str] = []
    index = 0
    while index < len(text):
        if text[index] == "\r":
            if index + 1 < len(text) and text[index + 1] == "\n":
                endings.append("\r\n")
                index += 2
            else:
                endings.append("\r")
                index += 1
            continue
        if text[index] == "\n":
            endings.append("\n")
        index += 1
    return tuple(endings)
def _nearby_line_ending(text: str, start: int, end: int) -> str:
    # Choose the nearest existing physical newline when a replacement introduces an additional line break.
    before = _line_endings(text[:start])
    if before:
        return before[-1]
    after = _line_endings(text[end:])
    return after[0] if after else "\n"
def _physical_replacement(
    replacement: str,
    *,
    original_span: str,
    source_text: str,
    start: int,
    end: int,
) -> str:
    # Reapply each original physical newline sequence to the canonical replacement text.
    canonical = _canonical_source_text(replacement)
    if "\n" not in canonical:
        return canonical
    endings = _line_endings(original_span)
    fallback = endings[0] if endings else _nearby_line_ending(source_text, start, end)
    parts = canonical.split("\n")
    rendered = [parts[0]]
    for index, part in enumerate(parts[1:]):
        rendered.append(endings[index] if index < len(endings) else fallback)
        rendered.append(part)
    return "".join(rendered)
def _resolve_mutant_span(text: str, mutant: Mutant) -> tuple[int, int]:
    # Resolve either physical offsets or canonical LF offsets and reject every stale source span.
    start = int(mutant.start)
    end = int(mutant.end)
    if 0 <= start <= end <= len(text) and text[start:end] == mutant.original:
        return start, end
    canonical_text, physical_offsets = _canonical_offset_map(text)
    canonical_original = _canonical_source_text(mutant.original)
    if (
        0 <= start <= end <= len(canonical_text)
        and canonical_text[start:end] == canonical_original
    ):
        return physical_offsets[start], physical_offsets[end]
    raw_value = text[start:end] if 0 <= start <= end <= len(text) else "<out-of-range>"
    canonical_value = (
        canonical_text[start:end]
        if 0 <= start <= end <= len(canonical_text)
        else "<out-of-range>"
    )
    raise InvalidMutantError(
        f"mutant source span mismatch for {mutant.mutant_id}: "
        f"expected {mutant.original!r}, raw={raw_value!r}, canonical={canonical_value!r}"
    )
def render_mutant(snapshot: SourceSnapshot, mutant: Mutant) -> bytes:
    # Render one validated patch against physical source offsets while preserving its original line endings.
    source_text = snapshot.text
    start, end = _resolve_mutant_span(source_text, mutant)
    replacement = _physical_replacement(
        mutant.replacement,
        original_span=source_text[start:end],
        source_text=source_text,
        start=start,
        end=end,
    )
    mutated_text = source_text[:start] + replacement + source_text[end:]
    try:
        compile(mutated_text, str(snapshot.target_path), "exec")
    except SyntaxError as exc:
        raise InvalidMutantError(f"invalid mutant {mutant.mutant_id}: {exc}") from exc
    return mutated_text.encode("utf-8")
def prepare_mutant(snapshot: SourceSnapshot, mutant: Mutant) -> PreparedMutant:
    # Compile and hash exactly one candidate immediately before the recovery record is armed.
    data = render_mutant(snapshot, mutant)
    return PreparedMutant(
        mutant_id=mutant.mutant_id,
        original_sha256=snapshot.original_sha256,
        data=data,
        sha256=sha256_bytes(data),
    )
def prepare_mutant_artifacts(
    target_path: Path,
    original_bytes: bytes,
    mutants: Sequence[Mutant],
) -> tuple[PreparedMutant, ...]:
    # Compile immutable mutant source images once without creating recovery state or changing the target.
    resolved = target_path.resolve()
    stat = resolved.stat()
    snapshot = SourceSnapshot(
        target_path=resolved,
        original_bytes=bytes(original_bytes),
        original_sha256=sha256_bytes(original_bytes),
        original_size=len(original_bytes),
        original_mtime_ns=stat.st_mtime_ns,
        mode=stat.st_mode,
        artifact_path=resolved,
    )
    return tuple(prepare_mutant(snapshot, mutant) for mutant in mutants)

def switch_prepared_mutant(
    snapshot: SourceSnapshot,
    prepared: PreparedMutant,
    *,
    expected_current_sha256: str,
    durability: str = "normal",
    metrics: PerformanceMetrics | None = None,
) -> str:
    # Atomically replace one trusted source image with another prepared image and verify the installed hash.
    current = current_sha256(snapshot)
    if current != expected_current_sha256:
        raise RestoreError(
            f"refusing direct mutant switch: expected current {expected_current_sha256}, got {current}"
        )
    if prepared.original_sha256 != snapshot.original_sha256:
        raise RestoreError(
            f"refusing prepared mutant from another snapshot: expected {snapshot.original_sha256}, "
            f"got {prepared.original_sha256}"
        )
    atomic_write_bytes(
        snapshot.target_path,
        prepared.data,
        mode=snapshot.mode,
        durability=durability,
        category="source_mutation",
        metrics=metrics,
    )
    installed = current_sha256(snapshot)
    if installed != prepared.sha256:
        raise RestoreError(
            f"direct mutant switch hash mismatch: expected {prepared.sha256}, got {installed}"
        )
    return installed
def apply_prepared_mutant(
    snapshot: SourceSnapshot,
    prepared: PreparedMutant,
    *,
    current_sha256_value: str | None = None,
    durability: str = "normal",
    metrics: PerformanceMetrics | None = None,
) -> str:
    # Verify the untouched target and atomically install bytes prepared for this exact snapshot.
    current = current_sha256_value if current_sha256_value is not None else current_sha256(snapshot)
    if current != snapshot.original_sha256:
        raise RestoreError(
            f"refusing to mutate changed target: expected {snapshot.original_sha256}, got {current}"
        )
    if prepared.original_sha256 != snapshot.original_sha256:
        raise RestoreError(
            f"refusing prepared mutant from another snapshot: expected {snapshot.original_sha256}, "
            f"got {prepared.original_sha256}"
        )
    atomic_write_bytes(
        snapshot.target_path,
        prepared.data,
        mode=snapshot.mode,
        durability=durability,
        category="source_mutation",
        metrics=metrics,
    )
    return prepared.sha256
def mutant_sha256(snapshot: SourceSnapshot, mutant: Mutant) -> str:
    # Retain the legacy hash helper for callers that need a standalone rendered digest.
    return sha256_bytes(render_mutant(snapshot, mutant))
def restore_snapshot(
    snapshot: SourceSnapshot,
    *,
    expected_sha256: str | None = None,
    durability: str = "critical",
    category: str = "source_mutation",
    metrics: PerformanceMetrics | None = None,
) -> str:
    # Restore only an expected image and keep the final verification boundary intact.
    current = current_sha256(snapshot)
    accepted = {snapshot.original_sha256}
    if expected_sha256:
        accepted.add(expected_sha256)
    if current not in accepted:
        raise RestoreError(
            f"RESTORE_ERROR: target changed unexpectedly; expected one of {sorted(accepted)}, got {current}"
        )
    atomic_write_bytes(
        snapshot.target_path,
        snapshot.original_bytes,
        mode=snapshot.mode,
        durability=durability,
        category=category,
        metrics=metrics,
    )
    restored = current_sha256(snapshot)
    if restored != snapshot.original_sha256:
        raise RestoreError(
            f"RESTORE_ERROR: restore hash mismatch; expected {snapshot.original_sha256}, got {restored}"
        )
    return restored
def write_manifest(
    path: Path,
    snapshot: SourceSnapshot,
    *,
    expected_hashes: Iterable[str] = (),
    extra: dict[str, object] | None = None,
    metrics: PerformanceMetrics | None = None,
) -> dict[str, object]:
    manifest = snapshot.to_dict()
    manifest["expected_hashes"] = sorted({snapshot.original_sha256, *expected_hashes})
    manifest["status"] = "active"
    if extra:
        manifest.update(extra)
    atomic_write_json(path, manifest, category="manifest", metrics=metrics)
    return manifest
def recover_manifest(path: Path, *, force: bool = False) -> str:
    # Refuse takeover while the recorded owner still controls the campaign target.
    manifest = read_json(path)
    if not isinstance(manifest, dict):
        raise RestoreError("recovery manifest is not an object")
    from .recovery import owner_is_alive
    if owner_is_alive(manifest):
        raise RestoreError("recovery refused: campaign owner is still alive")
    target = Path(str(manifest["target_path"])).resolve()
    artifact = Path(str(manifest["artifact_path"])).resolve()
    original_hash = str(manifest["original_sha256"])
    expected = {str(item) for item in manifest.get("expected_hashes", [])}
    expected.add(original_hash)
    current = sha256_bytes(target.read_bytes())
    if not force and current not in expected:
        raise RestoreError(
            f"recovery refused: target hash {current} is not listed in manifest; inspect before --force"
        )
    original = artifact.read_bytes()
    if sha256_bytes(original) != original_hash:
        raise RestoreError("recovery artifact hash does not match manifest")
    mode = int(manifest.get("mode", 0)) or None
    atomic_write_bytes(target, original, mode=mode)
    restored = sha256_bytes(target.read_bytes())
    if restored != original_hash:
        raise RestoreError(f"recovery verification failed: {restored}")
    lock_value = manifest.get("lock_path")
    if lock_value:
        lock_path = Path(str(lock_value)).resolve()
        if lock_path.exists():
            try:
                lock_payload = json.loads(lock_path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                raise RestoreError(f"recovery lock cannot be verified: {lock_path}: {exc}") from exc
            lock_run_id = lock_payload.get("run_id") if isinstance(lock_payload, dict) else None
            manifest_run_id = manifest.get("run_id")
            if manifest_run_id and lock_run_id and str(lock_run_id) != str(manifest_run_id):
                raise RestoreError("recovery lock belongs to another campaign")
            lock_path.unlink()
    return restored
