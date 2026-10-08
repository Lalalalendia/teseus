"""Fast local project index and frozen test-selection planner."""
from __future__ import annotations
import ast
import json
import os
import sqlite3
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping
from .impact import SQLiteImpactAdapter
from .io_utils import atomic_write_json, rel_path, sha256_bytes, sha256_file, stable_hash, utc_now_iso
from .models import (
    CollectionSnapshot,
    FunctionInfo,
    SelectionLevel,
    SelectionSnapshot,
    TestInfo,
    make_selection_evidence,
)
from .test_stats import load_selection_stats
EXCLUDED_DIRS = {
    ".git",
    ".hg",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "venv",
    "__pycache__",
    "coverage",
    "reports",
    "htmlcov",
    "node_modules",
}
SELECTION_ALGORITHM_VERSION = "selection-v5"
INDEX_SCHEMA_VERSION = 3
INTELLIGENCE_VERSION = "incremental-project-test-intelligence-v1"


def _is_test_path(relative: str) -> bool:
    # Classify test-owned Python inputs independently from source fingerprints.
    normalized = relative.replace("\\", "/").lstrip("./")
    name = Path(normalized).name.lower()
    parts = {item.lower() for item in Path(normalized).parts}
    return (
        name == "conftest.py"
        or name.startswith("test_")
        or name.endswith("_test.py")
        or bool(parts & {"test", "tests"})
    )


def _resolve_local_module(root: Path, current: Path, module: str | None, level: int) -> str | None:
    # Resolve only local Python modules; third-party imports remain outside the project graph.
    if level:
        base = current.parent
        for _ in range(max(0, level - 1)):
            base = base.parent
    else:
        base = root
    if not module:
        return None
    module_path = Path(*(str(module).split("."))) if module else Path()
    candidates = (
        base / module_path.with_suffix(".py"),
        base / module_path / "__init__.py",
    )
    for candidate in candidates:
        if candidate.is_file():
            return rel_path(candidate, root)
    return None


def _dependency_metadata(tree: ast.AST, path: Path, root: Path) -> tuple[tuple[str, ...], tuple[str, ...], bool]:
    # Extract local import edges and declared file inputs from one already parsed AST.
    dependencies: set[str] = set()
    candidate_inputs: set[str] = set()
    dynamic = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                resolved = _resolve_local_module(root, path, alias.name, 0)
                if resolved:
                    dependencies.add(resolved)
        elif isinstance(node, ast.ImportFrom):
            resolved = _resolve_local_module(root, path, node.module, int(node.level or 0))
            if resolved:
                dependencies.add(resolved)
            elif node.level or node.module:
                for alias in node.names:
                    resolved = _resolve_local_module(
                        root,
                        path,
                        ".".join(item for item in (node.module, alias.name) if item),
                        int(node.level or 0),
                    )
                    if resolved:
                        dependencies.add(resolved)
        elif isinstance(node, ast.Call):
            called = node.func.id if isinstance(node.func, ast.Name) else ""
            dynamic = dynamic or called in {"__import__", "import_module"}
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            value = node.value.replace("\\", "/")
            if value and not value.startswith(("/", "\\")):
                try:
                    candidate = (path.parent / value).resolve()
                except (OSError, ValueError):
                    continue
                if candidate.is_file() and candidate.is_relative_to(root):
                    candidate_inputs.add(rel_path(candidate, root))
    return tuple(sorted(dependencies)), tuple(sorted(candidate_inputs)), dynamic
@dataclass(frozen=True)
class NodeidValidationContext:
    """Immutable nodeid inventory reused by one selection campaign."""
    project_root: Path
    known_collected_nodeids: frozenset[str]
    known_bases: frozenset[str]
    existing_files: frozenset[str]
    authoritative: bool = False
    collection_snapshot: CollectionSnapshot | None = None
    @classmethod
    def from_index(
        cls,
        index: Mapping[str, Any],
        project_root: Path,
        *,
        collected_nodeids: Iterable[str] | None = None,
        collection_snapshot: CollectionSnapshot | Mapping[str, Any] | None = None,
    ) -> "NodeidValidationContext":
        # Build the base-name and existing-file sets once at the campaign boundary.
        snapshot = (
            CollectionSnapshot.from_dict(collection_snapshot)
            if isinstance(collection_snapshot, Mapping)
            else collection_snapshot
        )
        indexed_nodeids = (
            str(item.get("nodeid"))
            for item in index.get("tests", [])
            if isinstance(item, Mapping) and item.get("nodeid")
        )
        snapshot_is_authoritative = snapshot is not None and not snapshot.collection_errors
        source_nodeids = (
            tuple(snapshot.nodeids)
            if snapshot_is_authoritative
            else tuple(collected_nodeids)
            if collected_nodeids is not None
            else tuple(indexed_nodeids)
        )
        collected = frozenset(str(value) for value in source_nodeids if str(value))
        bases = frozenset(_nodeid_base(value) for value in collected if _nodeid_base(value))
        root = project_root.resolve()
        files = frozenset(
            base.split("::", 1)[0].replace("\\", "/").lstrip("./")
            for base in bases
            if (root / base.split("::", 1)[0].replace("\\", "/").lstrip("./")).is_file()
        )
        authoritative = snapshot_is_authoritative or (
            collected_nodeids is not None and snapshot is None
        )
        return cls(root, collected, bases, files, authoritative, snapshot)
def _nodeid_base(value: object) -> str:
    # Normalize a parametrized nodeid to the collection item's stable base.
    return str(value).split("[", 1)[0]
def parse_pytest_collection_output(output: str) -> tuple[str, ...]:
    # Extract one deterministic nodeid list from quiet pytest collection output.
    collected: list[str] = []
    seen: set[str] = set()
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if (
            not line
            or "::" not in line
            or line.startswith(("=", "ERROR", "INTERNALERROR", "collected "))
            or line.endswith((" warning", " warnings"))
        ):
            continue
        if line not in seen:
            seen.add(line)
            collected.append(line)
    return tuple(collected)
def build_collection_snapshot(
    output: str,
    *,
    revision: str | None = None,
    environment_fingerprint: str = "",
    pytest_version: str | None = None,
    plugin_fingerprint: str = "",
    collection_errors: Iterable[str] = (),
    created_at: str | None = None,
) -> CollectionSnapshot:
    # Convert one pytest collect-only artifact into a cacheable authoritative snapshot.
    nodes = parse_pytest_collection_output(output)
    errors = tuple(str(item) for item in collection_errors if str(item))
    if not errors:
        errors = tuple(
            line.strip()
            for line in output.splitlines()
            if line.strip().startswith(("ERROR collecting", "INTERNALERROR"))
        )
    collection_id = stable_hash(
        {
            "revision": revision,
            "environment_fingerprint": environment_fingerprint,
            "pytest_version": pytest_version,
            "plugin_fingerprint": plugin_fingerprint,
            "nodeids": nodes,
            "collection_errors": errors,
        }
    )[:20]
    return CollectionSnapshot(
        collection_id=collection_id,
        revision=revision,
        environment_fingerprint=environment_fingerprint,
        pytest_version=pytest_version,
        plugin_fingerprint=plugin_fingerprint,
        nodeids=nodes,
        collection_errors=errors,
        created_at=created_at or utc_now_iso(),
    )
def build_nodeid_validation_context(
    index: Mapping[str, Any],
    project_root: Path,
    *,
    collected_nodeids: Iterable[str] | None = None,
    collection_snapshot: CollectionSnapshot | Mapping[str, Any] | None = None,
) -> NodeidValidationContext:
    # Expose one explicit campaign-boundary constructor for selection callers.
    return NodeidValidationContext.from_index(
        index,
        project_root,
        collected_nodeids=collected_nodeids,
        collection_snapshot=collection_snapshot,
    )
def iter_python_files(root: Path) -> Iterator[Path]:
    root = root.resolve()
    pending = [root]
    while pending:
        directory = pending.pop()
        try:
            entries = list(os.scandir(directory))
        except OSError:
            continue
        for entry in sorted(entries, key=lambda item: item.name.lower(), reverse=True):
            if entry.is_dir(follow_symlinks=False):
                if entry.name not in EXCLUDED_DIRS and not entry.name.startswith("."):
                    pending.append(Path(entry.path))
            elif entry.is_file(follow_symlinks=False) and entry.name.endswith(".py"):
                yield Path(entry.path)
def _end_line(node: ast.AST) -> int:
    return int(getattr(node, "end_lineno", getattr(node, "lineno", 1)))
def _risk_score(node: ast.AST) -> float:
    branches = sum(isinstance(item, (ast.If, ast.For, ast.AsyncFor, ast.While, ast.Try, ast.Match, ast.BoolOp)) for item in ast.walk(node))
    calls = sum(isinstance(item, ast.Call) for item in ast.walk(node))
    return round(min(10.0, 1.0 + branches * 0.35 + calls * 0.05), 2)
class _DefinitionVisitor(ast.NodeVisitor):
    def __init__(self, rel_file: str) -> None:
        # Track lexical scopes so local definitions receive collision-free stable identities.
        self.rel_file = rel_file
        self.functions: list[FunctionInfo] = []
        self.tests: list[TestInfo] = []
        self._scopes: list[tuple[str, str, str]] = []
        self._scope_counts: dict[tuple[str, ...], int] = {}
        self._test_positions: dict[str, int] = {}
    def _unique_scope_name(self, name: str) -> str:
        # Disambiguate repeated definitions only within their exact lexical parent scope.
        key = (*tuple(item[1] for item in self._scopes), name)
        occurrence = self._scope_counts.get(key, 0) + 1
        self._scope_counts[key] = occurrence
        return name if occurrence == 1 else f"{name}#{occurrence}"
    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        # Include local and nested classes in descendant identities without indexing the class itself.
        scope_name = self._unique_scope_name(node.name)
        self._scopes.append(("class", scope_name, node.name))
        for child in node.body:
            self.visit(child)
        self._scopes.pop()
    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        # Build one identity from the complete lexical path and preserve pytest-compatible test node IDs.
        scope_name = self._unique_scope_name(node.name)
        scope_path = [item[1] for item in self._scopes]
        qualname = ".".join([*scope_path, scope_name])
        function_id = f"{self.rel_file}::{qualname}"
        class_name = next(
            (item[2] for item in reversed(self._scopes) if item[0] == "class"),
            None,
        )
        info = FunctionInfo(
            function_id=function_id,
            rel_path=self.rel_file,
            qualname=qualname,
            name=node.name,
            class_name=class_name,
            start_line=int(getattr(node, "lineno", 1)),
            end_line=_end_line(node),
            is_method=bool(self._scopes and self._scopes[-1][0] == "class"),
            is_async=isinstance(node, ast.AsyncFunctionDef),
            risk_score=_risk_score(node),
        )
        self.functions.append(info)
        inside_function = any(item[0] == "function" for item in self._scopes)
        if node.name.startswith("test_") and not inside_function:
            class_path = [item[2] for item in self._scopes if item[0] == "class"]
            nodeid = f"{self.rel_file}::" + "::".join([*class_path, node.name])
            test = TestInfo(
                nodeid=nodeid,
                rel_path=self.rel_file,
                name=node.name,
                class_name=class_name,
                start_line=info.start_line,
                end_line=info.end_line,
            )
            position = self._test_positions.get(nodeid)
            if position is None:
                self._test_positions[nodeid] = len(self.tests)
                self.tests.append(test)
            else:
                self.tests[position] = test
        self._scopes.append(("function", scope_name, node.name))
        for child in node.body:
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                self.visit(child)
        self._scopes.pop()
    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        # Index one synchronous function through the shared lexical-scope implementation.
        self._visit_function(node)
    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        # Index one asynchronous function through the shared lexical-scope implementation.
        self._visit_function(node)
def parse_python_file(
    path: Path,
    root: Path,
    *,
    include_dependencies: bool = False,
) -> tuple[list[FunctionInfo], list[TestInfo], str] | tuple[
    list[FunctionInfo], list[TestInfo], str, tuple[str, ...], tuple[str, ...], bool
]:
    # Parse one file once and optionally return dependency metadata for incremental intelligence.
    raw = path.read_bytes()
    text = raw.decode("utf-8-sig")
    tree = ast.parse(text, filename=str(path))
    visitor = _DefinitionVisitor(rel_path(path, root))
    visitor.visit(tree)
    digest = sha256_bytes(raw)
    if include_dependencies:
        dependencies, candidate_inputs, dynamic = _dependency_metadata(tree, path, root)
        return visitor.functions, visitor.tests, digest, dependencies, candidate_inputs, dynamic
    return visitor.functions, visitor.tests, digest
def _assemble_index(
    root: Path,
    files: Mapping[str, dict[str, Any]],
    *,
    reused_count: int = 0,
    created_at: str | None = None,
    index_version: str | None = None,
    stored_summary: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    # Rebuild the public index contract from normalized per-file entities.
    normalized_files = dict(sorted(files.items()))
    function_data = [
        item
        for entry in normalized_files.values()
        for item in entry.get("functions", [])
        if isinstance(item, dict)
    ]
    test_data = [
        item
        for entry in normalized_files.values()
        for item in entry.get("tests", [])
        if isinstance(item, dict)
    ]
    function_data.sort(
        key=lambda item: (
            str(item.get("rel_path", "")),
            int(item.get("start_line", 0)),
            str(item.get("qualname", "")),
        )
    )
    test_data.sort(
        key=lambda item: (
            str(item.get("rel_path", "")),
            int(item.get("start_line", 0)),
            str(item.get("nodeid", "")),
        )
    )
    parse_errors = [
        {"path": relative, "error": str(entry.get("parse_error", "parse error"))}
        for relative, entry in normalized_files.items()
        if entry.get("parse_status") == "error"
    ]
    computed_version = stable_hash(
        {"files": normalized_files, "functions": [item.get("function_id") for item in function_data]}
    )[:16]
    summary = {
        "files": len(normalized_files),
        "functions": len(function_data),
        "tests": len(test_data),
        "parse_errors": len(parse_errors),
        "reused_files": reused_count,
    }
    if stored_summary:
        summary["reused_files"] = int(stored_summary.get("reused_files", reused_count) or 0)
    return {
        "schema_version": INDEX_SCHEMA_VERSION,
        "index_version": index_version or computed_version,
        "created_at": created_at or utc_now_iso(),
        "project_root": str(root),
        "files": normalized_files,
        "functions": function_data,
        "tests": test_data,
        "parse_errors": parse_errors,
        "summary": summary,
    }
def _with_intelligence(
    index: dict[str, Any],
    *,
    changed_files: Iterable[str] = (),
    removed_files: Iterable[str] = (),
    previous_files: Mapping[str, Any] | None = None,
    reused_count: int | None = None,
) -> dict[str, Any]:
    # Rebuild dependency and test-to-code projections from cached per-file facts only.
    files = {
        str(relative): dict(entry)
        for relative, entry in index.get("files", {}).items()
        if isinstance(entry, Mapping)
    }
    normalized_files = set(files)
    forward = {
        relative: sorted(
            dependency
            for dependency in entry.get("dependencies", [])
            if str(dependency) in normalized_files
        )
        for relative, entry in sorted(files.items())
    }
    reverse: dict[str, list[str]] = {relative: [] for relative in sorted(files)}
    for relative, dependencies in forward.items():
        for dependency in dependencies:
            reverse.setdefault(dependency, []).append(relative)
    reverse = {relative: sorted(set(values)) for relative, values in sorted(reverse.items())}
    source_files = sorted(relative for relative, entry in files.items() if entry.get("kind") == "source")
    test_files = sorted(relative for relative, entry in files.items() if entry.get("kind") == "test")
    reachable_cache: dict[str, tuple[str, ...]] = {}

    def reachable(start: str) -> tuple[str, ...]:
        if start in reachable_cache:
            return reachable_cache[start]
        visited: set[str] = set()
        pending = [start]
        while pending:
            current = pending.pop()
            if current in visited:
                continue
            visited.add(current)
            pending.extend(forward.get(current, ()))
        result = tuple(sorted(visited))
        reachable_cache[start] = result
        return result

    test_to_code: dict[str, list[str]] = {}
    test_candidate_inputs: dict[str, list[str]] = {}
    for relative in test_files:
        closure = reachable(relative)
        code_files = tuple(item for item in closure if item in source_files)
        direct_inputs = set(str(item) for item in files[relative].get("candidate_inputs", ()))
        for dependency in closure:
            direct_inputs.update(str(item) for item in files[dependency].get("candidate_inputs", ()))
        for raw_test in files[relative].get("tests", ()):
            if not isinstance(raw_test, Mapping) or not raw_test.get("nodeid"):
                continue
            nodeid = str(raw_test["nodeid"])
            test_to_code[nodeid] = list(code_files)
            test_candidate_inputs[nodeid] = sorted(direct_inputs)
    source_to_tests: dict[str, list[str]] = {relative: [] for relative in source_files}
    for nodeid, code_files in test_to_code.items():
        for relative in code_files:
            source_to_tests.setdefault(relative, []).append(nodeid)
    source_to_tests = {
        relative: sorted(set(values))
        for relative, values in sorted(source_to_tests.items())
    }
    changed = tuple(sorted(set(str(item) for item in changed_files if str(item))))
    removed = tuple(sorted(set(str(item) for item in removed_files if str(item))))
    reverse_closure: set[str] = set(changed)
    pending = list(changed)
    while pending:
        current = pending.pop()
        for dependent in reverse.get(current, ()):
            if dependent not in reverse_closure:
                reverse_closure.add(dependent)
                pending.append(dependent)
    changed_source = tuple(item for item in changed if item in source_files)
    changed_test = tuple(item for item in changed if item in test_files)
    invalidated_tests = sorted(
        nodeid
        for nodeid, code_files in test_to_code.items()
        if set(code_files) & reverse_closure
        or nodeid.split("::", 1)[0] in reverse_closure
    )
    previous = previous_files or {}
    digest_to_removed = {
        str(entry.get("sha256")): relative
        for relative, entry in previous.items()
        if relative in removed and isinstance(entry, Mapping) and entry.get("sha256")
    }
    renames = [
        {"from": digest_to_removed[entry["sha256"]], "to": relative}
        for relative, entry in sorted(files.items())
        if entry.get("sha256") in digest_to_removed
        and digest_to_removed[entry["sha256"]] != relative
    ]
    project_rows = [
        (relative, str(entry.get("sha256", "")))
        for relative, entry in sorted(files.items())
    ]
    source_rows = [row for row in project_rows if row[0] in source_files]
    test_rows = [row for row in project_rows if row[0] in test_files]
    semantic_payload = {
        "files": {
            relative: {
                key: entry.get(key)
                for key in ("sha256", "parse_status", "parse_error", "kind", "functions", "tests", "dependencies", "candidate_inputs", "dynamic_dependencies")
            }
            for relative, entry in sorted(files.items())
        },
        "dependency_graph": {"forward": forward, "reverse": reverse},
        "test_to_code": test_to_code,
        "test_candidate_inputs": test_candidate_inputs,
    }
    semantic_fingerprint = stable_hash(semantic_payload)
    summary = dict(index.get("summary", {}))
    summary.update(
        {
            "reused_files": int(reused_count if reused_count is not None else summary.get("reused_files", 0) or 0),
            "parsed_files": len(changed),
            "removed_files": len(removed),
            "invalidated_files": len(reverse_closure),
            "invalidated_tests": len(invalidated_tests),
        }
    )
    index["summary"] = summary
    index["index_version"] = semantic_fingerprint[:16]
    index["project_fingerprint"] = stable_hash(project_rows)
    index["source_fingerprint"] = stable_hash(source_rows)
    index["test_fingerprint"] = stable_hash(test_rows)
    index["dependency_graph"] = {"forward": forward, "reverse": reverse}
    index["test_to_code"] = test_to_code
    index["test_candidate_inputs"] = test_candidate_inputs
    index["source_to_tests"] = source_to_tests
    index["intelligence"] = {
        "version": INTELLIGENCE_VERSION,
        "status": "loaded" if previous_files is None else "incremental" if previous else "clean",
        "project_fingerprint": index["project_fingerprint"],
        "source_fingerprint": index["source_fingerprint"],
        "test_fingerprint": index["test_fingerprint"],
        "semantic_fingerprint": semantic_fingerprint,
        "changed_files": list(changed),
        "changed_source_files": list(changed_source),
        "changed_test_files": list(changed_test),
        "removed_files": list(removed),
        "invalidated_files": sorted(reverse_closure),
        "invalidated_tests": invalidated_tests,
        "renames": renames,
        "reused_files": int(summary["reused_files"]),
        "parsed_files": len(changed),
        "dynamic_dependency_files": sorted(
            relative for relative, entry in files.items() if entry.get("dynamic_dependencies")
        ),
    }
    return index
def _read_sqlite_v3_state(
    path: Path,
    *,
    expected_root: Path | None = None,
) -> dict[str, Any] | None:
    # Read only normalized v3 rows so warm builds never materialize a JSON payload.
    uri = f"file:{path.resolve().as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    try:
        metadata = {
            str(row["key"]): str(row["value"])
            for row in connection.execute("SELECT key, value FROM metadata")
        }
        if metadata.get("schema_version") != str(INDEX_SCHEMA_VERSION):
            return None
        if expected_root is not None and Path(metadata.get("project_root", "")).resolve() != expected_root.resolve():
            return None
        files: dict[str, dict[str, Any]] = {}
        try:
            rows = connection.execute(
                "SELECT rel_path, size, mtime_ns, sha256, parse_status, parse_error, dependencies_json "
                "FROM files ORDER BY rel_path"
            )
            has_dependency_column = True
        except sqlite3.OperationalError:
            rows = connection.execute(
                "SELECT rel_path, size, mtime_ns, sha256, parse_status, parse_error FROM files ORDER BY rel_path"
            )
            has_dependency_column = False
        for row in rows:
            relative = str(row["rel_path"])
            raw_metadata: object = row["dependencies_json"] if has_dependency_column else "{}"
            try:
                dependency_metadata = json.loads(str(raw_metadata))
            except (TypeError, ValueError):
                dependency_metadata = {}
            if isinstance(dependency_metadata, list):
                dependency_metadata = {"dependencies": dependency_metadata}
            if not isinstance(dependency_metadata, Mapping):
                dependency_metadata = {}
            files[relative] = {
                "sha256": str(row["sha256"]),
                "size": int(row["size"]),
                "mtime_ns": int(row["mtime_ns"]),
                "parse_status": str(row["parse_status"]),
                "parse_error": row["parse_error"],
                "kind": "test" if _is_test_path(relative) else "source",
                "dependencies": [str(item) for item in dependency_metadata.get("dependencies", ())],
                "candidate_inputs": [str(item) for item in dependency_metadata.get("candidate_inputs", ())],
                "dynamic_dependencies": bool(dependency_metadata.get("dynamic", False)),
                "functions": [],
                "tests": [],
            }
        functions_by_file: dict[str, list[dict[str, Any]]] = {}
        for row in connection.execute(
            "SELECT function_id, rel_path, qualname, name, class_name, start_line, end_line, "
            "is_method, is_async, risk_score FROM functions ORDER BY rel_path, start_line, qualname"
        ):
            functions_by_file.setdefault(str(row["rel_path"]), []).append(
                {
                    "function_id": str(row["function_id"]),
                    "rel_path": str(row["rel_path"]),
                    "qualname": str(row["qualname"]),
                    "name": str(row["name"]),
                    "class_name": row["class_name"],
                    "start_line": int(row["start_line"]),
                    "end_line": int(row["end_line"]),
                    "is_method": bool(row["is_method"]),
                    "is_async": bool(row["is_async"]),
                    "risk_score": float(row["risk_score"]),
                }
            )
        tests_by_file: dict[str, list[dict[str, Any]]] = {}
        for row in connection.execute(
            "SELECT nodeid, rel_path, name, class_name, start_line, end_line "
            "FROM tests ORDER BY rel_path, start_line, nodeid"
        ):
            tests_by_file.setdefault(str(row["rel_path"]), []).append(
                {
                    "nodeid": str(row["nodeid"]),
                    "rel_path": str(row["rel_path"]),
                    "name": str(row["name"]),
                    "class_name": row["class_name"],
                    "start_line": int(row["start_line"]),
                    "end_line": int(row["end_line"]),
                }
            )
        for relative, entry in files.items():
            entry["functions"] = functions_by_file.get(relative, [])
            entry["tests"] = tests_by_file.get(relative, [])
        return {"metadata": metadata, "files": files}
    finally:
        connection.close()
def build_index(project_root: Path, output: Path | None = None) -> dict[str, Any]:
    # Incrementally parse changed Python files and update only affected SQLite rows.
    root = project_root.resolve()
    previous_files: dict[str, dict[str, Any]] = {}
    incremental_sqlite = False
    if output and output.exists() and output.suffix.lower() == ".sqlite":
        try:
            state = _read_sqlite_v3_state(output, expected_root=root)
        except (OSError, sqlite3.DatabaseError, ValueError):
            state = None
        if state is not None:
            previous_files = state["files"]
            sidecar_files = _load_intelligence_sidecar(output, expected_root=root)
            for relative, metadata in sidecar_files.items():
                if relative in previous_files:
                    previous_files[relative].update(metadata)
            incremental_sqlite = True
        else:
            try:
                candidate = load_index(output)
                if Path(str(candidate.get("project_root", ""))).resolve() == root:
                    previous_files = candidate.get("files", {}) if isinstance(candidate.get("files", {}), dict) else {}
            except (OSError, ValueError, sqlite3.DatabaseError):
                previous_files = {}
    elif output and output.exists():
        try:
            candidate = load_index(output)
            if Path(str(candidate.get("project_root", ""))).resolve() == root:
                previous_files = candidate.get("files", {}) if isinstance(candidate.get("files", {}), dict) else {}
        except (OSError, ValueError, sqlite3.DatabaseError):
            previous_files = {}
    files: dict[str, dict[str, Any]] = {}
    changed_files: set[str] = set()
    current_paths = sorted(iter_python_files(root), key=lambda item: rel_path(item, root))
    current_rel_paths = {rel_path(path, root) for path in current_paths}
    reused_count = 0
    for path in current_paths:
        relative = rel_path(path, root)
        stat = path.stat()
        try:
            current_digest = sha256_file(path)
        except OSError:
            current_digest = ""
        old = previous_files.get(relative, {})
        can_reuse = (
            isinstance(old, dict)
            and old.get("size") == stat.st_size
            and old.get("mtime_ns") == stat.st_mtime_ns
            and old.get("sha256") == current_digest
            and isinstance(old.get("functions"), list)
            and isinstance(old.get("tests"), list)
            and isinstance(old.get("dependencies"), (list, tuple))
        )
        if can_reuse:
            reused_count += 1
            files[relative] = dict(old)
            continue
        changed_files.add(relative)
        try:
            parsed = parse_python_file(path, root, include_dependencies=True)
            file_functions, file_tests, digest, dependencies, candidate_inputs, dynamic = parsed
        except (OSError, SyntaxError, UnicodeError) as exc:
            files[relative] = {
                "sha256": current_digest,
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
                "parse_status": "error",
                "parse_error": str(exc),
                "kind": "test" if _is_test_path(relative) else "source",
                "dependencies": [],
                "candidate_inputs": [],
                "dynamic_dependencies": False,
                "functions": [],
                "tests": [],
            }
            continue
        files[relative] = {
            "sha256": digest,
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            "parse_status": "ok",
            "parse_error": None,
            "kind": "test" if _is_test_path(relative) else "source",
            "dependencies": list(dependencies),
            "candidate_inputs": list(candidate_inputs),
            "dynamic_dependencies": bool(dynamic),
            "functions": [item.to_dict() for item in file_functions],
            "tests": [item.to_dict() for item in file_tests],
        }
    removed_files = set(previous_files).difference(current_rel_paths)
    index = _assemble_index(root, files, reused_count=reused_count)
    index = _with_intelligence(
        index,
        changed_files=changed_files,
        removed_files=removed_files,
        previous_files=previous_files,
        reused_count=reused_count,
    )
    if output:
        if output.suffix.lower() == ".sqlite":
            _write_sqlite_index(
                output,
                index,
                changed_files=changed_files,
                removed_files=removed_files,
                incremental=incremental_sqlite,
            )
            _write_intelligence_sidecar(output, index)
        else:
            atomic_write_json(output, index)
    return index
def _create_sqlite_v3_schema(connection: sqlite3.Connection) -> None:
    # Create normalized tables without storing duplicate per-file JSON payloads.
    connection.executescript(
        """
        CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE files (
            rel_path TEXT PRIMARY KEY,
            size INTEGER NOT NULL,
            mtime_ns INTEGER NOT NULL,
            sha256 TEXT NOT NULL,
            parse_status TEXT NOT NULL,
            parse_error TEXT
        );
        CREATE TABLE functions (
            function_id TEXT PRIMARY KEY,
            rel_path TEXT NOT NULL,
            qualname TEXT NOT NULL,
            name TEXT NOT NULL,
            class_name TEXT,
            start_line INTEGER NOT NULL,
            end_line INTEGER NOT NULL,
            is_method INTEGER NOT NULL,
            is_async INTEGER NOT NULL,
            risk_score REAL NOT NULL
        );
        CREATE TABLE tests (
            nodeid TEXT PRIMARY KEY,
            rel_path TEXT NOT NULL,
            name TEXT NOT NULL,
            class_name TEXT,
            start_line INTEGER NOT NULL,
            end_line INTEGER NOT NULL
        );
        CREATE INDEX ix_functions_file ON functions(rel_path);
        CREATE INDEX ix_tests_file ON tests(rel_path);
        """
    )
def _write_sqlite_metadata(connection: sqlite3.Connection, index: dict[str, Any]) -> None:
    # Store only scalar index metadata and compact diagnostics in the v3 catalog.
    values = {
        "schema_version": str(INDEX_SCHEMA_VERSION),
        "project_root": str(index["project_root"]),
        "index_version": str(index["index_version"]),
        "created_at": str(index["created_at"]),
        "summary": json.dumps(index.get("summary", {}), ensure_ascii=False, sort_keys=True),
        "parse_errors": json.dumps(index.get("parse_errors", []), ensure_ascii=False, sort_keys=True),
    }
    connection.executemany(
        "INSERT INTO metadata(key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        sorted(values.items()),
    )
def _intelligence_sidecar_path(path: Path) -> Path:
    # Keep the frozen v3 SQLite schema intact while persisting the richer intelligence cache.
    return path.with_name(f"{path.name}.intelligence.json")


def _load_intelligence_sidecar(
    path: Path,
    *,
    expected_root: Path | None = None,
) -> dict[str, dict[str, Any]]:
    sidecar = _intelligence_sidecar_path(path)
    if not sidecar.exists():
        return {}
    try:
        payload = json.loads(sidecar.read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeError):
        return {}
    if not isinstance(payload, Mapping) or payload.get("version") != INTELLIGENCE_VERSION:
        return {}
    if expected_root is not None and Path(str(payload.get("project_root", ""))).resolve() != expected_root.resolve():
        return {}
    raw_files = payload.get("files", {})
    if not isinstance(raw_files, Mapping):
        return {}
    return {
        str(relative): dict(metadata)
        for relative, metadata in raw_files.items()
        if isinstance(metadata, Mapping)
    }


def _write_intelligence_sidecar(path: Path, index: Mapping[str, Any]) -> None:
    # Persist only facts needed to avoid reparsing unchanged files on the next SQLite build.
    files = {
        str(relative): {
            "sha256": str(entry.get("sha256", "")),
            "size": int(entry.get("size", 0)),
            "mtime_ns": int(entry.get("mtime_ns", 0)),
            "kind": str(entry.get("kind", "source")),
            "dependencies": [str(item) for item in entry.get("dependencies", ())],
            "candidate_inputs": [str(item) for item in entry.get("candidate_inputs", ())],
            "dynamic_dependencies": bool(entry.get("dynamic_dependencies", False)),
        }
        for relative, entry in index.get("files", {}).items()
        if isinstance(entry, Mapping)
    }
    atomic_write_json(
        _intelligence_sidecar_path(path),
        {
            "version": INTELLIGENCE_VERSION,
            "project_root": str(index.get("project_root", "")),
            "files": files,
        },
        durability="normal",
        category="index",
    )
def _write_sqlite_index(
    path: Path,
    index: dict[str, Any],
    *,
    changed_files: Iterable[str] = (),
    removed_files: Iterable[str] = (),
    incremental: bool = False,
) -> None:
    # Apply a transactional row-level update or atomically create a fresh v3 catalog.
    path.parent.mkdir(parents=True, exist_ok=True)
    changed = tuple(sorted(set(changed_files)))
    removed = tuple(sorted(set(removed_files)))
    if incremental and path.exists():
        connection = sqlite3.connect(path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            for relative in removed:
                connection.execute("DELETE FROM functions WHERE rel_path = ?", (relative,))
                connection.execute("DELETE FROM tests WHERE rel_path = ?", (relative,))
                connection.execute("DELETE FROM files WHERE rel_path = ?", (relative,))
            for relative in changed:
                entry = index["files"][relative]
                connection.execute("DELETE FROM functions WHERE rel_path = ?", (relative,))
                connection.execute("DELETE FROM tests WHERE rel_path = ?", (relative,))
                connection.execute("DELETE FROM files WHERE rel_path = ?", (relative,))
                connection.execute(
                    "INSERT INTO files(rel_path, size, mtime_ns, sha256, parse_status, parse_error) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        relative,
                        int(entry.get("size", 0)),
                        int(entry.get("mtime_ns", 0)),
                        str(entry.get("sha256", "")),
                        str(entry.get("parse_status", "ok")),
                        entry.get("parse_error"),
                    ),
                )
                connection.executemany(
                    "INSERT INTO functions(function_id, rel_path, qualname, name, class_name, start_line, "
                    "end_line, is_method, is_async, risk_score) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (
                            item["function_id"],
                            item["rel_path"],
                            item["qualname"],
                            item["name"],
                            item.get("class_name"),
                            int(item["start_line"]),
                            int(item["end_line"]),
                            int(bool(item.get("is_method"))),
                            int(bool(item.get("is_async"))),
                            float(item.get("risk_score", 0.0)),
                        )
                        for item in entry.get("functions", [])
                    ],
                )
                connection.executemany(
                    "INSERT INTO tests(nodeid, rel_path, name, class_name, start_line, end_line) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    [
                        (
                            item["nodeid"],
                            item["rel_path"],
                            item["name"],
                            item.get("class_name"),
                            int(item["start_line"]),
                            int(item["end_line"]),
                        )
                        for item in entry.get("tests", [])
                    ],
                )
            _write_sqlite_metadata(connection, index)
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
        return
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(fd)
    temp_path = Path(temp_name)
    connection: sqlite3.Connection | None = None
    committed = False
    try:
        connection = sqlite3.connect(temp_path)
        _create_sqlite_v3_schema(connection)
        connection.execute("BEGIN")
        _write_sqlite_metadata(connection, index)
        for relative, entry in index.get("files", {}).items():
            connection.execute(
                "INSERT INTO files(rel_path, size, mtime_ns, sha256, parse_status, parse_error) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    relative,
                    int(entry.get("size", 0)),
                    int(entry.get("mtime_ns", 0)),
                    str(entry.get("sha256", "")),
                    str(entry.get("parse_status", "ok")),
                    entry.get("parse_error"),
                ),
            )
            connection.executemany(
                "INSERT INTO functions(function_id, rel_path, qualname, name, class_name, start_line, "
                "end_line, is_method, is_async, risk_score) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        item["function_id"],
                        item["rel_path"],
                        item["qualname"],
                        item["name"],
                        item.get("class_name"),
                        int(item["start_line"]),
                        int(item["end_line"]),
                        int(bool(item.get("is_method"))),
                        int(bool(item.get("is_async"))),
                        float(item.get("risk_score", 0.0)),
                    )
                    for item in entry.get("functions", [])
                ],
            )
            connection.executemany(
                "INSERT INTO tests(nodeid, rel_path, name, class_name, start_line, end_line) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (
                        item["nodeid"],
                        item["rel_path"],
                        item["name"],
                        item.get("class_name"),
                        int(item["start_line"]),
                        int(item["end_line"]),
                    )
                    for item in entry.get("tests", [])
                ],
            )
        connection.commit()
        committed = True
    finally:
        if connection is not None:
            connection.close()
        if temp_path.exists():
            if committed:
                os.replace(temp_path, path)
            else:
                temp_path.unlink()
def load_index(path: Path) -> dict[str, Any]:
    # Load v3 from normalized rows while retaining compatibility with v2 payload indexes.
    if path.suffix.lower() == ".sqlite":
        state = _read_sqlite_v3_state(path)
        if state is not None:
            metadata = state["metadata"]
            try:
                stored_summary = json.loads(metadata.get("summary", "{}"))
            except ValueError:
                stored_summary = {}
            project_root = Path(metadata.get("project_root", "")).resolve()
            for relative, sidecar_entry in _load_intelligence_sidecar(path, expected_root=project_root).items():
                if relative in state["files"]:
                    state["files"][relative].update(sidecar_entry)
            assembled = _assemble_index(
                project_root,
                state["files"],
                created_at=metadata.get("created_at"),
                index_version=metadata.get("index_version"),
                stored_summary=stored_summary if isinstance(stored_summary, dict) else None,
            )
            return _with_intelligence(
                assembled,
                reused_count=(
                    int(stored_summary.get("reused_files", 0) or 0)
                    if isinstance(stored_summary, dict)
                    else None
                ),
            )
        uri = f"file:{path.resolve().as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        try:
            row = connection.execute("SELECT value FROM metadata WHERE key = 'payload'").fetchone()
            if row is None:
                raise ValueError(f"SQLite index has no compatible payload: {path}")
            value = json.loads(str(row[0]))
            if not isinstance(value, dict):
                raise ValueError(f"SQLite index payload is not an object: {path}")
            return value
        finally:
            connection.close()
    return json.loads(path.read_text(encoding="utf-8"))
def find_function(index: dict[str, Any], source_path: str, function: str | None) -> dict[str, Any]:
    source = source_path.replace("\\", "/").lstrip("./")
    candidates = [item for item in index.get("functions", []) if item.get("rel_path", "").replace("\\", "/") == source]
    if function:
        exact = [item for item in candidates if item.get("qualname") == function or item.get("name") == function or item.get("function_id") == function]
        candidates = exact or [item for item in candidates if function in str(item.get("qualname", ""))]
    if not candidates:
        raise LookupError(f"function not found: {source_path}::{function or '*'}")
    if len(candidates) > 1:
        choices = ", ".join(str(item["function_id"]) for item in candidates[:12])
        raise LookupError(f"function is ambiguous ({len(candidates)} matches): {choices}")
    return candidates[0]
def load_context_map(path: Path | None) -> dict[str, Any] | None:
    if not path or not path.exists():
        return None
    import json
    return json.loads(path.read_text(encoding="utf-8"))
def _context_tests(context_map: dict[str, Any] | None, function_id: str, source_path: str) -> list[tuple[str, str]]:
    if not context_map:
        return []
    mapping = context_map.get("function_to_tests", {})
    keys = [function_id, source_path, source_path.replace("\\", "/")]
    entry: Any = None
    for key in keys:
        if key in mapping:
            entry = mapping[key]
            break
    if entry is None:
        for key, value in mapping.items():
            if str(key).replace("\\", "/").endswith(function_id.replace("\\", "/")) or str(key).endswith(function_id.rsplit("::", 1)[-1]):
                entry = value
                break
    if not isinstance(entry, dict):
        return []
    tests = entry.get("tests", entry)
    if not isinstance(tests, dict):
        return []
    return [(str(nodeid), "context-map") for nodeid in tests]
def _load_selected_tests(path: Path | None) -> list[str]:
    if not path or not path.exists():
        return []
    try:
        import json
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeDecodeError, ValueError):
        return [line.strip() for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip() and not line.startswith("#")]
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, dict):
        selected = value.get("selected_tests")
        if isinstance(selected, list):
            return [str(item) for item in selected]
        levels = value.get("levels", [])
        if levels and isinstance(levels[0], dict):
            return [str(item) for item in levels[0].get("nodeids", [])]
    return []
def _static_related_tests(index: dict[str, Any], source_path: str, function_name: str | None) -> list[tuple[str, str]]:
    source_stem = Path(source_path).stem.lower()
    function_name = (function_name or "").lower()
    result: list[tuple[str, str]] = []
    for item in index.get("tests", []):
        rel = str(item.get("rel_path", ""))
        stem = Path(rel).stem.lower()
        name = str(item.get("name", "")).lower()
        if stem in {f"test_{source_stem}", f"{source_stem}_test"}:
            result.append((str(item["nodeid"]), "static-name"))
        elif function_name and function_name in name:
            result.append((str(item["nodeid"]), "static-function-name"))
    return result


def _static_dependency_tests(index: Mapping[str, Any], source_path: str) -> list[tuple[str, str]]:
    # Use the incremental source-to-tests projection as bounded static dependency evidence.
    normalized = source_path.replace("\\", "/").lstrip("./")
    mapping = index.get("source_to_tests", {})
    if not isinstance(mapping, Mapping):
        return []
    values = mapping.get(normalized, ())
    if not isinstance(values, (list, tuple, set, frozenset)):
        return []
    return [(str(nodeid), "static-dependency") for nodeid in values if str(nodeid)]
def _unique_pairs(values: Iterable[tuple[str, str]]) -> tuple[tuple[str, str], ...]:
    seen: set[str] = set()
    result: list[tuple[str, str]] = []
    for nodeid, reason in values:
        if nodeid and nodeid not in seen:
            seen.add(nodeid)
            result.append((nodeid, reason))
    return tuple(result)
def _selection_score(nodeid: str, reason: str, stats: dict[str, Any] | None = None) -> float:
    # Calculate a stable ordering score while demoting historically unhealthy tests.
    source_weight = {
        "selected-tests-file": 100.0,
        "frozen-selection": 100.0,
        "sqlite:impact_links:line": 90.0,
        "sqlite:impact_links": 85.0,
        "sqlite:impact_links:function": 85.0,
        "context-map": 70.0,
        "static-name": 25.0,
        "static-function-name": 20.0,
    }.get(reason, 10.0)
    if not stats:
        return source_weight
    executions = max(
        0,
        int(stats.get("executions", 0) or 0),
        int(stats.get("test_executions", 0) or 0),
    )
    kills = max(0, int(stats.get("kills", 0) or 0), int(stats.get("test_kills", 0) or 0))
    median_ms = stats.get("test_median_ms", stats.get("median_ms"))
    kill_rate = kills / executions if executions else 0.0
    runtime_penalty = min(20.0, float(median_ms) / 100.0) if median_ms is not None else 0.0
    health_failure_rate = max(0.0, min(1.0, float(stats.get("test_health_failure_rate", 0.0) or 0.0)))
    health_penalty = min(25.0, health_failure_rate * 20.0)
    health_status = str(stats.get("test_health_status", ""))
    if health_status == "flaky":
        health_penalty += 8.0
    elif health_status == "failing":
        health_penalty += 12.0
    return source_weight + min(15.0, executions ** 0.5) + kill_rate * 25.0 - runtime_penalty - health_penalty
def _rank_pairs(
    pairs: Iterable[tuple[str, str]],
    *,
    impact_rows: Iterable[dict[str, Any]] = (),
    test_stats: Mapping[str, dict[str, Any]] | None = None,
) -> tuple[tuple[str, str], ...]:
    # Rank candidates deterministically using impact statistics and source priority.
    stats = {
        str(row.get("nodeid")): row
        for row in impact_rows
        if isinstance(row, dict) and row.get("nodeid")
    }
    for nodeid, row in (test_stats or {}).items():
        stats[str(nodeid)] = {**stats.get(str(nodeid), {}), **row}
    unique = _unique_pairs(pairs)
    return tuple(sorted(unique, key=lambda item: (-_selection_score(item[0], item[1], stats.get(item[0])), item[0])))
def validate_test_nodeids(
    index: dict[str, Any],
    project_root: Path,
    nodeids: Iterable[str],
    *,
    context: NodeidValidationContext | None = None,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    # Keep only nodeids with O(1) lookup against one immutable campaign inventory.
    validation_context = context or build_nodeid_validation_context(index, project_root)
    valid: list[str] = []
    dropped: list[str] = []
    for value in nodeids:
        nodeid = str(value)
        base = _nodeid_base(nodeid)
        rel_file = base.split("::", 1)[0].replace("\\", "/").lstrip("./")
        exact_match = (
            nodeid in validation_context.known_collected_nodeids
            if validation_context.authoritative
            else base in validation_context.known_bases
        )
        if rel_file in validation_context.existing_files and exact_match:
            if nodeid not in valid:
                valid.append(nodeid)
        elif nodeid not in dropped:
            dropped.append(nodeid)
    return tuple(valid), tuple(dropped)
def plan_selection(
    project_root: Path,
    index: dict[str, Any],
    source_path: str,
    function: str | None = None,
    *,
    context_map: dict[str, Any] | None = None,
    impact_db: Path | None = None,
    selected_tests_file: Path | None = None,
    map_version: str | None = None,
    test_stats_db: Path | None = None,
    validation_context: NodeidValidationContext | None = None,
) -> SelectionSnapshot:
    # Build a frozen, validated cascade of the narrowest available test source.
    root = project_root.resolve()
    context_for_validation = validation_context or build_nodeid_validation_context(index, root)
    source = source_path.replace("\\", "/").lstrip("./")
    function_info = find_function(index, source, function) if function else None
    function_id = function_info.get("function_id") if function_info else None
    function_name = function_info.get("name") if function_info else function
    explicit = [(item, "selected-tests-file") for item in _load_selected_tests(selected_tests_file)]
    impact: list[tuple[str, str]] = []
    impact_rows: tuple[dict[str, Any], ...] = ()
    impact_status = "missing"
    impact_schema_version: int | None = None
    impact_warning: str | None = None
    impact_error: str | None = None
    if impact_db:
        adapter = SQLiteImpactAdapter(impact_db)
        impact_rows = adapter.select_tests(source, function_id)
        impact = [(item["nodeid"], item["reason"]) for item in impact_rows]
        diagnostics = adapter.diagnostics
        impact_status = str(diagnostics.get("status", "missing"))
        impact_schema_version = diagnostics.get("schema_version")
        impact_warning = diagnostics.get("warning")
        impact_error = diagnostics.get("error")
    context = _context_tests(context_map, function_id or source, source)
    static = _static_related_tests(index, source, function_name)
    static_dependency = _static_dependency_tests(index, source)
    dynamic_graph_incomplete = bool(impact_db) and (impact_status != "ok" or not impact_rows)
    static_allowed = not dynamic_graph_incomplete or bool(context)
    source_groups = (
        ("explicit", explicit),
        ("impact", impact),
        ("context", context),
        ("static", static if static_allowed else []),
        ("static-dependency", static_dependency if static_allowed else []),
    )
    selected_group_name = "none"
    selected_group: list[tuple[str, str]] = []
    dropped: list[str] = []
    for name, values in source_groups:
        if not values:
            continue
        valid, invalid = validate_test_nodeids(
            index,
            root,
            (item[0] for item in values),
            context=context_for_validation,
        )
        dropped.extend(invalid)
        if valid:
            selected_group_name = name
            selected_group = [(nodeid, reason) for nodeid, reason in values if nodeid in valid]
            break
    historical_stats = load_selection_stats(test_stats_db, root) if test_stats_db else {}
    merged = _rank_pairs(selected_group, impact_rows=impact_rows, test_stats=historical_stats)
    nodeids = tuple(item[0] for item in merged)
    reasons: dict[str, list[str]] = {}
    selected_nodeids = set(nodeids)
    for nodeid, reason in [*explicit, *impact, *context, *static, *static_dependency]:
        if reason == "static-dependency" and selected_group_name != "static-dependency":
            continue
        if nodeid not in selected_nodeids:
            continue
        reasons.setdefault(nodeid, [])
        if reason not in reasons[nodeid]:
            reasons[nodeid].append(reason)
    files = tuple(dict.fromkeys(nodeid.split("::", 1)[0] for nodeid in nodeids))
    level_reasons = tuple(dict.fromkeys(reason for values in reasons.values() for reason in values))
    level = SelectionLevel(
        name="L1",
        reason=";".join(level_reasons) or f"no-selected-tests:{selected_group_name}",
        nodeids=nodeids,
        files=files,
    )
    source_file = root / source
    source_digest = sha256_file(source_file)
    index_version = str(index.get("index_version", "unknown"))
    version = map_version or f"{SELECTION_ALGORITHM_VERSION}:{index_version}"
    snapshot_id = stable_hash({"source": source, "function_id": function_id, "sha256": source_digest, "tests": nodeids, "map_version": version})[:20]
    revision = str(index.get("revision") or index_version)
    environment = stable_hash(
        {
            "project_root": str(root),
            "index_version": index_version,
            "impact_status": impact_status,
            "context_map": bool(context_map),
        }
    )[:20]
    impact_by_node = {
        str(row.get("nodeid")): row
        for row in impact_rows
        if isinstance(row, dict) and row.get("nodeid")
    }
    selected_group_reasons: dict[str, list[str]] = {}
    for candidate, reason in selected_group:
        if candidate in selected_nodeids:
            selected_group_reasons.setdefault(candidate, []).append(reason)
    dependency_nodeids = {nodeid for nodeid, _ in static_dependency} if selected_group_name == "static-dependency" else set()
    evidence: dict[str, tuple[Any, ...]] = {}
    for nodeid in nodeids:
        rows = [
            make_selection_evidence(
                reason,
                source_snapshot=snapshot_id,
                revision=revision,
                environment=environment,
                detail=reason,
            )
            for reason in selected_group_reasons.get(nodeid, reasons.get(nodeid, []))
        ]
        if nodeid in dependency_nodeids and "static-dependency" not in selected_group_reasons.get(nodeid, ()):
            rows.append(
                make_selection_evidence(
                    "static-dependency",
                    source_snapshot=snapshot_id,
                    revision=revision,
                    environment=environment,
                    detail="source_to_tests",
                )
            )
        historical = historical_stats.get(nodeid, {})
        historical_kills = max(
            int(historical.get("kills", 0) or 0),
            int(historical.get("test_kills", 0) or 0),
        ) if isinstance(historical, dict) else 0
        impact_kills = int(impact_by_node.get(nodeid, {}).get("kills", 0) or 0)
        if historical_kills or impact_kills:
            rows.append(
                make_selection_evidence(
                    "historical-kill",
                    source_snapshot=snapshot_id,
                    revision=revision,
                    environment=environment,
                    detail=f"kills={max(historical_kills, impact_kills)}",
                )
            )
        evidence[nodeid] = tuple(dict.fromkeys(rows))
    if dynamic_graph_incomplete and not context:
        evidence["__selection__"] = (
            make_selection_evidence(
                "domain-fallback",
                source_snapshot=snapshot_id,
                revision=revision,
                environment=environment,
                detail=impact_warning or impact_error or "dynamic impact graph is incomplete",
            ),
        )
    return SelectionSnapshot(
        schema_version=3,
        snapshot_id=snapshot_id,
        created_at=utc_now_iso(),
        project_root=str(root),
        source_path=source,
        function_id=function_id,
        source_sha256=source_digest,
        map_version=version,
        levels=(level,),
        selected_tests=nodeids,
        reasons=reasons,
        impact_status=impact_status,
        impact_schema_version=impact_schema_version,
        impact_warning=impact_warning,
        impact_error=impact_error,
        algorithm_version=SELECTION_ALGORITHM_VERSION,
        index_version=index_version,
        dropped_nodeids=tuple(dict.fromkeys(dropped)),
        evidence=evidence,
    )
def add_domain_levels(snapshot: SelectionSnapshot, domain: str | None) -> SelectionSnapshot:
    # Add broad escalation levels while preserving frozen L1 metadata.
    domain_files: tuple[str, ...] = (domain,) if domain else ("tests",)
    levels = list(snapshot.levels)
    levels.append(SelectionLevel(name="L2", reason="domain", files=domain_files))
    levels.append(SelectionLevel(name="L3", reason="common", files=("tests",)))
    evidence = dict(snapshot.evidence)
    metadata = next(
        (row for rows in evidence.values() for row in rows if rows),
        None,
    )
    source_snapshot = metadata.source_snapshot if metadata is not None else snapshot.snapshot_id
    revision = metadata.revision if metadata is not None else (snapshot.index_version or "")
    environment = metadata.environment if metadata is not None else ""
    evidence.setdefault(
        "__level__:L2",
        (
            make_selection_evidence(
                "domain-fallback",
                source_snapshot=source_snapshot,
                revision=revision,
                environment=environment,
                detail=domain or "tests",
            ),
        ),
    )
    evidence.setdefault(
        "__level__:L3",
        (
            make_selection_evidence(
                "domain-fallback",
                source_snapshot=source_snapshot,
                revision=revision,
                environment=environment,
                detail="tests",
            ),
        ),
    )
    return SelectionSnapshot(
        schema_version=snapshot.schema_version,
        snapshot_id=snapshot.snapshot_id,
        created_at=snapshot.created_at,
        project_root=snapshot.project_root,
        source_path=snapshot.source_path,
        function_id=snapshot.function_id,
        source_sha256=snapshot.source_sha256,
        map_version=snapshot.map_version,
        levels=tuple(levels),
        selected_tests=snapshot.selected_tests,
        reasons=snapshot.reasons,
        impact_status=snapshot.impact_status,
        impact_schema_version=snapshot.impact_schema_version,
        impact_warning=snapshot.impact_warning,
        impact_error=snapshot.impact_error,
        algorithm_version=snapshot.algorithm_version,
        index_version=snapshot.index_version,
        test_config_fingerprint=snapshot.test_config_fingerprint,
        dropped_nodeids=snapshot.dropped_nodeids,
        evidence=evidence,
    )