"""Minimal pytest plugin that writes one event per executed test."""
from __future__ import annotations
# ruff: noqa: E402
import time
_PYTEST_PLUGIN_IMPORTED_AT = time.perf_counter()
import json
import hashlib
import ast
import hmac
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import pytest
from .test_stats import canonical_nodeid, normalize_test_outcome
def _sha256_file(path: Path) -> str:
    # Hash a runtime dependency in bounded chunks so tracing never retains file contents.
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
def _is_runtime_support_path(path: Path, project_root: Path | None = None) -> bool:
    # Check project containment before filtering interpreter plumbing or TMPDIR support paths.
    try:
        resolved = path.resolve()
    except (OSError, RuntimeError, ValueError):
        return False
    if project_root is not None:
        try:
            normalized_project_root = project_root.resolve()
        except (OSError, RuntimeError, ValueError):
            normalized_project_root = None
        if normalized_project_root is not None and (
            resolved == normalized_project_root or normalized_project_root in resolved.parents
        ):
            return False
    support_roots = {
        Path(sys.prefix).resolve(),
        Path(sys.base_prefix).resolve(),
        Path("/dev").resolve(),
        Path("/proc").resolve(),
        Path("/sys").resolve(),
        Path(os.environ.get("TMPDIR", "/tmp")).resolve(),
    }
    if any(resolved == root or root in resolved.parents for root in support_roots):
        return True
    if any(part in {"site-packages", "dist-packages", ".pytest_cache", "__pycache__"} for part in resolved.parts):
        return True
    return resolved.suffix.lower() in {".py", ".pyc", ".pyo", ".so", ".dylib", ".dll"}
def _open_access_mode(event: str, args: tuple[Any, ...]) -> str:
    # Classify audit-hook file opens so generated outputs do not become reusable input dependencies.
    if event == "open":
        raw_mode = args[1] if len(args) > 1 else "r"
        if raw_mode is not None:
            mode = str(raw_mode or "r")
            if any(flag in mode for flag in ("w", "a", "x")):
                return "read_write" if "+" in mode else "write"
            return "read_write" if "+" in mode else "read"
        raw_flags = args[2] if len(args) > 2 else 0
    else:
        raw_flags = args[1] if len(args) > 1 else 0
    try:
        flags = int(raw_flags)
    except (TypeError, ValueError):
        return "unknown"
    write_only = getattr(os, "O_WRONLY", 1)
    read_write = getattr(os, "O_RDWR", 2)
    access_mode = flags & (write_only | read_write)
    if access_mode == read_write:
        return "read_write"
    if access_mode == write_only:
        return "write"
    return "read"
def _static_environment_reads(source: str) -> tuple[tuple[str, ...], bool]:
    # Find literal environment names without evaluating project code in the pytest plugin.
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return (), True
    names: set[str] = set()
    dynamic = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Subscript):
            value = node.value
            if not (
                isinstance(value, ast.Attribute)
                and value.attr == "environ"
                and isinstance(value.value, ast.Name)
                and value.value.id == "os"
            ):
                continue
            if isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, str):
                names.add(node.slice.value)
            else:
                dynamic = True
        elif isinstance(node, ast.Call):
            function = node.func
            is_getenv = (
                isinstance(function, ast.Attribute)
                and function.attr == "getenv"
                and isinstance(function.value, ast.Name)
                and function.value.id == "os"
            ) or (isinstance(function, ast.Name) and function.id == "getenv")
            if not is_getenv:
                continue
            if node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                names.add(node.args[0].value)
            else:
                dynamic = True
    return tuple(sorted(names)), dynamic
class _TestStatsPlugin:
    def __init__(self, *, performance_started: float | None = None) -> None:
        # Keep only the small phase state needed to emit one row per test and one process timing artifact.
        self.output_dir = Path(os.environ["TI_TEST_STATS_OUT_DIR"])
        self.run_id = os.environ.get("TI_TEST_STATS_RUN_ID", "unknown")
        self.phase = os.environ.get("TI_TEST_STATS_PHASE", "unknown")
        self._capture_runtime_evidence = self.phase != "mutant"
        self.level = os.environ.get("TI_TEST_STATS_LEVEL", "unknown")
        self.mutant_id = os.environ.get("TI_TEST_STATS_MUTANT_ID") or None
        self.source_path = os.environ.get("TI_TEST_STATS_SOURCE_PATH", "")
        self.target_sha256 = os.environ.get("TI_TEST_STATS_TARGET_SHA256") or None
        self.retry = os.environ.get("TI_TEST_STATS_RETRY") == "1"
        self.attempt = os.environ.get("TI_TEST_STATS_ATTEMPT", "attempt")
        self.worker_id = os.environ.get("PYTEST_XDIST_WORKER", "main")
        self.pid = os.getpid()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_path = self.output_dir / f"{self.run_id}.{self.attempt}.{self.worker_id}.{self.pid}.jsonl"
        self.performance_path = self.output_dir / f"{self.run_id}.{self.attempt}.{self.worker_id}.{self.pid}.performance.json"
        self._handle = self.output_path.open("a", encoding="utf-8", buffering=1)
        self._closed = False
        self._performance_started = time.perf_counter() if performance_started is None else float(performance_started)
        self._session_started_at: float | None = None
        self._collection_import_seconds = 0.0
        self._test_execution_seconds = 0.0
        self._session_finalize_seconds = 0.0
        self._tests: dict[str, dict[str, Any]] = {}
        self._emitted: set[str] = set()
        self._first_failure: str | None = None
        self._sequence = 0
        self._provenance_error: str | None = None
        self._provenance_reported = False
        self._project_root = Path(
            os.environ.get("TI_TEST_STATS_EXPECTED_PROJECT_ROOT", Path.cwd())
        ).resolve()
        self._active_nodeid: str | None = None
        self._active_owner = "<session>"
        self._dependency_paths: dict[str, set[Path]] = {}
        self._dependency_access_modes: dict[str, dict[Path, str]] = {}
        self._external_dependency_paths: dict[str, set[Path]] = {}
        self._external_access_modes: dict[str, dict[Path, str]] = {}
        self._dependency_blockers: dict[str, set[str]] = {}
        self._environment_mode = os.environ.get("TI_TEST_STATS_ENV_MODE", "strict").strip().lower()
        self._environment_keys = {
            item for item in os.environ.get("TI_TEST_STATS_ENV_KEYS", "").split(",") if item
        }
        self._environment_prefixes = tuple(
            item for item in os.environ.get("TI_TEST_STATS_ENV_PREFIXES", "").split(",") if item
        )
        self._environment_ignored = {
            item for item in os.environ.get("TI_TEST_STATS_ENV_IGNORED", "").split(",") if item
        }
        self._environment_secrets = {
            item for item in os.environ.get("TI_TEST_STATS_ENV_SECRETS", "").split(",") if item
        }
        if self._capture_runtime_evidence:
            try:
                sys.addaudithook(self._audit_event)
            except (AttributeError, RuntimeError):
                self._dependency_blockers.setdefault("<session>", set()).add("audit-hook-unavailable")
    def _audit_event(self, event: str, args: tuple[Any, ...]) -> None:
        # Record dependencies against session, collection, fixture-scope or node ownership.
        owner = self._active_owner
        if not owner:
            return
        if event in {"open", "os.open"}:
            raw_path = args[0] if args else None
            if not isinstance(raw_path, (str, bytes, os.PathLike)):
                return
            try:
                candidate = Path(os.fsdecode(raw_path))
                resolved = (Path.cwd() / candidate).resolve() if not candidate.is_absolute() else candidate.resolve()
            except (OSError, RuntimeError, TypeError, ValueError):
                self._dependency_blockers.setdefault(owner, set()).add("unresolvable-file-open")
                return
            access_mode = _open_access_mode(event, args)
            if access_mode == "write":
                return
            if _is_runtime_support_path(resolved, self._project_root):
                return
            if self._project_root == resolved or self._project_root in resolved.parents:
                if any(
                    part in {".pytest_cache", "__pycache__", ".venv", "venv", "reports", ".theseus"}
                    for part in resolved.parts
                ) or resolved.suffix in {".pyc", ".pyo"}:
                    return
                self._dependency_paths.setdefault(owner, set()).add(resolved)
                self._dependency_access_modes.setdefault(owner, {})[resolved] = access_mode
            else:
                self._external_dependency_paths.setdefault(owner, set()).add(resolved)
                self._external_access_modes.setdefault(owner, {})[resolved] = access_mode
                self._dependency_blockers.setdefault(owner, set()).add("external-file-read")
        elif event == "socket.connect":
            self._dependency_blockers.setdefault(owner, set()).add("network-access")
        elif event == "subprocess.Popen":
            self._dependency_blockers.setdefault(owner, set()).add("subprocess-access")
        elif event == "import":
            self._dependency_blockers.setdefault(owner, set()).add("runtime-import")
    @staticmethod
    def _owner_keys(nodeid: str) -> tuple[str, ...]:
        # Expand scope-owned dependencies so session and fixture resources reach every affected test.
        parts = [item for item in str(nodeid).split("::") if item]
        if not parts:
            return ("<session>", "<collection>", str(nodeid))
        keys = ["<session>", "<collection>", f"<module>:{parts[0]}"]
        for index in range(1, max(1, len(parts) - 1)):
            keys.append(f"<class>:{'::'.join(parts[: index + 1])}")
        keys.append(str(nodeid))
        return tuple(dict.fromkeys(keys))
    @staticmethod
    def _fixture_owner(nodeid: str, scope: str) -> str:
        # Map pytest fixture scope to the durable ownership key used by dependency events.
        normalized_scope = str(scope).strip().lower()
        if normalized_scope == "session":
            return "<session>"
        parts = [item for item in str(nodeid).split("::") if item]
        if normalized_scope == "module":
            return f"<module>:{parts[0] if parts else nodeid}"
        if normalized_scope == "class" and len(parts) > 1:
            return f"<class>:{'::'.join(parts[:2])}"
        return str(nodeid)
    def _check_import_provenance(self) -> None:
        # Reject production modules imported from outside the active isolated workspace.
        expected_root_value = os.environ.get("TI_TEST_STATS_EXPECTED_PROJECT_ROOT")
        expected_source = os.environ.get("TI_TEST_STATS_SOURCE_PATH", "").replace("\\", "/").lstrip("./")
        if not expected_source:
            return
        expected_root = Path(expected_root_value or Path.cwd()).resolve()
        expected_path = (expected_root / expected_source).resolve()
        suffix = "/" + expected_source
        for module in tuple(sys.modules.values()):
            module_path_value = getattr(module, "__file__", None)
            if not module_path_value:
                continue
            try:
                module_path = Path(str(module_path_value)).resolve()
            except (OSError, RuntimeError, TypeError, ValueError):
                continue
            module_text = module_path.as_posix().replace("\\", "/")
            if module_path != expected_path and (module_text.endswith(suffix) or module_text == expected_source):
                self._provenance_error = (
                    "IMPORT_PROVENANCE_ERROR: expected "
                    f"{expected_path}, imported {module_path}"
                )
                return
    def pytest_sessionstart(self, session: Any) -> None:
        # Mark the end of pytest/plugin configuration before collection and test execution begin.
        del session
        if self._session_started_at is None:
            self._session_started_at = time.perf_counter()
    def pytest_collection_finish(self, session: Any) -> None:
        # Check provenance after pytest has imported collection modules and conftest files.
        del session
        self._check_import_provenance()
    @pytest.hookimpl(hookwrapper=True)
    def pytest_collection(self, session: Any) -> Any:
        # Attribute and time collection-time imports and file reads without changing collection semantics.
        del session
        collection_started = time.perf_counter()
        previous_owner = self._active_owner
        self._active_owner = "<collection>"
        try:
            outcome = yield
            outcome.get_result()
        finally:
            self._collection_import_seconds += max(0.0, time.perf_counter() - collection_started)
            self._active_owner = previous_owner
    @pytest.hookimpl(hookwrapper=True)
    def pytest_fixture_setup(self, fixturedef: Any, request: Any) -> Any:
        # Attribute session, module and class fixture reads at their true pytest ownership level.
        previous_owner = self._active_owner
        node = getattr(request, "node", None)
        nodeid = str(getattr(node, "nodeid", "<session>"))
        self._active_owner = self._fixture_owner(nodeid, str(getattr(fixturedef, "scope", "function")))
        try:
            outcome = yield
            outcome.get_result()
        finally:
            self._active_owner = previous_owner
    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_protocol(self, item: Any, nextitem: Any) -> Any:
        # Keep dependency attribution active and time the complete setup/call/teardown protocol.
        del nextitem
        test_started = time.perf_counter()
        self._active_nodeid = str(item.nodeid)
        previous_owner = self._active_owner
        self._active_owner = self._active_nodeid
        try:
            outcome = yield
            outcome.get_result()
        finally:
            self._test_execution_seconds += max(0.0, time.perf_counter() - test_started)
            self._active_owner = previous_owner
            self._active_nodeid = None
    def _runtime_dependencies(self, nodeid: str) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
        # Materialize sorted scope-owned path hashes and explicit uncertainty markers for one test event.
        rows: list[dict[str, Any]] = []
        project_paths: dict[Path, tuple[str, str]] = {}
        external_paths: dict[Path, tuple[str, str]] = {}
        for owner in self._owner_keys(nodeid):
            for path in self._dependency_paths.get(owner, set()):
                mode = self._dependency_access_modes.get(owner, {}).get(path, "read")
                previous = project_paths.get(path)
                if previous is None or previous[1] == "read" and mode == "read_write":
                    project_paths[path] = (owner, mode)
            for path in self._external_dependency_paths.get(owner, set()):
                mode = self._external_access_modes.get(owner, {}).get(path, "read")
                previous = external_paths.get(path)
                if previous is None or previous[1] == "read" and mode == "read_write":
                    external_paths[path] = (owner, mode)
        for path in sorted(project_paths, key=lambda item: item.as_posix()):
            owner, access_mode = project_paths[path]
            try:
                relative = path.relative_to(self._project_root).as_posix()
                digest = _sha256_file(path) if path.is_file() else "missing"
                size_bytes = path.stat().st_size if path.is_file() else 0
            except (OSError, UnicodeError, ValueError):
                relative = str(path)
                digest = "unreadable"
                size_bytes = 0
            rows.append(
                {
                    "path": relative,
                    "relative_path": relative,
                    "sha256": digest,
                    "content_sha256": digest,
                    "size_bytes": size_bytes,
                    "outside_workspace": False,
                    "access_mode": access_mode,
                    "owner": owner,
                }
            )
        for path in sorted(external_paths, key=lambda item: item.as_posix()):
            owner, access_mode = external_paths[path]
            try:
                size_bytes = path.stat().st_size if path.is_file() else 0
            except OSError:
                size_bytes = 0
            rows.append(
                {
                    "path": "<external>",
                    "relative_path": "<external>",
                    "sha256": "",
                    "content_sha256": "",
                    "size_bytes": size_bytes,
                    "outside_workspace": True,
                    "access_mode": access_mode,
                    "owner": owner,
                }
            )
        blockers = set()
        for owner in self._owner_keys(nodeid):
            blockers.update(self._dependency_blockers.get(owner, set()))
        blockers.discard("runtime-import")
        return rows, tuple(sorted(blockers))
    def _environment_observations(self, nodeid: str) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
        # Emit HMAC-only environment observations and reject undeclared reads in strict modes.
        raw_path = nodeid.split("::", 1)[0]
        relative = canonical_nodeid(self._project_root, raw_path)
        source_path = self._project_root / relative
        try:
            source = source_path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            return [], ("environment-source-unreadable",)
        names, dynamic = _static_environment_reads(source)
        blockers: set[str] = {"dynamic-environment-read"} if dynamic else set()
        key = hashlib.sha256(
            os.environ.get("TI_TEST_STATS_ENV_SECRET_KEY", "theseus-environment-v1").encode("utf-8")
        ).digest()
        rows: list[dict[str, Any]] = []
        for name in names:
            declared = name in self._environment_keys or any(name.startswith(prefix) for prefix in self._environment_prefixes)
            if name in self._environment_ignored:
                declared = True
            if self._environment_mode != "track_all_except" and not declared:
                blockers.add(f"undeclared-environment-read:{name}")
            present = name in os.environ
            value_hmac = hmac.new(key, os.environ.get(name, "").encode("utf-8"), hashlib.sha256).hexdigest()
            rows.append(
                {
                    "name": name,
                    "present": present,
                    "value_hmac": value_hmac,
                    "declared": declared,
                    "secret": name in self._environment_secrets,
                }
            )
        return rows, tuple(sorted(blockers))
    def pytest_runtest_logreport(self, report: Any) -> None:
        # Accumulate setup/call/teardown into one compact test attempt.
        nodeid = str(report.nodeid)
        if nodeid in self._emitted:
            return
        state = self._tests.setdefault(
            nodeid,
            {
                "phase_outcomes": {},
                "duration_seconds": 0.0,
                "sequence": self._sequence,
                "xfailed": False,
                "xpassed": False,
            },
        )
        self._sequence += 1
        when = str(getattr(report, "when", "call"))
        outcome = str(getattr(report, "outcome", "error"))
        state["phase_outcomes"][when] = outcome
        state["duration_seconds"] += float(getattr(report, "duration", 0.0) or 0.0)
        if getattr(report, "wasxfail", False) and outcome == "skipped":
            state["xfailed"] = True
        elif getattr(report, "wasxfail", False) and outcome == "passed":
            state["xpassed"] = True
        if outcome == "failed" and not getattr(report, "wasxfail", False) and self._first_failure is None:
            self._first_failure = nodeid
        if when == "teardown" or (when == "setup" and outcome in {"failed", "skipped"}):
            self._emit(nodeid)
    def pytest_sessionfinish(self, session: Any, exitstatus: int) -> None:
        # Flush tests and publish bounded process timing evidence without changing the pytest exit contract.
        del exitstatus
        finalize_started = time.perf_counter()
        self._check_import_provenance()
        if self._provenance_error:
            session.exitstatus = 3
            if not self._provenance_reported:
                print(self._provenance_error)
                self._provenance_reported = True
        try:
            for nodeid in sorted(self._tests, key=lambda value: self._tests[value]["sequence"]):
                if nodeid not in self._emitted:
                    self._emit(nodeid)
        finally:
            try:
                self._close_output()
            finally:
                self._session_finalize_seconds += max(0.0, time.perf_counter() - finalize_started)
                self._write_performance()
    def _write_performance(self) -> None:
        # Write one exclusive single-process timing artifact while keeping measurement I/O non-fatal.
        finished = time.perf_counter()
        setup_finished = self._session_started_at if self._session_started_at is not None else self._performance_started
        config_initialization_seconds = max(0.0, setup_finished - self._performance_started)
        lifecycle_seconds = max(0.0, finished - self._performance_started)
        observed_seconds = (
            config_initialization_seconds
            + self._collection_import_seconds
            + self._test_execution_seconds
            + self._session_finalize_seconds
        )
        framework_residual_seconds = max(0.0, lifecycle_seconds - observed_seconds)
        payload = {
            "schema_version": 1,
            "run_id": self.run_id,
            "phase": self.phase,
            "level": self.level,
            "mutant_id": self.mutant_id,
            "retry": self.retry,
            "attempt": self.attempt,
            "worker_id": self.worker_id,
            "pid": self.pid,
            "exclusive": True,
            "metrics": {
                "config_initialization_seconds": config_initialization_seconds,
                "collection_import_seconds": self._collection_import_seconds,
                "test_execution_seconds": self._test_execution_seconds,
                "session_finalize_seconds": self._session_finalize_seconds,
                "framework_residual_seconds": framework_residual_seconds,
                "plugin_lifecycle_seconds": lifecycle_seconds,
            },
        }
        try:
            self.performance_path.write_text(
                json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )
        except (OSError, UnicodeError):
            return
    def _close_output(self) -> None:
        # Close the one session-level journal handle exactly once at the pytest boundary.
        if self._closed:
            return
        self._handle.flush()
        self._handle.close()
        self._closed = True
    def _emit(self, nodeid: str) -> None:
        # Append one idempotently attributable event without storing test output.
        if nodeid in self._emitted:
            return
        state = self._tests[nodeid]
        outcomes = state["phase_outcomes"]
        normalized_outcomes = tuple(normalize_test_outcome(value) for value in outcomes.values())
        if state["xpassed"]:
            outcome = "xpassed"
        elif state["xfailed"]:
            outcome = "xfailed"
        elif "timeout" in normalized_outcomes:
            outcome = "timeout"
        elif "cancelled" in normalized_outcomes:
            outcome = "cancelled"
        elif "error" in normalized_outcomes:
            outcome = "error"
        elif "failed" in normalized_outcomes:
            outcome = "failed"
        elif "skipped" in normalized_outcomes:
            outcome = "skipped"
        elif "unknown" in normalized_outcomes:
            outcome = "unknown"
        else:
            outcome = "passed"
        event = {
            "event_id": f"{self.run_id}:{self.phase}:{self.level}:{self.mutant_id or ''}:{self.retry}:{self.pid}:{self.worker_id}:{state['sequence']}:{nodeid}",
            "run_id": self.run_id,
            "phase": self.phase,
            "level": self.level,
            "mutant_id": self.mutant_id,
            "source_path": self.source_path,
            "target_sha256": self.target_sha256,
            "nodeid": nodeid,
            "outcome": normalize_test_outcome(outcome),
            "phase_outcomes": outcomes,
            "duration_ms": round(float(state["duration_seconds"]) * 1000.0, 3),
            "first_failure": nodeid == self._first_failure,
            "worker_id": self.worker_id,
            "retry": self.retry,
            "recorded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        if self._capture_runtime_evidence:
            runtime_dependencies, dependency_blockers = self._runtime_dependencies(nodeid)
            environment_reads, environment_blockers = self._environment_observations(nodeid)
            event["runtime_dependencies"] = runtime_dependencies
            event["runtime_dependency_complete"] = not dependency_blockers and not environment_blockers
            event["runtime_dependency_blockers"] = list(sorted(set(dependency_blockers) | set(environment_blockers)))
            event["environment_reads"] = environment_reads
            event["environment_dependency_complete"] = not environment_blockers
            event["environment_dependency_blockers"] = list(environment_blockers)
        else:
            event["runtime_dependencies"] = []
            event["runtime_dependency_complete"] = False
            event["runtime_dependency_blockers"] = ["mutant-runtime-evidence-not-collected"]
            event["environment_reads"] = []
            event["environment_dependency_complete"] = False
            event["environment_dependency_blockers"] = ["mutant-runtime-evidence-not-collected"]
        self._handle.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
        self._emitted.add(nodeid)
def pytest_configure(config: Any) -> None:
    # Enable collection only when the runner explicitly supplies stats metadata.
    if os.environ.get("TI_TEST_STATS_OUT_DIR"):
        config.pluginmanager.register(_TestStatsPlugin(performance_started=_PYTEST_PLUGIN_IMPORTED_AT), "test-intelligence-stats")
