"""Per-test execution journals, SQLite aggregates and selection history."""
from __future__ import annotations
import json
import math
import os
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Mapping
from .io_utils import ensure_dir, stable_hash, utc_now_iso
from .models import PerformanceMetrics, TestHealthStatus, TestOutcome
STATS_SCHEMA_VERSION = 4
STATS_PLUGIN = "test_intelligence_unified_v1.pytest_plugin"
HEALTH_PHASES = ("baseline", "standalone")
COMPARE_PHASES = ("baseline", "standalone", "mutant")
EVENT_IDENTITY_VERSION = 2
STATS_INSERT_BATCH_SIZE = 512
STATS_DURATION_SAMPLE_SIZE = 256
DURATION_SAMPLE_VERSION = 1
_OUTCOME_ALIASES = {
    "pass": TestOutcome.PASSED.value,
    "ok": TestOutcome.PASSED.value,
    "fail": TestOutcome.FAILED.value,
    "err": TestOutcome.ERROR.value,
    "internal_error": TestOutcome.ERROR.value,
    "skip": TestOutcome.SKIPPED.value,
    "xfail": TestOutcome.XFAILED.value,
    "xpass": TestOutcome.XPASSED.value,
    "cancel": TestOutcome.CANCELLED.value,
    "canceled": TestOutcome.CANCELLED.value,
    "timed_out": TestOutcome.TIMEOUT.value,
    "timedout": TestOutcome.TIMEOUT.value,
}
_OUTCOME_VALUES = frozenset(item.value for item in TestOutcome)
_TEST_ATTEMPT_INSERT_SQL = """
INSERT OR IGNORE INTO test_attempts(
    event_key, project_root, run_id, source_path, target_sha256,
    phase, level, mutant_id, nodeid, outcome, duration_ms,
    first_failure, worker_id, retry, recorded_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
def normalize_test_outcome(value: object, *, was_xfail: bool = False) -> str:
    # Normalize pytest and external execution labels into the versioned journal vocabulary.
    raw = (value.value if isinstance(value, TestOutcome) else str(value or "")).strip().lower()
    if was_xfail and raw in {TestOutcome.PASSED.value, TestOutcome.XPASSED.value, "xpass"}:
        return TestOutcome.XPASSED.value
    if was_xfail and raw in {TestOutcome.SKIPPED.value, TestOutcome.XFAILED.value, "xfail"}:
        return TestOutcome.XFAILED.value
    if raw in _OUTCOME_VALUES:
        return raw
    return _OUTCOME_ALIASES.get(raw, TestOutcome.UNKNOWN.value)
def _normalize_compare_phases(phases: Iterable[str] | None) -> tuple[str, ...]:
    # Validate and freeze the phase scope before it reaches the grouped comparison query.
    if phases is None:
        return HEALTH_PHASES
    values = (phases,) if isinstance(phases, str) else phases
    normalized = tuple(dict.fromkeys(str(value).strip().lower() for value in values if str(value).strip()))
    invalid = sorted(set(normalized) - set(COMPARE_PHASES))
    if invalid:
        raise ValueError(f"unsupported comparison phase: {', '.join(invalid)}")
    if not normalized:
        raise ValueError("comparison phase scope cannot be empty")
    return normalized
def _event_identity(event: Mapping[str, Any], line_number: int) -> str:
    # Build a relocation-safe fallback identity from event data instead of an absolute journal path.
    explicit = event.get("event_id") or event.get("event_key")
    if explicit:
        return str(explicit)
    normalized = {
        key: event.get(key)
        for key in (
            "run_id",
            "phase",
            "level",
            "mutant_id",
            "nodeid",
            "outcome",
            "duration_ms",
            "first_failure",
            "worker_id",
            "retry",
            "recorded_at",
        )
    }
    normalized["outcome"] = normalize_test_outcome(
        event.get("outcome"),
        was_xfail=bool(event.get("was_xfail") or event.get("xfail")),
    )
    normalized["line_number"] = line_number
    return stable_hash(normalized)
def is_pytest_command(argv: Iterable[str]) -> bool:
    # Detect pytest invocations without inspecting shell syntax.
    values = [str(value) for value in argv]
    for index, value in enumerate(values):
        lowered = value.lower()
        if Path(value).name.lower() in {"pytest", "pytest.exe"}:
            return True
        if lowered == "-m" and index + 1 < len(values) and values[index + 1].lower() == "pytest":
            return True
    return False
def instrument_pytest_command(argv: Iterable[str]) -> tuple[str, ...]:
    # Inject the per-test plugin before pytest's end-of-options delimiter.
    values = tuple(str(value) for value in argv)
    if not is_pytest_command(values):
        return values
    if STATS_PLUGIN in values or "--no-intelligence-test-stats" in values:
        return values
    insert_at = next(
        (
            index + 1
            for index, value in enumerate(values)
            if Path(value).name.lower() in {"pytest", "pytest.exe"}
            or (value.lower() == "pytest" and index > 0)
        ),
        next((index for index, value in enumerate(values) if value == "--"), len(values)),
    )
    return (*values[:insert_at], "-p", STATS_PLUGIN, *values[insert_at:])
def build_test_stats_env(
    event_dir: Path,
    *,
    run_id: str,
    phase: str,
    level: str,
    mutant_id: str | None,
    source_path: str,
    target_sha256: str | None,
    retry: bool = False,
    base_env: Mapping[str, str] | None = None,
    environment_mode: str = "strict",
    environment_keys: Iterable[str] = (),
    environment_prefixes: Iterable[str] = (),
    environment_ignored: Iterable[str] = (),
    environment_secrets: Iterable[str] = (),
    environment_secret_key: str = "theseus-environment-v1",
    pytest_plugin_autoload: bool = True,
) -> dict[str, str]:
    # Build isolated child metadata so every pytest event is attributable.
    environment = dict(os.environ if base_env is None else base_env)
    ensure_dir(event_dir)
    environment.update(
        {
            "TI_TEST_STATS_OUT_DIR": str(event_dir.resolve()),
            "TI_TEST_STATS_RUN_ID": run_id,
            "TI_TEST_STATS_PHASE": phase,
            "TI_TEST_STATS_LEVEL": level,
            "TI_TEST_STATS_MUTANT_ID": mutant_id or "",
            "TI_TEST_STATS_SOURCE_PATH": source_path,
            "TI_TEST_STATS_TARGET_SHA256": target_sha256 or "",
            "TI_TEST_STATS_RETRY": "1" if retry else "0",
            "TI_TEST_STATS_ATTEMPT": stats_attempt_id(run_id, phase, level, mutant_id, retry),
            "TI_TEST_STATS_ENV_MODE": str(environment_mode),
            "TI_TEST_STATS_ENV_KEYS": ",".join(sorted(str(item) for item in environment_keys if str(item))),
            "TI_TEST_STATS_ENV_PREFIXES": ",".join(sorted(str(item) for item in environment_prefixes if str(item))),
            "TI_TEST_STATS_ENV_IGNORED": ",".join(sorted(str(item) for item in environment_ignored if str(item))),
            "TI_TEST_STATS_ENV_SECRETS": ",".join(sorted(str(item) for item in environment_secrets if str(item))),
            "TI_TEST_STATS_ENV_SECRET_KEY": str(environment_secret_key),
        }
    )
    if not pytest_plugin_autoload:
        # Synthetic internal fixtures use only the explicitly injected Theseus plugin profile.
        environment["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    package_root = str(Path(__file__).resolve().parent.parent)
    pythonpath: list[str] = []
    for item in environment.get("PYTHONPATH", "").split(os.pathsep):
        if not item:
            continue
        candidate = Path(item)
        pythonpath.append(str((Path.cwd() / candidate).resolve() if not candidate.is_absolute() else candidate))
    if package_root not in pythonpath:
        pythonpath.insert(0, package_root)
    environment["PYTHONPATH"] = os.pathsep.join(pythonpath)
    return environment
def stats_attempt_id(run_id: str, phase: str, level: str, mutant_id: str | None, retry: bool) -> str:
    # Build a stable per-subprocess token so ingestion never rescans old journals.
    return stable_hash(
        {"run_id": run_id, "phase": phase, "level": level, "mutant_id": mutant_id or "", "retry": retry}
    )[:16]
def load_pytest_attempt_performance(
    event_dir: Path,
    *,
    run_id: str,
    phase: str,
    level: str,
    mutant_id: str | None,
    retry: bool,
) -> dict[str, Any]:
    # Load one exclusive pytest-process timing artifact and reject ambiguous parallel evidence.
    attempt = stats_attempt_id(run_id, phase, level, mutant_id, retry)
    try:
        artifacts = sorted(event_dir.glob(f"{run_id}.{attempt}.*.performance.json"))
    except OSError:
        return {"status": "unavailable", "reason": "performance_artifact_scan_error", "artifact_count": 0}
    if not artifacts:
        return {"status": "unavailable", "reason": "performance_artifact_missing", "artifact_count": 0}
    if len(artifacts) != 1:
        return {
            "status": "unavailable",
            "reason": "parallel_or_ambiguous_pytest_processes",
            "artifact_count": len(artifacts),
        }
    artifact = artifacts[0]
    try:
        payload = json.loads(artifact.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {"status": "unavailable", "reason": "performance_artifact_invalid", "artifact_count": 1}
    if not isinstance(payload, dict):
        return {"status": "unavailable", "reason": "performance_artifact_schema", "artifact_count": 1}
    try:
        schema_version = int(payload.get("schema_version", 0) or 0)
    except (TypeError, ValueError):
        return {"status": "unavailable", "reason": "performance_artifact_schema", "artifact_count": 1}
    if schema_version != 1:
        return {"status": "unavailable", "reason": "performance_artifact_schema", "artifact_count": 1}
    if (
        str(payload.get("run_id", "")) != run_id
        or str(payload.get("phase", "")) != phase
        or str(payload.get("level", "")) != level
        or (str(payload.get("mutant_id") or "") or None) != (str(mutant_id or "") or None)
        or bool(payload.get("retry")) is not bool(retry)
        or str(payload.get("attempt", "")) != attempt
    ):
        return {"status": "unavailable", "reason": "performance_artifact_identity", "artifact_count": 1}
    if payload.get("exclusive") is not True:
        return {"status": "unavailable", "reason": "performance_artifact_non_exclusive", "artifact_count": 1}
    raw_metrics = payload.get("metrics")
    if not isinstance(raw_metrics, dict):
        return {"status": "unavailable", "reason": "performance_metrics_missing", "artifact_count": 1}
    names = (
        "config_initialization_seconds",
        "collection_import_seconds",
        "test_execution_seconds",
        "session_finalize_seconds",
        "framework_residual_seconds",
        "plugin_lifecycle_seconds",
    )
    metrics: dict[str, float] = {}
    try:
        for name in names:
            value = float(raw_metrics[name])
            if value < 0.0 or not math.isfinite(value):
                raise ValueError(name)
            metrics[name] = value
    except (KeyError, TypeError, ValueError):
        return {"status": "unavailable", "reason": "performance_metrics_invalid", "artifact_count": 1}
    component_total = sum(metrics[name] for name in names[:-1])
    lifecycle = metrics["plugin_lifecycle_seconds"]
    tolerance = max(1e-6, lifecycle * 1e-6)
    if abs(component_total - lifecycle) > tolerance:
        return {"status": "unavailable", "reason": "performance_metrics_unreconciled", "artifact_count": 1}
    try:
        pid = int(payload.get("pid", 0) or 0)
    except (TypeError, ValueError):
        return {"status": "unavailable", "reason": "performance_artifact_pid", "artifact_count": 1}
    if pid <= 0:
        return {"status": "unavailable", "reason": "performance_artifact_pid", "artifact_count": 1}
    return {
        "status": "observed",
        "reason": None,
        "artifact_count": 1,
        "artifact": str(artifact),
        "pid": pid,
        "worker_id": str(payload.get("worker_id", "main")),
        "metrics": metrics,
    }
def stats_db_path(reports_dir: Path) -> Path:
    # Return the stable historical database location for one reports directory.
    return reports_dir.resolve() / "test_stats.sqlite"
def _runtime_event_directories(db_path: Path, run_id: str) -> tuple[Path, ...]:
    # Resolve direct and engine-scoped pytest journals without scanning unrelated report trees.
    base = db_path.resolve().parent
    candidates = [base / "test_stats_events" / run_id]
    engine_root = base / "engine"
    if engine_root.is_dir():
        campaign_id = run_id.removeprefix("engine-baseline-")
        if campaign_id:
            candidates.append(engine_root / campaign_id / "test_stats_events" / run_id)
        try:
            candidates.extend(
                child / "test_stats_events" / run_id
                for child in sorted(engine_root.iterdir())
                if child.is_dir()
            )
        except OSError:
            pass
    if base.name != "engine":
        parent_engine = base.parent / "engine"
        if parent_engine.is_dir():
            candidates.extend(
                child / "test_stats_events" / run_id
                for child in sorted(parent_engine.iterdir())
                if child.is_dir()
            )
    return tuple(dict.fromkeys(path.resolve() for path in candidates))
def canonical_nodeid(root: Path, nodeid: str) -> str:
    # Normalize pytest node IDs to project-relative paths so copy-workspace names cannot affect reuse.
    raw_path, *selectors = str(nodeid).replace("\\", "/").split("::")
    resolved_root = Path(root).expanduser().resolve()
    normalized_path = raw_path
    candidate = Path(raw_path)
    if candidate.is_absolute():
        try:
            normalized_path = candidate.resolve().relative_to(resolved_root).as_posix()
        except ValueError:
            normalized_path = raw_path.lstrip("/")
    else:
        root_parts = tuple(part for part in resolved_root.as_posix().split("/") if part)
        path_parts = tuple(part for part in raw_path.split("/") if part and part != ".")
        for length in range(min(len(root_parts), len(path_parts)), 0, -1):
            if path_parts[:length] == root_parts[-length:]:
                normalized_path = "/".join(path_parts[length:])
                break
        else:
            possible = (resolved_root / raw_path).resolve()
            if possible.is_relative_to(resolved_root):
                normalized_path = possible.relative_to(resolved_root).as_posix()
    return "::".join((normalized_path, *selectors))
def _connect(path: Path) -> sqlite3.Connection:
    # Open the stats database with a small busy timeout for worker ingestion.
    ensure_dir(path.parent)
    connection = sqlite3.connect(path, check_same_thread=False)
    connection.execute("PRAGMA busy_timeout=5000")
    connection.execute("PRAGMA journal_mode=WAL")
    return connection
def _duration_sample_values(rows: Iterable[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    # Derive deterministic bounded-sample slots from immutable test-attempt identities.
    values: list[tuple[Any, ...]] = []
    for row in rows:
        event_key = str(row[0])
        project_root = str(row[1])
        nodeid = str(row[8])
        duration_ms = float(row[10] or 0.0)
        sample_rank = stable_hash({"event_key": event_key})
        sample_slot = int(sample_rank[:16], 16) % STATS_DURATION_SAMPLE_SIZE
        values.append((project_root, nodeid, sample_slot, event_key, sample_rank, duration_ms))
    return values
def _insert_duration_samples(connection: sqlite3.Connection, rows: Iterable[tuple[Any, ...]]) -> None:
    # Maintain at most one deterministic minimum-rank duration in every sample slot.
    values = _duration_sample_values(rows)
    if not values:
        return
    connection.executemany(
        """
        INSERT INTO test_duration_samples(
            project_root, nodeid, sample_slot, event_key, sample_rank, duration_ms
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(project_root, nodeid, event_key) DO NOTHING
        ON CONFLICT(project_root, nodeid, sample_slot) DO UPDATE SET
            event_key = excluded.event_key,
            sample_rank = excluded.sample_rank,
            duration_ms = excluded.duration_ms
        WHERE excluded.sample_rank < test_duration_samples.sample_rank
        """,
        values,
    )
def _backfill_duration_samples(connection: sqlite3.Connection) -> None:
    # Perform one streaming schema migration instead of rescanning history on every summary query.
    row = connection.execute(
        "SELECT value FROM metadata WHERE key = 'duration_sample_version'"
    ).fetchone()
    if row is not None and str(row[0]) == str(DURATION_SAMPLE_VERSION):
        return
    cursor = connection.execute(
        "SELECT event_key, project_root, run_id, source_path, target_sha256, phase, level, mutant_id, nodeid, outcome, duration_ms, first_failure, worker_id, retry, recorded_at FROM test_attempts ORDER BY event_key"
    )
    while True:
        batch = cursor.fetchmany(STATS_INSERT_BATCH_SIZE)
        if not batch:
            break
        _insert_duration_samples(connection, batch)
    connection.execute(
        "INSERT OR REPLACE INTO metadata(key, value) VALUES ('duration_sample_version', ?)",
        (str(DURATION_SAMPLE_VERSION),),
    )
def _ensure_schema(connection: sqlite3.Connection) -> None:
    # Create append-only events and health-oriented lookup paths exactly once.
    connection.executescript(
        f"""
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        INSERT OR REPLACE INTO metadata(key, value)
        VALUES ('schema_version', '{STATS_SCHEMA_VERSION}');
        INSERT OR REPLACE INTO metadata(key, value)
        VALUES ('outcome_schema_version', '1');
        INSERT OR REPLACE INTO metadata(key, value)
        VALUES ('event_identity_version', '{EVENT_IDENTITY_VERSION}');
        CREATE TABLE IF NOT EXISTS test_attempts (
            event_key TEXT PRIMARY KEY,
            project_root TEXT NOT NULL,
            run_id TEXT NOT NULL,
            source_path TEXT NOT NULL,
            target_sha256 TEXT,
            phase TEXT NOT NULL,
            level TEXT NOT NULL,
            mutant_id TEXT,
            nodeid TEXT NOT NULL,
            outcome TEXT NOT NULL,
            duration_ms REAL NOT NULL,
            first_failure INTEGER NOT NULL,
            worker_id TEXT NOT NULL,
            retry INTEGER NOT NULL,
            recorded_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_test_attempts_nodeid ON test_attempts(nodeid);
        CREATE INDEX IF NOT EXISTS ix_test_attempts_project_node ON test_attempts(project_root, nodeid);
        CREATE INDEX IF NOT EXISTS ix_test_attempts_phase_mutant ON test_attempts(phase, mutant_id);
        CREATE INDEX IF NOT EXISTS ix_test_attempts_project_phase_node ON test_attempts(project_root, phase, nodeid);
        CREATE TABLE IF NOT EXISTS test_duration_samples (
            project_root TEXT NOT NULL,
            nodeid TEXT NOT NULL,
            sample_slot INTEGER NOT NULL,
            event_key TEXT NOT NULL,
            sample_rank TEXT NOT NULL,
            duration_ms REAL NOT NULL,
            PRIMARY KEY(project_root, nodeid, sample_slot),
            UNIQUE(project_root, nodeid, event_key)
        );
        CREATE INDEX IF NOT EXISTS ix_test_duration_samples_project_node
        ON test_duration_samples(project_root, nodeid, sample_slot);
        """
    )
    _backfill_duration_samples(connection)
def open_test_stats_connection(path: Path) -> sqlite3.Connection:
    # Open and durably initialize one reusable campaign connection for reads and repeated ingestion calls.
    connection = _connect(path)
    _ensure_schema(connection)
    connection.commit()
    return connection
def _insert_stats_batch(connection: sqlite3.Connection, rows: list[tuple[Any, ...]]) -> int:
    # Insert attempts and update bounded percentile samples without changing replay counts.
    if not rows:
        return 0
    before = connection.total_changes
    connection.executemany(_TEST_ATTEMPT_INSERT_SQL, rows)
    inserted = int(connection.total_changes - before)
    _insert_duration_samples(connection, rows)
    return inserted
def ingest_test_stats(
    event_dir: Path,
    db_path: Path,
    *,
    project_root: Path,
    run_id: str,
    source_path: str,
    event_files: Iterable[Path] | None = None,
    connection: sqlite3.Connection | None = None,
    metrics: PerformanceMetrics | None = None,
) -> dict[str, Any]:
    # Ingest each worker journal idempotently and return a bounded ingestion summary.
    if event_files is not None:
        files = sorted({Path(item).resolve() for item in event_files})
    elif event_dir.exists():
        files = sorted(event_dir.glob("*.jsonl"))
    else:
        files = []
    inserted = 0
    invalid = 0
    owns_connection = connection is None
    db_connection = connection or open_test_stats_connection(db_path)
    try:
        for journal in files:
            batch: list[tuple[Any, ...]] = []
            event_bytes = 0
            inserted_before_file = inserted
            try:
                with journal.open("r", encoding="utf-8") as handle:
                    for line_number, line in enumerate(handle, start=1):
                        event_bytes += len(line.encode("utf-8"))
                        try:
                            event = json.loads(line)
                            if not isinstance(event, dict) or not event.get("nodeid"):
                                raise ValueError("event has no nodeid")
                            event_key = _event_identity(event, line_number)
                            batch.append(
                                (
                                    event_key,
                                    str(project_root.resolve()),
                                    str(event.get("run_id") or run_id),
                                    str(event.get("source_path") or source_path),
                                    str(event.get("target_sha256") or "") or None,
                                    str(event.get("phase") or "unknown"),
                                    str(event.get("level") or "unknown"),
                                    str(event.get("mutant_id") or "") or None,
                                    str(event["nodeid"]),
                                    normalize_test_outcome(
                                        event.get("outcome"),
                                        was_xfail=bool(event.get("was_xfail") or event.get("xfail")),
                                    ),
                                    float(event.get("duration_ms", 0.0) or 0.0),
                                    int(bool(event.get("first_failure"))),
                                    str(event.get("worker_id") or "main"),
                                    int(bool(event.get("retry"))),
                                    str(event.get("recorded_at") or utc_now_iso()),
                                )
                            )
                        except (TypeError, ValueError, json.JSONDecodeError):
                            invalid += 1
                            continue
                        if len(batch) >= STATS_INSERT_BATCH_SIZE:
                            inserted += _insert_stats_batch(db_connection, batch)
                            batch.clear()
            except (OSError, UnicodeError):
                inserted += _insert_stats_batch(db_connection, batch)
                batch.clear()
                invalid += 1
                continue
            inserted += _insert_stats_batch(db_connection, batch)
            if metrics is not None:
                # Attribute raw event and resulting database traffic without buffering the journal.
                metrics.record_write(event_bytes, durability="normal", category="stats_event")
                inserted_for_file = inserted - inserted_before_file
                if inserted_for_file:
                    metrics.record_write(event_bytes, durability="normal", category="stats_database")
        db_connection.commit()
    except BaseException:
        db_connection.rollback()
        raise
    finally:
        if owns_connection:
            db_connection.close()
    return {
        "db_path": str(db_path.resolve()),
        "event_dir": str(event_dir.resolve()),
        "event_files": len(files),
        "events_ingested": inserted,
        "invalid_events": invalid,
    }
def _runtime_evidence_from_event(event: Mapping[str, Any]) -> dict[str, Any]:
    # Extract bounded runtime and environment evidence from one real pytest journal event.
    dependencies = event.get("runtime_dependencies", [])
    return {
        "runtime_dependencies": [
            dict(item)
            for item in dependencies
            if isinstance(item, Mapping) and item.get("path")
        ] if isinstance(dependencies, (list, tuple)) else [],
        "runtime_dependency_complete": bool(event.get("runtime_dependency_complete", True)),
        "runtime_dependency_blockers": [
            str(item)
            for item in event.get("runtime_dependency_blockers", [])
            if str(item)
        ] if isinstance(event.get("runtime_dependency_blockers", []), (list, tuple)) else [],
        "environment_dependencies": [
            str(item.get("name"))
            for item in event.get("environment_reads", [])
            if isinstance(item, Mapping) and str(item.get("name"))
        ] if isinstance(event.get("environment_reads", []), (list, tuple)) else [],
        "environment_blockers": [
            str(item)
            for item in event.get("environment_dependency_blockers", [])
            if str(item)
        ] if isinstance(event.get("environment_dependency_blockers", []), (list, tuple)) else [],
    }
def _mutant_observation_key(mutant_id: str, observation: Mapping[str, Any]) -> tuple[str, str, str, bool, str, str]:
    # Build a stable attempt-level identity so SQLite rows and their source JSONL event cannot duplicate evidence.
    return (
        str(mutant_id),
        str(observation.get("test_id", "")),
        str(observation.get("level", "")),
        bool(observation.get("retry", False)),
        str(observation.get("recorded_at", "")),
        str(observation.get("outcome", "")),
    )
def load_mutant_test_observations(
    db_path: Path,
    *,
    run_id: str,
    mutant_ids: Iterable[str],
    root: Path | None = None,
    event_dir: Path | None = None,
) -> dict[str, tuple[dict[str, Any], ...]]:
    # Load real per-test events from the exact current journal first and merge any durable SQLite rows.
    ids = tuple(sorted({str(item) for item in mutant_ids if str(item)}))
    if not ids:
        return {}
    observations: dict[str, dict[tuple[str, str, str, bool, str, str], dict[str, Any]]] = {
        mutant_id: {} for mutant_id in ids
    }
    runtime_by_test: dict[tuple[str, str], dict[str, Any]] = {}
    event_dirs = [
        *((Path(event_dir).resolve(),) if event_dir is not None else ()),
        *_runtime_event_directories(db_path, run_id),
    ]
    for candidate_dir in dict.fromkeys(event_dirs):
        if not candidate_dir.is_dir():
            continue
        for journal in sorted(candidate_dir.glob(f"{run_id}.*.jsonl")):
            try:
                handle = journal.open("r", encoding="utf-8")
            except (OSError, UnicodeError):
                continue
            with handle:
                for line in handle:
                    try:
                        event = json.loads(line)
                    except (TypeError, ValueError, json.JSONDecodeError):
                        continue
                    if (
                        not isinstance(event, Mapping)
                        or str(event.get("run_id", "")) != str(run_id)
                        or str(event.get("phase", "")) != "mutant"
                    ):
                        continue
                    mutant_id = str(event.get("mutant_id") or "")
                    raw_nodeid = str(event.get("nodeid") or "")
                    if mutant_id not in observations or not raw_nodeid:
                        continue
                    test_id = canonical_nodeid(root, raw_nodeid) if root is not None else raw_nodeid
                    runtime = _runtime_evidence_from_event(event)
                    runtime_by_test[(mutant_id, raw_nodeid)] = runtime
                    runtime_by_test[(mutant_id, test_id)] = runtime
                    observation = {
                        "test_id": test_id,
                        "outcome": normalize_test_outcome(
                            event.get("outcome"),
                            was_xfail=bool(event.get("was_xfail") or event.get("xfail")),
                        ),
                        "duration_ms": float(event.get("duration_ms", 0.0) or 0.0),
                        "first_failure": bool(event.get("first_failure", False)),
                        "worker_id": str(event.get("worker_id") or "main"),
                        "retry": bool(event.get("retry", False)),
                        "recorded_at": str(event.get("recorded_at") or ""),
                        "level": str(event.get("level") or "unknown"),
                        "evidence_kind": "pytest_test_event",
                        "observation_schema_version": 1,
                        **runtime,
                    }
                    observations[mutant_id][_mutant_observation_key(mutant_id, observation)] = observation
    rows: list[sqlite3.Row] = []
    if db_path.exists():
        placeholders = ",".join("?" for _ in ids)
        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row
        try:
            rows = connection.execute(
                f"SELECT mutant_id, nodeid, outcome, duration_ms, first_failure, worker_id, retry, recorded_at, level "
                f"FROM test_attempts WHERE run_id = ? AND phase = 'mutant' AND mutant_id IN ({placeholders}) "
                "ORDER BY mutant_id, nodeid, recorded_at, level",
                (run_id, *ids),
            ).fetchall()
        finally:
            connection.close()
    for row in rows:
        mutant_id = str(row["mutant_id"] or "")
        raw_nodeid = str(row["nodeid"] or "")
        if mutant_id not in observations or not raw_nodeid:
            continue
        test_id = canonical_nodeid(root, raw_nodeid) if root is not None else raw_nodeid
        observation = {
            "test_id": test_id,
            "outcome": str(row["outcome"]),
            "duration_ms": float(row["duration_ms"] or 0.0),
            "first_failure": bool(row["first_failure"]),
            "worker_id": str(row["worker_id"] or "main"),
            "retry": bool(row["retry"]),
            "recorded_at": str(row["recorded_at"]),
            "level": str(row["level"]),
            "evidence_kind": "pytest_test_event",
            "observation_schema_version": 1,
        }
        observation.update(
            runtime_by_test.get((mutant_id, raw_nodeid))
            or runtime_by_test.get((mutant_id, test_id))
            or {}
        )
        observations[mutant_id].setdefault(
            _mutant_observation_key(mutant_id, observation),
            observation,
        )
    return {
        mutant_id: tuple(
            sorted(
                rows_by_key.values(),
                key=lambda item: (
                    str(item.get("test_id", "")),
                    str(item.get("recorded_at", "")),
                    str(item.get("level", "")),
                    bool(item.get("retry", False)),
                ),
            )
        )
        for mutant_id, rows_by_key in observations.items()
        if rows_by_key
    }
def load_runtime_dependency_manifest(
    db_path: Path,
    *,
    run_id: str,
    nodeids: Iterable[str] = (),
    root: Path | None = None,
) -> dict[str, dict[str, Any]]:
    # Read per-test runtime dependency evidence from the append-only pytest journals.
    wanted = {canonical_nodeid(root, str(item)) if root is not None else str(item) for item in nodeids if str(item)}
    result: dict[str, dict[str, Any]] = {}
    for event_dir in _runtime_event_directories(db_path, run_id):
        if not event_dir.is_dir():
            continue
        for journal in sorted(event_dir.glob(f"{run_id}.*.jsonl")):
            try:
                lines = journal.read_text(encoding="utf-8").splitlines()
            except (OSError, UnicodeError):
                continue
            for line in lines:
                try:
                    event = json.loads(line)
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue
                if not isinstance(event, Mapping) or not event.get("nodeid"):
                    continue
                nodeid = canonical_nodeid(root, str(event["nodeid"])) if root is not None else str(event["nodeid"])
                if wanted and nodeid not in wanted:
                    continue
                dependencies = event.get("runtime_dependencies", [])
                normalized = tuple(
                    dict(item)
                    for item in dependencies
                    if isinstance(item, Mapping) and item.get("path")
                ) if isinstance(dependencies, (list, tuple)) else ()
                blockers = tuple(
                    str(item)
                    for item in event.get("runtime_dependency_blockers", [])
                    if str(item)
                ) if isinstance(event.get("runtime_dependency_blockers", []), (list, tuple)) else ()
                result[nodeid] = {
                    "dependencies": normalized,
                    "complete": bool(event.get("runtime_dependency_complete", not blockers)),
                "blockers": blockers,
                "environment_dependencies": tuple(
                    str(item.get("name"))
                    for item in event.get("environment_reads", [])
                    if isinstance(item, Mapping) and str(item.get("name"))
                ) if isinstance(event.get("environment_reads", []), (list, tuple)) else (),
                "environment_blockers": tuple(
                    str(item)
                    for item in event.get("environment_dependency_blockers", [])
                    if str(item)
                ) if isinstance(event.get("environment_dependency_blockers", []), (list, tuple)) else (),
            }
    return result
def merge_test_stats_databases(
    sources: Iterable[Path],
    target: Path,
    *,
    project_root: Path | None = None,
    metrics: PerformanceMetrics | None = None,
) -> dict[str, Any]:
    # Merge isolated worker databases while retaining idempotent event identity.
    inserted = 0
    source_count = 0
    connection = _connect(target)
    try:
        _ensure_schema(connection)
        for source in sorted({Path(item).resolve() for item in sources}):
            if not source.exists():
                continue
            source_count += 1
            source_connection = sqlite3.connect(source)
            try:
                cursor = source_connection.execute(
                    "SELECT event_key, project_root, run_id, source_path, target_sha256, phase, level, mutant_id, nodeid, outcome, duration_ms, first_failure, worker_id, retry, recorded_at FROM test_attempts"
                )
                while True:
                    rows = cursor.fetchmany(STATS_INSERT_BATCH_SIZE)
                    if not rows:
                        break
                    values_batch = []
                    for row in rows:
                        values = list(row)
                        values[0] = str(row[0])
                        if project_root is not None:
                            values[1] = str(project_root.resolve())
                        values_batch.append(tuple(values))
                    inserted += _insert_stats_batch(connection, values_batch)
                    if metrics is not None:
                        # Count one bounded database batch without materializing another copy of the source.
                        batch_bytes = sum(len(json.dumps(item, default=str)) for item in values_batch)
                        metrics.record_write(batch_bytes, durability="normal", category="stats_database")
            finally:
                source_connection.close()
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return {"db_path": str(target.resolve()), "source_databases": source_count, "events_ingested": inserted}
def classify_test_health(
    *,
    total_executions: int,
    passed_executions: int,
    failed_executions: int,
    error_executions: int,
    skipped_executions: int,
    timeout_executions: int,
    cancelled_executions: int = 0,
    unknown_executions: int = 0,
) -> str:
    # Derive health from every normalized outcome instead of treating only failed as unhealthy.
    total = max(0, int(total_executions))
    passed = max(0, int(passed_executions))
    failed = max(0, int(failed_executions))
    errors = max(0, int(error_executions))
    skipped = max(0, int(skipped_executions))
    timeouts = max(0, int(timeout_executions))
    cancelled = max(0, int(cancelled_executions))
    unknown = max(0, int(unknown_executions))
    if total == 0:
        return TestHealthStatus.INSUFFICIENT_DATA.value
    bad = failed + errors + timeouts + cancelled + unknown
    if passed and bad:
        return TestHealthStatus.FLAKY.value
    if passed and skipped > passed and not bad:
        return TestHealthStatus.MOSTLY_SKIPPED.value
    if passed:
        return TestHealthStatus.HEALTHY.value if total >= 2 else TestHealthStatus.INSUFFICIENT_DATA.value
    if errors + timeouts + cancelled and not failed and not skipped and not unknown:
        return TestHealthStatus.ERRORING.value
    if failed and not errors and not timeouts and not cancelled and not skipped and not unknown:
        return TestHealthStatus.FAILING.value
    if skipped and not failed and not errors and not timeouts and not cancelled and not unknown:
        return TestHealthStatus.MOSTLY_SKIPPED.value
    if unknown and not failed and not errors and not timeouts and not cancelled and not skipped:
        return TestHealthStatus.UNKNOWN.value
    return TestHealthStatus.NEVER_PASSED.value
def summarize_test_stats(
    db_path: Path,
    *,
    project_root: Path | None = None,
    nodeid: str | None = None,
    limit: int = 50,
    order_by: str = "kill_rate",
) -> list[dict[str, Any]]:
    # Compute counters, baseline/regression health and percentile timings without N+1 queries.
    if not db_path.exists():
        return []
    filters: list[str] = []
    parameters: list[str] = []
    if project_root is not None:
        filters.append("project_root = ?")
        parameters.append(str(project_root.resolve()))
    if nodeid:
        filters.append("nodeid = ?")
        parameters.append(nodeid)
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    connection = open_test_stats_connection(db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            f"""
            SELECT nodeid,
                   COUNT(*) AS executions,
                   SUM(CASE WHEN outcome = 'passed' THEN 1 ELSE 0 END) AS passed,
                   SUM(CASE WHEN outcome = 'failed' THEN 1 ELSE 0 END) AS failed,
                   SUM(CASE WHEN outcome = 'skipped' THEN 1 ELSE 0 END) AS skipped,
                   SUM(CASE WHEN outcome = 'xfailed' THEN 1 ELSE 0 END) AS xfailed,
                   SUM(CASE WHEN outcome = 'xpassed' THEN 1 ELSE 0 END) AS xpassed,
                   SUM(CASE WHEN outcome = 'cancelled' THEN 1 ELSE 0 END) AS cancelled,
                   SUM(CASE WHEN outcome = 'timeout' THEN 1 ELSE 0 END) AS timeout,
                   SUM(CASE WHEN outcome = 'unknown' THEN 1 ELSE 0 END) AS unknown,
                   SUM(CASE WHEN outcome IN ('error', 'cancelled', 'timeout', 'unknown') THEN 1 ELSE 0 END) AS errors,
                   SUM(CASE WHEN phase = 'baseline' AND outcome = 'failed' THEN 1 ELSE 0 END) AS baseline_failures,
                   SUM(CASE WHEN phase = 'baseline' AND outcome = 'passed' THEN 1 ELSE 0 END) AS baseline_passes,
                   SUM(CASE WHEN phase = 'standalone' AND outcome = 'failed' THEN 1 ELSE 0 END) AS standalone_failures,
                   SUM(CASE WHEN phase = 'standalone' AND outcome = 'passed' THEN 1 ELSE 0 END) AS standalone_passes,
                   SUM(CASE WHEN phase IN ('baseline', 'standalone') THEN 1 ELSE 0 END) AS health_executions,
                   SUM(CASE WHEN phase IN ('baseline', 'standalone') AND outcome = 'failed' THEN 1 ELSE 0 END) AS health_failures,
                   SUM(CASE WHEN phase IN ('baseline', 'standalone') AND outcome IN ('passed', 'xpassed') THEN 1 ELSE 0 END) AS health_passes,
                   SUM(CASE WHEN phase IN ('baseline', 'standalone') AND outcome = 'error' THEN 1 ELSE 0 END) AS health_errors,
                   SUM(CASE WHEN phase IN ('baseline', 'standalone') AND outcome IN ('skipped', 'xfailed') THEN 1 ELSE 0 END) AS health_skipped,
                   SUM(CASE WHEN phase IN ('baseline', 'standalone') AND outcome = 'timeout' THEN 1 ELSE 0 END) AS health_timeouts,
                   SUM(CASE WHEN phase IN ('baseline', 'standalone') AND outcome = 'cancelled' THEN 1 ELSE 0 END) AS health_cancelled,
                   SUM(CASE WHEN phase IN ('baseline', 'standalone') AND outcome = 'unknown' THEN 1 ELSE 0 END) AS health_unknown,
                   COUNT(DISTINCT CASE WHEN phase = 'mutant' AND mutant_id IS NOT NULL THEN mutant_id END) AS mutant_attempts,
                   COUNT(DISTINCT CASE WHEN phase = 'mutant' AND first_failure = 1 AND outcome = 'failed' THEN mutant_id END) AS mutant_kills,
                   SUM(duration_ms) AS total_duration_ms,
                   AVG(duration_ms) AS avg_duration_ms,
                   MAX(recorded_at) AS last_seen
            FROM test_attempts {where}
            GROUP BY nodeid
            """,
            parameters,
        ).fetchall()
        duration_rows = connection.execute(
            f"SELECT nodeid, duration_ms FROM test_duration_samples {where} ORDER BY nodeid, duration_ms",
            parameters,
        ).fetchall()
    finally:
        connection.close()
    durations: dict[str, list[float]] = {}
    for row in duration_rows:
        durations.setdefault(str(row[0]), []).append(float(row[1] or 0.0))
    result: list[dict[str, Any]] = []
    for row in rows:
        values = durations.get(str(row["nodeid"]), [])
        mutant_attempts = int(row["mutant_attempts"] or 0)
        mutant_kills = int(row["mutant_kills"] or 0)
        baseline_passes = int(row["baseline_passes"] or 0)
        baseline_failures = int(row["baseline_failures"] or 0)
        standalone_passes = int(row["standalone_passes"] or 0)
        standalone_failures = int(row["standalone_failures"] or 0)
        health_executions = int(row["health_executions"] or 0)
        health_passes = int(row["health_passes"] or 0)
        health_failures = int(row["health_failures"] or 0)
        health_errors = int(row["health_errors"] or 0)
        health_skipped = int(row["health_skipped"] or 0)
        health_timeouts = int(row["health_timeouts"] or 0)
        health_cancelled = int(row["health_cancelled"] or 0)
        health_unknown = int(row["health_unknown"] or 0)
        health_bad = health_failures + health_errors + health_timeouts + health_cancelled + health_unknown
        health_failure_rate = health_bad / health_executions if health_executions else 0.0
        is_flaky = health_passes > 0 and health_bad > 0
        health_status = classify_test_health(
            total_executions=health_executions,
            passed_executions=health_passes,
            failed_executions=health_failures,
            error_executions=health_errors,
            skipped_executions=health_skipped,
            timeout_executions=health_timeouts,
            cancelled_executions=health_cancelled,
            unknown_executions=health_unknown,
        )
        result.append(
            {
                "nodeid": str(row["nodeid"]),
                "executions": int(row["executions"] or 0),
                "passed": int(row["passed"] or 0),
                "failed": int(row["failed"] or 0),
                "skipped": int(row["skipped"] or 0),
                "xfailed": int(row["xfailed"] or 0),
                "xpassed": int(row["xpassed"] or 0),
                "cancelled": int(row["cancelled"] or 0),
                "timeout": int(row["timeout"] or 0),
                "unknown": int(row["unknown"] or 0),
                "errors": int(row["errors"] or 0),
                "baseline_passes": baseline_passes,
                "baseline_failures": baseline_failures,
                "standalone_passes": standalone_passes,
                "standalone_failures": standalone_failures,
                "regression_failures": standalone_failures,
                "health_executions": health_executions,
                "health_passes": health_passes,
                "health_failures": health_failures,
                "health_passed_executions": health_passes,
                "health_failed_executions": health_failures,
                "health_error_executions": health_errors,
                "health_skipped_executions": health_skipped,
                "health_timeout_executions": health_timeouts,
                "health_cancelled_executions": health_cancelled,
                "health_unknown_executions": health_unknown,
                "health_total_considered": health_executions,
                "health_failure_rate": health_failure_rate,
                "flaky": is_flaky,
                "flaky_rate": health_failure_rate if is_flaky else 0.0,
                "health_status": health_status,
                "mutant_attempts": mutant_attempts,
                "mutant_kills": mutant_kills,
                "kill_rate": mutant_kills / mutant_attempts if mutant_attempts else 0.0,
                "total_duration_ms": round(float(row["total_duration_ms"] or 0.0), 3),
                "avg_duration_ms": round(float(row["avg_duration_ms"] or 0.0), 3),
                "median_ms": round(_percentile(values, 0.5), 3) if values else 0.0,
                "p95_ms": round(_percentile(values, 0.95), 3) if values else 0.0,
                "last_seen": str(row["last_seen"] or ""),
            }
        )
    sort_keys = {
        "kill_rate": lambda value: (-value["kill_rate"], -value["mutant_kills"], value["nodeid"]),
        "duration": lambda value: (-value["median_ms"], -value["p95_ms"], value["nodeid"]),
        "executions": lambda value: (-value["executions"], value["nodeid"]),
        "failure_rate": lambda value: (-(value["failed"] / value["executions"] if value["executions"] else 0.0), value["nodeid"]),
        "flaky_rate": lambda value: (-value["flaky_rate"], -value["health_failures"], value["nodeid"]),
        "health": lambda value: (
            {
                TestHealthStatus.FAILING.value: 0,
                TestHealthStatus.ERRORING.value: 1,
                TestHealthStatus.NEVER_PASSED.value: 2,
                TestHealthStatus.MOSTLY_SKIPPED.value: 3,
                TestHealthStatus.FLAKY.value: 4,
                TestHealthStatus.UNKNOWN.value: 5,
                TestHealthStatus.INSUFFICIENT_DATA.value: 6,
                TestHealthStatus.HEALTHY.value: 7,
            }.get(value["health_status"], 8),
            -value["health_failure_rate"],
            value["nodeid"],
        ),
        "nodeid": lambda value: value["nodeid"],
    }
    result.sort(key=sort_keys.get(order_by, sort_keys["kill_rate"]))
    return result[: max(0, int(limit))]
def summarize_test_runs(
    db_path: Path,
    *,
    project_root: Path | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    # Aggregate the historical attempt journal by run_id in one bounded SQL query.
    if not db_path.exists() or int(limit) <= 0:
        return []
    filters: list[str] = []
    parameters: list[Any] = []
    if project_root is not None:
        filters.append("project_root = ?")
        parameters.append(str(project_root.resolve()))
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    parameters.append(max(0, int(limit)))
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            f"""
            SELECT run_id,
                   COUNT(*) AS executions,
                   COUNT(DISTINCT nodeid) AS tests,
                   SUM(CASE WHEN outcome = 'passed' THEN 1 ELSE 0 END) AS passed,
                   SUM(CASE WHEN outcome = 'failed' THEN 1 ELSE 0 END) AS failed,
                   SUM(CASE WHEN outcome = 'skipped' THEN 1 ELSE 0 END) AS skipped,
                   SUM(CASE WHEN outcome = 'xfailed' THEN 1 ELSE 0 END) AS xfailed,
                   SUM(CASE WHEN outcome = 'xpassed' THEN 1 ELSE 0 END) AS xpassed,
                   SUM(CASE WHEN outcome = 'cancelled' THEN 1 ELSE 0 END) AS cancelled,
                   SUM(CASE WHEN outcome = 'timeout' THEN 1 ELSE 0 END) AS timeout,
                   SUM(CASE WHEN outcome = 'unknown' THEN 1 ELSE 0 END) AS unknown,
                   SUM(CASE WHEN outcome IN ('error', 'cancelled', 'timeout', 'unknown') THEN 1 ELSE 0 END) AS errors,
                   SUM(duration_ms) AS total_duration_ms,
                   AVG(duration_ms) AS avg_duration_ms,
                   COUNT(DISTINCT CASE WHEN mutant_id IS NOT NULL THEN mutant_id END) AS mutant_attempts,
                   GROUP_CONCAT(DISTINCT phase) AS phases,
                   MIN(recorded_at) AS first_seen,
                   MAX(recorded_at) AS last_seen
            FROM test_attempts {where}
            GROUP BY run_id
            ORDER BY last_seen DESC, run_id ASC
            LIMIT ?
            """,
            parameters,
        ).fetchall()
    finally:
        connection.close()
    result: list[dict[str, Any]] = []
    for row in rows:
        executions = int(row["executions"] or 0)
        failed = int(row["failed"] or 0)
        errors = int(row["errors"] or 0)
        result.append(
            {
                "run_id": str(row["run_id"]),
                "executions": executions,
                "tests": int(row["tests"] or 0),
                "passed": int(row["passed"] or 0),
                "failed": failed,
                "skipped": int(row["skipped"] or 0),
                "xfailed": int(row["xfailed"] or 0),
                "xpassed": int(row["xpassed"] or 0),
                "cancelled": int(row["cancelled"] or 0),
                "timeout": int(row["timeout"] or 0),
                "unknown": int(row["unknown"] or 0),
                "errors": errors,
                "failure_rate": (failed + errors) / executions if executions else 0.0,
                "total_duration_ms": round(float(row["total_duration_ms"] or 0.0), 3),
                "avg_duration_ms": round(float(row["avg_duration_ms"] or 0.0), 3),
                "mutant_attempts": int(row["mutant_attempts"] or 0),
                "phases": sorted({item for item in str(row["phases"] or "").split(",") if item}),
                "first_seen": str(row["first_seen"] or ""),
                "last_seen": str(row["last_seen"] or ""),
            }
        )
    return result
def compare_test_runs(
    db_path: Path,
    before_run_id: str,
    after_run_id: str,
    *,
    project_root: Path | None = None,
    limit: int = 50,
    phases: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    # Compare two run snapshots with one grouped SQL query and deterministic status ordering.
    if before_run_id == after_run_id:
        raise ValueError("before and after run_id must be different")
    if not db_path.exists() or int(limit) <= 0:
        return []
    comparison_phases = _normalize_compare_phases(phases)
    where_parts = ["run_id IN (?, ?)"]
    parameters: list[Any] = [before_run_id]
    if project_root is not None:
        where_parts.insert(0, "project_root = ?")
        parameters.append(str(project_root.resolve()))
    parameters.extend((before_run_id, after_run_id))
    phase_placeholders = ", ".join("?" for _ in comparison_phases)
    where_parts.append(f"phase IN ({phase_placeholders})")
    parameters.extend(comparison_phases)
    where = " AND ".join(where_parts)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            f"""
            WITH scoped AS (
                SELECT nodeid,
                       outcome,
                       duration_ms,
                       CASE WHEN run_id = ? THEN 'before' ELSE 'after' END AS side
                FROM test_attempts
                WHERE {where}
            )
            SELECT nodeid,
                   SUM(CASE WHEN side = 'before' THEN 1 ELSE 0 END) AS before_executions,
                   SUM(CASE WHEN side = 'after' THEN 1 ELSE 0 END) AS after_executions,
                   SUM(CASE WHEN side = 'before' AND outcome = 'passed' THEN 1 ELSE 0 END) AS before_passed,
                   SUM(CASE WHEN side = 'after' AND outcome = 'passed' THEN 1 ELSE 0 END) AS after_passed,
                   SUM(CASE WHEN side = 'before' AND outcome = 'failed' THEN 1 ELSE 0 END) AS before_failed,
                   SUM(CASE WHEN side = 'after' AND outcome = 'failed' THEN 1 ELSE 0 END) AS after_failed,
                   SUM(CASE WHEN side = 'before' AND outcome = 'skipped' THEN 1 ELSE 0 END) AS before_skipped,
                   SUM(CASE WHEN side = 'after' AND outcome = 'skipped' THEN 1 ELSE 0 END) AS after_skipped,
                   SUM(CASE WHEN side = 'before' AND outcome IN ('error', 'cancelled', 'timeout', 'unknown') THEN 1 ELSE 0 END) AS before_errors,
                   SUM(CASE WHEN side = 'after' AND outcome IN ('error', 'cancelled', 'timeout', 'unknown') THEN 1 ELSE 0 END) AS after_errors,
                   AVG(CASE WHEN side = 'before' THEN duration_ms END) AS before_avg_duration_ms,
                   AVG(CASE WHEN side = 'after' THEN duration_ms END) AS after_avg_duration_ms
            FROM scoped
            GROUP BY nodeid
            """,
            parameters,
        ).fetchall()
    finally:
        connection.close()
    result: list[dict[str, Any]] = []
    status_order = {"regressed": 0, "new": 1, "recovered": 2, "changed": 3, "removed": 4, "stable": 5}
    for row in rows:
        before_executions = int(row["before_executions"] or 0)
        after_executions = int(row["after_executions"] or 0)
        before_failed = int(row["before_failed"] or 0)
        after_failed = int(row["after_failed"] or 0)
        before_errors = int(row["before_errors"] or 0)
        after_errors = int(row["after_errors"] or 0)
        before_bad = before_failed + before_errors
        after_bad = after_failed + after_errors
        if before_executions == 0:
            status = "new"
        elif after_executions == 0:
            status = "removed"
        elif after_bad > before_bad:
            status = "regressed"
        elif after_bad < before_bad:
            status = "recovered"
        elif (
            int(row["before_passed"] or 0),
            before_failed,
            int(row["before_skipped"] or 0),
            before_errors,
        ) != (
            int(row["after_passed"] or 0),
            after_failed,
            int(row["after_skipped"] or 0),
            after_errors,
        ):
            status = "changed"
        else:
            status = "stable"
        before_rate = before_bad / before_executions if before_executions else 0.0
        after_rate = after_bad / after_executions if after_executions else 0.0
        before_avg = round(float(row["before_avg_duration_ms"] or 0.0), 3)
        after_avg = round(float(row["after_avg_duration_ms"] or 0.0), 3)
        result.append(
            {
                "nodeid": str(row["nodeid"]),
                "before_run_id": before_run_id,
                "after_run_id": after_run_id,
                "status": status,
                "before_executions": before_executions,
                "after_executions": after_executions,
                "delta_executions": after_executions - before_executions,
                "before_passed": int(row["before_passed"] or 0),
                "after_passed": int(row["after_passed"] or 0),
                "before_failed": before_failed,
                "after_failed": after_failed,
                "before_skipped": int(row["before_skipped"] or 0),
                "after_skipped": int(row["after_skipped"] or 0),
                "before_errors": before_errors,
                "after_errors": after_errors,
                "delta_failures": after_bad - before_bad,
                "before_failure_rate": before_rate,
                "after_failure_rate": after_rate,
                "delta_failure_rate": after_rate - before_rate,
                "before_avg_duration_ms": before_avg,
                "after_avg_duration_ms": after_avg,
                "delta_avg_duration_ms": round(after_avg - before_avg, 3),
            }
        )
    result.sort(key=lambda value: (status_order.get(value["status"], 99), value["nodeid"]))
    return result[: max(0, int(limit))]
def _percentile(values: list[float], percentile: float) -> float:
    # Interpolate a bounded percentile from already sorted test durations.
    if not values:
        return 0.0
    position = (len(values) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    fraction = position - lower
    return values[lower] + (values[upper] - values[lower]) * fraction
def load_selection_stats(db_path: Path, project_root: Path) -> dict[str, dict[str, Any]]:
    # Expose historical execution, kill and test-health signals to the selection ranker.
    try:
        rows = summarize_test_stats(db_path, project_root=project_root, limit=1_000_000, order_by="nodeid")
    except (OSError, sqlite3.Error, ValueError):
        return {}
    return {
        row["nodeid"]: {
            "test_executions": row["executions"],
            "test_kills": row["mutant_kills"],
            "test_median_ms": row["median_ms"],
            "test_health_executions": row["health_executions"],
            "test_health_failure_rate": row["health_failure_rate"],
            "test_flaky_rate": row["flaky_rate"],
            "test_baseline_failures": row["baseline_failures"],
            "test_regression_failures": row["regression_failures"],
            "test_health_status": row["health_status"],
        }
        for row in rows
    }
def summarize_test_health(db_path: Path, *, project_root: Path | None = None) -> dict[str, Any]:
    # Summarize the number of healthy, flaky and failing tests for report metadata.
    try:
        rows = summarize_test_stats(db_path, project_root=project_root, limit=1_000_000, order_by="nodeid")
    except (OSError, sqlite3.Error, ValueError):
        return summarize_test_health_rows(())
    return summarize_test_health_rows(rows)
def summarize_test_health_rows(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    # Derive every health category from already materialized test rows without reopening SQLite.
    counts = {status.value: 0 for status in TestHealthStatus}
    tests = 0
    baseline_failures = 0
    regression_failures = 0
    passed_executions = 0
    failed_executions = 0
    error_executions = 0
    skipped_executions = 0
    timeout_executions = 0
    cancelled_executions = 0
    unknown_executions = 0
    total_considered = 0
    for row in rows:
        tests += 1
        status = str(row.get("health_status", "insufficient_data"))
        if status not in counts:
            status = TestHealthStatus.UNKNOWN.value
        counts[status] = counts.get(status, 0) + 1
        baseline_failures += int(row.get("baseline_failures", 0) or 0)
        regression_failures += int(row.get("regression_failures", 0) or 0)
        passed_executions += int(row.get("health_passed_executions", row.get("health_passes", 0)) or 0)
        failed_executions += int(row.get("health_failed_executions", row.get("health_failures", 0)) or 0)
        error_executions += int(row.get("health_error_executions", row.get("health_errors", 0)) or 0)
        skipped_executions += int(row.get("health_skipped_executions", row.get("health_skipped", 0)) or 0)
        timeout_executions += int(row.get("health_timeout_executions", row.get("health_timeouts", 0)) or 0)
        cancelled_executions += int(row.get("health_cancelled_executions", row.get("health_cancelled", 0)) or 0)
        unknown_executions += int(row.get("health_unknown_executions", row.get("health_unknown", 0)) or 0)
        total_considered += int(row.get("health_total_considered", row.get("health_executions", 0)) or 0)
    return {
        "tests": tests,
        **counts,
        "passed_executions": passed_executions,
        "failed_executions": failed_executions,
        "error_executions": error_executions,
        "skipped_executions": skipped_executions,
        "timeout_executions": timeout_executions,
        "cancelled_executions": cancelled_executions,
        "unknown_executions": unknown_executions,
        "total_considered_executions": total_considered,
        "baseline_failures": baseline_failures,
        "regression_failures": regression_failures,
    }
