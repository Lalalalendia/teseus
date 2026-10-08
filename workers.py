"""Legacy compatibility workers retained for pre-E13 tests and resume commands.

The production coordinator backend is ``theseus_local.worker_runtime``.  This
module remains import-compatible for the historical CLI and recovery tests
until the legacy parity gate is explicitly closed.
"""

from __future__ import annotations

import multiprocessing
import os
import signal
import shutil
import subprocess
import time
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, ThreadPoolExecutor, wait
from dataclasses import dataclass, fields, replace
from datetime import datetime
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Any, Sequence

from .index import load_index, load_context_map
from .io_utils import append_json_line, atomic_write_json, atomic_write_text, ensure_dir, read_json, sha256_file, utc_now_iso
from .models import CampaignAccumulator, Mutant, PerformanceMetrics
from .mutations import RestoreError, SourceSnapshot, create_snapshot, recover_manifest
from .recovery import (
    JournalReplayError,
    campaign_input_fingerprint,
    current_process_birth_token,
    journal_metadata,
    merge_authoritative_results,
    owner_is_alive,
    replay_result_journal,
    result_identity_digests,
    verify_checkpoint,
)
from .test_stats import merge_test_stats_databases, stats_db_path
from .preparation_service import PreparedCampaign, prepare_campaign as _prepare_campaign  # noqa: F401
from .runner import (
    RUNNER_VERSION,
    MutationConfig,
    MutationRunner,
    LevelSpec,
    StaleInputError,
)


@dataclass(frozen=True)
class WorkerShard:
    """A stable mutant shard assigned to one isolated worker."""

    worker_id: str
    mutant_ids: tuple[str, ...]
    estimated_cost: float

    def to_dict(self) -> dict[str, Any]:
        # Serialize the shard manifest with stable list fields.
        return {
            "worker_id": self.worker_id,
            "mutant_ids": list(self.mutant_ids),
            "estimated_cost": round(self.estimated_cost, 4),
        }


class ResumeError(RuntimeError):
    """Raised when a worker campaign cannot be resumed safely."""


class SharedBaselineProvider:
    """Thread-safe coordinator-owned baseline results shared by worker runners."""

    def __init__(
        self,
        runner: MutationRunner,
        snapshot: SourceSnapshot,
        selection: Any,
        function_info: dict[str, Any] | None,
        report_id: str,
        levels: Sequence[LevelSpec],
    ) -> None:
        # Keep one coordinator baseline per escalation level while workers request it lazily.
        self._runner = runner
        self._snapshot = snapshot
        self._selection = selection
        self._function_info = function_info
        self._report_id = report_id
        self._levels = {level.name: level for level in levels}
        self._results: dict[str, dict[str, Any]] = {}
        self._lock = Lock()

    def get(self, levels: Sequence[LevelSpec]) -> list[dict[str, Any]]:
        # Return shared baseline rows without executing the same command in each worker.
        results: list[dict[str, Any]] = []
        for requested in levels:
            with self._lock:
                result = self._results.get(requested.name)
                if result is None:
                    coordinator_level = self._levels.get(requested.name)
                    if coordinator_level is None:
                        raise RuntimeError(f"unknown shared baseline level: {requested.name}")
                    result = self._runner._run_baselines(
                        (coordinator_level,),
                        self._snapshot,
                        self._selection,
                        self._function_info,
                        self._report_id,
                    )[0]
                    self._results[requested.name] = result
                shared = dict(result)
            shared["baseline_reused"] = True
            shared["shared_baseline"] = True
            for path_key in ("output_artifact", "cache_artifact"):
                value = shared.get(path_key)
                if value and not Path(str(value)).is_absolute():
                    shared[path_key] = str((self._runner.reports_dir / str(value)).resolve())
            results.append(shared)
        return results


class StaticBaselineProvider:
    """Pickle-safe coordinator baseline rows used by process workers."""

    def __init__(self, rows: Sequence[dict[str, Any]]) -> None:
        # Freeze coordinator results before they cross the process boundary.
        self._rows = {str(row.get("level", "")): dict(row) for row in rows}

    def get(self, levels: Sequence[LevelSpec]) -> list[dict[str, Any]]:
        # Return immutable baseline observations without executing duplicate commands.
        results: list[dict[str, Any]] = []
        for level in levels:
            if level.name not in self._rows:
                raise RuntimeError(f"missing shared baseline level: {level.name}")
            shared = dict(self._rows[level.name])
            shared["baseline_reused"] = True
            shared["shared_baseline"] = True
            results.append(shared)
        return results


def _worker_heartbeat(
    worker_meta_path: Path,
    metadata: dict[str, Any],
    stop_event: Event,
    *,
    interval_seconds: float = 1.0,
) -> None:
    # Publish a bounded lease heartbeat while a process worker owns its shard.
    sequence = 0
    while not stop_event.wait(interval_seconds):
        sequence += 1
        metadata["heartbeat_at"] = utc_now_iso()
        metadata["heartbeat_seq"] = sequence
        try:
            atomic_write_json(worker_meta_path, metadata, durability="normal", category="manifest")
        except (OSError, TypeError, ValueError):
            continue


def _stop_worker_heartbeat(stop_event: Event, heartbeat: Thread) -> None:
    # Stop lease updates before publishing the final worker metadata record.
    stop_event.set()
    heartbeat.join(timeout=2.0)


def _detach_worker_process_group() -> None:
    # Give each process worker a killable group without affecting the coordinator.
    if os.name != "nt":
        try:
            os.setsid()
        except OSError:
            pass


def _terminate_worker_pid(pid: object) -> bool:
    # Terminate one worker process tree through the platform-safe process boundary.
    try:
        worker_pid = int(pid)
    except (TypeError, ValueError):
        return False
    if worker_pid <= 0:
        return False
    if os.name == "nt":
        try:
            result = subprocess.run(
                ["taskkill", "/PID", str(worker_pid), "/T", "/F"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                shell=False,
                timeout=5,
            )
        except (OSError, subprocess.SubprocessError):
            return False
        return result.returncode == 0
    try:
        os.killpg(os.getpgid(worker_pid), signal.SIGTERM)
    except (OSError, ProcessLookupError):
        try:
            os.kill(worker_pid, signal.SIGTERM)
        except (OSError, ProcessLookupError):
            return False
    return True


def _safe_name(value: str) -> str:
    # Keep coordinator filenames portable across Windows and POSIX.
    return "".join(char if char.isalnum() or char in "_.-" else "_" for char in value)[:100]


def _mutant_cost(mutant: Mutant) -> float:
    # Estimate runtime from operator shape while keeping ties deterministic.
    weights = {
        "return_value_to_none": 1.35,
        "condition_to_not": 1.25,
        "remove_standalone_call": 1.45,
        "raise_to_pass": 1.10,
        "empty_list_to_none": 1.05,
        "empty_dict_to_sentinel": 1.05,
        "await_to_expression": 1.30,
    }
    return weights.get(mutant.mutation, 1.0)


def shard_mutants(mutants: Sequence[Mutant], workers: int) -> tuple[WorkerShard, ...]:
    # Greedily balance estimated cost with deterministic worker tie-breaking.
    count = max(1, min(int(workers), len(mutants) or 1))
    buckets: list[list[Mutant]] = [[] for _ in range(count)]
    loads = [0.0 for _ in range(count)]
    ordered = sorted(mutants, key=lambda item: (-_mutant_cost(item), item.mutant_id))
    for mutant in ordered:
        target = min(range(count), key=lambda index: (loads[index], index))
        buckets[target].append(mutant)
        loads[target] += _mutant_cost(mutant)
    return tuple(
        WorkerShard(
            worker_id=f"worker-{index:03d}",
            mutant_ids=tuple(sorted(item.mutant_id for item in bucket)),
            estimated_cost=loads[index],
        )
        for index, bucket in enumerate(buckets)
        if bucket
    )


def _relative_to(path: Path, root: Path) -> Path | None:
    # Return a relative path only when it is safely inside the supplied root.
    try:
        return path.resolve().relative_to(root.resolve())
    except ValueError:
        return None


def _remap_token(value: str, original_root: Path, worker_root: Path) -> str:
    # Rewrite absolute project arguments while preserving external executables and data.
    candidate = Path(value)
    relative = _relative_to(candidate, original_root) if candidate.is_absolute() else None
    if relative is not None:
        return str(worker_root / relative)
    if "=" in value:
        prefix, suffix = value.split("=", 1)
        candidate = Path(suffix)
        relative = _relative_to(candidate, original_root) if candidate.is_absolute() else None
        if relative is not None:
            return f"{prefix}={worker_root / relative}"
    return value


def _remap_argv(argv: Sequence[str] | None, original_root: Path, worker_root: Path) -> tuple[str, ...] | None:
    # Remap command paths without changing relative pytest nodeids or flags.
    if argv is None:
        return None
    return tuple(_remap_token(str(value), original_root, worker_root) for value in argv)


def _git_is_clean(root: Path) -> bool:
    # Use a real git worktree only when it cannot lose uncommitted project state.
    if not (root / ".git").exists():
        return False
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "status", "--porcelain", "--untracked-files=all"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return result.returncode == 0 and not result.stdout.strip()


def _copy_ignore(reports_dir: Path, root: Path):
    # Avoid copying caches and coordinator reports into a worker project.
    names = {".git", ".hg", ".venv", "venv", "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache"}
    reports_relative = _relative_to(reports_dir, root)
    if reports_relative is not None and reports_relative.parts:
        names.add(reports_relative.parts[0])

    def ignore(directory: str, entries: list[str]) -> set[str]:
        # Apply the same conservative exclusions at every copied directory.
        return {
            entry
            for entry in entries
            if entry in names or entry.endswith(".test_intelligence.lock") or entry.startswith(".test_intelligence_workers")
        }

    return ignore


def _create_workspace(config: MutationConfig, run_id: str, shard: WorkerShard) -> dict[str, Any]:
    # Create a disposable git worktree or byte-for-byte project copy for a shard.
    root = config.project_root.resolve()
    worker_root = ensure_dir(config.reports_dir / "workers" / run_id / shard.worker_id)
    project_root = worker_root / "project"
    worker_reports = worker_root / "reports"
    if project_root.exists():
        shutil.rmtree(project_root)
    mode = "copy"
    if _git_is_clean(root):
        try:
            result = subprocess.run(
                ["git", "-C", str(root), "worktree", "add", "--detach", str(project_root), "HEAD"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
                timeout=30,
            )
            mode = "git-worktree" if result.returncode == 0 else "copy"
        except (OSError, subprocess.SubprocessError):
            mode = "copy"
    if mode == "copy":
        if project_root.exists():
            shutil.rmtree(project_root)
        shutil.copytree(root, project_root, symlinks=True, ignore=_copy_ignore(config.reports_dir, root))
    ensure_dir(worker_reports)
    return {
        "worker_root": worker_root,
        "project_root": project_root,
        "reports_dir": worker_reports,
        "isolation_mode": mode,
    }


def _remove_workspace(original_root: Path, workspace: dict[str, Any]) -> None:
    # Remove only the exact worker project created by this coordinator.
    project_root = Path(workspace["project_root"])
    if not project_root.exists():
        return
    if workspace.get("isolation_mode") == "git-worktree":
        try:
            result = subprocess.run(
                ["git", "-C", str(original_root), "worktree", "remove", "--force", str(project_root)],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=30,
            )
            if result.returncode == 0:
                return
        except (OSError, subprocess.SubprocessError):
            pass
    shutil.rmtree(project_root)


def _worker_config(
    config: MutationConfig,
    workspace: dict[str, Any],
    shard: WorkerShard,
    selection: dict[str, Any],
    index_snapshot: dict[str, Any],
    baseline_provider: SharedBaselineProvider,
) -> MutationConfig:
    # Bind one shard to its isolated root and freeze the coordinator selection.
    original_root = config.project_root.resolve()
    worker_root = Path(workspace["project_root"])
    worker_index = dict(index_snapshot)
    worker_index["project_root"] = str(worker_root)
    python_executable = config.python_executable
    if python_executable is None:
        from .commands import resolve_python

        python_executable = resolve_python(original_root, None)
    return replace(
        config,
        project_root=worker_root,
        index_path=None,
        index_snapshot=worker_index,
        context_map_path=config.context_map_path,
        impact_db=config.impact_db,
        selected_tests_file=config.selected_tests_file,
        test_command_argv=_remap_argv(config.test_command_argv, original_root, worker_root),
        domain=(_remap_token(config.domain, original_root, worker_root) if config.domain else None),
        domain_command_argv=_remap_argv(config.domain_command_argv, original_root, worker_root),
        common_command_argv=_remap_argv(config.common_command_argv, original_root, worker_root),
        python_executable=python_executable,
        reports_dir=Path(workspace["reports_dir"]),
        mutant_ids=frozenset(shard.mutant_ids),
        selection_snapshot=selection,
        workers=1,
        shared_baseline_provider=baseline_provider,
        compact_report=config.compact_report,
    )


def _write_worker_meta(workspace: dict[str, Any], value: dict[str, Any]) -> None:
    # Persist worker lifecycle state independently from the runner report.
    target = Path(workspace["worker_root"]) / "worker.json"
    attempts = 4 if os.name == "nt" else 1
    for attempt in range(attempts):
        try:
            atomic_write_json(target, value, durability="normal", category="manifest")
            return
        except PermissionError:
            if attempt + 1 >= attempts:
                raise
            time.sleep(0.05 * (attempt + 1))


def _safe_worker_cleanup(report: dict[str, Any]) -> bool:
    # Keep failed or unrestored projects available for explicit recovery.
    status = str(report.get("status", "error"))
    if status not in {"complete", "no_mutants", "no_l1_selection", "baseline_failed", "stale_index", "stale_snapshot"}:
        return False
    return all(bool(item.get("restore_verified", True)) for item in report.get("results", []) if isinstance(item, dict))


def _report_relative(path: Path, reports_dir: Path) -> str:
    # Keep merged report references portable relative to the coordinator reports directory.
    try:
        return path.resolve().relative_to(reports_dir.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def _run_worker(
    config: MutationConfig,
    run_id: str,
    shard: WorkerShard,
    selection: dict[str, Any],
    index_snapshot: dict[str, Any],
    baseline_provider: Any,
) -> dict[str, Any]:
    # Execute one shard and retain its workspace whenever recovery may be needed.
    _detach_worker_process_group()
    workspace = _create_workspace(config, run_id, shard)
    worker_reports = Path(workspace["reports_dir"])
    lease_id = f"{run_id}:{shard.worker_id}:{time.time_ns()}"
    meta = {
        "schema_version": 1,
        "worker_id": shard.worker_id,
        "pid": os.getpid(),
        "status": "starting",
        "lease_id": lease_id,
        "lease_seconds": 10,
        "heartbeat_at": utc_now_iso(),
        "heartbeat_seq": 0,
        "started_at": utc_now_iso(),
        "mutant_ids": list(shard.mutant_ids),
        "estimated_cost": shard.estimated_cost,
        "isolation_mode": workspace["isolation_mode"],
        "project_root": str(workspace["project_root"]),
        "reports_dir": str(worker_reports),
        "created_at": utc_now_iso(),
        "index_snapshot_version": str(index_snapshot.get("index_version", "")),
        "shared_baseline": True,
    }
    worker_meta_path = Path(workspace["worker_root"]) / "worker.json"
    _write_worker_meta(workspace, meta)
    heartbeat_stop = Event()
    heartbeat = Thread(
        target=_worker_heartbeat,
        args=(worker_meta_path, meta, heartbeat_stop),
        name=f"theseus-heartbeat-{shard.worker_id}",
        daemon=True,
    )
    heartbeat.start()
    try:
        report = MutationRunner(
            _worker_config(config, workspace, shard, selection, index_snapshot, baseline_provider)
        ).run()
        worker_stats_db = None
        stats_info = report.get("metrics", {}).get("test_stats", {}) if isinstance(report, dict) else {}
        stats_value = stats_info.get("db_path") if isinstance(stats_info, dict) else None
        if stats_value:
            source_stats_db = Path(str(stats_value))
            if source_stats_db.exists():
                worker_stats_db = ensure_dir(config.reports_dir / "test_stats_workers") / f"{run_id}.{shard.worker_id}.sqlite"
                shutil.copy2(source_stats_db, worker_stats_db)
        meta.update(
            {
                "status": report.get("status", "error"),
                "report_path": report.get("report_path"),
                "finished_at": utc_now_iso(),
                "recovery_manifests": [str(path) for path in sorted(worker_reports.glob("*.manifest.json"))],
                "test_stats_db": str(worker_stats_db) if worker_stats_db else None,
            }
        )
        if _safe_worker_cleanup(report):
            _remove_workspace(config.project_root.resolve(), workspace)
            meta["project_cleaned"] = True
        else:
            meta["project_cleaned"] = False
        _stop_worker_heartbeat(heartbeat_stop, heartbeat)
        _write_worker_meta(workspace, meta)
        return {
            "worker_id": shard.worker_id,
            "status": report.get("status", "error"),
            "report": report,
            "report_path": str(report.get("report_path", "")),
            "workspace": meta,
        }
    except BaseException as exc:
        meta.update({"status": "error", "error": str(exc), "finished_at": utc_now_iso(), "project_cleaned": False})
        _stop_worker_heartbeat(heartbeat_stop, heartbeat)
        _write_worker_meta(workspace, meta)
        return {
            "worker_id": shard.worker_id,
            "status": "error",
            "error": str(exc),
            "report": None,
            "report_path": "",
            "workspace": meta,
        }


def _retry_worker_once(
    config: MutationConfig,
    run_id: str,
    shard: WorkerShard,
    selection: dict[str, Any],
    index_snapshot: dict[str, Any],
    baseline_provider: StaticBaselineProvider,
    previous: dict[str, Any],
    reports_dir: Path,
) -> dict[str, Any]:
    # Recover an interrupted shard before one bounded process retry on a fresh workspace.
    unresolved = _recover_worker_manifests({"workers": [previous]}, reports_dir)
    if unresolved:
        failed = dict(previous)
        failed["retry_error"] = "; ".join(item["error"] for item in unresolved)
        return failed
    retry_run_id = f"{run_id}.retry1"
    try:
        with ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context("spawn")) as pool:
            future = pool.submit(
                _run_worker,
                config,
                retry_run_id,
                shard,
                selection,
                index_snapshot,
                baseline_provider,
            )
            result = future.result()
    except BaseException as exc:
        result = {
            "worker_id": shard.worker_id,
            "status": "error",
            "error": str(exc),
            "report": None,
            "workspace": previous.get("workspace", {}),
        }
    result["retry_count"] = 1
    result["retry_run_id"] = retry_run_id
    return result


def _retry_failed_workers(
    config: MutationConfig,
    run_id: str,
    shards: Sequence[WorkerShard],
    selection: dict[str, Any],
    index_snapshot: dict[str, Any],
    baseline_provider: StaticBaselineProvider,
    records: dict[str, dict[str, Any]],
    manifest: dict[str, Any],
    workers_manifest: Path,
    reports_dir: Path,
) -> None:
    # Retry only process failures that produced no durable worker report, at most once per shard.
    for shard in shards:
        previous = records.get(shard.worker_id, {})
        if previous.get("status") != "error" or isinstance(previous.get("report"), dict):
            continue
        try:
            current_manifest = read_json(workers_manifest)
        except (OSError, ValueError):
            current_manifest = {}
        if isinstance(current_manifest, dict) and current_manifest.get("status") == "cancelled":
            return
        initial_error = str(previous.get("error", "worker process failed"))
        retrying = dict(previous)
        retrying.update({"status": "retrying", "retry_count": 0, "initial_error": initial_error})
        records[shard.worker_id] = retrying
        manifest["workers"] = [_public_record(records[key], reports_dir) for key in sorted(records)]
        atomic_write_json(workers_manifest, manifest, durability="normal", category="manifest")
        result = _retry_worker_once(
            config,
            run_id,
            shard,
            selection,
            index_snapshot,
            baseline_provider,
            retrying,
            reports_dir,
        )
        result.setdefault("retry_count", 1)
        result.setdefault("retry_run_id", f"{run_id}.retry1")
        result["initial_error"] = initial_error
        records[shard.worker_id] = result
        manifest["workers"] = [_public_record(records[key], reports_dir) for key in sorted(records)]
        atomic_write_json(workers_manifest, manifest, durability="normal", category="manifest")


def _aggregate_performance(records: Sequence[dict[str, Any]]) -> PerformanceMetrics:
    # Sum additive worker counters while retaining the existing metrics schema.
    aggregate = PerformanceMetrics()
    for record in records:
        report = record.get("report")
        performance = report.get("metrics", {}).get("performance", {}) if isinstance(report, dict) else {}
        for item in fields(PerformanceMetrics):
            value = performance.get(item.name)
            if isinstance(value, (int, float)):
                setattr(aggregate, item.name, getattr(aggregate, item.name) + value)
    wall_values = [
        float(
            (record.get("report", {}).get("metrics", {}).get("performance", {}) or {}).get(
                "campaign_wall_seconds", 0.0
            )
        )
        for record in records
        if isinstance(record.get("report"), dict)
    ]
    aggregate.worker_sum_seconds = sum(wall_values)
    aggregate.worker_max_seconds = max(wall_values, default=0.0)
    aggregate.worker_critical_path_seconds = aggregate.worker_max_seconds
    return aggregate


_PATH_KEYS = {
    "report_path",
    "markdown_path",
    "results_journal",
    "state_path",
    "recovery_manifest",
    "output_artifact",
    "output_artifacts",
    "cache_artifact",
    "artifact_path",
    "worker_report",
    "db_path",
    "event_dir",
}


def _translate_paths(value: Any, worker_reports: Path, coordinator_reports: Path, key: str | None = None) -> Any:
    # Rewrite worker-relative artifacts while leaving test and source paths intact.
    if isinstance(value, dict):
        return {name: _translate_paths(item, worker_reports, coordinator_reports, name) for name, item in value.items()}
    if isinstance(value, list):
        return [_translate_paths(item, worker_reports, coordinator_reports, key) for item in value]
    if key not in _PATH_KEYS or not isinstance(value, str) or not value:
        return value
    path = Path(value)
    if not path.is_absolute():
        path = worker_reports / path
    return _report_relative(path, coordinator_reports)


def _worker_results(record: dict[str, Any], coordinator_reports: Path) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    # Load report or journal results and annotate each row with its worker provenance.
    report = record.get("report")
    if not isinstance(report, dict):
        return [], None
    report_path = Path(str(record.get("report_path", "")))
    worker_reports = report_path.parent if report_path else coordinator_reports
    values = report.get("results", [])
    journal = report.get("results_journal")
    journal_path = Path(str(journal)) if journal else None
    if journal_path and not journal_path.is_absolute():
        journal_path = worker_reports / journal_path
    if journal_path and journal_path.exists():
        try:
            values = list(replay_result_journal(journal_path).rows)
        except JournalReplayError as exc:
            raise ResumeError(f"cannot replay worker journal {journal_path}: {exc}") from exc
    if not isinstance(values, list):
        values = []
    translated: list[dict[str, Any]] = []
    worker_report_ref = _report_relative(report_path, coordinator_reports) if report_path else None
    for item in values:
        if not isinstance(item, dict):
            continue
        current = _translate_paths(item, worker_reports, coordinator_reports)
        current["worker_id"] = record.get("worker_id")
        current["worker_report"] = worker_report_ref
        translated.append(current)
    return translated, _translate_paths(report, worker_reports, coordinator_reports)


def _public_record(record: dict[str, Any], coordinator_reports: Path) -> dict[str, Any]:
    # Remove in-memory report payloads before writing the coordinator manifest.
    value = {key: item for key, item in record.items() if key != "report"}
    workspace = value.get("workspace")
    if isinstance(workspace, dict):
        workspace = dict(workspace)
        for key in ("project_root", "reports_dir", "report_path"):
            if workspace.get(key):
                workspace[key] = _report_relative(Path(str(workspace[key])), coordinator_reports)
        value["workspace"] = workspace
    if value.get("report_path"):
        value["report_path"] = _report_relative(Path(str(value["report_path"])), coordinator_reports)
    return value


def _resolve_report_path(value: str | Path, reports_dir: Path) -> Path:
    # Resolve manifest paths relative to the coordinator reports directory.
    path = Path(value)
    return (path if path.is_absolute() else reports_dir / path).resolve()


def inspect_worker_workspaces(reports_dir: Path) -> dict[str, Any]:
    # Inspect nested worker projects without changing files or deleting recovery data.
    root = reports_dir.resolve()
    manifests: list[dict[str, Any]] = []
    active: list[str] = []
    stale: list[str] = []
    missing: list[str] = []
    for path in sorted(root.rglob("*.workers.manifest.json")):
        try:
            value = read_json(path)
        except (OSError, ValueError):
            continue
        if not isinstance(value, dict):
            continue
        status = str(value.get("status", "unknown"))
        lease_diagnostics = reconcile_worker_leases(value)
        manifest_info = {
            "manifest": str(path),
            "status": status,
            "workers": [],
            "lease_diagnostics": lease_diagnostics,
        }
        for worker in value.get("workers", []):
            if not isinstance(worker, dict):
                continue
            workspace = worker.get("workspace", {})
            if not isinstance(workspace, dict) or not workspace.get("project_root"):
                continue
            project = _resolve_report_path(str(workspace["project_root"]), root)
            if _relative_to(project, root) is None:
                continue
            exists = project.exists()
            item = {
                "worker_id": worker.get("worker_id"),
                "project_root": str(project),
                "exists": exists,
                "isolation_mode": workspace.get("isolation_mode"),
                "project_cleaned": bool(workspace.get("project_cleaned", False)),
            }
            manifest_info["workers"].append(item)
            if not exists:
                if status == "active":
                    missing.append(str(project))
            elif status == "active":
                active.append(str(project))
            elif status in {"complete", "resumed", "no_mutants", "baseline_failed", "no_l1_selection"}:
                stale.append(str(project))
        manifests.append(manifest_info)
    return {
        "reports_dir": str(root),
        "manifests": manifests,
        "active_workspaces": active,
        "stale_workspaces": stale,
        "missing_workspaces": missing,
        "active_manifests": [item["manifest"] for item in manifests if item["status"] == "active"],
        "orphaned_workers": [
            worker_id
            for item in manifests
            for worker_id in item.get("lease_diagnostics", {}).get("orphaned_workers", [])
        ],
    }


def cleanup_worker_workspaces(reports_dir: Path, *, keep_days: int = 30, dry_run: bool = True) -> dict[str, Any]:
    # Plan or remove only old completed worker projects, preserving active/error runs.
    root = reports_dir.resolve()
    cutoff = time.time() - max(0, int(keep_days)) * 86400
    planned: list[str] = []
    skipped_active: list[str] = []
    for path in sorted(root.rglob("*.workers.manifest.json")):
        try:
            value = read_json(path)
        except (OSError, ValueError):
            continue
        if not isinstance(value, dict):
            continue
        status = str(value.get("status", "unknown"))
        if status == "active":
            skipped_active.append(str(path))
            continue
        if status not in {"complete", "resumed", "no_mutants", "baseline_failed", "no_l1_selection"}:
            continue
        try:
            if path.stat().st_mtime > cutoff:
                continue
        except OSError:
            continue
        original_root = Path(str(value.get("project_root", ""))).resolve()
        for worker in value.get("workers", []):
            if not isinstance(worker, dict):
                continue
            workspace = worker.get("workspace", {})
            if not isinstance(workspace, dict) or not workspace.get("project_root"):
                continue
            project = _resolve_report_path(str(workspace["project_root"]), root)
            if _relative_to(project, root) is None or not project.exists():
                continue
            planned.append(str(project))
            if not dry_run:
                _remove_workspace(
                    original_root,
                    {"project_root": project, "isolation_mode": workspace.get("isolation_mode", "copy")},
                )
    return {
        "reports_dir": str(root),
        "dry_run": dry_run,
        "planned": planned,
        "skipped_active": skipped_active,
    }


def _pid_alive(pid: object) -> bool:
    # Check a coordinator PID without treating permission errors as proof of death.
    try:
        os.kill(int(pid), 0)
    except (TypeError, ValueError, ProcessLookupError):
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def reconcile_worker_leases(
    manifest: dict[str, Any],
    *,
    now: float | None = None,
) -> dict[str, Any]:
    # Mark workers with expired heartbeats and dead PIDs as orphaned without deleting recovery data.
    current_time = time.time() if now is None else float(now)
    orphaned: list[str] = []
    live: list[str] = []
    unknown: list[str] = []
    for record in manifest.get("workers", []):
        if not isinstance(record, dict):
            continue
        worker_id = str(record.get("worker_id", ""))
        status = str(record.get("status", "unknown"))
        if status in {"complete", "error", "baseline_failed", "restore_error", "cancelled", "orphaned"}:
            continue
        heartbeat = record.get("heartbeat_at")
        pid = record.get("pid")
        if pid is None and isinstance(record.get("workspace"), dict):
            pid = record["workspace"].get("pid")
        try:
            heartbeat_time = datetime.fromisoformat(str(heartbeat)).timestamp()
            lease_seconds = max(1.0, float(record.get("lease_seconds", 10)))
        except (TypeError, ValueError, OverflowError):
            unknown.append(worker_id)
            continue
        if _pid_alive(pid):
            live.append(worker_id)
            continue
        if current_time - heartbeat_time > lease_seconds:
            record["status"] = "orphaned"
            record["reconciled_at"] = utc_now_iso()
            orphaned.append(worker_id)
        else:
            unknown.append(worker_id)
    return {"orphaned_workers": orphaned, "live_workers": live, "unknown_workers": unknown}


def cancel_parallel_campaign(
    manifest_path: Path,
    *,
    reason: str = "cancelled by operator",
) -> dict[str, Any]:
    # Kill leased worker trees, retain recovery artifacts, and publish cancellation intent atomically.
    value = read_json(manifest_path)
    if not isinstance(value, dict):
        raise ResumeError(f"worker manifest is not an object: {manifest_path}")
    if str(value.get("status", "")) in {"complete", "resumed", "baseline_failed", "no_l1_selection", "cancelled"}:
        return {
            "manifest": str(manifest_path),
            "status": str(value.get("status")),
            "cancelled_workers": [],
            "unresolved_workers": [],
        }
    cancelled: list[str] = []
    unresolved: list[str] = []
    for record in value.get("workers", []):
        if not isinstance(record, dict):
            continue
        status = str(record.get("status", "unknown"))
        if status in {"complete", "error", "baseline_failed", "restore_error", "cancelled"}:
            continue
        worker_id = str(record.get("worker_id", ""))
        pid = record.get("pid")
        if pid is None and isinstance(record.get("workspace"), dict):
            pid = record["workspace"].get("pid")
        if _terminate_worker_pid(pid):
            record["status"] = "cancelled"
            record["cancel_reason"] = reason
            record["cancelled_at"] = utc_now_iso()
            cancelled.append(worker_id)
        else:
            record["status"] = "cancel_requested"
            record["cancel_reason"] = reason
            unresolved.append(worker_id)
    value["status"] = "cancelled"
    value["cancel_reason"] = reason
    value["cancelled_at"] = utc_now_iso()
    atomic_write_json(manifest_path, value, durability="critical", category="manifest")
    return {
        "manifest": str(manifest_path),
        "status": value["status"],
        "cancelled_workers": cancelled,
        "unresolved_workers": unresolved,
    }


def _refresh_worker_leases(
    records: dict[str, dict[str, Any]],
    reports_dir: Path,
    run_id: str,
    shards: Sequence[WorkerShard],
) -> None:
    # Merge child PID and heartbeat metadata into the coordinator lease view.
    for shard in shards:
        current = records.get(shard.worker_id, {})
        worker_root = reports_dir / "workers" / run_id / shard.worker_id
        worker_meta_path = worker_root / "worker.json"
        try:
            value = read_json(worker_meta_path)
        except (OSError, ValueError):
            continue
        if not isinstance(value, dict):
            continue
        current_workspace = current.get("workspace", {})
        if not isinstance(current_workspace, dict):
            current_workspace = {}
        workspace = {
            "worker_root": str(worker_root),
            "pid": value.get("pid"),
            "project_root": str(value.get("project_root", worker_root / "project")),
            "reports_dir": str(value.get("reports_dir", worker_root / "reports")),
            "report_path": value.get("report_path"),
            "isolation_mode": value.get("isolation_mode"),
            "project_cleaned": bool(value.get("project_cleaned", False)),
            "shared_baseline": bool(value.get("shared_baseline", current_workspace.get("shared_baseline", False))),
            "recovery_manifests": value.get("recovery_manifests", []),
            "test_stats_db": value.get("test_stats_db"),
        }
        records[shard.worker_id] = {
            **current,
            "worker_id": shard.worker_id,
            "status": value.get("status", current.get("status", "leased")),
            "pid": value.get("pid", current.get("pid")),
            "lease_id": value.get("lease_id", current.get("lease_id")),
            "lease_seconds": value.get("lease_seconds", 10),
            "heartbeat_at": value.get("heartbeat_at", current.get("heartbeat_at")),
            "heartbeat_seq": value.get("heartbeat_seq", current.get("heartbeat_seq", 0)),
            "started_at": value.get("started_at", current.get("started_at")),
            "workspace": workspace,
        }


def _load_worker_record(record: dict[str, Any], reports_dir: Path) -> dict[str, Any]:
    # Reconstruct a worker report from its JSON or durable results journal.
    workspace = record.get("workspace", {})
    workspace_reports = None
    if isinstance(workspace, dict) and workspace.get("reports_dir"):
        workspace_reports = _resolve_report_path(str(workspace["reports_dir"]), reports_dir)
    report_path_value = record.get("report_path")
    if not report_path_value and isinstance(workspace, dict):
        report_path_value = workspace.get("report_path")
    report_path = _resolve_report_path(str(report_path_value), reports_dir) if report_path_value else None
    report: dict[str, Any] | None = None
    if report_path and report_path.exists():
        try:
            value = read_json(report_path)
            report = value if isinstance(value, dict) else None
        except (OSError, ValueError):
            report = None
    if report is None and workspace_reports and workspace_reports.exists():
        for candidate in sorted(workspace_reports.glob("*.json")):
            if candidate.name == "worker.json":
                continue
            try:
                value = read_json(candidate)
            except (OSError, ValueError):
                continue
            if isinstance(value, dict) and ("results" in value or "status" in value):
                report = value
                report_path = candidate
                break
    if report is None:
        journal = None
        if workspace_reports and workspace_reports.exists():
            journals = sorted(workspace_reports.glob("*.results.jsonl"))
            journal = journals[-1] if journals else None
        report = {
            "schema_version": 3,
            "status": record.get("status", "active"),
            "results": [],
            "results_journal": str(journal) if journal else None,
            "report_path": str(report_path or (workspace_reports / "recovered.json" if workspace_reports else reports_dir / "recovered.json")),
        }
        report_path = Path(str(report["report_path"]))
    return {
        "worker_id": record.get("worker_id"),
        "status": record.get("status", report.get("status", "unknown")),
        "report": report,
        "report_path": str(report_path),
        "workspace": workspace if isinstance(workspace, dict) else {},
    }


def _recover_worker_manifests(manifest: dict[str, Any], reports_dir: Path) -> list[dict[str, str]]:
    # Restore active worker targets only when their current hash is manifest-approved.
    unresolved: list[dict[str, str]] = []
    for record in manifest.get("workers", []):
        if not isinstance(record, dict):
            continue
        workspace = record.get("workspace", {})
        if not isinstance(workspace, dict):
            continue
        paths: list[Path] = []
        for value in workspace.get("recovery_manifests", []):
            paths.append(_resolve_report_path(str(value), reports_dir))
        if not paths and workspace.get("reports_dir"):
            worker_reports = _resolve_report_path(str(workspace["reports_dir"]), reports_dir)
            paths.extend(sorted(worker_reports.glob("*.manifest.json")))
        for path in sorted(set(paths)):
            try:
                value = read_json(path)
            except (OSError, ValueError) as exc:
                unresolved.append({"manifest": str(path), "error": str(exc)})
                continue
            if not isinstance(value, dict) or value.get("status") != "active":
                continue
            try:
                recover_manifest(path, force=False)
            except (OSError, RestoreError, ValueError) as exc:
                unresolved.append({"manifest": str(path), "error": str(exc)})
    return unresolved


def _collect_manifest_results(manifest: dict[str, Any], report: dict[str, Any], reports_dir: Path) -> list[dict[str, Any]]:
    # Combine coordinator results with rows durable in every worker journal.
    values = [item for item in report.get("results", []) if isinstance(item, dict)]
    for record in manifest.get("workers", []):
        if not isinstance(record, dict):
            continue
        worker_record = _load_worker_record(record, reports_dir)
        worker_values, _ = _worker_results(worker_record, reports_dir)
        values.extend(worker_values)
    return _merge_result_rows(values)


def _merge_result_rows(values: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    # Preserve retry executions while selecting an explicit authoritative result per mutant.
    return merge_authoritative_results(values)


def _optional_path(value: object) -> Path | None:
    # Convert serialized optional paths without rejecting missing future inputs.
    return Path(str(value)).expanduser().resolve() if value else None


def _optional_argv(value: object) -> tuple[str, ...] | None:
    # Convert serialized argv lists back to the immutable command contract.
    return tuple(str(item) for item in value) if isinstance(value, list) and value else None


def _config_from_report(report: dict[str, Any], report_path: Path, mutant_ids: set[str]) -> MutationConfig:
    # Reconstruct a safe resume config from the persisted campaign report.
    serialized = report.get("config", {}) if isinstance(report.get("config", {}), dict) else {}
    target = report.get("target", {}) if isinstance(report.get("target", {}), dict) else {}
    source = str(target.get("source_path") or serialized.get("source") or "")
    project_root = Path(str(report.get("project_root", ""))).resolve()
    operators = serialized.get("operators")
    index_path = _optional_path(serialized.get("index"))
    index_snapshot = None
    frozen_index_path = index_path or (report_path.parent / "index.sqlite")
    if frozen_index_path.exists():
        try:
            candidate = load_index(frozen_index_path)
            if isinstance(candidate, dict):
                index_snapshot = candidate
        except (OSError, ValueError):
            index_snapshot = None
    return MutationConfig(
        project_root=project_root,
        source=source,
        function=target.get("function") or serialized.get("function"),
        index_path=None if index_snapshot is not None else index_path,
        context_map_path=_optional_path(serialized.get("context_map")),
        impact_db=_optional_path(serialized.get("impact_db")),
        selected_tests_file=_optional_path(serialized.get("selected_tests_file")),
        test_command_argv=_optional_argv(serialized.get("test_command_argv")),
        domain=serialized.get("domain"),
        domain_command_argv=_optional_argv(serialized.get("domain_command_argv")),
        common_command_argv=_optional_argv(serialized.get("common_command_argv")),
        python_executable=serialized.get("python_executable"),
        reports_dir=report_path.parent,
        timeout_seconds=float(serialized.get("timeout_seconds", 120.0)),
        timeout_retry_factor=float(serialized.get("timeout_retry_factor", 2.0)),
        max_mutants=None,
        from_line=serialized.get("from_line"),
        to_line=serialized.get("to_line"),
        mutant_ids=frozenset(mutant_ids),
        no_escalation=bool(serialized.get("no_escalation", False)),
        use_baseline_cache=bool(serialized.get("use_baseline_cache", True)),
        audit_percent=float(serialized.get("audit_percent", 0.0)),
        selection_snapshot=report.get("selection_snapshot"),
        operators=tuple(str(item) for item in operators) if isinstance(operators, list) else None,
        workers=max(1, int(serialized.get("workers", 1))),
        index_snapshot=index_snapshot,
        compact_report=bool(serialized.get("compact_report", False)),
    )


def _validate_resume_input_fingerprint(original: dict[str, Any], manifest: dict[str, Any]) -> None:
    # Reject resume when the repository, runtime, selection or execution configuration changed.
    recorded = original.get("input_fingerprint") or manifest.get("input_fingerprint")
    if not isinstance(recorded, str) or not recorded.strip():
        raise ResumeError("campaign input fingerprint is missing; start a new campaign")
    target = original.get("target", {})
    target = target if isinstance(target, dict) else {}
    source_value = str(target.get("source_path") or "")
    if not source_value:
        raise ResumeError("campaign target source is missing; start a new campaign")
    project_root = Path(str(original.get("project_root", ""))).resolve()
    source_rel = source_value.replace("\\", "/").lstrip("./")
    source_path = (project_root / source_rel).resolve()
    try:
        source_path.relative_to(project_root)
    except ValueError as exc:
        raise ResumeError("campaign target source escapes project root") from exc
    if not source_path.is_file():
        raise ResumeError(f"campaign target source is missing: {source_path}")
    current_sha256 = sha256_file(source_path)
    expected = campaign_input_fingerprint(
        project_root,
        source_rel,
        current_sha256,
        original.get("selection_snapshot") if isinstance(original.get("selection_snapshot"), dict) else {},
        original.get("config") if isinstance(original.get("config"), dict) else {},
    )
    if str(recorded) != expected:
        raise ResumeError("campaign input fingerprint mismatch; repository or test environment changed")


def _verify_worker_checkpoint(record: dict[str, Any], reports_dir: Path) -> None:
    # Verify each worker journal boundary before its rows participate in parallel recovery.
    worker_record = _load_worker_record(record, reports_dir)
    report = worker_record.get("report")
    if not isinstance(report, dict):
        raise ResumeError(f"worker report is unavailable: {record.get('worker_id')}")
    report_path = Path(str(worker_record.get("report_path", reports_dir)))
    state_value = report.get("state_path")
    if not state_value:
        workspace = worker_record.get("workspace", {})
        if isinstance(workspace, dict):
            state_value = workspace.get("state_path")
    status = str(report.get("status", record.get("status", "unknown")))
    if not state_value:
        if status in {"active", "starting", "running", "leased", "retrying"}:
            raise ResumeError(f"worker checkpoint is missing: {record.get('worker_id')}")
        return
    state_path = Path(str(state_value))
    if not state_path.is_absolute():
        state_path = report_path.parent / state_path
    if not state_path.exists():
        if status in {"active", "starting", "running", "leased", "retrying"}:
            raise ResumeError(f"worker checkpoint is missing: {state_path}")
        return
    try:
        verify_checkpoint(state_path)
    except JournalReplayError as exc:
        raise ResumeError(f"worker checkpoint verification failed for {record.get('worker_id')}: {exc}") from exc


def _write_resumed_report(
    original: dict[str, Any],
    original_report_path: Path,
    manifest_path: Path,
    results: list[dict[str, Any]],
    new_report: dict[str, Any] | None,
    config: MutationConfig,
    status: str,
) -> dict[str, Any]:
    # Publish a new deterministic report while leaving the interrupted report intact.
    reports_dir = original_report_path.parent
    run_id = f"{original_report_path.stem}.resume.{time.time_ns() % 1_000_000:06d}"
    report_path = reports_dir / f"{run_id}.json"
    results_path = report_path.with_suffix(".results.jsonl")
    state_path = report_path.with_suffix(".state.json")
    coordinator = MutationRunner(replace(config, workers=1, reports_dir=reports_dir))
    performance_records = [{"report": original}]
    if new_report is not None:
        performance_records.append({"report": new_report})
    coordinator.performance = _aggregate_performance(performance_records)
    report = dict(original)
    original_workers = original.get("workers", []) if isinstance(original.get("workers"), list) else []
    new_workers = new_report.get("workers", []) if isinstance(new_report, dict) and isinstance(new_report.get("workers"), list) else []
    original_worker_reports = original.get("worker_reports", []) if isinstance(original.get("worker_reports"), list) else []
    new_worker_reports = new_report.get("worker_reports", []) if isinstance(new_report, dict) and isinstance(new_report.get("worker_reports"), list) else []
    report.update(
        {
            "schema_version": 4,
            "runner_version": RUNNER_VERSION,
            "run_id": run_id,
            "status": status,
            "resumed_from": str(manifest_path),
            "resumed_at": utc_now_iso(),
            "results": [] if config.compact_report else results,
            "baseline": original.get("baseline") or (new_report.get("baseline", []) if new_report else []),
            "selection_audit": (original.get("selection_audit", []) if isinstance(original.get("selection_audit", []), list) else [])
            + (new_report.get("selection_audit", []) if new_report and isinstance(new_report.get("selection_audit", []), list) else []),
            "workers": original_workers + new_workers,
            "worker_reports": sorted(set(str(item) for item in [*original_worker_reports, *new_worker_reports])),
            "results_journal": str(results_path),
            "state_path": str(state_path),
            "recovery_manifest": str(manifest_path),
            "workers_manifest": str(manifest_path),
            "report_path": str(report_path),
            "markdown_path": str(report_path.with_suffix(".md")),
            "metrics": coordinator._metrics(results),
        }
    )
    for item in results:
        append_json_line(
            results_path,
            item,
            durability="normal",
            category="result_journal",
            metrics=coordinator.performance,
        )
    counts = Counter(str(item.get("status", "error")) for item in results)
    journal_info = journal_metadata(results_path)
    accumulator = CampaignAccumulator()
    for item in results:
        accumulator.add_result(item)
    identity_digests = result_identity_digests(tuple(results))
    atomic_write_json(
        state_path,
        {
            "checkpoint_schema_version": 2,
            "status": status,
            "completed_mutants": len(results),
            "total_mutants": len(report.get("mutants", [])),
            "counts": dict(counts),
            "resumed_from": str(manifest_path),
            "results_journal": str(results_path),
            "journal_offset": journal_info["journal_size"],
            "last_sequence": len(results),
            "completed_execution_ids": [
                str(item.get("execution_id")) for item in results if item.get("execution_id")
            ],
            "completed_mutant_ids": sorted(
                {
                    str(item.get("mutant", {}).get("mutant_id"))
                    for item in results
                    if isinstance(item.get("mutant"), dict) and item.get("mutant", {}).get("mutant_id")
                }
            ),
            "completed_identity_digests": identity_digests,
            "accumulator": accumulator.checkpoint_dict(),
            **journal_info,
            "performance": coordinator.performance.to_dict(),
        },
        durability="normal",
        category="state_checkpoint",
        metrics=coordinator.performance,
    )
    atomic_write_json(
        report_path,
        report,
        durability="normal",
        category="report",
        metrics=coordinator.performance,
    )
    atomic_write_text(
        report_path.with_suffix(".md"),
        coordinator._markdown(report),
        durability="normal",
        category="report",
        metrics=coordinator.performance,
    )
    return report


def resume_parallel_campaign(
    path: Path,
    *,
    force: bool = False,
    takeover: bool = False,
) -> dict[str, Any]:
    # Recover approved worker targets and rerun only mutants missing from durable journals.
    manifest_path = path.resolve()
    try:
        manifest = read_json(manifest_path)
    except (OSError, ValueError) as exc:
        raise ResumeError(f"cannot read worker manifest: {exc}") from exc
    if not isinstance(manifest, dict) or not manifest.get("report_path"):
        raise ResumeError("worker manifest has no coordinator report_path")
    if manifest.get("status") in {"complete", "resumed", "no_mutants", "baseline_failed", "no_l1_selection"}:
        report_ref = manifest.get("resume_report_path") if manifest.get("status") == "resumed" else manifest.get("report_path")
        report_path = _resolve_report_path(str(report_ref), manifest_path.parent)
        if report_path.exists():
            return read_json(report_path)
    if owner_is_alive(manifest):
        raise ResumeError("campaign owner is still alive; resume cannot run concurrently; use takeover only after termination")
    if not force and not takeover:
        raise ResumeError("resume of an active or partial campaign requires --force or --takeover after owner termination")
    report_path = _resolve_report_path(str(manifest["report_path"]), manifest_path.parent)
    try:
        original = read_json(report_path)
    except (OSError, ValueError) as exc:
        raise ResumeError(f"cannot read coordinator report: {exc}") from exc
    if not isinstance(original, dict):
        raise ResumeError("coordinator report is not an object")
    _validate_resume_input_fingerprint(original, manifest)
    state_value = manifest.get("state_path") or original.get("state_path")
    if state_value:
        state_path = _resolve_report_path(str(state_value), manifest_path.parent)
        if state_path.exists():
            try:
                verify_checkpoint(state_path)
            except JournalReplayError as exc:
                raise ResumeError(f"parallel checkpoint verification failed; resume stopped safely: {exc}") from exc
    unresolved = _recover_worker_manifests(manifest, manifest_path.parent)
    if unresolved:
        details = "; ".join(f"{item['manifest']}: {item['error']}" for item in unresolved[:3])
        raise ResumeError(f"worker recovery is unresolved; inspect or recover explicitly: {details}")
    for worker in manifest.get("workers", []):
        if isinstance(worker, dict):
            _verify_worker_checkpoint(worker, manifest_path.parent)
    results = _collect_manifest_results(manifest, original, manifest_path.parent)
    all_mutant_ids = {
        str(item.get("mutant_id"))
        for item in original.get("mutants", [])
        if isinstance(item, dict) and item.get("mutant_id")
    }
    completed_ids = {str(item.get("mutant", {}).get("mutant_id")) for item in results if item.get("mutant", {}).get("mutant_id")}
    remaining = all_mutant_ids - completed_ids
    config = _config_from_report(original, report_path, remaining)
    new_report = run_parallel_campaign(config) if remaining else None
    if new_report is not None:
        new_results = new_report.get("results", [])
        if (not isinstance(new_results, list) or not new_results) and new_report.get("results_journal"):
            journal_path = _resolve_report_path(str(new_report["results_journal"]), report_path.parent)
            try:
                new_results = list(replay_result_journal(journal_path).rows) if journal_path.exists() else []
            except JournalReplayError as exc:
                raise ResumeError(f"cannot replay resumed worker journal {journal_path}: {exc}") from exc
        results = _merge_result_rows([*results, *new_results])
    result_ids = {str(item.get("mutant", {}).get("mutant_id")) for item in results if item.get("mutant", {}).get("mutant_id")}
    new_status = str(new_report.get("status")) if new_report else "complete"
    if new_status in {"error", "restore_error", "baseline_failed", "no_l1_selection"}:
        status = new_status
    elif result_ids >= all_mutant_ids:
        status = "complete"
    else:
        status = "partial"
    resumed = _write_resumed_report(original, report_path, manifest_path, results, new_report, config, status)
    manifest.update(
        {
            "status": "resumed" if status == "complete" else "partial",
            "resume_report_path": resumed["report_path"],
            "completed_mutants": len(result_ids),
            "remaining_mutants": sorted(all_mutant_ids - result_ids),
            "resumed_at": utc_now_iso(),
        }
    )
    atomic_write_json(manifest_path, manifest, durability="normal", category="manifest")
    return resumed


def resume_serial_campaign(
    path: Path,
    *,
    force: bool = False,
    takeover: bool = False,
) -> dict[str, Any]:
    # Recover a serialized campaign from its checkpoint and rerun only unfinished mutants.
    manifest_path = path.resolve()
    try:
        manifest = read_json(manifest_path)
    except (OSError, ValueError) as exc:
        raise ResumeError(f"cannot read campaign manifest: {exc}") from exc
    if not isinstance(manifest, dict) or not manifest.get("report_path"):
        raise ResumeError("campaign manifest has no report_path; legacy run cannot be resumed safely")
    terminal = {"complete", "resumed", "no_mutants", "baseline_failed", "no_l1_selection"}
    if manifest.get("status") in terminal:
        report_ref = manifest.get("resume_report_path") or manifest.get("report_path")
        report_path = _resolve_report_path(str(report_ref), manifest_path.parent)
        if report_path.exists():
            return read_json(report_path)
    if owner_is_alive(manifest):
        raise ResumeError("campaign owner is still alive; resume cannot run concurrently; use takeover only after termination")
    if not force and not takeover:
        raise ResumeError("resume of an active campaign requires --force or --takeover after owner termination")
    report_path = _resolve_report_path(str(manifest["report_path"]), manifest_path.parent)
    try:
        original = read_json(report_path)
    except (OSError, ValueError) as exc:
        raise ResumeError(f"cannot read campaign report: {exc}") from exc
    if not isinstance(original, dict):
        raise ResumeError("campaign report is not an object")
    _validate_resume_input_fingerprint(original, manifest)

    state_value = manifest.get("state_path") or original.get("state_path")
    state_path = _resolve_report_path(str(state_value), manifest_path.parent) if state_value else None
    checkpoint: dict[str, Any] | None = None
    if state_path and state_path.exists():
        try:
            checkpoint = verify_checkpoint(state_path)
        except JournalReplayError as exc:
            raise ResumeError(f"checkpoint verification failed; resume stopped safely: {exc}") from exc
    journal_value = manifest.get("results_journal") or original.get("results_journal")
    if checkpoint and checkpoint.get("journal_path"):
        journal_path = Path(str(checkpoint["journal_path"]))
    elif journal_value:
        journal_path = _resolve_report_path(str(journal_value), manifest_path.parent)
    else:
        journal_path = None
    try:
        if checkpoint and checkpoint.get("verified") and journal_path:
            checkpoint_state = checkpoint.get("state", {})
            raw_identities = checkpoint_state.get("completed_identity_digests", {}) if isinstance(checkpoint_state, dict) else {}
            replay = replay_result_journal(
                journal_path,
                start_offset=int(checkpoint_state.get("journal_offset", 0) or 0) if isinstance(checkpoint_state, dict) else 0,
                initial_identities=raw_identities if isinstance(raw_identities, dict) else None,
            )
        else:
            replay = replay_result_journal(journal_path) if journal_path else None
    except JournalReplayError as exc:
        raise ResumeError(f"result journal replay failed; resume stopped safely: {exc}") from exc
    suffix_results = list(replay.rows) if replay else []
    fallback = original.get("results", [])
    results = [item for item in fallback if isinstance(item, dict)] if isinstance(fallback, list) else []
    if suffix_results:
        results = _merge_result_rows([*results, *suffix_results])
    try:
        recover_manifest(manifest_path, force=False)
    except (OSError, RestoreError, ValueError) as exc:
        raise ResumeError(f"serial target recovery is unresolved: {exc}") from exc

    all_mutant_ids = {
        str(item.get("mutant_id"))
        for item in original.get("mutants", [])
        if isinstance(item, dict) and item.get("mutant_id")
    }
    results = _merge_result_rows(results)
    completed_ids = {
        str(item.get("mutant", {}).get("mutant_id"))
        for item in results
        if item.get("mutant", {}).get("mutant_id")
    }
    if checkpoint and isinstance(checkpoint.get("state"), dict):
        completed_ids.update(
            str(item)
            for item in checkpoint["state"].get("completed_mutant_ids", [])
            if str(item).strip()
        )
    remaining = all_mutant_ids - completed_ids
    config = replace(_config_from_report(original, report_path, remaining), workers=1)
    new_report = MutationRunner(config).run() if remaining else None
    if new_report is not None:
        new_results = new_report.get("results", [])
        if (not isinstance(new_results, list) or not new_results) and new_report.get("results_journal"):
            new_journal_path = _resolve_report_path(str(new_report["results_journal"]), report_path.parent)
            try:
                new_results = list(replay_result_journal(new_journal_path).rows) if new_journal_path.exists() else []
            except JournalReplayError as exc:
                raise ResumeError(f"cannot replay resumed serial journal {new_journal_path}: {exc}") from exc
        if isinstance(new_results, list):
            results = _merge_result_rows([*results, *new_results])
    result_ids = {
        str(item.get("mutant", {}).get("mutant_id"))
        for item in results
        if item.get("mutant", {}).get("mutant_id")
    }
    new_status = str(new_report.get("status")) if new_report else "complete"
    if new_status in {"error", "restore_error", "baseline_failed", "no_l1_selection", "stale_index", "stale_snapshot"}:
        status = new_status
    elif result_ids >= all_mutant_ids:
        status = "complete"
    else:
        status = "partial"
    resumed = _write_resumed_report(original, report_path, manifest_path, results, new_report, config, status)
    manifest.update(
        {
            "status": "resumed" if status == "complete" else "partial",
            "phase": "completed" if status == "complete" else "running",
            "resume_report_path": resumed["report_path"],
            "completed_mutants": len(result_ids),
            "remaining_mutants": sorted(all_mutant_ids - result_ids),
            "resumed_at": utc_now_iso(),
        }
    )
    atomic_write_json(manifest_path, manifest, durability="critical", category="manifest")
    return resumed


def resume_campaign(
    path: Path,
    *,
    force: bool = False,
    takeover: bool = False,
) -> dict[str, Any]:
    # Route the public resume command to the matching serial or worker recovery protocol.
    try:
        manifest = read_json(path.resolve())
    except (OSError, ValueError) as exc:
        raise ResumeError(f"cannot read resume manifest: {exc}") from exc
    if isinstance(manifest, dict) and (
        isinstance(manifest.get("workers"), list)
        or isinstance(manifest.get("shards"), list)
        or str(manifest.get("recovery_manifest", "")).endswith(".workers.manifest.json")
    ):
        return resume_parallel_campaign(path, force=force, takeover=takeover)
    return resume_serial_campaign(path, force=force, takeover=takeover)


def _write_stale_report(config: MutationConfig, status: str, message: str) -> dict[str, Any]:
    # Return the same compact stale-input contract as the serialized runner.
    reports_dir = (config.reports_dir or Path(__file__).resolve().parent / "reports").resolve()
    ensure_dir(reports_dir)
    run_id = f"{utc_now_iso().replace(':', '').replace('+00:00', 'Z')}_{_safe_name(config.source)}"
    report_path = reports_dir / f"{run_id}.json"
    coordinator = MutationRunner(replace(config, workers=1, reports_dir=reports_dir))
    report = {
        "schema_version": 3,
        "runner_version": RUNNER_VERSION,
        "run_id": run_id,
        "coordinator_pid": os.getpid(),
        "process_birth_token": current_process_birth_token(),
        "status": status,
        "created_at": utc_now_iso(),
        "project_root": str(config.project_root.resolve()),
        "target": {"source_path": config.source, "function": config.function},
        "error": message,
        "results": [],
        "metrics": coordinator._metrics([]),
        "report_path": str(report_path),
        "markdown_path": str(report_path.with_suffix(".md")),
    }
    atomic_write_json(report_path, report, category="report")
    atomic_write_text(
        report_path.with_suffix(".md"),
        f"# {status}\n\n{message}\n",
        durability="normal",
        category="report",
    )
    return report


def run_parallel_campaign(config: MutationConfig) -> dict[str, Any]:
    # Coordinate isolated workers without ever mutating the original checkout.
    coordinator_started = time.perf_counter()
    reports_dir = (config.reports_dir or Path(__file__).resolve().parent / "reports").resolve()
    ensure_dir(reports_dir)
    effective_config = replace(config, reports_dir=reports_dir)
    coordinator = MutationRunner(replace(effective_config, workers=1))

    def set_coordinator_wall(report: dict[str, Any]) -> None:
        # Publish coordinator wall time before each terminal report write.
        coordinator.performance.coordinator_wall_seconds = time.perf_counter() - coordinator_started
        report.setdefault("metrics", {})["performance"] = coordinator.performance.to_dict()

    try:
        prepared = _prepare_campaign(effective_config, coordinator)
    except StaleInputError as exc:
        return _write_stale_report(effective_config, exc.status, str(exc))
    except (OSError, UnicodeError, SyntaxError, LookupError, ValueError, RuntimeError) as exc:
        return _write_stale_report(effective_config, "error", str(exc))

    run_id = f"{utc_now_iso().replace(':', '').replace('+00:00', 'Z')}_{_safe_name(prepared.source_rel)}_{time.time_ns() % 1_000_000:06d}"
    report_path = reports_dir / f"{run_id}.json"
    results_path = reports_dir / f"{run_id}.results.jsonl"
    state_path = reports_dir / f"{run_id}.state.json"
    workers_manifest = reports_dir / f"{run_id}.workers.manifest.json"
    shards = shard_mutants(prepared.mutants, effective_config.workers)
    report: dict[str, Any] = {
        "schema_version": 3,
        "runner_version": RUNNER_VERSION,
        "run_id": run_id,
        "status": "starting",
        "created_at": utc_now_iso(),
        "project_root": str(config.project_root.resolve()),
        "target": {"source_path": prepared.source_rel, "function_id": prepared.function_id, "function": config.function},
        "config": coordinator._config_dict()
        | {
            "workers": effective_config.workers,
            "index_snapshot_version": prepared.index_version,
            "shared_baseline": True,
        },
        "selection_snapshot": prepared.selection.to_dict(),
        "source_sha256": prepared.source_sha256,
        "input_fingerprint": campaign_input_fingerprint(
            config.project_root.resolve(),
            prepared.source_rel,
            prepared.source_sha256,
            prepared.selection.to_dict(),
            coordinator._config_dict()
            | {
                "workers": effective_config.workers,
                "index_snapshot_version": prepared.index_version,
                "shared_baseline": True,
            },
        ),
        "coordinator_preparation": {
            "index_snapshot": True,
            "index_snapshot_version": prepared.index_version,
            "shared_baseline": True,
        },
        "mutants": [item.to_dict() for item in prepared.mutants],
        "baseline": [],
        "results": [],
        "results_journal": str(results_path),
        "state_path": str(state_path),
        "selection_audit": [],
        "compact_report": effective_config.compact_report,
        "workers_manifest": str(workers_manifest),
        "recovery_manifest": str(workers_manifest),
        "worker_shards": [item.to_dict() for item in shards],
        "workers": [],
        "test_stats": {"db_path": str(stats_db_path(reports_dir))},
        "metrics": {},
        "report_path": str(report_path),
        "markdown_path": str(report_path.with_suffix(".md")),
    }
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "runner_version": RUNNER_VERSION,
        "run_id": run_id,
        "coordinator_pid": os.getpid(),
        "process_birth_token": current_process_birth_token(),
        "status": "active",
        "project_root": str(config.project_root.resolve()),
        "report_path": str(report_path),
        "state_path": str(state_path),
        "results_journal": str(results_path),
        "index_snapshot_version": prepared.index_version,
        "shared_baseline": True,
        "shards": [item.to_dict() for item in shards],
        "workers": [],
        "created_at": utc_now_iso(),
        "input_fingerprint": report["input_fingerprint"],
    }
    atomic_write_json(workers_manifest, manifest, durability="critical", category="manifest")
    atomic_write_json(report_path, report, durability="normal", category="report")

    if not prepared.mutants:
        report["status"] = "complete"
        report["metrics"] = coordinator._metrics([])
        set_coordinator_wall(report)
        manifest.update({"status": "complete", "finished_at": utc_now_iso()})
        atomic_write_json(workers_manifest, manifest, durability="normal", category="manifest")
        atomic_write_json(report_path, report, durability="normal", category="report")
        atomic_write_text(
            report_path.with_suffix(".md"),
            coordinator._markdown(report),
            durability="normal",
            category="report",
        )
        return report

    first_level = prepared.selection.levels[0] if prepared.selection.levels else None
    if not effective_config.test_command_argv and (first_level is None or not first_level.nodeids and not first_level.files):
        report["status"] = "no_l1_selection"
        report["selection_error"] = "no valid L1 test nodeids remain after validation"
        report["metrics"] = coordinator._metrics([])
        set_coordinator_wall(report)
        manifest.update({"status": "no_l1_selection", "finished_at": utc_now_iso()})
        atomic_write_json(workers_manifest, manifest, durability="normal", category="manifest")
        atomic_write_json(report_path, report, durability="normal", category="report")
        atomic_write_text(
            report_path.with_suffix(".md"),
            coordinator._markdown(report),
            durability="normal",
            category="report",
        )
        return report

    # Initialize coordinator-only campaign state before workers consume the immutable inputs.
    coordinator._campaign_index = prepared.index
    coordinator._campaign_selection = prepared.selection
    coordinator._campaign_function_info = prepared.function_info
    coordinator._campaign_source_rel = prepared.source_rel
    coordinator._campaign_function_id = prepared.function_id
    coordinator._mutant_by_id = {mutant.mutant_id: mutant for mutant in prepared.mutants}
    coordinator._context_map = load_context_map(effective_config.context_map_path)
    coordinator._line_selection_cache = {}
    coordinator._line_impact_cache = {}
    coordinator._function_impact_cache = None
    coordinator._context_selection_cache = None
    coordinator._static_selection_cache = None
    coordinator._baseline_results = {}
    coordinator._load_cache()
    coordinator._test_stats_run_id = run_id
    coordinator._test_stats_event_dir = reports_dir / "test_stats_events" / run_id
    ensure_dir(coordinator._test_stats_event_dir)
    levels = coordinator._level_specs(prepared.selection)
    report["levels"] = [coordinator._level_dict(level) for level in levels]
    report["test_stats"]["event_dir"] = str(coordinator._test_stats_event_dir)
    baseline_provider: SharedBaselineProvider
    try:
        snapshot = create_snapshot(
            (coordinator.root / prepared.source_rel).resolve(),
            reports_dir / "recovery" / run_id,
            metrics=coordinator.performance,
        )
        if snapshot.original_sha256 != prepared.source_sha256:
            raise StaleInputError(
                "stale_snapshot",
                f"source changed during coordinator preparation: expected {prepared.source_sha256}, got {snapshot.original_sha256}",
            )
        report["source_snapshot"] = snapshot.to_dict()
        manifest.update({"source_snapshot": snapshot.to_dict(), "levels": report["levels"]})
        atomic_write_json(
            workers_manifest,
            manifest,
            durability="critical",
            category="manifest",
            metrics=coordinator.performance,
        )
        atomic_write_json(
            report_path,
            report,
            durability="normal",
            category="report",
            metrics=coordinator.performance,
        )
        baseline_provider = SharedBaselineProvider(
            coordinator,
            snapshot,
            prepared.selection,
            prepared.function_info,
            run_id,
            levels,
        )
        shared_baseline = baseline_provider.get(levels)
        coordinator.performance.shared_baseline_count = len(shared_baseline)
        coordinator.performance.reused_baseline_count = sum(
            int(bool(item.get("baseline_reused"))) for item in shared_baseline
        )
        report["baseline"] = _translate_paths(shared_baseline, reports_dir, reports_dir)
    except StaleInputError as exc:
        coordinator._close_test_stats_connection()
        report["status"] = exc.status
        report["error"] = str(exc)
        report["metrics"] = coordinator._metrics([], refresh_health=True)
        set_coordinator_wall(report)
        manifest.update({"status": exc.status, "error": str(exc), "finished_at": utc_now_iso()})
        atomic_write_json(
            workers_manifest,
            manifest,
            durability="normal",
            category="manifest",
            metrics=coordinator.performance,
        )
        atomic_write_json(
            report_path,
            report,
            durability="normal",
            category="report",
            metrics=coordinator.performance,
        )
        atomic_write_text(
            report_path.with_suffix(".md"),
            coordinator._markdown(report),
            durability="normal",
            category="report",
            metrics=coordinator.performance,
        )
        return report
    except (OSError, RuntimeError, ValueError, UnicodeError) as exc:
        coordinator._close_test_stats_connection()
        report["status"] = "error"
        report["error"] = str(exc)
        report["metrics"] = coordinator._metrics([], refresh_health=True)
        set_coordinator_wall(report)
        manifest.update({"status": "error", "error": str(exc), "finished_at": utc_now_iso()})
        atomic_write_json(
            workers_manifest,
            manifest,
            durability="normal",
            category="manifest",
            metrics=coordinator.performance,
        )
        atomic_write_json(
            report_path,
            report,
            durability="normal",
            category="report",
            metrics=coordinator.performance,
        )
        atomic_write_text(
            report_path.with_suffix(".md"),
            coordinator._markdown(report),
            durability="normal",
            category="report",
            metrics=coordinator.performance,
        )
        return report

    failed_baselines = [item for item in report["baseline"] if not item.get("passed", False)]
    if failed_baselines:
        coordinator._close_test_stats_connection()
        report["status"] = "baseline_failed"
        report["metrics"] = coordinator._metrics([], refresh_health=True)
        set_coordinator_wall(report)
        manifest.update({"status": "baseline_failed", "finished_at": utc_now_iso()})
        atomic_write_json(
            workers_manifest,
            manifest,
            durability="normal",
            category="manifest",
            metrics=coordinator.performance,
        )
        atomic_write_json(
            report_path,
            report,
            durability="normal",
            category="report",
            metrics=coordinator.performance,
        )
        atomic_write_text(
            report_path.with_suffix(".md"),
            coordinator._markdown(report),
            durability="normal",
            category="report",
            metrics=coordinator.performance,
        )
        return report

    selection_dict = prepared.selection.to_dict()
    records: dict[str, dict[str, Any]] = {}
    lease_started = utc_now_iso()
    for shard in shards:
        lease_id = f"{run_id}:{shard.worker_id}:leased"
        worker_root = reports_dir / "workers" / run_id / shard.worker_id
        records[shard.worker_id] = {
            "worker_id": shard.worker_id,
            "status": "leased",
            "pid": None,
            "lease_id": lease_id,
            "lease_seconds": 10,
            "heartbeat_at": lease_started,
            "heartbeat_seq": 0,
            "started_at": lease_started,
            "retry_count": 0,
            "workspace": {
                "worker_root": str(worker_root),
                "project_root": str(worker_root / "project"),
                "reports_dir": str(worker_root / "reports"),
                "isolation_mode": "pending",
                "project_cleaned": False,
            },
        }
    manifest["workers"] = [_public_record(records[key], reports_dir) for key in sorted(records)]
    atomic_write_json(workers_manifest, manifest, durability="normal", category="manifest")
    process_baseline_provider = StaticBaselineProvider(shared_baseline)
    executor_type = (
        ProcessPoolExecutor
        if getattr(_run_worker, "__module__", "") == __name__
        and getattr(_run_worker, "__name__", "") == "_run_worker"
        else ThreadPoolExecutor
    )
    cancellation_requested = False
    cancellation_reason = ""
    pool_kwargs = (
        {"mp_context": multiprocessing.get_context("spawn")}
        if executor_type is ProcessPoolExecutor
        else {}
    )
    with executor_type(max_workers=len(shards), **pool_kwargs) as pool:
        futures: dict[Future[dict[str, Any]], WorkerShard] = {
            pool.submit(
                _run_worker,
                effective_config,
                run_id,
                shard,
                selection_dict,
                prepared.index,
                process_baseline_provider,
            ): shard
            for shard in shards
        }
        pending = set(futures)
        while pending:
            try:
                requested_manifest = read_json(workers_manifest)
            except (OSError, ValueError):
                requested_manifest = {}
            if isinstance(requested_manifest, dict) and requested_manifest.get("status") == "cancelled":
                cancellation_requested = True
                cancellation_reason = str(requested_manifest.get("cancel_reason", "cancelled by operator"))
            _refresh_worker_leases(records, reports_dir, run_id, shards)
            if cancellation_requested:
                for shard in shards:
                    current = records.get(shard.worker_id, {})
                    if current.get("status") in {"complete", "error", "cancelled", "restore_error"}:
                        continue
                    workspace = current.get("workspace", {})
                    pid = current.get("pid")
                    if pid is None and isinstance(workspace, dict):
                        pid = workspace.get("pid")
                    if pid is not None:
                        _terminate_worker_pid(pid)
                    current = dict(current)
                    current.update({"status": "cancelled", "cancel_reason": cancellation_reason})
                    records[shard.worker_id] = current
                for future in pending:
                    future.cancel()
            manifest["status"] = "cancelled" if cancellation_requested else "active"
            if cancellation_requested:
                manifest["cancel_reason"] = cancellation_reason
            manifest["workers"] = [_public_record(records[key], reports_dir) for key in sorted(records)]
            atomic_write_json(workers_manifest, manifest, durability="normal", category="manifest")
            done, pending = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
            for future in done:
                shard = futures[future]
                try:
                    record = future.result()
                except BaseException as exc:
                    record = {
                        "worker_id": shard.worker_id,
                        "status": "error",
                        "error": str(exc),
                        "report": None,
                        "workspace": records.get(shard.worker_id, {}).get("workspace", {}),
                    }
                if cancellation_requested and record.get("status") in {"error", "starting", "running", "leased"}:
                    record["status"] = "cancelled"
                    record["cancel_reason"] = cancellation_reason
                records[shard.worker_id] = record
        _refresh_worker_leases(records, reports_dir, run_id, shards)
        if cancellation_requested:
            for shard in shards:
                current = records.get(shard.worker_id, {})
                if current.get("status") not in {"complete", "baseline_failed", "restore_error"}:
                    current = dict(current)
                    current.update({"status": "cancelled", "cancel_reason": cancellation_reason})
                    records[shard.worker_id] = current
        manifest["status"] = "cancelled" if cancellation_requested else "active"
        if cancellation_requested:
            manifest["cancel_reason"] = cancellation_reason
        manifest["workers"] = [_public_record(records[key], reports_dir) for key in sorted(records)]
        atomic_write_json(workers_manifest, manifest, durability="normal", category="manifest")

    if not cancellation_requested and executor_type is ProcessPoolExecutor:
        _retry_failed_workers(
            effective_config,
            run_id,
            shards,
            selection_dict,
            prepared.index,
            process_baseline_provider,
            records,
            manifest,
            workers_manifest,
            reports_dir,
        )

    coordinator._close_test_stats_connection()
    ordered_records = [records[key] for key in sorted(records)]
    merged_results: list[dict[str, Any]] = []
    baseline: list[dict[str, Any]] = list(report.get("baseline", []))
    selection_audit: list[dict[str, Any]] = []
    worker_reports: list[str] = []
    worker_statuses: list[str] = []
    worker_stats_databases: list[Path] = []
    for record in ordered_records:
        worker_statuses.append(str(record.get("status", "error")))
        workspace = record.get("workspace")
        if isinstance(workspace, dict) and workspace.get("test_stats_db"):
            worker_stats_databases.append(Path(str(workspace["test_stats_db"])))
        report_data = record.get("report")
        if isinstance(report_data, dict) and report_data.get("report_path"):
            worker_reports.append(_report_relative(Path(str(report_data["report_path"])), reports_dir))
            translated_report = _translate_paths(report_data, Path(str(record.get("report_path", ""))).parent, reports_dir)
            if not baseline and isinstance(translated_report.get("baseline"), list):
                baseline = translated_report["baseline"]
            if isinstance(translated_report.get("selection_audit"), list):
                selection_audit.extend(translated_report["selection_audit"])
        values, _ = _worker_results(record, reports_dir)
        merged_results.extend(values)
    merged_results = _merge_result_rows(merged_results)
    if cancellation_requested:
        status = "cancelled"
    elif any(status == "restore_error" for status in worker_statuses):
        status = "restore_error"
    elif any(status == "baseline_failed" for status in worker_statuses):
        status = "baseline_failed"
    elif any(status in {"error", "process_tree_leak"} for status in worker_statuses):
        status = "error"
    elif any(status == "no_l1_selection" for status in worker_statuses):
        status = "no_l1_selection"
    else:
        status = "complete"
    worker_performance = _aggregate_performance(ordered_records)
    for metric in fields(PerformanceMetrics):
        setattr(
            worker_performance,
            metric.name,
            getattr(worker_performance, metric.name) + getattr(coordinator.performance, metric.name),
        )
    coordinator.performance = worker_performance
    coordinator_stats = dict(coordinator._test_stats_summary)
    if worker_stats_databases:
        stats_merge = merge_test_stats_databases(
            worker_stats_databases,
            stats_db_path(reports_dir),
            project_root=config.project_root,
            metrics=coordinator.performance,
        )
        coordinator._test_stats_summary = {
            "events_ingested": int(coordinator_stats.get("events_ingested", 0)) + int(stats_merge.get("events_ingested", 0)),
            "invalid_events": int(coordinator_stats.get("invalid_events", 0)),
            "event_files": int(coordinator_stats.get("event_files", 0)) + len(worker_stats_databases),
        }
    report["status"] = status
    report["baseline"] = baseline
    report["results"] = [] if effective_config.compact_report else merged_results
    report["selection_audit"] = selection_audit
    report["worker_reports"] = sorted(set(worker_reports))
    report["workers"] = [_public_record(record, reports_dir) for record in ordered_records]
    report["metrics"] = coordinator._metrics(merged_results, refresh_health=True)
    set_coordinator_wall(report)
    for item in merged_results:
        append_json_line(
            results_path,
            item,
            durability="normal",
            category="result_journal",
            metrics=coordinator.performance,
        )
    accumulator = CampaignAccumulator()
    for item in merged_results:
        accumulator.add_result(item)
    identity_digests = result_identity_digests(tuple(merged_results))
    journal_info = journal_metadata(results_path)
    atomic_write_json(
        state_path,
        {
            "checkpoint_schema_version": 2,
            "status": status,
            "completed_mutants": len(merged_results),
            "total_mutants": len(prepared.mutants),
            "workers": len(ordered_records),
            "worker_statuses": worker_statuses,
            "results_journal": str(results_path),
            "journal_offset": journal_info["journal_size"],
            "last_sequence": len(merged_results),
            "completed_execution_ids": [
                str(item.get("execution_id")) for item in merged_results if item.get("execution_id")
            ],
            "completed_mutant_ids": sorted(
                {
                    str(item.get("mutant", {}).get("mutant_id"))
                    for item in merged_results
                    if isinstance(item.get("mutant"), dict) and item.get("mutant", {}).get("mutant_id")
                }
            ),
            "completed_identity_digests": identity_digests,
            "accumulator": accumulator.checkpoint_dict(),
            **journal_info,
            "performance": coordinator.performance.to_dict(),
        },
        durability="critical",
        category="state_checkpoint",
        metrics=coordinator.performance,
    )
    atomic_write_json(
        report_path,
        report,
        durability="critical",
        category="report",
        metrics=coordinator.performance,
    )
    atomic_write_text(
        report_path.with_suffix(".md"),
        coordinator._markdown(report),
        durability="critical",
        category="report",
        metrics=coordinator.performance,
    )
    manifest.update({
        "status": status,
        "workers": [_public_record(record, reports_dir) for record in ordered_records],
        "finished_at": utc_now_iso(),
    })
    atomic_write_json(
        workers_manifest,
        manifest,
        durability="critical",
        category="manifest",
        metrics=coordinator.performance,
    )
    return report
