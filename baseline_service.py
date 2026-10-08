"""Baseline execution service extracted behind the E-09 facade boundary."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Sequence

from .io_utils import copy_file_atomic, ensure_dir
from .models import ProcessResult


class BaselineService:
    """Execute or reuse baseline levels through runner-owned ports and caches."""

    def __init__(self, runner: Any) -> None:
        # Keep the first extraction small by using runner ports while moving policy out of runner.py.
        self.runner = runner

    def run(
        self,
        levels: Sequence[Any],
        snapshot: Any,
        selection: Any,
        function_info: dict[str, Any] | None,
        report_id: str,
    ) -> list[dict[str, Any]]:
        # Run or reuse one baseline per escalation level and collect per-test events.
        runner = self.runner
        if runner.config.shared_baseline_provider is not None:
            return runner.config.shared_baseline_provider.get(levels)
        results: list[dict[str, Any]] = []
        for level in levels:
            if level.name in runner._baseline_results:
                results.append(runner._baseline_results[level.name])
                continue
            key = runner._baseline_key(level, snapshot, selection, function_info)
            cached = runner._baseline_cache.get(key) if runner.config.use_baseline_cache else None
            if isinstance(cached, dict) and cached.get("passed"):
                runner.performance.baseline_cache_hits += 1
                result = dict(cached)
                result["level"] = level.name
                result["baseline_reused"] = True
                cache_artifact = cached.get("cache_artifact")
                output_path = runner._artifact_path(report_id, "baseline_reused", level.name)
                if cache_artifact:
                    cache_path = Path(str(cache_artifact))
                    if not cache_path.is_absolute():
                        cache_path = runner.reports_dir / cache_path
                    try:
                        copy_file_atomic(
                            cache_path,
                            output_path,
                            category="baseline_artifact",
                            metrics=runner.performance,
                        )
                        result["output_artifact"] = runner._relative_artifact(output_path)
                    except OSError:
                        result["artifact_missing"] = True
                else:
                    result["output_artifact"] = None
                result["artifact_run_id"] = cached.get("artifact_run_id")
                runner._baseline_results[level.name] = result
                results.append(result)
                continue
            runner.performance.baseline_cache_misses += 1
            phase_started = time.perf_counter()
            output_path = runner._artifact_path(report_id, "baseline", level.name)
            try:
                process = runner._run_test_command(
                    level.command_argv,
                    phase="baseline",
                    level=level.name,
                    mutant_id=None,
                    target_sha256=snapshot.original_sha256,
                    report_id=report_id,
                    output_artifact=output_path,
                    timeout_seconds=runner.config.timeout_seconds,
                )
                error = None
            except OSError as exc:
                process = ProcessResult(level.command_argv, str(runner.test_cwd), 127, 0.0, False, str(exc))
                error = str(exc)
            runner.performance.baseline_seconds += time.perf_counter() - phase_started
            result = process.to_dict() | {
                "level": level.name,
                "passed": process.passed,
                "baseline_reused": False,
                "cache_key": key,
                "output_artifact": runner._relative_artifact(output_path) if output_path.exists() else None,
                "error": error,
            }
            if runner.config.use_baseline_cache and process.passed:
                cache_artifact_path = ensure_dir(runner.reports_dir / "baseline_cache_artifacts") / f"{key}.txt"
                copy_file_atomic(
                    output_path,
                    cache_artifact_path,
                    category="baseline_artifact",
                    metrics=runner.performance,
                )
                result["cache_artifact"] = str(cache_artifact_path.relative_to(runner.reports_dir).as_posix())
                result["artifact_run_id"] = report_id
                runner._baseline_cache[key] = result | {"output_artifact": result["cache_artifact"]}
                runner._save_cache()
            runner._baseline_results[level.name] = result
            results.append(result)
        return results
