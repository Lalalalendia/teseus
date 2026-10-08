"""Lightweight subprocess tracing for selected test commands.

The collector is injected as ``sitecustomize.py`` and uses only stdlib
``sys.settrace``. xdist is removed for this command so workers cannot race on
one trace artifact; normal mutation runs remain free to use xdist at L3.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .commands import run_argv, without_parallelism
from .io_utils import atomic_write_json, atomic_write_text, ensure_dir, read_json, utc_now_iso


TRACE_SITE = r'''
import atexit
import json
import os
import sys
import threading

_root = os.path.normcase(os.path.abspath(os.environ["TI_TRACE_ROOT"]))
_root_prefix = _root + os.sep
_output = os.environ["TI_TRACE_OUT"]
if os.environ.get("TI_TRACE_WORKERS") == "1":
    _output_dir = os.environ.get("TI_TRACE_OUT_DIR") or os.path.dirname(_output)
    _worker = os.environ.get("PYTEST_XDIST_WORKER") or os.environ.get("TI_TRACE_WORKER") or "main"
    _output = os.path.join(_output_dir, "trace." + _worker + ".json")
    os.makedirs(_output_dir, exist_ok=True)
_flush_every = int(os.environ.get("TI_TRACE_FLUSH_EVERY", "0") or "0")
_files = {}
_functions = {}
_code_locations = {}
_events = 0
_state = threading.local()

_excluded_parts = {
    ".venv",
    "venv",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "__pycache__",
    "reports",
    "generated",
}

def _inside(filename):
    name = os.path.normcase(filename)
    if not (name == _root or name.startswith(_root_prefix)):
        return False
    relative = os.path.relpath(name, _root)
    parts = set(relative.split(os.sep))
    return not any(part in _excluded_parts or part.startswith(".trace_runtime_") for part in parts)

def _relative(filename):
    return os.path.relpath(filename, _root).replace(os.sep, "/")

def _code_location(code):
    # Resolve root membership, relative path and qualname once for each code object.
    cached = _code_locations.get(code)
    if cached is not None:
        return cached
    filename = code.co_filename
    if not _inside(filename):
        cached = (False, "", "")
    else:
        cached = (
            True,
            _relative(filename),
            str(getattr(code, "co_qualname", code.co_name)),
        )
    _code_locations[code] = cached
    return cached

def _trace(frame, event, arg):
    # Reuse the code-object path cache on every trace event.
    global _events
    if event == "call":
        if not _code_location(frame.f_code)[0]:
            return None
        return _trace
    if event != "line":
        return _trace
    if getattr(_state, "active", False):
        return _trace
    _state.active = True
    try:
        inside, rel, qualname = _code_location(frame.f_code)
        if inside:
            _events += 1
            line = int(frame.f_lineno)
            _files.setdefault(rel, set()).add(line)
            key = rel + "::" + qualname
            _functions.setdefault(key, {"rel_path": rel, "qualname": qualname, "lines": set()})["lines"].add(line)
            if _flush_every and _events % _flush_every == 0:
                _flush()
    finally:
        _state.active = False
    return _trace

def _flush():
    payload = {
        "schema_version": 1,
        "files": {key: sorted(value) for key, value in _files.items()},
        "functions": {key: {**value, "lines": sorted(value["lines"])} for key, value in _functions.items()},
    }
    temp = _output + ".tmp"
    with open(temp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
    os.replace(temp, _output)

sys.settrace(_trace)
threading.settrace(_trace)
atexit.register(_flush)
'''


@dataclass(frozen=True)
class TraceConfig:
    project_root: Path
    command_argv: tuple[str, ...]
    output: Path
    timeout_seconds: float = 120.0
    keep_runtime: bool = False
    merge_workers: bool = False


def merge_trace_payloads(inputs: list[Path], *, project_root: Path | None = None) -> dict[str, Any]:
    # Union worker line/function sets while preserving deterministic ordering and diagnostics.
    files: dict[str, set[int]] = {}
    functions: dict[str, dict[str, Any]] = {}
    invalid_inputs: list[dict[str, str]] = []
    for path in sorted(inputs, key=lambda item: str(item)):
        try:
            value = read_json(path)
        except (OSError, ValueError) as exc:
            invalid_inputs.append({"path": str(path), "error": str(exc)})
            continue
        if not isinstance(value, dict):
            invalid_inputs.append({"path": str(path), "error": "trace payload is not an object"})
            continue
        for rel_path, lines in value.get("files", {}).items():
            if not isinstance(lines, list):
                continue
            bucket = files.setdefault(str(rel_path), set())
            bucket.update(int(line) for line in lines if isinstance(line, int))
        for key, item in value.get("functions", {}).items():
            if not isinstance(item, dict):
                continue
            current = functions.setdefault(
                str(key),
                {
                    "rel_path": str(item.get("rel_path", "")),
                    "qualname": str(item.get("qualname", "")),
                    "lines": set(),
                },
            )
            lines = item.get("lines", [])
            if isinstance(lines, list):
                current["lines"].update(int(line) for line in lines if isinstance(line, int))
    normalized_functions = {
        key: {**value, "lines": sorted(value["lines"])}
        for key, value in sorted(functions.items())
    }
    return {
        "schema_version": 2,
        "merge_version": "trace-merge-v1",
        "created_at": utc_now_iso(),
        "project_root": str(project_root.resolve()) if project_root else None,
        "trace_inputs": [str(path) for path in sorted(inputs, key=lambda item: str(item))],
        "input_count": len(inputs),
        "invalid_inputs": invalid_inputs,
        "files": {key: sorted(value) for key, value in sorted(files.items())},
        "functions": normalized_functions,
    }


def merge_trace_files(inputs: list[Path], output: Path, *, project_root: Path | None = None) -> dict[str, Any]:
    # Merge trace JSON files and atomically publish one compact artifact.
    merged = merge_trace_payloads(inputs, project_root=project_root)
    atomic_write_json(output, merged)
    return merged


def run_trace(config: TraceConfig) -> dict[str, Any]:
    # Run one trace command and optionally merge xdist worker artifacts.
    root = config.project_root.resolve()
    ensure_dir(config.output.parent)
    trace_dir = Path(tempfile.mkdtemp(prefix=f".trace_runtime_{config.output.stem}_", dir=config.output.parent))
    sitecustomize = trace_dir / "sitecustomize.py"
    raw_trace_dir = ensure_dir(trace_dir / "worker_traces")
    raw_trace = raw_trace_dir / "trace.main.json"
    atomic_write_text(sitecustomize, TRACE_SITE)
    env = dict(os.environ)
    env["TI_TRACE_ROOT"] = str(root)
    env["TI_TRACE_OUT"] = str(raw_trace)
    if config.merge_workers:
        env["TI_TRACE_WORKERS"] = "1"
        env["TI_TRACE_OUT_DIR"] = str(raw_trace_dir)
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(trace_dir) + (os.pathsep + existing_pythonpath if existing_pythonpath else "")
    command = tuple(config.command_argv) if config.merge_workers else without_parallelism(config.command_argv)
    process_artifact = config.output.with_suffix(".process.txt")
    process = run_argv(
        command,
        cwd=root,
        timeout_seconds=config.timeout_seconds,
        env=env,
        output_artifact=process_artifact,
    )
    trace_inputs = sorted(raw_trace_dir.glob("trace.*.json")) if config.merge_workers else [raw_trace]
    if config.merge_workers:
        trace_data = merge_trace_payloads(trace_inputs, project_root=root)
    else:
        try:
            trace_data = read_json(raw_trace)
        except (OSError, ValueError):
            trace_data = {"schema_version": 1, "files": {}, "functions": {}}
    result = {
        "schema_version": 2 if config.merge_workers else 1,
        "created_at": utc_now_iso(),
        "project_root": str(root),
        "command_argv": list(command),
        "parallelism_disabled": tuple(command) != tuple(config.command_argv),
        "merge_workers": config.merge_workers,
        "trace_inputs": [str(path) for path in trace_inputs],
        "trace_merge": (
            {
                "merge_version": trace_data.get("merge_version"),
                "input_count": trace_data.get("input_count", 0),
                "invalid_inputs": trace_data.get("invalid_inputs", []),
            }
            if config.merge_workers
            else None
        ),
        "process": process.to_dict(),
        "files": trace_data.get("files", {}),
        "functions": trace_data.get("functions", {}),
        "trace_runtime": str(trace_dir) if config.keep_runtime else None,
        "trace_runtime_kept": config.keep_runtime,
    }
    atomic_write_json(config.output, result)
    if not config.keep_runtime and not process.timed_out:
        shutil.rmtree(trace_dir, ignore_errors=True)
    return result
