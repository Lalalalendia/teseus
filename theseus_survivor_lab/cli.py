"""Standalone command-line interface for offline survivor analysis."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .errors import SurvivorLabError
from .serialization import (
    canonical_json,
    load_json,
    request_from_dict,
    result_from_dict,
    result_to_dict,
    result_to_markdown,
    write_json,
    write_markdown,
)
from .service import analyze_request
from .validation import validate_request


def _parser() -> argparse.ArgumentParser:
    # Build the CLI parser without coupling it to any project runtime.
    parser = argparse.ArgumentParser(prog="theseus-survivor-lab")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("validate-input", "classify", "analyze", "explain", "render-markdown"):
        command = commands.add_parser(name)
        command.add_argument("input", help="input bundle or result JSON path")
        if name in {"analyze", "explain", "render-markdown"}:
            command.add_argument("--out", help="output JSON or Markdown path")
    return parser


def _read_request(path: str):
    # Read and validate one input bundle from disk.
    payload = load_json(path)
    if isinstance(payload, dict) and "expected" in payload:
        payload = dict(payload)
        payload.pop("expected", None)
    request = request_from_dict(payload)
    validate_request(request)
    return request


def _write_or_print(path: str | None, content: str) -> None:
    # Write a requested output path as explicit UTF-8 LF bytes or print it.
    if path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content.encode("utf-8"))
    else:
        sys.stdout.write(content)


def _result_for_input(path: str):
    # Load either a request bundle or an already computed result projection.
    value = load_json(path)
    if isinstance(value, dict) and "expected" in value:
        value = dict(value)
        value.pop("expected", None)
    if isinstance(value, dict) and "classification" in value and "result_id" in value:
        return result_from_dict(value)
    return analyze_request(request_from_dict(value))


def main(argv: list[str] | None = None) -> int:
    # Execute one standalone CLI command and return a process status.
    args = _parser().parse_args(argv)
    try:
        if args.command == "validate-input":
            request = _read_request(args.input)
            sys.stdout.write(canonical_json({"valid": True, "request_id": request.request_id}) + "\n")
            return 0
        if args.command == "classify":
            result = _result_for_input(args.input)
            output = {
                "request_id": result.request_id,
                "result_id": result.result_id,
                "category": result.classification.category.value,
                "confidence": result.confidence,
                "reason": result.classification.reason,
                "evidence_ids": list(result.classification.evidence_ids),
                "blockers": list(result.blockers),
            }
            sys.stdout.write(canonical_json(output) + "\n")
            return 0
        if args.command == "analyze":
            result = analyze_request(_read_request(args.input))
            content = canonical_json(result_to_dict(result)) + "\n"
            if args.out:
                write_json(args.out, result_to_dict(result))
            else:
                sys.stdout.write(content)
            return 0
        if args.command == "explain":
            result = _result_for_input(args.input)
            _write_or_print(args.out, result_to_markdown(result))
            return 0
        if args.command == "render-markdown":
            result = _result_for_input(args.input)
            if args.out:
                write_markdown(args.out, result)
            else:
                sys.stdout.write(result_to_markdown(result))
            return 0
    except (OSError, ValueError, SurvivorLabError, json.JSONDecodeError) as exc:
        sys.stderr.write(f"theseus-survivor-lab: {exc}\n")
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
