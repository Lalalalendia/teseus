"""Staged JSONL engine worker used by the first local Theseus vertical slice."""
from __future__ import annotations
import argparse
import asyncio
import json
import sys
import traceback
from pathlib import Path
from typing import Any, Mapping
from theseus_contracts import (
    CampaignConfiguration,
    CampaignId,
    DiscoverMutantsRequest,
    ExecuteShardRequest,
    FinalizeCampaignRequest,
    JsonlEventSink,
    PrepareCampaignRequest,
    ShardDescriptor,
    WorkerId,
)
from theseus_contracts.serialization import loads_object
from test_intelligence_unified_v1.engine import RunnerMutationEngine
from test_intelligence_unified_v1.mutations import restore_snapshot
def _response(
    result: Mapping[str, Any] | None = None,
    *,
    error: str | None = None,
    request_id: str | None = None,
) -> str:
    # Encode one protocol response while keeping pytest output outside the protocol stream.
    payload: dict[str, Any] = {"request_id": request_id, "ok": error is None}
    if error is None:
        payload["result"] = dict(result or {})
    else:
        payload["error"] = error
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
async def _dispatch(
    frame: Mapping[str, Any],
    engine: RunnerMutationEngine,
    prepared: bool,
) -> tuple[dict[str, Any], bool]:
    # Dispatch one public command through the facade and retain only staged process state.
    command = frame.get("command")
    raw_request = frame.get("request", {})
    if not isinstance(command, str) or not isinstance(raw_request, Mapping):
        raise ValueError("command and request object are required")
    if command == "shutdown":
        return {"status": "stopped"}, True
    if command == "prepare":
        raw_configuration = raw_request.get("configuration")
        if not isinstance(raw_configuration, Mapping):
            raise ValueError("prepare requires configuration")
        configuration = CampaignConfiguration.from_dict(raw_configuration)
        result = await engine.prepare_campaign_async(PrepareCampaignRequest(configuration))
        return result.to_dict(), False
    if command == "collect":
        if not prepared:
            raise ValueError(f"{command} requires prepare")
        result = await engine.collect_campaign_async(CampaignId(str(raw_request["campaign_id"])))
        return dict(result), False
    if command == "index":
        if not prepared:
            raise ValueError(f"{command} requires prepare")
        result = await engine.index_campaign_async(CampaignId(str(raw_request["campaign_id"])))
        return dict(result), False
    if command == "baseline":
        if not prepared:
            raise ValueError(f"{command} requires prepare")
        result = await engine.baseline_campaign_async(CampaignId(str(raw_request["campaign_id"])))
        return dict(result), False
    if command == "discover":
        if not prepared:
            raise ValueError("discover requires prepare")
        campaign_id = CampaignId(str(raw_request["campaign_id"]))
        result = await engine.discover_mutants_async(DiscoverMutantsRequest(campaign_id))
        return result.to_dict(), False
    if command == "execute-shard":
        raw_shard = raw_request.get("shard")
        if not isinstance(raw_shard, Mapping):
            raise ValueError("execute-shard requires shard")
        request = ExecuteShardRequest(
            campaign_id=CampaignId(str(raw_request["campaign_id"])),
            shard=ShardDescriptor.from_dict(raw_shard),
            attempt=max(0, int(raw_request.get("attempt", 0))),
            worker_id=WorkerId(str(raw_request["worker_id"])) if raw_request.get("worker_id") else None,
            lease_id=str(raw_request["lease_id"]) if raw_request.get("lease_id") else None,
            test_overrides={
                str(mutant_id): tuple(str(nodeid) for nodeid in nodeids)
                for mutant_id, nodeids in (raw_request.get("test_overrides", {}) or {}).items()
                if isinstance(nodeids, (list, tuple))
            },
            worker_instance_id=(
                str(raw_request["worker_instance_id"])
                if raw_request.get("worker_instance_id")
                else None
            ),
            worker_process_id=(
                int(raw_request["worker_process_id"])
                if raw_request.get("worker_process_id") is not None
                else None
            ),
            worker_process_birth_token=(
                str(raw_request["worker_process_birth_token"])
                if raw_request.get("worker_process_birth_token")
                else None
            ),
            mutant_spool_root=(
                str(raw_request["mutant_spool_root"])
                if raw_request.get("mutant_spool_root")
                else None
            ),
            expected_source_sha256=(
                str(raw_request["expected_source_sha256"])
                if raw_request.get("expected_source_sha256")
                else None
            ),
            test_fingerprints={
                str(mutant_id): {str(nodeid): str(fingerprint) for nodeid, fingerprint in rows.items()}
                for mutant_id, rows in (raw_request.get("test_fingerprints", {}) or {}).items()
                if isinstance(rows, Mapping)
            },
            prepared_snapshot_id=(
                str(raw_request["prepared_snapshot_id"])
                if raw_request.get("prepared_snapshot_id")
                else None
            ),
        )
        result = await engine.execute_shard_async(request)
        return result.to_dict(), False
    if command == "finalize":
        campaign_id = CampaignId(str(raw_request["campaign_id"]))
        request = FinalizeCampaignRequest(
            campaign_id=campaign_id,
            status_override=(str(raw_request["status_override"]) if raw_request.get("status_override") else None),
        )
        result = await engine.finalize_campaign_async(request)
        return result.to_dict(), False
    raise ValueError(f"unknown engine command: {command}")
def _campaign_id_from_frame(frame: Mapping[str, Any]) -> str | None:
    # Extract the command campaign identity without trusting arbitrary request payload shapes.
    raw_request = frame.get("request")
    if not isinstance(raw_request, Mapping):
        return None
    raw_campaign_id = raw_request.get("campaign_id")
    return str(raw_campaign_id) if raw_campaign_id else None
def _cleanup_execution_context(engine: RunnerMutationEngine, campaign_id: str | None) -> None:
    # Restore source and close runner-owned resources before any execute-shard response is published.
    if not campaign_id:
        return
    contexts = getattr(engine, "_contexts", {})
    context = contexts.get(campaign_id) if isinstance(contexts, Mapping) else None
    if context is None:
        return
    runner = getattr(context, "runner", None)
    if runner is None:
        return
    cleanup_errors: list[str] = []
    snapshot = getattr(runner, "_campaign_snapshot", None)
    if snapshot is not None:
        try:
            restore_snapshot(snapshot, expected_sha256=None, durability="critical")
        except Exception as exc:
            cleanup_errors.append(f"source restore failed: {exc}")
    close_stats = getattr(runner, "_close_test_stats_connection", None)
    if callable(close_stats):
        try:
            close_stats()
        except Exception as exc:
            cleanup_errors.append(f"test stats close failed: {exc}")
    impact_adapter = getattr(runner, "_impact_adapter", None)
    if impact_adapter is not None:
        try:
            impact_adapter.close()
        except Exception as exc:
            cleanup_errors.append(f"impact adapter close failed: {exc}")
    if cleanup_errors:
        raise RuntimeError("; ".join(cleanup_errors))
def _combine_errors(command_error: BaseException | None, cleanup_error: BaseException | None) -> BaseException | None:
    # Preserve the command failure while making a cleanup failure visible in the same protocol response.
    if command_error is None:
        return cleanup_error
    if cleanup_error is None:
        return command_error
    return RuntimeError(f"{command_error}; cleanup failed: {cleanup_error}")
async def _run(events_path: Path) -> int:
    # Consume complete JSONL request frames until shutdown or parent pipe closure.
    sink = JsonlEventSink(events_path)
    engine = RunnerMutationEngine(sink)
    prepared = False
    for line in sys.stdin:
        if not line.strip():
            continue
        request_id: str | None = None
        command: str | None = None
        frame: Mapping[str, Any] = {}
        result: dict[str, Any] = {}
        should_stop = False
        command_error: BaseException | None = None
        cleanup_error: BaseException | None = None
        try:
            decoded_frame = loads_object(line)
            if not isinstance(decoded_frame, Mapping):
                raise ValueError("protocol frame must be an object")
            frame = decoded_frame
            if frame.get("request_id") is not None:
                request_id = str(frame["request_id"])
            if frame.get("command") is not None:
                command = str(frame["command"])
            result, should_stop = await _dispatch(frame, engine, prepared)
            if command == "prepare":
                prepared = True
        except Exception as exc:
            command_error = exc
        finally:
            if command == "execute-shard":
                try:
                    _cleanup_execution_context(engine, _campaign_id_from_frame(frame))
                except Exception as exc:
                    cleanup_error = exc
        error = _combine_errors(command_error, cleanup_error)
        if error is not None:
            if command_error is not None:
                traceback.print_exception(
                    type(command_error),
                    command_error,
                    command_error.__traceback__,
                    file=sys.stderr,
                )
            if cleanup_error is not None:
                traceback.print_exception(
                    type(cleanup_error),
                    cleanup_error,
                    cleanup_error.__traceback__,
                    file=sys.stderr,
                )
            sys.stdout.write(_response(error=str(error), request_id=request_id) + "\n")
            sys.stdout.flush()
            continue
        sys.stdout.write(_response(result, request_id=request_id) + "\n")
        sys.stdout.flush()
        if should_stop:
            return 0
    return 0
def main(argv: list[str] | None = None) -> int:
    # Parse only transport arguments so the worker remains independent from the user CLI.
    parser = argparse.ArgumentParser(prog="theseus-engine")
    parser.add_argument("--events", required=True, type=Path)
    args = parser.parse_args(argv)
    return asyncio.run(_run(args.events))
if __name__ == "__main__":
    raise SystemExit(main())
