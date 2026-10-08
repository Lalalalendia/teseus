"""Detached campaign launch worker for replay-safe local operator actions."""
from __future__ import annotations
import argparse
import json
import os
import subprocess
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from gallifrey_mutation import (
    MutationCampaignService,
    OperatorActionStatus,
    SQLiteMutationStore,
    Success,
)
from test_intelligence_unified_v1.recovery import current_process_birth_token
from .coordinator import LocalCampaignCoordinator
def _process_exists(process_id: int) -> bool:
    # Detect whether one process identifier still names a live local process.
    if process_id <= 0:
        return False
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True
def process_identity(process_id: int) -> str:
    # Resolve a PID-reuse-resistant token when the platform exposes one.
    return current_process_birth_token(process_id) or f"pid-{process_id}-unknown"
def launch_process_is_alive(metadata: Mapping[str, Any]) -> bool:
    # Verify both PID and birth token before treating a recorded launch as active.
    raw_process_id = metadata.get("process_id")
    token = str(metadata.get("process_birth_token") or "")
    if isinstance(raw_process_id, bool) or not isinstance(raw_process_id, int) or raw_process_id <= 0 or not token:
        return False
    observed = current_process_birth_token(raw_process_id)
    if observed is not None:
        return observed == token
    return token == f"pid-{raw_process_id}-unknown" and _process_exists(raw_process_id)
def spawn_campaign_action(
    database_path: Path,
    campaign_id: str,
    action_id: str,
) -> dict[str, Any]:
    # Start one detached launch worker and return only bounded process identity metadata.
    resolved_database = Path(database_path).expanduser().resolve()
    resolved_database.parent.mkdir(parents=True, exist_ok=True)
    stdout_path = resolved_database.parent / "campaign-launch.stdout.log"
    stderr_path = resolved_database.parent / "campaign-launch.stderr.log"
    command = (
        sys.executable,
        "-m",
        "theseus_local.launcher",
        "run",
        "--database",
        str(resolved_database),
        "--campaign-id",
        str(campaign_id),
        "--action-id",
        str(action_id),
    )
    stdout_handle = stdout_path.open("ab")
    stderr_handle = stderr_path.open("ab")
    try:
        kwargs: dict[str, Any] = {
            "stdin": subprocess.DEVNULL,
            "stdout": stdout_handle,
            "stderr": stderr_handle,
            "cwd": str(resolved_database.parent),
            "shell": False,
            "close_fds": True,
        }
        if os.name == "nt":
            kwargs["creationflags"] = int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
        else:
            kwargs["start_new_session"] = True
        process = subprocess.Popen(command, **kwargs)
    finally:
        stdout_handle.close()
        stderr_handle.close()
    return {
        "process_id": int(process.pid),
        "process_birth_token": process_identity(int(process.pid)),
    }

def _write_launch_diagnostic(
    database_path: Path,
    campaign_id: str,
    action_id: str,
    *,
    stage: str,
    exception_type: str | None = None,
) -> None:
    # Persist one private bounded launch diagnostic next to the campaign database.
    payload = {
        "schema_version": 1,
        "campaign_id": str(campaign_id),
        "action_id": str(action_id),
        "stage": str(stage),
        "exception_type": str(exception_type) if exception_type else None,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "stderr_log": "campaign-launch.stderr.log",
    }
    path = Path(database_path).expanduser().resolve().parent / "campaign-launch.diagnostic.json"
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        os.replace(temporary, path)
    except OSError:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass

def _clear_launch_diagnostic(database_path: Path) -> None:
    # Remove one stale prior-attempt launch diagnostic before a fresh coordinator resume begins.
    try:
        Path(database_path).expanduser().resolve().parent.joinpath("campaign-launch.diagnostic.json").unlink(missing_ok=True)
    except OSError:
        return

def run_campaign_action(
    database_path: Path,
    campaign_id: str,
    action_id: str,
    *,
    resume_executor: Callable[[Path, str], object] | None = None,
) -> int:
    # Execute one durable start action and materialize its terminal result exactly once.
    resolved_database = Path(database_path).expanduser().resolve()
    store = SQLiteMutationStore(resolved_database)
    try:
        service = MutationCampaignService(store)
        action_result = service.get_operator_action(str(action_id))
        if not isinstance(action_result, Success) or action_result.value is None:
            return 2
        action = action_result.value
        if action.action_type != "start_campaign" or action.campaign_id.value != str(campaign_id):
            return 2
        if action.status == OperatorActionStatus.COMPLETED:
            return 0
        if action.status in {OperatorActionStatus.REJECTED, OperatorActionStatus.FAILED}:
            return 1
        if action.status == OperatorActionStatus.REQUESTED:
            started = service.start_operator_action(str(action_id))
            if not isinstance(started, Success):
                return 2
        running = service.update_running_operator_action(
            str(action_id),
            {
                "campaign_revision": action.expected_revision,
                "process_id": os.getpid(),
                "process_birth_token": process_identity(os.getpid()),
            },
        )
        if not isinstance(running, Success):
            return 2
        if running.value.status == OperatorActionStatus.COMPLETED:
            return 0
        if running.value.status in {OperatorActionStatus.REJECTED, OperatorActionStatus.FAILED}:
            return 1
    finally:
        store.close()
    _clear_launch_diagnostic(resolved_database)
    try:
        executor = resume_executor or LocalCampaignCoordinator.resume_campaign
        result = executor(resolved_database, str(campaign_id))
        campaign = getattr(result, "campaign", result)
        succeeded = bool(getattr(result, "succeeded", True))
        campaign_revision = int(getattr(campaign, "revision_number"))
        campaign_status = str(getattr(getattr(campaign, "status", "unknown"), "value", getattr(campaign, "status", "unknown")))
    except Exception as exc:
        if type(exc).__name__ == "StartupRecoveryBusy":
            return 0
        traceback.print_exc(file=sys.stderr)
        _write_launch_diagnostic(
            resolved_database,
            str(campaign_id),
            str(action_id),
            stage="resume_campaign",
            exception_type=type(exc).__name__,
        )
        store = SQLiteMutationStore(resolved_database)
        try:
            service = MutationCampaignService(store)
            service.fail_operator_action(
                str(action_id),
                "campaign_launch_failed",
                retriable=True,
                result={"stage": "resume_campaign", "exception_type": type(exc).__name__},
            )
        finally:
            store.close()
        return 1
    store = SQLiteMutationStore(resolved_database)
    try:
        service = MutationCampaignService(store)
        if succeeded:
            completed = service.complete_operator_action(
                str(action_id),
                {
                    "campaign_revision": campaign_revision,
                    "campaign_status": campaign_status,
                },
            )
            return 0 if isinstance(completed, Success) else 2
        _write_launch_diagnostic(
            resolved_database,
            str(campaign_id),
            str(action_id),
            stage="campaign",
        )
        failed = service.fail_operator_action(
            str(action_id),
            "campaign_failed",
            retriable=False,
            result={
                "campaign_revision": campaign_revision,
                "campaign_status": campaign_status,
                "stage": "campaign",
            },
        )
        return 1 if isinstance(failed, Success) else 2
    finally:
        store.close()
def build_parser() -> argparse.ArgumentParser:
    # Build the private detached-worker command without exposing extra network surfaces.
    parser = argparse.ArgumentParser(prog="python -m theseus_local.launcher")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument("--database", required=True)
    run.add_argument("--campaign-id", required=True)
    run.add_argument("--action-id", required=True)
    return parser
def main(argv: list[str] | None = None) -> int:
    # Dispatch one private launch-worker command and return a conventional exit code.
    args = build_parser().parse_args(argv)
    if args.command == "run":
        return run_campaign_action(
            Path(args.database),
            args.campaign_id,
            args.action_id,
        )
    return 2
if __name__ == "__main__":
    raise SystemExit(main())
__all__ = [
    "launch_process_is_alive",
    "process_identity",
    "run_campaign_action",
    "spawn_campaign_action",
]
