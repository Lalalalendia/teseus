"""Replay and stale-attempt handling for coordinator-owned worker spools."""
from __future__ import annotations
from typing import Mapping
from .spool import DurableExecutionSpool, SpoolEntry
def matching_pending_delivery(
    spool: DurableExecutionSpool,
    *,
    campaign_id: str,
    shard_id: str,
    lease_id: str,
    attempt: int,
) -> SpoolEntry | None:
    # Return only a pending delivery owned by the exact current assignment identity.
    matches = []
    for entry in spool.pending():
        assignment = entry.payload.get("assignment")
        if not isinstance(assignment, Mapping):
            continue
        if (
            str(assignment.get("campaign_id")) == str(campaign_id)
            and str(assignment.get("shard_id")) == str(shard_id)
            and str(assignment.get("lease_id")) == str(lease_id)
            and int(assignment.get("attempt", -1)) == int(attempt)
        ):
            matches.append(entry)
    if len(matches) > 1:
        raise RuntimeError("worker spool contains multiple pending deliveries for one assignment")
    return matches[0] if matches else None
def quarantine_non_current_deliveries(
    spool: DurableExecutionSpool,
    *,
    campaign_id: str,
    shard_id: str,
    current_lease_id: str,
    current_attempt: int,
) -> tuple[str, ...]:
    # Quarantine old attempts while preserving any delivery that belongs to the live reassignment.
    quarantined: list[str] = []
    for entry in spool.pending():
        assignment = entry.payload.get("assignment")
        if not isinstance(assignment, Mapping):
            continue
        same_assignment = (
            str(assignment.get("campaign_id")) == str(campaign_id)
            and str(assignment.get("shard_id")) == str(shard_id)
        )
        current = (
            str(assignment.get("lease_id")) == str(current_lease_id)
            and int(assignment.get("attempt", -1)) == int(current_attempt)
        )
        if same_assignment and not current:
            spool.quarantine(entry.event_id, reason="stale_worker_attempt")
            quarantined.append(entry.event_id)
    return tuple(quarantined)
def recorded_process_is_alive(process_id: int | None, process_birth_token: str | None) -> bool:
    # Match a live OS process only when both its PID and immutable creation token still agree.
    if process_id is None or process_birth_token is None or int(process_id) <= 0:
        return False
    from test_intelligence_unified_v1.recovery import current_process_birth_token
    return current_process_birth_token(int(process_id)) == str(process_birth_token)

def terminate_recorded_process(process_id: int | None, process_birth_token: str | None) -> bool:
    # Terminate one abandoned process tree only after PID-reuse-safe birth-token verification.
    if not recorded_process_is_alive(process_id, process_birth_token):
        return False
    import os
    import signal
    import subprocess
    pid = int(process_id)
    if os.name == "nt":
        try:
            completed = subprocess.run(
                ("taskkill", "/PID", str(pid), "/T", "/F"),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=5,
            )
        except (OSError, subprocess.SubprocessError):
            completed = None
        if completed is not None and completed.returncode == 0:
            return True
        try:
            os.kill(pid, signal.SIGTERM)
        except (OSError, ProcessLookupError, PermissionError):
            return False
        return True
    try:
        process_group = os.getpgid(pid)
        if process_group == os.getpgrp():
            os.kill(pid, signal.SIGKILL)
        else:
            os.killpg(process_group, signal.SIGKILL)
    except (OSError, ProcessLookupError, PermissionError):
        try:
            os.kill(pid, signal.SIGKILL)
        except (OSError, ProcessLookupError, PermissionError):
            return False
    return True

__all__ = [
    "matching_pending_delivery",
    "quarantine_non_current_deliveries",
    "recorded_process_is_alive",
    "terminate_recorded_process",
]
