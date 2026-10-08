from __future__ import annotations

import os

import pytest

from test_intelligence_unified_v1.recovery import (
    current_process_birth_token,
    owner_is_alive,
)


@pytest.mark.skipif(os.name != "nt", reason="Windows process probe regression")
def test_windows_owner_probe_does_not_send_console_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Prove that checking a live Windows owner never calls os.kill or emits Ctrl+C.
    def reject_os_kill(pid: int, signal_number: int) -> None:
        # Fail immediately if the POSIX-only liveness probe returns on Windows.
        raise AssertionError(
            f"os.kill({pid}, {signal_number}) must not be used as a Windows liveness probe"
        )

    monkeypatch.setattr(os, "kill", reject_os_kill)

    token = current_process_birth_token()
    assert token is not None
    assert owner_is_alive(
        {
            "coordinator_pid": os.getpid(),
            "process_birth_token": token,
        }
    )


@pytest.mark.skipif(os.name != "nt", reason="Windows process probe regression")
def test_windows_owner_probe_rejects_reused_pid_identity() -> None:
    # Prove that a live PID with a different creation token is not the recorded owner.
    assert owner_is_alive(
        {
            "coordinator_pid": os.getpid(),
            "process_birth_token": "win:0000000000000000",
        }
    ) is False