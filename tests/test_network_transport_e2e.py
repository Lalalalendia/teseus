from __future__ import annotations

import socket
import sys
import threading
import time
import json
from datetime import datetime, timedelta, timezone
from dataclasses import replace
from pathlib import Path

import pytest

from theseus_local.network_control import NetworkCoordinator, NetworkWorkerAgent
from theseus_local.distributed_observability import DistributedObservability
from theseus_local.network_security import EnrollmentAuthority
from theseus_local.network_transport import (
    FramedSocket,
    TransportClosed,
    TransportFramingError,
    TransportMessage,
    TransportTimeout,
)

from post_pr63_helpers import remote_fixture


class _DuplicateResultWorker(NetworkWorkerAgent):
    def _execute_assignment(
        self,
        request,
        lease_id,
        attempt,
        leader_epoch,
        connection,
        session_stop,
        runtime,
    ) -> None:
        super()._execute_assignment(request, lease_id, attempt, leader_epoch, connection, session_stop, runtime)
        if self.connection is connection and not self._stop.is_set() and not session_stop.is_set():
            duplicate = runtime.execute(request)
            connection.send_message(
                "execution.result",
                {
                    "lease_id": lease_id,
                    "attempt": int(attempt),
                    "leader_epoch": leader_epoch,
                    "result": duplicate.to_dict(),
                },
            )


def test_framed_transport_handles_partial_reads_and_coalesced_messages() -> None:
    left, right = socket.socketpair()
    sender = FramedSocket(left)
    receiver = FramedSocket(right)

    def write_fragments() -> None:
        payloads = []
        for index in (1, 2):
            raw = (
                b'{"transport_version":1,"message_type":"test","message_id":"msg-'
                + str(index).encode("ascii")
                + b'","reply_to":null,"created_at":"2026-01-01T00:00:00Z","payload":{"index":'
                + str(index).encode("ascii")
                + b'}}'
            )
            payloads.append(len(raw).to_bytes(8, "big") + raw)
        combined = b"".join(payloads)
        for offset in range(0, len(combined), 3):
            left.sendall(combined[offset : offset + 3])
    thread = threading.Thread(target=write_fragments)
    thread.start()
    first = receiver.receive_message(deadline=time.monotonic() + 2.0)
    second = receiver.receive_message(deadline=time.monotonic() + 2.0)
    thread.join(timeout=2.0)
    assert (first.payload["index"], second.payload["index"]) == (1, 2)
    sender.close()
    receiver.close()


def test_framed_transport_rejects_oversized_and_truncated_frames() -> None:
    left, right = socket.socketpair()
    sender = FramedSocket(left, max_frame_bytes=32)
    receiver = FramedSocket(right, max_frame_bytes=32)
    with pytest.raises(TransportFramingError):
        sender.send_bytes(b"x" * 33)
    left.sendall((10).to_bytes(8, "big") + b"short")
    left.close()
    with pytest.raises(TransportClosed):
        receiver.receive_bytes(deadline=time.monotonic() + 2.0)
    receiver.close()


def test_framed_transport_deadline_is_fail_closed() -> None:
    left, right = socket.socketpair()
    receiver = FramedSocket(right)
    with pytest.raises(TransportTimeout):
        receiver.receive_message(deadline=time.monotonic() + 0.05)
    left.close()
    receiver.close()


def test_framed_transport_handles_large_message_and_protocol_mismatch() -> None:
    left, right = socket.socketpair()
    sender = FramedSocket(left)
    receiver = FramedSocket(right)
    payload = {"blob": "x" * (2 * 1024 * 1024)}
    thread = threading.Thread(target=lambda: sender.send_message("large", payload))
    thread.start()
    message = receiver.receive_message(deadline=time.monotonic() + 5.0)
    thread.join(timeout=5.0)
    assert message.message_type == "large"
    assert len(message.payload["blob"]) == 2 * 1024 * 1024
    malformed = TransportMessage("bad", "bad-id", {}, transport_version=99, created_at="2026-01-01T00:00:00Z")
    raw = json.dumps(malformed.to_dict()).encode("utf-8")
    left.sendall(len(raw).to_bytes(8, "big") + raw)
    with pytest.raises(TransportFramingError, match="version"):
        receiver.receive_message(deadline=time.monotonic() + 2.0)
    sender.close()
    receiver.close()


def test_real_socket_coordinator_worker_e2e_preserves_semantics_and_cache(tmp_path: Path) -> None:
    coordinator_store, request, _ = remote_fixture(tmp_path / "source")
    secret = "network-test-secret-0123456789"
    enrollment = EnrollmentAuthority(tmp_path / "enrollment.json", secret)
    enrollment.authorize("worker-0", runtime_fingerprint=request.runtime_identity["runtime_fingerprint"])
    coordinator = NetworkCoordinator(
        state_path=tmp_path / "scheduler.json",
        artifact_store=coordinator_store,
        enrollment=enrollment,
        ha_state_path=tmp_path / "leader.json",
        coordinator_id="leader-a",
    )
    address = coordinator.start()
    agent = NetworkWorkerAgent(
        host=address[0],
        port=address[1],
        shared_secret=secret,
        worker_id="worker-0",
        root=tmp_path / "worker",
    )
    worker_thread, worker_stop = agent.start_background()
    try:
        assert coordinator.wait_for_workers(timeout_seconds=5.0) == ("worker-0",)
        report = coordinator.run((request,), timeout_seconds=20.0)
        assert report.semantic_outcomes == {request.evidence_identity: "survived"}
        assert report.results[0].workspace_integrity == "verified"
        assert report.transfer_stats["worker-0"].bytes_transferred > 0
        assert report.metrics.bytes_transferred > 0
        assert (tmp_path / "worker" / "cache" / request.project_snapshot_id).is_file()
    finally:
        worker_stop.set()
        if agent.connection is not None:
            agent.connection.close()
        worker_thread.join(timeout=5.0)
        coordinator.stop()


def test_duplicate_network_result_is_stale_and_never_second_authority(tmp_path: Path) -> None:
    coordinator_store, request, _ = remote_fixture(tmp_path / "source")
    observation = DistributedObservability()
    secret = "network-duplicate-secret-0123456789"
    enrollment = EnrollmentAuthority(tmp_path / "enrollment.json", secret)
    enrollment.authorize("worker-duplicate", runtime_fingerprint=request.runtime_identity["runtime_fingerprint"])
    coordinator = NetworkCoordinator(state_path=tmp_path / "scheduler.json", artifact_store=coordinator_store, enrollment=enrollment, observability=observation)
    address = coordinator.start()
    agent = _DuplicateResultWorker(host=address[0], port=address[1], shared_secret=secret, worker_id="worker-duplicate", root=tmp_path / "worker")
    thread, stop = agent.start_background()
    try:
        assert coordinator.wait_for_workers(timeout_seconds=5.0) == ("worker-duplicate",)
        report = coordinator.run((request,), timeout_seconds=20.0, campaign_id="duplicate-campaign")
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and observation.events() and not any(event.event_type == "result.stale" for event in observation.events()):
            time.sleep(0.02)
        assert len(report.scheduler_snapshot["authoritative"]) == 1
        assert any(event.event_type == "result.stale" for event in observation.events())
    finally:
        stop.set()
        if agent.connection is not None:
            agent.connection.close()
        thread.join(timeout=8.0)
        coordinator.stop()


def test_real_socket_warm_cache_reuses_verified_objects_and_recovers_corruption(tmp_path: Path) -> None:
    coordinator_store, request, _ = remote_fixture(tmp_path / "source")
    secret = "network-warm-secret-0123456789"
    enrollment = EnrollmentAuthority(tmp_path / "enrollment.json", secret)
    enrollment.authorize("worker-0", runtime_fingerprint=request.runtime_identity["runtime_fingerprint"])
    coordinator = NetworkCoordinator(
        state_path=tmp_path / "scheduler.json",
        artifact_store=coordinator_store,
        enrollment=enrollment,
    )
    address = coordinator.start()
    agent = NetworkWorkerAgent(host=address[0], port=address[1], shared_secret=secret, worker_id="worker-0", root=tmp_path / "worker")
    thread, stop = agent.start_background()
    try:
        assert coordinator.wait_for_workers(timeout_seconds=5.0) == ("worker-0",)
        first = coordinator.run((request,), timeout_seconds=20.0, campaign_id="cold")
        second_request = replace(
            request,
            execution_attempt_id="network-warm-attempt",
            evidence_identity="network-warm-evidence",
            mutation_identity="network-warm-mutation",
        )
        second = coordinator.run((second_request,), timeout_seconds=20.0, campaign_id="warm")
        assert first.metrics.bytes_transferred > 0
        assert second.metrics.bytes_transferred == 0
        assert second.metrics.cache_misses == 0
        assert second.metrics.cache_hits > 0
        corrupted = Path(tmp_path / "worker" / "cache" / request.project_snapshot_id)
        corrupted.write_bytes(b"corrupt-cache-entry")
        third_request = replace(
            request,
            execution_attempt_id="network-repair-attempt",
            evidence_identity="network-repair-evidence",
            mutation_identity="network-repair-mutation",
        )
        third = coordinator.run((third_request,), timeout_seconds=20.0, campaign_id="repair")
        assert third.metrics.bytes_transferred > 0
        assert third.metrics.cache_misses > 0
        assert corrupted.read_bytes() == coordinator_store.get_bytes(request.project_snapshot_id)
        assert all(record.pinned == 0 for record in coordinator_store.records())
    finally:
        stop.set()
        if agent.connection is not None:
            agent.connection.close()
        thread.join(timeout=5.0)
        coordinator.stop()


def test_network_deadline_while_queued_returns_without_dispatch(tmp_path: Path) -> None:
    coordinator_store, template, _ = remote_fixture(tmp_path / "source")
    slow = replace(
        template,
        execution_attempt_id="network-slow-attempt",
        evidence_identity="a-slow",
        mutation_identity="a-slow-mutation",
        argv=(sys.executable, "-c", "import time; time.sleep(0.4)"),
        timeout_seconds=5.0,
    )
    expired = replace(
        template,
        execution_attempt_id="network-expired-attempt",
        evidence_identity="z-expired",
        mutation_identity="z-expired-mutation",
        deadline_utc=(datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
    )
    secret = "network-deadline-secret-0123456789"
    enrollment = EnrollmentAuthority(tmp_path / "enrollment.json", secret)
    enrollment.authorize("worker-0", runtime_fingerprint=template.runtime_identity["runtime_fingerprint"])
    coordinator = NetworkCoordinator(state_path=tmp_path / "scheduler.json", artifact_store=coordinator_store, enrollment=enrollment)
    address = coordinator.start()
    agent = NetworkWorkerAgent(host=address[0], port=address[1], shared_secret=secret, worker_id="worker-0", root=tmp_path / "worker")
    thread, stop = agent.start_background()
    try:
        assert coordinator.wait_for_workers(timeout_seconds=5.0) == ("worker-0",)
        report = coordinator.run((slow, expired), timeout_seconds=20.0)
        by_evidence = {item.evidence_identity: item for item in report.results}
        assert by_evidence["a-slow"].started is True
        assert by_evidence["z-expired"].started is False
        assert by_evidence["z-expired"].timed_out is True
        assert "before network dispatch" in (by_evidence["z-expired"].diagnostic_error or "")
    finally:
        stop.set()
        if agent.connection is not None:
            agent.connection.close()
        thread.join(timeout=5.0)
        coordinator.stop()


def test_network_execution_deadline_terminates_fresh_process_and_returns_timeout_fact(tmp_path: Path) -> None:
    coordinator_store, template, _ = remote_fixture(tmp_path / "source")
    request = replace(
        template,
        execution_attempt_id="execution-timeout-attempt",
        evidence_identity="execution-timeout-evidence",
        mutation_identity="execution-timeout-mutation",
        argv=(sys.executable, "-c", "import time; time.sleep(2)"),
        timeout_seconds=0.1,
    )
    secret = "network-execution-timeout-secret-0123456789"
    enrollment = EnrollmentAuthority(tmp_path / "enrollment.json", secret)
    enrollment.authorize("worker-timeout", runtime_fingerprint=template.runtime_identity["runtime_fingerprint"])
    coordinator = NetworkCoordinator(state_path=tmp_path / "scheduler.json", artifact_store=coordinator_store, enrollment=enrollment)
    address = coordinator.start()
    agent = NetworkWorkerAgent(host=address[0], port=address[1], shared_secret=secret, worker_id="worker-timeout", root=tmp_path / "worker")
    thread, stop = agent.start_background()
    try:
        assert coordinator.wait_for_workers(timeout_seconds=5.0) == ("worker-timeout",)
        report = coordinator.run((request,), timeout_seconds=20.0)
        result = report.results[0]
        assert result.started is True
        assert result.timed_out is True
        assert result.workspace_integrity == "verified"
        assert report.semantic_outcomes == {request.evidence_identity: "timeout"}
    finally:
        stop.set()
        if agent.connection is not None:
            agent.connection.close()
        thread.join(timeout=8.0)
        coordinator.stop()


def test_network_worker_advertised_capacity_dispatches_multiple_leases_without_duplication(tmp_path: Path) -> None:
    coordinator_store, template, _ = remote_fixture(tmp_path / "source")
    requests = tuple(
        replace(
            template,
            execution_attempt_id=f"capacity-attempt-{index}",
            evidence_identity=f"capacity-evidence-{index}",
            mutation_identity=f"capacity-mutation-{index}",
            argv=(sys.executable, "-c", "import time; time.sleep(0.15)"),
        )
        for index in range(8)
    )
    secret = "network-capacity-secret-0123456789"
    enrollment = EnrollmentAuthority(tmp_path / "enrollment.json", secret)
    enrollment.authorize("worker-capacity", runtime_fingerprint=template.runtime_identity["runtime_fingerprint"], slots=2)
    coordinator = NetworkCoordinator(state_path=tmp_path / "scheduler.json", artifact_store=coordinator_store, enrollment=enrollment)
    address = coordinator.start()
    agent = NetworkWorkerAgent(host=address[0], port=address[1], shared_secret=secret, worker_id="worker-capacity", root=tmp_path / "worker", slots=2)
    thread, stop = agent.start_background()
    try:
        assert coordinator.wait_for_workers(timeout_seconds=5.0) == ("worker-capacity",)
        report = coordinator.run(requests, timeout_seconds=30.0)
        assert len(report.results) == len(requests)
        assert len({item.evidence_identity for item in report.results}) == len(requests)
        assert report.scheduler_snapshot["workers"]["worker-capacity"]["available_slots"] == 2
        assert report.metrics.counters["result.accepted"] == len(requests)
    finally:
        stop.set()
        if agent.connection is not None:
            agent.connection.close()
        thread.join(timeout=8.0)
        coordinator.stop()


def test_network_cancel_fences_running_attempt_and_reports_cancelled(tmp_path: Path) -> None:
    coordinator_store, template, _ = remote_fixture(tmp_path / "source")
    request = replace(
        template,
        execution_attempt_id="cancel-attempt",
        evidence_identity="cancel-evidence",
        mutation_identity="cancel-mutation",
        argv=(sys.executable, "-c", "import time; time.sleep(5)"),
        timeout_seconds=10.0,
    )
    secret = "network-cancel-secret-0123456789"
    enrollment = EnrollmentAuthority(tmp_path / "enrollment.json", secret)
    enrollment.authorize("worker-cancel", runtime_fingerprint=template.runtime_identity["runtime_fingerprint"])
    coordinator = NetworkCoordinator(state_path=tmp_path / "scheduler.json", artifact_store=coordinator_store, enrollment=enrollment)
    address = coordinator.start()
    agent = NetworkWorkerAgent(host=address[0], port=address[1], shared_secret=secret, worker_id="worker-cancel", root=tmp_path / "worker")
    thread, stop = agent.start_background()
    outcome: list[object] = []
    try:
        assert coordinator.wait_for_workers(timeout_seconds=5.0) == ("worker-cancel",)
        runner = threading.Thread(target=lambda: outcome.append(coordinator.run((request,), timeout_seconds=20.0, campaign_id="cancel-campaign")), daemon=True)
        runner.start()
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and coordinator.scheduler.active_lease_count() == 0:
            time.sleep(0.02)
        assert coordinator.scheduler.active_lease_count() == 1
        assert coordinator.cancel(request.evidence_identity, reason="operator_cancelled") is True
        runner.join(timeout=15.0)
        assert not runner.is_alive()
        assert len(outcome) == 1
        report = outcome[0]
        assert report.semantic_outcomes == {request.evidence_identity: "cancelled"}
        assert report.results[0].cancelled is True
        assert coordinator.scheduler.active_lease_count() == 0
    finally:
        stop.set()
        if agent.connection is not None:
            agent.connection.close()
        thread.join(timeout=8.0)
        coordinator.stop()


def test_real_socket_two_host_scale_has_single_authority_per_evidence(tmp_path: Path) -> None:
    coordinator_store, template, _ = remote_fixture(tmp_path / "source")
    requests = tuple(
        replace(
            template,
            execution_attempt_id=f"network-attempt-{index}",
            evidence_identity=f"network-evidence-{index}",
            mutation_identity=f"network-mutation-{index}",
        )
        for index in range(24)
    )
    secret = "network-scale-secret-0123456789"
    enrollment = EnrollmentAuthority(tmp_path / "enrollment.json", secret)
    enrollment.authorize("worker-a", runtime_fingerprint=template.runtime_identity["runtime_fingerprint"])
    enrollment.authorize("worker-b", runtime_fingerprint=template.runtime_identity["runtime_fingerprint"])
    coordinator = NetworkCoordinator(
        state_path=tmp_path / "scheduler.json",
        artifact_store=coordinator_store,
        enrollment=enrollment,
    )
    address = coordinator.start()
    agents = [
        NetworkWorkerAgent(host=address[0], port=address[1], shared_secret=secret, worker_id=worker_id, root=tmp_path / worker_id)
        for worker_id in ("worker-a", "worker-b")
    ]
    running = [agent.start_background() for agent in agents]
    try:
        assert coordinator.wait_for_workers(2, timeout_seconds=5.0) == ("worker-a", "worker-b")
        report = coordinator.run(requests, timeout_seconds=40.0)
        assert len(report.results) == 24
        assert set(report.semantic_outcomes) == {item.evidence_identity for item in requests}
        assert set(report.scheduler_snapshot["authoritative"]) == set(report.semantic_outcomes)
        assert report.metrics.bytes_transferred > 0
        assert sum(report.metrics.counters.get(name, 0) for name in ("result.accepted",)) == 24
        assert set(report.transfer_stats) == {"worker-a", "worker-b"}
    finally:
        for agent, (thread, stop) in zip(agents, running):
            stop.set()
            if agent.connection is not None:
                agent.connection.close()
            thread.join(timeout=5.0)
        coordinator.stop()
