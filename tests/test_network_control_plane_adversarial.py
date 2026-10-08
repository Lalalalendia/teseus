from __future__ import annotations

import time
import sys
import threading
from datetime import datetime, timedelta, timezone
from dataclasses import replace
from pathlib import Path

import pytest

from theseus_local.coordinator_ha import CoordinatorAuthority, LeadershipError
from theseus_local.distributed_observability import CorrelationIds, DistributedObservability
from theseus_local.network_control import NetworkControlError, NetworkCoordinator, NetworkWorkerAgent
from theseus_local.network_security import EnrollmentAuthority, enrollment_proof
from theseus_local.network_transport import connect_tcp

from post_pr63_helpers import remote_fixture


SECRET = "network-adversarial-secret-0123456789"


def test_enrollment_rejects_unknown_wrong_and_revoked_workers(tmp_path: Path) -> None:
    authority = EnrollmentAuthority(tmp_path / "enrollment.json", SECRET)
    agent = NetworkWorkerAgent(
        host="127.0.0.1",
        port=1,
        shared_secret=SECRET,
        worker_id="unknown",
        root=tmp_path / "unknown",
    )
    payload = agent._enrollment_payload()
    assert not authority.verify(payload, "wrong").accepted
    assert authority.verify(payload, "wrong").reason == "unknown_worker"
    authority.authorize("unknown")
    assert authority.verify(payload, "wrong").reason == "invalid_credential"
    assert authority.verify(payload, enrollment_proof(SECRET, payload)).accepted
    authority.revoke("unknown")
    assert authority.verify(payload, enrollment_proof(SECRET, payload)).reason == "revoked_worker"


def test_durable_worker_identity_survives_restart_but_session_changes(tmp_path: Path) -> None:
    first = NetworkWorkerAgent(
        host="127.0.0.1",
        port=1,
        shared_secret=SECRET,
        worker_id="worker-identity",
        root=tmp_path / "worker",
    )
    second = NetworkWorkerAgent(
        host="127.0.0.1",
        port=1,
        shared_secret=SECRET,
        worker_id="worker-identity",
        root=tmp_path / "worker",
    )
    assert first.identity.identity_id == second.identity.identity_id
    assert first.session.session_id != second.session.session_id


def test_enrollment_rejects_expired_credential_and_duplicate_durable_identity(tmp_path: Path) -> None:
    expired = EnrollmentAuthority(tmp_path / "expired.json", SECRET)
    expired.authorize(
        "worker-expired",
        credential_expires_at=(datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
    )
    agent = NetworkWorkerAgent(host="127.0.0.1", port=1, shared_secret=SECRET, worker_id="worker-expired", root=tmp_path / "expired-worker")
    payload = agent._enrollment_payload()
    assert expired.verify(payload, enrollment_proof(SECRET, payload)).reason == "expired_credential"

    authority = EnrollmentAuthority(tmp_path / "identity.json", SECRET)
    authority.authorize("worker-bound")
    first = NetworkWorkerAgent(host="127.0.0.1", port=1, shared_secret=SECRET, worker_id="worker-bound", root=tmp_path / "bound-a")
    first_payload = first._enrollment_payload()
    assert authority.verify(first_payload, enrollment_proof(SECRET, first_payload)).accepted
    authority.record_session("worker-bound", first_payload)
    second = NetworkWorkerAgent(host="127.0.0.1", port=1, shared_secret=SECRET, worker_id="worker-bound", root=tmp_path / "bound-b")
    second_payload = second._enrollment_payload()
    assert authority.verify(second_payload, enrollment_proof(SECRET, second_payload)).reason == "duplicate_worker_identity"


def test_coordinator_authority_fences_split_brain_and_old_epoch(tmp_path: Path) -> None:
    authority = CoordinatorAuthority(tmp_path / "leader.json", lease_seconds=5.0)
    first = authority.acquire("leader-a")
    with pytest.raises(LeadershipError):
        authority.acquire("leader-b")
    authority.release("leader-a", first.epoch)
    second = authority.acquire("leader-b")
    assert second.epoch > first.epoch
    with pytest.raises(LeadershipError):
        authority.assert_leader("leader-a", first.epoch)
    authority.assert_leader("leader-b", second.epoch)


def test_observability_sink_failure_does_not_change_authority() -> None:
    class BrokenSink:
        def publish(self, event: object) -> None:
            raise RuntimeError("sink unavailable")

    observation = DistributedObservability(sink=BrokenSink())
    observation.record("result.accepted", CorrelationIds("campaign-1"))
    snapshot = observation.snapshot()
    assert snapshot.counters["result.accepted"] == 1
    assert snapshot.counters["observability_sink_errors"] == 1


def test_network_observability_reconstructs_phases_without_authority_dependency(tmp_path: Path) -> None:
    store, request, _ = remote_fixture(tmp_path / "fixture")

    class BrokenSink:
        def publish(self, event: object) -> None:
            raise RuntimeError("diagnostics unavailable")

    authority = EnrollmentAuthority(tmp_path / "enrollment.json", SECRET)
    authority.authorize("worker-observed", runtime_fingerprint=request.runtime_identity["runtime_fingerprint"])
    observation = DistributedObservability(sink=BrokenSink())
    coordinator = NetworkCoordinator(
        state_path=tmp_path / "scheduler.json",
        artifact_store=store,
        enrollment=authority,
        observability=observation,
    )
    address = coordinator.start()
    agent = NetworkWorkerAgent(host=address[0], port=address[1], shared_secret=SECRET, worker_id="worker-observed", root=tmp_path / "worker")
    thread, stop = agent.start_background()
    try:
        assert coordinator.wait_for_workers(timeout_seconds=5.0) == ("worker-observed",)
        diagnostic = authority.workers()[0]
        assert diagnostic["identity_id"] == agent.identity.identity_id
        assert diagnostic["capabilities"]
        assert diagnostic["available_slots"] == 1
        assert diagnostic["last_heartbeat_at"]
        report = coordinator.run((request,), timeout_seconds=20.0, campaign_id="observable-campaign")
        event_types = {event.event_type for event in observation.events()}
        assert {"request.queued", "artifact.transfer", "assignment.sent", "result.accepted"} <= event_types
        assert report.semantic_outcomes == {request.evidence_identity: "survived"}
        assert report.metrics.timings["artifact_transfer_seconds"] >= 0.0
        assert report.metrics.timings["result_round_trip_seconds"] >= 0.0
        assert report.metrics.counters["observability_sink_errors"] >= 1
    finally:
        stop.set()
        if agent.connection is not None:
            agent.connection.close()
        thread.join(timeout=8.0)
        coordinator.stop()


def test_real_network_rejects_revoked_session(tmp_path: Path) -> None:
    store, request, _ = remote_fixture(tmp_path / "fixture")
    authority = EnrollmentAuthority(tmp_path / "enrollment.json", SECRET)
    authority.authorize("worker-0", runtime_fingerprint=request.runtime_identity["runtime_fingerprint"])
    coordinator = NetworkCoordinator(
        state_path=tmp_path / "scheduler.json",
        artifact_store=store,
        enrollment=authority,
    )
    address = coordinator.start()
    agent = NetworkWorkerAgent(host=address[0], port=address[1], shared_secret=SECRET, worker_id="worker-0", root=tmp_path / "worker")
    thread, stop = agent.start_background()
    try:
        assert coordinator.wait_for_workers(timeout_seconds=5.0) == ("worker-0",)
        authority.revoke("worker-0")
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and coordinator.sessions():
            time.sleep(0.02)
        assert coordinator.sessions() == ()
        assert coordinator.scheduler.pending_count() == 0
        assert coordinator.scheduler.active_lease_count() == 0
    finally:
        stop.set()
        if agent.connection is not None:
            agent.connection.close()
        thread.join(timeout=5.0)
        coordinator.stop()


def test_unauthorized_socket_is_rejected_before_artifact_or_assignment_access(tmp_path: Path) -> None:
    store, request, _ = remote_fixture(tmp_path / "fixture")
    authority = EnrollmentAuthority(tmp_path / "enrollment.json", SECRET)
    coordinator = NetworkCoordinator(state_path=tmp_path / "scheduler.json", artifact_store=store, enrollment=authority)
    address = coordinator.start()
    agent = NetworkWorkerAgent(host=address[0], port=address[1], shared_secret=SECRET, worker_id="not-authorized", root=tmp_path / "worker")
    connection = connect_tcp(address[0], address[1])
    try:
        enrollment = agent._enrollment_payload()
        request_id = connection.send_message(
            "worker.enroll",
            {
                "enrollment": enrollment,
                "proof": enrollment_proof(SECRET, enrollment),
                "registration": agent.runtime.registration().to_dict(),
            },
        )
        response = connection.receive_message(deadline=time.monotonic() + 5.0)
        assert response.reply_to == request_id
        assert response.message_type == "worker.enrollment_rejected"
        assert coordinator.wait_for_workers(timeout_seconds=0.2) == ()
        assert coordinator.scheduler.pending_count() == 0
        assert not (tmp_path / "worker" / "cache" / request.project_snapshot_id).exists()
    finally:
        connection.close()
        coordinator.stop()


def test_worker_reconnect_preserves_identity_and_requeues_after_coordinator_restart(tmp_path: Path) -> None:
    store, request, _ = remote_fixture(tmp_path / "fixture")
    authority = EnrollmentAuthority(tmp_path / "enrollment.json", SECRET)
    authority.authorize("worker-0", runtime_fingerprint=request.runtime_identity["runtime_fingerprint"])
    first = NetworkCoordinator(
        state_path=tmp_path / "scheduler.json",
        artifact_store=store,
        enrollment=authority,
        coordinator_id="coordinator-first",
    )
    first_address = first.start()
    agent = NetworkWorkerAgent(
        host=first_address[0],
        port=first_address[1],
        shared_secret=SECRET,
        worker_id="worker-0",
        root=tmp_path / "worker",
        heartbeat_seconds=0.1,
        reconnect_seconds=0.05,
    )
    thread, stop = agent.start_background()
    try:
        assert first.wait_for_workers(timeout_seconds=5.0) == ("worker-0",)
        durable_identity = agent.identity.identity_id
        first.stop()
        second = NetworkCoordinator(
            state_path=tmp_path / "scheduler.json",
            artifact_store=store,
            enrollment=authority,
            coordinator_id="coordinator-second",
        )
        second_address = second.start()
        agent.port = second_address[1]
        try:
            deadline = time.monotonic() + 8.0
            while time.monotonic() < deadline and second.sessions() != ():
                time.sleep(0.05)
            assert second.wait_for_workers(timeout_seconds=8.0) == ("worker-0",)
            assert agent.identity.identity_id == durable_identity
            assert second.sessions()[0]["session_id"] != ""
            report = second.run((request,), timeout_seconds=20.0)
            assert report.semantic_outcomes == {request.evidence_identity: "survived"}
        finally:
            stop.set()
            if agent.connection is not None:
                agent.connection.close()
            second.stop()
    finally:
        stop.set()
        if agent.connection is not None:
            agent.connection.close()
        thread.join(timeout=8.0)


def test_network_coordinator_failover_preserves_authority_and_fences_old_epoch(tmp_path: Path) -> None:
    store, request, _ = remote_fixture(tmp_path / "fixture")
    authority = EnrollmentAuthority(tmp_path / "enrollment.json", SECRET)
    authority.authorize("worker-0", runtime_fingerprint=request.runtime_identity["runtime_fingerprint"])
    first = NetworkCoordinator(
        state_path=tmp_path / "scheduler.json",
        artifact_store=store,
        enrollment=authority,
        ha_state_path=tmp_path / "ha.json",
        coordinator_id="leader-a",
        lease_seconds=0.25,
    )
    first_address = first.start()
    agent = NetworkWorkerAgent(host=first_address[0], port=first_address[1], shared_secret=SECRET, worker_id="worker-0", root=tmp_path / "worker", reconnect_seconds=0.05)
    thread, stop = agent.start_background()
    try:
        assert first.wait_for_workers(timeout_seconds=5.0) == ("worker-0",)
        first_report = first.run((request,), timeout_seconds=20.0, campaign_id="ha-first")
        first_epoch = first_report.leader_epoch
        assert first_epoch is not None
        # Simulate a crashed process: stop serving and renewing, but do not
        # release the authority record as a graceful shutdown would.
        first._stop.set()
        first.server.stop()
        if first._authority_thread is not None:
            first._authority_thread.join(timeout=2.0)
        time.sleep(0.5)
        second = NetworkCoordinator(
            state_path=tmp_path / "scheduler.json",
            artifact_store=store,
            enrollment=authority,
            ha_state_path=tmp_path / "ha.json",
            coordinator_id="leader-b",
            lease_seconds=0.25,
        )
        second_address = second.start()
        agent.port = second_address[1]
        try:
            assert second.wait_for_workers(timeout_seconds=8.0) == ("worker-0",)
            second_request = replace(request, execution_attempt_id="ha-second", evidence_identity="ha-second-evidence", mutation_identity="ha-second-mutation")
            second_report = second.run((second_request,), timeout_seconds=20.0, campaign_id="ha-second")
            assert second_report.leader_epoch is not None and second_report.leader_epoch > first_epoch
            assert set(second_report.scheduler_snapshot["authoritative"]) == {request.evidence_identity, second_request.evidence_identity}
            with pytest.raises(NetworkControlError):
                first._assert_leader()
        finally:
            stop.set()
            if agent.connection is not None:
                agent.connection.close()
            second.stop()
    finally:
        stop.set()
        if agent.connection is not None:
            agent.connection.close()
        thread.join(timeout=8.0)
        first.stop()


def test_worker_disconnect_requeues_lease_and_second_host_completes_once(tmp_path: Path) -> None:
    store, request, _ = remote_fixture(tmp_path / "fixture")
    request = replace(request, argv=(sys.executable, "-c", "import time; time.sleep(0.5)"))
    authority = EnrollmentAuthority(tmp_path / "enrollment.json", SECRET)
    authority.authorize("worker-a", runtime_fingerprint=request.runtime_identity["runtime_fingerprint"])
    authority.authorize("worker-b", runtime_fingerprint=request.runtime_identity["runtime_fingerprint"])
    coordinator = NetworkCoordinator(
        state_path=tmp_path / "scheduler.json",
        artifact_store=store,
        enrollment=authority,
        lease_seconds=5.0,
    )
    address = coordinator.start()
    first = NetworkWorkerAgent(host=address[0], port=address[1], shared_secret=SECRET, worker_id="worker-a", root=tmp_path / "worker-a")
    first_thread, first_stop = first.start_background()
    outcome: list[object] = []
    second = NetworkWorkerAgent(host=address[0], port=address[1], shared_secret=SECRET, worker_id="worker-b", root=tmp_path / "worker-b")
    second_thread: tuple[object, object] | None = None
    try:
        assert coordinator.wait_for_workers(timeout_seconds=5.0) == ("worker-a",)
        runner = threading.Thread(
            target=lambda: outcome.append(coordinator.run((request,), timeout_seconds=20.0, campaign_id="worker-loss")),
            daemon=True,
        )
        runner.start()
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and coordinator.scheduler.active_lease_count() == 0:
            time.sleep(0.02)
        assert coordinator.scheduler.active_lease_count() == 1
        first_stop.set()
        if first.connection is not None:
            first.connection.close()
        first_thread.join(timeout=8.0)
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and coordinator.sessions():
            time.sleep(0.02)
        second_thread = second.start_background()
        assert coordinator.wait_for_workers(timeout_seconds=8.0) == ("worker-b",)
        runner.join(timeout=20.0)
        assert not runner.is_alive()
        assert len(outcome) == 1
        report = outcome[0]
        assert report.semantic_outcomes == {request.evidence_identity: "survived"}
        assert any(item.get("reason") == "worker_disconnected" for item in coordinator.scheduler.stale_records())
        assert len(report.scheduler_snapshot["authoritative"]) == 1
    finally:
        first_stop.set()
        if first.connection is not None:
            first.connection.close()
        if second_thread is not None:
            second_thread[1].set()
            if second.connection is not None:
                second.connection.close()
            second_thread[0].join(timeout=8.0)
        first_thread.join(timeout=8.0)
        coordinator.stop()
