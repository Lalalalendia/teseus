from __future__ import annotations

import os
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from theseus_local.network_control import NetworkCoordinator, NetworkWorkerAgent
from theseus_local.network_security import EnrollmentAuthority

from post_pr63_helpers import remote_fixture


@pytest.mark.scale
def test_real_network_campaign_accounts_for_100_plus_mutants_with_two_hosts(tmp_path: Path) -> None:
    size = max(100, int(os.environ.get("THESEUS_NETWORK_SCALE_SIZE", "100")))
    coordinator_store, template, _ = remote_fixture(tmp_path / "source")
    requests = tuple(
        replace(
            template,
            execution_attempt_id=f"network-scale-attempt-{index}",
            evidence_identity=f"network-scale-evidence-{index}",
            mutation_identity=f"network-scale-mutation-{index}",
            argv=(sys.executable, "-c", "print('network-scale')"),
        )
        for index in range(size)
    )
    secret = "network-scale-100-secret-0123456789"
    enrollment = EnrollmentAuthority(tmp_path / "enrollment.json", secret)
    for worker_id in ("host-a", "host-b"):
        enrollment.authorize(worker_id, runtime_fingerprint=template.runtime_identity["runtime_fingerprint"])
    coordinator = NetworkCoordinator(
        state_path=tmp_path / "scheduler.json",
        artifact_store=coordinator_store,
        enrollment=enrollment,
    )
    address = coordinator.start()
    agents = [
        NetworkWorkerAgent(
            host=address[0],
            port=address[1],
            shared_secret=secret,
            worker_id=worker_id,
            root=tmp_path / worker_id,
        )
        for worker_id in ("host-a", "host-b")
    ]
    running = [agent.start_background() for agent in agents]
    try:
        assert coordinator.wait_for_workers(2, timeout_seconds=10.0) == ("host-a", "host-b")
        report = coordinator.run(requests, timeout_seconds=max(90.0, size * 1.5), campaign_id="network-scale-100")
        assert len(report.results) == size
        assert len(set(report.semantic_outcomes)) == size
        assert len(report.scheduler_snapshot["authoritative"]) == size
        assert report.metrics.bytes_transferred > 0
        assert report.metrics.cache_hits >= size - 2
        assert report.metrics.counters["result.accepted"] == size
        assert report.metrics.stale_results == 0
        for worker_id in ("host-a", "host-b"):
            assert not tuple((tmp_path / worker_id / "workspaces").glob(".attempt-*/workspace"))
    finally:
        for agent, (thread, stop) in zip(agents, running):
            stop.set()
            if agent.connection is not None:
                agent.connection.close()
            thread.join(timeout=10.0)
        coordinator.stop()
