from __future__ import annotations

from pathlib import Path

import pytest

from theseus_local.worker_runtime import PersistentWorkerEntrypoint


def _write_run_artifacts(root: Path, run_id: str) -> None:
    # Materialize one worker-local shard artifact set with unrelated campaign evidence beside it.
    root.mkdir(parents=True)
    (root / f"{run_id}.json").write_text('{"status":"complete"}\n', encoding="utf-8")
    (root / f"{run_id}.results.jsonl").write_text('{"status":"killed"}\n', encoding="utf-8")
    (root / "unrelated.json").write_text("{}\n", encoding="utf-8")
    recovery = root / "recovery" / run_id
    recovery.mkdir(parents=True)
    (recovery / "original.py.bin").write_bytes(b"source")
    events = root / "test_stats_events" / run_id
    events.mkdir(parents=True)
    (events / "event.json").write_text("{}\n", encoding="utf-8")


def test_worker_publishes_only_completed_run_artifacts_to_canonical_root(tmp_path: Path) -> None:
    # Preserve worker-local evidence while atomically exposing the same run through the canonical engine tree.
    run_id = "engine-shard-campaign-shard-000-attempt-0"
    source = tmp_path / "worker"
    destination = tmp_path / "canonical"
    _write_run_artifacts(source, run_id)

    published = PersistentWorkerEntrypoint._publish_engine_run_artifacts(
        source,
        destination,
        run_id,
    )

    assert f"{run_id}.json" in published
    assert (destination / f"{run_id}.json").is_file()
    assert (destination / "recovery" / run_id / "original.py.bin").read_bytes() == b"source"
    assert (destination / "test_stats_events" / run_id / "event.json").is_file()
    assert not (destination / "unrelated.json").exists()
    assert (source / f"{run_id}.json").is_file()


def test_worker_rejects_conflicting_canonical_engine_artifact(tmp_path: Path) -> None:
    # Fail closed when another worker already published different bytes under the same run identity.
    run_id = "engine-shard-campaign-shard-000-attempt-0"
    source = tmp_path / "worker"
    destination = tmp_path / "canonical"
    _write_run_artifacts(source, run_id)
    destination.mkdir(parents=True)
    (destination / f"{run_id}.json").write_text('{"status":"different"}\n', encoding="utf-8")

    with pytest.raises(RuntimeError, match="conflicts with worker evidence"):
        PersistentWorkerEntrypoint._publish_engine_run_artifacts(source, destination, run_id)


def test_worker_process_publishes_fake_engine_report_before_delivery(tmp_path: Path) -> None:
    # Exercise the complete parent-to-agent-to-engine publication path before durable delivery ACK.
    import sys
    from datetime import datetime, timedelta, timezone

    from theseus_contracts import ShardAssignment
    from theseus_local.worker_runtime import PersistentWorkerProcess

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    worker_reports = tmp_path / "worker-reports"
    canonical_root = tmp_path / "canonical-engine"
    engine_script = tmp_path / "publishing_engine.py"
    engine_script.write_text(
        "import json, pathlib, sys\n"
        "configuration = None\n"
        "for line in sys.stdin:\n"
        "    frame = json.loads(line)\n"
        "    command = frame['command']\n"
        "    if command == 'prepare':\n"
        "        configuration = frame['request']['configuration']\n"
        "        result = {'campaign_id': 'campaign-publish', 'source_path': 'app.py', 'source_sha256': 'source-sha', 'index_version': 'index-v1', 'mutants': [{'mutant_id': 'm1', 'mutation': 'condition_to_not', 'source_path': 'app.py', 'line_no': 1, 'column_no': 0, 'original': 'x', 'replacement': 'y'}]}\n"
        "    elif command == 'execute-shard':\n"
        "        run_id = 'engine-shard-campaign-publish-shard-000-attempt-0'\n"
        "        root = pathlib.Path(configuration['reports_dir']) / 'engine' / 'campaign-publish'\n"
        "        root.mkdir(parents=True, exist_ok=True)\n"
        "        (root / f'{run_id}.json').write_text(json.dumps({'status': 'complete', 'results': []}) + '\\\\n', encoding='utf-8')\n"
        "        result = {'shard_id': 'shard-000', 'worker_id': 'worker-publish', 'status': 'complete', 'completed_mutants': 0, 'results': []}\n"
        "    elif command == 'shutdown':\n"
        "        result = {'status': 'stopped'}\n"
        "    else:\n"
        "        result = {}\n"
        "    print(json.dumps({'request_id': frame['request_id'], 'ok': True, 'result': result}), flush=True)\n"
        "    if command == 'shutdown':\n"
        "        break\n",
        encoding="utf-8",
    )
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat().replace("+00:00", "Z")
    assignment = ShardAssignment(
        campaign_id="campaign-publish",
        shard_id="shard-000",
        lease_id="lease-publish",
        attempt=0,
        mutant_ids=("m1",),
        prepared_snapshot_id="snapshot-publish",
        workspace_descriptor_id="workspace-publish",
        expires_at=expires_at,
    )
    run_id = "engine-shard-campaign-publish-shard-000-attempt-0"
    with PersistentWorkerProcess(
        worker_id="worker-publish",
        instance_id="instance-publish",
        spool_root=tmp_path / "spool",
        heartbeat_interval_seconds=0.01,
        cwd=Path(__file__).parents[1],
    ) as worker:
        worker.wait_for("registered")
        worker.wait_for("acquire")
        correlation_id = worker.send_engine_assignment(
            assignment,
            configuration={
                "campaign_id": assignment.campaign_id,
                "reports_dir": str(worker_reports),
            },
            execute_request={
                "campaign_id": assignment.campaign_id,
                "shard": {"shard_id": assignment.shard_id, "mutant_ids": ["m1"]},
                "attempt": 0,
                "worker_id": "worker-publish",
                "lease_id": assignment.lease_id,
                "test_overrides": {},
            },
            workspace=workspace,
            report_root=tmp_path / "transport",
            publish_engine_root=canonical_root,
            expected_source_sha256="source-sha",
            expected_mutant_ids=("m1",),
            test_fingerprints={"m1": {}},
            command_timeouts={"prepare": 5.0, "execute-shard": 5.0, "shutdown": 2.0},
            engine_command=(sys.executable, str(engine_script)),
        )
        delivery = worker.wait_for("delivery", correlation_id=correlation_id)
        assert (worker_reports / "engine" / assignment.campaign_id / f"{run_id}.json").is_file()
        assert (canonical_root / f"{run_id}.json").is_file()
        worker.acknowledge(delivery["payload"]["event_id"])
        worker.wait_for("acknowledged", correlation_id=correlation_id)
