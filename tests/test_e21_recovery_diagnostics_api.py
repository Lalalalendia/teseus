from __future__ import annotations
import json
import os
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from theseus_api import ApiSuccess, LocalApiService
from theseus_local.worker_runtime.spool import DurableExecutionSpool


def _campaign_payload(root: Path) -> dict[str, object]:
    # Build one compact campaign projection accepted by the local API read model.
    return {
        "campaign_id": "campaign-recovery-ui",
        "project_id": "project-recovery-ui",
        "revision_id": "revision-recovery-ui",
        "status": "running",
        "mode": "mutation",
        "plan_id": "plan-recovery-ui",
        "prepared_snapshot_id": "snapshot-recovery-ui",
        "scope": {"source_path": "app.py", "function": "choose"},
        "total_mutants": 2,
        "completed_mutants": 1,
        "revision_number": 4,
        "configuration": {
            "project": {
                "project_id": "project-recovery-ui",
                "display_name": "Recovery UI",
                "root_path": str(root),
            }
        },
    }


def _write_database(root: Path) -> tuple[Path, Path]:
    # Materialize one campaign with an expired lease, stopped worker, spool, and registry row.
    database = root / "campaign.sqlite3"
    spool_root = root / "private" / "worker-spool"
    spool = DurableExecutionSpool(spool_root)
    spool.publish(
        "delivery-1",
        {
            "assignment": {
                "campaign_id": "campaign-recovery-ui",
                "shard_id": "shard-1",
                "lease_id": "lease-1",
                "attempt": 0,
            },
            "result": {"results": [{"mutant_id": "mutant-1"}]},
        },
    )
    connection = sqlite3.connect(database)
    connection.executescript(
        """
        CREATE TABLE mutation_campaigns(campaign_id TEXT PRIMARY KEY, revision_number INTEGER, payload TEXT NOT NULL);
        CREATE TABLE mutation_leases(lease_id TEXT PRIMARY KEY, revision_number INTEGER, payload TEXT NOT NULL);
        CREATE TABLE mutation_workers(worker_key TEXT PRIMARY KEY, revision_number INTEGER, payload TEXT NOT NULL);
        CREATE TABLE mutation_outbox(effect_id TEXT PRIMARY KEY, event_type TEXT, campaign_id TEXT, payload TEXT, created_at TEXT, delivered_at TEXT);
        CREATE TABLE mutation_finalizations(campaign_id TEXT PRIMARY KEY, status TEXT, payload TEXT);
        CREATE TABLE mutation_artifacts(logical_key TEXT, campaign_id TEXT, payload TEXT, PRIMARY KEY(campaign_id, logical_key));
        """
    )
    campaign = _campaign_payload(root)
    connection.execute(
        "INSERT INTO mutation_campaigns VALUES (?, ?, ?)",
        ("campaign-recovery-ui", 4, json.dumps(campaign)),
    )
    lease = {
        "campaign_id": "campaign-recovery-ui",
        "shard_id": "shard-1",
        "lease_id": "lease-1",
        "worker_id": "worker-1",
        "attempt": 0,
        "lease_seconds": 1.0,
        "heartbeat_at": "2020-01-01T00:00:00Z",
        "status": "running",
        "revision_number": 2,
    }
    connection.execute(
        "INSERT INTO mutation_leases VALUES (?, ?, ?)",
        ("lease-1", 2, json.dumps(lease)),
    )
    worker = {
        "campaign_id": "campaign-recovery-ui",
        "identity": {
            "worker_id": "worker-1",
            "instance_id": "instance-1",
            "process_id": os.getpid(),
            "process_birth_token": "not-the-current-token",
        },
        "capabilities": {},
        "workspace": str(root / "private" / "workspace"),
        "spool_path": str(spool_root),
        "status": "stopped",
        "last_heartbeat_at": "2020-01-01T00:00:00Z",
        "workspace_healthy": True,
        "revision_number": 3,
    }
    connection.execute(
        "INSERT INTO mutation_workers VALUES (?, ?, ?)",
        ("campaign-recovery-ui\x1fworker-1", 3, json.dumps(worker)),
    )
    connection.execute(
        "INSERT INTO mutation_outbox VALUES (?, ?, ?, ?, ?, NULL)",
        ("effect-1", "mutation.execute_shard", "campaign-recovery-ui", "{}", "2026-08-06T00:00:00Z"),
    )
    connection.execute(
        "INSERT INTO mutation_finalizations VALUES (?, ?, ?)",
        ("campaign-recovery-ui", "registered", "{}"),
    )
    artifact = {
        "campaign_id": "campaign-recovery-ui",
        "logical_key": "report",
        "logical_role": "campaign_report",
        "content_sha256": "a" * 64,
        "size_bytes": 10,
        "schema_version": 1,
        "producer": "coordinator",
        "created_at": "2026-08-06T00:00:00Z",
        "logical_path": str(root / "private" / "report.json"),
        "content_path": str(root / "private" / "content"),
        "metadata": {"workspace_path": str(root / "private" / "workspace"), "safe": "value"},
    }
    connection.execute(
        "INSERT INTO mutation_artifacts VALUES (?, ?, ?)",
        ("report", "campaign-recovery-ui", json.dumps(artifact)),
    )
    connection.commit()
    connection.close()
    recovery = root / "recovery.json"
    recovery.write_text(
        json.dumps(
            {
                "status": "complete",
                "completed_at": "2026-08-06T00:00:00Z",
                "actions": [],
            }
        ),
        encoding="utf-8",
    )
    return database, recovery


class _KnowledgeStore:
    def __init__(self, _: Path) -> None:
        # Keep one frozen reuse plan for bounded API pagination tests.
        self.plan = {
            "decisions": [
                {
                    "mutant_id": "mutant-1",
                    "kind": "exact",
                    "eligible": True,
                    "authorized": True,
                    "audit_required": True,
                    "evidence_quality": "validated",
                    "result_status": "killed",
                    "source_event_id": "event-1",
                    "source_execution_id": "execution-1",
                    "blockers": [],
                },
                {
                    "mutant_id": "mutant-2",
                    "kind": "none",
                    "eligible": False,
                    "authorized": False,
                    "audit_required": False,
                    "evidence_quality": "ineligible",
                    "blockers": ["fingerprint_mismatch"],
                },
            ]
        }

    def query_reuse_decisions(
        self,
        campaign_id: str,
        *,
        limit: int,
        after_mutant_id: str | None = None,
    ) -> object:
        # Page the frozen fixture ledger through the same keyset contract as production.
        decisions = list(self.plan["decisions"]) if campaign_id == "campaign-recovery-ui" else []
        decisions.sort(key=lambda item: str(item["mutant_id"]))
        if after_mutant_id is not None:
            decisions = [item for item in decisions if str(item["mutant_id"]) > after_mutant_id]
        selected = decisions[:limit + 1]
        visible = selected[:limit]
        return SimpleNamespace(
            rows=tuple(visible),
            next_cursor=str(visible[-1]["mutant_id"]) if len(selected) > limit and visible else None,
        )

    def close(self) -> None:
        # Match the production store lifecycle without external state.
        return None


def test_recovery_diagnostics_are_bounded_and_hide_private_paths(tmp_path: Path) -> None:
    # Expose stalled work, pending spool, finalization, and blockers without filesystem identities.
    database, recovery = _write_database(tmp_path)
    service = LocalApiService(database, recovery_path_resolver=lambda _: recovery)
    outcome = service.get_recovery_diagnostics("campaign-recovery-ui", limit=1)
    assert isinstance(outcome, ApiSuccess)
    value = outcome.value
    assert value.campaign_revision == 4
    assert value.pending_outbox == 1
    assert value.pending_finalization is True
    assert value.leases.items[0].stalled is True
    assert value.spool.pending_count == 1
    assert value.spool.oldest_pending_at is not None
    assert value.spool.deliveries.items[0].event_id == "delivery-1"
    assert value.last_recovery_at == "2026-08-06T00:00:00Z"
    serialized = json.dumps(outcome.to_dict(), sort_keys=True).lower()
    for forbidden in (str(tmp_path).lower(), "workspace_path", "spool_path", "process_birth_token"):
        assert forbidden not in serialized


def test_artifact_registry_and_reuse_evidence_use_independent_keyset_pages(tmp_path: Path) -> None:
    # Keep artifact and reuse reads bounded while preserving planner-owned decision truth.
    database, _ = _write_database(tmp_path)
    service = LocalApiService(database, knowledge_store_factory=_KnowledgeStore)
    registry = service.get_artifact_registry("campaign-recovery-ui", limit=1)
    assert isinstance(registry, ApiSuccess)
    assert registry.value.finalization_status == "registered"
    assert registry.value.artifacts.items[0].logical_key == "report"
    assert registry.value.artifacts.items[0].metadata == {"safe": "value"}
    first = service.get_reuse_evidence("campaign-recovery-ui", limit=1)
    assert isinstance(first, ApiSuccess)
    assert first.value.items[0].authorized is True
    assert first.value.next_cursor is not None
    second = service.get_reuse_evidence(
        "campaign-recovery-ui",
        limit=1,
        cursor=first.value.next_cursor,
    )
    assert isinstance(second, ApiSuccess)
    assert second.value.items[0].mutant_id == "mutant-2"
