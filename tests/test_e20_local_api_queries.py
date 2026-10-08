from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

from theseus_api import ApiRejected, ApiSuccess, LocalApiService


def _campaign_payload(
    campaign_id: str,
    project_id: str,
    *,
    status: str = "running",
    plan_id: str | None = None,
    total_mutants: int = 3,
) -> dict[str, object]:
    # Build one persisted campaign aggregate compatible with the authoritative SQLite projection.
    return {
        "campaign_id": campaign_id,
        "project_id": project_id,
        "revision_id": f"revision-{project_id}",
        "configuration": {
            "campaign_id": campaign_id,
            "project": {
                "project_id": project_id,
                "display_name": f"Project {project_id}",
                "root_path": f"D:/projects/{project_id}",
                "main_root_path": f"D:/projects/{project_id}",
                "revision": {"revision_id": f"revision-{project_id}"},
            },
            "scope": {"source_path": "app.py", "function": "calculate"},
        },
        "scope": {"source_path": "app.py", "function": "calculate"},
        "mode": "standard",
        "status": status,
        "plan_id": plan_id,
        "prepared_snapshot_id": f"snapshot-{campaign_id}" if plan_id else None,
        "total_mutants": total_mutants,
        "completed_mutants": 1,
        "revision_number": 4,
    }


def _worker_payload(campaign_id: str, worker_id: str, sequence: int) -> dict[str, object]:
    # Build one authoritative worker aggregate without relying on domain constructors in the API tests.
    return {
        "campaign_id": campaign_id,
        "identity": {
            "worker_id": worker_id,
            "instance_id": f"instance-{worker_id}",
            "process_id": 1000 + sequence,
            "process_birth_token": f"birth-{worker_id}",
        },
        "capabilities": {
            "platform": "windows",
            "architecture": "amd64",
            "python_versions": ["3.12"],
            "engine_protocol_versions": [1],
            "workspace_backends": ["copy"],
            "cpu_count": 4,
            "memory_limit_bytes": None,
        },
        "workspace": f"D:/state/{worker_id}",
        "spool_path": f"D:/state/{worker_id}/spool.jsonl",
        "status": "running",
        "heartbeat_sequence": sequence,
        "last_heartbeat_at": f"2026-08-05T12:00:0{sequence}Z",
        "current_shard_id": f"shard-{sequence}",
        "current_lease_id": f"lease-{sequence}",
        "current_attempt": 0,
        "current_mutant_id": f"mutant-{sequence}",
        "child_process_id": 2000 + sequence,
        "completed_mutants": sequence,
        "completed_assignments": sequence,
        "workspace_healthy": True,
        "revision_number": sequence,
    }


def _shard_payload(campaign_id: str, sequence: int) -> dict[str, object]:
    # Build one committed shard topology row with bounded membership.
    return {
        "shard_id": f"shard-{sequence}",
        "campaign_id": campaign_id,
        "ordinal": sequence - 1,
        "mutant_ids": [f"mutant-{sequence}"],
        "estimated_cost": float(sequence),
        "plan_id": "plan-1",
        "status": "running",
        "worker_id": f"worker-{sequence}",
        "lease": {"lease_id": f"lease-{sequence}"},
        "attempt": 0,
        "completed_count": 0,
        "revision_number": sequence,
    }


def _execution_payload(campaign_id: str, sequence: int) -> dict[str, object]:
    # Build one immutable execution projection with nested evidence represented only by counts in the API.
    return {
        "execution_id": f"execution-{sequence}",
        "campaign_id": campaign_id,
        "shard_id": f"shard-{sequence}",
        "mutant_id": f"mutant-{sequence}",
        "attempt": 0,
        "status": "complete",
        "semantic_result": "killed",
        "selected_tests": ["tests/test_app.py::test_value"],
        "artifacts": [f"artifact-{sequence}"],
        "duration_seconds": float(sequence),
        "restore_verified": True,
        "error": None,
        "revision_number": 1,
        "lease_id": f"lease-{sequence}",
        "test_observations": [{"test_id": "tests/test_app.py::test_value"}],
    }


def _artifact_payload(campaign_id: str, sequence: int) -> dict[str, object]:
    # Build one path-bearing registry row so the API test can prove paths are removed.
    return {
        "campaign_id": campaign_id,
        "logical_key": f"artifact-{sequence}",
        "logical_role": "execution_evidence",
        "content_sha256": f"{sequence:064x}",
        "size_bytes": sequence * 10,
        "schema_version": 1,
        "producer": "coordinator",
        "content_path": f"content/{sequence}/artifact.json",
        "logical_path": f"campaigns/{campaign_id}/artifact-{sequence}.json",
        "created_at": f"2026-08-05T12:00:0{sequence}Z",
        "shard_id": f"shard-{sequence}",
        "execution_id": f"execution-{sequence}",
        "metadata": {"kind": "json", "staging_path": "D:/private/staging.json"},
    }


def _create_campaign_database(path: Path) -> None:
    # Create the existing campaign SQLite schema and populate multiple bounded relation rows.
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE mutation_campaigns (campaign_id TEXT PRIMARY KEY, revision_number INTEGER NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE mutation_shards (shard_id TEXT PRIMARY KEY, revision_number INTEGER NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE mutation_workers (worker_key TEXT PRIMARY KEY, revision_number INTEGER NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE mutation_executions (execution_id TEXT PRIMARY KEY, revision_number INTEGER NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE mutation_artifacts (
                artifact_key TEXT PRIMARY KEY,
                campaign_id TEXT NOT NULL,
                logical_key TEXT NOT NULL,
                content_sha256 TEXT NOT NULL,
                payload TEXT NOT NULL,
                UNIQUE(campaign_id, logical_key)
            );
            CREATE TABLE mutation_finalizations (campaign_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL UNIQUE, status TEXT NOT NULL, payload TEXT NOT NULL);
            """
        )
        campaigns = (
            _campaign_payload("campaign-1", "project-1", plan_id="plan-1"),
            _campaign_payload("campaign-2", "project-1", status="completed", plan_id="plan-2", total_mutants=1),
            _campaign_payload("campaign-3", "project-2", plan_id=None, total_mutants=2),
        )
        connection.executemany(
            "INSERT INTO mutation_campaigns(campaign_id, revision_number, payload) VALUES (?, ?, ?)",
            [(str(item["campaign_id"]), int(item["revision_number"]), json.dumps(item)) for item in campaigns],
        )
        for sequence in range(1, 4):
            worker = _worker_payload("campaign-1", f"worker-{sequence}", sequence)
            shard = _shard_payload("campaign-1", sequence)
            execution = _execution_payload("campaign-1", sequence)
            artifact = _artifact_payload("campaign-1", sequence)
            connection.execute(
                "INSERT INTO mutation_workers(worker_key, revision_number, payload) VALUES (?, ?, ?)",
                (f"campaign-1\x1fworker-{sequence}", sequence, json.dumps(worker)),
            )
            connection.execute(
                "INSERT INTO mutation_shards(shard_id, revision_number, payload) VALUES (?, ?, ?)",
                (f"shard-{sequence}", sequence, json.dumps(shard)),
            )
            connection.execute(
                "INSERT INTO mutation_executions(execution_id, revision_number, payload) VALUES (?, ?, ?)",
                (f"execution-{sequence}", 1, json.dumps(execution)),
            )
            connection.execute(
                "INSERT INTO mutation_artifacts(artifact_key, campaign_id, logical_key, content_sha256, payload) VALUES (?, ?, ?, ?, ?)",
                (
                    f"campaign-1\x1fartifact-{sequence}",
                    "campaign-1",
                    f"artifact-{sequence}",
                    str(artifact["content_sha256"]),
                    json.dumps(artifact),
                ),
            )
        connection.execute(
            "INSERT INTO mutation_finalizations(campaign_id, intent_id, status, payload) VALUES (?, ?, ?, ?)",
            ("campaign-1", "intent-1", "registered", "{}"),
        )
        connection.commit()
    finally:
        connection.close()


@dataclass(frozen=True)
class _KnowledgeSummary:
    # Provide the public attributes consumed from KnowledgePlaneStore.summarize_campaign.
    executions: int = 2
    mutants: int = 2
    attempts: int = 3
    counts: dict[str, int] = None
    revision: int = 7

    def __post_init__(self) -> None:
        # Install an independent deterministic counter mapping for the fake boundary.
        object.__setattr__(self, "counts", {"killed": 2} if self.counts is None else dict(self.counts))


class _KnowledgeStore:
    def __init__(self, path: Path) -> None:
        # Retain the configured path only to mimic the real store constructor contract.
        self.path = path
        self.closed = False

    def summarize_campaign(self, campaign_id: str) -> _KnowledgeSummary:
        # Return one deterministic materialized campaign knowledge summary.
        assert campaign_id == "campaign-1"
        return _KnowledgeSummary()

    def query_executions(self, **kwargs):
        # Return one bounded page and verify raw payload materialization remains disabled.
        assert kwargs["campaign_id"] == "campaign-1"
        assert kwargs["include_payload"] is False
        rows = (
            {
                "event_id": "knowledge-event-1",
                "effect_id": "effect-1",
                "campaign_id": "campaign-1",
                "execution_id": "execution-1",
                "mutant_id": "mutant-1",
                "attempt": 0,
                "status": "killed",
                "created_at": "2026-08-05T12:00:00Z",
                "compacted": False,
                "function_id": "module.calculate",
                "source_kind": "observed",
                "evidence_quality": "validated",
                "evidence_schema_version": 1,
                "retention_class": "campaign",
                "project_id": "project-1",
                "revision_id": "revision-project-1",
                "environment_id": "environment-1",
            },
        )
        return SimpleNamespace(rows=rows, next_cursor=None, snapshot_revision=9, schema_version=4)

    def close(self) -> None:
        # Mark the fake store closed to match the production resource lifecycle.
        self.closed = True


class _StatisticsStore:
    def __init__(self, path: Path) -> None:
        # Retain the configured path only to mimic the real projection constructor contract.
        self.path = path
        self.item = SimpleNamespace(
            entity_type="campaign",
            entity_id="campaign-1",
            event_count=10,
            started_count=1,
            completed_count=1,
            passed_count=7,
            failed_count=1,
            error_count=1,
            timeout_count=1,
            retry_count=2,
            recovery_count=1,
            escalation_count=1,
            reuse_count=2,
            infrastructure_failure_count=1,
            flaky_transition_count=1,
            duration_count=8,
            duration_total_ms=800.0,
            duration_min_ms=10.0,
            duration_max_ms=200.0,
            duration_avg_ms=100.0,
            duration_median_ms=90.0,
            duration_p95_ms=180.0,
            duration_sample_count=8,
            busy_duration_ms=400.0,
            active_duration_ms=1000.0,
            utilization=0.4,
            active=False,
            last_outcome="passed",
            last_event_type="test.completed",
            last_event_timestamp="2026-08-05T12:00:10Z",
        )

    def query(self, entity_type: str, *, limit: int, cursor: str | None):
        # Return one bounded projection page using the same method shape as StatisticsProjectionStore.
        assert entity_type == "campaign"
        assert limit == 2
        assert cursor is None
        return SimpleNamespace(items=(self.item,), next_cursor=None)

    def get(self, entity_type: str, entity_id: str):
        # Return the exact fake projection only for its stable identity.
        return self.item if (entity_type, entity_id) == ("campaign", "campaign-1") else None


def _service(tmp_path: Path, *, observer=None) -> LocalApiService:
    # Build one API service over real campaign SQLite and injected knowledge/statistics read boundaries.
    database = tmp_path / "campaign.sqlite3"
    _create_campaign_database(database)
    recovery = tmp_path / "startup-recovery.report.json"
    recovery.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "database_path": str(database),
                "started_at": "2026-08-05T11:00:00Z",
                "completed_at": "2026-08-05T11:00:01Z",
                "status": "completed",
                "run_count": 2,
                "action_count": 3,
                "actions": [
                    {"campaign_id": "campaign-1", "status": "worker_stopped", "worker_id": "worker-1", "workspace_path": "D:/private/worker"},
                    {"campaign_id": "campaign-1", "status": "lease_recovered", "lease_id": "lease-1"},
                    {"campaign_id": "campaign-1", "status": "finalization_completed", "intent_id": "intent-1"},
                ],
                "error": None,
            }
        ),
        encoding="utf-8",
    )
    return LocalApiService(
        database,
        knowledge_database=tmp_path / "knowledge.sqlite3",
        statistics_database=tmp_path / "statistics.sqlite",
        knowledge_store_factory=_KnowledgeStore,
        statistics_store_factory=_StatisticsStore,
        recovery_path_resolver=lambda _: recovery,
        query_observer=observer,
    )


def test_campaign_and_project_lists_are_bounded_keyset_pages(tmp_path: Path) -> None:
    # Traverse stable project and campaign pages without duplicates or unbounded results.
    service = _service(tmp_path)
    first = service.list_campaigns(limit=1)
    assert isinstance(first, ApiSuccess)
    assert [item.campaign_id for item in first.value.items] == ["campaign-1"]
    assert first.value.next_cursor is not None
    second = service.list_campaigns(limit=1, cursor=first.value.next_cursor)
    assert isinstance(second, ApiSuccess)
    assert [item.campaign_id for item in second.value.items] == ["campaign-2"]
    projects = service.list_projects(limit=1)
    assert isinstance(projects, ApiSuccess)
    assert projects.value.items[0].project_id == "project-1"
    assert projects.value.items[0].campaign_count == 2
    assert projects.value.next_cursor is not None
    next_projects = service.list_projects(limit=1, cursor=projects.value.next_cursor)
    assert isinstance(next_projects, ApiSuccess)
    assert [item.project_id for item in next_projects.value.items] == ["project-2"]


def test_campaign_detail_uses_fixed_bounded_queries_without_n_plus_one(tmp_path: Path) -> None:
    # Load multiple related rows with one fixed query per relation and no per-row database reads.
    statements: list[str] = []
    service = _service(tmp_path, observer=statements.append)
    result = service.get_campaign("campaign-1", related_limit=2)
    assert isinstance(result, ApiSuccess)
    detail = result.value
    assert detail.plan is not None
    assert detail.plan.plan_id == "plan-1"
    assert len(detail.shards.items) == 2
    assert len(detail.workers.items) == 2
    assert len(detail.executions.items) == 2
    assert len(detail.artifacts.items) == 2
    assert all(page.next_cursor is not None for page in (detail.shards, detail.workers, detail.executions, detail.artifacts))
    assert detail.finalization_status == "registered"
    selects = [statement for statement in statements if statement.lstrip().upper().startswith(("SELECT", "WITH"))]
    assert len(selects) == 6
    repeated = service.get_campaign("campaign-1", related_limit=2)
    assert isinstance(repeated, ApiSuccess)
    assert repeated.to_dict() == result.to_dict()


def test_artifacts_and_recovery_hide_filesystem_structure_and_page_actions(tmp_path: Path) -> None:
    # Read artifact and recovery metadata without returning registry or report paths to the client.
    service = _service(tmp_path)
    artifacts = service.list_artifacts("campaign-1", limit=2)
    assert isinstance(artifacts, ApiSuccess)
    serialized = artifacts.to_dict()
    text = json.dumps(serialized, sort_keys=True)
    assert "content_path" not in text
    assert "logical_path" not in text
    assert "D:/state" not in text
    assert "staging_path" not in text
    first = service.get_recovery(limit=2)
    assert isinstance(first, ApiSuccess)
    assert first.value.available is True
    assert len(first.value.actions.items) == 2
    assert first.value.actions.next_cursor is not None
    assert "database_path" not in json.dumps(first.to_dict(), sort_keys=True)
    assert "workspace_path" not in json.dumps(first.to_dict(), sort_keys=True)
    second = service.get_recovery(limit=2, cursor=first.value.actions.next_cursor)
    assert isinstance(second, ApiSuccess)
    assert [item.sequence for item in second.value.actions.items] == [3]


def test_knowledge_and_statistics_use_separate_public_dtos(tmp_path: Path) -> None:
    # Adapt existing bounded stores without exposing their page, row or summary implementations.
    service = _service(tmp_path)
    knowledge = service.get_knowledge("campaign-1", limit=2)
    assert isinstance(knowledge, ApiSuccess)
    assert knowledge.value.executions == 2
    assert knowledge.value.records.items[0].event_id == "knowledge-event-1"
    assert "payload" not in knowledge.value.records.items[0].to_dict()
    statistics = service.list_statistics("campaign", limit=2)
    assert isinstance(statistics, ApiSuccess)
    assert statistics.value.items[0].entity_id == "campaign-1"
    exact = service.get_statistics("campaign", "campaign-1")
    assert isinstance(exact, ApiSuccess)
    assert exact.value.duration_p95_ms == 180.0


def test_invalid_pagination_and_missing_campaign_return_typed_rejections(tmp_path: Path) -> None:
    # Convert invalid limit, cross-resource cursor and missing identity into stable rejections.
    service = _service(tmp_path)
    invalid_limit = service.list_campaigns(limit=0)
    assert isinstance(invalid_limit, ApiRejected)
    assert invalid_limit.error.code == "invalid_limit"
    campaign_page = service.list_campaigns(limit=1)
    assert isinstance(campaign_page, ApiSuccess)
    wrong_cursor = service.list_workers("campaign-1", limit=1, cursor=campaign_page.value.next_cursor)
    assert isinstance(wrong_cursor, ApiRejected)
    assert wrong_cursor.error.code == "invalid_cursor"
    missing = service.get_campaign("campaign-missing", related_limit=2)
    assert isinstance(missing, ApiRejected)
    assert missing.error.code == "not_found"
    assert missing.error.details == {"campaign_id": "campaign-missing"}


def test_plan_and_relation_pages_resume_without_duplicates(tmp_path: Path) -> None:
    # Traverse plan, worker, shard and execution pages through their opaque keyset cursors.
    service = _service(tmp_path)
    plans = service.list_plans(limit=1)
    assert isinstance(plans, ApiSuccess)
    next_plans = service.list_plans(limit=1, cursor=plans.value.next_cursor)
    assert isinstance(next_plans, ApiSuccess)
    assert {plans.value.items[0].plan_id, next_plans.value.items[0].plan_id} == {"plan-1", "plan-2"}
    for method in (service.list_workers, service.list_shards, service.list_executions):
        first = method("campaign-1", limit=2)
        assert isinstance(first, ApiSuccess)
        second = method("campaign-1", limit=2, cursor=first.value.next_cursor)
        assert isinstance(second, ApiSuccess)
        first_ids = [item.to_dict() for item in first.value.items]
        second_ids = [item.to_dict() for item in second.value.items]
        assert len(first_ids) == 2
        assert len(second_ids) == 1
        assert first_ids[0] not in second_ids
