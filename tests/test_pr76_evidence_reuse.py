from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    MutationScope,
    ProjectDescriptor,
    ProjectId,
    TestCommandDescriptor,
)
from theseus_knowledge import (
    KnowledgeConflict,
    KnowledgePlaneStore,
    ReuseKind,
    fingerprint_evidence_identity,
)
from theseus_local import LocalCampaignCoordinator
from theseus_local.workspace import WorkspaceProvider


def _fingerprints() -> dict[str, str]:
    # Keep the fixture proof boundary explicit so each PR76 invalidation dimension is testable.
    return {
        "function": "function-v1",
        "mutant": "mutant-v1",
        "test_one": "test-one-v1",
        "test_two": "test-two-v1",
        "test_set": "tests-v1",
        "conftest": "conftest-v1",
        "environment": "environment-v1",
        "selection": "selection-v1",
        "result": "evidence-v1",
    }


def _execution(
    fingerprints: dict[str, str],
    *,
    execution_id: str = "exec-1",
    mutant_id: str = "m1",
    status: str = "killed",
    duplicate_test: bool = False,
) -> dict[str, object]:
    observations: list[dict[str, object]] = [
        {
            "test_id": "test_one",
            "test_fingerprint": fingerprints["test_one"],
            "outcome": "failed" if status == "killed" else "passed",
            "evidence_kind": "pytest_test_event",
            "observation_schema_version": 1,
        },
        {
            "test_id": "test_two",
            "test_fingerprint": fingerprints["test_two"],
            "outcome": "passed",
            "evidence_kind": "pytest_test_event",
            "observation_schema_version": 1,
        },
    ]
    if duplicate_test:
        observations.append(dict(observations[0]))
    return {
        "execution_id": execution_id,
        "mutant_id": mutant_id,
        "attempt": 0,
        "semantic_result": status,
        "status": "complete",
        "restore_verified": True,
        "evidence_schema_version": 2,
        "source_kind": "observed",
        "evidence_origin": "pytest_test_stats",
        "function_id": "module.calculate",
        "function_fingerprint": fingerprints["function"],
        "mutant_fingerprint": fingerprints["mutant"],
        "test_fingerprint": fingerprints["test_set"],
        "conftest_fingerprint": fingerprints["conftest"],
        "environment_fingerprint": fingerprints["environment"],
        "selection_fingerprint": fingerprints["selection"],
        "result_fingerprint": fingerprints["result"],
        "test_observations": observations,
    }


def _request(
    fingerprints: dict[str, str],
    *,
    mutant_id: str = "m1",
    mutant_fingerprint: str | None = None,
    test_fingerprints: dict[str, str] | None = None,
    test_fingerprint: str | None = None,
    result_fingerprint: str | None = None,
) -> dict[str, object]:
    # Build one planner request with optional aggregate changes for partial reuse.
    return {
        "mutant_id": mutant_id,
        "function_id": "module.calculate",
        "function_fingerprint": fingerprints["function"],
        "mutant_fingerprint": mutant_fingerprint or fingerprints["mutant"],
        "test_fingerprint": test_fingerprint if test_fingerprint is not None else fingerprints["test_set"],
        "test_fingerprints": test_fingerprints
        if test_fingerprints is not None
        else {"test_one": fingerprints["test_one"], "test_two": fingerprints["test_two"]},
        "conftest_fingerprint": fingerprints["conftest"],
        "environment_fingerprint": fingerprints["environment"],
        "selection_fingerprint": fingerprints["selection"],
        "result_fingerprint": result_fingerprint if result_fingerprint is not None else fingerprints["result"],
        "estimated_execution_seconds": 2.0,
    }


def test_pr76_evidence_identity_binds_every_reuse_boundary() -> None:
    # Reordering nodes is harmless, but changing any semantic proof input changes the key.
    base = {
        "mutation_fingerprint": "mutant-v1",
        "test_fingerprint": "tests-v1",
        "environment_fingerprint": "env-v1",
        "function_fingerprint": "function-v1",
        "conftest_fingerprint": "conftest-v1",
        "test_fingerprints": {"test_a": "a-v1", "test_b": "b-v1"},
        "configuration_fingerprint": "config-v1",
    }
    identity = fingerprint_evidence_identity(**base)
    assert identity == fingerprint_evidence_identity(**{**base, "test_fingerprints": {"test_b": "b-v1", "test_a": "a-v1"}})
    for field, value in (
        ("mutation_fingerprint", "mutant-v2"),
        ("test_fingerprints", {"test_a": "a-v2", "test_b": "b-v1"}),
        ("environment_fingerprint", "env-v2"),
        ("function_fingerprint", "function-v2"),
        ("conftest_fingerprint", "conftest-v2"),
        ("configuration_fingerprint", "config-v2"),
    ):
        assert fingerprint_evidence_identity(**{**base, field: value}) != identity


def test_pr76_killed_partial_reuse_is_allowed_but_survived_is_strict(tmp_path: Path) -> None:
    # Killed evidence can safely retain one unchanged killer; survived evidence must replay.
    fingerprints = _fingerprints()
    killed_store = KnowledgePlaneStore(tmp_path / "killed.sqlite3")
    killed_store.ingest_effect(
        effect_id="effect-killed",
        campaign_id="campaign-killed",
        effect_type="mutation.execute_shard",
        payload={"executions": [_execution(fingerprints)]},
    )
    partial_request = _request(
        fingerprints,
        test_fingerprints={"test_one": fingerprints["test_one"], "test_two": "test-two-v2"},
        test_fingerprint="tests-v2",
        result_fingerprint="evidence-v2",
    )
    partial_request.pop("estimated_execution_seconds")
    killed = killed_store.decide_reuse(**partial_request)
    assert killed.kind is ReuseKind.PARTIAL
    assert killed.eligible is True
    killed_store.close()

    survived_store = KnowledgePlaneStore(tmp_path / "survived.sqlite3")
    survived_store.ingest_effect(
        effect_id="effect-survived",
        campaign_id="campaign-survived",
        effect_type="mutation.execute_shard",
        payload={"executions": [_execution(fingerprints, status="survived")]},
    )
    survived = survived_store.decide_reuse(**partial_request)
    assert survived.kind is ReuseKind.HISTORICAL_HINT
    assert survived.eligible is False
    survived_store.close()


def test_pr76_duplicate_and_corrupt_evidence_fail_closed(tmp_path: Path) -> None:
    # Duplicate effect delivery is idempotent, conflicting identity is rejected, and duplicate test nodes are corrupt.
    fingerprints = _fingerprints()
    store = KnowledgePlaneStore(tmp_path / "integrity.sqlite3")
    payload = {"executions": [_execution(fingerprints)]}
    first = store.ingest_effect(
        effect_id="effect-duplicate",
        campaign_id="campaign-duplicate",
        effect_type="mutation.execute_shard",
        payload=payload,
    )
    second = store.ingest_effect(
        effect_id="effect-duplicate",
        campaign_id="campaign-duplicate",
        effect_type="mutation.execute_shard",
        payload=payload,
    )
    assert first.duplicate is False
    assert second.duplicate is True
    with pytest.raises(KnowledgeConflict):
        store.ingest_effect(
            effect_id="effect-duplicate",
            campaign_id="campaign-duplicate",
            effect_type="mutation.execute_shard",
            payload={"executions": [_execution(fingerprints, status="survived")]},
        )
    with pytest.raises(ValueError, match="duplicated"):
        store.ingest_effect(
            effect_id="effect-corrupt",
            campaign_id="campaign-corrupt",
            effect_type="mutation.execute_shard",
            payload={"executions": [_execution(fingerprints, duplicate_test=True)]},
        )
    store.close()


def test_pr76_reuse_metrics_measure_hits_avoided_work_and_validation_cost(tmp_path: Path) -> None:
    # Metrics distinguish factual eligibility from policy-authorized avoided execution.
    fingerprints = _fingerprints()
    store = KnowledgePlaneStore(tmp_path / "metrics.sqlite3")
    store.ingest_effect(
        effect_id="effect-metrics",
        campaign_id="campaign-metrics",
        effect_type="mutation.execute_shard",
        payload={"executions": [_execution(fingerprints)]},
    )
    request_one = _request(fingerprints)
    request_two = _request(
        fingerprints,
        mutant_id="m2",
        mutant_fingerprint="mutant-v2",
        result_fingerprint="evidence-v2",
    )
    plan = store.build_reuse_plan([request_two, request_one], reuse_mode="partial")
    metrics = plan.metrics.to_dict()
    assert metrics["candidate_count"] == 2
    assert metrics["authorized_hits"] == 1
    assert metrics["reuse_hit_rate"] == 0.5
    assert metrics["executions_avoided"] == 1
    assert metrics["wall_saved_seconds"] == 2.0
    assert metrics["validation_cost_seconds"] >= 0.0
    assert plan.to_dict()["metrics"] == metrics
    store.close()


def _configuration(root: Path, campaign_id: str, reuse_mode: str) -> CampaignConfiguration:
    # Build a deterministic one-mutant oracle campaign for reuse-on/reuse-off comparison.
    return CampaignConfiguration(
        campaign_id=CampaignId(campaign_id),
        project=ProjectDescriptor(
            project_id=ProjectId("project-pr76-golden"),
            display_name="PR76 golden fixture",
            root_path=str(root),
            test_command=TestCommandDescriptor((sys.executable, "-m", "pytest", "-q", "test_app.py")),
        ),
        scope=MutationScope(source_path="app.py", function="choose", operators=("condition_to_not",)),
        budget=CampaignBudget(max_mutants=1, max_workers=1),
        no_escalation=True,
        reports_dir=str(root / "reports"),
        reuse_mode=reuse_mode,
    )


def test_pr76_golden_oracle_reuse_matches_reuse_off_semantics(tmp_path: Path) -> None:
    # The optimization may avoid execution, but authoritative semantic results must match a fresh oracle.
    (tmp_path / "app.py").write_text(
        "def choose(value):\n    if value > 0:\n        return 1\n    return 0\n",
        encoding="utf-8",
    )
    (tmp_path / "test_app.py").write_text(
        "from app import choose\n\n\ndef test_choose():\n    assert choose(1) == 1\n",
        encoding="utf-8",
    )
    coordinator = LocalCampaignCoordinator()
    seed_config = _configuration(tmp_path, "pr76-seed", "experimental")
    reuse_config = _configuration(tmp_path, "pr76-reuse-on", "experimental")
    off_config = _configuration(tmp_path, "pr76-reuse-off", "off")
    assert coordinator.run(seed_config).succeeded
    reused = coordinator.run(reuse_config)
    fresh = coordinator.run(off_config)
    assert reused.succeeded
    assert fresh.succeeded
    reused_report = json.loads(
        (WorkspaceProvider(reuse_config).reports_root / "pr76-reuse-on" / "canonical.report.json").read_text(encoding="utf-8")
    )
    fresh_report = json.loads(
        (WorkspaceProvider(off_config).reports_root / "pr76-reuse-off" / "canonical.report.json").read_text(encoding="utf-8")
    )
    assert [row["status"] for row in reused_report["results"]] == [row["status"] for row in fresh_report["results"]]
    reused_plan = json.loads(
        (WorkspaceProvider(reuse_config).reports_root / "pr76-reuse-on" / "reuse.plan.json").read_text(encoding="utf-8")
    )
    fresh_plan = json.loads(
        (WorkspaceProvider(off_config).reports_root / "pr76-reuse-off" / "reuse.plan.json").read_text(encoding="utf-8")
    )
    assert reused_plan["metrics"]["executions_avoided"] == 1
    assert reused_plan["metrics"]["reuse_hit_rate"] == 1.0
    assert fresh_plan["metrics"]["executions_avoided"] == 0
    assert fresh_plan["metrics"]["reuse_hit_rate"] == 0.0
