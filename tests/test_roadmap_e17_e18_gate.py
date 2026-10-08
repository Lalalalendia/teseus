from __future__ import annotations
from pathlib import Path
import pytest
pytestmark = pytest.mark.roadmap
def _production_text() -> str:
    # Read only production Python sources so roadmap checks cannot pass through test doubles.
    root = Path(__file__).parents[1]
    directories = (root / "theseus_local", root / "theseus_contracts", root / "gallifrey_mutation")
    return "\n".join(
        path.read_text(encoding="utf-8")
        for directory in directories
        for path in sorted(directory.glob("*.py"))
    )
def _source(name: str) -> str:
    # Load one production module by repository-relative name for ordering-sensitive gates.
    return (Path(__file__).parents[1] / name).read_text(encoding="utf-8")
def _campaign_plan_sources() -> tuple[str, ...]:
    # Locate the production module that owns the immutable campaign plan contract.
    root = Path(__file__).parents[1]
    sources: list[str] = []
    for path in sorted(root.rglob("*.py")):
        if "tests" in path.parts or not path.is_file():
            continue
        try:
            source = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            # Pytest fixtures can leave inaccessible directories named *.py;
            # roadmap source discovery must remain scoped to readable files.
            continue
        if "CampaignPlan" in source:
            sources.append(source)
    return tuple(sources)
def test_e17_campaign_plan_is_a_first_class_immutable_artifact() -> None:
    # Require a named campaign plan boundary instead of treating a mutable reuse projection as the plan.
    source = _production_text()
    assert "CampaignPlan" in source
    assert "campaign.plan.json" in source
    assert "planning_fingerprint" in source
    assert "content_hash" in source or "plan_content_hash" in source
def test_e17_plan_integrity_and_deterministic_identity_are_explicit() -> None:
    # Require tamper detection, path-independent identity and deterministic plan serialization in production code.
    sources = _campaign_plan_sources()
    assert sources
    source = "\n".join(sources)
    assert "verify" in source.lower()
    assert "plan_id" in source
    assert "reports_dir" not in source.split("CampaignPlan", 1)[-1].split("class ", 1)[0]
    assert "created_at" not in source.split("CampaignPlan", 1)[-1].split("class ", 1)[0]
def test_e17_scope_and_empty_plan_contracts_fail_closed() -> None:
    # Require explicit unknown-ID rejection and no synthetic empty shard for an empty selection.
    sources = _campaign_plan_sources()
    assert sources
    source = "\n".join(sources).lower()
    assert "explicit" in source
    assert "unknown" in source and "mutant" in source
    assert "empty shard" in source or "empty_shard" in source
def test_e17_cost_model_distinguishes_wall_cpu_and_weighted_sharding() -> None:
    # Require shard planning to use measured cost dimensions instead of mutant cardinality alone.
    sources = _campaign_plan_sources()
    assert sources
    source = "\n".join(sources).lower()
    assert "cpu" in source
    assert "wall" in source or "duration" in source
    assert "weighted" in source or "estimated_cost" in source
    assert "max_cpu_seconds" in source
def test_e18_production_engine_has_one_runtime_without_legacy_workers_imports() -> None:
    # Prevent the old workers.py execution path from remaining reachable through production entry points.
    engine = _source("engine.py")
    runner = _source("runner.py")
    assert "from .workers" not in engine
    assert "run_parallel_campaign" not in runner
def test_e18_durable_spool_has_pending_replay_and_per_mutant_events() -> None:
    # Require crash recovery to discover and replay unacknowledged durable events at startup.
    source = _production_text()
    assert "DurableExecutionSpool" in source
    assert "pending(" in source
    assert "replay" in source.lower()
    assert "per-mutant" in source.lower() or "per_mutant" in source.lower()
def test_e18_worker_agent_is_persistent_and_can_claim_follow_on_work() -> None:
    # Require an OS-level worker lifecycle that survives a shard and can acquire the next shard.
    source = _production_text().lower()
    assert "workeragent" in source
    assert "acquire" in source
    assert "work stealing" in source or "work_stealing" in source or "next shard" in source
def test_e18_heartbeat_remains_active_until_after_fan_in_ack() -> None:
    # Serialize renewal shutdown with authoritative commit and keep ACK strictly after that commit.
    coordinator = _source("theseus_local/coordinator.py")
    barrier_position = coordinator.find("with worker.heartbeat_barrier()")
    record_position = coordinator.find("service.record_shard_result", barrier_position)
    clear_position = coordinator.find("worker.set_heartbeat_handler(None)", record_position)
    ack_position = coordinator.find("worker.acknowledge", clear_position)
    assert barrier_position >= 0
    assert record_position > barrier_position
    assert clear_position > record_position
    assert ack_position > clear_position
