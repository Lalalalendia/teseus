import asyncio
import sys
from types import SimpleNamespace
from pathlib import Path

from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    DiscoverMutantsRequest,
    EngineEventType,
    ExecuteShardRequest,
    FinalizeCampaignRequest,
    MemoryEventSink,
    MutationScope,
    PrepareCampaignRequest,
    ProjectDescriptor,
    ProjectId,
    ShardDescriptor,
    ShardId,
    TestCommandDescriptor as CommandDescriptor,
)
from test_intelligence_unified_v1.models import PerformanceMetrics
from test_intelligence_unified_v1.mutation_discovery_service import MutationDiscoveryService
from test_intelligence_unified_v1.engine import RunnerMutationEngine


def test_direct_test_command_uses_static_index_collection_contract() -> None:
    # A self-contained wheel may execute a direct oracle without carrying pytest.
    command = CommandDescriptor((sys.executable, "-B", "-c", "assert True"))
    context = SimpleNamespace(configuration=SimpleNamespace(project=SimpleNamespace(test_command=command)))
    engine = RunnerMutationEngine()

    assert engine._collection_command(context) == command.argv
    assert engine._indexed_nodeids(
        {
            "tests": [
                {"nodeid": "tests/test_app.py::test_two"},
                {"nodeid": "tests/test_app.py::test_one"},
                {"nodeid": "tests/test_app.py::test_one"},
            ]
        }
    ) == ("tests/test_app.py::test_one", "tests/test_app.py::test_two")


def test_engine_facade_runs_golden_single_mutant_and_emits_events(tmp_path: Path) -> None:
    # Keep the first public facade contract semantically aligned with the existing runner.
    source = tmp_path / "app.py"
    source_text = (
        "def choose(value):\n"
        "    if value > 0:\n"
        "        return 1\n"
        "    return 0\n"
    )
    source.write_text(source_text, encoding="utf-8")
    configuration = CampaignConfiguration(
        campaign_id=CampaignId("cmp_facade"),
        project=ProjectDescriptor(
            project_id=ProjectId("project_facade"),
            display_name="facade fixture",
            root_path=str(tmp_path),
            test_command=CommandDescriptor(
                (sys.executable, "-c", "from app import choose; assert choose(1) == 1")
            ),
        ),
        scope=MutationScope(source_path="app.py", function="choose", operators=("condition_to_not",)),
        budget=CampaignBudget(max_mutants=1, max_workers=1),
        no_escalation=True,
        reports_dir=str(tmp_path / "reports"),
    )
    sink = MemoryEventSink()
    result = asyncio.run(RunnerMutationEngine(sink).run_async(configuration))
    event_names = [event.event_type for event in sink.events]
    assert result.summary.status == "complete"
    assert result.summary.total_mutants == 1
    assert result.summary.completed_mutants == 1
    assert EngineEventType.CAMPAIGN_PREPARATION_STARTED.value in event_names
    assert EngineEventType.MUTANTS_DISCOVERED.value in event_names
    assert EngineEventType.MUTANT_COMPLETED.value in event_names
    assert EngineEventType.REPORT_MATERIALIZED.value in event_names
    assert source.read_text(encoding="utf-8") == source_text


def test_engine_facade_explicit_stages_reuse_preparation(tmp_path: Path) -> None:
    # Keep prepare, discover and execute requests independently addressable for future Gallifrey effects.
    source = tmp_path / "app.py"
    source.write_text("def choose(value):\n    if value > 0:\n        return 1\n    return 0\n", encoding="utf-8")
    configuration = CampaignConfiguration(
        campaign_id=CampaignId("cmp_stages"),
        project=ProjectDescriptor(
            project_id=ProjectId("project_stages"),
            display_name="stages fixture",
            root_path=str(tmp_path),
            test_command=CommandDescriptor(
                (sys.executable, "-c", "from app import choose; assert choose(1) == 1")
            ),
        ),
        scope=MutationScope(source_path="app.py", function="choose", operators=("condition_to_not",)),
        budget=CampaignBudget(max_mutants=1, max_workers=1),
        no_escalation=True,
        reports_dir=str(tmp_path / "reports"),
    )
    sink = MemoryEventSink()
    engine = RunnerMutationEngine(sink)

    async def execute() -> tuple[int, str, str]:
        # Run the public lifecycle one stage at a time without importing runner internals.
        prepared = await engine.prepare_campaign_async(PrepareCampaignRequest(configuration))
        discovered = await engine.discover_mutants_async(DiscoverMutantsRequest(configuration.campaign_id))
        shard = ShardDescriptor(
            shard_id=ShardId("shard_stages"),
            mutant_ids=tuple(item.mutant_id.value for item in prepared.mutants),
        )
        shard_result = await engine.execute_shard_async(ExecuteShardRequest(configuration.campaign_id, shard))
        final = await engine.finalize_campaign_async(FinalizeCampaignRequest(configuration.campaign_id))
        return discovered.total_mutants + len(prepared.mutants), str(shard_result.status), final.summary.status

    discovered_count, shard_status, final_status = asyncio.run(execute())
    assert discovered_count == 2
    assert shard_status == "complete"
    assert final_status == "complete"


def test_mutation_discovery_service_preserves_deterministic_limit() -> None:
    # Keep discovery policy independently callable while preserving the existing mutant ordering contract.
    metrics = PerformanceMetrics()
    mutants = MutationDiscoveryService(metrics).discover(
        "def choose(value):\n    if value > 0:\n        return 1\n    return 0\n",
        function_range=(1, 4),
        max_mutants=1,
        operators=("condition_to_not",),
    )
    assert len(mutants) == 1
    assert metrics.mutant_generation_seconds >= 0.0
