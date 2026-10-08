import sys
from pathlib import Path
from test_intelligence_unified_v1.mutations import generate_mutants
from test_intelligence_unified_v1.runner import MutationConfig
from test_intelligence_unified_v1.workers import run_parallel_campaign, shard_mutants
def test_shard_mutants_is_deterministic_and_balanced() -> None:
    # Assign expensive mutants first and keep stable IDs inside each shard.
    source = "def choose(value):\n    if value > 0:\n        return value\n    if value == 1:\n        return 1\n    return 0\n"
    mutants = generate_mutants(source, operators=("condition_to_not",))
    shards = shard_mutants(mutants, 2)
    assert [item.worker_id for item in shards] == ["worker-000", "worker-001"]
    assert sorted(item.mutant_ids for item in shards) == sorted(
        tuple(sorted(item.mutant_id for item in bucket)) for bucket in (mutants[:1], mutants[1:])
    )
def test_parallel_campaign_keeps_original_checkout_untouched(tmp_path: Path) -> None:
    # Keep legacy isolation coverage on the explicit compatibility API instead of the serial production runner.
    source = tmp_path / "app.py"
    source_text = (
        "def choose(value):\n"
        "    if value > 0:\n"
        "        result = 1\n"
        "    else:\n"
        "        result = 0\n"
        "    if value == 1:\n"
        "        return result\n"
        "    return 0\n"
    )
    source.write_text(source_text, encoding="utf-8")
    report = run_parallel_campaign(
        MutationConfig(
            project_root=tmp_path,
            source="app.py",
            function="choose",
            test_command_argv=(sys.executable, "-c", "from app import choose; assert choose(1) == 1"),
            operators=("condition_to_not",),
            max_mutants=2,
            no_escalation=True,
            use_baseline_cache=False,
            reports_dir=tmp_path / "reports",
            workers=2,
        )
    )
    assert report["status"] == "complete"
    assert [item["status"] for item in report["results"]] == ["killed", "killed"]
    assert [item["mutant"]["mutant_id"] for item in report["results"]] == sorted(
        item["mutant"]["mutant_id"] for item in report["results"]
    )
    assert len(report["workers"]) == 2
    assert source.read_text(encoding="utf-8") == source_text
    assert not list(tmp_path.glob("*.test_intelligence.lock"))
