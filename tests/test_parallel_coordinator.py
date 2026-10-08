import os
import sys
from pathlib import Path
from test_intelligence_unified_v1 import runner as runner_module
from test_intelligence_unified_v1.runner import LevelSpec, MutationConfig
from test_intelligence_unified_v1.workers import SharedBaselineProvider, run_parallel_campaign
def test_shared_baseline_provider_runs_each_level_once(tmp_path: Path) -> None:
    # Serialize concurrent level requests into one coordinator baseline execution.
    class FakeRunner:
        def __init__(self) -> None:
            # Keep the fake runner state minimal for shared-baseline serialization checks.
            self.reports_dir = tmp_path
            self.calls: list[str] = []
        def _run_baselines(self, levels, snapshot, selection, function_info, report_id):
            # Record one backend call per requested level while returning a reusable baseline row.
            self.calls.extend(level.name for level in levels)
            return [{"level": levels[0].name, "passed": True, "output_artifact": "baseline.txt"}]
    fake = FakeRunner()
    levels = (
        LevelSpec("L1", "selected", (sys.executable, "-c", "pass")),
        LevelSpec("L2", "domain", (sys.executable, "-c", "pass")),
    )
    provider = SharedBaselineProvider(fake, None, None, None, "run", levels)
    first = provider.get((levels[0],))
    second = provider.get((levels[0],))
    third = provider.get((levels[1],))
    fourth = provider.get((levels[1],))
    assert fake.calls == ["L1", "L2"]
    assert first[0]["shared_baseline"] is True
    assert second[0]["baseline_reused"] is True
    assert third[0]["level"] == "L2"
    assert fourth[0]["shared_baseline"] is True
def test_parallel_campaign_reuses_coordinator_index_and_l1_baseline(tmp_path: Path, monkeypatch) -> None:
    # Exercise the explicit legacy compatibility backend without reopening production runner routing.
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
    original_build_index = runner_module.build_index
    original_run_argv = runner_module.run_argv
    build_calls: list[tuple[object, ...]] = []
    command_calls: list[tuple[object, ...]] = []
    def counting_build_index(*args, **kwargs):
        # Record compatibility preparation while delegating to the actual index builder.
        build_calls.append(args)
        return original_build_index(*args, **kwargs)
    def counting_run_argv(*args, **kwargs):
        # Record coordinator baseline commands while delegating to the actual process runner.
        command_calls.append(args)
        return original_run_argv(*args, **kwargs)
    monkeypatch.setattr(runner_module, "build_index", counting_build_index)
    monkeypatch.setattr(runner_module, "run_argv", counting_run_argv)
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
    assert len(build_calls) == 1
    assert len(command_calls) == 1
    assert report["coordinator_preparation"]["index_snapshot"] is True
    assert report["coordinator_preparation"]["shared_baseline"] is True
    assert len(report["baseline"]) == 1
    assert report["baseline"][0]["passed"] is True
    assert all(item["workspace"].get("shared_baseline") is True for item in report["workers"])
    worker_pids = {int(item["workspace"]["pid"]) for item in report["workers"]}
    assert len(worker_pids) == 2
    assert all(pid != os.getpid() for pid in worker_pids)
    assert source.read_text(encoding="utf-8") == source_text
