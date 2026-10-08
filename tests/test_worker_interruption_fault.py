import sys
from pathlib import Path
from test_intelligence_unified_v1 import workers as workers_module
from test_intelligence_unified_v1.runner import MutationConfig
def test_worker_interruption_becomes_error_without_touching_checkout(tmp_path: Path, monkeypatch) -> None:
    # Verify the explicit legacy backend converts an interrupted worker into a deterministic campaign error.
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
    def interrupt_worker(*args, **kwargs):
        # Simulate a compatibility worker process disappearing after coordinator preparation.
        raise KeyboardInterrupt("injected worker interruption")
    monkeypatch.setattr(workers_module, "_run_worker", interrupt_worker)
    report = workers_module.run_parallel_campaign(
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
    assert report["status"] == "error"
    assert [item["status"] for item in report["workers"]] == ["error", "error"]
    assert "injected worker interruption" in report["workers"][0]["error"]
    assert source.read_text(encoding="utf-8") == source_text
    assert not list(tmp_path.glob("*.test_intelligence.lock"))
