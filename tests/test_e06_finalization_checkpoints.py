import json
import sys
from pathlib import Path

from test_intelligence_unified_v1 import runner as runner_module
from test_intelligence_unified_v1.mutations import create_snapshot, write_manifest
from test_intelligence_unified_v1.runner import MutationConfig, MutationRunner


def test_state_checkpoint_is_sparse_until_forced(tmp_path: Path, monkeypatch) -> None:
    # Keep the append-only result journal authoritative between bounded state checkpoints.
    runner = MutationRunner(MutationConfig(project_root=tmp_path, source="app.py"))
    runner._state_path = tmp_path / "run.state.json"
    runner._mutant_by_id = {"m1": {}, "m2": {}}
    writes: list[Path] = []
    original = runner_module.atomic_write_json

    def tracked(path, value, **kwargs):
        # Count state materializations without changing the atomic writer.
        if Path(path).resolve() == runner._state_path.resolve():
            writes.append(Path(path))
        return original(path, value, **kwargs)

    monkeypatch.setattr(runner_module, "atomic_write_json", tracked)
    for mutant_id in ("m1", "m2"):
        runner._record_campaign_result(
            {
                "status": "survived",
                "mutant": {"mutant_id": mutant_id, "mutation": "plus_to_minus"},
                "level_results": [],
                "selection": {"levels": []},
            }
        )
        runner._write_state("running", [])
    assert writes == []
    runner._write_state("complete", [], force=True)
    assert len(writes) == 1
    assert json.loads((tmp_path / "run.state.json").read_text(encoding="utf-8"))["status"] == "complete"


def test_manifest_terminal_status_follows_report_and_state(tmp_path: Path, monkeypatch) -> None:
    # Publish restored, ready_to_commit and terminal states only around completed artifacts.
    target = tmp_path / "app.py"
    target.write_text("value = 1\n", encoding="utf-8")
    reports = tmp_path / "reports"
    snapshot = create_snapshot(target, reports / "recovery")
    manifest_path = reports / "run.manifest.json"
    write_manifest(manifest_path, snapshot)
    report_path = reports / "run.json"
    runner = MutationRunner(MutationConfig(project_root=tmp_path, source="app.py", reports_dir=reports))
    runner._campaign_snapshot = snapshot
    runner._state_path = reports / "run.state.json"
    events: list[tuple[str, str]] = []
    original = runner_module.atomic_write_json

    def tracked(path, value, **kwargs):
        # Record manifest phases while allowing normal report/state writes.
        resolved = Path(path).resolve()
        if resolved == manifest_path.resolve():
            events.append(("manifest", str(value.get("status", ""))))
        elif resolved == report_path.resolve():
            events.append(("report", str(value.get("status", ""))))
        elif resolved == (reports / "run.state.json").resolve():
            events.append(("state", str(value.get("status", ""))))
        return original(path, value, **kwargs)

    monkeypatch.setattr(runner_module, "atomic_write_json", tracked)
    report = {"run_id": "run", "target": {"source_path": "app.py"}, "results": []}
    runner._finish_report(report_path, report, manifest_path, status="complete")
    manifest_events = [value for kind, value in events if kind == "manifest"]
    assert manifest_events == ["restored", "ready_to_commit", "complete"]
    assert events.index(("report", "complete")) < events.index(("manifest", "ready_to_commit"))
    assert events.index(("state", "complete")) < events.index(("manifest", "complete"))


def test_mutant_restore_is_normal_and_campaign_restore_is_critical(tmp_path: Path, monkeypatch) -> None:
    # Reserve critical durability for the final campaign restore while preserving per-mutant SHA checks.
    source = tmp_path / "app.py"
    source.write_text("def choose(value):\n    return value + 1\n", encoding="utf-8")
    restores: list[str] = []
    original = runner_module.restore_snapshot

    def tracked(snapshot, **kwargs):
        # Capture the durability boundary before delegating to the real safe restore.
        restores.append(str(kwargs.get("durability")))
        return original(snapshot, **kwargs)

    monkeypatch.setattr(runner_module, "restore_snapshot", tracked)
    report = MutationRunner(
        MutationConfig(
            project_root=tmp_path,
            source="app.py",
            function="choose",
            test_command_argv=(
                sys.executable,
                "-c",
                "from app import choose; assert choose(1) == 2",
            ),
            reports_dir=tmp_path / "reports",
            operators=("plus_to_minus",),
            max_mutants=1,
            no_escalation=True,
            use_baseline_cache=False,
        )
    ).run()
    assert report["status"] == "complete"
    assert "normal" in restores
    assert restores[-1] == "critical"
