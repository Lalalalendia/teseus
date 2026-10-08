from __future__ import annotations
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import theseus_performance.project_benchmark as benchmark_module
from theseus_local.workspace import WorkspaceProvider
from theseus_performance.project_benchmark import ProjectBenchmarkRequest, fingerprint_project, run_project_benchmark


class RetentionFakeCoordinator:
    """Benchmark executor that materializes representative disposable campaign and worker state."""

    fail_warm = False
    warm_observed_cold_workspace = False

    def run(self, configuration):
        # Materialize benchmark-owned state and compact report evidence without launching real workers.
        reports_root = Path(configuration.reports_dir)
        state_root = reports_root.parent / "state"
        campaign_id = configuration.campaign_id.value
        campaign_workspace = state_root / "workspaces" / campaign_id
        campaign_workspace.mkdir(parents=True, exist_ok=True)
        (campaign_workspace / "project.bin").write_bytes(b"x" * 1024)
        (campaign_workspace.parent / f"{campaign_id}.ownership.json").write_text("{}\n", encoding="utf-8")

        workers_root = state_root / "workers" / campaign_id
        workers_root.mkdir(parents=True, exist_ok=True)
        (workers_root / "worker-registry.json").write_text("{}\n", encoding="utf-8")
        for ordinal in range(int(configuration.budget.max_workers)):
            worker_root = workers_root / f"local-worker-{ordinal:03d}"
            spool_root = worker_root / "spool"
            spool_root.mkdir(parents=True, exist_ok=True)
            (spool_root / "delivery.json").write_text("{}\n", encoding="utf-8")
            attempts = 2 if ordinal == 0 else 1
            for attempt in range(attempts):
                attempt_root = worker_root / f"attempt-{attempt}-worker-instance-fixture-{ordinal}"
                workspace = attempt_root / "workspace"
                workspace.mkdir(parents=True, exist_ok=True)
                (workspace / "project.bin").write_bytes(b"x" * 1024)
                (attempt_root / "workspace.ownership.json").write_text("{}\n", encoding="utf-8")
                diagnostic = attempt_root / "reports" / "diagnostic.json"
                diagnostic.parent.mkdir(parents=True, exist_ok=True)
                diagnostic.write_text("{}\n", encoding="utf-8")

        if campaign_id.endswith("-warm"):
            cold_campaign_id = campaign_id.removesuffix("-warm") + "-cold"
            cold_workspace = state_root / "workspaces" / cold_campaign_id
            type(self).warm_observed_cold_workspace = cold_workspace.is_dir()

        report_root = reports_root / campaign_id
        report_root.mkdir(parents=True, exist_ok=True)
        raw_path = report_root / "raw-engine.report.json"
        raw_path.write_text(
            json.dumps(
                {
                    "status": "complete",
                    "metrics": {
                        "performance": {
                            "campaign_wall_seconds": 1.0,
                            "index_seconds": 0.1,
                            "selection_seconds": 0.1,
                            "mutant_generation_seconds": 0.1,
                            "snapshot_seconds": 0.1,
                            "pytest_seconds": 0.5,
                            "test_stats_ingestion_seconds": 0.05,
                            "report_materialization_seconds": 0.05,
                            "processes_started": int(configuration.budget.max_workers) + 1,
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        selected = int(configuration.budget.max_mutants or 2)
        shard_count = min(int(configuration.budget.max_workers), selected)
        (report_root / "campaign.plan.json").write_text(
            json.dumps(
                {
                    "candidate_count": selected,
                    "eligible_count": selected,
                    "selected_count": selected,
                    "shards": [
                        {
                            "shard_id": f"shard-{index:03d}",
                            "mutant_ids": [f"m{index:03d}"],
                            "estimated_cost": 1.0,
                        }
                        for index in range(shard_count)
                    ],
                }
            ),
            encoding="utf-8",
        )
        canonical_path = report_root / "canonical.report.json"
        canonical_path.write_text("{}\n", encoding="utf-8")
        failed = self.fail_warm and campaign_id.endswith("-warm")
        campaign = SimpleNamespace(
            campaign_id=configuration.campaign_id,
            status=SimpleNamespace(value="failed" if failed else "completed"),
            total_mutants=selected,
            completed_mutants=0 if failed else selected,
        )
        return SimpleNamespace(
            campaign=campaign,
            engine_result=SimpleNamespace(
                report={"raw_engine_report_path": str(raw_path)},
                summary=SimpleNamespace(
                    report_path=str(canonical_path),
                    total_mutants=selected,
                    completed_mutants=0 if failed else selected,
                ),
            ),
            database_path=report_root / "campaign.sqlite3",
            succeeded=not failed,
            error="fixture failure" if failed else None,
        )


def _request(tmp_path: Path) -> ProjectBenchmarkRequest:
    # Build one bounded cold/warm lane for benchmark state-retention tests.
    project = tmp_path / "project"
    project.mkdir()
    (project / "app.py").write_text("def choose(value):\n    return value\n", encoding="utf-8")
    return ProjectBenchmarkRequest(
        project_root=project,
        source_path="app.py",
        test_command=(sys.executable, "-m", "pytest", "-q"),
        output_root=tmp_path / "benchmarks",
        max_mutants=2,
        mutant_counts=(2,),
        worker_counts=(2,),
    )


def _lane_state(report: dict[str, object]) -> Path:
    # Resolve the single fixture lane state directory from the benchmark report path.
    return Path(str(report["report_path"])).parent / "mutants-2" / "workers-2" / "state"


def test_successful_lane_reclaims_campaign_and_all_attempt_workspaces(tmp_path: Path) -> None:
    # Remove every heavy copied project tree after cold and warm evidence are durably persisted.
    RetentionFakeCoordinator.fail_warm = False
    RetentionFakeCoordinator.warm_observed_cold_workspace = False
    report = run_project_benchmark(_request(tmp_path), coordinator_factory=RetentionFakeCoordinator)
    assert report["completed"] is True
    assert RetentionFakeCoordinator.warm_observed_cold_workspace is True

    state_root = _lane_state(report)
    for scenario in report["scenarios"]:
        campaign_id = str(scenario["metadata"]["campaign_id"])
        assert not (state_root / "workspaces" / campaign_id).exists()
        assert not (state_root / "workspaces" / f"{campaign_id}.ownership.json").exists()
        workers_root = state_root / "workers" / campaign_id
        assert (workers_root / "worker-registry.json").is_file()
        for worker_root in sorted(path for path in workers_root.iterdir() if path.name.startswith("local-worker-")):
            assert (worker_root / "spool" / "delivery.json").is_file()
            for attempt_root in sorted(path for path in worker_root.iterdir() if path.name.startswith("attempt-")):
                assert not (attempt_root / "workspace").exists()
                assert not (attempt_root / "workspace.ownership.json").exists()
                assert (attempt_root / "reports" / "diagnostic.json").is_file()


def test_failed_warm_lane_preserves_cold_and_warm_state_for_diagnostics(tmp_path: Path) -> None:
    # Preserve disposable copies when the cold/warm lane is not fully successful.
    RetentionFakeCoordinator.fail_warm = True
    RetentionFakeCoordinator.warm_observed_cold_workspace = False
    report = run_project_benchmark(_request(tmp_path), coordinator_factory=RetentionFakeCoordinator)
    assert report["completed"] is False
    assert RetentionFakeCoordinator.warm_observed_cold_workspace is True

    state_root = _lane_state(report)
    for scenario in report["scenarios"]:
        campaign_id = str(scenario["metadata"]["campaign_id"])
        assert (state_root / "workspaces" / campaign_id / "project.bin").is_file()
        workers_root = state_root / "workers" / campaign_id
        assert any(
            (attempt_root / "workspace" / "project.bin").is_file()
            for worker_root in workers_root.iterdir()
            if worker_root.name.startswith("local-worker-")
            for attempt_root in worker_root.iterdir()
            if attempt_root.name.startswith("attempt-")
        )


def test_lane_cleanup_runs_only_after_both_performance_json_files_exist(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Prove cleanup happens outside measured scenarios and only after cold and warm evidence files are written.
    RetentionFakeCoordinator.fail_warm = False
    observed: list[tuple[bool, bool]] = []
    original_cleanup = benchmark_module._cleanup_benchmark_lane_state

    def checked_cleanup(configurations):
        # Record evidence publication state immediately before delegating to the real cleanup.
        reports_root = Path(configurations[0].reports_dir)
        lane_root = reports_root.parent
        observed.append(
            (
                (lane_root / "cold.performance.json").is_file(),
                (lane_root / "warm.performance.json").is_file(),
            )
        )
        original_cleanup(configurations)

    monkeypatch.setattr(benchmark_module, "_cleanup_benchmark_lane_state", checked_cleanup)
    run_project_benchmark(_request(tmp_path), coordinator_factory=RetentionFakeCoordinator)
    assert observed == [(True, True)]


def test_benchmark_identity_and_workspace_copy_share_generated_path_policy(tmp_path: Path) -> None:
    # Keep benchmark identity and physical materialization aligned for generated caches and legitimate hidden inputs.
    project = tmp_path / "project"
    project.mkdir()
    (project / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
    (project / "loose.pyc").write_bytes(b"generated")
    ignored = (
        project / ".ruff_cache" / "cache.bin",
        project / ".mypy_cache" / "cache.bin",
        project / "node_modules" / "package.bin",
        project / "htmlcov" / "index.html",
    )
    for path in ignored:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"ignored")
    hidden_input = project / ".project-config" / "settings.toml"
    hidden_input.parent.mkdir()
    hidden_input.write_text("enabled = true\n", encoding="utf-8")
    target_input = project / "target" / "runtime.dat"
    target_input.parent.mkdir()
    target_input.write_bytes(b"runtime")

    first = fingerprint_project(project)
    destination = tmp_path / "workspace"
    WorkspaceProvider._copy_tree(project, destination)

    assert not (destination / "loose.pyc").exists()
    for path in ignored:
        assert not (destination / path.relative_to(project)).exists()
    assert (destination / hidden_input.relative_to(project)).is_file()
    assert (destination / target_input.relative_to(project)).is_file()

    for path in ignored:
        path.write_bytes(b"changed generated bytes")
    assert fingerprint_project(project).sha256 == first.sha256

    hidden_input.write_text("enabled = false\n", encoding="utf-8")
    assert fingerprint_project(project).sha256 != first.sha256
