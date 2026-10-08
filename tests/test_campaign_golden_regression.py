import hashlib
import json
import sys
from pathlib import Path

from test_intelligence_unified_v1.runner import MutationConfig, MutationRunner


def _write_golden_project(root: Path) -> str:
    # Write one byte-identical LF source image on every operating system.
    source = (
        "def classify(value):\n"
        "    if value > 0:\n"
        "        return value + 1\n"
        "    return 0\n"
    )
    (root / "app.py").write_bytes(source.encode("utf-8"))
    return source


def _run_golden_campaign(root: Path, reports_dir: Path) -> dict[str, object]:
    # Run one direct argv campaign so the golden test is independent of optional pytest installs.
    return MutationRunner(
        MutationConfig(
            project_root=root,
            source="app.py",
            function="classify",
            test_command_argv=(
                sys.executable,
                "-B",
                "-c",
                "from app import classify; assert classify(1) == 2",
            ),
            operators=("condition_to_not",),
            max_mutants=1,
            no_escalation=True,
            use_baseline_cache=False,
            reports_dir=reports_dir,
        )
    ).run()


def test_golden_campaign_preserves_ids_status_score_and_recovery(tmp_path: Path) -> None:
    # Freeze semantic campaign output across future performance and maintenance changes.
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    first_root.mkdir()
    second_root.mkdir()
    source = _write_golden_project(first_root)
    _write_golden_project(second_root)
    first = _run_golden_campaign(first_root, first_root / "reports")
    second = _run_golden_campaign(second_root, second_root / "reports")
    expected_id = "m3:condition_to_not:L2:C8:ccaadac969af"
    expected_sha = hashlib.sha256(source.encode("utf-8")).hexdigest()

    for report in (first, second):
        assert report["status"] == "complete"
        assert [item["mutant_id"] for item in report["mutants"]] == [expected_id]
        assert [item["status"] for item in report["results"]] == ["killed"]
        result = report["results"][0]
        assert result["mutant"]["mutant_id"] == expected_id
        assert result["mutant"]["mutation"] == "condition_to_not"
        assert result["source_sha256_before"] == expected_sha
        assert result["restore_sha256"] == expected_sha
        assert result["restore_verified"] is True
        assert result["level_results"][0]["level"] == "L1"
        assert result["level_results"][0]["result"]["exit_code"] == 1
        assert result["selection"]["levels"][0]["source"] == "explicit-command"
        assert report["source_snapshot"]["original_sha256"] == expected_sha
        assert report["metrics"]["mutation_score"] == 1.0
        assert report["metrics"]["operator_stats"] == {
            "condition_to_not": {
                "operator_version": "m3",
                "total_mutants": 1,
                "counts": {"killed": 1},
                "mutation_score": 1.0,
            }
        }
        assert report["metrics"]["performance"]["source_mutation_writes"] == 2
        journal_rows = [
            json.loads(line)
            for line in Path(str(report["results_journal"])).read_text(encoding="utf-8").splitlines()
        ]
        assert [row["mutant"]["mutant_id"] for row in journal_rows] == [expected_id]
        assert [row["status"] for row in journal_rows] == ["killed"]

    assert [item["mutant_id"] for item in first["mutants"]] == [item["mutant_id"] for item in second["mutants"]]
    assert first["status"] == second["status"]
