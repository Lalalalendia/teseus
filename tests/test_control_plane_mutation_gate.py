from __future__ import annotations

import shutil
import sys
from pathlib import Path

from test_intelligence_unified_v1.runner import MutationConfig, MutationRunner


def test_control_plane_mutation_gate_kills_known_schema_hash_lease_identity_and_candidate_mutants(
    tmp_path: Path,
) -> None:
    # Run the real Theseus mutation engine against an isolated control-plane
    # contract fixture; the checkout itself is never modified by this gate.
    source = Path(__file__).parent / "fixtures" / "control_plane_mutation_lab.py"
    test_source = Path(__file__).parent / "fixtures" / "control_plane_mutation_lab_test.py"
    shutil.copy2(source, tmp_path / source.name)
    shutil.copy2(test_source, tmp_path / test_source.name)
    report = MutationRunner(
        MutationConfig(
            project_root=tmp_path,
            source=source.name,
            test_command_argv=(sys.executable, "-m", "pytest", "-q", test_source.name),
            operators=("eq_to_ne", "ne_to_eq", "le_to_lt", "lt_to_le", "condition_to_not"),
            no_escalation=True,
            use_baseline_cache=False,
            reports_dir=tmp_path / "reports",
        )
    ).run()
    assert report["status"] == "complete"
    assert report["metrics"]["total_mutants"] >= 5
    assert report["metrics"]["counts"].get("survived", 0) == 0
    assert report["metrics"]["counts"].get("infrastructure_error", 0) == 0
