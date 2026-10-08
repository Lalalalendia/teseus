from __future__ import annotations

from pathlib import Path

from test_intelligence_unified_v1.commands import env_fingerprint
from test_intelligence_unified_v1.test_stats import build_test_stats_env
from theseus_contracts import EnvironmentDescriptor
from theseus_local import LocalCampaignCoordinator


def test_tracked_environment_value_changes_fingerprint_without_plaintext(tmp_path: Path, monkeypatch) -> None:
    # Distinguish absent, empty and valued tracked variables while keeping the value out of the digest payload.
    monkeypatch.delenv("THESEUS_FEATURE", raising=False)
    absent = env_fingerprint(
        tmp_path,
        include_cwd=False,
        tracked_variables=("THESEUS_FEATURE",),
        inherit_policy="allowlisted",
        secret_key="project-test",
    )
    monkeypatch.setenv("THESEUS_FEATURE", "")
    empty = env_fingerprint(
        tmp_path,
        include_cwd=False,
        tracked_variables=("THESEUS_FEATURE",),
        inherit_policy="allowlisted",
        secret_key="project-test",
    )
    monkeypatch.setenv("THESEUS_FEATURE", "enabled")
    valued = env_fingerprint(
        tmp_path,
        include_cwd=False,
        tracked_variables=("THESEUS_FEATURE",),
        inherit_policy="allowlisted",
        secret_key="project-test",
    )
    assert len({absent, empty, valued}) == 3
    assert "enabled" not in valued


def test_undeclared_environment_read_blocks_exact_fingerprint(tmp_path: Path) -> None:
    # Require an explicit allowlisted contract for every environment name used by a test.
    source = "import os\n\ndef test_feature():\n    assert os.getenv('FEATURE_FLAG') is not None\n"
    descriptor = EnvironmentDescriptor(
        fingerprint="env",
        python_version="3",
        tracked_variables=("OTHER_FLAG",),
        inherit_policy="allowlisted",
    )
    (tmp_path / "test_feature.py").write_text(source, encoding="utf-8")
    fingerprints, blockers = LocalCampaignCoordinator._test_fingerprint_bundle(
        tmp_path,
        None,
        ("test_feature.py::test_feature",),
        environment_descriptor=descriptor,
    )
    assert fingerprints
    assert any("undeclared-environment-read:FEATURE_FLAG" in item for item in blockers)


def test_declared_environment_read_is_reusable_and_workspace_independent(tmp_path: Path) -> None:
    # A declared read is complete and the semantic fingerprint does not contain an absolute workspace path.
    (tmp_path / "test_feature.py").write_text(
        "import os\n\ndef test_feature():\n    assert os.getenv('FEATURE_FLAG', 'off') in {'on', 'off'}\n",
        encoding="utf-8",
    )
    descriptor = EnvironmentDescriptor(
        fingerprint="env",
        python_version="3",
        tracked_variables=("FEATURE_FLAG",),
        inherit_policy="allowlisted",
    )
    first, first_blockers = LocalCampaignCoordinator._test_fingerprint_bundle(
        tmp_path,
        None,
        ("test_feature.py::test_feature",),
        environment_descriptor=descriptor,
    )
    second_root = tmp_path / "copy"
    second_root.mkdir()
    (second_root / "test_feature.py").write_text((tmp_path / "test_feature.py").read_text(), encoding="utf-8")
    second, second_blockers = LocalCampaignCoordinator._test_fingerprint_bundle(
        second_root,
        None,
        ("test_feature.py::test_feature",),
        environment_descriptor=descriptor,
    )
    assert not first_blockers
    assert not second_blockers
    assert first == second


def test_internal_pytest_profile_disables_global_plugin_autoload(tmp_path: Path) -> None:
    # Keep synthetic engine fixtures isolated while leaving user projects on the explicit default profile.
    environment = build_test_stats_env(
        tmp_path / "events",
        run_id="run",
        phase="mutant",
        level="L1",
        mutant_id="m1",
        source_path="app.py",
        target_sha256="sha",
        pytest_plugin_autoload=False,
    )
    assert environment["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] == "1"
