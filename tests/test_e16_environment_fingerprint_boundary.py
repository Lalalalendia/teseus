from __future__ import annotations

from pathlib import Path

from test_intelligence_unified_v1.commands import env_fingerprint


def test_allowlisted_environment_ignores_undeclared_runtime_variables(
    tmp_path: Path,
) -> None:
    # Prove that coordinator-only transport variables cannot invalidate a worker snapshot.
    coordinator_environment = {
        "PYTHONPATH": "D:/teseus",
        "THESEUS_COORDINATOR_PIPE": "coordinator-pipe",
        "UNRELATED_VALUE": "first",
    }
    worker_environment = {
        "PYTHONPATH": "D:/teseus;D:/worker",
        "THESEUS_WORKER_INSTANCE": "worker-001",
        "UNRELATED_VALUE": "second",
    }

    coordinator_fingerprint = env_fingerprint(
        tmp_path,
        coordinator_environment,
        include_cwd=False,
        inherit_policy="allowlisted",
    )
    worker_fingerprint = env_fingerprint(
        tmp_path,
        worker_environment,
        include_cwd=False,
        inherit_policy="allowlisted",
    )

    assert coordinator_fingerprint == worker_fingerprint


def test_allowlisted_environment_tracks_declared_variable(
    tmp_path: Path,
) -> None:
    # Prove that a declared semantic input still invalidates the prepared snapshot.
    first = env_fingerprint(
        tmp_path,
        {
            "FEATURE_FLAG": "disabled",
            "THESEUS_WORKER_INSTANCE": "worker-a",
        },
        include_cwd=False,
        tracked_variables=("FEATURE_FLAG",),
        inherit_policy="allowlisted",
    )
    second = env_fingerprint(
        tmp_path,
        {
            "FEATURE_FLAG": "enabled",
            "THESEUS_WORKER_INSTANCE": "worker-b",
        },
        include_cwd=False,
        tracked_variables=("FEATURE_FLAG",),
        inherit_policy="allowlisted",
    )

    assert first != second


def test_allowlisted_environment_tracks_declared_pattern(
    tmp_path: Path,
) -> None:
    # Prove that explicit wildcard contracts retain their semantic invalidation behavior.
    first = env_fingerprint(
        tmp_path,
        {
            "APP_REGION": "eu",
            "APP_MODE": "test",
            "PYTHONPATH": "coordinator",
        },
        include_cwd=False,
        tracked_prefixes=("APP_*",),
        inherit_policy="allowlisted",
    )
    second = env_fingerprint(
        tmp_path,
        {
            "APP_REGION": "us",
            "APP_MODE": "test",
            "PYTHONPATH": "worker",
        },
        include_cwd=False,
        tracked_prefixes=("APP_*",),
        inherit_policy="allowlisted",
    )

    assert first != second


def test_track_all_except_detects_runtime_environment_difference(
    tmp_path: Path,
) -> None:
    # Keep the explicitly broad policy sensitive to all nonignored environment values.
    first = env_fingerprint(
        tmp_path,
        {
            "APPLICATION_MODE": "first",
            "IGNORED_VALUE": "coordinator",
        },
        include_cwd=False,
        ignored_variables=("IGNORED_VALUE",),
        inherit_policy="track_all_except",
    )
    second = env_fingerprint(
        tmp_path,
        {
            "APPLICATION_MODE": "second",
            "IGNORED_VALUE": "worker",
        },
        include_cwd=False,
        ignored_variables=("IGNORED_VALUE",),
        inherit_policy="track_all_except",
    )

    assert first != second


def test_secret_variable_is_tracked_without_exposing_plaintext(
    tmp_path: Path,
) -> None:
    # Track explicitly declared secrets through keyed digests rather than raw values.
    first = env_fingerprint(
        tmp_path,
        {"ACCESS_TOKEN": "secret-first"},
        include_cwd=False,
        secret_variables=("ACCESS_TOKEN",),
        inherit_policy="allowlisted",
        secret_key="project-key",
    )
    second = env_fingerprint(
        tmp_path,
        {"ACCESS_TOKEN": "secret-second"},
        include_cwd=False,
        secret_variables=("ACCESS_TOKEN",),
        inherit_policy="allowlisted",
        secret_key="project-key",
    )

    assert first != second
    assert "secret-first" not in first
    assert "secret-second" not in second


def test_ignored_variable_wins_over_explicit_tracking(
    tmp_path: Path,
) -> None:
    # Let an explicit ignore rule remove even a separately declared environment name.
    first = env_fingerprint(
        tmp_path,
        {"FEATURE_FLAG": "first"},
        include_cwd=False,
        tracked_variables=("FEATURE_FLAG",),
        ignored_variables=("FEATURE_FLAG",),
        inherit_policy="allowlisted",
    )
    second = env_fingerprint(
        tmp_path,
        {"FEATURE_FLAG": "second"},
        include_cwd=False,
        tracked_variables=("FEATURE_FLAG",),
        ignored_variables=("FEATURE_FLAG",),
        inherit_policy="allowlisted",
    )

    assert first == second