from pathlib import PurePath

from theseus_contracts.project_tree import project_tree_path_is_excluded


def test_generated_pytest_directories_are_excluded_from_materialization() -> None:
    # Benchmark and audit fixtures must not become project inputs or worker-copy payload.
    assert project_tree_path_is_excluded(PurePath(".pytest-tmp-audit")) is True
    assert project_tree_path_is_excluded(PurePath(".pytest-pr73-suite", "nested", "file.py")) is True


def test_local_campaign_state_is_excluded_from_materialization() -> None:
    # Durable campaign state belongs to the control plane, not to the runnable project tree.
    assert project_tree_path_is_excluded(PurePath("state", "workspaces", "campaign-1")) is True
    assert project_tree_path_is_excluded(PurePath("src", "module.py")) is False
