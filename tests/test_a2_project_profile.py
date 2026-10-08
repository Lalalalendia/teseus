from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from theseus_api import ApiSuccess
from theseus_ui import LocalWorkspaceCampaignClient


class _FakeActions:
    def __init__(self, configuration):
        self.configuration = configuration

    def create_campaign(self, action_id, configuration):
        return ApiSuccess({"action_id": action_id, "campaign_id": configuration.campaign_id.value, "status": "completed", "campaign_revision": 0})

    def start_campaign(self, action_id, campaign_id, *, expected_revision):
        return ApiSuccess({"action_id": action_id, "campaign_id": campaign_id, "status": "running", "campaign_revision": expected_revision})


def _project(tmp_path: Path) -> tuple[Path, Path, str]:
    # Build one cross-platform src-layout checkout with a discoverable local interpreter.
    project = tmp_path / "project"
    (project / "src" / "sample").mkdir(parents=True)
    (project / "tests").mkdir()
    (project / "src" / "sample" / "app.py").write_text("def choose():\n    return True\n", encoding="utf-8")
    (project / "tests" / "test_app.py").write_text("def test_choose():\n    assert True\n", encoding="utf-8")
    (project / "pyproject.toml").write_text('[tool.pytest.ini_options]\ntestpaths = ["tests"]\n', encoding="utf-8")
    relative = ".venv/Scripts/python.exe" if sys.platform == "win32" else ".venv/bin/python"
    interpreter = project / Path(relative)
    interpreter.parent.mkdir(parents=True)
    interpreter.write_text("", encoding="utf-8")
    if sys.platform != "win32":
        interpreter.chmod(interpreter.stat().st_mode | 0o111)
    return project, interpreter.resolve(), relative


def test_a2_profile_is_persisted_and_restored_without_absolute_paths_in_browser_payload(tmp_path: Path) -> None:
    # Persist one discovered project profile and verify a restarted browser client reuses it.
    project, interpreter, relative = _project(tmp_path)
    state = tmp_path / "ui-state"
    client = LocalWorkspaceCampaignClient(state, actions_factory=lambda configuration: _FakeActions(configuration))

    registered = client.register_project(str(project), "Profile project")

    assert registered["ok"] is True
    profile = registered["value"]["profile"]
    assert profile["python_interpreter"] == relative
    assert profile["source_roots"] == ["src"]
    assert profile["test_roots"] == ["tests"]
    assert profile["max_mutants"] == 100
    assert profile["preferred_workers"] is None
    assert str(interpreter) not in repr(profile)

    restarted = LocalWorkspaceCampaignClient(state, actions_factory=lambda configuration: _FakeActions(configuration))
    restored = restarted.list_projects(limit=10)["value"]["items"][0]["profile"]
    assert restored == profile
    persisted = json.loads((state / "registry.json").read_text(encoding="utf-8"))["projects"][0]["profile"]
    assert persisted["python_interpreter"] == str(interpreter)


def test_a2_campaign_uses_profile_defaults_and_keeps_manual_values_as_run_overrides(tmp_path: Path) -> None:
    # Build campaigns from profile defaults unless one launch explicitly supplies an override.
    project, interpreter, _relative = _project(tmp_path)
    captured = []

    def actions_factory(configuration):
        # Capture the authoritative campaign contract passed to the existing action boundary.
        captured.append(configuration)
        return _FakeActions(configuration)

    client = LocalWorkspaceCampaignClient(tmp_path / "ui-state", actions_factory=actions_factory)
    project_id = client.register_project(str(project))["value"]["project_id"]
    defaulted = client.create_campaign({"project_id": project_id, "source_path": "src/sample/app.py", "scope_kind": "file", "operators": []})

    assert defaulted["ok"] is True
    default_config = captured[-1]
    assert default_config.project.test_command.argv == (str(interpreter), "-m", "pytest", "-q")
    assert default_config.budget.max_mutants == 100
    assert default_config.budget.max_workers == 1
    assert default_config.budget.max_seconds == 600.0
    assert default_config.budget.max_test_seconds == 120.0
    assert default_config.reuse_mode == "hint"

    override_command = [sys.executable, "-m", "pytest", "-q", "tests/test_app.py"]
    overridden = client.create_campaign(
        {
            "project_id": project_id,
            "source_path": "src/sample/app.py",
            "scope_kind": "file",
            "operators": [],
            "test_command": override_command,
            "max_mutants": 7,
            "max_workers": 2,
            "max_seconds": 30,
            "max_test_seconds": 4,
            "reuse_mode": "off",
            "no_escalation": True,
        }
    )

    assert overridden["ok"] is True
    override_config = captured[-1]
    assert override_config.project.test_command.argv == tuple(override_command)
    assert override_config.budget.max_mutants == 7
    assert override_config.budget.max_workers == 2
    assert override_config.budget.max_seconds == 30.0
    assert override_config.budget.max_test_seconds == 4.0
    assert override_config.reuse_mode == "off"
    assert override_config.no_escalation is True


def test_a2_legacy_registry_row_is_backfilled_with_profile_on_first_listing(tmp_path: Path) -> None:
    # Upgrade an A1 registry row lazily without losing the registered project identity.
    project, _interpreter, _relative = _project(tmp_path)
    state = tmp_path / "ui-state"
    client = LocalWorkspaceCampaignClient(state, actions_factory=lambda configuration: _FakeActions(configuration))
    project_id = client.register_project(str(project), "Legacy project")["value"]["project_id"]
    registry_path = state / "registry.json"
    payload = json.loads(registry_path.read_text(encoding="utf-8"))
    payload["projects"][0].pop("profile", None)
    registry_path.write_text(json.dumps(payload), encoding="utf-8")

    restarted = LocalWorkspaceCampaignClient(state, actions_factory=lambda configuration: _FakeActions(configuration))
    row = restarted.list_projects(limit=10)["value"]["items"][0]

    assert row["project_id"] == project_id
    assert row["profile"]["source_roots"] == ["src"]
    persisted = json.loads(registry_path.read_text(encoding="utf-8"))["projects"][0]
    assert isinstance(persisted["profile"], dict)


def test_a2_persisted_profile_drops_generated_pytest_roots_on_restart(tmp_path: Path) -> None:
    # Sanitize A1/A2 profiles created before canonical .pytest-* prefix exclusions were applied.
    project, _interpreter, _relative = _project(tmp_path)
    state = tmp_path / "ui-state"
    client = LocalWorkspaceCampaignClient(state, actions_factory=lambda configuration: _FakeActions(configuration))
    client.register_project(str(project), "Polluted profile")
    registry_path = state / "registry.json"
    payload = json.loads(registry_path.read_text(encoding="utf-8"))
    payload["projects"][0]["profile"]["source_roots"] = [".", "src", ".pytest-pr73-suite"]
    registry_path.write_text(json.dumps(payload), encoding="utf-8")

    restarted = LocalWorkspaceCampaignClient(state, actions_factory=lambda configuration: _FakeActions(configuration))
    profile = restarted.list_projects(limit=10)["value"]["items"][0]["profile"]

    assert profile["source_roots"] == ["."]
