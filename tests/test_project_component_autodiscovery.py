from __future__ import annotations

import json
import sys
from pathlib import Path

from theseus_local.project_discovery import discover_project
from theseus_local.project_profile import ProjectProfile
from theseus_local.project_run import project_source_files, project_test_cwd
from theseus_ui.local_control import LocalUiRegistry, LocalWorkspaceCampaignClient


class _FakeActions:
    def __init__(self, configuration, captured: list[object]) -> None:
        # Retain one child configuration while matching the launch action boundary.
        self.configuration = configuration
        self.captured = captured

    def create_campaign(self, action_id: str, configuration=None):
        # Capture the exact campaign configuration without touching a real coordinator.
        self.captured.append(configuration or self.configuration)
        return {"ok": True, "kind": "success", "value": {"status": "completed", "campaign_revision": 0}}

    def start_campaign(self, action_id: str, campaign_id: str, *, expected_revision: int):
        # Return one stable running receipt for the component-cwd contract.
        return {"ok": True, "kind": "success", "value": {"status": "running", "campaign_revision": expected_revision}}


def _component_project(root: Path) -> Path:
    # Build a monorepo-style fixture with root tooling and two independently configured pytest components.
    project = root / "project"
    (project / "tests").mkdir(parents=True)
    (project / "tests" / "test_tool.py").write_text("def test_tool():\n    assert True\n", encoding="utf-8")
    (project / "__init__.py").write_text("", encoding="utf-8")
    (project / "dump.py").write_text("VALUE = 1\n", encoding="utf-8")
    for component, package in (("backend", "app"), ("shadow_backend", "shadow_backend")):
        (project / component / package).mkdir(parents=True)
        (project / component / package / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
        (project / component / "tests").mkdir()
        (project / component / "tests" / "test_module.py").write_text("def test_value():\n    assert True\n", encoding="utf-8")
        (project / component / "pytest.ini").write_text("[pytest]\npythonpath = .\n", encoding="utf-8")
    return project


def test_discovery_prefers_explicit_nested_pytest_components_over_root_tooling(tmp_path: Path) -> None:
    # Keep root helper files out of one-click production scope when explicit child pytest components exist.
    project = _component_project(tmp_path)
    result = discover_project(project)
    assert result.source_roots == ("backend", "shadow_backend")
    assert result.test_roots == ("backend/tests", "shadow_backend/tests")
    assert "multiple pytest components were detected" in "\n".join(result.warnings)
    profile = ProjectProfile.from_discovery(result)
    assert project_source_files(project, profile) == (
        "backend/app/module.py",
        "shadow_backend/shadow_backend/module.py",
    )
    assert project_test_cwd(profile, "backend/app/module.py") == "backend"
    assert project_test_cwd(profile, "shadow_backend/shadow_backend/module.py") == "shadow_backend"


def test_project_run_binds_each_component_to_its_own_pytest_cwd(tmp_path: Path) -> None:
    # Launch child campaigns with component-local cwd so pytest.ini relative settings retain their meaning.
    project = _component_project(tmp_path)
    captured: list[object] = []
    state_dir = tmp_path / "ui-state"
    client = LocalWorkspaceCampaignClient(
        state_dir,
        actions_factory=lambda configuration: _FakeActions(configuration, captured),
    )
    project_id = client.register_project(str(project), "Components")["value"]["project_id"]
    result = client.create_project_run({"project_id": project_id, "max_workers": 1})
    assert result["ok"] is True
    assert len(captured) == 2
    cwd_by_source = {item.scope.source_path: Path(item.project.test_command.cwd).resolve() for item in captured}
    assert cwd_by_source["backend/app/module.py"] == (project / "backend").resolve()
    assert cwd_by_source["shadow_backend/shadow_backend/module.py"] == (project / "shadow_backend").resolve()


def test_profile_schema_bump_forces_legacy_registry_rediscovery(tmp_path: Path) -> None:
    # Reject the old profile shape so the registry backfills component-aware discovery on first access.
    project = _component_project(tmp_path)
    state_dir = tmp_path / "ui-state"
    registry = LocalUiRegistry(state_dir)
    registered = registry.register_project(project)
    raw = json.loads(registry.path.read_text(encoding="utf-8"))
    raw["projects"][0]["profile"]["schema_version"] = 1
    registry.path.write_text(json.dumps(raw), encoding="utf-8")
    restarted = LocalUiRegistry(state_dir)
    profile = restarted.project_profile(registered.project_id)
    assert profile is not None
    assert profile.schema_version == 2
    assert profile.source_roots == ("backend", "shadow_backend")
