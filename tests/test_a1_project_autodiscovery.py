from __future__ import annotations

import sys
from pathlib import Path

from theseus_local import discover_project
from theseus_ui import LocalWorkspaceCampaignClient


def test_a1_discovers_src_pytest_git_and_project_virtualenv(tmp_path: Path) -> None:
    # Verify A1 derives the complete deterministic onboarding contract from one project root.
    project = tmp_path / "sample"
    (project / ".git").mkdir(parents=True)
    (project / ".venv" / "Scripts").mkdir(parents=True)
    (project / ".venv" / "bin").mkdir(parents=True)
    windows_python = project / ".venv" / "Scripts" / "python.exe"
    posix_python = project / ".venv" / "bin" / "python"
    windows_python.write_bytes(b"")
    posix_python.write_bytes(b"")
    posix_python.chmod(0o755)
    (project / "src" / "sample").mkdir(parents=True)
    (project / "src" / "sample" / "__init__.py").write_text("", encoding="utf-8")
    (project / "src" / "sample" / "service.py").write_text("def choose():\n    return True\n", encoding="utf-8")
    (project / "tests").mkdir()
    (project / "tests" / "test_service.py").write_text("def test_choose():\n    assert True\n", encoding="utf-8")
    (project / ".pytest-pr73-suite").mkdir()
    (project / ".pytest-pr73-suite" / "generated.py").write_text("VALUE = 1\n", encoding="utf-8")
    (project / "pyproject.toml").write_text(
        '[tool.pytest.ini_options]\ntestpaths = ["tests"]\n[tool.setuptools.packages.find]\nwhere = ["src"]\n',
        encoding="utf-8",
    )

    result = discover_project(project)

    assert result.is_python_project is True
    assert result.interpreter_origin == "project_virtualenv"
    expected_interpreter = ".venv/Scripts/python.exe" if sys.platform == "win32" else ".venv/bin/python"
    assert result.python_interpreter_relative == expected_interpreter
    assert result.project_markers == ("pyproject.toml",)
    assert result.pytest_config == "pyproject.toml"
    assert result.source_roots == ("src",)
    assert result.test_roots == ("tests",)
    assert result.test_command[1:] == ("-m", "pytest", "-q")
    assert result.git_root == str(project.resolve())
    assert result.fingerprint.file_count == 4
    assert len(result.fingerprint.sha256) == 64
    assert result.warnings == ()


def test_a1_falls_back_to_runtime_and_conventional_layout_with_diagnostics(tmp_path: Path) -> None:
    # Verify A1 remains useful when a checkout has no local virtual environment or explicit packaging metadata.
    project = tmp_path / "plain"
    project.mkdir()
    (project / "app.py").write_text("VALUE = 1\n", encoding="utf-8")
    (project / "package").mkdir()
    (project / "package" / "module.py").write_text("VALUE = 2\n", encoding="utf-8")
    (project / "tests").mkdir()
    (project / "tests" / "test_app.py").write_text("def test_value():\n    assert True\n", encoding="utf-8")
    (project / "pytest.ini").write_text("[pytest]\ntestpaths = tests\n", encoding="utf-8")

    first = discover_project(project)
    second = discover_project(project)

    assert first.python_interpreter == str(Path(sys.executable).resolve())
    assert first.interpreter_origin == "theseus_runtime_fallback"
    assert first.source_roots == (".",)
    assert first.test_roots == ("tests",)
    assert first.pytest_config == "pytest.ini"
    assert first.fingerprint == second.fingerprint
    assert first.public_summary() == second.public_summary()
    assert first.warnings == ("project-local Python interpreter was not found; Theseus runtime would be used",)


def test_a1_discovers_nested_python_root_without_package_marker(tmp_path: Path) -> None:
    # Verify A1 detects common backend-style source roots even when the top-level directory is not a Python package.
    project = tmp_path / "service"
    (project / "backend" / "app").mkdir(parents=True)
    (project / "backend" / "app" / "service.py").write_text("VALUE = 1\n", encoding="utf-8")
    (project / "backend" / "tests").mkdir()
    (project / "backend" / "tests" / "test_service.py").write_text("def test_value():\n    assert True\n", encoding="utf-8")

    result = discover_project(project)

    assert result.is_python_project is True
    assert result.source_roots == ("backend",)
    assert result.test_roots == ("backend/tests",)
    assert "pytest configuration was not found; default pytest discovery would be used" in result.warnings


def test_a1_registration_returns_browser_safe_discovery_summary(tmp_path: Path) -> None:
    # Verify E25 project registration exposes A1 facts without leaking absolute checkout or interpreter paths.
    project = tmp_path / "browser-project"
    (project / "src" / "pkg").mkdir(parents=True)
    (project / "src" / "pkg" / "__init__.py").write_text("", encoding="utf-8")
    (project / "src" / "pkg" / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
    (project / "tests").mkdir()
    (project / "tests" / "test_module.py").write_text("def test_value():\n    assert True\n", encoding="utf-8")
    (project / "pyproject.toml").write_text('[tool.pytest.ini_options]\ntestpaths = ["tests"]\n', encoding="utf-8")
    client = LocalWorkspaceCampaignClient(tmp_path / "ui-state")

    registered = client.register_project(str(project), "Browser project")

    assert registered["ok"] is True
    discovery = registered["value"]["discovery"]
    assert discovery["is_python_project"] is True
    assert discovery["source_roots"] == ["src"]
    assert discovery["test_roots"] == ["tests"]
    assert discovery["python_interpreter"] == "Theseus runtime interpreter"
    assert discovery["test_command"] == ["Theseus runtime interpreter", "-m", "pytest", "-q"]
    assert str(project.resolve()) not in repr(discovery)
