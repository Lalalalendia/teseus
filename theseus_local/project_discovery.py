"""Deterministic local Python-project discovery for Theseus project onboarding."""
from __future__ import annotations

import configparser
import os
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from theseus_contracts.project_tree import PROJECT_TREE_EXCLUDED_DIRS, project_tree_path_is_excluded
from theseus_performance.project_benchmark import fingerprint_project

PROJECT_DISCOVERY_SCHEMA_VERSION = 1
PROJECT_MARKERS = (
    "pyproject.toml",
    "pytest.ini",
    "setup.cfg",
    "setup.py",
    "tox.ini",
    "requirements.txt",
)
DISCOVERY_EXCLUDED_DIRS = frozenset((*PROJECT_TREE_EXCLUDED_DIRS, ".nox", ".tox", "build", "dist"))
_TEST_FILE_NAMES = ("test_", "_test.py")


def _directory_excluded(name: str) -> bool:
    # Reuse canonical project-tree exclusions, including generated directory prefixes such as .pytest-*.
    return name.casefold() in DISCOVERY_EXCLUDED_DIRS or project_tree_path_is_excluded(Path(name))


@dataclass(frozen=True, slots=True)
class ProjectDiscoveryFingerprint:
    """Stable project content identity reused from the performance authority."""

    sha256: str
    file_count: int
    byte_count: int

    def to_dict(self) -> dict[str, object]:
        # Serialize stable project identity without timing-dependent scan metadata.
        return {"sha256": self.sha256, "file_count": self.file_count, "byte_count": self.byte_count}


@dataclass(frozen=True, slots=True)
class ProjectDiscoveryResult:
    """Deterministic project facts required by later one-click onboarding stages."""

    project_root: str
    is_python_project: bool
    python_interpreter: str
    python_interpreter_relative: str | None
    interpreter_origin: str
    project_markers: tuple[str, ...]
    pytest_config: str | None
    source_roots: tuple[str, ...]
    test_roots: tuple[str, ...]
    test_command: tuple[str, ...]
    excluded_dirs: tuple[str, ...]
    git_root: str | None
    fingerprint: ProjectDiscoveryFingerprint
    warnings: tuple[str, ...]
    schema_version: int = PROJECT_DISCOVERY_SCHEMA_VERSION

    def to_dict(self) -> dict[str, object]:
        # Serialize discovery facts with deterministic ordering and immutable sequence semantics.
        return {
            "schema_version": self.schema_version,
            "project_root": self.project_root,
            "is_python_project": self.is_python_project,
            "python_interpreter": self.python_interpreter,
            "python_interpreter_relative": self.python_interpreter_relative,
            "interpreter_origin": self.interpreter_origin,
            "project_markers": list(self.project_markers),
            "pytest_config": self.pytest_config,
            "source_roots": list(self.source_roots),
            "test_roots": list(self.test_roots),
            "test_command": list(self.test_command),
            "excluded_dirs": list(self.excluded_dirs),
            "git_root": self.git_root,
            "fingerprint": self.fingerprint.to_dict(),
            "warnings": list(self.warnings),
        }

    def public_summary(self) -> dict[str, object]:
        # Expose browser-safe discovery facts without absolute checkout or interpreter paths.
        interpreter = self.python_interpreter_relative or "Theseus runtime interpreter"
        return {
            "schema_version": self.schema_version,
            "is_python_project": self.is_python_project,
            "python_interpreter": interpreter,
            "interpreter_origin": self.interpreter_origin,
            "project_markers": list(self.project_markers),
            "pytest_config": self.pytest_config,
            "source_roots": list(self.source_roots),
            "test_roots": list(self.test_roots),
            "test_command": [interpreter, *self.test_command[1:]],
            "excluded_dirs": list(self.excluded_dirs),
            "git_repository": self.git_root is not None,
            "fingerprint": self.fingerprint.to_dict(),
            "warnings": list(self.warnings),
        }


def _relative(root: Path, path: Path) -> str:
    # Convert one path known to be inside the selected project root into canonical POSIX form.
    relative = path.resolve().relative_to(root)
    return "." if relative == Path(".") else relative.as_posix()


def _read_pyproject(root: Path, warnings: list[str]) -> Mapping[str, Any]:
    # Parse one optional pyproject file without making malformed metadata fatal to discovery.
    path = root / "pyproject.toml"
    if not path.is_file():
        return {}
    try:
        value = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError):
        warnings.append("pyproject.toml could not be parsed")
        return {}
    return value if isinstance(value, Mapping) else {}


def _read_ini(path: Path, warnings: list[str]) -> configparser.ConfigParser | None:
    # Parse one optional INI-style project configuration while retaining deterministic fallback behavior.
    if not path.is_file():
        return None
    parser = configparser.ConfigParser(interpolation=None)
    try:
        parser.read(path, encoding="utf-8")
    except (OSError, UnicodeError, configparser.Error):
        warnings.append(f"{path.name} could not be parsed")
        return None
    return parser


def _pytest_configuration(root: Path, pyproject: Mapping[str, Any], warnings: list[str]) -> tuple[str | None, tuple[str, ...]]:
    # Follow pytest configuration precedence and extract declared testpaths when available.
    pytest_ini = _read_ini(root / "pytest.ini", warnings)
    if pytest_ini is not None:
        testpaths = pytest_ini.get("pytest", "testpaths", fallback="") if pytest_ini.has_section("pytest") else ""
        return "pytest.ini", tuple(testpaths.split())
    tool = pyproject.get("tool")
    pytest = tool.get("pytest") if isinstance(tool, Mapping) else None
    ini_options = pytest.get("ini_options") if isinstance(pytest, Mapping) else None
    if isinstance(ini_options, Mapping):
        raw = ini_options.get("testpaths", ())
        if isinstance(raw, str):
            return "pyproject.toml", tuple(raw.split())
        if isinstance(raw, (list, tuple)):
            return "pyproject.toml", tuple(str(item) for item in raw if str(item).strip())
        return "pyproject.toml", ()
    tox_ini = _read_ini(root / "tox.ini", warnings)
    if tox_ini is not None and tox_ini.has_section("pytest"):
        return "tox.ini", tuple(tox_ini.get("pytest", "testpaths", fallback="").split())
    setup_cfg = _read_ini(root / "setup.cfg", warnings)
    if setup_cfg is not None:
        for section in ("tool:pytest", "pytest"):
            if setup_cfg.has_section(section):
                return "setup.cfg", tuple(setup_cfg.get(section, "testpaths", fallback="").split())
    return None, ()


def _resolve_interpreter(root: Path) -> tuple[Path, str, str | None]:
    # Prefer a project-owned virtual environment and otherwise fall back to the current Theseus runtime.
    windows = (root / ".venv" / "Scripts" / "python.exe", root / "venv" / "Scripts" / "python.exe")
    posix = (root / ".venv" / "bin" / "python", root / "venv" / "bin" / "python")
    candidates = windows if sys.platform == "win32" else posix
    for candidate in candidates:
        if candidate.is_file() and (sys.platform == "win32" or os.access(candidate, os.X_OK)):
            return candidate.resolve(), "project_virtualenv", _relative(root, candidate)
    return Path(sys.executable).resolve(), "theseus_runtime_fallback", None


def _declared_source_roots(root: Path, pyproject: Mapping[str, Any]) -> tuple[str, ...]:
    # Extract source roots declared by supported setuptools pyproject layouts.
    discovered: set[str] = set()
    tool = pyproject.get("tool")
    setuptools = tool.get("setuptools") if isinstance(tool, Mapping) else None
    if not isinstance(setuptools, Mapping):
        return ()
    package_dir = setuptools.get("package-dir")
    if isinstance(package_dir, Mapping):
        for value in package_dir.values():
            if not isinstance(value, str) or not value.strip():
                continue
            candidate = (root / value).resolve()
            if candidate.is_dir() and candidate.is_relative_to(root):
                discovered.add(_relative(root, candidate))
    packages = setuptools.get("packages")
    find = packages.get("find") if isinstance(packages, Mapping) else None
    where = find.get("where") if isinstance(find, Mapping) else None
    values = (where,) if isinstance(where, str) else where if isinstance(where, (list, tuple)) else ()
    for value in values:
        candidate = (root / str(value)).resolve()
        if candidate.is_dir() and candidate.is_relative_to(root):
            discovered.add(_relative(root, candidate))
    return tuple(sorted(discovered))


def _has_production_python(path: Path) -> bool:
    # Detect at least one non-test Python file below a candidate source root while honoring discovery exclusions.
    pending = [path]
    while pending:
        directory = pending.pop()
        try:
            entries = tuple(directory.iterdir())
        except OSError:
            continue
        for candidate in sorted(entries, key=lambda item: item.name.casefold(), reverse=True):
            if candidate.is_dir():
                if not _directory_excluded(candidate.name) and candidate.name.casefold() not in {"test", "tests"}:
                    pending.append(candidate)
                continue
            name = candidate.name.casefold()
            if candidate.suffix.casefold() == ".py" and name != "conftest.py" and not name.startswith("test_") and not name.endswith("_test.py"):
                return True
    return False


def _source_roots(root: Path, pyproject: Mapping[str, Any]) -> tuple[str, ...]:
    # Combine declared packaging roots with conventional and top-level Python package roots.
    discovered = set(_declared_source_roots(root, pyproject))
    src = root / "src"
    if src.is_dir() and _has_production_python(src):
        discovered.add("src")
    for candidate in sorted(root.iterdir(), key=lambda item: item.name.casefold()):
        if not candidate.is_dir() or _directory_excluded(candidate.name) or candidate.name.casefold() in {"test", "tests", "src"}:
            continue
        if _has_production_python(candidate):
            discovered.add(candidate.name)
    if any(
        item.is_file()
        and item.suffix.casefold() == ".py"
        and item.name.casefold() not in {"conftest.py", "setup.py"}
        and not item.name.casefold().startswith("test_")
        and not item.name.casefold().endswith("_test.py")
        for item in root.iterdir()
    ):
        discovered.add(".")
    return (".",) if "." in discovered else tuple(sorted(discovered))


def _is_test_file(path: Path) -> bool:
    # Classify conventional pytest-owned Python files for fallback test-root discovery.
    name = path.name.casefold()
    return path.suffix.casefold() == ".py" and (name == "conftest.py" or name.startswith(_TEST_FILE_NAMES[0]) or name.endswith(_TEST_FILE_NAMES[1]))


def _test_roots(root: Path, declared: tuple[str, ...]) -> tuple[str, ...]:
    # Resolve declared or conventional test roots and fall back to minimal parents of discovered pytest files.
    discovered: set[str] = set()
    for value in declared:
        candidate = (root / value).resolve()
        if candidate.is_dir() and candidate.is_relative_to(root):
            discovered.add(_relative(root, candidate))
    for name in ("tests", "test"):
        candidate = root / name
        if candidate.is_dir():
            discovered.add(name)
    if discovered:
        return tuple(sorted(discovered))
    parents: set[Path] = set()
    pending = [root]
    while pending:
        directory = pending.pop()
        try:
            entries = tuple(directory.iterdir())
        except OSError:
            continue
        for candidate in sorted(entries, key=lambda item: item.name.casefold(), reverse=True):
            if candidate.is_dir():
                if not _directory_excluded(candidate.name):
                    pending.append(candidate)
            elif _is_test_file(candidate):
                parents.add(candidate.parent.resolve())
    minimal = {candidate for candidate in parents if not any(parent != candidate and parent in candidate.parents for parent in parents)}
    return tuple(sorted(_relative(root, candidate) for candidate in minimal))


def _nested_pytest_components(root: Path, warnings: list[str]) -> tuple[tuple[str, str, tuple[str, ...]], ...]:
    # Discover direct child Python components that own an explicit pytest configuration and tests.
    components: list[tuple[str, str, tuple[str, ...]]] = []
    for child in sorted(root.iterdir(), key=lambda item: item.name.casefold()):
        if not child.is_dir() or _directory_excluded(child.name):
            continue
        pyproject = _read_pyproject(child, warnings)
        config, declared_testpaths = _pytest_configuration(child, pyproject, warnings)
        if config is None or not _has_production_python(child):
            continue
        test_roots = _test_roots(child, declared_testpaths)
        if not test_roots:
            continue
        components.append(
            (
                _relative(root, child),
                _relative(root, child / config),
                tuple(_relative(root, child / item) for item in test_roots),
            )
        )
    return tuple(components)


def _git_root(root: Path) -> Path | None:
    # Find the nearest Git worktree root at or above the selected project directory without invoking Git.
    current = root
    while True:
        if (current / ".git").exists():
            return current
        if current.parent == current:
            return None
        current = current.parent


def discover_project(project_root: str | Path) -> ProjectDiscoveryResult:
    # Discover one Python checkout deterministically without launching project code or external commands.
    root = Path(project_root).expanduser().resolve()
    if not root.is_dir():
        raise ValueError(f"project root is not a directory: {root}")
    warnings: list[str] = []
    markers = tuple(name for name in PROJECT_MARKERS if (root / name).is_file())
    pyproject = _read_pyproject(root, warnings)
    pytest_config, declared_testpaths = _pytest_configuration(root, pyproject, warnings)
    components = _nested_pytest_components(root, warnings) if pytest_config is None else ()
    if components:
        source_roots = tuple(item[0] for item in components)
        test_roots = tuple(sorted(test_root for item in components for test_root in item[2]))
        pytest_config = components[0][1] if len(components) == 1 else None
        if len(components) > 1:
            warnings.append("multiple pytest components were detected; ProjectRun will use per-component test roots")
    else:
        source_roots = _source_roots(root, pyproject)
        test_roots = _test_roots(root, declared_testpaths)
    interpreter, interpreter_origin, interpreter_relative = _resolve_interpreter(root)
    git_root = _git_root(root)
    fingerprint = fingerprint_project(root)
    is_python_project = bool(markers or source_roots or test_roots)
    if not is_python_project:
        warnings.append("no Python project structure was detected")
    if interpreter_origin != "project_virtualenv":
        warnings.append("project-local Python interpreter was not found; Theseus runtime would be used")
    if pytest_config is None and not components:
        warnings.append("pytest configuration was not found; default pytest discovery would be used")
    if not source_roots:
        warnings.append("production Python source roots were not detected")
    if not test_roots:
        warnings.append("pytest test roots were not detected")
    result = ProjectDiscoveryResult(
        project_root=str(root),
        is_python_project=is_python_project,
        python_interpreter=str(interpreter),
        python_interpreter_relative=interpreter_relative,
        interpreter_origin=interpreter_origin,
        project_markers=markers,
        pytest_config=pytest_config,
        source_roots=source_roots,
        test_roots=test_roots,
        test_command=(str(interpreter), "-m", "pytest", "-q"),
        excluded_dirs=tuple(sorted(DISCOVERY_EXCLUDED_DIRS)),
        git_root=str(git_root) if git_root is not None else None,
        fingerprint=ProjectDiscoveryFingerprint(
            sha256=fingerprint.sha256,
            file_count=fingerprint.file_count,
            byte_count=fingerprint.byte_count,
        ),
        warnings=tuple(warnings),
    )
    return result


__all__ = [
    "DISCOVERY_EXCLUDED_DIRS",
    "PROJECT_DISCOVERY_SCHEMA_VERSION",
    "ProjectDiscoveryFingerprint",
    "ProjectDiscoveryResult",
    "discover_project",
]
