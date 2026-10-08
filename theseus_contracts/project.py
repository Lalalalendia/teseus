"""Project and execution-environment descriptors for public campaign requests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .ids import ProjectId, RevisionId
from .serialization import WireModel, optional_string, required_string, sequence_of_strings


@dataclass(frozen=True, slots=True)
class RepositoryRevision(WireModel):
    """Repository image used as the campaign input."""

    revision_id: RevisionId
    git_revision: str | None = None
    dirty: bool = False

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "RepositoryRevision":
        # Restore revision identity while ignoring compatible future metadata.
        return cls(
            revision_id=RevisionId(required_string(value, "revision_id")),
            git_revision=optional_string(value, "git_revision"),
            dirty=bool(value.get("dirty", False)),
        )


@dataclass(frozen=True, slots=True)
class EnvironmentDescriptor(WireModel):
    """Fingerprint of the Python and pytest environment used for selection."""

    fingerprint: str
    python_version: str
    pytest_version: str | None = None
    plugins: tuple[str, ...] = ()
    configuration_fingerprint: str | None = None
    declared_env_keys: tuple[str, ...] = ()
    declared_env_patterns: tuple[str, ...] = ()
    tracked_variables: tuple[str, ...] = ()
    tracked_prefixes: tuple[str, ...] = ()
    ignored_variables: tuple[str, ...] = ()
    secret_variables: tuple[str, ...] = ()
    inherit_policy: str = "allowlisted"

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "EnvironmentDescriptor":
        # Parse environment data without depending on installed-package APIs.
        return cls(
            fingerprint=required_string(value, "fingerprint"),
            python_version=required_string(value, "python_version"),
            pytest_version=optional_string(value, "pytest_version"),
            plugins=sequence_of_strings(value, "plugins"),
            configuration_fingerprint=optional_string(value, "configuration_fingerprint"),
            declared_env_keys=sequence_of_strings(value, "declared_env_keys"),
            declared_env_patterns=sequence_of_strings(value, "declared_env_patterns"),
            tracked_variables=sequence_of_strings(value, "tracked_variables"),
            tracked_prefixes=sequence_of_strings(value, "tracked_prefixes"),
            ignored_variables=sequence_of_strings(value, "ignored_variables"),
            secret_variables=sequence_of_strings(value, "secret_variables"),
            inherit_policy=str(value.get("inherit_policy", "allowlisted")),
        )


@dataclass(frozen=True, slots=True)
class TestCommandDescriptor(WireModel):
    """Safe argv command description; shell execution is never part of the contract."""

    __test__ = False

    argv: tuple[str, ...]
    cwd: str | None = None
    shell: bool = False

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "TestCommandDescriptor":
        # Restore argv as immutable strings and reject a shell-enabled request.
        argv = sequence_of_strings(value, "argv")
        if not argv:
            raise ValueError("argv must not be empty")
        if bool(value.get("shell", False)):
            raise ValueError("shell execution is forbidden by the Theseus contract")
        return cls(argv=argv, cwd=optional_string(value, "cwd"), shell=False)


@dataclass(frozen=True, slots=True)
class IsolationProfile(WireModel):
    """Workspace and process isolation guarantees exposed to the control plane."""

    mode: str = "copy"
    clean_checkout: bool = True
    one_process_per_mutant: bool = True
    kill_process_tree: bool = True

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "IsolationProfile":
        # Preserve the safety defaults when an older producer omits optional flags.
        return cls(
            mode=str(value.get("mode", "copy")),
            clean_checkout=bool(value.get("clean_checkout", True)),
            one_process_per_mutant=bool(value.get("one_process_per_mutant", True)),
            kill_process_tree=bool(value.get("kill_process_tree", True)),
        )


@dataclass(frozen=True, slots=True)
class ProjectDescriptor(WireModel):
    """Project identity plus non-identity checkout location metadata."""

    project_id: ProjectId
    display_name: str
    root_path: str
    main_root_path: str | None = None
    revision: RepositoryRevision | None = None
    environment: EnvironmentDescriptor | None = None
    test_command: TestCommandDescriptor | None = None
    data_dependency_globs: tuple[str, ...] = ()
    data_dependency_exclude_globs: tuple[str, ...] = ()
    data_dependency_max_file_size_bytes: int = 10 * 1024 * 1024
    pytest_plugin_autoload: bool = True
    isolation: IsolationProfile = IsolationProfile()

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ProjectDescriptor":
        # Restore nested project descriptors while leaving absolute paths out of IDs.
        revision = value.get("revision")
        environment = value.get("environment")
        command = value.get("test_command")
        isolation = value.get("isolation")
        return cls(
            project_id=ProjectId(required_string(value, "project_id")),
            display_name=required_string(value, "display_name"),
            root_path=required_string(value, "root_path"),
            main_root_path=optional_string(value, "main_root_path"),
            revision=RepositoryRevision.from_dict(revision) if isinstance(revision, Mapping) else None,
            environment=EnvironmentDescriptor.from_dict(environment) if isinstance(environment, Mapping) else None,
            test_command=TestCommandDescriptor.from_dict(command) if isinstance(command, Mapping) else None,
            data_dependency_globs=sequence_of_strings(value, "data_dependency_globs"),
            data_dependency_exclude_globs=sequence_of_strings(value, "data_dependency_exclude_globs"),
            data_dependency_max_file_size_bytes=max(
                1,
                int(value.get("data_dependency_max_file_size_bytes", 10 * 1024 * 1024)),
            ),
            pytest_plugin_autoload=bool(value.get("pytest_plugin_autoload", True)),
            isolation=IsolationProfile.from_dict(isolation) if isinstance(isolation, Mapping) else IsolationProfile(),
        )
