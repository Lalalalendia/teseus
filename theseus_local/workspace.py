"""External state and copy-workspace ownership for local Theseus campaigns."""
from __future__ import annotations
import errno
import os
import shutil
import subprocess
import tarfile
import tempfile
from functools import partial
from dataclasses import dataclass, replace
from pathlib import Path
from theseus_contracts import CampaignConfiguration
from theseus_contracts.project_tree import project_tree_path_is_excluded
from test_intelligence_unified_v1.io_utils import FileLock, atomic_write_json, read_json, sha256_file, stable_hash, utc_now_iso
def _inside(path: Path, parent: Path) -> bool:
    # Decide containment using resolved paths so a report path cannot escape through symlinks or dot segments.
    resolved_path = path.resolve()
    resolved_parent = parent.resolve()
    return resolved_path == resolved_parent or resolved_parent in resolved_path.parents
def _tree_fingerprint(root: Path) -> str:
    # Hash exactly the project-relative inputs admitted by the canonical materialization policy.
    resolved_root = Path(root).resolve()
    rows: list[tuple[str, str]] = []
    pending = [resolved_root]
    while pending:
        directory = pending.pop()
        try:
            entries = list(os.scandir(directory))
        except OSError:
            continue
        for entry in sorted(entries, key=lambda item: item.name.lower(), reverse=True):
            path = Path(entry.path)
            try:
                relative_path = path.relative_to(resolved_root)
            except ValueError:
                continue
            if project_tree_path_is_excluded(relative_path):
                continue
            if entry.is_dir(follow_symlinks=False):
                pending.append(path)
                continue
            if not entry.is_file(follow_symlinks=False):
                continue
            try:
                rows.append((relative_path.as_posix(), sha256_file(path)))
            except OSError:
                rows.append((relative_path.as_posix(), "unreadable"))
    rows.sort()
    return stable_hash(rows)
def _copy_tree_ignored_names(source_root: Path, directory: str, names: list[str]) -> set[str]:
    # Return the exact generated entries excluded by the canonical project-tree policy for one copy directory.
    resolved_source = Path(source_root).resolve()
    resolved_directory = Path(directory).resolve()
    relative_directory = resolved_directory.relative_to(resolved_source)
    return {
        name
        for name in names
        if project_tree_path_is_excluded(relative_directory / name)
    }

def project_tree_logical_bytes(root: Path) -> int:
    # Sum canonical project input bytes without reading file contents.
    resolved_root = Path(root).resolve()
    total = 0
    pending = [resolved_root]
    while pending:
        directory = pending.pop()
        try:
            entries = list(os.scandir(directory))
        except OSError:
            continue
        for entry in entries:
            path = Path(entry.path)
            try:
                relative_path = path.relative_to(resolved_root)
            except ValueError:
                continue
            if project_tree_path_is_excluded(relative_path):
                continue
            if entry.is_dir(follow_symlinks=False):
                pending.append(path)
                continue
            if not entry.is_file(follow_symlinks=False):
                continue
            try:
                total += max(0, int(entry.stat(follow_symlinks=False).st_size))
            except OSError:
                continue
    return total

def _remove_owned_path(path: Path, state_root: Path, *, directory: bool) -> int:
    # Remove one heavy state path only when it is a normal path inside the private state root.
    resolved_state = Path(state_root).resolve()
    candidate = Path(path)
    if candidate.is_symlink():
        raise RuntimeError(f"workspace cleanup refuses symlink: {candidate}")
    try:
        resolved = candidate.resolve()
    except OSError as exc:
        raise RuntimeError(f"workspace cleanup cannot resolve path: {candidate}") from exc
    if resolved == resolved_state or resolved_state not in resolved.parents:
        raise RuntimeError(f"workspace cleanup path escapes state root: {resolved}")
    if not resolved.exists():
        return 0
    if directory:
        if not resolved.is_dir():
            raise RuntimeError(f"workspace cleanup expected directory: {resolved}")
        logical_bytes = project_tree_logical_bytes(resolved)
        shutil.rmtree(resolved)
        return logical_bytes
    if not resolved.is_file():
        raise RuntimeError(f"workspace cleanup expected file: {resolved}")
    try:
        logical_bytes = max(0, int(resolved.stat().st_size))
    except OSError:
        logical_bytes = 0
    resolved.unlink()
    return logical_bytes

def cleanup_campaign_workspaces(reports_root: Path, campaign_id: str) -> dict[str, int]:
    # Reclaim only disposable campaign and worker project copies while preserving durable reports, spools and diagnostics.
    reports = Path(reports_root).expanduser().resolve()
    state_root = reports.parent / "state"
    reclaimed_bytes = 0
    removed_paths = 0
    workspaces_root = state_root / "workspaces"
    for path, directory in (
        (workspaces_root / str(campaign_id), True),
        (workspaces_root / f"{campaign_id}.ownership.json", False),
    ):
        existed = path.exists()
        reclaimed_bytes += _remove_owned_path(path, state_root, directory=directory)
        removed_paths += int(existed and not path.exists())
    workers_root = state_root / "workers" / str(campaign_id)
    if workers_root.is_symlink():
        raise RuntimeError(f"workspace cleanup refuses symlink: {workers_root}")
    if workers_root.is_dir():
        for worker_root in workers_root.iterdir():
            if worker_root.is_symlink() or not worker_root.is_dir():
                continue
            for attempt_root in worker_root.iterdir():
                if attempt_root.is_symlink() or not attempt_root.is_dir() or not attempt_root.name.startswith("attempt-"):
                    continue
                for path, directory in (
                    (attempt_root / "workspace", True),
                    (attempt_root / "workspace.ownership.json", False),
                ):
                    existed = path.exists()
                    reclaimed_bytes += _remove_owned_path(path, state_root, directory=directory)
                    removed_paths += int(existed and not path.exists())
    return {"reclaimed_bytes": reclaimed_bytes, "removed_paths": removed_paths}

@dataclass(frozen=True, slots=True)
class WorkspaceHandle:
    """Durable paths for one campaign whose mutations are isolated from the main checkout."""
    main_root: Path
    workspace_root: Path
    state_root: Path
    reports_root: Path
    knowledge_root: Path
    ownership_manifest: Path
    def runtime_configuration(self, configuration: CampaignConfiguration) -> CampaignConfiguration:
        # Rebind engine paths and command cwd to the copied workspace while retaining campaign identity inputs.
        command = configuration.project.test_command
        runtime_command = command
        if command is not None:
            command_cwd = Path(command.cwd).expanduser() if command.cwd else self.main_root
            if not command_cwd.is_absolute():
                command_cwd = self.main_root / command_cwd
            if _inside(command_cwd, self.main_root):
                relative = command_cwd.resolve().relative_to(self.main_root.resolve())
                runtime_cwd = self.workspace_root / relative
            else:
                runtime_cwd = command_cwd.resolve()
            runtime_command = replace(command, cwd=str(runtime_cwd))
        runtime_project = replace(
            configuration.project,
            root_path=str(self.workspace_root),
            main_root_path=str(self.main_root),
            test_command=runtime_command,
        )
        return replace(
            configuration,
            project=runtime_project,
            reports_dir=str(self.reports_root),
        )
class WorkspaceProvider:
    """Create and reuse copy-based workspaces with all control state outside the checkout."""
    def __init__(self, configuration: CampaignConfiguration) -> None:
        # Resolve the main checkout once so every derived path can be checked against it.
        self.configuration = configuration
        configured_root = configuration.project.main_root_path or configuration.project.root_path
        self.main_root = Path(configured_root).expanduser().resolve()
        if not self.main_root.is_dir():
            raise ValueError(f"project root is not a directory: {self.main_root}")
        configured_reports = configuration.reports_dir
        self.compatibility_reports_root: Path | None = None
        if configured_reports:
            reports_root = Path(configured_reports).expanduser()
            if not reports_root.is_absolute():
                reports_root = self.main_root / reports_root
            reports_root = reports_root.resolve()
            if _inside(reports_root, self.main_root):
                self.compatibility_reports_root = reports_root
        else:
            reports_root = self.main_root.parent / ".theseus-state" / configuration.project.project_id.value / "reports"
        if _inside(reports_root, self.main_root):
            reports_root = self.main_root.parent / ".theseus-state" / configuration.project.project_id.value / "reports"
        self.reports_root = reports_root
        self.state_root = reports_root.parent / "state"
        self.knowledge_root = reports_root.parent / "knowledge"
    def publish_compatibility_projection(self, handle: WorkspaceHandle) -> None:
        # Keep compatibility projections disabled so a campaign cannot add entries to the main checkout.
        del handle
    @staticmethod
    def _copy_tree(source: Path, destination: Path) -> None:
        # Copy only canonical project inputs while excluding generated state and mutable dependency trees.
        resolved_source = Path(source).resolve()
        shutil.copytree(
            resolved_source,
            destination,
            ignore=partial(_copy_tree_ignored_names, resolved_source),
        )
    @staticmethod
    def _hardlink_tree(source: Path, destination: Path) -> tuple[int, int]:
        # Materialize an isolated directory tree by hardlinking immutable files and copying only when links are unavailable.
        resolved_source = Path(source).resolve()
        linked_files = 0
        copied_files = 0
        fallback_errnos = {errno.EXDEV, errno.EPERM, errno.EACCES, errno.EINVAL, errno.ENOSYS}
        if hasattr(errno, "ENOTSUP"):
            fallback_errnos.add(errno.ENOTSUP)
        if hasattr(errno, "EOPNOTSUPP"):
            fallback_errnos.add(errno.EOPNOTSUPP)
        def link_or_copy(source_file: str, destination_file: str) -> str:
            # Link one immutable source file or copy it when the filesystem cannot safely provide a hardlink.
            nonlocal linked_files, copied_files
            if Path(source_file).is_symlink():
                shutil.copy2(source_file, destination_file, follow_symlinks=True)
                copied_files += 1
                return destination_file
            try:
                os.link(source_file, destination_file)
                linked_files += 1
            except OSError as exc:
                winerror = getattr(exc, "winerror", None)
                if exc.errno not in fallback_errnos and winerror not in {1, 17, 50, 87, 1314}:
                    raise
                shutil.copy2(source_file, destination_file)
                copied_files += 1
            return destination_file
        shutil.copytree(
            resolved_source,
            destination,
            ignore=partial(_copy_tree_ignored_names, resolved_source),
            copy_function=link_or_copy,
        )
        return linked_files, copied_files
    @staticmethod
    def _extract_git_archive(source: Path, revision: str, destination: Path) -> None:
        # Materialize an exact Git revision without creating a worktree or changing checkout metadata.
        destination.mkdir(parents=True, exist_ok=True)
        process = subprocess.Popen(
            ["git", "-C", str(source), "archive", revision],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
        )
        if process.stdout is None:
            raise RuntimeError("git archive did not provide a stream")
        try:
            with tarfile.open(fileobj=process.stdout, mode="r|*") as archive:
                for member in archive:
                    target = (destination / member.name).resolve()
                    if not _inside(target, destination) or member.issym() or member.islnk():
                        raise ValueError("Git revision contains an unsafe workspace entry")
                    archive.extract(member, destination)
        finally:
            process.stdout.close()
        stderr = process.stderr.read().decode("utf-8", errors="replace") if process.stderr is not None else ""
        return_code = process.wait()
        if return_code != 0:
            raise RuntimeError(f"git archive failed: {stderr.strip() or return_code}")
    def prepare(self, campaign_id: str) -> WorkspaceHandle:
        # Publish an ownership-verified atomic workspace copy or exact revision for one campaign.
        workspace_root = self.state_root / "workspaces" / str(campaign_id)
        ownership_manifest = workspace_root.parent / f"{campaign_id}.ownership.json"
        lock_path = workspace_root.parent / f"{campaign_id}.prepare.lock"
        self.reports_root.mkdir(parents=True, exist_ok=True)
        self.state_root.mkdir(parents=True, exist_ok=True)
        self.knowledge_root.mkdir(parents=True, exist_ok=True)
        workspace_root.parent.mkdir(parents=True, exist_ok=True)
        with FileLock(lock_path, {"campaign_id": str(campaign_id), "main_root": str(self.main_root)}):
            source_fingerprint = _tree_fingerprint(self.main_root)
            if workspace_root.is_symlink() or ownership_manifest.is_symlink():
                raise ValueError("workspace ownership paths must not be symlinks")
            if workspace_root.exists():
                if not ownership_manifest.is_file():
                    raise ValueError("existing workspace has no ownership manifest")
                manifest = read_json(ownership_manifest)
                if not isinstance(manifest, dict):
                    raise ValueError("workspace ownership manifest is invalid")
                if manifest.get("campaign_id") != str(campaign_id) or manifest.get("main_root") != str(self.main_root):
                    raise ValueError("workspace ownership does not match the campaign")
                if manifest.get("source_fingerprint") != source_fingerprint:
                    raise ValueError("main checkout changed since workspace creation")
                if manifest.get("workspace_fingerprint") != _tree_fingerprint(workspace_root):
                    raise ValueError("workspace integrity verification failed")
            else:
                temporary_parent = Path(tempfile.mkdtemp(prefix=f".{campaign_id}.copying-", dir=self.state_root))
                temporary_root = temporary_parent / "workspace"
                revision = self.configuration.project.revision
                try:
                    if revision is not None and revision.git_revision and not revision.dirty:
                        if not (self.main_root / ".git").exists():
                            raise ValueError("exact Git revision requested but checkout has no .git metadata")
                        self._extract_git_archive(self.main_root, revision.git_revision, temporary_root)
                        mode = "git-archive"
                    else:
                        self._copy_tree(self.main_root, temporary_root)
                        mode = "copy"
                    workspace_fingerprint = _tree_fingerprint(temporary_root)
                    if mode == "copy" and _tree_fingerprint(self.main_root) != source_fingerprint:
                        raise ValueError("main checkout changed while workspace was being copied")
                    os.replace(temporary_root, workspace_root)
                    atomic_write_json(
                        ownership_manifest,
                        {
                            "schema_version": 1,
                            "campaign_id": str(campaign_id),
                            "main_root": str(self.main_root),
                            "source_fingerprint": source_fingerprint,
                            "workspace_fingerprint": workspace_fingerprint,
                            "mode": mode,
                            "git_revision": revision.git_revision if revision is not None else None,
                            "created_at": utc_now_iso(),
                        },
                        durability="critical",
                        category="workspace_manifest",
                    )
                except BaseException:
                    if workspace_root.exists():
                        shutil.rmtree(workspace_root)
                    raise
                finally:
                    shutil.rmtree(temporary_parent, ignore_errors=True)
        if _inside(workspace_root, self.main_root):
            raise ValueError("workspace root must be outside the main checkout")
        return WorkspaceHandle(
            main_root=self.main_root,
            workspace_root=workspace_root.resolve(),
            state_root=self.state_root.resolve(),
            reports_root=self.reports_root.resolve(),
            knowledge_root=self.knowledge_root.resolve(),
            ownership_manifest=ownership_manifest.resolve(),
        )
__all__ = ["WorkspaceHandle", "WorkspaceProvider", "cleanup_campaign_workspaces", "project_tree_logical_bytes"]
