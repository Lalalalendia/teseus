"""Copy-on-write helpers for hardlink-backed worker workspaces."""
from __future__ import annotations
import os
import shutil
import tempfile
from pathlib import Path


def _inside(path: Path, root: Path) -> bool:
    # Check resolved containment before any copy-up touches a worker-owned path.
    resolved = Path(path).resolve()
    base = Path(root).resolve()
    return resolved == base or base in resolved.parents


def copy_up_hardlink(path: Path, *, root: Path | None = None) -> bool:
    # Replace one multiply-linked regular file with a private byte-for-byte copy in the same directory.
    target = Path(path).resolve()
    if root is not None and not _inside(target, root):
        raise ValueError("copy-up target escapes the worker workspace")
    try:
        stat = target.stat()
    except FileNotFoundError:
        return False
    if not target.is_file() or int(getattr(stat, "st_nlink", 1)) <= 1:
        return False
    fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.theseus-cow-", suffix=".tmp", dir=target.parent)
    os.close(fd)
    temporary = Path(temp_name)
    try:
        shutil.copy2(target, temporary)
        os.replace(temporary, target)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return True


def privatize_hardlinked_tree(root: Path) -> int:
    # Copy up every multiply-linked regular file before an uninstrumented child process can mutate the workspace.
    base = Path(root).resolve()
    copied = 0
    pending = [base]
    while pending:
        directory = pending.pop()
        for entry in os.scandir(directory):
            path = Path(entry.path)
            if entry.is_dir(follow_symlinks=False):
                pending.append(path)
            elif entry.is_file(follow_symlinks=False) and copy_up_hardlink(path, root=base):
                copied += 1
    return copied
