"""Small cross-process file locks used by durable local state stores."""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any


class InterProcessLockError(RuntimeError):
    """Raised when a durable state lock cannot be acquired in time."""


class InterProcessFileLock:
    """Portable advisory lock for one state root shared by multiple processes."""

    def __init__(self, path: Path, *, timeout_seconds: float = 30.0) -> None:
        self.path = Path(path)
        self.timeout_seconds = max(0.1, float(timeout_seconds))
        self._handle: Any | None = None

    def __enter__(self) -> "InterProcessFileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        handle.seek(0, 2)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        deadline = time.monotonic() + self.timeout_seconds
        try:
            while True:
                try:
                    handle.seek(0)
                    if os.name == "nt":
                        import msvcrt

                        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    else:
                        import fcntl

                        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self._handle = handle
                    return self
                except (BlockingIOError, OSError) as exc:
                    if time.monotonic() >= deadline:
                        raise InterProcessLockError(f"timed out acquiring state lock: {self.path}") from exc
                    time.sleep(0.01)
        except BaseException:
            handle.close()
            raise

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        handle = self._handle
        self._handle = None
        if handle is None:
            return
        try:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


__all__ = ["InterProcessFileLock", "InterProcessLockError"]
