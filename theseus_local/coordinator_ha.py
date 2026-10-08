"""Single-active coordinator authority with durable fencing epochs."""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from theseus_contracts.serialization import dumps, utc_now
from .locking import InterProcessFileLock, InterProcessLockError


COORDINATOR_AUTHORITY_SCHEMA_VERSION = 1


class LeadershipError(RuntimeError):
    """Raised when a coordinator is not the current authoritative leader."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _expires(seconds: float) -> str:
    return (_now() + timedelta(seconds=max(0.1, float(seconds)))).isoformat().replace("+00:00", "Z")


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


@dataclass(frozen=True, slots=True)
class LeadershipRecord:
    leader_id: str
    epoch: int
    expires_at: str
    acquired_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "leader_id": self.leader_id,
            "epoch": int(self.epoch),
            "expires_at": self.expires_at,
            "acquired_at": self.acquired_at,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "LeadershipRecord":
        leader_id = value.get("leader_id")
        epoch = value.get("epoch")
        expires_at = value.get("expires_at")
        acquired_at = value.get("acquired_at")
        if not isinstance(leader_id, str) or not leader_id.strip() or isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 1:
            raise LeadershipError("leadership record identity is invalid")
        if not isinstance(expires_at, str) or not isinstance(acquired_at, str):
            raise LeadershipError("leadership record timestamps are invalid")
        _parse(expires_at)
        _parse(acquired_at)
        return cls(leader_id, epoch, expires_at, acquired_at)


class CoordinatorAuthority:
    """A lock-protected durable lease for one active coordinator."""

    def __init__(self, state_path: Path, *, lease_seconds: float = 15.0) -> None:
        if float(lease_seconds) <= 0:
            raise ValueError("leadership lease_seconds must be positive")
        self.state_path = Path(state_path).resolve()
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.state_path.with_name(f".{self.state_path.name}.lock")
        self.lease_seconds = float(lease_seconds)

    def _load(self) -> tuple[int, LeadershipRecord | None]:
        if not self.state_path.is_file():
            return 0, None
        raw = json.loads(self.state_path.read_text(encoding="utf-8"))
        if not isinstance(raw, Mapping) or raw.get("schema_version") != COORDINATOR_AUTHORITY_SCHEMA_VERSION:
            raise LeadershipError("coordinator authority state schema is unsupported")
        epoch = raw.get("epoch", 0)
        current = raw.get("leader")
        if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 0:
            raise LeadershipError("coordinator authority epoch is invalid")
        return epoch, LeadershipRecord.from_dict(current) if isinstance(current, Mapping) else None

    def _save(self, epoch: int, leader: LeadershipRecord | None) -> None:
        temporary = self.state_path.with_name(f".{self.state_path.name}.{uuid.uuid4().hex}.tmp")
        try:
            temporary.write_text(
                dumps(
                    {
                        "schema_version": COORDINATOR_AUTHORITY_SCHEMA_VERSION,
                        "epoch": int(epoch),
                        "leader": leader.to_dict() if leader else None,
                    }
                )
                + "\n",
                encoding="utf-8",
                newline="\n",
            )
            os.replace(temporary, self.state_path)
        finally:
            temporary.unlink(missing_ok=True)

    def current(self) -> LeadershipRecord | None:
        try:
            with InterProcessFileLock(self.lock_path):
                return self._load()[1]
        except InterProcessLockError as exc:
            raise LeadershipError("coordinator authority lock failed") from exc
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise LeadershipError("coordinator authority state is unavailable or corrupt") from exc

    def acquire(self, leader_id: str) -> LeadershipRecord:
        identifier = str(leader_id).strip()
        if not identifier:
            raise ValueError("leader_id must be non-empty")
        try:
            with InterProcessFileLock(self.lock_path):
                epoch, current = self._load()
                if current is not None and _parse(current.expires_at) > _now() and current.leader_id != identifier:
                    raise LeadershipError(f"coordinator authority is held by {current.leader_id}")
                next_epoch = max(epoch, current.epoch if current else 0) + 1
                record = LeadershipRecord(identifier, next_epoch, _expires(self.lease_seconds), utc_now())
                self._save(next_epoch, record)
                return record
        except InterProcessLockError as exc:
            raise LeadershipError("coordinator authority lock failed") from exc
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise LeadershipError("coordinator authority state is unavailable or corrupt") from exc

    def renew(self, leader_id: str, epoch: int) -> LeadershipRecord:
        try:
            with InterProcessFileLock(self.lock_path):
                last_epoch, current = self._load()
                if current is None or current.leader_id != str(leader_id) or current.epoch != int(epoch):
                    raise LeadershipError("coordinator leadership epoch is fenced")
                record = LeadershipRecord(current.leader_id, current.epoch, _expires(self.lease_seconds), current.acquired_at)
                self._save(max(last_epoch, record.epoch), record)
                return record
        except InterProcessLockError as exc:
            raise LeadershipError("coordinator authority lock failed") from exc
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise LeadershipError("coordinator authority state is unavailable or corrupt") from exc

    def assert_leader(self, leader_id: str, epoch: int) -> None:
        current = self.current()
        if current is None or current.leader_id != str(leader_id) or current.epoch != int(epoch) or _parse(current.expires_at) <= _now():
            raise LeadershipError("coordinator is not the active authority")

    def release(self, leader_id: str, epoch: int) -> None:
        try:
            with InterProcessFileLock(self.lock_path):
                last_epoch, current = self._load()
                if current is None:
                    return
                if current.leader_id != str(leader_id) or current.epoch != int(epoch):
                    raise LeadershipError("coordinator leadership epoch is fenced")
                self._save(last_epoch, None)
        except InterProcessLockError as exc:
            raise LeadershipError("coordinator authority lock failed") from exc
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise LeadershipError("coordinator authority state is unavailable or corrupt") from exc


__all__ = [
    "COORDINATOR_AUTHORITY_SCHEMA_VERSION",
    "CoordinatorAuthority",
    "LeadershipError",
    "LeadershipRecord",
]
