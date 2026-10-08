"""Certified warm-execution policy with fresh-process audit and quarantine."""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from threading import RLock
from typing import Callable, Generic, Mapping, TypeVar
from uuid import uuid4

T = TypeVar("T")
WARM_EXECUTOR_SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class WarmExecutionKey:
    """Exact semantic boundary within which warm execution may earn trust."""

    project_fingerprint: str
    environment_fingerprint: str
    test_selection_fingerprint: str
    executor_version: str

    @property
    def key(self) -> str:
        # Hash one canonical warm-execution trust boundary without leaking raw identities to filenames.
        payload = json.dumps(asdict(self), sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True, slots=True)
class WarmTrustState:
    """Bounded certification evidence for one exact warm execution key."""

    audits: int = 0
    matches: int = 0
    mismatches: int = 0
    warm_seconds: float = 0.0
    fresh_seconds: float = 0.0
    quarantined: bool = False

    @property
    def certified(self) -> bool:
        # Require repeated exact matches and a measured speed benefit before warm-only authority is allowed.
        return not self.quarantined and self.mismatches == 0 and self.matches >= 3 and self.warm_seconds < self.fresh_seconds


class WarmExecutionTrustStore:
    """Atomic JSON authority for warm/fresh equivalence audits."""

    def __init__(self, path: str | Path) -> None:
        # Bind certification evidence to one private file outside target project sources.
        self.path = Path(path).expanduser().resolve()
        self._lock = RLock()

    def _load(self) -> dict[str, dict[str, object]]:
        # Read only the current schema and ignore corrupted state rather than trusting it.
        if not self.path.is_file():
            return {}
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            return {}
        if not isinstance(raw, Mapping) or raw.get("schema_version") != WARM_EXECUTOR_SCHEMA_VERSION:
            return {}
        values = raw.get("keys", {})
        return {str(key): dict(value) for key, value in values.items() if isinstance(value, Mapping)} if isinstance(values, Mapping) else {}

    def _save(self, values: Mapping[str, Mapping[str, object]]) -> None:
        # Persist one deterministic trust snapshot through atomic replacement.
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"schema_version": WARM_EXECUTOR_SCHEMA_VERSION, "keys": {key: dict(value) for key, value in sorted(values.items())}}
        temporary = self.path.with_name(f".{self.path.name}.{os.getpid()}-{uuid4().hex}.tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        os.replace(temporary, self.path)

    def state(self, key: WarmExecutionKey) -> WarmTrustState:
        # Restore bounded numeric trust evidence for one exact semantic key.
        with self._lock:
            raw = self._load().get(key.key, {})
        try:
            return WarmTrustState(
                audits=max(0, int(raw.get("audits", 0))),
                matches=max(0, int(raw.get("matches", 0))),
                mismatches=max(0, int(raw.get("mismatches", 0))),
                warm_seconds=max(0.0, float(raw.get("warm_seconds", 0.0))),
                fresh_seconds=max(0.0, float(raw.get("fresh_seconds", 0.0))),
                quarantined=raw.get("quarantined") is True,
            )
        except (TypeError, ValueError):
            return WarmTrustState(quarantined=True)

    def record_audit(self, key: WarmExecutionKey, *, matched: bool, warm_seconds: float, fresh_seconds: float) -> WarmTrustState:
        # Record one warm/fresh comparison and permanently quarantine the key after any semantic mismatch.
        if warm_seconds < 0.0 or fresh_seconds < 0.0:
            raise ValueError("audit durations must be non-negative")
        current = self.state(key)
        updated = WarmTrustState(
            audits=current.audits + 1,
            matches=current.matches + int(matched),
            mismatches=current.mismatches + int(not matched),
            warm_seconds=current.warm_seconds + float(warm_seconds),
            fresh_seconds=current.fresh_seconds + float(fresh_seconds),
            quarantined=current.quarantined or not matched,
        )
        with self._lock:
            values = self._load()
            values[key.key] = asdict(updated)
            self._save(values)
        return updated


@dataclass(frozen=True, slots=True)
class CertifiedExecutionResult(Generic[T]):
    """Authoritative result plus audit disposition for one warm-execution attempt."""

    result: T
    mode: str
    audited: bool
    matched: bool | None
    trust: WarmTrustState


def _sample(execution_id: str, key: WarmExecutionKey, rate: float) -> bool:
    # Select deterministic post-certification audits without process-local random state.
    bounded = max(0.0, min(1.0, float(rate)))
    if bounded <= 0.0:
        return False
    digest = hashlib.sha256(f"{execution_id}|{key.key}|warm-audit-v1".encode("utf-8")).digest()
    value = int.from_bytes(digest[:8], "big") / float(2**64 - 1)
    return value < bounded


class CertifiedWarmExecutor(Generic[T]):
    """Gate a caller-supplied warm backend behind fresh-process equivalence evidence."""

    def __init__(self, store: WarmExecutionTrustStore, *, audit_rate: float = 0.05) -> None:
        # Retain one trust authority and deterministic audit rate without owning execution semantics.
        self.store = store
        self.audit_rate = max(0.0, min(1.0, float(audit_rate)))

    def execute(
        self,
        execution_id: str,
        key: WarmExecutionKey,
        *,
        warm_call: Callable[[], tuple[T, float]],
        fresh_call: Callable[[], tuple[T, float]],
        result_digest: Callable[[T], str],
    ) -> CertifiedExecutionResult[T]:
        # Use warm-only execution only after certification and return fresh authority on every audited mismatch.
        trust = self.store.state(key)
        must_audit = not trust.certified or _sample(execution_id, key, self.audit_rate)
        if trust.quarantined:
            fresh, _fresh_seconds = fresh_call()
            return CertifiedExecutionResult(fresh, "fresh_quarantined", False, None, trust)
        if must_audit:
            warm, warm_seconds = warm_call()
            fresh, fresh_seconds = fresh_call()
            matched = result_digest(warm) == result_digest(fresh)
            trust = self.store.record_audit(key, matched=matched, warm_seconds=warm_seconds, fresh_seconds=fresh_seconds)
            return CertifiedExecutionResult(fresh, "fresh_audit", True, matched, trust)
        warm, _warm_seconds = warm_call()
        return CertifiedExecutionResult(warm, "warm_certified", False, None, trust)


__all__ = [
    "CertifiedExecutionResult",
    "CertifiedWarmExecutor",
    "WarmExecutionKey",
    "WarmExecutionTrustStore",
    "WarmTrustState",
    "WARM_EXECUTOR_SCHEMA_VERSION",
]
