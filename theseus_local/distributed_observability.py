"""Non-authoritative distributed timeline and metrics projections."""

from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from typing import Any, Mapping, Protocol

from theseus_contracts.serialization import dumps, utc_now


class ObservabilitySink(Protocol):
    def publish(self, event: Mapping[str, Any]) -> None: ...


class NullObservabilitySink:
    def publish(self, event: Mapping[str, Any]) -> None:
        return None


class JsonlObservabilitySink:
    """Best-effort durable diagnostics sink; failures never become campaign authority."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()

    def publish(self, event: Mapping[str, Any]) -> None:
        line = dumps(dict(event)) + "\n"
        with self._lock:
            with self.path.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(line)


@dataclass(frozen=True, slots=True)
class CorrelationIds:
    campaign_id: str
    evidence_identity: str | None = None
    attempt_identity: str | None = None
    worker_id: str | None = None
    worker_session_id: str | None = None
    lease_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "evidence_identity": self.evidence_identity,
            "attempt_identity": self.attempt_identity,
            "worker_id": self.worker_id,
            "worker_session_id": self.worker_session_id,
            "lease_id": self.lease_id,
        }


@dataclass(frozen=True, slots=True)
class TimelineEvent:
    event_type: str
    timestamp: str
    monotonic_seconds: float
    correlation: CorrelationIds
    payload: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_type": self.event_type,
            "timestamp": self.timestamp,
            "monotonic_seconds": float(self.monotonic_seconds),
            "correlation": self.correlation.to_dict(),
            "payload": dict(self.payload),
        }


@dataclass(frozen=True, slots=True)
class DistributedMetrics:
    counters: Mapping[str, int]
    timings: Mapping[str, float]
    bytes_transferred: int
    cache_hits: int
    cache_misses: int
    active_workers: int
    available_slots: int
    active_leases: int
    stale_results: int
    retries: int
    round_trips: int = 0
    transfer_wall_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "counters": {key: int(value) for key, value in sorted(self.counters.items())},
            "timings": {key: float(value) for key, value in sorted(self.timings.items())},
            "bytes_transferred": int(self.bytes_transferred),
            "cache_hits": int(self.cache_hits),
            "cache_misses": int(self.cache_misses),
            "active_workers": int(self.active_workers),
            "available_slots": int(self.available_slots),
            "active_leases": int(self.active_leases),
            "stale_results": int(self.stale_results),
            "retries": int(self.retries),
            "round_trips": int(self.round_trips),
            "transfer_wall_seconds": float(self.transfer_wall_seconds),
        }


class DistributedObservability:
    """Collect reconstructable phases while swallowing sink failures."""

    def __init__(self, *, sink: ObservabilitySink | None = None) -> None:
        self.sink = sink or NullObservabilitySink()
        self._events: list[TimelineEvent] = []
        self._counters: Counter[str] = Counter()
        self._timings: dict[str, float] = {}
        self._started = time.monotonic()
        self._lock = RLock()

    def record(
        self,
        event_type: str,
        correlation: CorrelationIds,
        payload: Mapping[str, Any] | None = None,
    ) -> TimelineEvent:
        event = TimelineEvent(str(event_type), utc_now(), time.monotonic(), correlation, dict(payload or {}))
        with self._lock:
            self._events.append(event)
            self._counters[str(event_type)] += 1
        try:
            self.sink.publish(event.to_dict())
        except Exception:
            # Observability is explicitly non-authoritative.
            self._counters["observability_sink_errors"] += 1
        return event

    def observe_timing(self, name: str, seconds: float) -> None:
        with self._lock:
            self._timings[str(name)] = self._timings.get(str(name), 0.0) + max(0.0, float(seconds))

    def snapshot(
        self,
        *,
        bytes_transferred: int = 0,
        cache_hits: int = 0,
        cache_misses: int = 0,
        active_workers: int = 0,
        available_slots: int = 0,
        active_leases: int = 0,
        stale_results: int = 0,
        retries: int = 0,
        round_trips: int = 0,
        transfer_wall_seconds: float = 0.0,
    ) -> DistributedMetrics:
        with self._lock:
            return DistributedMetrics(
                counters=dict(self._counters),
                timings=dict(self._timings) | {"wall_seconds": max(0.0, time.monotonic() - self._started)},
                bytes_transferred=max(0, int(bytes_transferred)),
                cache_hits=max(0, int(cache_hits)),
                cache_misses=max(0, int(cache_misses)),
                active_workers=max(0, int(active_workers)),
                available_slots=max(0, int(available_slots)),
                active_leases=max(0, int(active_leases)),
                stale_results=max(0, int(stale_results)),
                retries=max(0, int(retries)),
                round_trips=max(0, int(round_trips)),
                transfer_wall_seconds=max(0.0, float(transfer_wall_seconds)),
            )

    def events(self) -> tuple[TimelineEvent, ...]:
        with self._lock:
            return tuple(self._events)


__all__ = [
    "CorrelationIds",
    "DistributedMetrics",
    "DistributedObservability",
    "JsonlObservabilitySink",
    "NullObservabilitySink",
    "ObservabilitySink",
    "TimelineEvent",
]
