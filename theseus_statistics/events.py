"""Producer-facing sinks for canonical Theseus statistics events."""
from __future__ import annotations
import os
from pathlib import Path
from threading import Lock
from typing import Protocol
from theseus_contracts.events import StatisticsEvent
from theseus_contracts.errors import ContractError
class StatisticsEventSink(Protocol):
    """Minimal append-only boundary implemented by statistics event publishers."""
    def publish(self, event: StatisticsEvent) -> None:
        # Publish one immutable canonical event without exposing a projection implementation.
        ...
class MemoryStatisticsEventSink:
    """In-memory append-only sink with replay and conflict semantics."""
    def __init__(self) -> None:
        # Keep insertion order while indexing canonical events by deterministic event ID.
        self._events: list[StatisticsEvent] = []
        self._by_id: dict[str, StatisticsEvent] = {}
    def publish(self, event: StatisticsEvent) -> None:
        # Accept exact replay and fail closed when an event ID is reused for different content.
        if not isinstance(event, StatisticsEvent):
            raise ContractError("statistics sink accepts StatisticsEvent values only")
        existing = self._by_id.get(event.event_id.value)
        if existing is not None:
            if existing.to_json() != event.to_json():
                raise ContractError("statistics_event_id_conflict")
            return
        self._by_id[event.event_id.value] = event
        self._events.append(event)
    def events(self) -> tuple[StatisticsEvent, ...]:
        # Return events in their original append order without exposing mutable storage.
        return tuple(self._events)
class JsonlStatisticsEventSink:
    """Thread-safe append-only JSONL producer sink consumed incrementally by the projection store."""
    def __init__(self, path: Path) -> None:
        # Store the journal path without creating files before the first published event.
        self.path = Path(path)
        self._lock = Lock()
    def publish(self, event: StatisticsEvent) -> None:
        # Append and fsync one complete canonical event while leaving replay handling to ingestion.
        if not isinstance(event, StatisticsEvent):
            raise ContractError("statistics sink accepts StatisticsEvent values only")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = (event.to_json() + "\n").encode("utf-8")
        with self._lock, self.path.open("ab") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
