"""Crash-safe delivery spool for coordinator-owned worker evidence."""
from __future__ import annotations
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Mapping, Sequence
from test_intelligence_unified_v1.io_utils import append_mutant_spool_frame, build_mutant_spool_frame, stable_hash
class SpoolError(RuntimeError):
    """Raised when a worker delivery journal contains conflicting identity data."""
@dataclass(frozen=True, slots=True)
class SpoolEntry:
    """One idempotent worker delivery pending coordinator acknowledgement."""
    event_id: str
    payload_sha256: str
    payload: Mapping[str, Any]
    def to_dict(self) -> dict[str, Any]:
        # Serialize a detached spool entry for recovery and diagnostics.
        return {
            "event_id": self.event_id,
            "payload_sha256": self.payload_sha256,
            "payload": dict(self.payload),
        }
@dataclass(frozen=True, slots=True)
class SpoolInspection:
    """Read-only bounded spool state used by recovery diagnostics."""
    entries: tuple[SpoolEntry, ...]
    pending_count: int
    acknowledged_count: int
    quarantined_count: int
    next_event_id: str | None = None
    oldest_pending_at: str | None = None
class DurableExecutionSpool:
    """Append-only shard and per-mutant journals with fsync-before-return semantics."""
    def __init__(self, root: Path) -> None:
        # Keep raw worker evidence outside the mutable checkout and initialize every journal eagerly.
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.events_path = self.root / "events.jsonl"
        self.acks_path = self.root / "acks.jsonl"
        self.quarantine_path = self.root / "quarantine.jsonl"
        self.mutant_events_path = self.root / "mutant-events.jsonl"
        self.mutant_states_path = self.root / "mutant-states.jsonl"
        self._lock = RLock()
        for path in (
            self.events_path,
            self.acks_path,
            self.quarantine_path,
            self.mutant_events_path,
            self.mutant_states_path,
        ):
            path.touch(exist_ok=True)
    @classmethod
    def inspect_existing(
        cls,
        root: Path,
        *,
        limit: int = 100,
        after_event_id: str | None = None,
    ) -> SpoolInspection:
        # Inspect existing journals without creating directories or touching absent spool files.
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        resolved = Path(root)
        if not resolved.is_dir():
            return SpoolInspection((), 0, 0, 0, None)
        spool = object.__new__(cls)
        spool.root = resolved
        spool.events_path = resolved / "events.jsonl"
        spool.acks_path = resolved / "acks.jsonl"
        spool.quarantine_path = resolved / "quarantine.jsonl"
        spool.mutant_events_path = resolved / "mutant-events.jsonl"
        spool.mutant_states_path = resolved / "mutant-states.jsonl"
        spool._lock = RLock()
        required = (spool.events_path, spool.acks_path, spool.quarantine_path)
        if not all(path.is_file() for path in required):
            raise SpoolError("worker spool journals are incomplete")
        events, acknowledged, quarantined = spool._replay()
        event_times = spool._event_recorded_at()
        pending = tuple(events[key] for key in events if key not in acknowledged and key not in quarantined)
        start = 0
        if after_event_id is not None:
            identities = tuple(item.event_id for item in pending)
            try:
                start = identities.index(str(after_event_id)) + 1
            except ValueError as exc:
                raise ValueError("spool cursor does not match pending delivery state") from exc
        selected = pending[start:start + limit]
        has_more = start + len(selected) < len(pending)
        return SpoolInspection(
            entries=selected,
            pending_count=len(pending),
            acknowledged_count=len(acknowledged),
            quarantined_count=len(quarantined),
            next_event_id=selected[-1].event_id if has_more and selected else None,
            oldest_pending_at=min(
                (event_times[item.event_id] for item in pending if item.event_id in event_times),
                default=None,
            ),
        )
    def _event_recorded_at(self) -> dict[str, str]:
        # Read optional event timestamps without changing the immutable delivery identity.
        values: dict[str, str] = {}
        for frame in self._read_frames(self.events_path, label="worker spool event"):
            event_id = str(frame.get("event_id", ""))
            recorded_at = str(frame.get("recorded_at", ""))
            if event_id and recorded_at:
                values[event_id] = recorded_at
        return values
    @staticmethod
    def _line(payload: Mapping[str, Any]) -> bytes:
        # Encode one canonical journal frame so replay is independent of mapping insertion order.
        return (
            json.dumps(dict(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            + "\n"
        ).encode("utf-8")
    @staticmethod
    def _append(path: Path, payload: Mapping[str, Any]) -> None:
        # Flush and fsync each ownership transition before exposing it to another process.
        with path.open("ab") as handle:
            handle.write(DurableExecutionSpool._line(payload))
            handle.flush()
            os.fsync(handle.fileno())
    @staticmethod
    def _read_frames(path: Path, *, label: str) -> tuple[Mapping[str, Any], ...]:
        # Replay complete JSONL records and ignore only a torn final write after a process crash.
        try:
            raw = path.read_bytes()
        except OSError as exc:
            raise SpoolError(f"cannot read {label}: {exc}") from exc
        frames: list[Mapping[str, Any]] = []
        lines = raw.splitlines(keepends=True)
        for index, line in enumerate(lines):
            complete = line.endswith((b"\n", b"\r"))
            payload = line.rstrip(b"\r\n")
            if not payload:
                continue
            try:
                value = json.loads(payload.decode("utf-8"))
            except (UnicodeDecodeError, TypeError, ValueError) as exc:
                if index == len(lines) - 1 and not complete:
                    break
                raise SpoolError(f"invalid {label} frame: {exc}") from exc
            if not isinstance(value, Mapping):
                raise SpoolError(f"{label} frame is not an object")
            frames.append(dict(value))
        return tuple(frames)
    def _replay(self) -> tuple[dict[str, SpoolEntry], set[str], set[str]]:
        # Replay shard delivery journals and reject malformed or conflicting event identities.
        events: dict[str, SpoolEntry] = {}
        acknowledged: set[str] = set()
        quarantined: set[str] = set()
        for value in self._read_frames(self.events_path, label="worker spool event"):
            if value.get("kind") != "event":
                raise SpoolError("worker spool event frame is invalid")
            event_id = str(value.get("event_id", ""))
            payload = value.get("payload")
            payload_sha256 = str(value.get("payload_sha256", ""))
            if not event_id or not isinstance(payload, Mapping) or not payload_sha256:
                raise SpoolError("worker spool event identity is incomplete")
            if stable_hash(payload) != payload_sha256:
                raise SpoolError(f"worker spool payload hash mismatch: {event_id}")
            entry = SpoolEntry(event_id, payload_sha256, dict(payload))
            previous = events.get(event_id)
            if previous is not None and previous != entry:
                raise SpoolError(f"worker spool event conflict: {event_id}")
            events[event_id] = entry
        for value in self._read_frames(self.acks_path, label="worker spool ack"):
            if value.get("kind") != "ack" or not value.get("event_id"):
                raise SpoolError("worker spool ack frame is invalid")
            acknowledged.add(str(value["event_id"]))
        for value in self._read_frames(self.quarantine_path, label="worker spool quarantine"):
            if value.get("kind") != "quarantine" or not value.get("event_id"):
                raise SpoolError("worker spool quarantine frame is invalid")
            quarantined.add(str(value["event_id"]))
        return events, acknowledged, quarantined
    @staticmethod
    def _mutant_identity(frame: Mapping[str, Any]) -> tuple[str, str, str, int, str, str]:
        # Return the immutable generation identity used to reject conflicting duplicate results.
        return (
            str(frame.get("campaign_id", "")),
            str(frame.get("shard_id", "")),
            str(frame.get("lease_id", "")),
            DurableExecutionSpool._attempt(frame.get("attempt", 0), label="per-mutant event"),
            str(frame.get("mutant_id", "")),
            str(frame.get("execution_id", "")),
        )
    @staticmethod
    def _attempt(value: Any, *, label: str) -> int:
        # Parse one lease generation without silently normalizing corrupt negative identities.
        try:
            attempt = int(value)
        except (TypeError, ValueError) as exc:
            raise SpoolError(f"{label} attempt must be an integer: value={value!r}") from exc
        if attempt < 0:
            raise SpoolError(f"{label} attempt must not be negative: value={attempt}")
        return attempt
    @classmethod
    def _validate_mutant_event_frame(cls, raw: Mapping[str, Any]) -> dict[str, Any]:
        # Verify top-level, nested payload, worker ownership, and deterministic event identity together.
        if raw.get("kind") != "mutant_event" or int(raw.get("schema_version", 0)) != 2:
            raise SpoolError("per-mutant worker event frame has an unsupported kind or schema")
        frame = dict(raw)
        event_id = str(frame.get("event_id", ""))
        payload = frame.get("payload")
        payload_sha256 = str(frame.get("payload_sha256", ""))
        if not event_id or not isinstance(payload, Mapping) or not payload_sha256:
            raise SpoolError("per-mutant worker event identity is incomplete")
        if stable_hash(payload) != payload_sha256:
            raise SpoolError(f"per-mutant worker payload hash mismatch: {event_id}")
        assignment = payload.get("assignment")
        worker = payload.get("worker")
        result = payload.get("mutant_result")
        if not isinstance(assignment, Mapping) or not isinstance(worker, Mapping) or not isinstance(result, Mapping):
            raise SpoolError(
                f"per-mutant worker event payload is incomplete: event_id={event_id}"
            )
        top_attempt = cls._attempt(frame.get("attempt", 0), label="per-mutant event")
        nested_attempt = cls._attempt(assignment.get("attempt", 0), label="per-mutant payload assignment")
        result_attempt = cls._attempt(result.get("attempt", -1), label="per-mutant result")
        identity_pairs = (
            ("campaign_id", str(frame.get("campaign_id", "")), str(assignment.get("campaign_id", ""))),
            ("shard_id", str(frame.get("shard_id", "")), str(assignment.get("shard_id", ""))),
            ("lease_id", str(frame.get("lease_id", "")), str(assignment.get("lease_id", ""))),
            ("worker_id", str(frame.get("worker_id", "")), str(worker.get("worker_id", ""))),
            (
                "worker_instance_id",
                str(frame.get("worker_instance_id", "")),
                str(worker.get("instance_id", "")),
            ),
            ("mutant_id", str(frame.get("mutant_id", "")), str(result.get("mutant_id", ""))),
            (
                "execution_id",
                str(frame.get("execution_id", "")),
                str(result.get("execution_id", "")),
            ),
            ("source_sha256", str(frame.get("source_sha256", "")), str(payload.get("source_sha256", ""))),
        )
        conflicts = tuple(
            f"{name}:top={top!r},nested={nested!r}"
            for name, top, nested in identity_pairs
            if not top or top != nested
        )
        top_source_event_id = str(frame.get("source_event_id") or "")
        nested_source_event_id = str(payload.get("source_event_id") or "")
        if top_source_event_id != nested_source_event_id:
            conflicts += (
                "source_event_id:"
                f"top={top_source_event_id!r},nested={nested_source_event_id!r}",
            )
        result_lease_id = str(result.get("lease_id", ""))
        assignment_lease_id = str(assignment.get("lease_id", ""))
        if result_lease_id != assignment_lease_id:
            conflicts += (
                f"result_lease_id:assignment={assignment_lease_id!r},result={result_lease_id!r}",
            )
        if top_attempt != nested_attempt or result_attempt != nested_attempt:
            conflicts += (
                "attempt:"
                f"top={top_attempt},assignment={nested_attempt},result={result_attempt}",
            )
        try:
            worker_process_id = int(worker.get("process_id", 0))
        except (TypeError, ValueError) as exc:
            raise SpoolError(
                f"per-mutant worker process_id is invalid: event_id={event_id}; value={worker.get('process_id')!r}"
            ) from exc
        worker_birth_token = str(worker.get("process_birth_token", ""))
        if worker_process_id <= 0 or not worker_birth_token:
            conflicts += (
                f"worker_process:process_id={worker_process_id},birth_token={worker_birth_token!r}",
            )
        if not bool(result.get("restore_verified", False)):
            conflicts += ("restore_verified:false",)
        if conflicts:
            raise SpoolError(
                "per-mutant worker event ownership conflict: "
                f"event_id={event_id}; conflicts={conflicts}"
            )
        expected_event_id = stable_hash(
            {
                "schema_version": 2,
                "campaign_id": str(frame["campaign_id"]),
                "shard_id": str(frame["shard_id"]),
                "lease_id": str(frame["lease_id"]),
                "attempt": top_attempt,
                "worker_instance_id": str(frame["worker_instance_id"]),
                "mutant_id": str(frame["mutant_id"]),
                "execution_id": str(frame["execution_id"]),
            }
        )[:32]
        if event_id != expected_event_id:
            raise SpoolError(
                "per-mutant worker event_id conflicts with deterministic identity: "
                f"expected={expected_event_id}; actual={event_id}; "
                f"campaign={frame['campaign_id']}; shard={frame['shard_id']}; "
                f"lease={frame['lease_id']}; attempt={top_attempt}; "
                f"mutant={frame['mutant_id']}; execution={frame['execution_id']}"
            )
        return frame
    def _replay_mutant_events(self) -> dict[str, dict[str, Any]]:
        # Replay per-mutant evidence and reject hash or execution-identity conflicts.
        events: dict[str, dict[str, Any]] = {}
        identities: dict[tuple[str, str, str, int, str, str], str] = {}
        for raw in self._read_frames(self.mutant_events_path, label="per-mutant worker event"):
            frame = self._validate_mutant_event_frame(raw)
            event_id = str(frame["event_id"])
            payload_sha256 = str(frame["payload_sha256"])
            previous = events.get(event_id)
            if previous is not None and previous != frame:
                raise SpoolError(f"per-mutant worker event conflict: {event_id}")
            identity = self._mutant_identity(frame)
            prior_event_id = identities.get(identity)
            if prior_event_id is not None and prior_event_id != event_id:
                prior = events[prior_event_id]
                if prior.get("payload_sha256") != payload_sha256:
                    raise SpoolError(
                        "conflicting per-mutant execution identity: "
                        f"{identity[0]}/{identity[1]}/{identity[4]}/{identity[5]}"
                    )
            identities[identity] = event_id
            events[event_id] = frame
        return events
    def _replay_mutant_states(self) -> dict[str, tuple[dict[str, Any], ...]]:
        # Replay append-only mutant lifecycle transitions without rewriting immutable evidence.
        states: dict[str, list[dict[str, Any]]] = {}
        for raw in self._read_frames(self.mutant_states_path, label="per-mutant worker state"):
            if raw.get("kind") != "mutant_state" or not raw.get("event_id") or not raw.get("state"):
                raise SpoolError("per-mutant worker state frame is invalid")
            states.setdefault(str(raw["event_id"]), []).append(dict(raw))
        return {event_id: tuple(rows) for event_id, rows in states.items()}
    @staticmethod
    def _event_state(rows: Sequence[Mapping[str, Any]]) -> str:
        # Resolve the final append-only state while treating the immutable event itself as committed.
        return str(rows[-1].get("state", "committed")) if rows else "committed"
    def publish_mutant_result(
        self,
        *,
        assignment: Mapping[str, Any],
        worker: Mapping[str, Any],
        source_sha256: str,
        mutant_result: Mapping[str, Any],
        source_event_id: str | None = None,
    ) -> str:
        # Commit one restored mutant result before the engine is allowed to start another mutant.
        try:
            frame = build_mutant_spool_frame(
                assignment=dict(assignment),
                worker=dict(worker),
                source_sha256=str(source_sha256),
                mutant_result=dict(mutant_result),
                source_event_id=source_event_id,
            )
        except ValueError as exc:
            raise SpoolError(str(exc)) from exc
        event_id = str(frame["event_id"])
        with self._lock:
            try:
                return append_mutant_spool_frame(self.mutant_events_path, frame)
            except RuntimeError as exc:
                self._append(
                    self.mutant_states_path,
                    {
                        "kind": "mutant_state",
                        "event_id": event_id,
                        "state": "conflicted",
                        "reason": str(exc),
                    },
                )
                raise SpoolError(str(exc)) from exc
    def publish_per_mutant(self, parent_event_id: str, payload: Mapping[str, Any]) -> tuple[str, ...]:
        # Retain compatibility for fixture deliveries that do not have an engine-side commit hook.
        raw_assignment = payload.get("assignment")
        raw_worker = payload.get("worker")
        raw_result = payload.get("result")
        shard_result = raw_result.get("shard_result") if isinstance(raw_result, Mapping) else None
        if isinstance(shard_result, Mapping):
            rows = shard_result.get("results", ())
        elif isinstance(raw_result, Mapping):
            rows = raw_result.get("results", ())
        else:
            rows = ()
        if not isinstance(raw_assignment, Mapping) or not isinstance(raw_worker, Mapping):
            return ()
        if not isinstance(rows, (list, tuple)):
            return ()
        source_sha256 = str(raw_result.get("source_sha256", "fixture")) if isinstance(raw_result, Mapping) else "fixture"
        published: list[str] = []
        for row in rows:
            if not isinstance(row, Mapping) or not bool(row.get("restore_verified", False)):
                continue
            event_id = self.publish_mutant_result(
                assignment=raw_assignment,
                worker=raw_worker,
                source_sha256=source_sha256,
                mutant_result=row,
            )
            self.include_mutant_in_delivery(event_id, parent_event_id)
            published.append(event_id)
        return tuple(published)
    def mutant_events(
        self,
        *,
        parent_event_id: str | None = None,
        include_quarantined: bool = True,
    ) -> tuple[Mapping[str, Any], ...]:
        # Return immutable per-mutant evidence with optional delivery and quarantine filtering.
        with self._lock:
            events = self._replay_mutant_events()
            states = self._replay_mutant_states()
            rows: list[Mapping[str, Any]] = []
            for event_id, value in events.items():
                event_states = states.get(event_id, ())
                final_state = self._event_state(event_states)
                if not include_quarantined and final_state == "quarantined":
                    continue
                if parent_event_id is not None and not any(
                    str(item.get("delivery_event_id", "")) == str(parent_event_id)
                    for item in event_states
                    if item.get("state") in {"included_in_delivery", "acknowledged"}
                ):
                    continue
                rows.append(dict(value))
            return tuple(rows)
    def mutant_event(self, event_id: str) -> Mapping[str, Any]:
        # Load one exact immutable mutant event for typed delivery envelope reconstruction.
        with self._lock:
            event = self._replay_mutant_events().get(str(event_id))
            if event is None:
                raise SpoolError(f"unknown per-mutant worker event: {event_id}")
            return dict(event)
    def include_mutant_in_delivery(self, event_id: str, delivery_event_id: str) -> None:
        # Link committed mutant evidence to one shard delivery without mutating the immutable event.
        if not event_id or not delivery_event_id:
            raise ValueError("mutant event_id and delivery_event_id are required")
        with self._lock:
            events = self._replay_mutant_events()
            if event_id not in events:
                raise SpoolError(f"cannot include unknown per-mutant event: {event_id}")
            states = self._replay_mutant_states().get(event_id, ())
            for row in states:
                if (
                    row.get("state") == "included_in_delivery"
                    and str(row.get("delivery_event_id", "")) == str(delivery_event_id)
                ):
                    return
            self._append(
                self.mutant_states_path,
                {
                    "kind": "mutant_state",
                    "event_id": str(event_id),
                    "state": "included_in_delivery",
                    "delivery_event_id": str(delivery_event_id),
                },
            )
    def quarantine_mutant_event(self, event_id: str, *, reason: str) -> None:
        # Fence obsolete attempt evidence while retaining immutable bytes for audit and carry-forward provenance.
        if not event_id or not reason:
            raise ValueError("mutant event_id and reason are required")
        with self._lock:
            if event_id not in self._replay_mutant_events():
                raise SpoolError(f"cannot quarantine unknown per-mutant event: {event_id}")
            states = self._replay_mutant_states().get(event_id, ())
            if self._event_state(states) == "quarantined":
                return
            self._append(
                self.mutant_states_path,
                {
                    "kind": "mutant_state",
                    "event_id": str(event_id),
                    "state": "quarantined",
                    "reason": str(reason),
                },
            )
    def salvage_mutant_events(
        self,
        *,
        campaign_id: str,
        shard_id: str,
        current_attempt: int,
        source_sha256: str,
        mutant_ids: Sequence[str],
    ) -> tuple[Mapping[str, Any], ...]:
        # Select at most one latest restored result per mutant from attempts preceding the current generation.
        wanted = tuple(str(item) for item in mutant_ids)
        wanted_set = set(wanted)
        current_generation = self._attempt(current_attempt, label="current assignment")
        selected: dict[str, Mapping[str, Any]] = {}
        selected_attempts: dict[str, int] = {}
        with self._lock:
            events = self._replay_mutant_events()
            states = self._replay_mutant_states()
            for event_id, frame in events.items():
                if self._event_state(states.get(event_id, ())) == "quarantined":
                    continue
                if (
                    str(frame.get("campaign_id", "")) != str(campaign_id)
                    or str(frame.get("shard_id", "")) != str(shard_id)
                    or str(frame.get("source_sha256", "")) != str(source_sha256)
                ):
                    continue
                attempt = self._attempt(frame.get("attempt", 0), label="salvage event")
                if attempt >= current_generation:
                    continue
                mutant_id = str(frame.get("mutant_id", ""))
                if mutant_id not in wanted_set:
                    continue
                payload = frame.get("payload")
                result = payload.get("mutant_result") if isinstance(payload, Mapping) else None
                if not isinstance(result, Mapping) or not bool(result.get("restore_verified", False)):
                    self.quarantine_mutant_event(event_id, reason="mutant result is not safely restored")
                    continue
                previous_attempt = selected_attempts.get(mutant_id, -1)
                if attempt > previous_attempt:
                    selected[mutant_id] = dict(frame)
                    selected_attempts[mutant_id] = attempt
                elif attempt == previous_attempt:
                    previous = selected.get(mutant_id)
                    if previous is not None and previous.get("payload_sha256") != frame.get("payload_sha256"):
                        raise SpoolError(
                            f"conflicting salvage results for mutant {mutant_id} at attempt {attempt}"
                        )
            return tuple(selected[item] for item in wanted if item in selected)
    def bind_mutant_events(
        self,
        delivery_event_id: str,
        payload: Mapping[str, Any],
        *,
        allow_publish_missing: bool,
    ) -> tuple[str, ...]:
        # Bind current-generation durable events to the final shard delivery in result order.
        raw_assignment = payload.get("assignment")
        raw_worker = payload.get("worker")
        raw_result = payload.get("result")
        shard_result = raw_result.get("shard_result") if isinstance(raw_result, Mapping) else None
        if isinstance(shard_result, Mapping):
            rows = shard_result.get("results", ())
        elif isinstance(raw_result, Mapping):
            rows = raw_result.get("results", ())
        else:
            rows = ()
        if not isinstance(raw_assignment, Mapping) or not isinstance(raw_worker, Mapping):
            raise SpoolError("durable delivery has no assignment or worker identity")
        if not isinstance(rows, (list, tuple)):
            raise SpoolError("durable shard result rows must be an array")
        campaign_id = str(raw_assignment.get("campaign_id", ""))
        shard_id = str(raw_assignment.get("shard_id", ""))
        lease_id = str(raw_assignment.get("lease_id", ""))
        attempt = self._attempt(raw_assignment.get("attempt", 0), label="durable delivery")
        source_sha256 = str(raw_result.get("source_sha256", "")) if isinstance(raw_result, Mapping) else ""
        bound: list[str] = []
        with self._lock:
            events = self._replay_mutant_events()
            states = self._replay_mutant_states()
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                normalized_row = dict(row)
                if allow_publish_missing and "restore_verified" not in normalized_row:
                    # Legacy fixture adapters predate explicit restoration evidence.
                    # Only this compatibility path may synthesize the field.
                    normalized_row["restore_verified"] = True
                if allow_publish_missing:
                    # Bind legacy fixture rows to the current assignment before strict durable publication.
                    normalized_row.setdefault("lease_id", lease_id)
                    normalized_row.setdefault("attempt", attempt)
                if not bool(normalized_row.get("restore_verified", False)):
                    continue
                mutant_id = str(normalized_row.get("mutant_id", ""))
                execution_id = str(normalized_row.get("execution_id", ""))
                candidates = [
                    frame
                    for event_id, frame in events.items()
                    if self._event_state(states.get(event_id, ())) != "quarantined"
                    and str(frame.get("campaign_id", "")) == campaign_id
                    and str(frame.get("shard_id", "")) == shard_id
                    and str(frame.get("lease_id", "")) == lease_id
                    and self._attempt(frame.get("attempt", 0), label="bound mutant event") == attempt
                    and str(frame.get("mutant_id", "")) == mutant_id
                    and str(frame.get("execution_id", "")) == execution_id
                ]
                if len(candidates) > 1:
                    hashes = {str(item.get("payload_sha256", "")) for item in candidates}
                    if len(hashes) > 1:
                        raise SpoolError(f"conflicting current-attempt events for mutant {mutant_id}")
                if candidates:
                    event_id = str(candidates[0]["event_id"])
                    event_payload = candidates[0].get("payload")
                    stored_result = event_payload.get("mutant_result") if isinstance(event_payload, Mapping) else None
                    if (
                        not isinstance(stored_result, Mapping)
                        or dict(stored_result) != normalized_row
                    ):
                        raise SpoolError(
                            f"durable mutant result differs from shard delivery: {mutant_id}"
                        )
                elif allow_publish_missing:
                    event_id = self.publish_mutant_result(
                        assignment=raw_assignment,
                        worker=raw_worker,
                        source_sha256=source_sha256 or "fixture",
                        mutant_result=normalized_row,
                    )
                    events = self._replay_mutant_events()
                else:
                    raise SpoolError(f"engine result has no precommitted mutant event: {mutant_id}")
                self.include_mutant_in_delivery(event_id, delivery_event_id)
                bound.append(event_id)
        return tuple(bound)
    def publish(self, event_id: str, payload: Mapping[str, Any]) -> SpoolEntry:
        # Publish an idempotent shard evidence event only after its durable hash has been written.
        if not event_id or not isinstance(payload, Mapping):
            raise ValueError("event_id and mapping payload are required")
        payload_copy = dict(payload)
        payload_sha256 = stable_hash(payload_copy)
        entry = SpoolEntry(str(event_id), payload_sha256, payload_copy)
        with self._lock:
            events, _, quarantined = self._replay()
            if entry.event_id in quarantined:
                raise SpoolError(f"worker spool event was quarantined: {entry.event_id}")
            previous = events.get(entry.event_id)
            if previous is not None:
                if previous != entry:
                    raise SpoolError(f"worker spool event conflict: {entry.event_id}")
                return previous
            self._append(
                self.events_path,
                {"kind": "event", "recorded_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), **entry.to_dict()},
            )
            return entry
    def acknowledge(self, event_id: str) -> None:
        # Append shard and linked mutant acknowledgements after authoritative fan-in accepts the delivery.
        if not event_id:
            raise ValueError("event_id is required")
        with self._lock:
            events, acknowledged, quarantined = self._replay()
            if event_id not in events:
                raise SpoolError(f"cannot acknowledge unknown worker event: {event_id}")
            if event_id in quarantined:
                raise SpoolError(f"cannot acknowledge quarantined worker event: {event_id}")
            if event_id not in acknowledged:
                self._append(self.acks_path, {"kind": "ack", "event_id": str(event_id)})
            states = self._replay_mutant_states()
            for mutant_event_id, rows in states.items():
                linked = any(
                    item.get("state") in {"included_in_delivery", "acknowledged"}
                    and str(item.get("delivery_event_id", "")) == str(event_id)
                    for item in rows
                )
                already_acknowledged = any(
                    item.get("state") == "acknowledged"
                    and str(item.get("delivery_event_id", "")) == str(event_id)
                    for item in rows
                )
                if linked and not already_acknowledged:
                    self._append(
                        self.mutant_states_path,
                        {
                            "kind": "mutant_state",
                            "event_id": mutant_event_id,
                            "state": "acknowledged",
                            "delivery_event_id": str(event_id),
                        },
                    )
    def pending(self) -> tuple[SpoolEntry, ...]:
        # Return unacknowledged shard events in journal order for crash reassignment or replay.
        with self._lock:
            events, acknowledged, quarantined = self._replay()
            return tuple(events[key] for key in events if key not in acknowledged and key not in quarantined)
    def quarantine(self, event_id: str, *, reason: str) -> None:
        # Permanently exclude obsolete shard evidence without deleting its audit trail.
        if not event_id or not reason:
            raise ValueError("event_id and reason are required")
        with self._lock:
            events, acknowledged, quarantined = self._replay()
            if event_id not in events:
                raise SpoolError(f"cannot quarantine unknown worker event: {event_id}")
            if event_id in acknowledged or event_id in quarantined:
                return
            self._append(
                self.quarantine_path,
                {"kind": "quarantine", "event_id": str(event_id), "reason": str(reason)},
            )
    def quarantined(self, event_id: str) -> bool:
        # Expose stale shard-attempt classification without making quarantine reversible.
        with self._lock:
            _, _, quarantined = self._replay()
            return str(event_id) in quarantined
    def mutant_quarantined(self, event_id: str) -> bool:
        # Expose whether one immutable mutant event was fenced from direct future fan-in.
        with self._lock:
            rows = self._replay_mutant_states().get(str(event_id), ())
            return self._event_state(rows) == "quarantined"
    def acknowledged(self, event_id: str) -> bool:
        # Expose idempotent shard acknowledgement state without changing the journal.
        with self._lock:
            _, acknowledged, _ = self._replay()
            return str(event_id) in acknowledged
