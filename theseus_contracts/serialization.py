"""Dependency-free JSON wire helpers for Theseus contracts."""
from __future__ import annotations
import json
from dataclasses import fields, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping
class SerializationError(ValueError):
    """Raised when a value cannot cross the Theseus JSON boundary."""
def to_json_value(value: Any) -> Any:
    # Convert only explicitly supported values so private runtime objects cannot leak onto the wire.
    if isinstance(value, Enum):
        return to_json_value(value.value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "to_wire_value"):
        return to_json_value(value.to_wire_value())
    if is_dataclass(value):
        return {
            item.name: to_json_value(getattr(value, item.name))
            for item in fields(value)
        }
    if isinstance(value, Mapping):
        return {str(key): to_json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [to_json_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        # Set iteration order is process-dependent; canonicalize after recursively
        # converting each item so snapshots and journal hashes cannot drift.
        converted = [to_json_value(item) for item in value]
        return sorted(
            converted,
            key=lambda item: json.dumps(
                item,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
    raise SerializationError(f"unsupported wire value: {type(value).__name__}")
def dumps(value: Any) -> str:
    # Produce deterministic compact JSON suitable for snapshots and event journals.
    try:
        return json.dumps(
            to_json_value(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise SerializationError(str(exc)) from exc
def loads_object(raw: str | bytes) -> dict[str, Any]:
    # Decode one JSON object and reject arrays or scalar values at the message boundary.
    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise SerializationError(f"duplicate wire field: {key}")
            result[key] = item
        return result

    try:
        value = json.loads(raw, object_pairs_hook=reject_duplicate_keys)
    except (TypeError, ValueError) as exc:
        raise SerializationError(f"invalid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise SerializationError("wire message must be a JSON object")
    return value
def require_mapping(value: Any, *, field_name: str) -> Mapping[str, Any]:
    # Validate nested DTO payloads without imposing a storage implementation.
    if not isinstance(value, Mapping):
        raise SerializationError(f"{field_name} must be an object")
    return value
def required_string(value: Mapping[str, Any], field_name: str) -> str:
    # Read a non-empty string field and give callers a stable validation error.
    current = value.get(field_name)
    if not isinstance(current, str) or not current.strip():
        raise SerializationError(f"{field_name} must be a non-empty string")
    return current
def required_integer(
    value: Mapping[str, Any],
    field_name: str,
    *,
    minimum: int | None = None,
) -> int:
    # Read one required integer while rejecting booleans and values below the contract minimum.
    current = value.get(field_name)
    if isinstance(current, bool) or not isinstance(current, int):
        raise SerializationError(f"{field_name} must be an integer")
    if minimum is not None and current < minimum:
        raise SerializationError(f"{field_name} must be at least {minimum}")
    return current
def optional_string(value: Mapping[str, Any], field_name: str) -> str | None:
    # Read optional strings while treating explicit null as absent.
    current = value.get(field_name)
    if current is None:
        return None
    if not isinstance(current, str):
        raise SerializationError(f"{field_name} must be a string or null")
    return current
def sequence_of_strings(value: Mapping[str, Any], field_name: str) -> tuple[str, ...]:
    # Normalize repeated wire fields into immutable strings and ignore no malformed items.
    current = value.get(field_name, [])
    if not isinstance(current, (list, tuple)):
        raise SerializationError(f"{field_name} must be an array")
    if any(not isinstance(item, str) for item in current):
        raise SerializationError(f"{field_name} must contain strings")
    return tuple(current)
def utc_now() -> str:
    # Return the single UTC timestamp representation used by all public messages.
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
def validate_utc_timestamp(value: str, *, field_name: str = "created_at") -> str:
    # Reject naive timestamps so event ordering never depends on a local timezone.
    if not isinstance(value, str) or not value.strip():
        raise SerializationError(f"{field_name} must be a non-empty UTC timestamp")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise SerializationError(f"{field_name} must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SerializationError(f"{field_name} must include a UTC offset")
    if parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise SerializationError(f"{field_name} must be UTC")
    return value
class WireModel:
    """Mixin for immutable DTOs with JSON-compatible field serialization."""
    def to_dict(self) -> dict[str, Any]:
        # Serialize dataclass fields recursively while preserving unknown future payload keys elsewhere.
        if not is_dataclass(self):
            raise SerializationError("WireModel must be combined with a dataclass")
        return {
            item.name: to_json_value(getattr(self, item.name))
            for item in fields(self)
        }
