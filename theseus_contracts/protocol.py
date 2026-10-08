"""Versioned message envelope used by the Theseus local protocol."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Mapping
from .errors import IncompatibleProtocolError, MissingFieldError, UnsupportedSchemaError
from .serialization import (
    SerializationError,
    dumps,
    loads_object,
    require_mapping,
    required_string,
    to_json_value,
    validate_utc_timestamp,
)
PROTOCOL_VERSION = 1
SCHEMA_VERSION = 1
@dataclass(frozen=True, slots=True)
class MessageEnvelope:
    """Generic forward-compatible message boundary."""
    protocol_version: int
    schema_version: int
    message_type: str
    message_id: str
    created_at: str
    payload: Mapping[str, Any]
    def __post_init__(self) -> None:
        # Validate every envelope before it can be handed to an adapter or journal.
        if isinstance(self.protocol_version, bool) or not isinstance(self.protocol_version, int):
            raise IncompatibleProtocolError("protocol_version must be an integer")
        if isinstance(self.schema_version, bool) or not isinstance(self.schema_version, int):
            raise UnsupportedSchemaError("schema_version must be an integer")
        if self.protocol_version != PROTOCOL_VERSION:
            raise IncompatibleProtocolError(
                f"unsupported protocol_version: {self.protocol_version}"
            )
        if self.schema_version < 0 or self.schema_version > SCHEMA_VERSION:
            raise UnsupportedSchemaError(
                f"unsupported schema_version: {self.schema_version}"
            )
        if not isinstance(self.message_type, str) or not isinstance(self.message_id, str):
            raise MissingFieldError("message_type and message_id must be strings")
        if not self.message_type.strip() or not self.message_id.strip():
            raise MissingFieldError("message_type and message_id are required")
        if not isinstance(self.payload, Mapping):
            raise MissingFieldError("payload must be an object")
        validate_utc_timestamp(self.created_at)
    def to_dict(self) -> dict[str, Any]:
        # Emit only JSON-compatible fields while allowing readers to ignore future additions.
        return {
            "protocol_version": self.protocol_version,
            "schema_version": self.schema_version,
            "message_type": self.message_type,
            "message_id": self.message_id,
            "created_at": self.created_at,
            "payload": to_json_value(self.payload),
        }
    def to_json(self) -> str:
        # Serialize the envelope through the canonical deterministic JSON writer.
        return dumps(self.to_dict())
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MessageEnvelope":
        # Parse known fields only so compatible readers tolerate future envelope keys.
        if not isinstance(value, Mapping):
            raise MissingFieldError("message must be an object")
        try:
            protocol_version = value["protocol_version"]
            schema_version = value["schema_version"]
            message_type = required_string(value, "message_type")
            message_id = required_string(value, "message_id")
            created_at = required_string(value, "created_at")
            payload = require_mapping(value.get("payload"), field_name="payload")
        except (KeyError, SerializationError) as exc:
            raise MissingFieldError(str(exc)) from exc
        if isinstance(protocol_version, bool) or not isinstance(protocol_version, int):
            raise IncompatibleProtocolError("protocol_version must be an integer")
        if isinstance(schema_version, bool) or not isinstance(schema_version, int):
            raise UnsupportedSchemaError("schema_version must be an integer")
        return cls(
            protocol_version=protocol_version,
            schema_version=schema_version,
            message_type=message_type,
            message_id=message_id,
            created_at=created_at,
            payload=dict(payload),
        )
    @classmethod
    def from_json(cls, raw: str | bytes) -> "MessageEnvelope":
        # Decode one complete JSON envelope without accepting scalar input.
        return cls.from_dict(loads_object(raw))
def encode_message(message: MessageEnvelope | Any) -> str:
    # Encode a contract object or mapping using the same canonical wire representation.
    if isinstance(message, MessageEnvelope):
        return message.to_json()
    if hasattr(message, "to_dict"):
        return dumps(message.to_dict())
    if isinstance(message, Mapping):
        return dumps(message)
    raise SerializationError(f"unsupported message type: {type(message).__name__}")
def decode_message(raw: str | bytes) -> MessageEnvelope | Any:
    # Decode generic messages and route specialized protocols without eager runtime imports.
    value = loads_object(raw)
    message_type = value.get("message_type")
    if message_type == "engine_event":
        from .events import EngineEvent
        return EngineEvent.from_dict(value)
    if message_type == "statistics_event":
        from .events import StatisticsEvent
        return StatisticsEvent.from_dict(value)
    if isinstance(message_type, str) and message_type.startswith("worker."):
        from .worker_protocol import WorkerProtocolFrame
        return WorkerProtocolFrame.from_dict(value)
    return MessageEnvelope.from_dict(value)
