"""JSON serialization and privacy filtering for the local campaign UI."""
from __future__ import annotations
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import fields, is_dataclass
from enum import Enum
from pathlib import Path
from typing import TypeAlias
JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
_PRIVATE_KEYS = frozenset(
    {
        "content_path",
        "database_path",
        "logical_path",
        "main_root_path",
        "pid",
        "process_id",
        "child_process_id",
        "process_birth_token",
        "project_root",
        "root_path",
        "spool_path",
        "workspace_path",
    }
)
_PRIVATE_KEY_PARTS = ("environment_value", "secret", "token")
_PRIVATE_PATH_SUFFIXES = ("_path", "_root", "_directory")
_PUBLIC_RELATIVE_PATH_KEYS = frozenset({"source_path"})
_WINDOWS_ABSOLUTE = re.compile(r"^[A-Za-z]:[\\/]")
def _is_absolute_path(value: str) -> bool:
    # Detect Windows, POSIX, UNC and file-URL absolute paths without touching the filesystem.
    stripped = value.strip()
    return bool(
        stripped.startswith(("/", "\\\\", "file://"))
        or _WINDOWS_ABSOLUTE.match(stripped)
    )
def _is_private_key(value: str) -> bool:
    # Reject explicit private identities and path-bearing keys without hiding public diagnostic sections.
    lowered = value.lower()
    if lowered in _PUBLIC_RELATIVE_PATH_KEYS:
        return False
    return bool(
        lowered in _PRIVATE_KEYS
        or lowered.endswith(_PRIVATE_PATH_SUFFIXES)
        or any(part in lowered for part in _PRIVATE_KEY_PARTS)
    )
def _field_value(value: object) -> dict[str, object]:
    # Convert one dataclass into a plain field mapping without calling private serializers.
    return {item.name: getattr(value, item.name) for item in fields(value)}
def to_json_value(value: object, *, hide_private_paths: bool = True) -> JsonValue:
    # Convert public client data to deterministic JSON while redacting private filesystem values.
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return "[redacted]" if hide_private_paths and _is_absolute_path(value) else value
    if isinstance(value, Enum):
        return to_json_value(value.value, hide_private_paths=hide_private_paths)
    if isinstance(value, Path):
        return "[redacted]" if hide_private_paths else str(value)
    serializer = getattr(value, "to_dict", None)
    if callable(serializer):
        return to_json_value(serializer(), hide_private_paths=hide_private_paths)
    if is_dataclass(value):
        return to_json_value(_field_value(value), hide_private_paths=hide_private_paths)
    if isinstance(value, Mapping):
        result: dict[str, JsonValue] = {}
        for key, item in value.items():
            name = str(key)
            if hide_private_paths and _is_private_key(name):
                continue
            result[name] = to_json_value(item, hide_private_paths=hide_private_paths)
        return result
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [to_json_value(item, hide_private_paths=hide_private_paths) for item in value]
    raise TypeError(f"unsupported UI value: {type(value).__name__}")
def canonical_json_bytes(value: object, *, hide_private_paths: bool = True) -> bytes:
    # Encode one response as stable compact UTF-8 JSON with optional privacy filtering.
    normalized = to_json_value(value, hide_private_paths=hide_private_paths)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
def json_object_bytes(value: object) -> bytes:
    # Encode one sanitized response as readable UTF-8 JSON with a final newline.
    normalized = to_json_value(value)
    return (
        json.dumps(normalized, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")
