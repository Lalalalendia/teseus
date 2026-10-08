"""Public reuse policy modes shared by configuration, planning, and execution."""
from __future__ import annotations
from enum import Enum
from typing import Any
class ReuseMode(str, Enum):
    """Canonical authority levels for historical execution reuse."""
    OFF = "off"
    HINT = "hint"
    EXACT = "exact"
    PARTIAL = "partial"
def normalize_reuse_mode(value: Any) -> ReuseMode:
    # Normalize one public mode and retain the former experimental spelling as a migration alias.
    if isinstance(value, ReuseMode):
        return value
    raw = str(value or ReuseMode.HINT.value).strip().lower()
    aliases = {
        "disabled": ReuseMode.OFF.value,
        "experimental": ReuseMode.PARTIAL.value,
    }
    raw = aliases.get(raw, raw)
    try:
        return ReuseMode(raw)
    except ValueError as exc:
        allowed = ", ".join(item.value for item in ReuseMode)
        raise ValueError(f"reuse_mode must be one of: {allowed}") from exc
__all__ = ["ReuseMode", "normalize_reuse_mode"]
