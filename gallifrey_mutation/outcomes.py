"""Typed outcomes used by the independent Theseus mutation domain."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Generic, TypeAlias, TypeGuard, TypeVar

T = TypeVar("T")
DetailValue: TypeAlias = str | int | float | bool | None
OutcomeDetails: TypeAlias = Mapping[str, DetailValue]


def _freeze_details(details: OutcomeDetails) -> OutcomeDetails:
    # Return an independent immutable copy so domain diagnostics cannot be mutated later.
    return MappingProxyType(dict(details))


@dataclass(frozen=True, slots=True)
class Success(Generic[T]):
    """Successful domain operation carrying its new immutable value."""

    value: T
    message: str = ""
    details: OutcomeDetails = field(default_factory=dict)
    duplicate: bool = False

    def __post_init__(self) -> None:
        # Freeze diagnostic metadata while retaining a cheap boolean idempotency signal.
        object.__setattr__(self, "details", _freeze_details(self.details))


@dataclass(frozen=True, slots=True)
class Rejected:
    """Expected domain rejection such as an illegal transition or stale revision."""

    code: str
    message: str
    details: OutcomeDetails = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Freeze rejection metadata before it crosses an application boundary.
        object.__setattr__(self, "details", _freeze_details(self.details))


@dataclass(frozen=True, slots=True)
class Failed:
    """Technical persistence or serialization failure represented as data."""

    code: str
    message: str
    retriable: bool = False
    details: OutcomeDetails = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Freeze failure metadata so recovery diagnostics remain deterministic.
        object.__setattr__(self, "details", _freeze_details(self.details))


@dataclass(frozen=True, slots=True)
class Cancelled:
    """Explicit cancellation outcome for adapters that do not return a campaign value."""

    code: str = "cancelled"
    message: str = "Operation cancelled"
    details: OutcomeDetails = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Freeze cancellation metadata for the same contract as other domain outcomes.
        object.__setattr__(self, "details", _freeze_details(self.details))


Outcome: TypeAlias = Success[T] | Rejected | Failed | Cancelled


def is_success(outcome: Outcome[T]) -> TypeGuard[Success[T]]:
    # Provide the narrow type guard used by repository and service adapters.
    return isinstance(outcome, Success)
