"""Errors raised by the standalone Theseus Survivor Lab package."""


class SurvivorLabError(Exception):
    """Base class for predictable Survivor Lab failures."""


class ContractError(SurvivorLabError):
    """Raised when an input or result contract is malformed."""


class UnsupportedSchemaVersion(ContractError):
    """Raised when a bundle uses a schema version this package cannot read."""


class ProviderError(SurvivorLabError):
    """Raised when a proposal provider violates its boundary contract."""
