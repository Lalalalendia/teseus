"""Stable validation errors for the Theseus wire boundary."""


class ContractError(ValueError):
    """Base error for invalid or incompatible public messages."""


class MissingFieldError(ContractError):
    """Raised when a required wire field is absent or malformed."""


class IncompatibleProtocolError(ContractError):
    """Raised when a message belongs to an unsupported protocol major version."""


class UnsupportedSchemaError(ContractError):
    """Raised when a message uses a newer schema than this reader understands."""


class UnknownMessageTypeError(ContractError):
    """Raised when a protocol frame uses an unknown or directionally invalid message type."""


class IdentityMismatchError(ContractError):
    """Raised when a frame belongs to another worker instance or recycled process identity."""


class CorrelationMismatchError(ContractError):
    """Raised when a response cannot be tied to its initiating request or assignment."""
