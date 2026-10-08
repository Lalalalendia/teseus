from __future__ import annotations

import pytest

from theseus_contracts import (
    HOST_TO_WORKER_MESSAGE_TYPES,
    WORKER_TO_HOST_MESSAGE_TYPES,
    NoAssignment,
    WorkerMessageType,
    WorkerProtocolFrame,
    decode_worker_frame,
)
from theseus_contracts.errors import (
    IncompatibleProtocolError,
    UnknownMessageTypeError,
    UnsupportedSchemaError,
)


def _frame_dict() -> dict[str, object]:
    # Build one valid host frame that can be mutated at the wire boundary.
    return WorkerProtocolFrame.create(
        WorkerMessageType.NO_ASSIGNMENT,
        NoAssignment(0.1),
        worker_id="worker-version",
        instance_id="instance-version",
        process_id=1234,
        sequence=1,
        state="host",
    ).to_dict()


def test_worker_protocol_rejects_unknown_major_and_newer_schema() -> None:
    # Fail before payload routing when protocol or schema compatibility is not proven.
    wrong_protocol = _frame_dict()
    wrong_protocol["protocol_version"] = 2
    with pytest.raises(IncompatibleProtocolError, match="protocol_version"):
        WorkerProtocolFrame.from_dict(wrong_protocol)
    wrong_schema = _frame_dict()
    wrong_schema["schema_version"] = 2
    with pytest.raises(UnsupportedSchemaError, match="schema_version"):
        WorkerProtocolFrame.from_dict(wrong_schema)


def test_worker_protocol_rejects_unknown_and_wrong_direction_message_types() -> None:
    # Keep both unknown frames and valid frames travelling in the wrong direction fail-closed.
    unknown = _frame_dict()
    unknown["message_type"] = "worker.future_unknown"
    with pytest.raises(UnknownMessageTypeError, match="message_type"):
        WorkerProtocolFrame.from_dict(unknown)
    valid_raw = WorkerProtocolFrame.from_dict(_frame_dict()).to_json()
    assert decode_worker_frame(valid_raw, allowed_types=HOST_TO_WORKER_MESSAGE_TYPES)
    with pytest.raises(UnknownMessageTypeError, match="not allowed"):
        decode_worker_frame(valid_raw, allowed_types=WORKER_TO_HOST_MESSAGE_TYPES)
