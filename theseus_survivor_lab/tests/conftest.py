"""Shared fixture loading helpers for the E24 test suite."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from theseus_survivor_lab.serialization import request_from_dict


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "survivor_lab"


def load_fixture_payload(name: str) -> tuple[dict, dict]:
    # Load fixture metadata separately from the versioned input bundle.
    payload = json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))
    expected = copy.deepcopy(payload.pop("expected", {}))
    return payload, expected


@pytest.fixture
def real_gap_request():
    # Provide a parsed real-gap request for tests that need a canonical object.
    payload, _ = load_fixture_payload("real_gap.json")
    return request_from_dict(payload)
