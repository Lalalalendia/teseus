from __future__ import annotations
from theseus_api.service import _execution_dto
from theseus_ui import asset_bytes
def _execution_payload(*, semantic_result: str = "killed") -> dict[str, object]:
    # Build one dense persisted execution payload whose public projection must stay compact.
    return {
        "execution_id": "execution-1",
        "campaign_id": "campaign-1",
        "shard_id": "shard-1",
        "mutant_id": "mutant-1",
        "attempt": 0,
        "status": "complete",
        "semantic_result": semantic_result,
        "selected_tests": [f"tests/test_app.py::test_{index}" for index in range(2000)],
        "artifacts": [f"artifact-{index}" for index in range(500)],
        "duration_seconds": 0.125,
        "restore_verified": True,
        "error": None,
        "revision_number": 3,
        "lease_id": "lease-1",
        "test_observations": [
            {
                "test_id": "tests/test_app.py::test_later",
                "outcome": "failed",
                "evidence_kind": "pytest_test_event",
                "first_failure": False,
            },
            {
                "test_id": "tests/test_app.py::test_killer",
                "outcome": "failed",
                "evidence_kind": "pytest_test_event",
                "first_failure": True,
            },
            {
                "test_id": "tests/test_app.py::test_missing",
                "outcome": "failed",
                "evidence_kind": "missing_observation",
            },
        ],
    }
def test_execution_report_projection_is_compact_and_preserves_explicit_killer() -> None:
    # Keep normal execution reads bounded to counters plus one explicit killer identity.
    value = _execution_dto(_execution_payload()).to_dict()
    assert value["killer_test_id"] == "tests/test_app.py::test_killer"
    assert value["selected_test_count"] == 2000
    assert value["artifact_count"] == 500
    assert value["observation_count"] == 3
    assert "selected_tests" not in value
    assert "artifacts" not in value
    assert "test_observations" not in value
def test_non_killed_execution_never_promotes_a_failed_observation_to_killer() -> None:
    # Require the authoritative semantic result before exposing a failed pytest event as a killer.
    value = _execution_dto(_execution_payload(semantic_result="survived"))
    assert value.killer_test_id is None
def test_killer_projection_requires_explicit_first_failure_evidence() -> None:
    # Fail closed when failed pytest observations do not identify an authoritative first failure.
    payload = _execution_payload()
    observations = payload["test_observations"]
    assert isinstance(observations, list)
    for observation in observations:
        if isinstance(observation, dict):
            observation["first_failure"] = False
    value = _execution_dto(payload)
    assert value.killer_test_id is None
def test_report_ui_renders_only_compact_execution_and_knowledge_fields() -> None:
    # Bind report columns to compact API evidence and keep heavy nested execution payloads out of browser code.
    html = asset_bytes("index.html").decode("utf-8")
    script = asset_bytes("app.js").decode("utf-8")
    for heading in ("Killer", "Test plan", "Executed / reused", "Timing", "Evidence quality"):
        assert heading in html
    for field in ("killer_test_id", "selected_test_count", "source_kind", "evidence_quality", "reuse_count", "escalation_count"):
        assert field in script
    for heavy in ("test_observations", "artifact_paths", "runtime_dependencies"):
        assert heavy not in script
