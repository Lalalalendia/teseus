from __future__ import annotations

import inspect
import json
from enum import Enum
from pathlib import Path

from theseus_api import (
    ApiError,
    ApiFailed,
    ApiPage,
    ApiRejected,
    ApiSuccess,
    ArtifactDto,
    LocalApiService,
    ProjectDto,
)


def _assert_json_only(value: object) -> None:
    # Recursively reject runtime-only values from a serialized API response.
    if value is None or isinstance(value, (str, int, float, bool)):
        return
    if isinstance(value, list):
        for item in value:
            _assert_json_only(item)
        return
    if isinstance(value, dict):
        assert all(isinstance(key, str) for key in value)
        for item in value.values():
            _assert_json_only(item)
        return
    raise AssertionError(f"non-JSON API value: {type(value).__name__}")


def test_api_outcomes_are_stable_discriminated_json_shapes() -> None:
    # Keep success, expected rejection and technical failure distinguishable without exceptions.
    success = ApiSuccess(ProjectDto("project-1", "Project", None, None, "revision-1", 1)).to_dict()
    rejected = ApiRejected(ApiError("not_found", "campaign does not exist", details={"campaign_id": "c1"})).to_dict()
    failed = ApiFailed(ApiError("store_read_failed", "database read failed", retriable=True)).to_dict()
    assert success["ok"] is True
    assert success["kind"] == "success"
    assert rejected == {
        "ok": False,
        "kind": "rejected",
        "error": {
            "code": "not_found",
            "message": "campaign does not exist",
            "retriable": False,
            "details": {"campaign_id": "c1"},
        },
    }
    assert failed["error"]["retriable"] is True
    _assert_json_only(success)
    _assert_json_only(rejected)
    _assert_json_only(failed)


def test_artifact_dto_never_exposes_filesystem_paths_or_runtime_objects() -> None:
    # Publish immutable registry metadata while keeping physical and logical paths private.
    artifact = ArtifactDto(
        campaign_id="campaign-1",
        logical_key="report",
        logical_role="campaign_report",
        content_sha256="a" * 64,
        size_bytes=10,
        schema_version=1,
        producer="coordinator",
        created_at="2026-08-05T12:00:00Z",
        shard_id=None,
        execution_id=None,
        metadata={"format": "json", "parts": [1, 2]},
    )
    value = artifact.to_dict()
    assert "content_path" not in value
    assert "logical_path" not in value
    assert not any(isinstance(item, (Path, Enum)) for item in value.values())
    _assert_json_only(value)


def test_all_list_reads_expose_explicit_limit_and_cursor_parameters() -> None:
    # Keep pagination visible and uniform across every transport-neutral collection read.
    methods = (
        "list_projects",
        "list_campaigns",
        "list_plans",
        "list_workers",
        "list_shards",
        "list_executions",
        "list_artifacts",
        "get_knowledge",
        "list_statistics",
        "get_recovery",
    )
    for name in methods:
        signature = inspect.signature(getattr(LocalApiService, name))
        assert "limit" in signature.parameters
        assert "cursor" in signature.parameters


def test_local_api_has_no_command_or_network_surface() -> None:
    # Reserve campaign commands and network adapters for later roadmap stages.
    public = {name for name in dir(LocalApiService) if not name.startswith("_")}
    assert not public.intersection({"run", "start", "cancel", "retry", "resume", "listen", "serve"})
    service_source = Path(inspect.getsourcefile(LocalApiService)).read_text(encoding="utf-8").lower()
    assert " offset " not in service_source
    assert "fastapi" not in service_source
    assert "flask" not in service_source
    assert "http.server" not in service_source


def test_api_page_serialization_is_deterministic() -> None:
    # Return identical JSON for repeated serialization of the same frozen page.
    page = ApiPage(
        items=(ProjectDto("project-1", "Project", "D:/project", None, None, 2),),
        limit=10,
        next_cursor="cursor-1",
    )
    first = json.dumps(page.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    second = json.dumps(page.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    assert first == second
