from __future__ import annotations
import json
import os
import sys
from pathlib import Path
import pytest
from test_intelligence_unified_v1.commands import env_fingerprint
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    MutationScope,
    ProjectDescriptor,
    ProjectId,
    TestCommandDescriptor,
)
from theseus_knowledge import KnowledgePlaneStore, ReuseKind
from theseus_local import LocalCampaignCoordinator
from theseus_local.workspace import WorkspaceProvider
def _fingerprint(root: Path, nodeid: str) -> str:
    # Compute one production test fingerprint without invoking pytest collection.
    return LocalCampaignCoordinator._test_fingerprints(root, None, (nodeid,))[nodeid]
def _configuration(root: Path, campaign_id: str, *, reuse_mode: str = "hint") -> CampaignConfiguration:
    # Build a minimal campaign that exercises the real local coordinator and external workspace.
    return CampaignConfiguration(
        campaign_id=CampaignId(campaign_id),
        project=ProjectDescriptor(
            project_id=ProjectId("project_e16_correctness"),
            display_name="E16 correctness fixture",
            root_path=str(root),
            test_command=TestCommandDescriptor((sys.executable, "-m", "pytest", "-q")),
            pytest_plugin_autoload=False,
        ),
        scope=MutationScope(source_path="app.py", function="choose", operators=("condition_to_not",)),
        budget=CampaignBudget(max_mutants=1, max_workers=1),
        no_escalation=True,
        reports_dir=str(root / "reports"),
        reuse_mode=reuse_mode,
    )
def _knowledge_execution(*, evidence_origin: str) -> dict[str, object]:
    # Build explicit per-test evidence so the Knowledge Plane can distinguish proof from projection.
    return {
        "execution_id": "exec-proof",
        "mutant_id": "m-proof",
        "attempt": 0,
        "semantic_result": "killed",
        "status": "complete",
        "restore_verified": True,
        "evidence_schema_version": 2,
        "source_kind": "observed",
        "evidence_origin": evidence_origin,
        "function_id": "choose",
        "function_fingerprint": "function-proof",
        "mutant_fingerprint": "mutant-proof",
        "test_fingerprint": "test-proof",
        "conftest_fingerprint": "conftest-proof",
        "environment_fingerprint": "environment-proof",
        "selection_fingerprint": "selection-proof",
        "result_fingerprint": "result-proof",
        "test_observations": [
            {
                "test_id": "test_app.py::test_choose",
                "test_fingerprint": "test-node-proof",
                "outcome": "failed",
                "evidence_kind": "pytest_test_event",
                "observation_schema_version": 1,
            }
        ],
    }
def test_fingerprints_include_helpers_import_aliases_and_exact_class_context(tmp_path: Path) -> None:
    # Ensure same-module helpers, imported aliases and duplicate class method names cannot collide.
    (tmp_path / "test_module.py").write_text(
        "def helper(value):\n    return value + 1\n\n"
        "class TestA:\n    def test_value(self):\n        assert helper(1) == 2\n\n"
        "class TestB:\n    def test_value(self):\n        assert helper(1) == 2\n",
        encoding="utf-8",
    )
    first_a = _fingerprint(tmp_path, "test_module.py::TestA::test_value")
    first_b = _fingerprint(tmp_path, "test_module.py::TestB::test_value")
    assert first_a != first_b
    (tmp_path / "test_module.py").write_text(
        "def helper(value):\n    return value + 2\n\n"
        "class TestA:\n    def test_value(self):\n        assert helper(1) == 2\n\n"
        "class TestB:\n    def test_value(self):\n        assert helper(1) == 2\n",
        encoding="utf-8",
    )
    assert _fingerprint(tmp_path, "test_module.py::TestA::test_value") != first_a
    package = tmp_path / "pkg"
    package.mkdir()
    (package / "__init__.py").write_text("from .helper import helper\n", encoding="utf-8")
    helper_path = package / "helper.py"
    helper_path.write_text("def helper(value):\n    return value + 1\n", encoding="utf-8")
    test_path = tmp_path / "test_import.py"
    test_path.write_text(
        "from pkg import helper\n\ndef test_import():\n    assert helper(1) == 2\n",
        encoding="utf-8",
    )
    imported_before = _fingerprint(tmp_path, "test_import.py::test_import")
    helper_path.write_text("def helper(value):\n    return value + 2\n", encoding="utf-8")
    assert _fingerprint(tmp_path, "test_import.py::test_import") != imported_before
def test_environment_fingerprint_respects_explicit_inheritance_contract(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # Prove allowlisted mode ignores undeclared variables while declared and strict modes track them.
    monkeypatch.setenv("ARBITRARY_FEATURE_FLAG", "strict")
    allowlisted_strict = env_fingerprint(tmp_path, include_cwd=False)
    declared_strict = env_fingerprint(
        tmp_path,
        include_cwd=False,
        declared_keys=("ARBITRARY_FEATURE_FLAG",),
    )
    tracked_strict = env_fingerprint(
        tmp_path,
        include_cwd=False,
        inherit_policy="track_all_except",
    )
    monkeypatch.setenv("ARBITRARY_FEATURE_FLAG", "permissive")
    allowlisted_permissive = env_fingerprint(tmp_path, include_cwd=False)
    declared_permissive = env_fingerprint(
        tmp_path,
        include_cwd=False,
        declared_keys=("ARBITRARY_FEATURE_FLAG",),
    )
    tracked_permissive = env_fingerprint(
        tmp_path,
        include_cwd=False,
        inherit_policy="track_all_except",
    )
    assert allowlisted_strict == allowlisted_permissive
    assert declared_strict != declared_permissive
    assert tracked_strict != tracked_permissive
def test_knowledge_rejects_projection_as_reuse_proof(tmp_path: Path) -> None:
    # A matching aggregate projection remains a historical hint until real pytest events are present.
    store = KnowledgePlaneStore(tmp_path / "knowledge.sqlite3")
    try:
        projection = _knowledge_execution(evidence_origin="aggregate_projection")
        store.ingest_effect(
            effect_id="effect-projection",
            campaign_id="campaign-projection",
            effect_type="mutation.execute_shard",
            payload={"executions": [projection]},
        )
        request = {key: projection[key] for key in (
            "mutant_fingerprint",
            "test_fingerprint",
            "environment_fingerprint",
            "selection_fingerprint",
            "result_fingerprint",
            "conftest_fingerprint",
            "function_id",
            "function_fingerprint",
        )}
        decision = store.decide_reuse(mutant_id="m-proof", **request)
        assert decision.kind is ReuseKind.HISTORICAL_HINT
        assert decision.eligible is False
    finally:
        store.close()
def test_default_mode_writes_hint_but_runs_and_reports_gallifrey_state(tmp_path: Path) -> None:
    # The safe default exposes exact evidence in the plan but still executes a fresh shard.
    (tmp_path / "app.py").write_text(
        "def choose(value):\n    if value > 0:\n        return 1\n    return 0\n",
        encoding="utf-8",
    )
    (tmp_path / "test_app.py").write_text(
        "from app import choose\n\ndef test_choose():\n    assert choose(1) == 1\n",
        encoding="utf-8",
    )
    coordinator = LocalCampaignCoordinator()
    first = coordinator.run(_configuration(tmp_path, "cmp_hint_first"))
    second = coordinator.run(_configuration(tmp_path, "cmp_hint_second"))
    assert first.succeeded
    assert second.succeeded
    reports_root = WorkspaceProvider(_configuration(tmp_path, "cmp_hint_second")).reports_root
    plan = json.loads((reports_root / "cmp_hint_second" / "reuse.plan.json").read_text(encoding="utf-8"))
    assert plan["reuse_mode"] == "hint"
    assert plan["decisions"][0]["kind"] == "exact", plan["decisions"][0]
    assert plan["decisions"][0]["eligible"] is True
    assert plan["decisions"][0]["authorized"] is False
    audit = json.loads((reports_root / "cmp_hint_second" / "reuse.audit.json").read_text(encoding="utf-8"))
    assert audit["decisions"][0]["passed"] is True
    assert list((reports_root / "engine" / "cmp_hint_second").glob("engine-shard-*.json"))
    canonical = json.loads((reports_root / "cmp_hint_second" / "canonical.report.json").read_text(encoding="utf-8"))
    assert canonical["source"] == "gallifrey_authoritative"
    assert canonical["completed_mutants"] == 1
    assert canonical["results"][0]["provenance"]["execution_id"]
def test_sampled_fresh_reuse_mismatch_quarantines_rule(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Force one sampled audit to disagree and verify the campaign falls back to fresh execution.
    (tmp_path / "app.py").write_text(
        "def choose(value):\n    if value > 0:\n        return 1\n    return 0\n",
        encoding="utf-8",
    )
    (tmp_path / "test_app.py").write_text(
        "from app import choose\n\ndef test_choose():\n    assert choose(1) == 1\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("THESEUS_REUSE_AUDIT_EXACT_RATE", "1")
    coordinator = LocalCampaignCoordinator()
    first = coordinator.run(_configuration(tmp_path, "cmp_audit_source"))
    assert first.succeeded
    def fake_fresh_audit(self, *, process, campaign, mutant_id, request, root):
        # Return a structurally valid but semantically different observation for the audit boundary.
        del self, process, campaign, root
        nodeid = next(iter(request["test_fingerprints"]))
        return {
            "status": "survived",
            "semantic_result": "survived",
            "restore_verified": True,
            "test_observations": [
                {
                    "test_id": nodeid,
                    "test_fingerprint": request["test_fingerprints"][nodeid],
                    "outcome": "passed",
                    "evidence_kind": "pytest_test_event",
                    "observation_schema_version": 1,
                }
            ],
            **{
                key: request[key]
                for key in (
                    "function_id",
                    "function_fingerprint",
                    "mutant_fingerprint",
                    "test_fingerprint",
                    "conftest_fingerprint",
                    "environment_fingerprint",
                    "selection_fingerprint",
                    "result_fingerprint",
                )
                if request.get(key) is not None
            },
        }
    monkeypatch.setattr(LocalCampaignCoordinator, "_fresh_reuse_audit_result", fake_fresh_audit)
    second_configuration = _configuration(tmp_path, "cmp_audit_mismatch", reuse_mode="partial")
    second = coordinator.run(second_configuration)
    assert second.succeeded
    report_root = WorkspaceProvider(second_configuration).reports_root / "cmp_audit_mismatch"
    audit = json.loads((report_root / "reuse.audit.json").read_text(encoding="utf-8"))
    assert audit["decisions"][0]["sampled"] is True, audit["decisions"][0]
    assert "semantic_mismatch" in audit["decisions"][0]["mismatches"]
    assert audit["decisions"][0]["incident_id"]
    assert audit["decisions"][0]["passed"] is False
def test_partial_reuse_survives_changed_aggregate_result_fingerprint(tmp_path: Path) -> None:
    # Preserve unchanged per-test evidence when one test and the aggregate result fingerprint change.
    store = KnowledgePlaneStore(tmp_path / "partial-invalidation.sqlite3")
    source = {
        "execution_id": "exec-partial-source",
        "mutant_id": "m-partial",
        "attempt": 0,
        "semantic_result": "killed",
        "status": "complete",
        "restore_verified": True,
        "evidence_schema_version": 2,
        "source_kind": "observed",
        "evidence_origin": "pytest_test_stats",
        "function_id": "choose",
        "function_fingerprint": "function-v1",
        "mutant_fingerprint": "mutant-v1",
        "test_fingerprint": "tests-v1",
        "conftest_fingerprint": "conftest-v1",
        "environment_fingerprint": "environment-v1",
        "selection_fingerprint": "selection-v1",
        "result_fingerprint": "result-v1",
        "test_observations": [
            {
                "test_id": "test_app.py::test_positive",
                "test_fingerprint": "positive-v1",
                "outcome": "failed",
                "evidence_kind": "pytest_test_event",
                "observation_schema_version": 1,
            },
            {
                "test_id": "test_app.py::test_negative",
                "test_fingerprint": "negative-v1",
                "outcome": "passed",
                "evidence_kind": "pytest_test_event",
                "observation_schema_version": 1,
            },
        ],
    }
    request = {
        "mutant_id": "m-partial",
        "function_id": "choose",
        "function_fingerprint": "function-v1",
        "mutant_fingerprint": "mutant-v1",
        "test_fingerprint": "tests-v2",
        "test_fingerprints": {
            "test_app.py::test_positive": "positive-v1",
            "test_app.py::test_negative": "negative-v2",
        },
        "conftest_fingerprint": "conftest-v1",
        "environment_fingerprint": "environment-v1",
        "selection_fingerprint": "selection-v1",
        "result_fingerprint": "result-v2",
    }
    try:
        store.ingest_effect(
            effect_id="effect-partial-source",
            campaign_id="campaign-partial-source",
            effect_type="mutation.execute_shard",
            payload={"executions": [source]},
        )
        store.invalidate_changed_inputs(
            (request,),
            campaign_id="campaign-partial-current",
            invalidated_at="2099-01-01T00:00:00Z",
        )
        decision = store.decide_reuse(**request)
    finally:
        store.close()
    assert decision.kind is ReuseKind.PARTIAL
    assert decision.eligible is True
    assert decision.matched_test_ids == ("test_app.py::test_positive",)
    assert decision.missing_test_ids == ("test_app.py::test_negative",)
def test_workspace_ownership_is_external_and_cancel_marker_is_recoverable(tmp_path: Path) -> None:
    # Keep control artifacts out of the checkout and make cancellation state explicit and disposable.
    configuration = _configuration(tmp_path, "cmp_workspace_guard")
    provider = WorkspaceProvider(configuration)
    handle = provider.prepare(configuration.campaign_id.value)
    assert handle.workspace_root != tmp_path.resolve()
    assert handle.ownership_manifest.is_file()
    assert not (tmp_path / ".theseus" / "reports").exists()
    with pytest.raises(ValueError, match="main checkout changed"):
        (tmp_path / "new_file.py").write_text("value = 1\n", encoding="utf-8")
        provider.prepare(configuration.campaign_id.value)
    marker = LocalCampaignCoordinator.request_cancel(configuration)
    assert json.loads(marker.read_text(encoding="utf-8"))["campaign_id"] == "cmp_workspace_guard"
    LocalCampaignCoordinator.clear_cancel(configuration)
    assert not marker.exists()
def test_workspace_rejects_symlinked_ownership_paths(tmp_path: Path) -> None:
    # Reject a pre-existing symlink before any campaign state can escape its ownership boundary.
    configuration = _configuration(tmp_path, "cmp_workspace_symlink")
    provider = WorkspaceProvider(configuration)
    workspace_root = provider.state_root / "workspaces" / "cmp_workspace_symlink"
    workspace_root.parent.mkdir(parents=True, exist_ok=True)
    target = tmp_path / "symlink-target"
    target.mkdir()
    try:
        workspace_root.symlink_to(target, target_is_directory=True)
    except OSError as exc:
        if os.name == "nt" and getattr(exc, "winerror", None) == 1314:
            pytest.skip("Windows symlink privilege is unavailable")
        raise
    with pytest.raises(ValueError, match="must not be symlinks"):
        provider.prepare(configuration.campaign_id.value)
