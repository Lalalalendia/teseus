from __future__ import annotations

import hashlib
import sqlite3
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from gallifrey_mutation import (
    CampaignState,
    EffectReceipt,
    MutationCampaignService,
    SQLiteMutationStore,
    Success,
)
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    MutationScope,
    ProjectDescriptor,
    ProjectId,
    TestCommandDescriptor,
)
from theseus_local import LocalCampaignCoordinator
from theseus_local.finalization import FinalizationArtifactError


def _configuration(root: Path, campaign_id: str) -> CampaignConfiguration:
    # Build one deterministic single-mutant campaign for finalization crash-boundary tests.
    return CampaignConfiguration(
        campaign_id=CampaignId(campaign_id),
        project=ProjectDescriptor(
            project_id=ProjectId(f"project-{campaign_id}"),
            display_name="E14 atomic finalization diagnostics",
            root_path=str(root),
            test_command=TestCommandDescriptor((sys.executable, "-m", "pytest", "-q")),
            pytest_plugin_autoload=False,
        ),
        scope=MutationScope(
            source_path="app.py",
            function="choose",
            operators=("condition_to_not",),
        ),
        budget=CampaignBudget(max_mutants=1, max_workers=1, max_test_seconds=5.0),
        no_escalation=True,
        reports_dir=str(root / "reports"),
    )


def _write_project(root: Path) -> None:
    # Create one observable branch mutation with a stable pytest oracle.
    (root / "app.py").write_text(
        "def choose(value):\n"
        "    if value > 0:\n"
        "        return 1\n"
        "    return 0\n",
        encoding="utf-8",
    )
    (root / "test_app.py").write_text(
        "from app import choose\n\n"
        "def test_choose_positive():\n"
        "    assert choose(1) == 1\n",
        encoding="utf-8",
    )


def _file_sha256(path: Path) -> str:
    # Hash one registry artifact through bounded reads for exact filesystem verification.
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_finalization(database_path: Path, campaign_id: CampaignId):
    # Read campaign, intent, and registry together for diagnostic assertions.
    store = SQLiteMutationStore(database_path)
    try:
        campaign = store.get_campaign(campaign_id)
        intent = store.get_finalization_intent(campaign_id)
        registry = store.list_artifacts(campaign_id)
        assert isinstance(campaign, Success), (
            "cannot load campaign finalization state; "
            f"campaign={campaign_id.value}; database={database_path}; outcome={campaign!r}"
        )
        assert isinstance(intent, Success), (
            "cannot load durable finalization intent; "
            f"campaign={campaign_id.value}; database={database_path}; outcome={intent!r}"
        )
        assert isinstance(registry, Success), (
            "cannot load authoritative artifact registry; "
            f"campaign={campaign_id.value}; database={database_path}; outcome={registry!r}"
        )
        return campaign.value, intent.value, registry.value
    finally:
        store.close()


def _registry_counts(database_path: Path) -> tuple[int, int, int]:
    # Return stable intent, artifact, and finalization-effect cardinalities from SQLite.
    with sqlite3.connect(database_path) as connection:
        intent_count = int(connection.execute("SELECT COUNT(*) FROM mutation_finalizations").fetchone()[0])
        artifact_count = int(connection.execute("SELECT COUNT(*) FROM mutation_artifacts").fetchone()[0])
        effect_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM mutation_effects "
                "WHERE effect_type IN ('mutation.finalization_intent', "
                "'mutation.artifact_registry_committed', 'mutation.finalize')"
            ).fetchone()[0]
        )
    return intent_count, artifact_count, effect_count


def test_completed_campaign_has_exact_registry_and_idempotent_recovery(tmp_path: Path) -> None:
    # Require every completed campaign to have a verified canonical registry and stable replay cardinality.
    _write_project(tmp_path)
    configuration = _configuration(tmp_path, "campaign-e14-finalization-complete")
    result = LocalCampaignCoordinator().run(configuration)
    assert result.succeeded, (
        "baseline finalization campaign did not complete; "
        f"campaign={configuration.campaign_id.value}; status={result.campaign.status}; "
        f"error={result.error!r}; database={result.database_path}; stderr={result.stderr_path}"
    )
    campaign, intent, registry = _load_finalization(result.database_path, configuration.campaign_id)
    assert campaign.status == CampaignState.COMPLETED and intent is not None, (
        "completed campaign lost durable finalization identity; "
        f"campaign={configuration.campaign_id.value}; status={campaign.status}; intent={intent}; "
        f"database={result.database_path}"
    )
    registry_by_key = {item.logical_key: item for item in registry}
    missing = sorted(set(intent.required_logical_keys) - set(registry_by_key))
    assert intent.status == "completed" and not missing, (
        "completed campaign has incomplete artifact registry; "
        f"campaign={configuration.campaign_id.value}; intent_id={intent.intent_id}; "
        f"intent_status={intent.status}; required={intent.required_logical_keys}; "
        f"registry_keys={sorted(registry_by_key)}; missing={missing}; database={result.database_path}"
    )
    reports_root = result.database_path.parent.parent
    for entry in registry:
        content_path = reports_root / entry.content_path
        logical_path = reports_root / entry.logical_path
        assert content_path.is_file() and logical_path.is_file(), (
            "registered artifact is missing from content or logical storage; "
            f"campaign={configuration.campaign_id.value}; logical_key={entry.logical_key}; "
            f"content_path={content_path}; logical_path={logical_path}; database={result.database_path}"
        )
        assert _file_sha256(content_path) == entry.content_sha256, (
            "content-addressed artifact hash conflicts with SQLite registry; "
            f"campaign={configuration.campaign_id.value}; logical_key={entry.logical_key}; "
            f"expected_sha256={entry.content_sha256}; actual_sha256={_file_sha256(content_path)}; "
            f"path={content_path}"
        )
        assert _file_sha256(logical_path) == entry.content_sha256, (
            "logical artifact alias hash conflicts with SQLite registry; "
            f"campaign={configuration.campaign_id.value}; logical_key={entry.logical_key}; "
            f"expected_sha256={entry.content_sha256}; actual_sha256={_file_sha256(logical_path)}; "
            f"path={logical_path}"
        )
    before = _registry_counts(result.database_path)
    first_actions = LocalCampaignCoordinator.reconcile_startup(result.database_path)
    second_actions = LocalCampaignCoordinator.reconcile_startup(result.database_path)
    after = _registry_counts(result.database_path)
    assert before == after, (
        "idempotent startup recovery duplicated finalization rows or effects; "
        f"campaign={configuration.campaign_id.value}; before={before}; after={after}; "
        f"first_actions={first_actions}; second_actions={second_actions}; database={result.database_path}"
    )


def test_startup_recovery_registers_published_artifacts_after_pre_registry_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Crash after physical publication but before SQLite registry commit, then recover exactly once.
    _write_project(tmp_path)
    configuration = _configuration(tmp_path, "campaign-e14-finalization-pre-registry")
    original = MutationCampaignService.register_finalization
    injected = {"raised": False}

    def crash_before_registry(self, effect_id, campaign_id, intent, artifacts, *, expected_revision):
        # Inject one failure after publish_finalization_intent has verified all physical files.
        if not injected["raised"]:
            injected["raised"] = True
            raise RuntimeError(
                "injected crash after artifact publication and before SQLite registry commit"
            )
        return original(
            self,
            effect_id,
            campaign_id,
            intent,
            artifacts,
            expected_revision=expected_revision,
        )

    monkeypatch.setattr(MutationCampaignService, "register_finalization", crash_before_registry)
    interrupted = LocalCampaignCoordinator().run(configuration)
    assert injected["raised"] and not interrupted.succeeded, (
        "pre-registry crash fixture did not stop at the intended boundary; "
        f"campaign={configuration.campaign_id.value}; status={interrupted.campaign.status}; "
        f"error={interrupted.error!r}; database={interrupted.database_path}"
    )
    assert "before SQLite registry commit" in str(interrupted.error), (
        "coordinator error lost the injected finalization boundary; "
        f"campaign={configuration.campaign_id.value}; error={interrupted.error!r}"
    )
    campaign, intent, registry = _load_finalization(interrupted.database_path, configuration.campaign_id)
    assert campaign.status == CampaignState.MATERIALIZING and intent is not None and registry == (), (
        "pre-registry crash persisted an illegal partial database state; "
        f"campaign={configuration.campaign_id.value}; status={campaign.status}; "
        f"intent_status={getattr(intent, 'status', None)}; registry_count={len(registry)}; "
        f"database={interrupted.database_path}"
    )
    reports_root = interrupted.database_path.parent.parent
    missing_physical = [
        entry.logical_key
        for entry in intent.artifacts
        if not (reports_root / entry.content_path).is_file()
        or not (reports_root / entry.logical_path).is_file()
    ]
    assert not missing_physical, (
        "crash fixture occurred before physical artifact publication completed; "
        f"campaign={configuration.campaign_id.value}; intent_id={intent.intent_id}; "
        f"missing={missing_physical}; reports_root={reports_root}"
    )
    monkeypatch.setattr(MutationCampaignService, "register_finalization", original)
    actions = LocalCampaignCoordinator.reconcile_startup(interrupted.database_path)
    recovered_campaign, recovered_intent, recovered_registry = _load_finalization(
        interrupted.database_path,
        configuration.campaign_id,
    )
    assert recovered_campaign.status == CampaignState.COMPLETED, (
        "startup recovery did not complete the pre-registry crash; "
        f"campaign={configuration.campaign_id.value}; status={recovered_campaign.status}; "
        f"intent={recovered_intent}; actions={actions}; database={interrupted.database_path}"
    )
    assert recovered_intent is not None and recovered_intent.status == "completed", (
        "startup recovery did not advance the durable intent to completed; "
        f"campaign={configuration.campaign_id.value}; intent={recovered_intent}; actions={actions}"
    )
    assert len(recovered_registry) == len(intent.artifacts), (
        "startup recovery registered a partial or duplicate artifact set; "
        f"campaign={configuration.campaign_id.value}; expected={len(intent.artifacts)}; "
        f"actual={len(recovered_registry)}; registry={[item.logical_key for item in recovered_registry]}; "
        f"actions={actions}"
    )


def test_startup_recovery_completes_ready_campaign_after_post_registry_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Crash after atomic registry commit but before completion and require recovery to finish without republishing.
    _write_project(tmp_path)
    configuration = _configuration(tmp_path, "campaign-e14-finalization-post-registry")
    original = MutationCampaignService.complete_finalization
    injected = {"raised": False}

    def crash_before_completion(self, effect_id, campaign_id, *, expected_revision):
        # Inject one failure after READY_TO_COMMIT and the full registry are durable.
        if not injected["raised"]:
            injected["raised"] = True
            raise RuntimeError(
                "injected crash after SQLite registry commit and before campaign completion"
            )
        return original(
            self,
            effect_id,
            campaign_id,
            expected_revision=expected_revision,
        )

    monkeypatch.setattr(MutationCampaignService, "complete_finalization", crash_before_completion)
    interrupted = LocalCampaignCoordinator().run(configuration)
    assert injected["raised"] and not interrupted.succeeded, (
        "post-registry crash fixture did not stop at the intended boundary; "
        f"campaign={configuration.campaign_id.value}; status={interrupted.campaign.status}; "
        f"error={interrupted.error!r}; database={interrupted.database_path}"
    )
    campaign, intent, registry = _load_finalization(interrupted.database_path, configuration.campaign_id)
    assert campaign.status == CampaignState.READY_TO_COMMIT, (
        "registry crash window did not leave the campaign at READY_TO_COMMIT; "
        f"campaign={configuration.campaign_id.value}; status={campaign.status}; "
        f"intent_status={getattr(intent, 'status', None)}; registry_count={len(registry)}; "
        f"database={interrupted.database_path}"
    )
    assert intent is not None and intent.status == "registered" and len(registry) == len(intent.artifacts), (
        "READY_TO_COMMIT campaign lacks its complete durable registry; "
        f"campaign={configuration.campaign_id.value}; intent={intent}; "
        f"registry_keys={[item.logical_key for item in registry]}; database={interrupted.database_path}"
    )
    before = _registry_counts(interrupted.database_path)
    monkeypatch.setattr(MutationCampaignService, "complete_finalization", original)
    actions = LocalCampaignCoordinator.reconcile_startup(interrupted.database_path)
    recovered_campaign, recovered_intent, recovered_registry = _load_finalization(
        interrupted.database_path,
        configuration.campaign_id,
    )
    after = _registry_counts(interrupted.database_path)
    assert recovered_campaign.status == CampaignState.COMPLETED, (
        "startup recovery did not finish a registry-complete campaign; "
        f"campaign={configuration.campaign_id.value}; status={recovered_campaign.status}; "
        f"intent={recovered_intent}; actions={actions}; database={interrupted.database_path}"
    )
    assert recovered_intent is not None and recovered_intent.status == "completed", (
        "completion recovery did not persist the terminal finalization receipt; "
        f"campaign={configuration.campaign_id.value}; intent={recovered_intent}; actions={actions}"
    )
    assert len(recovered_registry) == len(registry), (
        "completion recovery changed artifact registry cardinality; "
        f"campaign={configuration.campaign_id.value}; before={len(registry)}; "
        f"after={len(recovered_registry)}; actions={actions}"
    )
    assert after[0] == before[0] and after[1] == before[1] and after[2] == before[2] + 1, (
        "post-registry recovery must append only the terminal completion effect; "
        f"campaign={configuration.campaign_id.value}; before={before}; after={after}; actions={actions}"
    )


def test_registry_rejects_content_replacement_and_missing_canonical_fails_closed(tmp_path: Path) -> None:
    # Reject a second hash for one logical key and fail recovery when a completed canonical alias disappears.
    _write_project(tmp_path)
    configuration = _configuration(tmp_path, "campaign-e14-finalization-conflict")
    result = LocalCampaignCoordinator().run(configuration)
    assert result.succeeded, (
        "conflict fixture campaign did not reach a valid completed baseline; "
        f"campaign={configuration.campaign_id.value}; error={result.error!r}; database={result.database_path}"
    )
    campaign, intent, registry = _load_finalization(result.database_path, configuration.campaign_id)
    assert campaign.status == CampaignState.COMPLETED and intent is not None, (
        "conflict fixture has no completed intent; "
        f"campaign={configuration.campaign_id.value}; status={campaign.status}; intent={intent}"
    )
    canonical = next(item for item in registry if item.logical_key == intent.canonical_report_key)
    conflicting = replace(
        canonical,
        content_sha256="f" * 64 if canonical.content_sha256 != "f" * 64 else "e" * 64,
        size_bytes=canonical.size_bytes + 1,
    )
    store = SQLiteMutationStore(result.database_path)
    try:
        conflict = store.commit_finalization(
            receipt=EffectReceipt(
                effect_id="effect.finalization.conflicting-replacement",
                effect_type="mutation.artifact_registry_committed",
                campaign_id=configuration.campaign_id,
                payload={"finalization_intent": intent.to_dict()},
            ),
            intent=intent,
            artifacts=(conflicting,),
        )
    finally:
        store.close()
    assert getattr(conflict, "code", None) == "artifact_logical_key_conflict", (
        "artifact registry accepted a different content hash under an existing logical key; "
        f"campaign={configuration.campaign_id.value}; logical_key={canonical.logical_key}; "
        f"existing_sha256={canonical.content_sha256}; candidate_sha256={conflicting.content_sha256}; "
        f"outcome={conflict!r}; database={result.database_path}"
    )
    canonical_path = result.database_path.parent.parent / canonical.logical_path
    canonical_bytes = canonical_path.read_bytes()
    canonical_path.unlink()
    with pytest.raises(FinalizationArtifactError) as captured:
        LocalCampaignCoordinator.reconcile_startup(result.database_path)
    message = str(captured.value)
    assert canonical.logical_key in message and str(canonical_path) in message, (
        "missing canonical recovery failure omitted logical identity or expected path; "
        f"campaign={configuration.campaign_id.value}; logical_key={canonical.logical_key}; "
        f"expected_path={canonical_path}; message={message!r}; database={result.database_path}"
    )
    canonical_path.write_bytes(canonical_bytes)
