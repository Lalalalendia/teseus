from __future__ import annotations
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from gallifrey_mutation import MutationCampaign, MutationCampaignService, SQLiteMutationStore, Success
from theseus_api import ApiRejected, ApiSuccess, LocalOperatorActions
from theseus_contracts import CampaignBudget, CampaignConfiguration, CampaignId, MutationScope, ProjectDescriptor, ProjectId


def _configuration(root: Path, campaign_id: str) -> CampaignConfiguration:
    # Build one durable campaign contract for recovery action authority tests.
    return CampaignConfiguration(
        campaign_id=CampaignId(campaign_id),
        project=ProjectDescriptor(ProjectId("project-recovery-actions"), "Recovery actions", str(root)),
        scope=MutationScope("app.py", scope_kind="project"),
        budget=CampaignBudget(max_workers=1),
        reports_dir=str(root / "reports"),
    )


def _database(tmp_path: Path, campaign_id: str = "campaign-recovery-actions") -> tuple[Path, MutationCampaign]:
    # Persist one revision-zero campaign in the production operator journal schema.
    database = tmp_path / "campaign.sqlite3"
    store = SQLiteMutationStore(database)
    campaign = MutationCampaign.create(_configuration(tmp_path, campaign_id))
    assert isinstance(store.save_campaign(campaign, expected_revision=None), Success)
    store.close()
    return database, campaign


def test_reconcile_action_is_durable_exact_replay_and_stale_fenced(tmp_path: Path) -> None:
    # Execute startup reconciliation once and replay the terminal receipt without another call.
    database, campaign = _database(tmp_path)
    calls: list[str] = []
    actions = LocalOperatorActions(
        database,
        reconcile_executor=lambda _database, campaign_id: calls.append(campaign_id) or ({"status": "reconciled"},),
    )
    first = actions.reconcile_campaign(
        "action-reconcile",
        campaign.campaign_id.value,
        expected_revision=campaign.revision_number,
    )
    repeated = actions.reconcile_campaign(
        "action-reconcile",
        campaign.campaign_id.value,
        expected_revision=campaign.revision_number,
    )
    stale = actions.reconcile_campaign(
        "action-reconcile-stale",
        campaign.campaign_id.value,
        expected_revision=campaign.revision_number + 1,
    )
    assert isinstance(first, ApiSuccess)
    assert isinstance(repeated, ApiSuccess)
    assert repeated.to_dict() == first.to_dict()
    assert calls == [campaign.campaign_id.value]
    assert isinstance(stale, ApiRejected)
    assert stale.error.code == "stale_revision"


def test_recover_action_replays_running_receipt_after_crash_and_completes_once(tmp_path: Path) -> None:
    # Resume a pre-existing running recovery receipt and retain the immutable campaign identity.
    database, campaign = _database(tmp_path, "campaign-recover-running")
    store = SQLiteMutationStore(database)
    service = MutationCampaignService(store)
    requested = service.request_operator_action(
        "action-recover",
        "recover_campaign",
        campaign.campaign_id,
        expected_revision=campaign.revision_number,
    )
    assert isinstance(requested, Success)
    started = service.start_operator_action("action-recover")
    assert isinstance(started, Success)
    store.close()
    calls: list[str] = []

    def recover_executor(_database: Path, campaign_id: str) -> object:
        # Simulate one idempotent coordinator recovery after the API process crashed.
        calls.append(campaign_id)
        return SimpleNamespace(
            campaign=replace(campaign, revision_number=campaign.revision_number + 1),
        )

    actions = LocalOperatorActions(database, recover_executor=recover_executor)
    first = actions.recover_campaign(
        "action-recover",
        campaign.campaign_id.value,
        expected_revision=campaign.revision_number,
    )
    repeated = actions.recover_campaign(
        "action-recover",
        campaign.campaign_id.value,
        expected_revision=campaign.revision_number,
    )
    assert isinstance(first, ApiSuccess)
    assert isinstance(repeated, ApiSuccess)
    assert first.value.campaign_revision == 1
    assert repeated.to_dict() == first.to_dict()
    assert calls == [campaign.campaign_id.value]
