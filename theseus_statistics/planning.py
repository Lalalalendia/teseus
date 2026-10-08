"""Canonical statistics projection for immutable campaign planning decisions."""
from __future__ import annotations
from typing import Any, Iterable, Mapping, Sequence
from theseus_contracts import CampaignId, PlanDecision, StatisticsEvent, StatisticsEventType
from theseus_contracts.serialization import SerializationError, loads_object
from .projections import STATISTICS_PROJECTION_MAX_BATCH, StatisticsProjectionStore
from .store import StatisticsEventStore
PLANNER_DECISION_PRODUCER_ID = "theseus.planner.decisions"

def plan_decision_statistics_events(
    *,
    campaign_id: CampaignId | str,
    plan_id: str,
    decisions: Sequence[PlanDecision],
    timestamp: str,
    artifact_sha256: str = "",
) -> tuple[StatisticsEvent, ...]:
    # Convert the frozen planner ledger into deterministic canonical statistics events.
    current_campaign = campaign_id if isinstance(campaign_id, CampaignId) else CampaignId(str(campaign_id))
    normalized_plan_id = str(plan_id).strip()
    normalized_artifact_sha256 = str(artifact_sha256).strip()
    if not normalized_plan_id:
        raise ValueError("plan_id must be a non-empty string")
    rows = tuple(decisions)
    mutant_ids = tuple(item.mutant_id for item in rows)
    if len(mutant_ids) != len(set(mutant_ids)):
        raise ValueError("planner decision statistics contain duplicate mutant identities")
    return tuple(
        StatisticsEvent.create(
            StatisticsEventType.PLAN_DECIDED,
            PLANNER_DECISION_PRODUCER_ID,
            sequence,
            {
                **decision.to_dict(),
                "artifact_sha256": normalized_artifact_sha256 or None,
            },
            timestamp=timestamp,
            campaign_id=current_campaign,
            plan_id=normalized_plan_id,
            mutant_id=decision.mutant_id,
        )
        for sequence, decision in enumerate(rows)
    )

def _outbox_receipt(row: Mapping[str, Any]) -> Mapping[str, Any]:
    # Decode one durable outbox receipt without accepting an ambiguous payload shape.
    raw = row.get("payload")
    try:
        value = loads_object(raw) if isinstance(raw, (str, bytes)) else raw
    except (SerializationError, TypeError, ValueError) as exc:
        raise ValueError("plan topology outbox payload is not valid canonical JSON") from exc
    if not isinstance(value, Mapping):
        raise ValueError("plan topology outbox payload must be an object")
    return value

def plan_decision_events_from_outbox(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[StatisticsEvent, ...]:
    # Restore canonical planner events only from committed plan-topology outbox receipts.
    events: list[StatisticsEvent] = []
    for row in rows:
        if str(row.get("event_type", "")) != "mutation.plan_topology":
            continue
        receipt = _outbox_receipt(row)
        payload = receipt.get("payload")
        if not isinstance(payload, Mapping):
            raise ValueError("plan topology outbox receipt has no payload object")
        row_campaign_id = str(row.get("campaign_id", "")).strip()
        receipt_campaign_id = str(receipt.get("campaign_id", "")).strip()
        receipt_effect_type = str(receipt.get("effect_type", "")).strip()
        if receipt_effect_type != "mutation.plan_topology":
            raise ValueError("plan topology outbox event type conflicts with its receipt")
        if not row_campaign_id or row_campaign_id != receipt_campaign_id:
            raise ValueError("plan topology outbox campaign identity conflicts with its receipt")
        raw_decisions = payload.get("plan_decisions")
        if raw_decisions is None:
            # Receipts created before PR21 carry no planner statistics evidence.
            continue
        if not isinstance(raw_decisions, (list, tuple)):
            raise ValueError("plan topology outbox decisions must be an array")
        decisions = tuple(
            PlanDecision.from_dict(item)
            for item in raw_decisions
            if isinstance(item, Mapping)
        )
        if len(decisions) != len(raw_decisions):
            raise ValueError("plan topology outbox contains a malformed planner decision")
        campaign_id = row_campaign_id
        plan_id = str(payload.get("plan_id", "")).strip()
        raw_campaign = payload.get("campaign")
        stored_plan_id = str(raw_campaign.get("plan_id", "")).strip() if isinstance(raw_campaign, Mapping) else ""
        created_at = str(row.get("created_at", "")).strip()
        if not campaign_id or not plan_id or not created_at:
            raise ValueError("plan topology outbox is missing campaign, plan or timestamp identity")
        if stored_plan_id != plan_id:
            raise ValueError("plan topology outbox plan identity conflicts with its campaign projection")
        events.extend(
            plan_decision_statistics_events(
                campaign_id=campaign_id,
                plan_id=plan_id,
                decisions=decisions,
                timestamp=created_at,
                artifact_sha256=str(payload.get("plan_artifact_sha256", "")),
            )
        )
    return tuple(events)

def project_plan_decision_outbox(
    rows: Iterable[Mapping[str, Any]],
    *,
    event_store: StatisticsEventStore,
    projection_store: StatisticsProjectionStore,
) -> tuple[int, int]:
    # Append replay-safe planner events and advance every pending statistics summary batch.
    events = plan_decision_events_from_outbox(rows)
    appended, replays = event_store.append_many(events) if events else (0, 0)
    while True:
        projected = projection_store.project(max_events=STATISTICS_PROJECTION_MAX_BATCH)
        if not projected.has_more:
            break
    return appended, replays

__all__ = [
    "PLANNER_DECISION_PRODUCER_ID",
    "plan_decision_events_from_outbox",
    "plan_decision_statistics_events",
    "project_plan_decision_outbox",
]
