import json
from pathlib import Path

import pytest

from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    CampaignMode,
    EngineEvent,
    EngineEventType,
    EventJournal,
    MessageEnvelope,
    ProjectDescriptor,
    ProjectId,
    MutationScope,
    TestCommandDescriptor as CommandDescriptor,
    RevisionId,
)
from theseus_contracts.errors import IncompatibleProtocolError, MissingFieldError, UnsupportedSchemaError
from theseus_contracts.project import EnvironmentDescriptor, IsolationProfile, RepositoryRevision


def test_campaign_dto_roundtrip_ignores_future_fields_and_preserves_unicode_paths() -> None:
    # Keep the public request JSON-only while allowing future producers to add fields.
    configuration = CampaignConfiguration(
        campaign_id=CampaignId("cmp_unicode"),
        project=ProjectDescriptor(
            project_id=ProjectId("project_1"),
            display_name="Платёжный сервис",
            root_path=r"C:\work\Тесей\project",
            revision=RepositoryRevision(RevisionId("rev_1"), git_revision="abc123"),
            environment=EnvironmentDescriptor("env_1", "3.12", "8.3", ("pytest-xdist",)),
            test_command=CommandDescriptor(("python", "-m", "pytest", "tests/test_тест.py")),
            isolation=IsolationProfile(mode="copy"),
        ),
        scope=MutationScope(source_path="src/доступ.py", function="allow_access", operators=("condition_to_not",)),
        budget=CampaignBudget(max_mutants=10, max_workers=2),
        mode=CampaignMode.STANDARD,
    )
    wire = configuration.to_dict() | {"future_field": {"ignored": True}}
    restored = CampaignConfiguration.from_dict(wire)
    assert restored == configuration
    assert restored.project.root_path == r"C:\work\Тесей\project"
    assert json.loads(json.dumps(wire, ensure_ascii=False))["scope"]["source_path"] == "src/доступ.py"


def test_event_protocol_accepts_unknown_event_and_old_schema() -> None:
    # Preserve unknown event types and older compatible schema versions for replay.
    event = EngineEvent.create(
        "future_event",
        "cmp_1",
        {"value": "данные"},
        sequence=2,
        timestamp="2026-08-03T00:00:00Z",
    )
    wire = event.to_dict() | {"future_field": 42}
    restored = EngineEvent.from_dict(wire)
    old = EngineEvent.from_dict(wire | {"schema_version": 0})
    assert restored.event_type == "future_event"
    assert restored.known_type is False
    assert restored.payload["value"] == "данные"
    assert old.schema_version == 0


def test_event_journal_deduplicates_and_orders_out_of_order_events(tmp_path: Path) -> None:
    # Make retries idempotent while presenting consumers a deterministic sequence order.
    first = EngineEvent.create(
        EngineEventType.MUTANT_COMPLETED,
        "cmp_1",
        {"status": "killed"},
        sequence=2,
        event_id="evt_2",
        timestamp="2026-08-03T00:00:02Z",
    )
    second = EngineEvent.create(
        EngineEventType.CAMPAIGN_PREPARATION_STARTED,
        "cmp_1",
        {},
        sequence=1,
        event_id="evt_1",
        timestamp="2026-08-03T00:00:01Z",
    )
    journal = EventJournal([first, second, first])
    assert len(journal) == 2
    assert [item.event_id.value for item in journal.events()] == ["evt_1", "evt_2"]

    from theseus_contracts.events import JsonlEventSink, read_event_journal

    path = tmp_path / "events.jsonl"
    sink = JsonlEventSink(path)
    sink.publish(first)
    sink.publish(second)
    assert [item.event_id.value for item in read_event_journal(path).events()] == ["evt_1", "evt_2"]


def test_protocol_rejects_missing_and_incompatible_headers() -> None:
    # Fail closed on message identity and protocol major version mismatches.
    base = {
        "protocol_version": 1,
        "schema_version": 1,
        "message_type": "test",
        "message_id": "msg_1",
        "created_at": "2026-08-03T00:00:00Z",
        "payload": {},
    }
    with pytest.raises(MissingFieldError):
        MessageEnvelope.from_dict({key: value for key, value in base.items() if key != "message_id"})
    with pytest.raises(IncompatibleProtocolError):
        MessageEnvelope.from_dict(base | {"protocol_version": 2})
    with pytest.raises(UnsupportedSchemaError):
        MessageEnvelope.from_dict(base | {"schema_version": 2})
