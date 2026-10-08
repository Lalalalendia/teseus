from __future__ import annotations

from dataclasses import replace

import pytest

from gallifrey_mutation import (
    CampaignState,
    MutationCampaign,
    MutationExecution,
    MutationExecutionState,
    MutationResult,
    MutationShard,
    Success,
)
from theseus_contracts import (
    CampaignBudget,
    CampaignConfiguration,
    CampaignId,
    EnvironmentDescriptor,
    EngineEvent,
    EventId,
    ExecutionId,
    IsolationProfile,
    MessageEnvelope,
    MutantId,
    MutationScope,
    ProjectDescriptor,
    ProjectId,
    RepositoryRevision,
    RevisionId,
    ShardDescriptor,
    ShardId,
    ShardLease,
    TestCommandDescriptor,
    WorkerId,
    deterministic_id,
)
from theseus_contracts.enums import WorkerStatus
from theseus_contracts.errors import IncompatibleProtocolError
from theseus_contracts.serialization import SerializationError


def _configuration() -> CampaignConfiguration:
    # Build one complete nested configuration for wire and domain invariant tests.
    return CampaignConfiguration(
        campaign_id=CampaignId("campaign-invariants"),
        project=ProjectDescriptor(
            project_id=ProjectId("project-invariants"),
            display_name="Invariant fixture",
            root_path="/workspace/project",
            revision=RepositoryRevision(RevisionId("revision-1"), git_revision="abc123"),
            environment=EnvironmentDescriptor(
                fingerprint="environment-1",
                python_version="3.12.0",
                pytest_version="9.1.1",
                plugins=("pytest-cov",),
                declared_env_keys=("FEATURE_FLAG",),
            ),
            test_command=TestCommandDescriptor(("python", "-m", "pytest", "-q")),
            isolation=IsolationProfile(mode="copy", clean_checkout=True, one_process_per_mutant=True),
        ),
        scope=MutationScope(
            source_path="app.py",
            function="choose",
            from_line=10,
            to_line=30,
            mutant_ids=("mutant-1", "mutant-2"),
            operators=("condition_to_not",),
        ),
        budget=CampaignBudget(max_mutants=2, max_seconds=30.0, max_workers=2, max_test_seconds=5.0),
        no_escalation=True,
        reuse_mode="experimental",
    )


def test_identifier_domains_are_isolated_and_empty_values_are_rejected() -> None:
    # Keep identifiers from different domains unequal even when their wire values match.
    assert ProjectId("same") != CampaignId("same")
    assert deterministic_id("mutant", "source", 1).startswith("mutant_")
    with pytest.raises(ValueError, match="non-empty"):
        CampaignId(" ")


def test_nested_contract_roundtrip_is_wire_idempotent() -> None:
    # Preserve every nested contract field through one JSON-compatible round trip.
    configuration = _configuration()
    restored = CampaignConfiguration.from_dict(configuration.to_dict())

    assert restored == configuration
    assert restored.to_dict() == configuration.to_dict()


def test_engine_event_roundtrip_preserves_future_event_and_optional_identity() -> None:
    # Keep unknown event types forward-compatible without dropping worker or shard identity.
    event = EngineEvent.create(
        "future_event",
        CampaignId("campaign-events"),
        {"nested": {"value": 1}, "items": ["a", "b"]},
        sequence=3,
        event_id=EventId("event-1"),
        timestamp="2026-01-01T00:00:00+00:00",
        worker_id=WorkerId("worker-1"),
        shard_id=ShardId("shard-1"),
    )
    restored = EngineEvent.from_json(event.to_json())

    assert restored.to_dict() == event.to_dict()
    assert restored.known_type is False


def test_message_envelope_rejects_new_protocol_and_naive_timestamp() -> None:
    # Refuse incompatible protocol versions and timestamps whose timezone is ambiguous.
    with pytest.raises(IncompatibleProtocolError):
        MessageEnvelope.from_dict(
            {
                "protocol_version": 2,
                "schema_version": 1,
                "message_type": "test",
                "message_id": "message-1",
                "created_at": "2026-01-01T00:00:00+00:00",
                "payload": {},
            }
        )
    with pytest.raises(SerializationError, match="UTC"):
        MessageEnvelope.from_dict(
            {
                "protocol_version": 1,
                "schema_version": 1,
                "message_type": "test",
                "message_id": "message-1",
                "created_at": "2026-01-01T00:00:00",
                "payload": {},
            }
        )


def test_shard_domain_rejects_duplicate_membership_and_negative_cost() -> None:
    # Make immutable shard membership and cost boundaries fail before persistence.
    base = {
        "shard_id": ShardId("shard-invariants"),
        "campaign_id": CampaignId("campaign-invariants"),
        "ordinal": 0,
    }
    with pytest.raises(ValueError, match="unique"):
        MutationShard(mutant_ids=(MutantId("m1"), MutantId("m1")), **base)
    with pytest.raises(ValueError, match="negative"):
        MutationShard(mutant_ids=(MutantId("m1"),), estimated_cost=-1.0, **base)


def test_shard_heartbeat_sequence_is_monotonic() -> None:
    # Reject a delayed heartbeat that would move authoritative lease state backwards.
    shard = MutationShard.from_descriptor(
        CampaignId("campaign-heartbeat"),
        ShardDescriptor(ShardId("shard-heartbeat"), ("m1",)),
        ordinal=0,
    )
    claimed = shard.claim(
        ShardLease(
            worker_id=WorkerId("worker-heartbeat"),
            lease_id="lease-heartbeat",
            status=WorkerStatus.RUNNING,
            lease_seconds=30.0,
            heartbeat_at="2026-01-01T00:00:01Z",
            heartbeat_seq=3,
        ),
        expected_revision=0,
    )
    assert isinstance(claimed, Success)
    renewed = claimed.value.renew_lease(
        ShardLease(
            worker_id=WorkerId("worker-heartbeat"),
            lease_id="lease-heartbeat",
            status=WorkerStatus.RUNNING,
            lease_seconds=30.0,
            heartbeat_at="2026-01-01T00:00:02Z",
            heartbeat_seq=2,
        ),
        expected_revision=claimed.value.revision_number,
    )

    assert renewed.code == "stale_heartbeat"


def test_shard_expiry_is_strictly_after_lease_duration() -> None:
    # Treat the exact lease boundary as live and expire only after the full duration elapsed.
    shard = MutationShard.from_descriptor(
        CampaignId("campaign-expiry"),
        ShardDescriptor(ShardId("shard-expiry"), ("m1",)),
        ordinal=0,
    )
    claimed = shard.claim(
        ShardLease(
            worker_id=WorkerId("worker-expiry"),
            lease_id="lease-expiry",
            status=WorkerStatus.RUNNING,
            lease_seconds=10.0,
            heartbeat_at="2026-01-01T00:00:00Z",
        ),
        expected_revision=0,
    )
    assert isinstance(claimed, Success)

    assert claimed.value.lease_expired("2026-01-01T00:00:10Z") is False
    assert claimed.value.lease_expired("2026-01-01T00:00:10.001Z") is True


def test_execution_completion_is_terminal_and_idempotent() -> None:
    # Allow the exact completion to replay while rejecting a semantic rewrite of terminal evidence.
    execution = MutationExecution(
        execution_id=ExecutionId("execution-invariants"),
        campaign_id=CampaignId("campaign-invariants"),
        shard_id=ShardId("shard-invariants"),
        mutant_id=MutantId("mutant-invariants"),
    )
    started = execution.start(expected_revision=0)
    assert isinstance(started, Success)
    completed = started.value.complete(
        MutationResult.KILLED,
        expected_revision=started.value.revision_number,
        selected_tests=("tests/test_app.py::test_choose",),
        duration_seconds=1.25,
        restore_verified=True,
    )
    assert isinstance(completed, Success)
    duplicate = completed.value.complete(
        MutationResult.KILLED,
        expected_revision=completed.value.revision_number,
        selected_tests=("tests/test_app.py::test_choose",),
        duration_seconds=1.25,
        restore_verified=True,
    )
    conflict = completed.value.complete(
        MutationResult.SURVIVED,
        expected_revision=completed.value.revision_number,
        selected_tests=("tests/test_app.py::test_choose",),
        duration_seconds=1.25,
        restore_verified=True,
    )

    assert duplicate.duplicate is True
    assert conflict.code == "execution_immutable"
    assert completed.value.status is MutationExecutionState.COMPLETE


def test_campaign_progress_is_monotonic() -> None:
    # Prevent fan-in from reducing completed mutant counts after a newer projection was committed.
    campaign = replace(
        MutationCampaign.create(_configuration()),
        status=CampaignState.RUNNING,
        total_mutants=2,
        revision_number=1,
    )
    advanced = campaign.record_progress(1, expected_revision=1)
    assert isinstance(advanced, Success)
    rejected = advanced.value.record_progress(0, expected_revision=advanced.value.revision_number)

    assert rejected.code == "progress_not_monotonic"


def test_test_command_contract_forbids_shell_execution() -> None:
    # Keep process invocation represented as an argv vector with no shell interpretation.
    with pytest.raises(ValueError, match="shell"):
        TestCommandDescriptor.from_dict({"argv": ["pytest"], "shell": True})
    with pytest.raises(ValueError, match="must not be empty"):
        TestCommandDescriptor.from_dict({"argv": []})
