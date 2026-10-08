from __future__ import annotations

import inspect

from theseus_local.coordinator import LocalCampaignCoordinator


def test_durable_delivery_fences_lease_before_atomic_fan_in_boundary() -> None:
    # Fence durable delivery and stop renewal before the later authoritative fan-in block.
    source = inspect.getsource(LocalCampaignCoordinator._execute_parallel_shards)
    delivery_received = source.index('spec["delivery_event_id"] = delivery_payload.event_id')
    delivery_transition = source.index("service.begin_lease_delivery", delivery_received)
    heartbeat_disable = source.index("worker.set_heartbeat_handler(None)", delivery_transition)
    fan_in_block = source.index(
        "# Durable delivery already fenced ownership before this potentially delayed fan-in."
    )
    fan_in = source.index("service.record_shard_result", fan_in_block)
    assert "effect.lease-final-heartbeat" not in source
    assert delivery_received < delivery_transition < heartbeat_disable < fan_in
