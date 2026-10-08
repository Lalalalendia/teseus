def test_generated(value):
    # Keep the runtime-generated test deterministic for collection snapshots.
    assert value > 0
