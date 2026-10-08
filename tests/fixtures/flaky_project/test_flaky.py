import os


def test_controlled_outcome():
    # Toggle the outcome explicitly instead of relying on wall-clock randomness.
    assert os.environ.get("THESEUS_E00_FLAKY_PASS") == "1"
