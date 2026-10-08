from src_fixture import classify


def test_classify():
    # Verify the conventional src-layout fixture contract.
    assert classify(1) == 2
