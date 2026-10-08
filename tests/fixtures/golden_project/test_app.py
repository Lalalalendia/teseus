from app import classify


def test_classify_positive():
    # Keep the fixture assertion aligned with the direct golden campaign command.
    assert classify(1) == 2
