from namespace_fixture.core import classify


def test_classify():
    # Verify the namespace package fixture contract.
    assert classify(1) == 2
