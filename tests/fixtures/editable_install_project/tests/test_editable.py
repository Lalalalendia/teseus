from editable_fixture import classify


def test_classify():
    # Verify that the editable-style package exposes the expected production function.
    assert classify(1) == 2
