

def pytest_generate_tests(metafunc):
    # Generate nodeids at collection time so AST inventory is not authoritative.
    if "value" in metafunc.fixturenames:
        metafunc.parametrize("value", [1, 2], ids=["one", "two"])
