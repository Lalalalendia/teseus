# E-00 baseline fixtures

These small projects are the compatibility matrix for the v1.26 golden baseline.
They are intentionally minimal and deterministic.

| Fixture | Contract |
| --- | --- |
| `golden_project` | Stable mutation IDs, report semantics and restoration hash |
| `editable_install_project` | Package imported through an editable-style `src` layout |
| `src_layout_project` | Conventional `src/` package layout |
| `namespace_project` | Package without `__init__.py` in the namespace directory |
| `dynamic_pytest_project` | Runtime-generated parametrized nodeids |
| `flaky_project` | Explicitly controlled pass/fail behavior for health fixtures |

The fixtures are test inputs, not production applications. Do not add timestamps,
randomness, machine-specific paths or network calls to them.
