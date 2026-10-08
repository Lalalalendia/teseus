# Post-PR63 corrective roadmap closure

The first post-PR63 report treated a green regression suite as proof that every
T1–T13 subpoint was closed. The audit showed that this was too broad: several
tests used synthetic physical results, CAS and scheduler tests were only
thread-local, the clean-wheel test relied on the host interpreter for its test
command, and the release/scale/soak/mutation gates were not executable CI
lanes. This document records the corrective work and its evidence. The final
release gate also covers direct test oracles such as `python -c` without
requiring pytest to be installed in the wheel runtime.

## Corrective roadmap

| Stage | Implemented boundary | Evidence | Status |
| --- | --- | --- | --- |
| R0 | Exact campaign accounting, source-tree snapshots, explicit gate matrix | `tests/post_pr63_helpers.py`, full regression | closed |
| R1 | Mandatory source path/hash binding, fail-closed protocol parsing, zero-execution malformed JSONL | `theseus_contracts/remote_protocol.py`, `tests/test_remote_protocol_adversarial.py`, identity matrix | closed |
| R2 | Cross-process CAS index transactions and scheduler state transactions; no lost pin/pending updates | `theseus_local/locking.py`, `tests/test_artifact_integrity_adversarial.py`, `tests/test_distributed_scheduler_races.py` | closed |
| R3 | Clean wheel installation without dependencies or `PYTHONPATH`, broken-wheel rejection, installed JSONL worker subprocess | `tests/test_distribution_acceptance.py` | closed |
| R4 | Real process-tree cleanup on Windows, private environment/cwd, restart and journal recovery | `commands.py`, lifecycle/isolation/E13 tests | closed |
| R5 | Local/remote equivalence for success, failure and timeout; advisory AI and operations remain non-authoritative | `tests/test_local_remote_semantic_equivalence.py`, `tests/test_advisory_projection_non_authority.py` | closed |
| R6 | Real subprocess scale campaign, resource-bounded soak, mutation gate, executable CI lanes and workflow | `tests/test_distributed_scale_acceptance.py`, `maintenance.py`, `.github/workflows/post-pr63-gates.yml` | closed |

The critical mutation lane uses the existing deterministic mutation engine on a
known behavioral mutant and requires a killed result; it does not depend on an
optional third-party mutation-testing package.

## T1–T13 re-audit matrix

| Audit point | Corrective boundary | Evidence | Status |
| --- | --- | --- | --- |
| T1 | Golden control-plane behavior is a mandatory mutation target and CI lane | `tests/test_campaign_golden_regression.py`, `tests/test_control_plane_mutation_gate.py`, `maintenance.py` | closed |
| T2 | Canonical serialization, report reconstruction, and stable identity ordering | `tests/test_canonical_report_contract.py`, `tests/test_control_plane_determinism.py` | closed |
| T3 | Runtime identity plus fail-closed durable schema/version validation | `theseus_local/runtime_identity.py`, `theseus_local/distributed.py`, `theseus_local/remote_worker.py`, `tests/test_persisted_schema_fail_closed.py` | closed |
| T4 | Installed-wheel execution uses the installed interpreter; incomplete wheels fail diagnostically | `tests/test_distribution_acceptance.py` | closed |
| T5 | Protocol/result spoofing and binding matrix rejects mismatched identities and types | `tests/test_remote_protocol_adversarial.py` | closed |
| T6 | Worker lifecycle, slot release, replay, and injected journal failures are explicit | `tests/test_remote_worker_lifecycle_adversarial.py`, `tests/test_control_plane_fault_matrix.py` | closed |
| T7 | Scheduler submit/complete/cancel races are tested across processes | `tests/test_distributed_scheduler_races.py` | closed |
| T8 | CAS pin/evict transactions are tested concurrently with recovery boundaries | `tests/test_artifact_integrity_adversarial.py` | closed |
| T9 | Disposable attempt containers contain nested child writes and process cleanup | `tests/test_execution_isolation_adversarial.py` | closed |
| T10 | Coordinator-owned semantic projection is separated from physical worker facts | `theseus_local/remote_campaign.py`, `tests/test_local_remote_semantic_equivalence.py` | closed |
| T11 | Commit-boundary failures fail closed for scheduler, CAS, and worker journal state | `tests/test_control_plane_fault_matrix.py` | closed |
| T12 | Real subprocess scale, 100-process soak, mutation, release, and Windows control-plane gates | `tests/test_distributed_scale_acceptance.py`, `.github/workflows/post-pr63-gates.yml` | closed |
| T13 | Operations projections are bounded, deterministic, and expose report hashes | `theseus_local/operations.py`, `tests/test_advisory_projection_non_authority.py` | closed |

## Verification results

The final repository regression gate completed with:

```text
969 passed, 4 skipped in 566.78s (0:09:26)
```

The four default-suite skips are expected: one Windows symlink privilege case,
two release acceptance tests without `THESEUS_RELEASE_WHEEL`, and one soak test
without `THESEUS_RUN_SOAK=1`. The release, scale, and soak lanes were executed
separately and are included below.

Additional corrective gates completed:

```text
real scale lane: 5 passed, 1 deselected
100-process soak with resource counters: 1 passed, 5 deselected
clean-wheel and incomplete-wheel acceptance: 2 passed, 1 warning
maintenance CI entrypoint lanes (mutation, scale, soak, release): passed
ruff: passed
python -m py_compile: passed
```

The verified wheel was:

```text
theseus_mutation_platform-1.29.6-py3-none-any.whl
sha256=535c7e1e93c532043b018d8223679a29c02fae852ffc1536dd019c02b73b4f81
```

The release gate installed that wheel into two fresh virtual environments with
`--no-index --no-deps`, removed `PYTHONPATH`, executed a pure-Python check with
the installed interpreter, ran the local campaign through the installed CLI,
and exercised the installed JSONL worker in a separate subprocess. The local
campaign uses an installed-interpreter direct oracle; collection falls back to
the immutable source index instead of importing pytest. A wheel with
`theseus_local/remote_worker.py` removed was installed separately and correctly
failed the installed runtime gate.

Scale scenarios now execute actual worker subprocesses rather than fabricated
`RemoteExecutionResult` objects. The soak gate checks workspaces, output spool,
process registries, and process-local handle/descriptor/thread growth.
