# PR54–PR63 closure

The roadmap continuation is implemented as a reference-quality local/remote
execution control plane. The coordinator remains authoritative for campaign
semantics; workers provide physical execution facts only.

| PR | Delivered boundary | Verification |
| --- | --- | --- |
| PR54 | Runtime identity, deterministic fingerprint, package build, clean install, CLI runtime inspection, and explicit compatibility rejection | Final wheel install and `theseus runtime --json`; fixture campaign completed one mutant |
| PR55 | Immutable deterministic JSON request/result contracts, strict schema/version checks, UTC deadline validation, and no pickle transport | Remote protocol contract tests and JSONL worker smoke |
| PR56 | Worker registration, capability/slot advertisement, lifecycle states, fresh-process execution, workspace preflight, restart journal, and replay | Worker lifecycle, timeout cleanup, corruption, restart, and JSONL tests |
| PR57 | Durable scheduler, leases, heartbeats, expiration/requeue, stale-result fencing, idempotent submission, restart recovery, and bounded dispatch | Scheduler recovery and stale-delivery tests |
| PR58 | SHA-256 content-addressed artifacts, immutable project snapshots, verified transfer, atomic publication, pins, cache hits, and eviction | Artifact store and distributed transfer tests |
| PR59 | Trusted coordinator/worker boundary, explicit cwd/environment policy, bounded output diagnostics, deadline enforcement, and process-tree cleanup | Isolation and hostile-child/timeout tests |
| PR60 | Local/remote semantic comparison and recovery/scale/retention/quiescence acceptance helpers | Full acceptance suite and distributed equivalence tests |
| PR61 | Optional advisory analysis projection with unavailable-provider fallback and atomic sidecar output | Deterministic and unavailable analysis tests |
| PR62 | AI mutation candidates normalized through ordinary syntax, scope, identity, artifact, and execution preparation | Candidate validation and identity tests |
| PR63 | Projection-only campaign operations/status views over canonical reports and durable state | Operations projection tests and installed CLI smoke |

## Historical acceptance evidence

The numbers below are the original PR54–PR63 acceptance snapshot. They are kept
for traceability; the corrective re-audit and current gate results are recorded
in [`post-pr63-testing-closure.md`](post-pr63-testing-closure.md).

The final repository checks completed with:

```text
859 passed, 1 skipped in 91.01s
ruff check theseus_local theseus_contracts --no-cache: passed
python -m compileall -q theseus_local theseus_contracts: passed
vulture ... --min-confidence 80: passed
```

The release gate also built `theseus_mutation_platform-1.29.6-py3-none-any.whl`,
installed it without dependencies into a clean virtual environment, reported a
stable runtime fingerprint, and completed a fixture campaign with one discovered,
one executed, and one killed mutant. Advisory analysis and operations projections
were then read successfully from that canonical report.

PR54–PR63 intentionally closed the transport-neutral reference runtime and its
JSONL path. The subsequent real-network continuation is tracked separately in
`pr64-72-distributed-closure.md`; this document remains the historical closure
record for the reference control plane.
