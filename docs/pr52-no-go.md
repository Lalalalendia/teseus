# PR52 experiment registry

This registry records the local optimization decisions for the closed execution architecture. An experiment is not an implementation requirement: a safe `NO-GO` is a valid result when the correctness proof is incomplete.

## Native filesystem clone / reflink

Decision: `NO-GO` as a reference backend.

The current capability probe proves only the local hardlink capability used by the optional `hardlink-cow` workspace materializer. It does not claim ReFS, Btrfs, APFS, XFS, or another native clone primitive without a platform-specific proof. The selected fallback is a byte-for-byte copy. A backend may be revisited only with a capability proof, source-integrity proof, restoration proof, and an A/B equivalence matrix.

## Immutable mutation artifacts

Decision: `KEEP`.

Prepared mutation bytes are materialized once, content-addressed by their rendered hash, and selected during execution from the durable prepared snapshot. The round-trip, direct-switch, untrusted-source, and runner-preference checks live in `tests/test_pr49_immutable_mutation_artifact.py`.

## Import-time activation / process-global COW hooks

Decision: `NO-GO`.

Import-time activation would move isolation policy into a persistent interpreter and would require proving module-cache, plugin, child-process, cancellation, cleanup, and recovery equivalence. The production path therefore keeps one fresh pytest process per mutant and does not install a process-global COW audit hook. The guard is covered by `tests/test_pr49_hardlink_cow_worker_workspace.py`.

## Measurement closure

The authority measurement used the exact local benchmark command with 24 selected mutants, worker counts 1/2/4, no escalation, three cold and three warm observations, and a separate storage probe. The retained decision is `KEEP` for the correctness-preserving worker workspace optimization: the correctness suite passed, process isolation remained one fresh pytest process per mutant, worker scaling was positive, and the measured storage probe recovered space after cleanup. The storage probe is kept as a separate fingerprint epoch and is not merged into the authority medians.

No old cheap activation, direct source switching, or process-global import workaround is a registered follow-up. Reopening one requires a new assumption and a new proof record.

The resulting production architecture and PR53 closure checklist are recorded in [`pr53-closure.md`](pr53-closure.md).
