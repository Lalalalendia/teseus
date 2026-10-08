# PR53 — Advanced local execution closure

Status: `CLOSED` for the supported Windows/offline local execution architecture.

## Frozen production path

Every authoritative mutant attempt follows one durable sequence:

```text
campaign plan
  -> durable prepared mutation snapshot
  -> worker assignment and lease
  -> capability-selected workspace materialization
  -> prepared immutable mutation artifact
  -> one fresh shell-free pytest process
  -> restore and integrity verification
  -> durable execution/evidence spool
  -> coordinator fan-in and canonical report
```

The worker may retry an infrastructure failure with a new execution attempt identity. It does not regenerate a mutation from mutable source or silently change the workspace backend after pytest has started.

## Supported capability registry

| Capability | Supported contract |
| --- | --- |
| Execution backend | `local-process` |
| Workspace backend | verified `hardlink-cow`, or `copy` fallback |
| Mutation artifact | prepared once, immutable, content-addressed |
| Process isolation | one fresh pytest process per executed mutant |
| Native clone/reflink | disabled without a platform proof |
| Import-time/process-global COW | disabled |

The workspace manifest stores the observed capability proof, selected backend, source fingerprint, workspace fingerprint, and linked/copied counts. A mixed hardlink/copy tree is rebuilt as a plain copy before an external process can run.

## Identity and recovery invariants

- `MutationIdentity` identifies the source mutation and is independent of execution attempts.
- `EvidenceIdentity` identifies one durable terminal observation.
- `ExecutionAttemptIdentity` identifies the execution id plus attempt number.
- Replays are idempotent; stale leases and stale shard attempts cannot publish current evidence.
- One current mutant has at most one authoritative terminal evidence row.
- Worker/coordinator crash, timeout, cancellation, spool replay, partial report, and duplicate delivery paths retain durable authority or fail closed as `infrastructure_error`.
- Source, prepared snapshot, mutation artifact, restore, and workspace fingerprints are checked at their boundaries.

## Storage and scale contract

The durable campaign database and canonical reports are authoritative. Worker spool and recovery records remain available while recovery is possible; disposable workspaces and temporary process artifacts are cleanup material after the campaign is terminal and inactive. Benchmark output is kept outside the checkout.

The acceptance matrix covers standard and `src` layouts, sync/async code, file/function scopes, one/two/four local workers, spaces/Unicode paths, Windows process cleanup, the canonical 24-mutant matrix, and a 100-mutant scale run. The benchmark reports wall time, throughput, worker scaling, shard imbalance, process spawns, workspace setup, pytest execution, fan-in, cleanup, and retained storage evidence.

## Verification gates

- `ruff check . --no-cache` is the configured static gate and is green.
- No repository or CI vulture command is configured (`NO_REPO_LINT_COMMAND_FOUND`). The scoped audit
  `vulture . --exclude '.venv,md_dump,tests,theseus_survivor_lab/tests' --min-confidence 80`
  completed with exit code 0.
- The final focused architecture/recovery/identity/storage gate passed `62 tests`; the full current
  suite passed `843 passed, 1 skipped` with `pytest -n auto`.
- Focused PR53/recovery/identity/workspace gates and the full pytest suite are run from an external Windows basetemp to keep generated state outside the checkout.

## Recorded scale evidence

- The authoritative 24-mutant matrix is
  `D:\teseus_benchmarks\benchmark-20260811T133419740456Z-569921ce\project-benchmark.report.json`.
  All six cold/warm x 1/2/4-worker scenarios completed with `selected=24`, `completed=24`, and
  `process_spawn_count=24`.
- The final 100-mutant scale report is
  `D:\teseus_benchmarks\benchmark-20260811T155156319587Z-bc39bc37\project-benchmark.report.json`.
  It is `completed=true`: cold and warm w4 both selected/completed 100 mutants and recorded 100
  physical pytest launches; wall time was about 287.77s cold and 267.51s warm, with shard cost
  imbalance `1.001295` and warm speedup `1.075739`.
- Successful benchmark cleanup left no disposable workspace files under the final lane's
  `state\workspaces`; durable worker/report evidence remains for retention and audit. After the final
  full suite no Teseus worker, engine, or pytest child process remained.

The PR52 experiment decisions remain in [`pr52-no-go.md`](pr52-no-go.md). This document freezes the resulting local architecture rather than reopening those experiments.
