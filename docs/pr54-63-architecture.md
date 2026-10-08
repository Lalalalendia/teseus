# Roadmap continuation: release, remote execution, and advisory layers

This document describes the continuation after the closed local execution path.
The local execution invariants remain the authority: one fresh pytest process per
mutant, immutable prepared artifacts, verified workspace integrity, and durable
canonical evidence.

## Install and identify a runtime (PR54)

Build or install the single root distribution. The supported CLI remains the
existing `theseus` entry point; no second launcher or `PYTHONPATH` workaround is
required.

```powershell
python -m pip install . --no-deps
python -m theseus_local --version
python -m theseus_local runtime --json
```

`theseus runtime --json` reports the Teseus version, Python version, platform,
protocol/schema versions, and a path-independent runtime fingerprint. Coordinators
and workers reject incompatible fingerprints before assignment. Durable state must
be upgraded explicitly; it is never silently interpreted by a different protocol.

## Remote protocol (PR55)

`RemoteExecutionRequest` is immutable and deterministic JSON. It binds execution
attempt, evidence, mutation, project snapshot, prepared artifact, test plan,
argv, environment subset, and deadline before delivery. It does not carry Python
objects or pickle data.

`RemoteExecutionResult` contains physical facts only: process start/exit,
timeout/cancellation, timings, artifact references, runtime identity, and
workspace integrity. A worker never declares `killed` or `survived`; those remain
coordinator-owned semantic classifications.

## Worker and scheduler (PR56–PR57)

`RemoteWorkerRuntime` uses the existing `LocalProcessBackend`. A worker daemon may
remain alive, but each request still starts a fresh OS process. Registration
advertises runtime identity, capabilities, platform, backend, and slots.

`DistributedScheduler` persists pending requests, leases, retries, workers, and
authoritative results. Transport may deliver a request more than once, but an
`EvidenceIdentity` has at most one authoritative terminal result. Expired leases
are requeued and late results are stale; dispatch is bounded by worker capacity.

## Content-addressed artifacts (PR58)

`ContentAddressedArtifactStore` keys bytes by SHA-256, publishes uploads through a
temporary file and atomic replace, verifies every read, and protects active
artifacts with durable pins. `ProjectSnapshot` identities depend on immutable
content, never on `C:\` paths, mtimes, or file sizes alone.

## Isolation boundary (PR59)

The first supported trust model is a trusted coordinator and worker host running
an untrusted mutated project/test process. The process receives an explicit cwd,
bounded timeout, an allow-listed environment, and process-tree cleanup. Output
limits are diagnostics/enforcement in the reference backend; unsupported OS hard
resource limits are not advertised as guarantees.

## Acceptance (PR60)

Compare local and remote semantic results by mutation/evidence identities and
terminal classification. Run recovery cases for worker/coordinator crash,
disconnect, leases, duplicate delivery, timeout, cancellation, artifact
corruption, version mismatch, and hostile child processes. Scale tests measure
critical-path wall time, dispatch, transfer bytes, cache hits, retention, and
orphan processes.

## Advisory AI and operations (PR61–PR63)

AI is a projection over authoritative reports. If it is unavailable or fails, the
campaign still runs and its killed/survived/infrastructure classifications do not
change. AI mutation candidates must pass the ordinary syntax, scope, identity,
immutable-artifact, and execution pipeline; there is no AI bypass.

Operational campaign views are projections of canonical reports and durable
state. They do not create a second campaign database or a second evidence
authority.
