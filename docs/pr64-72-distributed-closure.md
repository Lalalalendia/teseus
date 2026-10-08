# PR64–PR72 distributed execution continuation

This document records the network continuation after the local/reference
control-plane closure in `pr54-63-closure.md`.

The design keeps the existing `RemoteExecutionRequest` and
`RemoteExecutionResult` contracts. The network layer only delivers those
contracts and the worker control messages; it does not classify mutants,
choose leases, or make a result authoritative.

## Implemented boundaries

| Roadmap | Delivered boundary | Acceptance evidence |
| --- | --- | --- |
| PR64 | Versioned transport SPI, bounded length-prefixed TCP framing, partial/coalesced/large frames, fail-closed deadlines, reconnect, duplicate-result fencing, cancellation, and real coordinator–worker E2E | `tests/test_network_transport_e2e.py`, `tests/test_network_artifacts.py` |
| PR65 | Durable worker identity plus ephemeral session identity; signed enrollment with protocol/runtime/capability/platform/slot checks; credential expiry and revocation | `tests/test_network_control_plane_adversarial.py` |
| PR66 | Network heartbeat, durable lease renewal, session replacement, reconnect requeue, multi-slot dispatch, capacity fencing, and late-result rejection | `tests/test_network_control_plane_adversarial.py`, `tests/test_network_transport_e2e.py` |
| PR67 | Manifest negotiation, bounded chunk streaming, SHA-256 verification, atomic CAS publication, corrupt-cache repair, partial-transfer discard, source pinning, and warm-cache deltas | `tests/test_network_artifacts.py`, `tests/test_network_transport_e2e.py` |
| PR68 | Single active coordinator authority, renewable lease, monotonically increasing fencing epoch, standby takeover, and old-epoch rejection | `theseus_local/coordinator_ha.py`, `tests/test_network_control_plane_adversarial.py` |
| PR69 | Correlation IDs, phase timeline, transfer/execution/network timings, counters, and best-effort diagnostic sinks | `tests/test_network_control_plane_adversarial.py` |
| PR70 | Real two-host socket campaigns, 24-request E2E, 100+ scale gate, worker loss/requeue, restart, revocation, corrupt cache, cancellation, drop, and failover paths | `tests/test_network_transport_e2e.py`, `tests/test_network_scale_acceptance.py`, `tests/test_network_artifacts.py` |
| PR71 | Worker start/listing, remote run alias, durable scheduler status/cancel projection, and worker diagnostics | `theseus_local/cli.py`, `tests/test_network_cli.py` |
| PR72 | Clean-wheel network smoke, compatibility/recovery matrix, documented commands, and dedicated CI scale lane | `tests/test_distribution_acceptance.py`, `maintenance.py`, this document |

## Control-plane invariants

- A worker identity is durable under its worker root; every connection gets a
  new session and a new runtime instance fence.
- An admitted registration is bound to the signed enrollment payload. An
  unauthorized socket receives no artifact, assignment, or result authority.
- A lease is extended only by the authenticated current worker instance. A
  disconnected or expired lease is requeued; a late physical result is stale.
- The scheduler is the only authority for terminal evidence. Metrics, event
  sinks, worker journals, and operational CLI views are projections.
- CAS objects are transferred by identity, streamed in bounded chunks, checked
  by size and SHA-256, and atomically published. Source objects remain pinned
  until their assignment finishes.
- Coordinator messages carry the current leader epoch. A previous epoch cannot
  accept results after takeover.
- Every remote attempt still delegates to `RemoteWorkerRuntime`, which starts a
  fresh child process and cleans its disposable workspace.

## Local acceptance commands

Run the focused network gate:

```text
D:\teseus\.venv\Scripts\python.exe -m pytest -q tests\test_network_transport_e2e.py tests\test_network_artifacts.py tests\test_network_control_plane_adversarial.py tests\test_network_cli.py
```

Run the 100+ campaign gate:

```text
D:\teseus\.venv\Scripts\python.exe -m pytest -q tests\test_network_scale_acceptance.py -m scale
```

Set `THESEUS_NETWORK_SCALE_SIZE=500` for the larger practical campaign
variant. The CI scale lane uses the 100-mutant default and keeps the 500-mutant
variant available for a host-sized acceptance run.

For a clean installed wheel, build with `python -m pip wheel . --no-deps
--no-build-isolation` and set `THESEUS_RELEASE_WHEEL` before running the release
lane. That lane starts a worker through the installed `theseus worker start`
entry point and completes a real socket request.

## Operator workflow

On the worker host:

```text
theseus worker start --host COORDINATOR_HOST --port PORT --secret SECRET --worker-id worker-a --root D:\theseus-worker --slots 1
```

On the coordinator host, inspect enrollment and durable scheduler state:

```text
theseus workers --registry PATH\enrollment.json --secret SECRET --json
theseus network status --state PATH\scheduler.json --json
```

Bound requests can be executed with either `theseus network run REQUESTS.json`
or the compact `theseus run --remote --requests REQUESTS.json` alias. A
single evidence item can be fenced from the durable state with
`theseus network cancel --state PATH\scheduler.json --evidence EVIDENCE_ID`.

The worker registry is diagnostic metadata. It does not replace the durable
scheduler or canonical report and cannot make a result accepted.

## Release status

PR72 is complete only when the focused network gate, the scale/chaos gate, the
full regression suite, and the clean-wheel release lane are green on the target
host. The final status is deliberately not inferred from the presence of a
socket adapter alone.
