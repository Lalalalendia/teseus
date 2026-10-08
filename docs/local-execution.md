# Local execution path

## Beginner setup

From the Teseus checkout, create the supported local environment and install the repository without pulling network dependencies:

```powershell
Set-Location D:\teseus
py -3.14 -m venv .venv
D:\teseus\.venv\Scripts\python.exe -m pip install -e . --no-deps
D:\teseus\.venv\Scripts\python.exe -m theseus_local --help
```

The supported beginner path is a local, offline campaign:

```powershell
Set-Location D:\teseus
D:\teseus\.venv\Scripts\python.exe -m theseus_local run D:\my-project app.py `
  --function choose --operator condition_to_not --max-mutants 2 --workers 2 `
  --no-escalation --json --test-command D:\teseus\.venv\Scripts\python.exe -m pytest -q
```

The command creates an external state directory and prints a compact summary. The durable authority is `canonical.report.json`; `canonical.report.md` is its human-readable alias. A failed or interrupted run keeps its SQLite database and can be resumed with:

```powershell
D:\teseus\.venv\Scripts\python.exe -m theseus_local campaign recover `
  "D:\path\to\campaign.sqlite3" --campaign-id campaign-id --json
```

The execution contract is deliberately small: the coordinator may remain alive, but every mutant test attempt starts one fresh pytest OS process. Each process is shell-free, has an explicit working directory and output artifact, and is killed as a process tree on timeout or cancellation. Mutation meaning, test selection, reuse, retries, and report status stay above the Execution Backend.

The production sequence is fixed:

`campaign plan` → `durable prepared mutation snapshot` → `worker assignment` → `workspace materialization` → `prepared immutable mutation artifact` → `fresh pytest process` → `restore/integrity` → `durable evidence` → `fan-in`.

The capability model has one execution backend (`local-process`) and two workspace backends: verified `hardlink-cow` or conservative `copy`. Capability selection happens before an attempt. If the probe or materialization cannot prove hardlink-COW, the workspace is rebuilt as a copy before pytest starts. Native clone/reflink and import-time activation are not enabled without a new proof record; see [`pr52-no-go.md`](pr52-no-go.md) and [`pr53-closure.md`](pr53-closure.md).

## Stop, recover, and retain evidence

Press `Ctrl+C` to stop a running campaign. Teseus terminates the worker/process tree, preserves the SQLite authority, spool/recovery records, canonical artifacts already published, and prints a recovery command. An explicit durable cancellation can be requested with:

```powershell
D:\teseus\.venv\Scripts\python.exe -m theseus_local campaign cancel `
  "D:\path\to\campaign.sqlite3" --campaign-id campaign-id
```

After an interruption, inspect first and recover the same campaign identity:

```powershell
D:\teseus\.venv\Scripts\python.exe -m theseus_local campaign status `
  "D:\path\to\campaign.sqlite3" --campaign-id campaign-id
D:\teseus\.venv\Scripts\python.exe -m theseus_local campaign recover `
  "D:\path\to\campaign.sqlite3" --campaign-id campaign-id --json
```

`infrastructure_error` means the test command, worker, process lifecycle, workspace, durable delivery, or integrity boundary failed; it is not evidence that the mutant survived. Baseline failure stops before mutant execution and is reported as a campaign error. A source/artifact fingerprint mismatch also fails closed.

Keep the SQLite database, canonical reports, immutable prepared snapshot, and spool until the campaign is complete and its evidence is archived. Temporary worker workspaces and per-attempt process artifacts may be removed only after no campaign or recovery process is active. State is intentionally outside the project checkout, so cleanup cannot change the measured source tree.
