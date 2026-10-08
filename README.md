# Teseus Developer Preview 0.1

Teseus is a local Python mutation-testing engine. The preview runs from VS Code or PowerShell, mutates selected Python code in an isolated workspace, executes the project test command, restores source bytes, and publishes canonical JSON and Markdown reports.

The first product path is fully offline. AI providers, containers, remote workers, an installer, and an EXE are not required.

## Requirements

- Windows with Python 3.11 or newer;
- a project tested with `pytest` or another shell-free command;
- the Teseus repository and a local virtual environment.

## Install for development

From the Teseus repository root:

```powershell
Set-Location D:\teseus; py -3.14 -m venv .venv; D:\teseus\.venv\Scripts\python.exe -m pip install -e . --no-deps
```

Verify the command:

```powershell
Set-Location D:\teseus; D:\teseus\.venv\Scripts\python.exe -m theseus_local --help
```

## Run a mutation campaign

Minimal command:

```powershell
Set-Location D:\teseus; D:\teseus\.venv\Scripts\python.exe -m theseus_local run D:\projects\sample app.py --function choose --max-mutants 10 --workers 2
```

For a `src` layout, pass the project-relative source path:

```powershell
Set-Location D:\teseus; D:\teseus\.venv\Scripts\python.exe -m theseus_local run D:\projects\sample src\sample\service.py --function calculate --max-mutants 20 --workers 2
```

Use `--json` for a stable machine-readable summary:

```powershell
Set-Location D:\teseus; D:\teseus\.venv\Scripts\python.exe -m theseus_local run D:\projects\sample app.py --function choose --max-mutants 10 --workers 2 --json
```

By default, Teseus runs the current interpreter with `-m pytest -q`. A custom command is shell-free and must be the final option:

```powershell
Set-Location D:\teseus; D:\teseus\.venv\Scripts\python.exe -m theseus_local run D:\projects\sample src\sample\service.py --function calculate --test-command D:\projects\sample\.venv\Scripts\python.exe -m pytest -q -o pythonpath=src tests
```

Useful options:

- `--operator <name>` can be repeated;
- `--max-mutants <n>` bounds the campaign;
- `--workers <n>` selects local parallel workers;
- `--test-timeout <seconds>` bounds one test execution;
- `--no-escalation` disables broader fallback test levels;
- `--reports-dir <path>` selects an external report root;
- `--campaign-id` and `--project-id` make a run replayable by explicit identity.

## Read the result

The console and JSON summary contain:

- campaign status;
- discovered and executed mutant counts;
- killed, survived, invalid, timeout, and infrastructure-failure counts;
- mutation score;
- elapsed time;
- canonical report path;
- durable campaign database path.

The authoritative report is `canonical.report.json`. A human-readable `canonical.report.md` is published beside it. Mutations run in a copied workspace outside the checkout; the selected source file in the main project must remain byte-for-byte unchanged.

## Recover an interrupted campaign

A Ctrl+C interruption exits with code `130` and prints the exact database path and recovery command. Recovery can also be started manually:

```powershell
Set-Location D:\teseus; D:\teseus\.venv\Scripts\python.exe -m theseus_local campaign recover D:\path\to\campaign.sqlite3 --campaign-id campaign-id --json
```

Inspect durable state without running work:

```powershell
Set-Location D:\teseus; D:\teseus\.venv\Scripts\python.exe -m theseus_local campaign status D:\path\to\campaign.sqlite3 --campaign-id campaign-id
```

Print an existing report:

```powershell
Set-Location D:\teseus; D:\teseus\.venv\Scripts\python.exe -m theseus_local campaign report D:\path\to\canonical.report.json
```

Local state normally lives in a sibling `.theseus-state` directory keyed by project and campaign identity. Delete that state only when no campaign or recovery process is active.

## Result interpretation

- `killed`: at least one selected test failed under the mutant;
- `survived`: selected tests passed under the mutant;
- `invalid`: the mutant could not produce valid executable Python;
- `timeout`: execution exceeded its bound;
- `infrastructure_error`: the test environment or worker failed independently of mutation semantics.

A baseline failure is not a mutation result. The command exits non-zero, preserves the checkout, and includes a diagnostic error and durable paths.

## Preview support boundary

The acceptance target covers:

- ordinary and `src` layouts;
- function and file scopes;
- synchronous tests and async application code executed by pytest;
- one or multiple local workers;
- paths containing spaces and Unicode;
- zero-mutant campaigns;
- baseline failures and interrupted-campaign recovery;
- canonical JSON and Markdown reports.

Complex monorepos, remote workers, containers, public installers, and packaged executables remain later work.

## Optional survivor intelligence and AI work

The existing `theseus_survivor_lab` and survivor-analysis components remain in the repository. They are optional and isolated from the mutation hot path. Offline classification, deterministic hypotheses, proposal evidence, human review, and export are preserved; real network providers are deferred until the offline mutator is stable.
