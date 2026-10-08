# Getting started

From a clean environment:

```powershell
Set-Location D:\teseus
py -3.11 -m venv .venv
D:\teseus\.venv\Scripts\python.exe -m pip install . --no-deps
D:\teseus\.venv\Scripts\python.exe -m theseus_local runtime --json
```

Run a local campaign with a shell-free test command:

```powershell
D:\teseus\.venv\Scripts\python.exe -m theseus_local run D:\my-project src\pkg\service.py `
  --function calculate --max-mutants 10 --workers 2 --json `
  --test-command D:\my-project\.venv\Scripts\python.exe -m pytest -q
```

Read the `canonical.report.json` path from the summary. `killed` means a selected
test failed under the mutation; `survived` means it passed; `invalid` means the
mutation was not executable; `infrastructure_error` means the environment or
execution boundary failed and is not evidence of survival.

Press `Ctrl+C` to cancel. Keep the printed SQLite/report paths and use the
`campaign status` and `campaign recover` commands to inspect or resume the same
campaign identity. Temporary workspaces are disposable only after no campaign or
recovery process is active.
