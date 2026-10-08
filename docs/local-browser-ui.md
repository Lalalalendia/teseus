# Local browser UI

Start the loopback control plane without a pre-existing campaign database:

```powershell
Set-Location D:\teseus
D:\teseus\.venv\Scripts\python.exe -m theseus_ui --port 8765
```

Open `http://127.0.0.1:8765/`. Register a project in **Add local project**,
select it, enter the project-relative Python source and test command, and use
**Create and start campaign**. The campaign detail page polls authoritative
state and exposes start/restart, stop testing, retry, resume and recovery
actions. Reports, event history, workers and diagnostics remain available from
the same page.

The registry and campaign state are stored outside the checkout in the local
UI state directory. Override it with `--state-dir`; add known projects at
startup with repeated `--project-root` options. The server binds only to
`127.0.0.1`.

The older form remains supported when a campaign database is supplied as the
positional argument; that mode is read-only unless an explicit public launch
authority is injected by the embedding application.
