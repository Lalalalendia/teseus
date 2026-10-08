# E24 isolation report

## Allowed imports

Production package imports only Python standard-library modules and its own modules. `pyproject.toml` declares an empty dependency list.

## Forbidden runtime boundaries

The package has no import statements for:

```text
theseus_local
gallifrey_mutation
test_intelligence_unified_v1.runner
test_intelligence_unified_v1.engine
test_intelligence_unified_v1.workers
test_intelligence_unified_v1.coordinator
```

It also has no filesystem/database/network client. AST analysis operates on strings already present in the request and never calls `exec`, `eval`, import machinery, subprocesses, or project code.

## Side-effect inventory

The service only constructs immutable dataclasses. The CLI writes the explicitly requested JSON or Markdown output path. No campaign artifact, worker spool, source file, control DB, Knowledge DB, or Git repository is written.

## Provider boundary

`RepairProposalProvider` receives `ProviderRequest`, not the original request. The request contains only bounded sanitized text, a category chosen locally, findings, and hypotheses. Provider output is treated as non-authoritative suggestion text and cannot replace classification or validation plan.

## Determinism

`analysis_id`, `proposal_set_id`, and `result_id` use sorted canonical JSON. Physical absolute paths, time, PID, and raw process output are excluded from semantic identity; source and inline test contents contribute through validated SHA-256 digests. JSON and Markdown writers emit explicit UTF-8 LF bytes on every platform.

## Integration points reserved for later

The future adapter belongs outside this package and may implement the following port shape without coupling the core to Gallifrey:

```python
class SurvivorAnalysisPort(Protocol):
    def analyze(self, request: SurvivorAnalysisRequest) -> SurvivorAnalysisResult:
        ...
```
