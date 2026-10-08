"""Public Theseus engine facade over the current mutation runner."""
from __future__ import annotations
import asyncio
import base64
import binascii
import hashlib
import importlib.metadata
import os
from collections import Counter
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Mapping, Protocol
from theseus_contracts import (
    CampaignConfiguration,
    CampaignId,
    CampaignResult,
    CampaignSummary,
    CollectionSnapshot,
    DiscoverMutantsRequest,
    EngineEvent,
    EngineEventSink,
    EngineEventType,
    ExecutionId,
    ExecuteShardRequest,
    FinalizeCampaignRequest,
    IndexSnapshot,
    MutantDescriptor,
    MutantId,
    MutantExecutionResult,
    MutationDiscoveryResult,
    PrepareCampaignRequest,
    PreparedCampaign,
    PreparedCampaignSnapshot,
    SelectionEvidence,
    SelectionSnapshot,
    ShardDescriptor,
    ShardExecutionResult,
    ShardId,
    TestLevelPlan,
    WorkerId,
    deterministic_id,
)
from theseus_contracts.mutation import PreparedMutant as ContractPreparedMutant
from .commands import env_fingerprint, resolve_python, run_argv
from .impact import SQLiteImpactAdapter
from .index import build_nodeid_validation_context, load_context_map, parse_pytest_collection_output
from .io_utils import FileLock, append_mutant_spool_frame, atomic_write_json, atomic_write_text, build_mutant_spool_frame, ensure_dir, iter_json_lines, read_json, sha256_file, stable_hash, utc_now_iso
from .models import CampaignAccumulator, Mutant
from .mutations import PreparedMutant as InternalPreparedMutant, create_snapshot, restore_snapshot, write_manifest
from .recovery import campaign_input_fingerprint, current_process_birth_token
from .runner import MutationConfig, MutationRunner, _snapshot_from_dict, _with_project_pythonpath
from .test_stats import canonical_nodeid, is_pytest_command, load_mutant_test_observations, stats_db_path
from .preparation_service import PreparedCampaign as InternalPreparedCampaign, prepare_campaign
class MutationEngine(Protocol):
    """Stable async protocol exposed to Theseus application adapters."""
    async def prepare_campaign_async(
        self,
        request: PrepareCampaignRequest,
        event_sink: EngineEventSink | None = None,
    ) -> PreparedCampaign:
        # Define the preparation boundary without importing runner internals in clients.
        ...
    async def discover_mutants_async(
        self,
        request: DiscoverMutantsRequest,
        event_sink: EngineEventSink | None = None,
    ) -> MutationDiscoveryResult:
        # Define the deterministic mutant-discovery boundary for planners.
        ...
    async def collect_campaign_async(
        self,
        campaign_id: CampaignId,
        event_sink: EngineEventSink | None = None,
    ) -> Mapping[str, Any]:
        # Define the authoritative pytest collection boundary for staged execution.
        ...
    async def index_campaign_async(
        self,
        campaign_id: CampaignId,
        event_sink: EngineEventSink | None = None,
    ) -> Mapping[str, Any]:
        # Define the immutable index artifact boundary for staged execution.
        ...
    async def baseline_campaign_async(
        self,
        campaign_id: CampaignId,
        event_sink: EngineEventSink | None = None,
    ) -> Mapping[str, Any]:
        # Define the baseline artifact boundary for staged execution.
        ...
    async def execute_shard_async(
        self,
        request: ExecuteShardRequest,
        event_sink: EngineEventSink | None = None,
    ) -> ShardExecutionResult:
        # Define one isolated execution boundary for a worker adapter.
        ...
    async def finalize_campaign_async(
        self,
        request: FinalizeCampaignRequest,
        event_sink: EngineEventSink | None = None,
    ) -> CampaignResult:
        # Define the report and summary materialization boundary.
        ...
@dataclass
class _CampaignContext:
    # Keep temporary facade state private until the durable control plane owns it.
    configuration: CampaignConfiguration
    config: MutationConfig
    runner: MutationRunner
    prepared: InternalPreparedCampaign
    reports: list[dict[str, Any]]
    snapshot: PreparedCampaignSnapshot
    artifact_dir: Path
    collection_snapshot: CollectionSnapshot | None = None
    index_snapshot: IndexSnapshot | None = None
    baseline_rows: tuple[dict[str, Any], ...] = ()
def _workspace_backend(root: Path) -> str:
    # Restore the worker materialization backend from its ownership manifest while defaulting old workspaces to copy.
    manifest_path = root.parent / f"{root.name}.ownership.json"
    try:
        value = read_json(manifest_path)
    except (OSError, ValueError):
        return "copy"
    if not isinstance(value, Mapping):
        return "copy"
    backend = str(value.get("workspace_backend") or "copy")
    return backend if backend in {"copy", "hardlink-cow"} else "copy"
def _config_from_contract(configuration: CampaignConfiguration) -> MutationConfig:
    # Translate only public DTO fields into the existing runner configuration.
    root = Path(configuration.project.root_path).expanduser().resolve()
    main_root = (
        Path(configuration.project.main_root_path).expanduser().resolve()
        if configuration.project.main_root_path
        else None
    )
    if main_root is not None and (root == main_root or main_root in root.parents):
        raise ValueError("engine workspace must be outside the registered main checkout")
    reports_value = configuration.reports_dir
    reports_dir = Path(reports_value) if reports_value else root / ".theseus" / "reports"
    if not reports_dir.is_absolute():
        reports_dir = root / reports_dir
    command = configuration.project.test_command
    return MutationConfig(
        project_root=root,
        source=configuration.scope.source_path,
        main_root_path=main_root,
        function=configuration.scope.function,
        test_command_argv=command.argv if command else None,
        test_command_cwd=Path(command.cwd).expanduser().resolve() if command and command.cwd else root,
        reports_dir=reports_dir.resolve(),
        max_mutants=configuration.budget.max_mutants,
        from_line=configuration.scope.from_line,
        to_line=configuration.scope.to_line,
        mutant_ids=frozenset(configuration.scope.mutant_ids),
        no_escalation=configuration.no_escalation,
        operators=configuration.scope.operators or None,
        workers=configuration.budget.max_workers,
        workspace_backend=_workspace_backend(root),
        environment_mode=(configuration.project.environment.inherit_policy if configuration.project.environment else "strict"),
        environment_keys=(
            tuple(configuration.project.environment.declared_env_keys)
            + tuple(configuration.project.environment.tracked_variables)
            if configuration.project.environment
            else ()
        ),
        environment_prefixes=(
            tuple(configuration.project.environment.declared_env_patterns)
            + tuple(configuration.project.environment.tracked_prefixes)
            if configuration.project.environment
            else ()
        ),
        environment_ignored=(
            tuple(configuration.project.environment.ignored_variables)
            if configuration.project.environment
            else ()
        ),
        environment_secrets=(
            tuple(configuration.project.environment.secret_variables)
            if configuration.project.environment
            else ()
        ),
        environment_secret_key=configuration.project.project_id.value,
        pytest_plugin_autoload=configuration.project.pytest_plugin_autoload,
    )
def _mutant_descriptor(prepared: InternalPreparedCampaign, mutant: Any) -> MutantDescriptor:
    # Adapt the runner mutant without exposing its dataclass as a public dependency.
    return MutantDescriptor(
        mutant_id=MutantId(mutant.mutant_id),
        mutation=mutant.mutation,
        source_path=prepared.source_rel,
        line_no=mutant.line_no,
        column_no=mutant.column_no,
        original=mutant.original,
        replacement=mutant.replacement,
        operator_version=mutant.operator_version,
        class_name=(
            str(prepared.function_info.get("class_name"))
            if prepared.function_info and prepared.function_info.get("class_name")
            else None
        ),
    )
def _selection_snapshot(prepared: InternalPreparedCampaign) -> SelectionSnapshot | None:
    # Convert the internal frozen selection into the public level-plan DTO.
    selection = prepared.selection
    if selection is None or not hasattr(selection, "to_dict"):
        return None
    value = selection.to_dict()
    levels = tuple(
        TestLevelPlan(
            name=str(item.get("name", "L1")),
            reason=str(item.get("reason", "selection")),
            nodeids=tuple(str(nodeid) for nodeid in item.get("nodeids", [])),
            files=tuple(str(path) for path in item.get("files", [])),
            command_argv=tuple(str(argument) for argument in item.get("command_argv", [])),
        )
        for item in value.get("levels", [])
        if isinstance(item, Mapping)
    )
    raw_evidence = value.get("evidence", {})
    evidence: dict[str, tuple[SelectionEvidence, ...]] = {}
    if isinstance(raw_evidence, Mapping):
        for nodeid, rows in raw_evidence.items():
            if not isinstance(rows, (list, tuple)):
                continue
            evidence[str(nodeid)] = tuple(
                SelectionEvidence.from_dict(item)
                for item in rows
                if isinstance(item, Mapping)
            )
    return SelectionSnapshot(
        snapshot_id=str(value.get("snapshot_id", "")),
        source_path=str(value.get("source_path", prepared.source_rel)),
        source_sha256=str(value.get("source_sha256", prepared.source_sha256)),
        algorithm_version=str(value.get("algorithm_version", "selection-v5")),
        levels=levels,
        selected_tests=tuple(str(item) for item in value.get("selected_tests", [])),
        dropped_nodeids=tuple(str(item) for item in value.get("dropped_nodeids", [])),
        index_version=str(value["index_version"]) if value.get("index_version") is not None else None,
        evidence=evidence,
    )
def _selection_with_test_override(selection: SelectionSnapshot, nodeids: tuple[str, ...]) -> SelectionSnapshot:
    # Build a frozen one-level selection used to execute only the missing partial-reuse tests.
    if not selection.levels:
        return selection
    first = replace(selection.levels[0], nodeids=tuple(nodeids), files=(), command_argv=())
    return replace(selection, levels=(first,), selected_tests=tuple(nodeids))
def _rows_from_report(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    # Read compact reports through their durable journal when result rows are omitted.
    values = report.get("results")
    if isinstance(values, list) and values:
        return [item for item in values if isinstance(item, dict)]
    journal = report.get("results_journal")
    if journal:
        path = Path(str(journal))
        if path.exists():
            return [item for item in iter_json_lines(path) if isinstance(item, dict)]
    return []
def _mutant_result_from_row(
    request: ExecuteShardRequest,
    row: Mapping[str, Any],
) -> MutantExecutionResult | None:
    # Convert one runner row into the stable public result used by spool and shard delivery.
    mutant = row.get("mutant", {})
    mutant_id = str(mutant.get("mutant_id", "")) if isinstance(mutant, Mapping) else ""
    if not mutant_id:
        return None
    return MutantExecutionResult(
        execution_id=ExecutionId(
            deterministic_id(
                "exec",
                request.campaign_id.value,
                mutant_id,
                request.attempt,
            )
        ),
        mutant_id=MutantId(mutant_id),
        status=str(row.get("status", "error")),
        classification_reason=str(row.get("classification_reason", "")),
        restore_verified=bool(row.get("restore_verified", False)),
        level_results=tuple(
            item for item in row.get("level_results", []) if isinstance(item, Mapping)
        ),
        artifact_paths=tuple(str(item) for item in row.get("output_artifacts", [])),
        lease_id=request.lease_id,
        attempt=request.attempt,
        test_observations=tuple(
            dict(item)
            for item in row.get("test_observations", [])
            if isinstance(item, Mapping)
        ),
        duration_seconds=(
            max(0.0, float(row["duration_seconds"]))
            if row.get("duration_seconds") is not None
            else None
        ),
    )
def _attach_result_fingerprints(
    result: MutantExecutionResult,
    request: ExecuteShardRequest,
    root: Path,
) -> MutantExecutionResult:
    # Bind observed tests to coordinator-provided immutable fingerprints before durable commit.
    if not request.test_fingerprints:
        return result
    rows = request.test_fingerprints.get(result.mutant_id.value, {})
    if not rows and result.test_observations:
        raise RuntimeError(
            f"test fingerprints are missing for mutant {result.mutant_id.value}"
        )
    observations: list[dict[str, Any]] = []
    for item in result.test_observations:
        observation = dict(item)
        test_id = canonical_nodeid(root, str(observation.get("test_id", "")))
        fingerprint = rows.get(test_id)
        if not test_id or not fingerprint:
            raise RuntimeError(
                f"test fingerprint is missing for observed test {test_id or '<unknown>'}"
            )
        observation["test_id"] = test_id
        observation["test_fingerprint"] = str(fingerprint)
        observations.append(observation)
    return replace(result, test_observations=tuple(observations))
def _publish_mutant_spool_event(
    request: ExecuteShardRequest,
    result: MutantExecutionResult,
) -> str | None:
    # Fsync one restored result in the shared worker spool before the next mutant begins.
    if request.mutant_spool_root is None:
        return None
    if not result.restore_verified:
        return None
    if (
        request.worker_id is None
        or not request.worker_instance_id
        or request.worker_process_id is None
        or not request.worker_process_birth_token
        or not request.lease_id
        or not request.expected_source_sha256
    ):
        raise RuntimeError("per-mutant spool request has incomplete worker ownership identity")
    frame = build_mutant_spool_frame(
        assignment={
            "campaign_id": request.campaign_id.value,
            "shard_id": request.shard.shard_id.value,
            "lease_id": request.lease_id,
            "attempt": request.attempt,
        },
        worker={
            "worker_id": request.worker_id.value,
            "instance_id": request.worker_instance_id,
            "process_id": request.worker_process_id,
            "process_birth_token": request.worker_process_birth_token,
        },
        source_sha256=request.expected_source_sha256,
        mutant_result=result.to_dict(),
    )
    return append_mutant_spool_frame(
        Path(request.mutant_spool_root) / "mutant-events.jsonl",
        frame,
    )
def _repository_fingerprint(index: Mapping[str, Any]) -> str:
    # Fingerprint every indexed file so a prepared campaign cannot cross repository contents.
    files = index.get("files", {})
    rows = []
    if isinstance(files, Mapping):
        rows = [
            (str(path), str(value.get("sha256", "")))
            for path, value in files.items()
            if isinstance(value, Mapping)
        ]
    return stable_hash(sorted(rows))
def _pytest_configuration_fingerprint(root: Path) -> str:
    # Hash test configuration files that can change collection semantics between restarts.
    names = ("pytest.ini", "pyproject.toml", "setup.cfg", "tox.ini", "conftest.py")
    rows: list[tuple[str, str]] = []
    for name in names:
        path = root / name
        if path.is_file():
            try:
                rows.append((name, sha256_file(path)))
            except OSError:
                rows.append((name, "unreadable"))
    return stable_hash(rows)
def _pytest_version() -> str | None:
    # Capture the exact pytest version used to create and later validate collection artifacts.
    try:
        return importlib.metadata.version("pytest")
    except importlib.metadata.PackageNotFoundError:
        return None
def _pytest_plugin_fingerprint() -> str:
    # Record pytest plugin distribution names and versions without embedding machine-specific paths.
    try:
        entries = importlib.metadata.entry_points(group="pytest11")
        plugins = []
        for item in entries:
            distribution = getattr(item, "dist", None)
            distribution_name = ""
            distribution_version = ""
            if distribution is not None:
                distribution_name = str(getattr(distribution, "name", "") or distribution.metadata.get("Name", ""))
                distribution_version = str(getattr(distribution, "version", "") or "")
            plugins.append((str(item.name), distribution_name, distribution_version))
        plugins = sorted(plugins)
    except (AttributeError, TypeError, importlib.metadata.PackageNotFoundError):
        plugins = []
    return stable_hash({"plugins": plugins})
def _dependency_manifest_fingerprint(root: Path) -> str:
    # Bind environment reuse to declared dependency manifests rather than absolute installation paths.
    names = (
        "requirements.txt",
        "requirements.lock",
        "requirements-dev.txt",
        "requirements-dev.lock",
        "poetry.lock",
        "Pipfile.lock",
        "uv.lock",
    )
    rows: list[tuple[str, str]] = []
    for name in names:
        path = root / name
        if path.is_file():
            try:
                rows.append((name, sha256_file(path)))
            except OSError:
                rows.append((name, "unreadable"))
    return stable_hash(rows)
def _installed_distribution_fingerprint() -> str:
    # Bind reuse to installed distribution versions even when a lockfile was not changed.
    rows: list[tuple[str, str]] = []
    try:
        for distribution in importlib.metadata.distributions():
            name = str(distribution.metadata.get("Name") or getattr(distribution, "name", ""))
            version = str(getattr(distribution, "version", ""))
            if name:
                rows.append((name.lower(), version))
    except (AttributeError, OSError, TypeError):
        return stable_hash("distribution-metadata-unavailable")
    return stable_hash(sorted(set(rows)))
def _engine_environment_fingerprint(
    configuration: CampaignConfiguration,
    root: Path,
) -> str:
    # Bind prepared state to env variables, pytest runtime, plugins and the configured test command.
    declared = configuration.project.environment
    command = configuration.project.test_command
    declared_keys = declared.declared_env_keys if declared is not None else ()
    declared_patterns = declared.declared_env_patterns if declared is not None else ()
    return stable_hash(
        {
            "declared": declared.to_dict() if declared is not None else None,
            "environment": env_fingerprint(
                root,
                include_cwd=False,
                declared_keys=declared_keys,
                declared_patterns=declared_patterns,
                tracked_variables=declared.tracked_variables if declared is not None else (),
                tracked_prefixes=declared.tracked_prefixes if declared is not None else (),
                ignored_variables=declared.ignored_variables if declared is not None else (),
                secret_variables=declared.secret_variables if declared is not None else (),
                inherit_policy=declared.inherit_policy if declared is not None else "allowlisted",
                secret_key=configuration.project.project_id.value,
            ),
            "pytest_version": _pytest_version(),
            "plugins": _pytest_plugin_fingerprint(),
            "dependency_manifests": _dependency_manifest_fingerprint(root),
            "installed_distributions": _installed_distribution_fingerprint(),
            "test_command": list(command.argv) if command is not None else None,
        }
    )
def _prepared_snapshot(
    configuration: CampaignConfiguration,
    prepared: InternalPreparedCampaign,
) -> PreparedCampaignSnapshot:
    # Materialize all AST, index and selection inputs required by a restarted engine process.
    root = Path(configuration.project.root_path).expanduser().resolve()
    return PreparedCampaignSnapshot(
        campaign_id=configuration.campaign_id,
        source_path=prepared.source_rel,
        source_sha256=prepared.source_sha256,
        index_version=prepared.index_version,
        index_payload=dict(prepared.index),
        mutants=tuple(_mutant_descriptor(prepared, mutant) for mutant in prepared.mutants),
        internal_mutants=tuple(mutant.to_dict() for mutant in prepared.mutants),
        prepared_mutants=tuple(
            ContractPreparedMutant(
                mutant=_mutant_descriptor(prepared, mutant),
                source_sha256_before=artifact.original_sha256,
                rendered_sha256=artifact.sha256,
                compiled=True,
                source_b64=base64.b64encode(artifact.data).decode("ascii"),
            )
            for mutant, artifact in zip(prepared.mutants, prepared.prepared_mutants, strict=True)
        ),
        selection=_selection_snapshot(prepared),
        function_id=prepared.function_id,
        function_range=prepared.function_range,
        function_info=dict(prepared.function_info) if prepared.function_info else None,
        repository_fingerprint=_repository_fingerprint(prepared.index),
        environment_fingerprint=_engine_environment_fingerprint(configuration, root),
        configuration_fingerprint=configuration.configuration_fingerprint
        or stable_hash(configuration.to_dict()),
        created_at=utc_now_iso(),
    )
def _prepared_from_snapshot(snapshot: PreparedCampaignSnapshot) -> InternalPreparedCampaign:
    # Rehydrate private mutation offsets from the immutable public snapshot without rediscovery.
    if not snapshot.internal_mutants:
        raise ValueError("prepared campaign snapshot has no internal mutant offsets")
    if snapshot.selection is None:
        raise ValueError("prepared campaign snapshot has no selection snapshot")
    selection = _snapshot_from_dict(snapshot.selection.to_dict())
    mutants = tuple(Mutant(**dict(item)) for item in snapshot.internal_mutants)
    prepared_rows = {item.mutant.mutant_id.value: item for item in snapshot.prepared_mutants}
    prepared_mutants: list[InternalPreparedMutant] = []
    for mutant in mutants:
        row = prepared_rows.get(mutant.mutant_id)
        if row is None:
            prepared_mutants = []
            break
        try:
            data = base64.b64decode(row.source_b64.encode("ascii"), validate=True)
        except (binascii.Error, UnicodeEncodeError) as exc:
            raise ValueError(f"prepared mutant source encoding is invalid: {mutant.mutant_id}") from exc
        rendered_sha256 = hashlib.sha256(data).hexdigest()
        if row.source_sha256_before != snapshot.source_sha256:
            raise ValueError(f"prepared mutant baseline hash is obsolete: {mutant.mutant_id}")
        if rendered_sha256 != row.rendered_sha256:
            raise ValueError(f"prepared mutant content hash mismatch: {mutant.mutant_id}")
        prepared_mutants.append(
            InternalPreparedMutant(
                mutant_id=mutant.mutant_id,
                original_sha256=row.source_sha256_before,
                data=data,
                sha256=row.rendered_sha256,
            )
        )
    return InternalPreparedCampaign(
        index=dict(snapshot.index_payload),
        source_rel=snapshot.source_path,
        source_sha256=snapshot.source_sha256,
        function_id=snapshot.function_id,
        function_range=snapshot.function_range,
        function_info=dict(snapshot.function_info) if snapshot.function_info else None,
        selection=selection,
        mutants=mutants,
        prepared_mutants=tuple(prepared_mutants),
        index_version=snapshot.index_version,
    )
def _merge_reports(reports: list[dict[str, Any]]) -> dict[str, Any]:
    # Merge shard reports through execution identity and an explicit retry policy.
    if not reports:
        return {"status": "partial", "results": []}
    rows: list[dict[str, Any]] = []
    for report in reports:
        rows.extend(_rows_from_report(report))
    from .recovery import merge_authoritative_results
    merged = merge_authoritative_results(rows)
    result = dict(reports[-1])
    result["results"] = sorted(
        merged,
        key=lambda row: str(row.get("mutant", {}).get("mutant_id", "")),
    )
    if all(report.get("status") in {"complete", "no_mutants", "no_l1_selection"} for report in reports):
        result["status"] = "complete"
    elif any(report.get("status") in {"error", "restore_error", "baseline_failed"} for report in reports):
        result["status"] = "error"
    else:
        result["status"] = "partial"
    return result
class RunnerMutationEngine:
    """E-09.1 facade that delegates execution to the v1.26 runner."""
    def __init__(self, event_sink: EngineEventSink | None = None) -> None:
        # Keep orchestration state local until Gallifrey supplies durable campaign storage.
        self.event_sink = event_sink
        self._contexts: dict[str, _CampaignContext] = {}
        self._sequences: dict[str, int] = {}
    def _emit(
        self,
        campaign_id: CampaignId,
        event_type: str | EngineEventType,
        payload: Mapping[str, Any],
        event_sink: EngineEventSink | None,
        *,
        worker_id: WorkerId | None = None,
        shard_id: ShardId | None = None,
    ) -> None:
        # Publish best-effort lifecycle events without changing runner result semantics.
        sink = event_sink or self.event_sink
        if sink is None:
            return
        sequence = self._sequences.get(campaign_id.value, 0) + 1
        self._sequences[campaign_id.value] = sequence
        sink.publish(
            EngineEvent.create(
                event_type,
                campaign_id,
                dict(payload),
                sequence=sequence,
                worker_id=worker_id,
                shard_id=shard_id,
            )
        )
    def _context(self, campaign_id: CampaignId) -> _CampaignContext:
        # Resolve one in-memory preparation context or fail at the public boundary.
        try:
            return self._contexts[campaign_id.value]
        except KeyError as exc:
            raise ValueError(f"campaign is not prepared: {campaign_id.value}") from exc
    @staticmethod
    def _artifact_dir(config: MutationConfig, campaign_id: CampaignId) -> Path:
        # Keep staged artifacts under one campaign-owned directory with stable names.
        return ensure_dir(config.reports_dir / "engine" / campaign_id.value)
    @staticmethod
    def _validate_prepared_snapshot(
        snapshot: PreparedCampaignSnapshot,
        configuration: CampaignConfiguration,
        config: MutationConfig,
    ) -> None:
        # Refuse to resume prepared data when source, repository or configuration inputs changed.
        source = (config.project_root / configuration.scope.source_path).resolve()
        if snapshot.campaign_id != configuration.campaign_id:
            raise ValueError("prepared snapshot campaign identity does not match request")
        if snapshot.source_path.replace("\\", "/").lstrip("./") != configuration.scope.source_path.replace("\\", "/").lstrip("./"):
            raise ValueError("prepared snapshot source path does not match request")
        if not source.is_file() or sha256_file(source) != snapshot.source_sha256:
            raise ValueError("prepared snapshot source hash is obsolete")
        expected_config = configuration.configuration_fingerprint or stable_hash(configuration.to_dict())
        if snapshot.configuration_fingerprint and snapshot.configuration_fingerprint != expected_config:
            raise ValueError("prepared snapshot configuration fingerprint is obsolete")
        current_rows: list[tuple[str, str]] = []
        files = snapshot.index_payload.get("files", {})
        if isinstance(files, Mapping):
            for relative in sorted(str(item) for item in files):
                candidate = (config.project_root / relative).resolve()
                if not candidate.is_file():
                    raise ValueError(f"prepared snapshot repository file is missing: {relative}")
                current_rows.append((relative, sha256_file(candidate)))
        if snapshot.repository_fingerprint and snapshot.repository_fingerprint != stable_hash(current_rows):
            raise ValueError("prepared snapshot repository fingerprint is obsolete")
        expected_environment = _engine_environment_fingerprint(configuration, config.project_root)
        if snapshot.environment_fingerprint and snapshot.environment_fingerprint != expected_environment:
            raise ValueError("prepared snapshot environment fingerprint is obsolete")
        declared_environment = configuration.project.environment
        if declared_environment is not None:
            current_pytest_version = _pytest_version()
            if declared_environment.pytest_version and declared_environment.pytest_version != current_pytest_version:
                raise ValueError("prepared snapshot pytest version is obsolete")
            current_plugins = tuple(
                sorted(
                    str(item.name)
                    for item in importlib.metadata.entry_points(group="pytest11")
                    if getattr(item, "name", None)
                )
            )
            if declared_environment.plugins and tuple(sorted(declared_environment.plugins)) != current_plugins:
                raise ValueError("prepared snapshot pytest plugins are obsolete")
            if (
                declared_environment.configuration_fingerprint
                and declared_environment.configuration_fingerprint
                != _pytest_configuration_fingerprint(config.project_root)
            ):
                raise ValueError("prepared snapshot pytest configuration is obsolete")
    def _hydrate_runner(self, context: _CampaignContext, run_id: str) -> None:
        # Reconnect runner ports to the frozen campaign context before any baseline or mutation work.
        runner = context.runner
        prepared = context.prepared
        runner._campaign_index = prepared.index
        runner._campaign_selection = prepared.selection
        runner._campaign_function_info = prepared.function_info
        runner._campaign_source_rel = prepared.source_rel
        runner._campaign_function_id = prepared.function_id
        runner._mutant_by_id = {mutant.mutant_id: mutant for mutant in prepared.mutants}
        runner._prepared_mutant_by_id = {item.mutant_id: item for item in prepared.prepared_mutants}
        runner._context_map = load_context_map(context.config.context_map_path)
        runner._nodeid_validation_context = build_nodeid_validation_context(prepared.index, runner.root)
        runner._line_selection_cache = {}
        runner._line_impact_cache = {}
        runner._function_impact_cache = None
        runner._context_selection_cache = None
        runner._static_selection_cache = None
        runner._impact_adapter = SQLiteImpactAdapter(context.config.impact_db) if context.config.impact_db else None
        runner._baseline_results = {}
        runner._campaign_accumulator = CampaignAccumulator()
        runner._completed_identity_digests = {}
        runner._completed_mutant_ids = set()
        runner._test_stats_run_id = run_id
        runner._test_stats_event_dir = context.artifact_dir / "test_stats_events" / run_id
        ensure_dir(runner._test_stats_event_dir)
        runner._load_cache()
    def _collection_command(self, context: _CampaignContext) -> tuple[str, ...]:
        # Preserve a direct test command so a no-dependency wheel need not import pytest just to collect.
        configured = context.configuration.project.test_command
        if configured is not None:
            command = list(configured.argv)
        else:
            command = [resolve_python(context.config.project_root, context.config.python_executable), "-m", "pytest"]
        if not is_pytest_command(command):
            return tuple(command)
        if "--collect-only" not in command:
            command.append("--collect-only")
        if "-q" not in command and "--quiet" not in command:
            command.append("-q")
        return tuple(command)
    @staticmethod
    def _test_command_cwd(context: _CampaignContext) -> Path:
        # Resolve the component-local test cwd already rebound into the isolated campaign workspace.
        return (context.config.test_command_cwd or context.config.project_root).resolve()

    @staticmethod
    def _indexed_nodeids(index: Mapping[str, Any]) -> tuple[str, ...]:
        # Use the immutable AST index when a direct command has no pytest collection protocol.
        rows = index.get("tests", ())
        if not isinstance(rows, (list, tuple)):
            return ()
        return tuple(
            sorted(
                {
                    str(row.get("nodeid"))
                    for row in rows
                    if isinstance(row, Mapping) and str(row.get("nodeid", "")).strip()
                }
            )
        )
    async def collect_campaign_async(
        self,
        campaign_id: CampaignId,
        event_sink: EngineEventSink | None = None,
    ) -> Mapping[str, Any]:
        # Execute pytest collection once and persist its complete authoritative inventory.
        context = self._context(campaign_id)
        artifact = context.artifact_dir / "collection.output.log"
        snapshot_path = context.artifact_dir / "collection.snapshot.json"
        if snapshot_path.exists():
            value = read_json(snapshot_path)
            snapshot = CollectionSnapshot.from_dict(value if isinstance(value, Mapping) else {})
            test_cwd = self._test_command_cwd(context)
            expected_configuration = _pytest_configuration_fingerprint(test_cwd)
            if snapshot.pytest_configuration_fingerprint != expected_configuration:
                raise ValueError("collection snapshot pytest configuration fingerprint is obsolete")
            if snapshot.pytest_version != _pytest_version():
                raise ValueError("collection snapshot pytest version is obsolete")
            if snapshot.plugin_fingerprint != _pytest_plugin_fingerprint():
                raise ValueError("collection snapshot pytest plugins are obsolete")
            expected_environment = stable_hash(
                {
                    "command": list(self._collection_command(context)),
                    "test_cwd": str(test_cwd.relative_to(context.config.project_root)) if test_cwd.is_relative_to(context.config.project_root) else str(test_cwd),
                    "environment": env_fingerprint(
                        context.config.project_root,
                        include_cwd=False,
                        declared_keys=(
                            context.configuration.project.environment.declared_env_keys
                            if context.configuration.project.environment is not None
                            else ()
                        ),
                        declared_patterns=(
                            context.configuration.project.environment.declared_env_patterns
                            if context.configuration.project.environment is not None
                            else ()
                        ),
                        tracked_variables=(
                            context.configuration.project.environment.tracked_variables
                            if context.configuration.project.environment is not None
                            else ()
                        ),
                        tracked_prefixes=(
                            context.configuration.project.environment.tracked_prefixes
                            if context.configuration.project.environment is not None
                            else ()
                        ),
                        ignored_variables=(
                            context.configuration.project.environment.ignored_variables
                            if context.configuration.project.environment is not None
                            else ()
                        ),
                        secret_variables=(
                            context.configuration.project.environment.secret_variables
                            if context.configuration.project.environment is not None
                            else ()
                        ),
                        inherit_policy=(
                            context.configuration.project.environment.inherit_policy
                            if context.configuration.project.environment is not None
                            else "allowlisted"
                        ),
                        secret_key=context.configuration.project.project_id.value,
                    ),
                }
            )
            if snapshot.environment_fingerprint != expected_environment:
                raise ValueError("collection snapshot environment fingerprint is obsolete")
            expected_revision = (
                context.configuration.project.revision.git_revision
                if context.configuration.project.revision
                else None
            )
            if snapshot.repository_revision != expected_revision:
                raise ValueError("collection snapshot repository revision is obsolete")
        else:
            command = self._collection_command(context)
            test_cwd = self._test_command_cwd(context)
            collection_errors: list[str] = []
            if is_pytest_command(command):
                try:
                    collection_env = dict(os.environ)
                    if not context.configuration.project.pytest_plugin_autoload:
                        # Keep collection and execution on the same minimal internal plugin profile.
                        collection_env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
                    collection_env = _with_project_pythonpath(command, test_cwd, collection_env)
                    process = await asyncio.to_thread(
                        run_argv,
                        command,
                        cwd=test_cwd,
                        timeout_seconds=context.config.timeout_seconds,
                        env=collection_env,
                        output_artifact=artifact,
                        metrics=context.runner.performance,
                    )
                    output = artifact.read_text(encoding="utf-8", errors="replace") if artifact.exists() else process.output
                    if process.timed_out:
                        collection_errors.append("pytest collect-only timed out")
                    elif process.exit_code not in {0, 5}:
                        collection_errors.append(f"pytest collect-only exited {process.exit_code}")
                    if process.diagnostic_excerpts:
                        collection_errors.extend(str(item) for item in process.diagnostic_excerpts)
                except OSError as exc:
                    output = ""
                    collection_errors.append(str(exc))
                nodeids = parse_pytest_collection_output(output)
                collection_mode = "pytest"
            else:
                # A direct executable (for example ``python -c``) is the test oracle itself;
                # it has no pytest nodeid stream, so bind selection to the immutable source index.
                nodeids = self._indexed_nodeids(context.prepared.index)
                collection_mode = "static_index"
                atomic_write_text(
                    artifact,
                    "direct test command; pytest collection is not applicable\n"
                    f"indexed_nodeids={len(nodeids)}\n",
                    durability="critical",
                    category="report",
                    metrics=context.runner.performance,
                )
            pytest_version = _pytest_version()
            plugin_fingerprint = _pytest_plugin_fingerprint()
            environment_fingerprint = stable_hash(
                {
                    "command": list(command),
                    "test_cwd": str(test_cwd.relative_to(context.config.project_root)) if test_cwd.is_relative_to(context.config.project_root) else str(test_cwd),
                    "environment": env_fingerprint(
                        context.config.project_root,
                        include_cwd=False,
                        declared_keys=(
                            context.configuration.project.environment.declared_env_keys
                            if context.configuration.project.environment is not None
                            else ()
                        ),
                        declared_patterns=(
                            context.configuration.project.environment.declared_env_patterns
                            if context.configuration.project.environment is not None
                            else ()
                        ),
                        tracked_variables=(
                            context.configuration.project.environment.tracked_variables
                            if context.configuration.project.environment is not None
                            else ()
                        ),
                        tracked_prefixes=(
                            context.configuration.project.environment.tracked_prefixes
                            if context.configuration.project.environment is not None
                            else ()
                        ),
                        ignored_variables=(
                            context.configuration.project.environment.ignored_variables
                            if context.configuration.project.environment is not None
                            else ()
                        ),
                        secret_variables=(
                            context.configuration.project.environment.secret_variables
                            if context.configuration.project.environment is not None
                            else ()
                        ),
                        inherit_policy=(
                            context.configuration.project.environment.inherit_policy
                            if context.configuration.project.environment is not None
                            else "allowlisted"
                        ),
                        secret_key=context.configuration.project.project_id.value,
                    ),
                }
            )
            revision = (
                context.configuration.project.revision.git_revision
                if context.configuration.project.revision
                else None
            )
            snapshot = CollectionSnapshot(
                collection_snapshot_id=stable_hash(
                    {
                        "revision": revision,
                        "environment": environment_fingerprint,
                        "pytest_version": pytest_version,
                        "plugins": plugin_fingerprint,
                        "configuration": _pytest_configuration_fingerprint(test_cwd),
                        "nodeids": nodeids,
                        "errors": collection_errors,
                        "mode": collection_mode,
                    }
                )[:32],
                repository_revision=revision,
                environment_fingerprint=environment_fingerprint,
                pytest_version=pytest_version,
                plugin_fingerprint=plugin_fingerprint,
                pytest_configuration_fingerprint=_pytest_configuration_fingerprint(test_cwd),
                nodeids=nodeids,
                collection_errors=tuple(collection_errors),
                created_at=utc_now_iso(),
                collection_mode=collection_mode,
            )
            atomic_write_json(snapshot_path, snapshot.to_dict(), durability="critical", category="report")
        context.collection_snapshot = snapshot
        selection_path = context.artifact_dir / "selection.snapshot.json"
        selection_was_persisted = selection_path.exists()
        if selection_was_persisted:
            raw_selection = read_json(selection_path)
            if not isinstance(raw_selection, Mapping):
                raise ValueError("selection artifact is not an object")
            selected = _snapshot_from_dict(dict(raw_selection))
        else:
            # Bind the precomputed plan to the authoritative collection inventory before execution.
            selected = context.prepared.selection
        runner = context.runner
        runner._campaign_index = context.prepared.index
        runner._nodeid_validation_context = build_nodeid_validation_context(
            context.prepared.index,
            context.config.project_root,
            collected_nodeids=snapshot.nodeids,
        )
        normalized_selected = runner._validate_selection_snapshot(selected, context.prepared.index)
        if selection_was_persisted and normalized_selected.to_dict() != selected.to_dict():
            raise ValueError("selection artifact conflicts with authoritative collection snapshot")
        if not selection_was_persisted:
            atomic_write_json(selection_path, normalized_selected.to_dict(), durability="critical", category="report")
        selected = normalized_selected
        context.prepared = replace(context.prepared, selection=selected)
        result = {
            "stage": "collect",
            "status": "complete" if not snapshot.collection_errors else "collection_error",
            "snapshot": snapshot.to_dict(),
            "snapshot_id": snapshot.collection_snapshot_id,
            "artifact_path": str(snapshot_path),
            "output_artifact": str(artifact) if artifact.exists() else None,
            "selection_snapshot_id": selected.snapshot_id,
            "selection_artifact_path": str(selection_path),
            "collection_mode": snapshot.collection_mode,
        }
        self._emit(
            campaign_id,
            EngineEventType.COLLECTION_COMPLETED,
            {"snapshot_id": snapshot.collection_snapshot_id, "nodeids": len(snapshot.nodeids)},
            event_sink,
        )
        return result
    async def index_campaign_async(
        self,
        campaign_id: CampaignId,
        event_sink: EngineEventSink | None = None,
    ) -> Mapping[str, Any]:
        # Persist the complete prepared index and its immutable bounded summary.
        context = self._context(campaign_id)
        payload_path = context.artifact_dir / "index.payload.json"
        snapshot_path = context.artifact_dir / "index.snapshot.json"
        if snapshot_path.exists() and payload_path.exists():
            raw_snapshot = read_json(snapshot_path)
            snapshot = IndexSnapshot.from_dict(raw_snapshot if isinstance(raw_snapshot, Mapping) else {})
            raw_payload = read_json(payload_path)
            if raw_payload != context.prepared.index:
                raise ValueError("index payload artifact conflicts with prepared snapshot")
        else:
            index = context.prepared.index
            atomic_write_json(payload_path, index, durability="critical", category="report")
            snapshot = IndexSnapshot(
                index_version=context.prepared.index_version,
                source_sha256=context.prepared.source_sha256,
                file_count=len(index.get("files", {})) if isinstance(index.get("files"), Mapping) else 0,
                function_count=len(index.get("functions", [])) if isinstance(index.get("functions"), list) else 0,
                test_count=len(index.get("tests", [])) if isinstance(index.get("tests"), list) else 0,
                created_at=utc_now_iso(),
            )
            atomic_write_json(snapshot_path, snapshot.to_dict(), durability="critical", category="report")
        context.index_snapshot = snapshot
        result = {
            "stage": "index",
            "status": "complete",
            "snapshot": snapshot.to_dict(),
            "snapshot_id": snapshot.index_version,
            "artifact_path": str(snapshot_path),
            "payload_path": str(payload_path),
        }
        self._emit(
            campaign_id,
            EngineEventType.INDEX_COMPLETED,
            {"index_version": snapshot.index_version, "files": snapshot.file_count},
            event_sink,
        )
        return result
    async def baseline_campaign_async(
        self,
        campaign_id: CampaignId,
        event_sink: EngineEventSink | None = None,
    ) -> Mapping[str, Any]:
        # Run the real baseline command against the frozen source and materialize its evidence.
        context = self._context(campaign_id)
        baseline_path = context.artifact_dir / "baseline.json"
        if baseline_path.exists():
            value = read_json(baseline_path)
            if not isinstance(value, Mapping):
                raise ValueError("baseline artifact is not an object")
            if str(value.get("source_sha256", "")) != context.prepared.source_sha256:
                raise ValueError("baseline artifact source hash is obsolete")
            rows = tuple(item for item in value.get("rows", []) if isinstance(item, dict))
            context.baseline_rows = rows
            self._emit(campaign_id, EngineEventType.BASELINE_COMPLETED, {"reused": True}, event_sink)
            return dict(value)
        report_id = f"engine-baseline-{campaign_id.value}"
        self._hydrate_runner(context, report_id)
        runner = context.runner
        source = (runner.root / context.prepared.source_rel).resolve()
        recovery_dir = context.artifact_dir / "recovery" / report_id
        snapshot = await asyncio.to_thread(create_snapshot, source, recovery_dir, metrics=runner.performance)
        runner._campaign_snapshot = snapshot
        selection = context.prepared.selection
        if selection is None:
            raise ValueError("prepared campaign selection is missing")
        levels = runner._level_specs(selection)
        rows: list[dict[str, Any]] = []
        status = "complete"
        self._emit(campaign_id, EngineEventType.BASELINE_STARTED, {"levels": len(levels)}, event_sink)
        try:
            if context.prepared.mutants and (not levels or not levels[0].command_argv):
                status = "no_l1_selection"
            elif levels:
                rows = await asyncio.to_thread(
                    runner._run_baselines,
                    levels[:1],
                    snapshot,
                    selection,
                    context.prepared.function_info,
                    report_id,
                )
                if any(not bool(item.get("passed", False)) for item in rows):
                    status = "baseline_failed"
            artifact = {
                "stage": "baseline",
                "status": status,
                "baseline_snapshot_id": stable_hash(
                    {"source_sha256": snapshot.original_sha256, "rows": rows}
                )[:32],
                "source_sha256": snapshot.original_sha256,
                "selection_snapshot_id": selection.snapshot_id,
                "rows": rows,
                "created_at": utc_now_iso(),
            }
            atomic_write_json(baseline_path, artifact, durability="critical", category="baseline_artifact")
            context.baseline_rows = tuple(rows)
            return artifact
        finally:
            try:
                await asyncio.to_thread(restore_snapshot, snapshot, expected_sha256=None, durability="critical")
            finally:
                runner._close_test_stats_connection()
                self._emit(
                    campaign_id,
                    EngineEventType.BASELINE_COMPLETED,
                    {"status": status, "rows": len(rows)},
                    event_sink,
                )
    async def prepare_campaign_async(
        self,
        request: PrepareCampaignRequest,
        event_sink: EngineEventSink | None = None,
    ) -> PreparedCampaign:
        # Load or create one immutable prepared snapshot before any staged command is accepted.
        configuration = request.configuration
        campaign_id = configuration.campaign_id
        self._emit(campaign_id, EngineEventType.CAMPAIGN_PREPARATION_STARTED, {}, event_sink)
        config = _config_from_contract(configuration)
        artifact_dir = self._artifact_dir(config, campaign_id)
        snapshot_path = artifact_dir / "prepared.snapshot.json"
        runner = MutationRunner(config)
        try:
            if snapshot_path.exists():
                raw_snapshot = read_json(snapshot_path)
                snapshot = PreparedCampaignSnapshot.from_dict(
                    raw_snapshot if isinstance(raw_snapshot, Mapping) else {}
                )
                self._validate_prepared_snapshot(snapshot, configuration, config)
                prepared = _prepared_from_snapshot(snapshot)
            else:
                prepared = await asyncio.to_thread(prepare_campaign, config, runner)
                snapshot = _prepared_snapshot(configuration, prepared)
                atomic_write_json(
                    snapshot_path,
                    snapshot.to_dict(),
                    durability="critical",
                    category="recovery",
                )
        except Exception as exc:
            self._emit(
                campaign_id,
                EngineEventType.ENGINE_ERROR,
                {"phase": "preparation", "error": str(exc)},
                event_sink,
            )
            raise
        context = _CampaignContext(configuration, config, runner, prepared, [], snapshot, artifact_dir)
        self._contexts[campaign_id.value] = context
        result = PreparedCampaign(
            campaign_id=campaign_id,
            source_path=prepared.source_rel,
            source_sha256=prepared.source_sha256,
            index_version=prepared.index_version,
            mutants=tuple(_mutant_descriptor(prepared, mutant) for mutant in prepared.mutants),
            selection=_selection_snapshot(prepared),
            snapshot_path=str(snapshot_path),
            snapshot_id=stable_hash(snapshot.to_dict())[:32],
            function_id=prepared.function_id,
            class_name=(
                str(prepared.function_info.get("class_name"))
                if prepared.function_info and prepared.function_info.get("class_name")
                else None
            ),
        )
        self._emit(
            campaign_id,
            EngineEventType.PLAN_CREATED,
            {"index_version": result.index_version, "mutants": len(result.mutants)},
            event_sink,
        )
        return result
    async def discover_mutants_async(
        self,
        request: DiscoverMutantsRequest,
        event_sink: EngineEventSink | None = None,
    ) -> MutationDiscoveryResult:
        # Expose the already frozen mutant catalog without regenerating AST positions.
        context = self._context(request.campaign_id)
        mutants = tuple(_mutant_descriptor(context.prepared, mutant) for mutant in context.prepared.mutants)
        result = MutationDiscoveryResult(request.campaign_id, mutants, len(mutants))
        self._emit(
            request.campaign_id,
            EngineEventType.MUTANTS_DISCOVERED,
            {"total_mutants": len(mutants)},
            event_sink,
        )
        return result
    async def execute_shard_async(
        self,
        request: ExecuteShardRequest,
        event_sink: EngineEventSink | None = None,
    ) -> ShardExecutionResult:
        # Execute only the requested frozen mutants through the already prepared runner context.
        context = self._context(request.campaign_id)
        expected_snapshot_id = stable_hash(context.snapshot.to_dict())[:32]
        if (
            request.prepared_snapshot_id is not None
            and request.prepared_snapshot_id != expected_snapshot_id
        ):
            raise ValueError("execute-shard prepared snapshot identity mismatch")
        if request.worker_id is not None and not request.prepared_snapshot_id:
            raise ValueError("prepared_snapshot_id must be a non-empty string")
        self._emit(
            request.campaign_id,
            EngineEventType.SHARD_STARTED,
            {"shard_id": request.shard.shard_id.value, "mutants": len(request.shard.mutant_ids)},
            event_sink,
            shard_id=request.shard.shard_id,
        )
        report_id = (
            f"engine-shard-{request.campaign_id.value}-{request.shard.shard_id.value}"
            f"-attempt-{max(0, request.attempt)}"
        )
        self._hydrate_runner(context, report_id)
        runner = context.runner
        report_path = context.artifact_dir / f"{report_id}.json"
        manifest_path = context.artifact_dir / f"{report_id}.manifest.json"
        results_path = context.artifact_dir / f"{report_id}.results.jsonl"
        state_path = context.artifact_dir / f"{report_id}.state.json"
        runner._results_path = results_path
        runner._state_path = state_path
        runner._results_journal_size = 0
        runner._results_journal_digest = hashlib.sha256()
        runner._completed_identity_digests = {}
        runner._completed_mutant_ids = set()
        source = (runner.root / context.prepared.source_rel).resolve()
        if runner.config.main_root_path is not None and (
            source == runner.config.main_root_path or runner.config.main_root_path in source.parents
        ):
            raise ValueError("mutation target is inside the registered main checkout")
        target_lock = source.with_name(source.name + ".test_intelligence.lock")
        selection = context.prepared.selection
        if selection is None:
            raise ValueError("prepared campaign selection is missing")
        input_fingerprint = campaign_input_fingerprint(
            runner.root,
            context.prepared.source_rel,
            context.prepared.source_sha256,
            selection.to_dict(),
            runner._config_dict(),
        )
        snapshot = await asyncio.to_thread(
            create_snapshot,
            source,
            context.artifact_dir / "recovery" / report_id,
            metrics=runner.performance,
        )
        runner._campaign_snapshot = snapshot
        write_manifest(
            manifest_path,
            snapshot,
            extra={
                "run_id": report_id,
                "coordinator_pid": os.getpid(),
                "process_birth_token": current_process_birth_token(),
                "report_path": str(report_path),
                "state_path": str(state_path),
                "results_journal": str(results_path),
                "lock_path": str(target_lock),
                "input_fingerprint": input_fingerprint,
                "phase": "running",
                "active_mutant": None,
            },
            metrics=runner.performance,
        )
        levels = runner._level_specs(selection)
        runner._baseline_results = {
            str(item.get("level")): dict(item)
            for item in context.baseline_rows
            if item.get("level")
        }
        report: dict[str, Any] = {
            "schema_version": 2,
            "run_id": report_id,
            "status": "running",
            "created_at": utc_now_iso(),
            "project_root": str(runner.root),
            "target": {"source_path": context.prepared.source_rel},
            "config": runner._config_dict(),
            "selection_snapshot": selection.to_dict(),
            "input_fingerprint": input_fingerprint,
            "baseline": list(context.baseline_rows),
            "mutants": [mutant.to_dict() for mutant in context.prepared.mutants],
            "results": [],
            "results_journal": str(results_path),
            "state_path": str(state_path),
            "recovery_manifest": str(manifest_path),
            "report_path": str(report_path),
            "markdown_path": str(report_path.with_suffix(".md")),
            "levels": [runner._level_dict(level) for level in levels],
        }
        rows: list[dict[str, Any]] = []
        status = "complete"
        error: str | None = None
        mutation_lock = FileLock(
            target_lock,
            {"target": str(source), "run_id": report_id},
        )
        mutation_lock.__enter__()
        try:
            missing = [item for item in request.shard.mutant_ids if item not in runner._mutant_by_id]
            if missing:
                raise ValueError(f"shard contains mutants outside prepared snapshot: {missing}")
            for mutant_id in request.shard.mutant_ids:
                override = tuple(request.test_overrides.get(mutant_id, ()))
                active_selection = selection
                active_levels = levels
                original_config = runner.config
                if override:
                    active_selection = _selection_with_test_override(selection, override)
                    runner._campaign_selection = active_selection
                    runner._line_selection_cache = {}
                    runner.config = replace(
                        runner.config,
                        test_command_argv=None,
                        selection_snapshot=active_selection.to_dict(),
                    )
                    active_levels = runner._level_specs(active_selection)
                try:
                    result = await asyncio.to_thread(
                        runner._run_mutant,
                        runner._mutant_by_id[mutant_id],
                        snapshot,
                        active_levels,
                        report_id,
                        manifest_path,
                    )
                finally:
                    runner.config = original_config
                    runner._campaign_selection = selection
                    runner._line_selection_cache = {}
                observations = load_mutant_test_observations(
                    stats_db_path(runner.reports_dir),
                    run_id=report_id,
                    mutant_ids=(mutant_id,),
                    root=runner.root,
                    event_dir=runner._test_stats_event_dir,
                ).get(mutant_id, ())
                payload = runner._append_result(
                    result,
                    attempt=request.attempt,
                    lease_id=request.lease_id,
                    worker_id=request.worker_id.value if request.worker_id else None,
                    test_observations=observations,
                ) or result.to_dict()
                public_result = _mutant_result_from_row(request, payload)
                if public_result is None:
                    raise RuntimeError(f"mutant result identity is missing for {mutant_id}")
                public_result = _attach_result_fingerprints(public_result, request, runner.root)
                _publish_mutant_spool_event(request, public_result)
                rows.append(payload)
                report["results"].append(payload)
                runner._record_campaign_result(payload)
                runner._write_state("running", report["results"])
                if result.status in {"restore_error", "baseline_failed", "error"}:
                    status = result.status
                    break
            report["status"] = status
            report["metrics"] = runner._metrics()
            runner._finish_report(report_path, report, manifest_path, status=status)
        except Exception as exc:
            status = "error"
            error = str(exc)
            report["status"] = status
            report["error"] = error
            report["metrics"] = runner._metrics()
            try:
                runner._finish_report(report_path, report, manifest_path, status=status)
            except Exception as finalization_error:
                report["finalization_error"] = str(finalization_error)
        finally:
            try:
                await asyncio.to_thread(restore_snapshot, snapshot, expected_sha256=None, durability="critical")
            except Exception as restore_error:
                status = "restore_error"
                error = error or str(restore_error)
            runner._close_test_stats_connection()
            mutation_lock.__exit__(None, None, None)
        report["status"] = status
        if error:
            report["error"] = error
        context.reports.append(report)
        rows = _rows_from_report(report)
        results: list[MutantExecutionResult] = []
        for row in rows:
            result = _mutant_result_from_row(request, row)
            if result is None:
                continue
            result = _attach_result_fingerprints(result, request, context.runner.root)
            results.append(result)
            self._emit(
                request.campaign_id,
                EngineEventType.MUTANT_COMPLETED,
                {"mutant_id": result.mutant_id.value, "status": result.status},
                event_sink,
                shard_id=request.shard.shard_id,
            )
        worker_id = request.worker_id or WorkerId(f"facade-{request.shard.shard_id.value}")
        shard_result = ShardExecutionResult(
            shard_id=request.shard.shard_id,
            worker_id=worker_id,
            status=str(report.get("status", "error")),
            completed_mutants=len(results),
            results=tuple(results),
            report_path=str(report.get("report_path")) if report.get("report_path") else None,
            error=str(report.get("error")) if report.get("error") else None,
        )
        self._emit(
            request.campaign_id,
            EngineEventType.SHARD_COMPLETED,
            {"shard_id": request.shard.shard_id.value, "completed_mutants": len(results)},
            event_sink,
            worker_id=worker_id,
            shard_id=request.shard.shard_id,
        )
        return shard_result
    async def finalize_campaign_async(
        self,
        request: FinalizeCampaignRequest,
        event_sink: EngineEventSink | None = None,
    ) -> CampaignResult:
        # Materialize a stable summary from the reports produced by facade shard calls.
        context = self._context(request.campaign_id)
        if not context.reports:
            for path in sorted(context.artifact_dir.glob("engine-shard-*.json")):
                try:
                    value = read_json(path)
                except (OSError, ValueError):
                    continue
                if isinstance(value, dict) and value.get("results") is not None:
                    context.reports.append(value)
        report = _merge_reports(context.reports)
        rows = _rows_from_report(report)
        counts = Counter(str(row.get("status", "error")) for row in rows)
        status_value = request.status_override or str(report.get("status", "partial"))
        summary = CampaignSummary(
            campaign_id=request.campaign_id,
            status=status_value,
            total_mutants=len(context.prepared.mutants),
            completed_mutants=len({str(row.get("mutant", {}).get("mutant_id")) for row in rows}),
            counts=dict(counts),
            report_path=str(report.get("report_path")) if report.get("report_path") else None,
            workers=context.config.workers,
            error=str(report.get("error")) if report.get("error") else None,
        )
        result = CampaignResult(summary=summary, report=report)
        self._emit(
            request.campaign_id,
            EngineEventType.REPORT_MATERIALIZED,
            {"status": status_value, "completed_mutants": summary.completed_mutants},
            event_sink,
        )
        return result
    async def run_async(
        self,
        configuration: CampaignConfiguration,
        event_sink: EngineEventSink | None = None,
    ) -> CampaignResult:
        # Offer one convenience lifecycle while retaining the four explicit protocol stages.
        prepared = await self.prepare_campaign_async(PrepareCampaignRequest(configuration), event_sink)
        await self.discover_mutants_async(DiscoverMutantsRequest(configuration.campaign_id), event_sink)
        shard = ShardDescriptor(
            shard_id=ShardId("shard-000"),
            mutant_ids=tuple(item.mutant_id.value for item in prepared.mutants),
            estimated_cost=float(len(prepared.mutants)),
        )
        await self.execute_shard_async(ExecuteShardRequest(configuration.campaign_id, shard), event_sink)
        return await self.finalize_campaign_async(FinalizeCampaignRequest(configuration.campaign_id), event_sink)
    async def run_config_async(
        self,
        config: MutationConfig,
        event_sink: EngineEventSink | None = None,
    ) -> dict[str, Any]:
        # Adapt the existing CLI configuration through the facade without changing runner report semantics.
        campaign_id = CampaignId(
            deterministic_id(
                "cmp",
                config.source,
                sorted(config.mutant_ids),
                config.max_mutants,
                config.workers,
            )
        )
        report = await asyncio.to_thread(MutationRunner(config).run)
        self._emit(
            campaign_id,
            EngineEventType.REPORT_MATERIALIZED,
            {"run_id": report.get("run_id"), "status": report.get("status")},
            event_sink,
        )
        return report
MutationEngineFacade = RunnerMutationEngine
