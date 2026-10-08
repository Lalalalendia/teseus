"""Campaign preparation service independent from legacy worker orchestration."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from .index import (
    SELECTION_ALGORITHM_VERSION,
    add_domain_levels,
    build_nodeid_validation_context,
    find_function,
    load_context_map,
    plan_selection,
)
from .io_utils import sha256_bytes, sha256_file
from .models import Mutant
from .mutations import PreparedMutant, prepare_mutant_artifacts
from .mutation_discovery_service import MutationDiscoveryService
from .runner import MutationConfig, MutationRunner, StaleInputError, _snapshot_from_dict
from .test_stats import stats_db_path
@dataclass(frozen=True)
class PreparedCampaign:
    """Read-only preparation output shared by engine and worker adapters."""
    index: dict[str, Any]
    source_rel: str
    source_sha256: str
    function_id: str | None
    function_range: tuple[int, int] | None
    function_info: dict[str, Any] | None
    selection: Any
    mutants: tuple[Mutant, ...]
    index_version: str
    prepared_mutants: tuple[PreparedMutant, ...] = ()
class CampaignPreparationService:
    """Build immutable index, selection and mutant inputs for one campaign."""
    def __init__(self, coordinator: MutationRunner) -> None:
        # Retain runner-owned ports while removing preparation from worker orchestration.
        self.coordinator = coordinator
    def prepare(self, config: MutationConfig) -> PreparedCampaign:
        # Build preparation inputs without mutating the source checkout.
        coordinator = self.coordinator
        index = coordinator._load_or_build_index()
        source_path = (coordinator.root / config.source).resolve()
        source_rel = source_path.relative_to(coordinator.root).as_posix()
        source_sha256 = sha256_file(source_path)
        indexed_file = index.get("files", {}).get(source_rel)
        if not isinstance(indexed_file, dict) or indexed_file.get("sha256") != source_sha256:
            raise StaleInputError("stale_index", f"stale index for {source_rel}")
        function_info = find_function(index, source_rel, config.function) if config.function else None
        function_id = str(function_info["function_id"]) if function_info else None
        function_range = (
            (int(function_info["start_line"]), int(function_info["end_line"]))
            if function_info
            else None
        )
        context_map = load_context_map(config.context_map_path)
        coordinator._nodeid_validation_context = build_nodeid_validation_context(index, coordinator.root)
        if config.selection_snapshot:
            selection = _snapshot_from_dict(config.selection_snapshot)
            if selection.source_path.replace("\\", "/").lstrip("./") != source_rel:
                raise StaleInputError("stale_snapshot", "selection snapshot target does not match --source")
            if selection.source_sha256 and selection.source_sha256 != source_sha256:
                raise StaleInputError("stale_snapshot", "selection snapshot source hash is obsolete")
            if selection.algorithm_version != SELECTION_ALGORITHM_VERSION:
                raise StaleInputError("stale_snapshot", "selection snapshot algorithm version is obsolete")
            if selection.index_version and selection.index_version != str(index.get("index_version", "")):
                raise StaleInputError("stale_snapshot", "selection snapshot index version is obsolete")
            selection = coordinator._validate_selection_snapshot(selection, index)
        else:
            selection = plan_selection(
                coordinator.root,
                index,
                source_rel,
                function_id or config.function,
                context_map=context_map,
                impact_db=config.impact_db,
                selected_tests_file=config.selected_tests_file,
                test_stats_db=stats_db_path(config.reports_dir or Path("reports")),
                validation_context=coordinator._nodeid_validation_context,
            )
        if not config.no_escalation and len(selection.levels) < 3:
            selection = add_domain_levels(selection, config.domain)
        source_bytes = source_path.read_bytes()
        if sha256_bytes(source_bytes) != source_sha256:
            raise StaleInputError("stale_index", f"source changed during preparation: {source_rel}")
        source_text = source_bytes.decode("utf-8")
        mutants = MutationDiscoveryService(coordinator.performance).discover(
            source_text,
            function_range=function_range,
            from_line=config.from_line,
            to_line=config.to_line,
            mutant_ids=config.mutant_ids or None,
            max_mutants=config.max_mutants,
            operators=config.operators,
        )
        prepared_mutants = prepare_mutant_artifacts(source_path, source_bytes, mutants)
        return PreparedCampaign(
            index=index,
            source_rel=source_rel,
            source_sha256=source_sha256,
            function_id=function_id,
            function_range=function_range,
            function_info=function_info,
            selection=selection,
            mutants=tuple(mutants),
            prepared_mutants=prepared_mutants,
            index_version=str(index.get("index_version", "")),
        )
def prepare_campaign(config: MutationConfig, coordinator: MutationRunner) -> PreparedCampaign:
    # Preserve the historical function call while routing through the extracted service.
    return CampaignPreparationService(coordinator).prepare(config)
