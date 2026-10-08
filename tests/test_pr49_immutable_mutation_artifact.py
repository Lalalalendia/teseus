from __future__ import annotations
import base64
import inspect
from pathlib import Path
import pytest
from test_intelligence_unified_v1.models import PerformanceMetrics
from test_intelligence_unified_v1.preparation_service import CampaignPreparationService
from test_intelligence_unified_v1.runner import MutationRunner
from test_intelligence_unified_v1.mutations import (
    RestoreError,
    create_snapshot,
    generate_mutants,
    prepare_mutant_artifacts,
    restore_snapshot,
    switch_prepared_mutant,
)
from theseus_contracts import CampaignId, MutantId
from theseus_contracts.engine import PreparedCampaignSnapshot
from theseus_contracts.mutation import MutantDescriptor, PreparedMutant

def _two_mutants(snapshot):
    # Build two deterministic same-file mutants that can be switched without a baseline write between them.
    mutants = generate_mutants(snapshot.text, operators=("plus_to_minus", "gt_to_ge"))
    assert len(mutants) >= 2
    return mutants[:2]

def test_prepared_mutation_artifact_round_trip_preserves_exact_bytes(tmp_path: Path) -> None:
    # Persist immutable prepared bytes through the public campaign snapshot without rebuilding the mutation.
    target = tmp_path / "app.py"
    target.write_text("def choose(value):\n    if value > 0:\n        return value + 1\n    return 0\n", encoding="utf-8")
    snapshot = create_snapshot(target, tmp_path / "recovery")
    mutant = _two_mutants(snapshot)[0]
    artifact = prepare_mutant_artifacts(target, snapshot.original_bytes, (mutant,))[0]
    descriptor = MutantDescriptor(
        mutant_id=MutantId(mutant.mutant_id),
        mutation=mutant.mutation,
        source_path="app.py",
        line_no=mutant.line_no,
        column_no=mutant.column_no,
        original=mutant.original,
        replacement=mutant.replacement,
        operator_version=mutant.operator_version,
    )
    public = PreparedMutant(
        mutant=descriptor,
        source_sha256_before=artifact.original_sha256,
        rendered_sha256=artifact.sha256,
        compiled=True,
        source_b64=base64.b64encode(artifact.data).decode("ascii"),
    )
    durable = PreparedCampaignSnapshot(
        campaign_id=CampaignId("campaign-pr49-artifact"),
        source_path="app.py",
        source_sha256=snapshot.original_sha256,
        index_version="index-test",
        index_payload={},
        prepared_mutants=(public,),
    )
    restored = PreparedCampaignSnapshot.from_dict(durable.to_dict())
    assert restored.prepared_mutants == (public,)
    assert base64.b64decode(restored.prepared_mutants[0].source_b64) == artifact.data

def test_same_file_prepared_mutants_switch_directly_without_baseline_write(tmp_path: Path) -> None:
    # Replace M1 with M2 directly and keep only one final baseline restoration write.
    target = tmp_path / "app.py"
    target.write_text("def choose(value):\n    if value > 0:\n        return value + 1\n    return 0\n", encoding="utf-8")
    snapshot = create_snapshot(target, tmp_path / "recovery")
    first_mutant, second_mutant = _two_mutants(snapshot)
    first, second = prepare_mutant_artifacts(target, snapshot.original_bytes, (first_mutant, second_mutant))
    metrics = PerformanceMetrics()
    first_hash = switch_prepared_mutant(
        snapshot,
        first,
        expected_current_sha256=snapshot.original_sha256,
        durability="normal",
        metrics=metrics,
    )
    second_hash = switch_prepared_mutant(
        snapshot,
        second,
        expected_current_sha256=first_hash,
        durability="normal",
        metrics=metrics,
    )
    assert second_hash == second.sha256
    assert target.read_bytes() == second.data
    restore_snapshot(snapshot, expected_sha256=second_hash, durability="critical", metrics=metrics)
    assert target.read_bytes() == snapshot.original_bytes
    assert metrics.source_mutation_writes == 3

def test_direct_switch_rejects_untrusted_active_source(tmp_path: Path) -> None:
    # Fail closed when a test or external process changes the active mutant before the next switch.
    target = tmp_path / "app.py"
    target.write_text("def choose(value):\n    if value > 0:\n        return value + 1\n    return 0\n", encoding="utf-8")
    snapshot = create_snapshot(target, tmp_path / "recovery")
    first_mutant, second_mutant = _two_mutants(snapshot)
    first, second = prepare_mutant_artifacts(target, snapshot.original_bytes, (first_mutant, second_mutant))
    first_hash = switch_prepared_mutant(
        snapshot,
        first,
        expected_current_sha256=snapshot.original_sha256,
        durability="normal",
    )
    target.write_text("def choose(value):\n    return 999\n", encoding="utf-8")
    external = target.read_bytes()
    with pytest.raises(RestoreError, match="refusing direct mutant switch"):
        switch_prepared_mutant(snapshot, second, expected_current_sha256=first_hash, durability="normal")
    assert target.read_bytes() == external

def test_production_runner_prefers_campaign_prepared_artifacts() -> None:
    # Lock the production hot path to preparation-time artifacts while retaining the legacy fallback explicitly.
    runner_source = inspect.getsource(MutationRunner._run_mutant_impl)
    preparation_source = inspect.getsource(CampaignPreparationService.prepare)
    assert "self._prepared_mutant_by_id.get(mutant.mutant_id)" in runner_source
    assert "prepare_mutant(snapshot, mutant)" in runner_source
    assert "prepare_mutant_artifacts(source_path, source_bytes, mutants)" in preparation_source
