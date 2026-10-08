from __future__ import annotations
import json
import zipfile
from dataclasses import replace
from io import BytesIO
from pathlib import Path
import pytest
from test_e24_survivor_human_review import _review, _validated_run
from theseus_survivor_adapter import (
    SurvivorEvidenceError,
    SurvivorExportFormat,
    SurvivorWorkflowState,
    publish_survivor_export,
)
def _approved_run():
    # Build one human-approved workflow ready for deterministic test-only export.
    workflow, run = _validated_run()
    return workflow, workflow.record_review(run, _review(run))
def test_export_requires_approval_and_emits_test_only_deterministic_zip() -> None:
    # Export exactly one test file and metadata with reproducible archive bytes.
    workflow, approved = _approved_run()
    first = workflow.export(approved, SurvivorExportFormat.OVERLAY_ZIP)
    workflow2, approved2 = _approved_run()
    second = workflow2.export(approved2, SurvivorExportFormat.OVERLAY_ZIP)
    assert first.checkpoint.state is SurvivorWorkflowState.EXPORTED
    assert first.export.export_id == second.export.export_id
    assert first.export.archive == second.export.archive
    with zipfile.ZipFile(BytesIO(first.export.archive)) as archive:
        names = archive.namelist()
        exported_path = first.export.files[0].path
        assert names == ["manifest.json", exported_path]
        assert exported_path.startswith("tests/test_app__survivor_")
        manifest = json.loads(archive.read("manifest.json"))
        assert manifest["mutant_id"] == approved.checkpoint.mutant_id
        assert manifest["validation_id"] == approved.validation.validation_id
        assert manifest["review_id"] == approved.review.review_id
        assert all(not name.startswith(("src/", "/")) for name in names)
def test_unified_diff_and_manifest_contain_no_workspace_or_temp_paths() -> None:
    # Diff identity and manifest must remain independent of physical checkout location.
    workflow, approved = _approved_run()
    exported = workflow.export(approved, SurvivorExportFormat.UNIFIED_DIFF)
    rendered = exported.export.unified_diff.decode("utf-8") + json.dumps(exported.export.manifest)
    assert "D:/" not in rendered
    assert "C:/" not in rendered
    assert "/home/" not in rendered
    assert f"+++ b/{exported.export.files[0].path}" in rendered
    assert "src/" not in "\n".join(item.path for item in exported.export.files)
def test_production_or_traversal_target_is_rejected() -> None:
    # A proposal cannot export production source or escape the project root.
    workflow, approved = _approved_run()
    evidence = next(item for item in approved.proposals.accepted if item.proposal_id == approved.checkpoint.proposal_id)
    forged_proposal = replace(evidence.proposal, target_test_file="../src/app.py")
    forged_evidence = replace(evidence, proposal=forged_proposal)
    forged_proposals = replace(
        approved.proposals,
        accepted=tuple(forged_evidence if item == evidence else item for item in approved.proposals.accepted),
    )
    forged_run = replace(approved, proposals=forged_proposals)
    with pytest.raises(SurvivorEvidenceError, match="export target"):
        workflow.export(forged_run, SurvivorExportFormat.OVERLAY_ZIP)
def test_overlay_directory_is_complete_and_exact_replay_is_idempotent(tmp_path: Path) -> None:
    # Publish manifest and candidate together through a complete atomic overlay directory.
    workflow, approved = _approved_run()
    exported = workflow.export(approved, SurvivorExportFormat.OVERLAY_DIRECTORY)
    destination = tmp_path / "overlay"
    first = publish_survivor_export(exported.export, destination)
    second = publish_survivor_export(exported.export, destination)
    assert first == second == destination
    assert (destination / "manifest.json").is_file()
    assert (destination / exported.export.files[0].path).is_file()
def test_overlay_directory_fsync_uses_the_original_writable_handle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Reject reopening completed overlay files read-only before fsync on Windows.
    original_open = Path.open
    opened_modes: list[str] = []
    def tracked_open(self: Path, mode: str = "r", *args, **kwargs):
        # Record only files created under this test's export destination.
        if self == tmp_path or tmp_path in self.parents:
            opened_modes.append(mode)
        return original_open(self, mode, *args, **kwargs)
    monkeypatch.setattr(Path, "open", tracked_open)
    workflow, approved = _approved_run()
    exported = workflow.export(approved, SurvivorExportFormat.OVERLAY_DIRECTORY)
    publish_survivor_export(exported.export, tmp_path / "overlay")
    assert "wb" in opened_modes
    assert "rb" not in opened_modes
def test_completed_export_rejects_format_drift() -> None:
    # Exact export replay may republish bytes but cannot change representation identity.
    from theseus_survivor_adapter import SurvivorProposalConflict
    workflow, approved = _approved_run()
    exported = workflow.export(approved, SurvivorExportFormat.OVERLAY_ZIP)
    with pytest.raises(SurvivorProposalConflict, match="export format conflict"):
        workflow.export(exported, SurvivorExportFormat.UNIFIED_DIFF)