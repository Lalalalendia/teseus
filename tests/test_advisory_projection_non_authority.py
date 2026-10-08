from __future__ import annotations

import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pytest

from theseus_local.ai_analysis import DeterministicAnalysisProvider, analyze_report, write_advisory_analysis
from theseus_local.ai_mutation import MutationCandidate, MutationCandidateError, deduplicate_candidates, prepare_candidate
from theseus_local.operations import list_campaigns


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_unavailable_or_failed_provider_cannot_change_canonical_report() -> None:
    report = {
        "campaign_id": "c1",
        "status": "complete",
        "counts": {"killed": 1, "survived": 0},
        "results": [{"status": "killed", "mutant_id": "m1"}],
    }
    before = json.dumps(report, sort_keys=True)
    unavailable = analyze_report(report)

    class Provider:
        name = "throws"

        def analyze(self, value):
            raise RuntimeError("down")

    failed = analyze_report(report, provider=Provider())
    assert unavailable.status == "unavailable"
    assert failed.status == "failed"
    assert json.dumps(report, sort_keys=True) == before


def test_different_advisory_providers_have_no_semantic_authority() -> None:
    report = {"campaign_id": "c1", "status": "complete", "counts": {"survived": 1}, "mutants": [{"status": "survived", "mutant_id": "m1"}]}
    deterministic = analyze_report(report, provider=DeterministicAnalysisProvider())

    class Alternative:
        name = "alternative"

        def analyze(self, value):
            return {"observations": [{"kind": "other", "campaign": value["campaign_id"]}]}

    alternative = analyze_report(report, provider=Alternative())
    assert deterministic.source_report_sha256 == alternative.source_report_sha256
    assert report["status"] == "complete"
    assert report["counts"] == {"survived": 1}


def test_corrupt_sidecar_does_not_block_canonical_report_and_can_be_regenerated(tmp_path: Path) -> None:
    report = {"campaign_id": "c1", "status": "complete", "counts": {"killed": 1}}
    analysis = analyze_report(report)
    sidecar = tmp_path / "analysis.json"
    write_advisory_analysis(sidecar, analysis)
    sidecar.write_text("not-json", encoding="utf-8")
    assert report["status"] == "complete"
    sidecar.unlink()
    write_advisory_analysis(sidecar, analysis)
    assert json.loads(sidecar.read_text(encoding="utf-8"))["campaign_id"] == "c1"


def test_sidecar_replace_failure_leaves_old_valid_or_no_file_and_cleans_temp(tmp_path: Path) -> None:
    report = {"campaign_id": "c1", "status": "complete", "counts": {}}
    sidecar = tmp_path / "analysis.json"
    first = analyze_report(report)
    write_advisory_analysis(sidecar, first)
    before = sidecar.read_text(encoding="utf-8")
    changed = replace(first, analysis_id="changed")
    replacement = tmp_path / "replacement-analysis.json"
    with patch("theseus_local.ai_analysis.os.replace", side_effect=OSError("crash during publish")):
        with pytest.raises(OSError):
            write_advisory_analysis(replacement, changed)
    assert sidecar.read_text(encoding="utf-8") == before
    assert not replacement.exists()
    assert not tuple(tmp_path.glob(".replacement-analysis.json.*.tmp"))


def test_ai_candidates_fail_closed_and_deduplicate_only_after_normalization(tmp_path: Path) -> None:
    source = tmp_path / "module.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")
    good = MutationCandidate("module.py", 1, 8, "1", "2")
    prepared = prepare_candidate(good, tmp_path, allowed_source_paths=("module.py",))
    duplicate = prepare_candidate(replace(good, source_path="module.py"), tmp_path, allowed_source_paths=("module.py",))
    assert len(deduplicate_candidates((duplicate, prepared))) == 1
    assert not hasattr(prepared, "evidence_identity")
    assert not hasattr(prepared, "status")
    for candidate in (
        MutationCandidate("../module.py", 1, 0, "1", "2"),
        MutationCandidate("module.py", 1, 8, "1", "not valid >>>"),
        MutationCandidate("other.py", 1, 0, "1", "2"),
    ):
        with pytest.raises(MutationCandidateError):
            prepare_candidate(candidate, tmp_path, allowed_source_paths=("module.py",))
    source.write_text("VALUE = 9\n", encoding="utf-8")
    with pytest.raises(MutationCandidateError):
        prepare_candidate(good, tmp_path, allowed_source_paths=("module.py",))


def test_operations_projection_is_read_only_and_stably_ordered(tmp_path: Path) -> None:
    first = tmp_path / "b" / "canonical.report.json"
    second = tmp_path / "a" / "canonical.report.json"
    for path, campaign in ((first, "b"), (second, "a")):
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({"campaign_id": campaign, "status": "complete", "counts": {}}), encoding="utf-8")
    before = tuple(sorted((path.as_posix(), _digest(path)) for path in tmp_path.rglob("*") if path.is_file()))
    views = list_campaigns(tmp_path)
    after = tuple(sorted((path.as_posix(), _digest(path)) for path in tmp_path.rglob("*") if path.is_file()))
    assert before == after
    assert [item.campaign_id for item in views] == ["a", "b"]


def test_operations_projection_never_invents_missing_report_facts(tmp_path: Path) -> None:
    assert list_campaigns(tmp_path) == ()
    malformed = tmp_path / "bad.report.json"
    malformed.write_text("{broken", encoding="utf-8")
    assert list_campaigns(tmp_path) == ()


def test_operations_projection_is_bounded_and_exposes_input_hash_without_authority(tmp_path: Path) -> None:
    for index in range(5):
        path = tmp_path / f"{index:02d}" / "canonical.report.json"
        path.parent.mkdir(parents=True)
        path.write_text(
            json.dumps({"campaign_id": f"c-{index}", "status": "complete", "counts": {}}),
            encoding="utf-8",
        )
    first_page = list_campaigns(tmp_path, limit=2)
    second_page = list_campaigns(tmp_path, offset=2, limit=2)
    assert [item.campaign_id for item in first_page] == ["c-0", "c-1"]
    assert [item.campaign_id for item in second_page] == ["c-2", "c-3"]
    assert all(item.report_sha256 for item in first_page + second_page)
    before = first_page[0].report_sha256
    report = tmp_path / "00" / "canonical.report.json"
    report.write_text(
        json.dumps({"campaign_id": "c-0", "status": "complete", "counts": {"killed": 1}}),
        encoding="utf-8",
    )
    after = list_campaigns(tmp_path, limit=1)[0].report_sha256
    assert before != after


def test_operations_projection_sees_atomic_old_or_new_snapshot_during_update(tmp_path: Path) -> None:
    report = tmp_path / "canonical.report.json"
    old = {"campaign_id": "old", "status": "running", "counts": {}}
    new = {"campaign_id": "new", "status": "complete", "counts": {"killed": 1}}
    report.write_text(json.dumps(old), encoding="utf-8")

    def replace_report() -> None:
        temporary = report.with_suffix(".tmp")
        temporary.write_text(json.dumps(new), encoding="utf-8")
        os.replace(temporary, report)

    with ThreadPoolExecutor(max_workers=2) as pool:
        writer = pool.submit(replace_report)
        observed = [list_campaigns(tmp_path) for _ in range(20)]
        writer.result()
    assert all(len(view) <= 1 for view in observed)
    assert all(not view or view[0].campaign_id in {"old", "new"} for view in observed)
