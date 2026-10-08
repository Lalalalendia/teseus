from __future__ import annotations
import unicodedata
from dataclasses import replace
import pytest
from theseus_survivor_lab.contracts import DependencyEvidence, TestEvidence as SurvivorTestEvidence
from theseus_survivor_lab.errors import ContractError
from theseus_survivor_lab.serialization import canonical_json
from theseus_survivor_lab.service import analyze_request
from theseus_survivor_lab.validation import (
    canonical_relative_path,
    canonical_utc_timestamp,
    semantic_text_sha256,
    sha256_text,
)
def _cross_platform_request(real_gap_request, *, checkout: str, newline: str, bom: bool, unicode_form: str):
    # Build one semantically identical request with host-specific path and text encodings.
    source_text = "def decide(x):\n    if x > 3:\n        return True\n    return False\n# café\n"
    test_text = "def test_boundary():\n    assert decide(3) is False\n# café\n"
    source_text = unicodedata.normalize(unicode_form, source_text).replace("\n", newline)
    test_text = unicodedata.normalize(unicode_form, test_text).replace("\n", newline)
    if bom:
        source_text = "\ufeff" + source_text
        test_text = "\ufeff" + test_text
    source_path = f"{checkout}/src/decision.py"
    test_path = f"{checkout}/tests/test_decision.py"
    related = SurvivorTestEvidence(
        nodeid="tests/test_decision.py::test_boundary",
        source_path=test_path,
        source_sha256=sha256_text(test_text),
        source_text=test_text,
        selection_reasons=("coverage",),
        executions=1,
        failures=0,
        median_duration_ms=1.25,
        killed_related_mutants=(),
    )
    mutant = replace(
        real_gap_request.mutant,
        source_path=source_path,
        diff=unicodedata.normalize(unicode_form, "- x > 3\n+ x >= 3\n").replace("\n", newline),
    )
    source = replace(
        real_gap_request.source,
        source_path=source_path,
        source_sha256=sha256_text(source_text),
        source_text=source_text,
        function_source=None,
        function_start_line=1,
        function_end_line=4,
    )
    selection = replace(
        real_gap_request.selection,
        selected_tests=(related.nodeid,),
        related_test_nodeids=(related.nodeid,),
    )
    execution = replace(
        real_gap_request.executions[0],
        selected_tests=(related.nodeid,),
        observed_tests=(related.nodeid,),
        output_excerpt=f"temporary output from {checkout}/tmp/run.log token=SECRET",
    )
    dependencies = (
        DependencyEvidence("unicodedata", "module", f"{checkout}/src/unicode_helper.py", "1", "direct_import"),
        DependencyEvidence("json", "module", f"{checkout}/src/json_helper.py", "1", "direct_import"),
    )
    return replace(
        real_gap_request,
        mutant=mutant,
        source=source,
        selection=selection,
        executions=(execution,),
        related_tests=(related,),
        related_dependencies=dependencies,
    )
def test_windows_linux_crlf_bom_and_unicode_forms_share_identity(real_gap_request) -> None:
    # Semantic text and project-relative paths must produce one analysis identity on both hosts.
    windows = _cross_platform_request(
        real_gap_request,
        checkout="C:/Users/Alice/project",
        newline="\r\n",
        bom=True,
        unicode_form="NFD",
    )
    linux = _cross_platform_request(
        real_gap_request,
        checkout="/home/alice/project",
        newline="\n",
        bom=False,
        unicode_form="NFC",
    )
    first = analyze_request(windows)
    second = analyze_request(linux)
    assert first.analysis_id == second.analysis_id
    assert first.result_id == second.result_id
    assert first.proposal_set_id == second.proposal_set_id
def test_input_order_and_unc_checkout_do_not_change_identity(real_gap_request) -> None:
    # Filesystem enumeration order and UNC checkout prefixes must not alter semantic identity.
    base = _cross_platform_request(
        real_gap_request,
        checkout="//server/share/project",
        newline="\n",
        bom=False,
        unicode_form="NFC",
    )
    reordered = replace(
        base,
        related_tests=tuple(reversed(base.related_tests)),
        related_dependencies=tuple(reversed(base.related_dependencies)),
        requested_modes=tuple(reversed(base.requested_modes)),
    )
    first = analyze_request(base)
    second = analyze_request(reordered)
    assert first.analysis_id == second.analysis_id
    assert first.result_id == second.result_id
def test_semantic_source_change_changes_identity(real_gap_request) -> None:
    # A real normalized source change must still create a distinct analysis identity.
    base = _cross_platform_request(real_gap_request, checkout="C:/repo/project", newline="\r\n", bom=True, unicode_form="NFC")
    changed_text = base.source.source_text.replace("return False", "return None")
    changed = replace(base, source=replace(base.source, source_text=changed_text, source_sha256=sha256_text(changed_text)))
    assert analyze_request(base).analysis_id != analyze_request(changed).analysis_id
@pytest.mark.parametrize(
    ("value", "expected"),
    (
        ("C:\\work\\project\\src\\pkg\\mod.py", "src/pkg/mod.py"),
        ("c:/work/project/src/pkg/mod.py", "src/pkg/mod.py"),
        ("/home/user/project/src/pkg/mod.py", "src/pkg/mod.py"),
        ("\\\\server\\share\\project\\tests\\test_mod.py", "tests/test_mod.py"),
        ("tests\\test_mod.py", "tests/test_mod.py"),
    ),
)
def test_path_normalization_is_cross_platform(value: str, expected: str) -> None:
    # Drive, POSIX, UNC, and slash variants must share one project-relative spelling.
    assert canonical_relative_path(value) == expected
@pytest.mark.parametrize("value", ("../secret.py", "src/../secret.py", "C:/repo/project/src/../../secret.py"))
def test_path_traversal_is_rejected(value: str) -> None:
    # Traversal segments must fail before they can enter any content identity.
    with pytest.raises(ContractError, match="traversal|escape"):
        canonical_relative_path(value)
def test_locale_and_timezone_helpers_are_canonical() -> None:
    # Decimal JSON and timestamps must not depend on locale or local timezone.
    assert canonical_json({"decimal": 1.25, "é": "cafe\u0301"}) == '{"decimal":1.25,"é":"café"}'
    assert canonical_utc_timestamp("2026-08-06T08:30:00+05:00") == "2026-08-06T03:30:00.000000Z"
    assert semantic_text_sha256("\ufeffcafe\u0301\r\n") == semantic_text_sha256("café\n")
