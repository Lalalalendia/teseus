from __future__ import annotations

from theseus_ui import asset_bytes
from theseus_ui.serialization import to_json_value


def test_a13_campaign_detail_uses_compact_priority_layout() -> None:
    # Keep the primary campaign screen compact and move secondary diagnostics behind disclosures.
    html = asset_bytes("index.html").decode("utf-8")
    assert 'id="campaign-alerts"' in html
    assert 'id="campaign-context"' in html
    assert 'id="campaign-summary" class="campaign-summary-grid"' in html
    assert 'id="campaign-secondary-summary"' in html
    assert "Технические детали" in html
    assert "Результаты мутантов" in html
    assert "Рабочие процессы" in html
    assert "Шарды" in html


def test_a13_metric_css_prevents_vertical_letter_collapse() -> None:
    # Prevent generic definition-list columns from squeezing metric values to a few pixels.
    css = asset_bytes("styles.css").decode("utf-8")
    assert "dl.metric { display: block; }" in css
    assert ".campaign-summary-grid { grid-template-columns: repeat(6, minmax(8.5rem, 1fr)); }" in css
    assert "word-break: normal" in css
    assert ".dense-table { min-width: 70rem; }" in css


def test_a13_status_badges_tooltips_progress_and_failure_callout_are_browser_safe() -> None:
    # Require visual state compression without introducing unsafe HTML rendering or inline script payloads.
    script = asset_bytes("app.js").decode("utf-8")
    for symbol in ("statusTone", "statusBadge", "attachTooltip", "addProgressMetric", "renderFailureAlert", "addStatusCell"):
        assert f"function {symbol}" in script
    assert 'className = "status-badge"' in script
    assert 'className = "has-tooltip"' not in script
    assert 'classList.add("has-tooltip")' in script
    assert 'document.createElement("progress")' in script
    assert "innerHTML" not in script


def test_a13_source_path_is_visible_only_when_relative() -> None:
    # Show useful relative source identity while retaining absolute-path privacy filtering.
    relative = to_json_value({"source_path": "backend/app/service.py", "root_path": "D:/private/project"})
    absolute = to_json_value({"source_path": "D:/private/project/backend/app/service.py"})
    assert relative == {"source_path": "backend/app/service.py"}
    assert absolute == {"source_path": "[redacted]"}
