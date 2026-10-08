from __future__ import annotations
import os
from pathlib import Path
from theseus_ui import UI_CSP, asset_bytes
def test_assets_are_loaded_from_package_resources_independently_of_cwd(tmp_path: Path) -> None:
    # Resolve every asset after changing to an unrelated empty working directory.
    original = Path.cwd()
    os.chdir(tmp_path)
    try:
        html = asset_bytes("index.html").decode("utf-8")
        script = asset_bytes("app.js").decode("utf-8")
        styles = asset_bytes("styles.css").decode("utf-8")
    finally:
        os.chdir(original)
    assert "Theseus — кампании" in html
    assert "Восстановление и диагностика" in html
    assert "DOMContentLoaded" in script
    assert ":focus-visible" in styles
def test_assets_have_no_inline_or_external_dependencies() -> None:
    # Keep scripts and styles local, CSP-compatible and free of telemetry endpoints.
    html = asset_bytes("index.html").decode("utf-8")
    script = asset_bytes("app.js").decode("utf-8")
    assert '<script src="/assets/app.js" defer></script>' in html
    assert "<style" not in html.lower()
    assert "onclick=" not in html.lower()
    assert "https://" not in html.lower()
    assert "http://" not in html.lower()
    assert "https://" not in script.lower()
    assert "http://" not in script.lower()
    assert "telemetry" not in script.lower()
    assert "innerhtml" not in script.lower()
    assert "eval(" not in script.lower()
    assert "Math.random" not in script
    assert "default-src 'self'" in UI_CSP
    assert "script-src 'self'" in UI_CSP
    assert "style-src 'self'" in UI_CSP
def test_browser_streaming_preserves_cursor_and_stops_when_page_closes() -> None:
    # Require bounded history, delayed polling, cursor persistence and explicit stop logic.
    script = asset_bytes("app.js").decode("utf-8")
    lowered = script.lower()
    assert "event_history_limit = 200" in lowered
    assert "settimeout" in lowered
    assert "cleartimeout" in lowered
    assert "sessionstorage.setitem" in lowered
    assert "state.eventcursor" in lowered
    assert "pagehide" in lowered
    assert "while (true)" not in lowered
    assert "setinterval" not in lowered
def test_browser_actions_reuse_one_action_id_and_do_not_retry_stale_revision() -> None:
    # Keep session-stored action identities and refresh stale state without replaying mutations.
    script = asset_bytes("app.js").decode("utf-8")
    assert "stableActionId" in script
    assert "sessionStorage.getItem(key)" in script
    assert "button.disabled" in script
    assert "window.confirm" in script
    stale_branch = script.split('errorCode === "stale_revision"', 1)[1]
    assert "refreshCampaignDetail" in stale_branch
    assert "performRecoveryAction(" not in stale_branch.split("}", 1)[0]
def test_recovery_assets_have_bounded_pages_and_authoritative_fields() -> None:
    # Keep recovery UI cursor-driven and display authoritative hung and orphaned values without inference.
    html = asset_bytes("index.html").decode("utf-8")
    script = asset_bytes("app.js").decode("utf-8")
    for heading in ("Сводка восстановления", "Аренды", "Рабочие процессы восстановления", "Очередь доставки", "Карантин", "Реестр артефактов", "Статистика тестов", "Подтверждения повторного использования"):
        assert heading in html
    assert "load-more-quarantine" in html
    assert "load-more-artifacts" in html
    assert "load-more-statistics" in html
    assert "load-more-reuse" in html
    assert "state.recoveryPages" in script
    assert "pageState.cursor" in script
    assert "lease.hung_state" in script
    assert "worker.orphaned" in script
    assert "heartbeat_age_seconds" in script
    assert "Date.now" not in script
def test_recovery_action_polling_is_bounded_and_campaign_scoped() -> None:
    # Poll one durable receipt with setTimeout and stop it when campaign identity changes.
    script = asset_bytes("app.js").decode("utf-8")
    assert "RECOVERY_ACTION_POLL_DELAY_MS" in script
    assert "pollRecoveryAction" in script
    assert "stopRecoveryActionPolling" in script
    assert "campaignId !== state.campaignId" in script
    assert "recovery-actions/" in script
    assert "resumeStoredRecoveryActionPolling" in script
    assert "setInterval" not in script
def test_packaging_declares_ui_script_and_static_package_data() -> None:
    # Verify editable and wheel installs receive the same UI package and assets.
    root = Path(__file__).resolve().parents[1]
    project = (root / "pyproject.toml").read_text(encoding="utf-8")
    assert 'theseus-ui = "theseus_ui.__main__:main"' in project
    assert '"theseus_ui"' in project
    assert 'theseus_ui = [' in project
    assert '"assets/*.html"' in project
    assert '"assets/*.js"' in project
    assert '"assets/*.css"' in project
