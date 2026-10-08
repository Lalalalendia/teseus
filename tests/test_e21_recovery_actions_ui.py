from __future__ import annotations
from pathlib import Path
from theseus_ui import CampaignUiApplication, asset_bytes
from test_e21_campaign_ui import FakeCampaignClient
def test_reconcile_and_recover_forward_one_action_identity_and_revision() -> None:
    # Make exactly one protocol mutation call for each explicit browser submission.
    client = FakeCampaignClient()
    app = CampaignUiApplication(client)
    reconcile = app.handle(
        "POST",
        "/api/campaigns/campaign-1/actions/reconcile",
        {},
        {"action_id": "action-reconcile", "expected_revision": 7},
    )
    recover = app.handle(
        "POST",
        "/api/campaigns/campaign-1/actions/recover",
        {},
        {"action_id": "action-recover", "expected_revision": 7},
    )
    assert reconcile.status == 200
    assert reconcile.body["value"]["status"] == "running"
    assert recover.status == 200
    assert recover.body["value"]["status"] == "completed"
    assert ("reconcile", ("action-reconcile", "campaign-1", 7)) in client.calls
    assert ("recover", ("action-recover", "campaign-1", 7)) in client.calls
    assert client.action_calls == 2
def test_running_action_receipt_is_read_without_reissuing_mutation() -> None:
    # Poll one durable action through GET while leaving the action call count unchanged.
    client = FakeCampaignClient()
    app = CampaignUiApplication(client)
    submitted = app.handle(
        "POST",
        "/api/campaigns/campaign-1/actions/reconcile",
        {},
        {"action_id": "action-reconcile", "expected_revision": 7},
    )
    before = client.action_calls
    receipt = app.handle(
        "GET",
        "/api/campaigns/campaign-1/recovery-actions/action-reconcile",
        {"limit": ["1"]},
    )
    assert submitted.body["value"]["status"] == "running"
    assert receipt.body["value"]["status"] == "running"
    assert client.action_calls == before
def test_stale_recovery_action_is_returned_without_automatic_retry() -> None:
    # Preserve one stale rejection for the browser to refresh without another action request.
    class StaleClient(FakeCampaignClient):
        def reconcile_campaign(self, action_id: str, campaign_id: str, *, expected_revision: int):
            # Return one stale durable rejection and count only the explicit submission.
            self.action_calls += 1
            return {"ok": False, "kind": "rejected", "error": {"code": "stale_revision", "message": "campaign changed", "retriable": False, "details": {"actual_revision": 8}}}
    client = StaleClient()
    response = CampaignUiApplication(client).handle(
        "POST",
        "/api/campaigns/campaign-1/actions/reconcile",
        {},
        {"action_id": "action-stale", "expected_revision": 7},
    )
    assert response.status == 409
    assert response.body["error"]["code"] == "stale_revision"
    assert client.action_calls == 1
def test_browser_recovery_actions_require_confirmation_and_reuse_session_identity() -> None:
    # Require confirmation, double-submit fencing and exact action ID reuse across transient failures.
    script = asset_bytes("app.js").decode("utf-8")
    assert "window.confirm(confirmation)" in script
    assert "state.recoveryActionBusy" in script
    assert "stableActionId(action)" in script
    assert "sessionStorage.getItem(key)" in script
    assert "the same action ID will be reused" in script
    assert "sessionStorage.removeItem(actionStorageKey(action))" in script
def test_browser_refresh_only_resumes_receipt_polling_and_never_posts_an_action() -> None:
    # Recover stored action state with GET polling rather than creating a mutation on page load.
    script = asset_bytes("app.js").decode("utf-8")
    resume_source = script.split("function resumeStoredRecoveryActionPolling", 1)[1].split("async function performRecoveryAction", 1)[0]
    assert "pollRecoveryAction" in resume_source
    assert "requestJson" not in resume_source
    assert 'method: "POST"' not in resume_source
    assert "setTimeout" not in resume_source
    polling_source = script.split("function pollRecoveryAction", 1)[1].split("function resumeStoredRecoveryActionPolling", 1)[0]
    assert "recovery-actions/" in polling_source
    assert "RECOVERY_ACTION_POLL_DELAY_MS" in polling_source
    assert "while (true)" not in polling_source.lower()
def test_changed_python_functions_keep_first_body_comment() -> None:
    # Enforce the repository rule for every changed Python function in the PR26-B overlay.
    import ast
    root = Path(__file__).resolve().parents[1]
    for relative in ("theseus_ui/app.py", "theseus_ui/client.py", "theseus_ui/serialization.py"):
        path = root / relative
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or not node.body:
                continue
            first = node.body[0]
            lines = path.read_text(encoding="utf-8").splitlines()
            assert first.lineno >= 2 and lines[first.lineno - 2].strip().startswith("#"), f"{relative}:{node.name}"
