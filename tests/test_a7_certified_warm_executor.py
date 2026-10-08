from __future__ import annotations

from theseus_performance import CertifiedWarmExecutor, WarmExecutionKey, WarmExecutionTrustStore


def _key() -> WarmExecutionKey:
    # Build one exact certification boundary reused across deterministic audit attempts.
    return WarmExecutionKey("project", "environment", "tests", "warm-v1")


def test_a7_warm_executor_requires_three_fresh_audits_before_warm_only(tmp_path) -> None:
    # Keep fresh execution authoritative until repeated equivalent warm results prove both correctness and speed.
    store = WarmExecutionTrustStore(tmp_path / "warm.json")
    executor = CertifiedWarmExecutor[str](store, audit_rate=0.0)
    calls = {"warm": 0, "fresh": 0}

    def warm():
        # Return one faster semantic result from the candidate warm backend.
        calls["warm"] += 1
        return "killed", 0.1

    def fresh():
        # Return the same semantic result from the existing fresh-process authority.
        calls["fresh"] += 1
        return "killed", 1.0

    for index in range(3):
        result = executor.execute(f"exec-{index}", _key(), warm_call=warm, fresh_call=fresh, result_digest=str)
        assert result.mode == "fresh_audit"
        assert result.matched is True
    certified = executor.execute("exec-4", _key(), warm_call=warm, fresh_call=fresh, result_digest=str)
    assert certified.mode == "warm_certified"
    assert certified.result == "killed"
    assert calls == {"warm": 4, "fresh": 3}


def test_a7_any_warm_mismatch_quarantines_exact_key_and_returns_fresh_authority(tmp_path) -> None:
    # Fail closed on the first semantic mismatch and never let the warm result become authoritative.
    store = WarmExecutionTrustStore(tmp_path / "warm.json")
    executor = CertifiedWarmExecutor[str](store)
    result = executor.execute(
        "exec-1",
        _key(),
        warm_call=lambda: ("survived", 0.1),
        fresh_call=lambda: ("killed", 1.0),
        result_digest=str,
    )
    assert result.result == "killed"
    assert result.matched is False
    assert result.trust.quarantined is True
    second = executor.execute(
        "exec-2",
        _key(),
        warm_call=lambda: (_ for _ in ()).throw(AssertionError("warm must not run after quarantine")),
        fresh_call=lambda: ("killed", 1.0),
        result_digest=str,
    )
    assert second.mode == "fresh_quarantined"
