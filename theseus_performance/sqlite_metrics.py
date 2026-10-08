"""Low-overhead SQLite statement metrics shared by control-plane stores."""
from __future__ import annotations

from dataclasses import dataclass, field
from threading import Lock


@dataclass(slots=True)
class SQLiteMetrics:
    """Count SQL work without retaining statements or changing transaction semantics."""

    _lock: Lock = field(default_factory=Lock, init=False, repr=False)
    _statement_count: int = field(default=0, init=False, repr=False)
    _query_count: int = field(default=0, init=False, repr=False)
    _write_count: int = field(default=0, init=False, repr=False)
    _transaction_count: int = field(default=0, init=False, repr=False)
    _commit_count: int = field(default=0, init=False, repr=False)
    _rollback_count: int = field(default=0, init=False, repr=False)

    def trace(self, statement: str) -> None:
        """Receive one sqlite trace statement and classify it in O(1) space."""
        normalized = str(statement).strip().upper()
        if not normalized:
            return
        keyword = normalized.split(None, 1)[0]
        with self._lock:
            self._statement_count += 1
            if keyword in {"SELECT", "PRAGMA", "EXPLAIN", "WITH"}:
                self._query_count += 1
            elif keyword in {"INSERT", "UPDATE", "DELETE", "REPLACE"}:
                self._write_count += 1
            if keyword == "BEGIN":
                self._transaction_count += 1
            elif keyword == "COMMIT":
                self._commit_count += 1
            elif keyword == "ROLLBACK":
                self._rollback_count += 1

    def reset(self) -> None:
        """Discard initialization/recovery setup counts before a measured campaign."""
        with self._lock:
            self._statement_count = 0
            self._query_count = 0
            self._write_count = 0
            self._transaction_count = 0
            self._commit_count = 0
            self._rollback_count = 0

    def snapshot(self) -> dict[str, float]:
        """Return a stable numeric snapshot suitable for timeline diagnostics."""
        with self._lock:
            return {
                "sqlite_statement_count": float(self._statement_count),
                "sqlite_query_count": float(self._query_count),
                "sqlite_write_count": float(self._write_count),
                "sqlite_transaction_count": float(self._transaction_count),
                "sqlite_commit_count": float(self._commit_count),
                "sqlite_rollback_count": float(self._rollback_count),
            }


__all__ = ["SQLiteMetrics"]
