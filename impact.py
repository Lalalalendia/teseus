"""Read-only adapters for normalized and legacy SQLite test-impact databases."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


V2_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS impact_links (
    source_path TEXT NOT NULL,
    function_id TEXT NOT NULL,
    line_no INTEGER,
    nodeid TEXT NOT NULL,
    executions INTEGER NOT NULL DEFAULT 0,
    kills INTEGER NOT NULL DEFAULT 0,
    median_ms REAL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(source_path, function_id, line_no, nodeid)
);
CREATE INDEX IF NOT EXISTS ix_impact_function ON impact_links(source_path, function_id);
CREATE INDEX IF NOT EXISTS ix_impact_line ON impact_links(source_path, line_no);
CREATE INDEX IF NOT EXISTS ix_impact_nodeid ON impact_links(nodeid);
"""


class SQLiteImpactAdapter:
    """Read normalized impact with an explicitly diagnosed legacy fallback."""

    TABLE_PREFERENCE = ("test_symbols", "test_symbol_links", "test_inputs")
    TEST_COLUMNS = ("nodeid", "test_nodeid", "test_id", "test_key", "test")
    SYMBOL_COLUMNS = ("function_id", "symbol_id", "symbol", "qualname", "function")
    PATH_COLUMNS = ("rel_path", "path", "source_path", "file_path")

    def __init__(self, database: Path) -> None:
        # Initialize diagnostics before opening an optional compatibility database.
        self.database = database
        self.diagnostics: dict[str, object] = {
            "status": "missing" if not database.exists() else "unknown",
            "schema_version": None,
            "impact_mode": "missing" if not database.exists() else "unknown",
            "warning": None,
            "error": None,
            "rows_scanned": 0,
            "rows_returned": 0,
            "pages_scanned": 0,
            "truncated": False,
        }
        self._connection: sqlite3.Connection | None = None
        self._objects_cache: set[str] | None = None
        self._columns_cache: dict[str, set[str]] = {}
        self._schema_mode: str | None = None

    def close(self) -> None:
        # Release the campaign-scoped read-only connection when selection ends.
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def select_tests(
        self,
        source_path: str,
        function_id: str | None = None,
        line_no: int | None = None,
        *,
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[dict[str, Any], ...]:
        # Select impact rows through a cached schema and one campaign-scoped connection.
        if not self.database.exists():
            return ()
        try:
            connection = self._connect()
        except sqlite3.Error as exc:
            self.diagnostics.update(status="error", impact_mode="error", error=str(exc))
            return ()
        try:
            tables = self._objects(connection)
            if self._schema_mode is None:
                if "impact_links" in tables:
                    columns = self._columns(connection, "impact_links")
                    required = {"source_path", "function_id", "line_no", "nodeid", "executions", "kills", "median_ms"}
                    self._schema_mode = "normalized" if required.issubset(columns) else "incompatible"
                else:
                    self._schema_mode = "legacy"
            if self._schema_mode == "normalized":
                self.diagnostics.update(
                    status="ok",
                    schema_version=2,
                    impact_mode="normalized",
                    warning=None,
                    error=None,
                )
                return self._select_normalized(
                    connection,
                    source_path,
                    function_id,
                    line_no,
                    limit=limit,
                    offset=offset,
                )
            if self._schema_mode == "incompatible":
                self.diagnostics.update(
                    status="incompatible",
                    impact_mode="incompatible",
                    schema_version=None,
                    error="impact_links table is missing required v2 columns",
                )
            if line_no is not None:
                self.diagnostics.update(
                    status="legacy-unavailable",
                    impact_mode="legacy-scan",
                    warning="legacy impact schema has no line-level query",
                )
                return ()
            return self._select_legacy(
                connection,
                tables,
                source_path,
                function_id,
                limit=limit,
                offset=offset,
            )
        except sqlite3.Error as exc:
            self.diagnostics.update(status="error", impact_mode="error", error=str(exc))
            return ()

    def _select_normalized(
        self,
        connection: sqlite3.Connection,
        source_path: str,
        function_id: str | None,
        line_no: int | None,
        *,
        limit: int | None,
        offset: int,
    ) -> tuple[dict[str, Any], ...]:
        # Execute a bounded indexed query for normalized impact data.
        normalized_source = source_path.replace("\\", "/").lstrip("./")
        source_values = (source_path, normalized_source, f"./{normalized_source}")
        clauses = ["source_path IN (?, ?, ?)"]
        parameters: list[object] = list(source_values)
        if function_id:
            clauses.append("function_id = ?")
            parameters.append(function_id)
        if line_no is not None:
            clauses.append("line_no = ?")
            parameters.append(line_no)
        paging = " LIMIT -1 OFFSET ?" if limit is None else " LIMIT ? OFFSET ?"
        query = f"""
            WITH filtered AS (
                SELECT nodeid, line_no, executions, kills, median_ms, source_path,
                       COUNT(*) OVER () AS matched_rows
                FROM impact_links
                WHERE {' AND '.join(clauses)}
            ),
            ranked AS (
                SELECT nodeid, line_no, executions, kills, median_ms, source_path, matched_rows,
                       ROW_NUMBER() OVER (
                           PARTITION BY nodeid
                           ORDER BY executions DESC, kills DESC, median_ms IS NULL,
                                    median_ms ASC, source_path = ? DESC,
                                    source_path ASC, line_no IS NULL, line_no ASC
                       ) AS duplicate_rank
                FROM filtered
            ),
            deduplicated AS (
                SELECT nodeid, line_no, executions, kills, median_ms, matched_rows,
                       COUNT(*) OVER () AS unique_rows
                FROM ranked
                WHERE duplicate_rank = 1
            )
            SELECT nodeid, line_no, executions, kills, median_ms, matched_rows, unique_rows
            FROM deduplicated
            ORDER BY executions DESC, kills DESC, median_ms IS NULL, median_ms ASC, nodeid ASC
            {paging}
        """
        parameters.append(normalized_source)
        parameters.append(max(0, offset))
        if limit is not None:
            parameters.insert(-1, max(0, limit))
        rows = connection.execute(query, parameters)
        raw_rows = list(rows)
        result = self._unique(
            [
                {
                    "nodeid": str(row[0]),
                    "reason": "sqlite:impact_links:line" if line_no is not None else "sqlite:impact_links:function",
                    "line_no": row[1],
                    "executions": int(row[2] or 0),
                    "kills": int(row[3] or 0),
                    "median_ms": float(row[4]) if row[4] is not None else None,
                }
                for row in raw_rows
                if row[0] is not None
            ]
        )
        matched_rows = int(raw_rows[0][5] or 0) if raw_rows else 0
        unique_rows = int(raw_rows[0][6] or 0) if raw_rows else 0
        self.diagnostics.update(
            rows_scanned=matched_rows,
            rows_returned=len(result),
            pages_scanned=1,
            truncated=limit is not None and offset + len(result) < unique_rows,
        )
        return result

    def _select_legacy(
        self,
        connection: sqlite3.Connection,
        tables: set[str],
        source_path: str,
        function_id: str | None,
        *,
        limit: int | None,
        offset: int,
    ) -> tuple[dict[str, Any], ...]:
        # Read legacy tables page by page so compatibility mode stays bounded.
        test_aliases = self._aliases(connection, tables, "tests", ("id", "test_id"), self.TEST_COLUMNS, "nodeid")
        symbol_aliases = self._aliases(
            connection,
            tables,
            "symbol_fingerprints",
            ("id", "symbol_id"),
            ("function_id", "symbol", "qualname", "path", "rel_path"),
            "function_id",
        )
        results: list[dict[str, Any]] = []
        rows_scanned = 0
        pages_scanned = 0
        max_rows = 1_000_000
        page_size = 10_000
        skipped = max(0, offset)
        truncated = False
        for table in self.TABLE_PREFERENCE:
            if table not in tables:
                continue
            columns = self._columns(connection, table)
            test_column = next((item for item in self.TEST_COLUMNS if item in columns), None)
            symbol_column = next((item for item in self.SYMBOL_COLUMNS if item in columns), None)
            path_column = next((item for item in self.PATH_COLUMNS if item in columns), None)
            if not test_column or not (symbol_column or path_column):
                continue
            quoted = '"' + table.replace('"', '""') + '"'
            selected_columns = [test_column]
            if symbol_column:
                selected_columns.append(symbol_column)
            if path_column:
                selected_columns.append(path_column)
            table_offset = 0
            while rows_scanned < max_rows:
                try:
                    rows = connection.execute(
                        f"SELECT {', '.join(selected_columns)} FROM {quoted} LIMIT ? OFFSET ?",
                        (page_size, table_offset),
                    ).fetchall()
                except sqlite3.Error:
                    break
                pages_scanned += 1
                if not rows:
                    break
                table_offset += len(rows)
                rows_scanned += len(rows)
                for row in rows:
                    test_value = str(row[0] or "")
                    test_value = test_aliases.get(test_value, test_value)
                    symbol_value = str(row[1] or "") if symbol_column else ""
                    symbol_value = symbol_aliases.get(symbol_value, symbol_value)
                    path_value = str(row[2 if symbol_column else 1] or "") if path_column else ""
                    if not test_value or not self._matches(symbol_value, path_value, source_path, function_id):
                        continue
                    if skipped:
                        skipped -= 1
                        continue
                    results.append({"nodeid": test_value, "reason": f"sqlite:{table}", "source": path_value})
                    if limit is not None and len(results) >= max(0, limit):
                        truncated = True
                        break
                if truncated or len(rows) < page_size:
                    break
            if truncated or rows_scanned >= max_rows:
                truncated = truncated or rows_scanned >= max_rows
                break
        unique = self._unique(results)
        self.diagnostics.update(
            status="legacy-scan" if unique or rows_scanned else "incompatible",
            impact_mode="legacy-scan",
            schema_version=None,
            warning="slow compatibility adapter",
            error=None if unique or rows_scanned else "no compatible impact table found",
            rows_scanned=rows_scanned,
            rows_returned=len(unique),
            pages_scanned=pages_scanned,
            truncated=truncated,
        )
        return unique

    def _connect(self) -> sqlite3.Connection:
        # Open and reuse one read-only connection for the campaign selection hot path.
        if self._connection is None:
            uri = f"file:{self.database.resolve().as_posix()}?mode=ro"
            self._connection = sqlite3.connect(uri, uri=True)
            self._connection.row_factory = sqlite3.Row
        return self._connection

    def _objects(self, connection: sqlite3.Connection) -> set[str]:
        # Return cached available tables and views for schema negotiation.
        if self._objects_cache is None:
            rows = connection.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')")
            self._objects_cache = {str(row[0]) for row in rows}
        return self._objects_cache

    def _columns(self, connection: sqlite3.Connection, table: str) -> set[str]:
        # Inspect and cache one table without constructing a writable schema.
        if table in self._columns_cache:
            return self._columns_cache[table]
        escaped = table.replace('"', '""')
        columns = {str(row[1]) for row in connection.execute(f'PRAGMA table_info("{escaped}")')}
        self._columns_cache[table] = columns
        return columns

    def _aliases(
        self,
        connection: sqlite3.Connection,
        tables: set[str],
        table: str,
        id_candidates: tuple[str, ...],
        value_candidates: tuple[str, ...],
        preferred_value: str,
    ) -> dict[str, str]:
        # Resolve legacy numeric IDs to textual nodeids when that table exists.
        if table not in tables:
            return {}
        columns = self._columns(connection, table)
        id_column = next((item for item in id_candidates if item in columns), None)
        value_column = preferred_value if preferred_value in columns else next((item for item in value_candidates if item in columns), None)
        if not id_column or not value_column:
            return {}
        quoted = '"' + table.replace('"', '""') + '"'
        try:
            rows = connection.execute(f'SELECT "{id_column}", "{value_column}" FROM {quoted} LIMIT 200000')
        except sqlite3.Error:
            return {}
        return {str(row[0]): str(row[1]) for row in rows if row[0] is not None and row[1] is not None}

    @staticmethod
    def _matches(symbol: str, path: str, source_path: str, function_id: str | None) -> bool:
        # Match a legacy symbol/path row against the requested source function.
        normalized_path = path.replace("\\", "/").lstrip("./")
        normalized_source = source_path.replace("\\", "/").lstrip("./")
        if normalized_path and normalized_source not in normalized_path and normalized_path not in normalized_source:
            return False
        if not function_id:
            return True
        if not symbol:
            return bool(path)
        return function_id in symbol or symbol in function_id or function_id.rsplit("::", 1)[-1] in symbol

    @staticmethod
    def _unique(results: list[dict[str, Any]]) -> tuple[dict[str, Any], ...]:
        # Preserve the first ranked row for each nodeid.
        seen: set[str] = set()
        unique: list[dict[str, Any]] = []
        for result in results:
            nodeid = str(result["nodeid"])
            if nodeid not in seen:
                seen.add(nodeid)
                unique.append(result)
        return tuple(unique)


def load_impact_tests(
    database: Path,
    source_path: str,
    function_id: str | None,
    line_no: int | None = None,
) -> tuple[dict[str, Any], ...]:
    # Load compatible impact rows through the public adapter contract.
    return SQLiteImpactAdapter(database).select_tests(source_path, function_id, line_no)


def migrate_impact_database(source: Path, output: Path) -> dict[str, object]:
    """Create schema v2 and copy rows when a legacy table is structurally compatible."""
    if not source.exists():
        raise FileNotFoundError(source)
    output.parent.mkdir(parents=True, exist_ok=True)
    source_connection = sqlite3.connect(f"file:{source.resolve().as_posix()}?mode=ro", uri=True)
    target_connection = sqlite3.connect(output)
    copied = 0
    try:
        target_connection.executescript(V2_SCHEMA_SQL)
        tables = {
            str(row[0])
            for row in source_connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        for table in ("impact_links", "test_symbol_links", "test_symbols", "test_inputs"):
            if table not in tables:
                continue
            columns = {
                str(row[1])
                for row in source_connection.execute(f'PRAGMA table_info("{table}")')
            }
            required = {"source_path", "function_id", "nodeid"}
            if not required.issubset(columns):
                continue
            line_expression = "line_no" if "line_no" in columns else "NULL"
            executions_expression = "executions" if "executions" in columns else "0"
            kills_expression = "kills" if "kills" in columns else "0"
            median_expression = "median_ms" if "median_ms" in columns else "NULL"
            rows = source_connection.execute(
                f"SELECT source_path, function_id, {line_expression}, nodeid, "
                f"{executions_expression}, {kills_expression}, {median_expression} FROM \"{table}\""
            )
            # Copy rows incrementally so the result reports the actual count.
            for row in rows:
                target_connection.execute(
                    "INSERT OR REPLACE INTO impact_links(source_path,function_id,line_no,nodeid,executions,kills,median_ms) VALUES (?,?,?,?,?,?,?)",
                    row,
                )
                copied += 1
            break
        target_connection.execute("INSERT OR REPLACE INTO metadata(key,value) VALUES ('schema_version','2')")
        target_connection.commit()
    finally:
        source_connection.close()
        target_connection.close()
    return {"source": str(source), "output": str(output), "schema_version": 2, "copied_rows": copied}
