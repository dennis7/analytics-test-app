"""MCP server for exploring DuckDB databases.

Connects to one or two DuckDB databases via ATTACH and exposes tools for
schema browsing, table inspection, and ad-hoc SQL queries.

Usage:
    analytics-mcp --db path/to/first.duckdb
    analytics-mcp --db path/to/first.duckdb --db path/to/second.duckdb
"""

import argparse
import logging
import logging.handlers
import sys
from pathlib import Path

import duckdb
from mcp.server.fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_DIR = Path.cwd() / "logs"
LOG_DIR.mkdir(exist_ok=True)

logger = logging.getLogger("analytics-mcp")
logger.setLevel(logging.INFO)

_handler = logging.handlers.RotatingFileHandler(
    LOG_DIR / "mcp.log", maxBytes=5_000_000, backupCount=3
)
_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(_handler)

# ---------------------------------------------------------------------------
# CLI args (parsed before FastMCP takes over stdio)
# ---------------------------------------------------------------------------


def _parse_db_args() -> list[Path]:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--db", action="append", required=True, help="Path to DuckDB file (max 2)")
    args, _ = parser.parse_known_args()

    if len(args.db) > 2:
        print("Error: at most 2 databases supported", file=sys.stderr)
        sys.exit(1)

    return [Path(p) for p in args.db]


DB_PATHS: list[Path] = _parse_db_args()
logger.info("Resolved DB paths: %s", DB_PATHS)

# ---------------------------------------------------------------------------
# DuckDB connection (single in-memory conn with ATTACHed databases)
# ---------------------------------------------------------------------------

_conn: duckdb.DuckDBPyConnection | None = None
_label_map: dict[str, str] = {}  # catalog_name -> display label


def _get_conn() -> duckdb.DuckDBPyConnection:
    global _conn, _label_map
    if _conn is None:
        _conn = duckdb.connect(":memory:")
        for path in DB_PATHS:
            safe_path = str(path).replace("'", "''")
            _conn.execute(f"ATTACH '{safe_path}' (READ_ONLY)")
        # Build label map from attached databases
        attached = _conn.execute(
            "SELECT database_name, path FROM duckdb_databases() WHERE NOT internal"
        ).fetchall()
        for db_name, db_path in attached:
            if db_path:
                _label_map[db_name] = Path(db_path).stem
    return _conn


def _run_query(sql: str, params: list | None = None) -> str:
    logger.info("Query: %s", sql.strip()[:200])
    conn = _get_conn()
    result = conn.execute(sql, params or [])
    columns = [desc[0] for desc in result.description]
    rows = result.fetchall()

    if not rows:
        return "_No rows returned._"

    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    body = "\n".join("| " + " | ".join(_fmt(v) for v in row) + " |" for row in rows)
    return f"{header}\n{separator}\n{body}"


def _fmt(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if 0 < abs(value) < 0.001:
            return f"{value:.6e}"
        return f"{value:,.4f}"
    return str(value)


_EXCLUDED_FILTER = """
    table_catalog NOT IN ('memory', 'system', 'temp')
    AND table_schema NOT LIKE 'sqlmesh%'
    AND table_schema NOT IN ('information_schema', 'sqlmesh')
"""

# ---------------------------------------------------------------------------
# MCP server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "DuckDB Explorer",
    instructions=(
        "You have access to one or more DuckDB databases for exploration. "
        "Use list_tables to see what's available, describe_table to inspect columns, "
        "and query to run any read-only SQL. "
        'For cross-database queries, use "catalog-name".schema.table syntax.'
    ),
)


@mcp.resource("schema://tables")
def schema_overview() -> str:
    """Return a description of all tables and their columns across all databases."""
    conn = _get_conn()
    tables = conn.execute(f"""
        SELECT table_catalog, table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE {_EXCLUDED_FILTER}
        ORDER BY table_catalog, table_schema, table_name
    """).fetchall()

    parts = []
    current_db = None
    for catalog, schema, table, ttype in tables:
        label = _label_map.get(catalog, catalog)
        if label != current_db:
            parts.append(f"\n## {label}\n")
            current_db = label
        cols = conn.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_catalog = $1 AND table_schema = $2 AND table_name = $3
            ORDER BY ordinal_position
            """,
            [catalog, schema, table],
        ).fetchall()
        col_list = ", ".join(f"{c[0]} ({c[1]})" for c in cols)
        parts.append(f"- **{schema}.{table}** [{ttype}]: {col_list}")

    return "# Database Schema\n" + "\n".join(parts)


@mcp.tool()
def list_tables() -> str:
    """List all tables and views across all attached databases with row counts."""
    conn = _get_conn()
    tables = conn.execute(f"""
        SELECT table_catalog, table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE {_EXCLUDED_FILTER}
        ORDER BY table_catalog, table_schema, table_name
    """).fetchall()

    rows = []
    for catalog, schema, table, ttype in tables:
        try:
            count = conn.execute(
                f'SELECT COUNT(*) FROM "{catalog}"."{schema}"."{table}"'
            ).fetchone()[0]
        except Exception:
            count = "?"
        label = _label_map.get(catalog, catalog)
        rows.append((label, schema, table, ttype, count))

    header = "| Database | Schema | Table | Type | Rows |"
    sep = "| --- | --- | --- | --- | --- |"
    body = "\n".join(f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]} |" for r in rows)
    return f"{header}\n{sep}\n{body}"


@mcp.tool()
def describe_table(table_name: str) -> str:
    """Show the columns, types, and sample values for a table.

    Args:
        table_name: Table name in any format:
                    'table' (searches all schemas/databases),
                    'schema.table', or 'catalog.schema.table'.
    """
    conn = _get_conn()
    parts = table_name.split(".")

    if len(parts) == 3:
        catalog, schema, tbl = parts
    elif len(parts) == 2:
        schema, tbl = parts
        match = conn.execute(
            f"""
            SELECT table_catalog FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = $2
              AND {_EXCLUDED_FILTER}
            LIMIT 1
            """,
            [schema, tbl],
        ).fetchone()
        if not match:
            return f"Table '{table_name}' not found."
        catalog = match[0]
    else:
        tbl = parts[0]
        match = conn.execute(
            f"""
            SELECT table_catalog, table_schema FROM information_schema.tables
            WHERE table_name = $1
              AND {_EXCLUDED_FILTER}
            LIMIT 1
            """,
            [tbl],
        ).fetchone()
        if not match:
            return f"Table '{table_name}' not found."
        catalog, schema = match

    cols = conn.execute(
        """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_catalog = $1 AND table_schema = $2 AND table_name = $3
        ORDER BY ordinal_position
        """,
        [catalog, schema, tbl],
    ).fetchall()

    if not cols:
        return f"Table '{table_name}' has no columns (or doesn't exist)."

    try:
        sample = conn.execute(f'SELECT * FROM "{catalog}"."{schema}"."{tbl}" LIMIT 1').fetchone()
    except Exception:
        sample = None

    label = _label_map.get(catalog, catalog)
    title = f"**[{label}] {schema}.{tbl}**"
    header = "| Column | Type | Nullable | Sample |"
    sep = "| --- | --- | --- | --- |"
    body_rows = []
    for i, (col, dtype, nullable) in enumerate(cols):
        val = _fmt(sample[i]) if sample else ""
        body_rows.append(f"| {col} | {dtype} | {nullable} | {val} |")

    return f"{title}\n\n{header}\n{sep}\n" + "\n".join(body_rows)


@mcp.tool()
def query(sql: str) -> str:
    """Run a read-only SQL query against the attached DuckDB database(s).

    All attached databases are accessible. Use fully-qualified names for
    cross-database queries: "catalog-name".schema.table_name

    Use list_tables() first to see available databases, schemas, and tables.

    Args:
        sql: A SELECT query. DML/DDL statements are not allowed (read-only mode).
    """
    try:
        return _run_query(sql)
    except duckdb.Error as e:
        logger.error("Query error: %s", e)
        return f"**Query error:** {e}"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    for path in DB_PATHS:
        if not path.exists():
            logger.error("Database not found at %s", path)
            print(f"Error: database not found at {path}", file=sys.stderr)
            sys.exit(1)
    logger.info("Starting MCP server with DBs: %s", DB_PATHS)
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
