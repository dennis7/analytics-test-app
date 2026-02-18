"""MCP server for conversational exploration of WACI portfolio carbon data.

Connects to the DuckDB database produced by either the dbt or SQLMesh pipeline
and exposes tools for querying portfolio holdings, carbon intensity, and WACI scores.

Usage:
    uv run python packages/mcp/server.py                          # dbt dev (default)
    uv run python packages/mcp/server.py --db packages/sqlmesh/output/dev/dev.duckdb
    uv run python packages/mcp/server.py --pipeline sqlmesh       # shorthand
    uv run python packages/mcp/server.py --pipeline dbt --env prd # dbt production
"""

import argparse
import sys
from pathlib import Path

import duckdb
from mcp.server.fastmcp import FastMCP

# ---------------------------------------------------------------------------
# CLI args (parsed before FastMCP takes over stdio)
# ---------------------------------------------------------------------------

def _resolve_db_path() -> Path:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--db", type=str, default=None, help="Path to DuckDB file")
    parser.add_argument("--pipeline", type=str, choices=["dbt", "sqlmesh"], default="dbt")
    parser.add_argument("--env", type=str, default="dev")
    args, _ = parser.parse_known_args()

    if args.db:
        return Path(args.db)

    # Resolve relative to this file's location (packages/mcp/) -> repo root
    repo_root = Path(__file__).resolve().parent.parent.parent
    if args.pipeline == "dbt":
        return repo_root / "packages" / "dbt" / "output" / args.env / f"{args.env}.duckdb"
    else:
        return repo_root / "packages" / "sqlmesh" / "output" / args.env / f"{args.env}.duckdb"


DB_PATH = _resolve_db_path()

# ---------------------------------------------------------------------------
# DuckDB connection helper
# ---------------------------------------------------------------------------

def _get_conn() -> duckdb.DuckDBPyConnection:
    """Open a read-only connection to the pipeline DuckDB."""
    return duckdb.connect(str(DB_PATH), read_only=True)


def _run_query(sql: str) -> str:
    """Execute SQL and return results as a formatted markdown table."""
    conn = _get_conn()
    try:
        result = conn.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()

        if not rows:
            return "_No rows returned._"

        # Build markdown table
        header = "| " + " | ".join(columns) + " |"
        separator = "| " + " | ".join("---" for _ in columns) + " |"
        body = "\n".join(
            "| " + " | ".join(_fmt(v) for v in row) + " |"
            for row in rows
        )
        return f"{header}\n{separator}\n{body}"
    finally:
        conn.close()


def _fmt(value) -> str:
    """Format a cell value for display."""
    if value is None:
        return ""
    if isinstance(value, float):
        # Use scientific notation for very small numbers, otherwise 4 decimals
        if 0 < abs(value) < 0.001:
            return f"{value:.6e}"
        return f"{value:,.4f}"
    return str(value)


# ---------------------------------------------------------------------------
# MCP server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "WACI Data Explorer",
    instructions=(
        "You have access to a DuckDB database containing WACI (Weighted Average Carbon Intensity) "
        "portfolio data. Use the tools to explore portfolios, holdings, carbon scores, and WACI metrics. "
        "Start with `list_tables` to see what's available, then use `query` for any SQL question."
    ),
)


@mcp.resource("schema://tables")
def schema_overview() -> str:
    """Return a description of all tables and their columns in the database."""
    conn = _get_conn()
    try:
        tables = conn.execute("""
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema NOT LIKE 'sqlmesh%'
              AND table_schema NOT IN ('information_schema', 'sqlmesh')
            ORDER BY table_schema, table_name
        """).fetchall()

        parts = []
        for schema, table, ttype in tables:
            cols = conn.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{schema}' AND table_name = '{table}'
                ORDER BY ordinal_position
            """).fetchall()
            col_list = ", ".join(f"{c[0]} ({c[1]})" for c in cols)
            parts.append(f"- **{schema}.{table}** [{ttype}]: {col_list}")

        return "# Database Schema\n\n" + "\n".join(parts)
    finally:
        conn.close()


@mcp.tool()
def list_tables() -> str:
    """List all tables and views in the database with row counts.

    Returns a table showing schema, name, type, and row count for each
    table/view. Excludes internal SQLMesh metadata tables.
    """
    conn = _get_conn()
    try:
        tables = conn.execute("""
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema NOT LIKE 'sqlmesh%'
              AND table_schema NOT IN ('information_schema', 'sqlmesh')
            ORDER BY table_schema, table_name
        """).fetchall()

        rows = []
        for schema, table, ttype in tables:
            try:
                count = conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]
            except Exception:
                count = "?"
            rows.append((schema, table, ttype, count))

        header = "| Schema | Table | Type | Rows |"
        sep = "| --- | --- | --- | --- |"
        body = "\n".join(f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} |" for r in rows)
        return f"{header}\n{sep}\n{body}"
    finally:
        conn.close()


@mcp.tool()
def describe_table(table_name: str) -> str:
    """Show the columns, types, and sample values for a table.

    Args:
        table_name: Fully qualified name like 'main_marts.fct_portfolio_waci'
                    or just 'fct_portfolio_waci' (will search all schemas).
    """
    conn = _get_conn()
    try:
        if "." in table_name:
            schema, tbl = table_name.split(".", 1)
        else:
            # Find the table in any schema
            match = conn.execute(f"""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_name = '{table_name}'
                  AND table_schema NOT LIKE 'sqlmesh%'
                  AND table_schema NOT IN ('information_schema', 'sqlmesh')
                LIMIT 1
            """).fetchone()
            if not match:
                return f"Table '{table_name}' not found."
            schema, tbl = match

        cols = conn.execute(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{tbl}'
            ORDER BY ordinal_position
        """).fetchall()

        if not cols:
            return f"Table '{schema}.{tbl}' has no columns (or doesn't exist)."

        # Get a sample row
        try:
            sample = conn.execute(f'SELECT * FROM "{schema}"."{tbl}" LIMIT 1').fetchone()
        except Exception:
            sample = None

        header = "| Column | Type | Nullable | Sample |"
        sep = "| --- | --- | --- | --- |"
        body_rows = []
        for i, (col, dtype, nullable) in enumerate(cols):
            val = _fmt(sample[i]) if sample else ""
            body_rows.append(f"| {col} | {dtype} | {nullable} | {val} |")

        return f"**{schema}.{tbl}**\n\n{header}\n{sep}\n" + "\n".join(body_rows)
    finally:
        conn.close()


@mcp.tool()
def query(sql: str) -> str:
    """Run a read-only SQL query against the DuckDB database and return results.

    The database contains WACI portfolio carbon data with these key tables:
    - staging layer: stg_portfolio_positions, stg_market_data, stg_carbon_scores
    - intermediate layer: int_portfolio_exposure, int_portfolio_carbon
    - marts layer: fct_portfolio_waci

    Schema prefixes vary by pipeline:
    - dbt: main_staging, main_intermediate, main_marts
    - SQLMesh: staging, intermediate, marts

    Args:
        sql: A SELECT query. DML/DDL statements are not allowed (read-only mode).
    """
    try:
        return _run_query(sql)
    except duckdb.Error as e:
        return f"**Query error:** {e}"


@mcp.tool()
def portfolio_summary() -> str:
    """Get a summary of all portfolios: WACI score, total market value, and number of holdings."""
    conn = _get_conn()
    try:
        # Detect schema prefix (dbt vs sqlmesh)
        schemas = [r[0] for r in conn.execute(
            "SELECT DISTINCT table_schema FROM information_schema.tables"
        ).fetchall()]
        prefix = "main_marts" if "main_marts" in schemas else "marts"

        return _run_query(f"""
            SELECT
                portfolio_id,
                as_of_date,
                waci,
                total_market_value,
                num_holdings
            FROM "{prefix}".fct_portfolio_waci
            ORDER BY portfolio_id, as_of_date
        """)
    finally:
        conn.close()


@mcp.tool()
def holdings_breakdown(portfolio_id: str) -> str:
    """Show all holdings in a portfolio with their carbon impact.

    Returns each security's weight, market value, carbon intensity,
    and contribution to the portfolio's WACI score.

    Args:
        portfolio_id: Portfolio identifier, e.g. 'PF001'.
    """
    conn = _get_conn()
    try:
        schemas = [r[0] for r in conn.execute(
            "SELECT DISTINCT table_schema FROM information_schema.tables"
        ).fetchall()]
        int_prefix = "main_intermediate" if "main_intermediate" in schemas else "intermediate"
        mart_prefix = "main_marts" if "main_marts" in schemas else "marts"

        return _run_query(f"""
            SELECT
                c.security_id,
                c.company_name,
                c.market_value,
                ROUND(c.portfolio_weight * 100, 2) AS weight_pct,
                c.carbon_intensity,
                c.weighted_carbon_intensity,
                ROUND(c.weighted_carbon_intensity / w.waci * 100, 1) AS pct_of_waci
            FROM "{int_prefix}".int_portfolio_carbon c
            JOIN "{mart_prefix}".fct_portfolio_waci w
              USING (portfolio_id, as_of_date)
            WHERE c.portfolio_id = '{portfolio_id}'
            ORDER BY c.weighted_carbon_intensity DESC
        """)
    finally:
        conn.close()


@mcp.tool()
def top_carbon_contributors(limit: int = 10) -> str:
    """Rank securities by carbon intensity across all portfolios.

    Shows which companies contribute the most carbon risk.

    Args:
        limit: Number of results to return (default 10).
    """
    conn = _get_conn()
    try:
        schemas = [r[0] for r in conn.execute(
            "SELECT DISTINCT table_schema FROM information_schema.tables"
        ).fetchall()]
        prefix = "main_intermediate" if "main_intermediate" in schemas else "intermediate"

        return _run_query(f"""
            SELECT DISTINCT
                security_id,
                company_name,
                carbon_emissions_tonnes,
                revenue_usd,
                carbon_intensity
            FROM "{prefix}".int_portfolio_carbon
            ORDER BY carbon_intensity DESC
            LIMIT {int(limit)}
        """)
    finally:
        conn.close()


@mcp.tool()
def compare_portfolios() -> str:
    """Compare all portfolios side by side on WACI, market value, and carbon exposure."""
    conn = _get_conn()
    try:
        schemas = [r[0] for r in conn.execute(
            "SELECT DISTINCT table_schema FROM information_schema.tables"
        ).fetchall()]
        int_prefix = "main_intermediate" if "main_intermediate" in schemas else "intermediate"
        mart_prefix = "main_marts" if "main_marts" in schemas else "marts"

        return _run_query(f"""
            WITH carbon_stats AS (
                SELECT
                    portfolio_id,
                    as_of_date,
                    MAX(weighted_carbon_intensity) AS max_holding_wci,
                    MIN(weighted_carbon_intensity) AS min_holding_wci,
                    AVG(carbon_intensity) AS avg_carbon_intensity
                FROM "{int_prefix}".int_portfolio_carbon
                GROUP BY portfolio_id, as_of_date
            )
            SELECT
                w.portfolio_id,
                w.as_of_date,
                w.waci,
                w.total_market_value,
                w.num_holdings,
                s.avg_carbon_intensity,
                s.max_holding_wci,
                s.min_holding_wci
            FROM "{mart_prefix}".fct_portfolio_waci w
            JOIN carbon_stats s USING (portfolio_id, as_of_date)
            ORDER BY w.waci DESC
        """)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not DB_PATH.exists():
        print(f"Error: database not found at {DB_PATH}", file=sys.stderr)
        print("Run the pipeline first: cd packages/dbt && uv run dbt build", file=sys.stderr)
        sys.exit(1)
    mcp.run(transport="stdio")
