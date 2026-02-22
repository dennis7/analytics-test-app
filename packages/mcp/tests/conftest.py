import importlib
import sys
from pathlib import Path

import duckdb
import pytest
from mcp.shared.memory import create_connected_server_and_client_session

REPO_ROOT = Path(__file__).resolve().parents[3]
MCP_PKG = REPO_ROOT / "packages" / "mcp"


@pytest.fixture(scope="session")
def test_db(tmp_path_factory):
    """Create a self-contained DuckDB with the schema the MCP server expects."""
    db_path = tmp_path_factory.mktemp("mcp") / "test.duckdb"
    conn = duckdb.connect(str(db_path))

    conn.execute("CREATE SCHEMA staging")
    conn.execute("CREATE SCHEMA intermediate")
    conn.execute("CREATE SCHEMA marts")

    # Staging tables (minimal — just enough for list_tables / describe_table)
    conn.execute("""
        CREATE TABLE staging.stg_portfolio_positions (
            portfolio_id VARCHAR, security_id VARCHAR, quantity DOUBLE, as_of_date DATE
        )
    """)
    conn.execute("""
        INSERT INTO staging.stg_portfolio_positions VALUES
            ('PF001', 'SEC001', 100, '2025-01-01'),
            ('PF001', 'SEC002', 200, '2025-01-01'),
            ('PF002', 'SEC002', 150, '2025-01-01'),
            ('PF002', 'SEC003', 250, '2025-01-01')
    """)

    conn.execute("""
        CREATE TABLE staging.stg_market_data (
            security_id VARCHAR, price DOUBLE, currency VARCHAR, as_of_date DATE
        )
    """)
    conn.execute("""
        INSERT INTO staging.stg_market_data VALUES
            ('SEC001', 60.0, 'USD', '2025-01-01'),
            ('SEC002', 20.0, 'USD', '2025-01-01'),
            ('SEC003', 16.0, 'USD', '2025-01-01')
    """)

    conn.execute("""
        CREATE TABLE staging.stg_carbon_scores (
            security_id VARCHAR, company_name VARCHAR,
            carbon_emissions_tonnes DOUBLE, revenue_usd DOUBLE, as_of_date DATE
        )
    """)
    conn.execute("""
        INSERT INTO staging.stg_carbon_scores VALUES
            ('SEC001', 'AlphaCorp', 5000, 50000, '2025-01-01'),
            ('SEC002', 'BetaInc', 10000, 50000, '2025-01-01'),
            ('SEC003', 'GammaCo', 2500, 50000, '2025-01-01')
    """)

    # Intermediate tables
    conn.execute("""
        CREATE TABLE intermediate.int_portfolio_exposure (
            portfolio_id VARCHAR, as_of_date DATE, security_id VARCHAR,
            market_value DOUBLE, portfolio_weight DOUBLE
        )
    """)
    conn.execute("""
        INSERT INTO intermediate.int_portfolio_exposure VALUES
            ('PF001', '2025-01-01', 'SEC001', 6000, 0.6),
            ('PF001', '2025-01-01', 'SEC002', 4000, 0.4),
            ('PF002', '2025-01-01', 'SEC002', 4000, 0.5),
            ('PF002', '2025-01-01', 'SEC003', 4000, 0.5)
    """)

    conn.execute("""
        CREATE TABLE intermediate.int_portfolio_carbon (
            portfolio_id VARCHAR, as_of_date DATE, security_id VARCHAR,
            company_name VARCHAR, market_value DOUBLE, portfolio_weight DOUBLE,
            carbon_intensity DOUBLE, weighted_carbon_intensity DOUBLE,
            carbon_emissions_tonnes DOUBLE, revenue_usd DOUBLE
        )
    """)
    conn.execute("""
        INSERT INTO intermediate.int_portfolio_carbon VALUES
            ('PF001', '2025-01-01', 'SEC001', 'AlphaCorp', 6000, 0.6, 100, 60, 5000, 50000),
            ('PF001', '2025-01-01', 'SEC002', 'BetaInc', 4000, 0.4, 200, 80, 10000, 50000),
            ('PF002', '2025-01-01', 'SEC002', 'BetaInc', 4000, 0.5, 200, 100, 10000, 50000),
            ('PF002', '2025-01-01', 'SEC003', 'GammaCo', 4000, 0.5, 50, 25, 2500, 50000)
    """)

    # Marts table
    conn.execute("""
        CREATE TABLE marts.fct_portfolio_waci (
            portfolio_id VARCHAR, as_of_date DATE, waci DOUBLE,
            total_market_value DOUBLE, num_holdings BIGINT
        )
    """)
    conn.execute("""
        INSERT INTO marts.fct_portfolio_waci VALUES
            ('PF001', '2025-01-01', 140, 10000, 2),
            ('PF002', '2025-01-01', 125, 8000, 2)
    """)

    conn.close()
    return db_path


def _load_server(db_path: Path):
    """Import (or reload) server.py with sys.argv pointing at db_path."""
    sys.argv = ["analytics-mcp", "--db", str(db_path)]
    if str(MCP_PKG) not in sys.path:
        sys.path.insert(0, str(MCP_PKG))
    if "server" in sys.modules:
        return importlib.reload(sys.modules["server"])
    import server  # noqa: F401

    return server


@pytest.fixture(scope="module")
async def mcp_session(test_db):
    """Module-scoped MCP ClientSession connected to the test database."""
    server_module = _load_server(test_db)
    async with create_connected_server_and_client_session(server_module.mcp) as session:
        yield session
