import importlib
import sys
from pathlib import Path

import duckdb
import pytest
from mcp.shared.memory import create_connected_server_and_client_session

REPO_ROOT = Path(__file__).resolve().parents[3]
MCP_PKG = REPO_ROOT / "packages" / "mcp"


@pytest.fixture(scope="session")
def test_dbs(tmp_path_factory):
    """Create two self-contained DuckDB databases for testing multi-DB mode."""
    db_dir = tmp_path_factory.mktemp("mcp")

    # DB 1: alpha
    db1_path = db_dir / "alpha.duckdb"
    conn1 = duckdb.connect(str(db1_path))
    conn1.execute("CREATE SCHEMA data")
    conn1.execute("""
        CREATE TABLE data.users (
            id INTEGER, name VARCHAR, email VARCHAR
        )
    """)
    conn1.execute("""
        INSERT INTO data.users VALUES
            (1, 'Alice', 'alice@alpha.com'),
            (2, 'Bob', 'bob@alpha.com')
    """)
    conn1.execute("""
        CREATE TABLE data.orders (
            id INTEGER, user_id INTEGER, amount DOUBLE, as_of_date DATE
        )
    """)
    conn1.execute("""
        INSERT INTO data.orders VALUES
            (1, 1, 100.50, '2025-01-01'),
            (2, 2, 200.75, '2025-01-01'),
            (3, 1, 50.25, '2025-02-01')
    """)
    conn1.close()

    # DB 2: beta (overlapping table names for comparison testing)
    db2_path = db_dir / "beta.duckdb"
    conn2 = duckdb.connect(str(db2_path))
    conn2.execute("CREATE SCHEMA data")
    conn2.execute("""
        CREATE TABLE data.users (
            id INTEGER, name VARCHAR, email VARCHAR, active BOOLEAN
        )
    """)
    conn2.execute("""
        INSERT INTO data.users VALUES
            (1, 'Alice', 'alice@beta.com', true),
            (3, 'Charlie', 'charlie@beta.com', false)
    """)
    conn2.close()

    return db1_path, db2_path


@pytest.fixture(scope="session")
def test_db(test_dbs):
    """Single DB for tests that only need one database."""
    return test_dbs[0]


def _load_server(db_paths: list[Path]):
    """Import (or reload) server.py with sys.argv pointing at db paths."""
    sys.argv = ["analytics-mcp"] + [f"--db={p}" for p in db_paths]
    if str(MCP_PKG) not in sys.path:
        sys.path.insert(0, str(MCP_PKG))
    if "server" in sys.modules:
        return importlib.reload(sys.modules["server"])
    import server  # noqa: F401

    return server


@pytest.fixture(scope="module")
async def mcp_session(test_dbs):
    """Module-scoped MCP ClientSession connected to both test databases."""
    server_module = _load_server(list(test_dbs))
    async with create_connected_server_and_client_session(server_module.mcp) as session:
        yield session
