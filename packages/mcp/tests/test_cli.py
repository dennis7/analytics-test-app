"""Smoke test for the analytics-mcp CLI entry point via stdio transport."""

from pathlib import Path

import pytest
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

pytestmark = pytest.mark.anyio

REPO_ROOT = Path(__file__).resolve().parents[3]


async def test_cli_entry_point_connects_and_lists_tools(test_db):
    """Spawn analytics-mcp as a subprocess and verify it responds over stdio."""
    params = StdioServerParameters(
        command="uv",
        args=["run", "analytics-mcp", "--db", str(test_db)],
        cwd=str(REPO_ROOT),
    )
    async with stdio_client(params) as streams, ClientSession(*streams) as session:
        await session.initialize()
        result = await session.list_tools()
        names = {t.name for t in result.tools}
        assert len(names) == 7
        assert "query" in names
