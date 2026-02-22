"""Integration tests for MCP server tools and resources.

Uses an in-memory MCP session backed by two self-contained DuckDB fixtures.
No dependency on dbt or sqlmesh output.
"""

import pytest

pytestmark = pytest.mark.anyio

# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


async def test_list_tools_returns_all_three(mcp_session):
    result = await mcp_session.list_tools()
    names = {t.name for t in result.tools}
    assert names == {"list_tables", "describe_table", "query"}


async def test_list_resources_includes_schema(mcp_session):
    result = await mcp_session.list_resources()
    uris = [str(r.uri) for r in result.resources]
    assert "schema://tables" in uris


# ---------------------------------------------------------------------------
# Resource: schema://tables
# ---------------------------------------------------------------------------


async def test_schema_resource_contains_tables_from_both_dbs(mcp_session):
    result = await mcp_session.read_resource("schema://tables")
    text = result.contents[0].text
    assert "# Database Schema" in text
    assert "users" in text
    assert "orders" in text


async def test_schema_resource_groups_by_database(mcp_session):
    result = await mcp_session.read_resource("schema://tables")
    text = result.contents[0].text
    assert "## alpha" in text
    assert "## beta" in text


# ---------------------------------------------------------------------------
# Tool: list_tables
# ---------------------------------------------------------------------------


async def test_list_tables_markdown_format(mcp_session):
    result = await mcp_session.call_tool("list_tables", {})
    assert not result.isError
    text = result.content[0].text
    assert "| Database | Schema | Table | Type | Rows |" in text
    assert "| --- | --- | --- | --- | --- |" in text


async def test_list_tables_shows_both_databases(mcp_session):
    result = await mcp_session.call_tool("list_tables", {})
    text = result.content[0].text
    assert "alpha" in text
    assert "beta" in text


async def test_list_tables_shows_all_tables(mcp_session):
    result = await mcp_session.call_tool("list_tables", {})
    text = result.content[0].text
    assert "users" in text
    assert "orders" in text


async def test_list_tables_shows_row_counts(mcp_session):
    result = await mcp_session.call_tool("list_tables", {})
    text = result.content[0].text
    # alpha.data.orders has 3 rows
    assert "3" in text


async def test_list_tables_excludes_internal(mcp_session):
    result = await mcp_session.call_tool("list_tables", {})
    text = result.content[0].text
    assert "information_schema" not in text
    assert "memory" not in text


# ---------------------------------------------------------------------------
# Tool: describe_table
# ---------------------------------------------------------------------------


async def test_describe_table_fully_qualified(mcp_session):
    result = await mcp_session.call_tool("describe_table", {"table_name": "alpha.data.users"})
    assert not result.isError
    text = result.content[0].text
    assert "[alpha]" in text
    assert "name" in text
    assert "email" in text


async def test_describe_table_schema_qualified(mcp_session):
    result = await mcp_session.call_tool("describe_table", {"table_name": "data.orders"})
    assert not result.isError
    text = result.content[0].text
    assert "amount" in text


async def test_describe_table_unqualified(mcp_session):
    result = await mcp_session.call_tool("describe_table", {"table_name": "orders"})
    assert not result.isError
    text = result.content[0].text
    assert "amount" in text
    assert "as_of_date" in text


async def test_describe_table_shows_sample_values(mcp_session):
    result = await mcp_session.call_tool("describe_table", {"table_name": "alpha.data.users"})
    text = result.content[0].text
    assert "Sample" in text
    assert "Alice" in text


async def test_describe_table_not_found(mcp_session):
    result = await mcp_session.call_tool("describe_table", {"table_name": "nonexistent_table"})
    assert "not found" in result.content[0].text.lower()


# ---------------------------------------------------------------------------
# Tool: query
# ---------------------------------------------------------------------------


async def test_query_basic_select(mcp_session):
    result = await mcp_session.call_tool(
        "query", {"sql": 'SELECT COUNT(*) AS cnt FROM "alpha".data.orders'}
    )
    assert not result.isError
    text = result.content[0].text
    assert "cnt" in text
    assert "3" in text


async def test_query_cross_db_join(mcp_session):
    result = await mcp_session.call_tool(
        "query",
        {
            "sql": """
                SELECT a.name, a.email AS alpha_email, b.email AS beta_email
                FROM "alpha".data.users a
                JOIN "beta".data.users b USING (name)
            """
        },
    )
    assert not result.isError
    text = result.content[0].text
    assert "Alice" in text
    assert "alpha_email" in text
    assert "beta_email" in text


async def test_query_empty_result(mcp_session):
    result = await mcp_session.call_tool(
        "query", {"sql": "SELECT * FROM \"alpha\".data.users WHERE name = 'Nobody'"}
    )
    assert "_No rows returned._" in result.content[0].text


async def test_query_invalid_sql(mcp_session):
    result = await mcp_session.call_tool("query", {"sql": "SELECT * FROM bogus_table_xyz"})
    assert "**Query error:**" in result.content[0].text


async def test_query_float_formatting(mcp_session):
    result = await mcp_session.call_tool(
        "query", {"sql": 'SELECT amount FROM "alpha".data.orders WHERE id = 1'}
    )
    text = result.content[0].text
    assert "100.5000" in text
