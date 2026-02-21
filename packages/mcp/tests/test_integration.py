"""Integration tests for MCP server tools and resources.

Uses an in-memory MCP session backed by a self-contained DuckDB fixture.
No dependency on dbt or sqlmesh output.
"""

import pytest

pytestmark = pytest.mark.anyio

# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


async def test_list_tools_returns_all_seven(mcp_session):
    result = await mcp_session.list_tools()
    names = {t.name for t in result.tools}
    assert names == {
        "list_tables",
        "describe_table",
        "query",
        "portfolio_summary",
        "holdings_breakdown",
        "top_carbon_contributors",
        "compare_portfolios",
    }


async def test_list_resources_includes_schema(mcp_session):
    result = await mcp_session.list_resources()
    uris = [str(r.uri) for r in result.resources]
    assert "schema://tables" in uris


# ---------------------------------------------------------------------------
# Resource: schema://tables
# ---------------------------------------------------------------------------


async def test_schema_resource_contains_expected_tables(mcp_session):
    result = await mcp_session.read_resource("schema://tables")
    text = result.contents[0].text
    assert "# Database Schema" in text
    assert "fct_portfolio_waci" in text
    assert "int_portfolio_carbon" in text
    assert "stg_portfolio_positions" in text


# ---------------------------------------------------------------------------
# Tool: list_tables
# ---------------------------------------------------------------------------


async def test_list_tables_markdown_format(mcp_session):
    result = await mcp_session.call_tool("list_tables", {})
    assert not result.isError
    text = result.content[0].text
    assert "| Schema | Table | Type | Rows |" in text
    assert "| --- | --- | --- | --- |" in text


async def test_list_tables_includes_all_tables(mcp_session):
    result = await mcp_session.call_tool("list_tables", {})
    text = result.content[0].text
    for table in [
        "fct_portfolio_waci",
        "int_portfolio_carbon",
        "int_portfolio_exposure",
        "stg_carbon_scores",
        "stg_market_data",
        "stg_portfolio_positions",
    ]:
        assert table in text


async def test_list_tables_excludes_internal(mcp_session):
    result = await mcp_session.call_tool("list_tables", {})
    text = result.content[0].text
    assert "information_schema" not in text


# ---------------------------------------------------------------------------
# Tool: describe_table
# ---------------------------------------------------------------------------


async def test_describe_table_qualified_name(mcp_session):
    result = await mcp_session.call_tool("describe_table", {"table_name": "marts.fct_portfolio_waci"})
    assert not result.isError
    text = result.content[0].text
    assert "**marts.fct_portfolio_waci**" in text
    for col in ["portfolio_id", "as_of_date", "waci", "total_market_value", "num_holdings"]:
        assert col in text


async def test_describe_table_unqualified_name(mcp_session):
    result = await mcp_session.call_tool("describe_table", {"table_name": "fct_portfolio_waci"})
    assert not result.isError
    assert "fct_portfolio_waci" in result.content[0].text


async def test_describe_table_not_found(mcp_session):
    result = await mcp_session.call_tool("describe_table", {"table_name": "nonexistent_table"})
    assert "not found" in result.content[0].text.lower()


# ---------------------------------------------------------------------------
# Tool: query
# ---------------------------------------------------------------------------


async def test_query_basic_select(mcp_session):
    result = await mcp_session.call_tool("query", {"sql": "SELECT COUNT(*) AS cnt FROM marts.fct_portfolio_waci"})
    assert not result.isError
    text = result.content[0].text
    assert "cnt" in text
    assert "2" in text


async def test_query_empty_result(mcp_session):
    result = await mcp_session.call_tool(
        "query", {"sql": "SELECT * FROM marts.fct_portfolio_waci WHERE portfolio_id = 'NONE'"}
    )
    assert "_No rows returned._" in result.content[0].text


async def test_query_invalid_sql(mcp_session):
    result = await mcp_session.call_tool("query", {"sql": "SELECT * FROM bogus_table_xyz"})
    assert "**Query error:**" in result.content[0].text


# ---------------------------------------------------------------------------
# Tool: portfolio_summary
# ---------------------------------------------------------------------------


async def test_portfolio_summary_both_portfolios(mcp_session):
    result = await mcp_session.call_tool("portfolio_summary", {})
    assert not result.isError
    text = result.content[0].text
    assert "PF001" in text
    assert "PF002" in text


async def test_portfolio_summary_columns(mcp_session):
    result = await mcp_session.call_tool("portfolio_summary", {})
    text = result.content[0].text
    for col in ["portfolio_id", "as_of_date", "waci", "total_market_value", "num_holdings"]:
        assert col in text


# ---------------------------------------------------------------------------
# Tool: holdings_breakdown
# ---------------------------------------------------------------------------


async def test_holdings_breakdown_pf001(mcp_session):
    result = await mcp_session.call_tool("holdings_breakdown", {"portfolio_id": "PF001"})
    assert not result.isError
    text = result.content[0].text
    assert "AlphaCorp" in text
    assert "BetaInc" in text


async def test_holdings_breakdown_columns(mcp_session):
    result = await mcp_session.call_tool("holdings_breakdown", {"portfolio_id": "PF001"})
    text = result.content[0].text
    for col in [
        "security_id",
        "company_name",
        "market_value",
        "weight_pct",
        "carbon_intensity",
        "weighted_carbon_intensity",
        "pct_of_waci",
    ]:
        assert col in text


async def test_holdings_breakdown_unknown_portfolio(mcp_session):
    result = await mcp_session.call_tool("holdings_breakdown", {"portfolio_id": "UNKNOWN"})
    assert "_No rows returned._" in result.content[0].text


# ---------------------------------------------------------------------------
# Tool: top_carbon_contributors
# ---------------------------------------------------------------------------


async def test_top_carbon_contributors_default(mcp_session):
    result = await mcp_session.call_tool("top_carbon_contributors", {})
    assert not result.isError
    text = result.content[0].text
    assert "BetaInc" in text  # highest carbon_intensity = 200


async def test_top_carbon_contributors_custom_limit(mcp_session):
    result = await mcp_session.call_tool("top_carbon_contributors", {"limit": 1})
    text = result.content[0].text
    lines = text.strip().splitlines()
    data_rows = [row for row in lines if row.startswith("|") and "---" not in row and "security_id" not in row]
    assert len(data_rows) == 1


async def test_top_carbon_contributors_ordered(mcp_session):
    result = await mcp_session.call_tool("top_carbon_contributors", {"limit": 1})
    text = result.content[0].text
    assert "BetaInc" in text  # carbon_intensity 200 > 100 > 50


# ---------------------------------------------------------------------------
# Tool: compare_portfolios
# ---------------------------------------------------------------------------


async def test_compare_portfolios_both(mcp_session):
    result = await mcp_session.call_tool("compare_portfolios", {})
    assert not result.isError
    text = result.content[0].text
    assert "PF001" in text
    assert "PF002" in text


async def test_compare_portfolios_columns(mcp_session):
    result = await mcp_session.call_tool("compare_portfolios", {})
    text = result.content[0].text
    for col in [
        "portfolio_id",
        "waci",
        "total_market_value",
        "num_holdings",
        "avg_carbon_intensity",
        "max_holding_wci",
        "min_holding_wci",
    ]:
        assert col in text
