# Analytics SQLMesh Project

A SQLMesh project for computing portfolio analytics, powered by DuckDB.

> Part of the [Analytics Monorepo](../../README.md). See also the [dbt implementation](../dbt/).

## Quick Start

```bash
# From the repo root
just sync
just seed
just sqlmesh plan
```

## Project Structure

```
packages/sqlmesh/
├── models/
│   ├── staging/               # 1:1 source mappings with type casting (views)
│   ├── intermediate/          # Business logic (exposure, carbon calcs) (tables)
│   └── marts/                 # Final output tables (fct_portfolio_waci) (tables)
├── audits/                    # Data quality audits per layer
│   ├── staging/
│   ├── intermediate/
│   └── marts/
├── config.py                  # SQLMesh project configuration (Python-based)
└── pyproject.toml             # Workspace member (sqlmesh-specific deps)
```

## Common Commands

```bash
# All commands run from the repo root via just

just sqlmesh plan                                     # Validate models and show changes
just sqlmesh plan --auto-apply                        # Apply all changes
just sqlmesh audit                                    # Run audits only
just sqlmesh dag                                      # View the DAG
just sqlmesh render staging.stg_portfolio_positions   # Render a model's SQL
```

## MCP Server

The monorepo includes an [MCP server](../mcp/server.py) for conversational exploration of the pipeline output. After running the SQLMesh pipeline:

```bash
# SQLMesh dev database
just mcp --db data/dev/output/sqlmesh-warehouse.duckdb
```

Available tools: `list_tables`, `describe_table`, `query`, `portfolio_summary`, `holdings_breakdown`, `top_carbon_contributors`, `compare_portfolios`. See the [root README](../../README.md#mcp-server) for full details.
