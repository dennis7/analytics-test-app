# Analytics dbt Project

A dbt project for computing portfolio analytics, powered by DuckDB.

> Part of the [Analytics Monorepo](../../README.md). See also the [SQLMesh implementation](../sqlmesh/).

## Quick Start

```bash
# From the repo root
uv sync
just dbt deps
just seed
just dbt build
```

## Project Structure

```
packages/dbt/
├── models/
│   ├── sources.yml            # Source definitions (upstream parquet files)
│   ├── staging/               # 1:1 source mappings with type casting
│   ├── intermediate/          # Business logic (exposure, carbon calcs)
│   └── marts/                 # Final output tables (fct_portfolio_waci)
├── tests/                     # Custom data tests per layer
├── macros/                    # Reusable SQL macros
├── dbt_project.yml            # dbt project configuration
├── profiles.yml               # Connection profiles (DuckDB)
├── packages.yml               # dbt package dependencies
└── pyproject.toml             # Workspace member (dbt-specific deps)
```

## Common Commands

```bash
# All commands run from the repo root via just

just dbt debug                  # Verify configuration and connection
just dbt build                  # Build everything (models + tests)
just dbt run                    # Run models only
just dbt test                   # Run tests only
just dbt build --target sit     # Target a specific environment
just dbt build --target prd
```

## Environments

| Target | DuckDB Path                                  | Threads |
|--------|----------------------------------------------|---------|
| `dev`  | `data/dev/output/dbt-warehouse.duckdb`       | 4       |
| `sit`  | `data/sit/output/dbt-warehouse.duckdb`       | 4       |
| `uat`  | `data/uat/output/dbt-warehouse.duckdb`       | 4       |
| `prd`  | `data/prd/output/dbt-warehouse.duckdb`       | 8       |

All paths are resolved via the `DATA_DIR` environment variable, set automatically by the justfile.

## MCP Server

The monorepo includes an [MCP server](../mcp/server.py) for conversational exploration of the pipeline output. After running the dbt pipeline:

```bash
# dbt dev database (default)
just mcp

# Specific environment
just mcp --db data/prd/output/dbt-warehouse.duckdb
```

Available tools: `list_tables`, `describe_table`, `query`, `portfolio_summary`, `holdings_breakdown`, `top_carbon_contributors`, `compare_portfolios`. See the [root README](../../README.md#mcp-server) for full details.
