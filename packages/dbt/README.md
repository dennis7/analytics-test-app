# WACI dbt Project

A dbt project for computing **Weighted Average Carbon Intensity (WACI)** for investment portfolios, powered by DuckDB.

> Part of the [WACI Monorepo](../../README.md). See also the [SQLMesh implementation](../sqlmesh/).

## Quick Start

```bash
# From the repo root: install all workspace dependencies
uv sync

# Install dbt packages, then run the pipeline
cd packages/dbt
uv run dbt deps
uv run dbt build
```

## Project Structure

```
packages/dbt/
├── data/                      # Seed data (parquet) per environment
│   ├── generate_seed_data.py
│   ├── dev/
│   ├── sit/
│   ├── uat/
│   └── prd/
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
# All commands run from packages/dbt/

uv run dbt debug                  # Verify configuration and connection
uv run dbt build                  # Build everything (models + tests)
uv run dbt run                    # Run models only
uv run dbt test                   # Run tests only
uv run dbt build --target sit     # Target a specific environment
uv run dbt build --target prd
```

## Environments

| Target | DuckDB Path                | Threads |
|--------|----------------------------|---------|
| `dev`  | `output/dev/dev.duckdb`    | 4       |
| `sit`  | `output/sit/sit.duckdb`    | 4       |
| `uat`  | `output/uat/uat.duckdb`    | 4       |
| `prd`  | `output/prd/prd.duckdb`    | 8       |

Override the database path with `DBT_DUCKDB_PATH` env var.
