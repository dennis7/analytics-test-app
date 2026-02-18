# WACI SQLMesh Project

A SQLMesh project for computing **Weighted Average Carbon Intensity (WACI)** for investment portfolios, powered by DuckDB.

> Part of the [WACI Monorepo](../../README.md). See also the [dbt implementation](../dbt/).

## Quick Start

```bash
# From the repo root: install all workspace dependencies
uv sync

# Run the pipeline
cd packages/sqlmesh
uv run sqlmesh plan --auto-apply
```

## Project Structure

```
packages/sqlmesh/
├── data/                      # Seed data (parquet) per environment
│   ├── generate_seed_data.py
│   ├── dev/
│   ├── sit/
│   ├── uat/
│   └── prd/
├── models/
│   ├── staging/               # 1:1 source mappings with type casting (views)
│   ├── intermediate/          # Business logic (exposure, carbon calcs) (tables)
│   └── marts/                 # Final output tables (fct_portfolio_waci) (tables)
├── audits/                    # Data quality audits per layer
│   ├── staging/
│   ├── intermediate/
│   └── marts/
├── config.yaml                # SQLMesh project configuration
└── pyproject.toml             # Workspace member (sqlmesh-specific deps)
```

## Common Commands

```bash
# All commands run from packages/sqlmesh/

uv run sqlmesh plan                                     # Validate models and show changes
uv run sqlmesh plan --auto-apply                        # Apply all changes
uv run sqlmesh audit                                    # Run audits only
uv run sqlmesh dag                                      # View the DAG
uv run sqlmesh render staging.stg_portfolio_positions   # Render a model's SQL
```
