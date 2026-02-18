# WACI SQLMesh Project

A SQLMesh project for computing **Weighted Average Carbon Intensity (WACI)** for investment portfolios, powered by DuckDB.

> Part of the [WACI Monorepo](../../README.md). See also the [dbt implementation](../dbt-waci/).

## Prerequisites

- [Python 3.12+](https://www.python.org/downloads/)
- [uv](https://docs.astral.sh/uv/getting-started/installation/)

## Quick Start

```bash
# From this directory (packages/sqlmesh-waci/)
uv sync

# Run the full pipeline (plan + apply)
uv run sqlmesh plan --auto-apply
```

## Project Structure

```
packages/sqlmesh-waci/
├── data/                      # Seed data (parquet) per environment
│   ├── generate_seed_data.py  # Script to regenerate seed parquet files
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
└── pyproject.toml             # Python project & tool configuration
```

## Data Flow

```
Sources (parquet)
  ├── portfolio_positions
  ├── market_data
  └── carbon_scores
        ↓
Staging (views)
  ├── stg_portfolio_positions
  ├── stg_market_data
  └── stg_carbon_scores
        ↓
Intermediate (tables)
  ├── int_portfolio_exposure   # market value per holding
  └── int_portfolio_carbon     # weighted carbon intensity per holding
        ↓
Marts (tables)
  └── fct_portfolio_waci       # WACI per portfolio
```

## Common Commands

```bash
# Validate models and configuration
uv run sqlmesh plan

# Apply all changes (plan + run)
uv run sqlmesh plan --auto-apply

# Run audits only
uv run sqlmesh audit

# View the DAG
uv run sqlmesh dag

# Render a model's SQL
uv run sqlmesh render staging.stg_portfolio_positions
```

## Seed Data

To regenerate the parquet seed files:

```bash
uv run python data/generate_seed_data.py
```

## Key Dependencies

| Package          | Version  |
|------------------|----------|
| Python           | >= 3.12  |
| sqlmesh[duckdb]  | >= 0.142 |
| pyarrow          | >= 19.0  |
| ruff (dev)       | >= 0.11  |
