# WACI dbt Project

A dbt project for computing **Weighted Average Carbon Intensity (WACI)** for investment portfolios, powered by DuckDB.

## Prerequisites

- [Python 3.12+](https://www.python.org/downloads/)
- [uv](https://docs.astral.sh/uv/getting-started/installation/)

## Quick Start

```bash
# Install Python dependencies
uv sync

# Install dbt packages
uv run dbt deps

# Run the full pipeline (models + tests)
uv run dbt build
```

## Project Structure

```
.
├── data/                      # Seed data (parquet) per environment
│   ├── generate_seed_data.py  # Script to regenerate seed parquet files
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
├── pyproject.toml             # Python project & tool configuration
└── packages.yml               # dbt package dependencies
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
Intermediate (views)
  ├── int_portfolio_exposure   # market value per holding
  └── int_portfolio_carbon     # weighted carbon intensity per holding
        ↓
Marts (tables)
  └── fct_portfolio_waci       # WACI per portfolio
```

## Common Commands

```bash
# Verify configuration and connection
uv run dbt debug

# Build everything (run models + run tests)
uv run dbt build

# Run models only
uv run dbt run

# Run tests only
uv run dbt test

# Target a specific environment (dev is default)
uv run dbt build --target sit
uv run dbt build --target uat
uv run dbt build --target prd
```

## Seed Data

To regenerate the parquet seed files:

```bash
uv run python data/generate_seed_data.py
```

## Environments

| Target | DuckDB Path                | Threads |
|--------|----------------------------|---------|
| `dev`  | `output/dev/dev.duckdb`    | 4       |
| `sit`  | `output/sit/sit.duckdb`    | 4       |
| `uat`  | `output/uat/uat.duckdb`    | 4       |
| `prd`  | `output/prd/prd.duckdb`    | 8       |

Override the database path with `DBT_DUCKDB_PATH` env var.

## Linting & Formatting

This project uses [ruff](https://docs.astral.sh/ruff/) for Python linting and formatting.

```bash
# Check for lint issues
uv run ruff check .

# Auto-fix lint issues
uv run ruff check --fix .

# Check formatting
uv run ruff format --check .

# Apply formatting
uv run ruff format .
```

## Key Dependencies

| Package      | Version |
|--------------|---------|
| Python       | >= 3.12 |
| dbt-duckdb   | >= 1.10 |
| pyarrow      | >= 19.0 |
| ruff (dev)   | >= 0.11 |
