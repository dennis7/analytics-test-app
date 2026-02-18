# WACI Data Pipeline Monorepo

A monorepo containing **two parallel implementations** of a Weighted Average Carbon Intensity (WACI) data pipeline -- one in [dbt](https://www.getdbt.com/) and one in [SQLMesh](https://sqlmesh.com/). Both produce identical outputs from the same source data, making this an ideal environment for comparing the two frameworks side by side.

## Repository Structure

```
.
├── packages/
│   ├── dbt-waci/              # dbt implementation
│   │   ├── models/            #   staging / intermediate / marts
│   │   ├── tests/             #   data quality tests
│   │   ├── macros/            #   reusable SQL macros
│   │   ├── data/              #   seed parquet files (dev/sit/uat/prd)
│   │   ├── dbt_project.yml
│   │   ├── profiles.yml
│   │   ├── packages.yml
│   │   └── pyproject.toml
│   │
│   └── sqlmesh-waci/          # SQLMesh implementation
│       ├── models/            #   staging / intermediate / marts
│       ├── audits/            #   data quality audits
│       ├── data/              #   seed parquet files (dev/sit/uat/prd)
│       ├── config.yaml
│       └── pyproject.toml
│
├── SQLMESH_VS_DBT.md          # Detailed comparison: SQLMesh vs dbt
└── README.md                  # This file
```

## Prerequisites

- [Python 3.12+](https://www.python.org/downloads/)
- [uv](https://docs.astral.sh/uv/getting-started/installation/) (fast Python package manager)

## Quick Start

Each package is self-contained with its own `pyproject.toml` and virtual environment. You run them independently from their respective directories.

### Running the dbt pipeline

```bash
cd packages/dbt-waci

# Install dependencies
uv sync

# Install dbt packages (dbt_utils)
uv run dbt deps

# Run the full pipeline (models + tests)
uv run dbt build
```

### Running the SQLMesh pipeline

```bash
cd packages/sqlmesh-waci

# Install dependencies
uv sync

# Run the full pipeline (plan + apply)
uv run sqlmesh plan --auto-apply
```

### Running both pipelines

```bash
# From the repo root -- run both back to back
(cd packages/dbt-waci && uv sync && uv run dbt deps && uv run dbt build) && \
(cd packages/sqlmesh-waci && uv sync && uv run sqlmesh plan --auto-apply)
```

## How the Monorepo Works

### Independent packages, shared data schema

Each package under `packages/` is a **fully independent project** with its own:

- `pyproject.toml` -- Python dependencies and tool configuration
- `.venv` -- isolated virtual environment (created by `uv sync`)
- Configuration files -- `dbt_project.yml` + `profiles.yml` for dbt, `config.yaml` for SQLMesh
- Seed data -- identical parquet files in `data/{dev,sit,uat,prd}/`

There is **no shared Python dependency resolution** between packages. This means:
- You can upgrade dbt without affecting SQLMesh and vice versa
- Each package can pin its own versions independently
- Virtual environments are isolated -- no cross-contamination

### Why a monorepo?

1. **Apples-to-apples comparison** -- same data, same pipeline logic, different frameworks
2. **Shared documentation** -- the [comparison guide](SQLMESH_VS_DBT.md) lives at the root, referencing both implementations
3. **Single git history** -- changes to the pipeline logic can be committed atomically across both implementations
4. **Easy onboarding** -- clone once, explore both frameworks

### Package layout convention

```
packages/
  <name>/
    pyproject.toml     # uv/pip project definition
    data/              # seed data (parquet files per environment)
    models/            # SQL transformation models
    ...                # framework-specific files (tests, audits, macros, configs)
```

## The WACI Pipeline

Both implementations compute the same thing:

```
Sources (parquet files)
  ├── portfolio_positions    # holdings per portfolio
  ├── market_data            # security prices
  └── carbon_scores          # emissions data per company
        ↓
Staging (type casting, cleaning)
  ├── stg_portfolio_positions
  ├── stg_market_data
  └── stg_carbon_scores
        ↓
Intermediate (business logic)
  ├── int_portfolio_exposure   # market_value = quantity * price
  │                            # portfolio_weight = market_value / total
  └── int_portfolio_carbon     # weighted_carbon_intensity = weight * intensity
        ↓
Marts (final output)
  └── fct_portfolio_waci       # WACI = SUM(weighted_carbon_intensity)
                               # per portfolio, per date
```

**WACI** (Weighted Average Carbon Intensity) is a standard ESG metric defined by TCFD. It measures the carbon intensity of an investment portfolio, weighted by each holding's share of the total portfolio value.

## Environments

Both implementations support four environments with progressively larger datasets:

| Environment | Portfolios | Securities | Use Case |
|-------------|-----------|------------|----------|
| `dev`       | 2         | 7          | Local development |
| `sit`       | 2         | 7          | System integration testing |
| `uat`       | 3         | 9          | User acceptance testing |
| `prd`       | 4         | 11         | Production |

### Targeting environments

**dbt:**
```bash
uv run dbt build --target sit
uv run dbt build --target uat
uv run dbt build --target prd
```

**SQLMesh:**
```bash
# SQLMesh uses gateway config for different environments
# Default: dev (configured in config.yaml)
uv run sqlmesh plan --auto-apply
```

## Data Quality

Both implementations include 11 data quality checks covering:

- **Null checks** -- no null values in critical columns
- **Uniqueness** -- no duplicate security IDs per date
- **Valid values** -- currency codes must be in (USD, EUR, GBP, JPY)
- **Bounds** -- WACI must be non-negative

**dbt** implements these as singular SQL tests in `tests/`.
**SQLMesh** implements these as audits in `audits/`, attached directly to model definitions.

## Regenerating Seed Data

Both packages include a `data/generate_seed_data.py` script:

```bash
# dbt seed data
cd packages/dbt-waci && uv run python data/generate_seed_data.py

# SQLMesh seed data
cd packages/sqlmesh-waci && uv run python data/generate_seed_data.py
```

## Linting

Both packages use [ruff](https://docs.astral.sh/ruff/) for Python linting:

```bash
# From either package directory
uv run ruff check .
uv run ruff format --check .
```

## Further Reading

- [SQLMesh vs dbt: Detailed Comparison](SQLMESH_VS_DBT.md) -- pros, cons, tradeoffs, integrations with Databricks, Spark, DuckDB, Delta Lake, and Iceberg
- [dbt Package README](packages/dbt-waci/README.md) -- dbt-specific commands and configuration
- [SQLMesh Package README](packages/sqlmesh-waci/README.md) -- SQLMesh-specific commands and configuration
