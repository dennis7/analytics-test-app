# Analytics Data Pipeline Monorepo

A monorepo containing **two parallel implementations** of an analytics data pipeline -- one in [dbt](https://www.getdbt.com/) and one in [SQLMesh](https://sqlmesh.com/). Both produce identical outputs from the same source data, making this an ideal environment for comparing the two frameworks side by side.

## Repository Structure

```
.
├── packages/
│   ├── dbt/                       # dbt implementation
│   │   ├── models/                #   staging / intermediate / marts
│   │   ├── tests/                 #   data quality tests
│   │   ├── macros/                #   reusable SQL macros
│   │   ├── dbt_project.yml
│   │   ├── profiles.yml
│   │   └── packages.yml
│   │
│   ├── sqlmesh/                   # SQLMesh implementation
│   │   ├── models/                #   staging / intermediate / marts
│   │   ├── audits/                #   data quality audits
│   │   └── config.py
│   │
│   └── mcp/                       # MCP server for conversational data access
│       ├── server.py              #   FastMCP server (stdio transport)
│       └── tests/                 #   integration tests
│
├── scripts/
│   └── generate_seed_data.py      # Seed data generator (all environments)
│
├── data/                          # Generated data (gitignored)
│   └── {env}/
│       ├── input/                 #   parquet seed files
│       └── output/                #   DuckDB warehouse files
│
├── justfile                       # Task runner (just seed, just dbt build, etc.)
├── pyproject.toml                 # Workspace root (shared deps, single venv)
├── uv.lock                        # Single lockfile for all packages
├── SQLMESH_VS_DBT.md              # Detailed comparison: SQLMesh vs dbt
└── README.md                      # This file
```

## Prerequisites

- [Python 3.12+](https://www.python.org/downloads/)
- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- [just](https://just.systems/man/en/installation.html)

## Quick Start

```bash
# Install all dependencies (one shared venv for the entire workspace)
uv sync

# Install dbt packages (dbt_utils)
just dbt deps

# Generate seed data for all environments
just seed

# Run dbt pipeline
just dbt build

# Run SQLMesh pipeline
just sqlmesh plan

# Run MCP integration tests
just test-mcp
```

## How the Monorepo Works

This project uses a **[uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/)** -- a single Python environment that serves both the dbt and SQLMesh packages.

### One venv, one lockfile

The root `pyproject.toml` defines a `[tool.uv.workspace]` with `members = ["packages/*"]`. Running `uv sync` from the repo root:

1. Resolves **all** dependencies (dbt + SQLMesh + shared) into a single `uv.lock`
2. Creates **one** `.venv` at the repo root
3. Installs workspace members as editable packages

This means:
- `just dbt build` and `just sqlmesh plan` both work from the same venv
- No duplicate installs of shared dependencies like `pyarrow`
- Dependency versions are guaranteed consistent across both packages

### Workspace members

Each package under `packages/` has its own `pyproject.toml` declaring only its specific dependencies:

| Package | Key dependency |
|---------|---------------|
| `packages/dbt/` | `dbt-duckdb>=1.10` |
| `packages/sqlmesh/` | `sqlmesh[duckdb]>=0.142` |
| `packages/mcp/` | `mcp>=1.0`, `duckdb>=1.0` |

The root `pyproject.toml` aggregates everything and adds shared dev dependencies (ruff, pytest). Shared tool config (ruff rules, Python version) lives at the root.

### Task runner

All commands are run from the repo root via [just](https://just.systems/):

```bash
just seed              # Generate seed data
just lint              # Run ruff linter and formatter
just dbt build         # Run dbt pipeline
just dbt test          # Run dbt tests only
just sqlmesh plan      # Run SQLMesh pipeline
just mcp               # Start MCP server (dbt dev output)
just test-mcp          # Run MCP integration tests
```

## The Pipeline

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

```bash
# dbt
just dbt build --target sit

# SQLMesh (default: dev, configured in config.py)
just sqlmesh plan
```

## Data Quality

Both implementations include 11 data quality checks covering:

- **Null checks** -- no null values in critical columns
- **Uniqueness** -- no duplicate security IDs per date
- **Valid values** -- currency codes must be in (USD, EUR, GBP, JPY)
- **Bounds** -- WACI must be non-negative

**dbt** implements these as singular SQL tests in `tests/`.
**SQLMesh** implements these as audits in `audits/`, attached directly to model definitions.

## MCP Server

The `packages/mcp/` package provides an [MCP](https://modelcontextprotocol.io/) server for conversational exploration of the pipeline data. It connects to the DuckDB output and exposes tools an LLM can call to query the data.

### Available tools

| Tool | Description |
|------|-------------|
| `list_tables` | Show all tables with row counts |
| `describe_table` | Column names, types, and sample values |
| `query` | Run arbitrary read-only SQL |
| `portfolio_summary` | WACI, market value, holdings per portfolio |
| `holdings_breakdown` | Per-security carbon impact for a portfolio |
| `top_carbon_contributors` | Securities ranked by carbon intensity |
| `compare_portfolios` | Side-by-side portfolio comparison |

### Usage with Claude Desktop

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "analytics": {
      "command": "uv",
      "args": ["run", "analytics-mcp", "--db", "data/dev/output/dbt-warehouse.duckdb"],
      "cwd": "/path/to/this/repo"
    }
  }
}
```

### Flags

```bash
# Default: dbt dev database
just mcp

# SQLMesh database
just mcp --db data/dev/output/sqlmesh-warehouse.duckdb

# Explicit path
just mcp --db /path/to/any.duckdb
```

## Regenerating Seed Data

Seed data lives in `scripts/generate_seed_data.py` and writes to `data/{env}/input/`:

```bash
just seed
```

## Linting

Ruff config lives in the root `pyproject.toml` and applies to all packages:

```bash
just lint
```

## Further Reading

- [SQLMesh vs dbt: Detailed Comparison](SQLMESH_VS_DBT.md) -- pros, cons, tradeoffs, integrations with Databricks, Spark, DuckDB, Delta Lake, and Iceberg
- [dbt Package README](packages/dbt/README.md) -- dbt-specific commands and configuration
- [SQLMesh Package README](packages/sqlmesh/README.md) -- SQLMesh-specific commands and configuration
