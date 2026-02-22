# Analytics Data Pipeline Monorepo

A monorepo containing **two parallel implementations** of an ESG analytics pipeline -- one in [dbt](https://www.getdbt.com/) and one in [SQLMesh](https://sqlmesh.com/). Both produce identical outputs from the same source data, making this an ideal environment for comparing the two frameworks side by side.

The pipeline computes two standard ESG metrics:
- **WACI** (Weighted Average Carbon Intensity) -- carbon intensity of an investment portfolio, weighted by each holding's share of total portfolio value
- **WAGR** (Weighted Average Green Revenue) -- green revenue share of an investment portfolio, weighted by each holding's share of total portfolio value

## Repository Structure

```
.
├── packages/
│   ├── dbt/                       # dbt implementation
│   │   ├── models/
│   │   │   ├── staging/           #   10 staging models (type casting, cleaning)
│   │   │   ├── waci/              #   7 WACI models (preprocess → harmonize → waterfall → metrics)
│   │   │   └── wagr/              #   7 WAGR models (same structure as WACI)
│   │   ├── tests/                 #   32 data quality tests
│   │   ├── macros/                #   read_parquet, generate_schema_name
│   │   ├── dbt_project.yml
│   │   ├── profiles.yml
│   │   └── packages.yml
│   │
│   ├── sqlmesh/                   # SQLMesh implementation
│   │   ├── models/
│   │   │   ├── staging/           #   10 staging models
│   │   │   ├── waci/              #   7 WACI models
│   │   │   └── wagr/              #   7 WAGR models
│   │   ├── audits/                #   35 data quality audits
│   │   ├── macros/                #   shared SQL logic via Python @macro()
│   │   └── config.py
│   │
│   └── mcp/                       # MCP server for DuckDB exploration
│       ├── server.py              #   FastMCP server (stdio transport)
│       └── tests/                 #   integration tests
│
├── scripts/
│   └── generate_seed_data.py      # Seed data generator (dev + test environments)
│
├── data/                          # Generated data (gitignored)
│   └── {env}/
│       ├── input/                 #   10 parquet seed files
│       └── output/                #   DuckDB warehouse files
│
├── justfile                       # Task runner
├── pyproject.toml                 # Workspace root (shared deps, single venv)
├── uv.lock                        # Single lockfile for all packages
├── SQLMESH_VS_DBT.md              # Detailed comparison: SQLMesh vs dbt
└── README.md
```

## Prerequisites

- [Python 3.12+](https://www.python.org/downloads/)
- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- [just](https://just.systems/man/en/installation.html)

## Quick Start

```bash
# Install all dependencies (Python + dbt packages)
just sync

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

This project uses a **[uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/)** -- a single Python environment that serves all three packages.

### One venv, one lockfile

The root `pyproject.toml` defines a `[tool.uv.workspace]` with `members = ["packages/*"]`. Running `uv sync` from the repo root:

1. Resolves **all** dependencies (dbt + SQLMesh + MCP + shared) into a single `uv.lock`
2. Creates **one** `.venv` at the repo root
3. Installs workspace members as editable packages

### Workspace members

| Package | Key dependency |
|---------|---------------|
| `packages/dbt/` | `dbt-duckdb>=1.10` |
| `packages/sqlmesh/` | `sqlmesh[duckdb]>=0.142` |
| `packages/mcp/` | `mcp>=1.0`, `duckdb>=1.0` |

### Task runner

All commands are run from the repo root via [just](https://just.systems/):

```bash
just sync              # Install Python + dbt dependencies
just seed              # Generate seed data
just lint              # Run ruff linter and formatter
just dbt build         # Run dbt pipeline
just dbt test          # Run dbt tests only
just sqlmesh plan      # Run SQLMesh pipeline
just sqlmesh audit     # Run SQLMesh audits only
just mcp               # Start MCP server (both DuckDB databases)
just test-mcp          # Run MCP integration tests
```

## The Pipeline

Both implementations follow the same per-metric schema design with four layers:

```
Sources (10 parquet files)
  ├── security_master         # Security reference data with entity mapping
  ├── entity_hierarchy        # Parent-child entity relationships
  ├── public_positions        # Public market holdings per portfolio
  ├── private_positions       # Private fund holdings per portfolio
  ├── public_market_data      # Security prices
  ├── private_valuations      # Fund NAVs
  ├── entity_carbon           # Entity-level carbon emissions
  ├── sector_carbon           # Sector average carbon intensities
  ├── entity_green_revenue    # Entity-level green revenue
  └── sector_green_revenue    # Sector average green revenue shares
        ↓
Staging (type casting, cleaning, optional date filtering)
  └── 10 stg_* views
        ↓
Per-Metric Pipeline (WACI and WAGR each follow this pattern):
  ├── Preprocess              # Join positions with market data + security master
  │   ├── {metric}_prep_public_securities
  │   └── {metric}_prep_private_securities
  ├── Harmonize               # Compute market values and portfolio weights
  │   ├── {metric}_harm_valued_positions
  │   └── {metric}_harm_portfolio_weights
  ├── Waterfall               # Apply metric-specific calculations
  │   ├── {metric}_wf_*       # (carbon intensity / green revenue lookup)
  │   └── {metric}_wf_*       # (weighted metric per position)
  └── Metrics                 # Final aggregation
      └── {metric}_met_portfolio_{metric}   # Per portfolio, per date
```

## Environments

Both implementations support two environments:

| Environment | Portfolios | Securities | Dates | Use Case |
|-------------|-----------|------------|-------|----------|
| `dev`       | 2         | 7 public + 3 private | 2 month-ends | Local development |
| `test`      | 2         | 7 public + 3 private | 2 month-ends | Automated testing |

### Single-date filtering

Both pipelines read the `REPORTING_DATE` environment variable. When unset, all dates are processed.

```bash
REPORTING_DATE=2025-12-31 just dbt build
REPORTING_DATE=2025-12-31 just sqlmesh plan
```

## Data Quality

Both implementations include comprehensive data quality checks:

| Category | Checks |
|----------|--------|
| **Null checks** | All staging models validated for null values in critical columns |
| **Uniqueness** | Security master IDs unique, market data ISINs unique per date |
| **Valid values** | Currency codes (USD, EUR, GBP, JPY), security types, valuation methods |
| **Bounds** | WACI >= 0, WAGR between 0 and 1 |
| **Row counts** | Preprocess, harmonize, waterfall, and metrics outputs validated |

**dbt** implements these as singular SQL tests in `tests/` (32 tests).
**SQLMesh** implements these as audits in `audits/`, attached directly to model definitions (35 audits).

## MCP Server

The `packages/mcp/` package provides an [MCP](https://modelcontextprotocol.io/) server for conversational exploration of DuckDB databases. It connects to one or two DuckDB files via `ATTACH` and exposes tools an LLM can call.

### Available tools

| Tool | Description |
|------|-------------|
| `list_tables` | Show all tables with row counts across all attached databases |
| `describe_table` | Column names, types, and sample values |
| `query` | Run arbitrary read-only SQL (supports cross-database joins) |

### Usage with Claude Desktop

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "analytics": {
      "command": "uv",
      "args": [
        "run", "analytics-mcp",
        "--db", "data/dev/output/dbt-warehouse.duckdb",
        "--db", "data/dev/output/sqlmesh-warehouse.duckdb"
      ],
      "cwd": "/path/to/this/repo"
    }
  }
}
```

### CLI usage

```bash
# Default: both dbt and SQLMesh databases
just mcp

# Single database
just mcp --db data/dev/output/dbt-warehouse.duckdb
```

## Regenerating Seed Data

```bash
just seed
```

Generates 10 parquet files per environment in `data/{env}/input/`.

## Linting

Ruff config lives in the root `pyproject.toml` and applies to all packages:

```bash
just lint
```

## Further Reading

- [SQLMesh vs dbt: Detailed Comparison](SQLMESH_VS_DBT.md)
