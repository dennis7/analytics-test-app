set dotenv-load

export DATA_DIR := justfile_directory() / "data"

dbt_dir := "packages/dbt"
sqlmesh_dir := "packages/sqlmesh"

# sync: install all Python and dbt dependencies
sync:
    uv sync --group dev
    uv run dbt deps --project-dir {{dbt_dir}} --profiles-dir {{dbt_dir}}

# lint: run ruff linter and formatter
lint:
    uv run ruff check --fix .
    uv run ruff format .

# seed: generate parquet seed data
seed:
    uv run scripts/generate_seed_data.py

# dbt: run dbt commands (e.g. just dbt build)
dbt *ARGS:
    @test -f logs/dbt.log && mv logs/dbt.log "logs/dbt_$(date +%Y_%m_%d_%H_%M_%S).log" || true
    uv run dbt {{ARGS}} --project-dir {{dbt_dir}} --profiles-dir {{dbt_dir}} --log-path logs

# sqlmesh: run sqlmesh commands (e.g. just sqlmesh plan)
sqlmesh *ARGS:
    uv run sqlmesh --paths {{sqlmesh_dir}} {{ARGS}}

# mcp: start the MCP server for DuckDB exploration
mcp *ARGS='--db data/dev/output/dbt-warehouse.duckdb --db data/dev/output/sqlmesh-warehouse.duckdb':
    uv run analytics-mcp {{ARGS}}

# test-mcp: run MCP integration tests
test-mcp *ARGS:
    uv run pytest packages/mcp/tests {{ARGS}}
