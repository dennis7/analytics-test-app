set dotenv-load

export DATA_DIR := justfile_directory() / "data"

dbt_dir := "packages/dbt"
sqlmesh_dir := "packages/sqlmesh"

# lint: run ruff linter and formatter
lint:
    uv run ruff check --fix .
    uv run ruff format .

# seed: generate parquet seed data for all environments
seed:
    uv run scripts/generate_seed_data.py

# dbt: run any dbt command (e.g. just dbt build, just dbt test)
dbt *ARGS:
    uv run dbt {{ARGS}} --project-dir {{dbt_dir}} --profiles-dir {{dbt_dir}} --log-path logs

# sqlmesh: run any sqlmesh command (e.g. just sqlmesh plan, just sqlmesh run)
sqlmesh *ARGS:
    uv run sqlmesh --paths {{sqlmesh_dir}} {{ARGS}}

# mcp: start the MCP server (defaults to dbt dev output)
mcp *ARGS='--db data/dev/output/dbt-warehouse.duckdb':
    uv run analytics-mcp {{ARGS}}

# test-mcp: run MCP integration tests
test-mcp *ARGS:
    uv run pytest packages/mcp/tests {{ARGS}}
