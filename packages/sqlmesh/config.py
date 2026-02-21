import os
from pathlib import Path

from sqlmesh.core.config import Config, DuckDBConnectionConfig, GatewayConfig, ModelDefaultsConfig

DATA_DIR = Path(os.environ.get("DATA_DIR", str(Path(__file__).resolve().parents[2] / "data")))

config = Config(
    gateways={
        "local": GatewayConfig(
            connection=DuckDBConnectionConfig(
                database=str(DATA_DIR / "dev" / "output" / "sqlmesh-warehouse.duckdb"),
            ),
        ),
    },
    default_gateway="local",
    model_defaults=ModelDefaultsConfig(dialect="duckdb", start="2025-12-31"),
    variables={
        "data_path": str(DATA_DIR / "dev" / "input"),
    },
)
