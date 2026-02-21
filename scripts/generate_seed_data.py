"""Generate sample parquet files per environment for the analytics pipeline."""

import os
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

DATA_DIR = Path(os.environ.get("DATA_DIR", ""))
if not DATA_DIR.name:
    print("Error: DATA_DIR environment variable is not set.", file=sys.stderr)
    print("Run via: just seed", file=sys.stderr)
    sys.exit(1)


ENVIRONMENTS = {
    "dev": {
        "as_of_date": "2025-12-31",
        "positions": {
            "portfolio_id": ["PF001"] * 5 + ["PF002"] * 5,
            "security_id": [
                "SEC001",
                "SEC002",
                "SEC003",
                "SEC004",
                "SEC005",
                "SEC001",
                "SEC003",
                "SEC005",
                "SEC006",
                "SEC007",
            ],
            "quantity": [
                1000.0,
                2000.0,
                1500.0,
                3000.0,
                500.0,
                2500.0,
                1800.0,
                1200.0,
                4000.0,
                600.0,
            ],
        },
        "market_data": {
            "security_id": ["SEC001", "SEC002", "SEC003", "SEC004", "SEC005", "SEC006", "SEC007"],
            "price": [150.00, 85.50, 220.00, 45.75, 310.00, 62.25, 175.00],
            "currency": ["USD"] * 7,
        },
        "carbon_scores": {
            "security_id": ["SEC001", "SEC002", "SEC003", "SEC004", "SEC005", "SEC006", "SEC007"],
            "company_name": [
                "GreenEnergy Corp",
                "PetroMax Inc",
                "TechNova Ltd",
                "SteelWorks Global",
                "CleanAir Systems",
                "CoalPower Holdings",
                "SolarBright Inc",
            ],
            "carbon_emissions_tonnes": [50000.0, 850000.0, 12000.0, 620000.0, 8000.0, 1200000.0, 3000.0],
            "revenue_usd": [2e9, 5e9, 8e9, 3e9, 1.5e9, 2.5e9, 1e9],
        },
    },
    "sit": {
        "as_of_date": "2025-12-31",
        "positions": {
            "portfolio_id": ["PF001"] * 5 + ["PF002"] * 5,
            "security_id": [
                "SEC001",
                "SEC002",
                "SEC003",
                "SEC004",
                "SEC005",
                "SEC002",
                "SEC004",
                "SEC005",
                "SEC006",
                "SEC007",
            ],
            "quantity": [
                800.0,
                1500.0,
                2200.0,
                1800.0,
                700.0,
                3000.0,
                2500.0,
                900.0,
                3500.0,
                400.0,
            ],
        },
        "market_data": {
            "security_id": ["SEC001", "SEC002", "SEC003", "SEC004", "SEC005", "SEC006", "SEC007"],
            "price": [152.30, 83.10, 225.50, 44.00, 315.75, 60.00, 180.25],
            "currency": ["USD"] * 7,
        },
        "carbon_scores": {
            "security_id": ["SEC001", "SEC002", "SEC003", "SEC004", "SEC005", "SEC006", "SEC007"],
            "company_name": [
                "GreenEnergy Corp",
                "PetroMax Inc",
                "TechNova Ltd",
                "SteelWorks Global",
                "CleanAir Systems",
                "CoalPower Holdings",
                "SolarBright Inc",
            ],
            "carbon_emissions_tonnes": [48000.0, 870000.0, 11500.0, 640000.0, 7500.0, 1250000.0, 2800.0],
            "revenue_usd": [2.1e9, 4.8e9, 8.2e9, 3.1e9, 1.6e9, 2.4e9, 1.1e9],
        },
    },
    "uat": {
        "as_of_date": "2025-12-31",
        "positions": {
            "portfolio_id": (["PF001"] * 6 + ["PF002"] * 6 + ["PF003"] * 6),
            "security_id": [
                "SEC001",
                "SEC002",
                "SEC003",
                "SEC004",
                "SEC005",
                "SEC008",
                "SEC001",
                "SEC003",
                "SEC005",
                "SEC006",
                "SEC007",
                "SEC008",
                "SEC002",
                "SEC004",
                "SEC006",
                "SEC007",
                "SEC008",
                "SEC009",
            ],
            "quantity": [
                1200.0,
                2500.0,
                1800.0,
                3200.0,
                600.0,
                1500.0,
                2800.0,
                2000.0,
                1400.0,
                4500.0,
                800.0,
                2200.0,
                1900.0,
                2700.0,
                3800.0,
                500.0,
                1600.0,
                3000.0,
            ],
        },
        "market_data": {
            "security_id": ["SEC001", "SEC002", "SEC003", "SEC004", "SEC005", "SEC006", "SEC007", "SEC008", "SEC009"],
            "price": [148.75, 87.00, 218.30, 46.50, 308.00, 64.10, 172.50, 95.00, 42.25],
            "currency": ["USD"] * 9,
        },
        "carbon_scores": {
            "security_id": ["SEC001", "SEC002", "SEC003", "SEC004", "SEC005", "SEC006", "SEC007", "SEC008", "SEC009"],
            "company_name": [
                "GreenEnergy Corp",
                "PetroMax Inc",
                "TechNova Ltd",
                "SteelWorks Global",
                "CleanAir Systems",
                "CoalPower Holdings",
                "SolarBright Inc",
                "BioFuel Partners",
                "HeavyMetal Mining",
            ],
            "carbon_emissions_tonnes": [
                52000.0,
                830000.0,
                13000.0,
                600000.0,
                8500.0,
                1180000.0,
                3200.0,
                25000.0,
                950000.0,
            ],
            "revenue_usd": [2.05e9, 5.1e9, 7.8e9, 3.2e9, 1.45e9, 2.6e9, 0.95e9, 1.2e9, 2.8e9],
        },
    },
    "prd": {
        "as_of_date": "2025-12-31",
        "positions": {
            "portfolio_id": (["PF001"] * 8 + ["PF002"] * 7 + ["PF003"] * 8 + ["PF004"] * 7),
            "security_id": [
                "SEC001",
                "SEC002",
                "SEC003",
                "SEC004",
                "SEC005",
                "SEC008",
                "SEC009",
                "SEC010",
                "SEC001",
                "SEC003",
                "SEC005",
                "SEC006",
                "SEC007",
                "SEC008",
                "SEC010",
                "SEC002",
                "SEC004",
                "SEC005",
                "SEC006",
                "SEC007",
                "SEC008",
                "SEC009",
                "SEC010",
                "SEC001",
                "SEC002",
                "SEC003",
                "SEC006",
                "SEC009",
                "SEC010",
                "SEC011",
            ],
            "quantity": [
                1500.0,
                3000.0,
                2200.0,
                4000.0,
                800.0,
                1800.0,
                2500.0,
                1200.0,
                3500.0,
                2400.0,
                1600.0,
                5000.0,
                1000.0,
                2800.0,
                900.0,
                2200.0,
                3500.0,
                1900.0,
                4200.0,
                700.0,
                2000.0,
                3200.0,
                1500.0,
                2800.0,
                1800.0,
                3000.0,
                3800.0,
                2600.0,
                1100.0,
                4500.0,
            ],
        },
        "market_data": {
            "security_id": [
                "SEC001",
                "SEC002",
                "SEC003",
                "SEC004",
                "SEC005",
                "SEC006",
                "SEC007",
                "SEC008",
                "SEC009",
                "SEC010",
                "SEC011",
            ],
            "price": [151.25, 86.00, 222.50, 45.00, 312.00, 63.00, 176.50, 93.75, 41.50, 128.00, 55.30],
            "currency": ["USD"] * 11,
        },
        "carbon_scores": {
            "security_id": [
                "SEC001",
                "SEC002",
                "SEC003",
                "SEC004",
                "SEC005",
                "SEC006",
                "SEC007",
                "SEC008",
                "SEC009",
                "SEC010",
                "SEC011",
            ],
            "company_name": [
                "GreenEnergy Corp",
                "PetroMax Inc",
                "TechNova Ltd",
                "SteelWorks Global",
                "CleanAir Systems",
                "CoalPower Holdings",
                "SolarBright Inc",
                "BioFuel Partners",
                "HeavyMetal Mining",
                "WindTech Global",
                "CementCo Ltd",
            ],
            "carbon_emissions_tonnes": [
                51000.0,
                845000.0,
                12500.0,
                615000.0,
                8200.0,
                1190000.0,
                3100.0,
                24000.0,
                940000.0,
                15000.0,
                780000.0,
            ],
            "revenue_usd": [2.02e9, 5.05e9, 7.9e9, 3.15e9, 1.48e9, 2.55e9, 0.98e9, 1.15e9, 2.75e9, 3.5e9, 2.2e9],
        },
    },
}


def write_env_data(env: str, config: dict) -> None:
    """Write all parquet files for a single environment."""
    env_dir = DATA_DIR / env / "input"
    env_dir.mkdir(parents=True, exist_ok=True)
    (DATA_DIR / env / "output").mkdir(parents=True, exist_ok=True)

    as_of_date = config["as_of_date"]
    pos = config["positions"]
    n_pos = len(pos["portfolio_id"])

    pq.write_table(
        pa.table({**pos, "as_of_date": [as_of_date] * n_pos}),
        env_dir / "portfolio_positions.parquet",
    )

    mkt = config["market_data"]
    n_mkt = len(mkt["security_id"])

    pq.write_table(
        pa.table({**mkt, "as_of_date": [as_of_date] * n_mkt}),
        env_dir / "market_data.parquet",
    )

    cs = config["carbon_scores"]
    n_cs = len(cs["security_id"])

    pq.write_table(
        pa.table({**cs, "as_of_date": [as_of_date] * n_cs}),
        env_dir / "carbon_scores.parquet",
    )


if __name__ == "__main__":
    for env, config in ENVIRONMENTS.items():
        write_env_data(env, config)
        n_positions = len(config["positions"]["portfolio_id"])
        n_securities = len(config["market_data"]["security_id"])
        print(f"  {env}: {n_positions} positions, {n_securities} securities")

    print(f"\nSeed data written to {DATA_DIR}/")
