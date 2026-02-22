"""Generate sample parquet files for the analytics pipeline.

Produces 10 parquet files per environment with public/private positions,
security master, entity carbon data, entity green revenue data,
entity hierarchy, and sector averages.  Each environment carries data
for two month-end dates so the pipeline can be exercised with multi-period
or single-period runs.
"""

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


# ---------------------------------------------------------------------------
# Reference data (shared across all environments and dates)
# ---------------------------------------------------------------------------

SECURITY_MASTER = {
    "internal_id": [
        "SEC001", "SEC002", "SEC003", "SEC004", "SEC005",
        "SEC006", "SEC007", "SEC008", "SEC009", "SEC010",
    ],
    "isin": [
        "US0001", "US0002", "US0003", "US0004", "US0005",
        "US0006", "US0007", None, None, None,
    ],
    "fund_id": [
        None, None, None, None, None,
        None, None, "FUND001", "FUND002", "FUND003",
    ],
    "security_type": [
        "equity", "equity", "equity", "equity", "bond",
        "equity", "equity", "private_equity", "private_equity", "private_equity",
    ],
    "sector": [
        "Energy", "Technology", "Utilities", "Financials", "Energy",
        "Healthcare", "Materials", "Technology", "Energy", "Financials",
    ],
    "entity_id": [
        "ENT001", "ENT002", "ENT003", "ENT004", "ENT006",
        "ENT007", "ENT008", "ENT009", "ENT010", "ENT011",
    ],
}

ENTITY_HIERARCHY = {
    "child_entity_id": ["ENT004", "ENT006", "ENT011"],
    "parent_entity_id": ["ENT005", "ENT001", "ENT005"],
}

SECTOR_CARBON_INTENSITIES = {
    "Energy": 0.00015,
    "Technology": 0.000005,
    "Utilities": 0.0002,
    "Financials": 0.000003,
    "Healthcare": 0.00001,
    "Materials": 0.0003,
}

SECTOR_GREEN_REVENUE_SHARES = {
    "Energy": 0.08,
    "Technology": 0.25,
    "Utilities": 0.20,
    "Financials": 0.12,
    "Healthcare": 0.15,
    "Materials": 0.05,
}

# ---------------------------------------------------------------------------
# Per-environment data — each environment has 2 month-end dates
# ---------------------------------------------------------------------------

_ENTITY_NAMES = ["PetroMax Inc", "TechGlobal Corp", "PowerGen Ltd", "MegaBank Holdings", "PrivTechFund LP"]
_ENTITY_IDS = ["ENT001", "ENT002", "ENT003", "ENT005", "ENT009"]

ENVIRONMENTS = {
    "dev": {
        "dates": {
            "2025-11-30": {
                "entity_carbon": {
                    "entity_id": _ENTITY_IDS,
                    "entity_name": _ENTITY_NAMES,
                    "carbon_emissions_tonnes": [840_000.0, 11_800.0, 610_000.0, 29_000.0, 7_800.0],
                    "revenue_usd": [4.9e9, 7.8e9, 2.9e9, 19.5e9, 1.9e9],
                },
                "entity_green_revenue": {
                    "entity_id": _ENTITY_IDS,
                    "entity_name": _ENTITY_NAMES,
                    "green_revenue_usd": [240e6, 2.3e9, 720e6, 1.9e9, 1.15e9],
                    "total_revenue_usd": [4.9e9, 7.8e9, 2.9e9, 19.5e9, 1.9e9],
                },
                "public_positions": {
                    "portfolio_id": ["PF001", "PF001", "PF001", "PF001", "PF001",
                                     "PF002", "PF002", "PF002", "PF002"],
                    "isin": ["US0001", "US0002", "US0003", "US0004", "US0006",
                             "US0002", "US0003", "US0005", "US0007"],
                    "quantity": [900.0, 1800.0, 1400.0, 2800.0, 450.0,
                                 2300.0, 1600.0, 1100.0, 550.0],
                },
                "private_positions": {
                    "portfolio_id": ["PF001", "PF002", "PF002"],
                    "fund_id": ["FUND001", "FUND002", "FUND003"],
                    "units_held": [90.0, 180.0, 140.0],
                },
                "public_market_data": {
                    "isin": ["US0001", "US0002", "US0003", "US0004", "US0005", "US0006", "US0007"],
                    "price": [145.00, 82.00, 215.00, 44.50, 305.00, 60.00, 170.00],
                    "currency": ["USD"] * 7,
                },
                "private_valuations": {
                    "fund_id": ["FUND001", "FUND002", "FUND003"],
                    "nav_per_unit": [1220.00, 960.00, 1080.00],
                    "valuation_method": ["transaction", "appraisal", "appraisal"],
                },
            },
            "2025-12-31": {
                "entity_carbon": {
                    "entity_id": _ENTITY_IDS,
                    "entity_name": _ENTITY_NAMES,
                    "carbon_emissions_tonnes": [850_000.0, 12_000.0, 620_000.0, 30_000.0, 8_000.0],
                    "revenue_usd": [5e9, 8e9, 3e9, 20e9, 2e9],
                },
                "entity_green_revenue": {
                    "entity_id": _ENTITY_IDS,
                    "entity_name": _ENTITY_NAMES,
                    "green_revenue_usd": [250e6, 2.4e9, 750e6, 2e9, 1.2e9],
                    "total_revenue_usd": [5e9, 8e9, 3e9, 20e9, 2e9],
                },
                "public_positions": {
                    "portfolio_id": ["PF001", "PF001", "PF001", "PF001", "PF001",
                                     "PF002", "PF002", "PF002", "PF002"],
                    "isin": ["US0001", "US0002", "US0003", "US0004", "US0006",
                             "US0002", "US0003", "US0005", "US0007"],
                    "quantity": [1000.0, 2000.0, 1500.0, 3000.0, 500.0,
                                 2500.0, 1800.0, 1200.0, 600.0],
                },
                "private_positions": {
                    "portfolio_id": ["PF001", "PF002", "PF002"],
                    "fund_id": ["FUND001", "FUND002", "FUND003"],
                    "units_held": [100.0, 200.0, 150.0],
                },
                "public_market_data": {
                    "isin": ["US0001", "US0002", "US0003", "US0004", "US0005", "US0006", "US0007"],
                    "price": [150.00, 85.50, 220.00, 45.75, 310.00, 62.25, 175.00],
                    "currency": ["USD"] * 7,
                },
                "private_valuations": {
                    "fund_id": ["FUND001", "FUND002", "FUND003"],
                    "nav_per_unit": [1250.00, 980.00, 1100.00],
                    "valuation_method": ["transaction", "appraisal", "appraisal"],
                },
            },
        },
    },
    "test": {
        "dates": {
            "2025-11-30": {
                "entity_carbon": {
                    "entity_id": _ENTITY_IDS,
                    "entity_name": _ENTITY_NAMES,
                    "carbon_emissions_tonnes": [860_000.0, 11_500.0, 630_000.0, 28_500.0, 7_600.0],
                    "revenue_usd": [4.85e9, 7.9e9, 2.95e9, 19.8e9, 1.95e9],
                },
                "entity_green_revenue": {
                    "entity_id": _ENTITY_IDS,
                    "entity_name": _ENTITY_NAMES,
                    "green_revenue_usd": [235e6, 2.35e9, 710e6, 1.95e9, 1.1e9],
                    "total_revenue_usd": [4.85e9, 7.9e9, 2.95e9, 19.8e9, 1.95e9],
                },
                "public_positions": {
                    "portfolio_id": ["PF001", "PF001", "PF001", "PF001", "PF001",
                                     "PF002", "PF002", "PF002", "PF002"],
                    "isin": ["US0001", "US0002", "US0003", "US0004", "US0006",
                             "US0002", "US0003", "US0005", "US0007"],
                    "quantity": [850.0, 1700.0, 1350.0, 2700.0, 420.0,
                                 2200.0, 1550.0, 1050.0, 520.0],
                },
                "private_positions": {
                    "portfolio_id": ["PF001", "PF002", "PF002"],
                    "fund_id": ["FUND001", "FUND002", "FUND003"],
                    "units_held": [85.0, 170.0, 130.0],
                },
                "public_market_data": {
                    "isin": ["US0001", "US0002", "US0003", "US0004", "US0005", "US0006", "US0007"],
                    "price": [143.00, 80.50, 212.00, 43.50, 302.00, 58.50, 168.00],
                    "currency": ["USD"] * 7,
                },
                "private_valuations": {
                    "fund_id": ["FUND001", "FUND002", "FUND003"],
                    "nav_per_unit": [1210.00, 950.00, 1070.00],
                    "valuation_method": ["transaction", "appraisal", "appraisal"],
                },
            },
            "2025-12-31": {
                "entity_carbon": {
                    "entity_id": _ENTITY_IDS,
                    "entity_name": _ENTITY_NAMES,
                    "carbon_emissions_tonnes": [870_000.0, 12_200.0, 640_000.0, 30_500.0, 8_300.0],
                    "revenue_usd": [5.1e9, 8.1e9, 3.1e9, 20.5e9, 2.05e9],
                },
                "entity_green_revenue": {
                    "entity_id": _ENTITY_IDS,
                    "entity_name": _ENTITY_NAMES,
                    "green_revenue_usd": [260e6, 2.45e9, 760e6, 2.05e9, 1.25e9],
                    "total_revenue_usd": [5.1e9, 8.1e9, 3.1e9, 20.5e9, 2.05e9],
                },
                "public_positions": {
                    "portfolio_id": ["PF001", "PF001", "PF001", "PF001", "PF001",
                                     "PF002", "PF002", "PF002", "PF002"],
                    "isin": ["US0001", "US0002", "US0003", "US0004", "US0006",
                             "US0002", "US0003", "US0005", "US0007"],
                    "quantity": [950.0, 1900.0, 1450.0, 2900.0, 480.0,
                                 2400.0, 1700.0, 1150.0, 580.0],
                },
                "private_positions": {
                    "portfolio_id": ["PF001", "PF002", "PF002"],
                    "fund_id": ["FUND001", "FUND002", "FUND003"],
                    "units_held": [95.0, 190.0, 145.0],
                },
                "public_market_data": {
                    "isin": ["US0001", "US0002", "US0003", "US0004", "US0005", "US0006", "US0007"],
                    "price": [148.00, 84.00, 218.00, 45.00, 308.00, 61.00, 173.00],
                    "currency": ["USD"] * 7,
                },
                "private_valuations": {
                    "fund_id": ["FUND001", "FUND002", "FUND003"],
                    "nav_per_unit": [1240.00, 975.00, 1095.00],
                    "valuation_method": ["transaction", "appraisal", "appraisal"],
                },
            },
        },
    },
}


def _concat_rows(records: list[dict], columns: list[str]) -> dict:
    """Concatenate a list of column-dicts into one dict with appended lists."""
    result = {col: [] for col in columns}
    for rec in records:
        for col in columns:
            result[col].extend(rec[col])
    return result


def write_env_data(env: str, config: dict) -> None:
    """Write all parquet files for a single environment."""
    env_dir = DATA_DIR / env / "input"
    env_dir.mkdir(parents=True, exist_ok=True)
    (DATA_DIR / env / "output").mkdir(parents=True, exist_ok=True)

    dates = sorted(config["dates"].keys())

    # --- Security master (no as_of_date) ---
    pq.write_table(pa.table(SECURITY_MASTER), env_dir / "security_master.parquet")

    # --- Entity hierarchy (no as_of_date) ---
    pq.write_table(pa.table(ENTITY_HIERARCHY), env_dir / "entity_hierarchy.parquet")

    # --- Sector carbon (repeated per date) ---
    sectors = list(SECTOR_CARBON_INTENSITIES.keys())
    sector_rows = []
    for d in dates:
        sector_rows.append({
            "sector": sectors,
            "avg_carbon_intensity": [SECTOR_CARBON_INTENSITIES[s] for s in sectors],
            "as_of_date": [d] * len(sectors),
        })
    pq.write_table(
        pa.table(_concat_rows(sector_rows, ["sector", "avg_carbon_intensity", "as_of_date"])),
        env_dir / "sector_carbon.parquet",
    )

    # --- Sector green revenue (repeated per date) ---
    sgr_rows = []
    for d in dates:
        sgr_rows.append({
            "sector": sectors,
            "avg_green_revenue_share": [SECTOR_GREEN_REVENUE_SHARES[s] for s in sectors],
            "as_of_date": [d] * len(sectors),
        })
    pq.write_table(
        pa.table(_concat_rows(sgr_rows, ["sector", "avg_green_revenue_share", "as_of_date"])),
        env_dir / "sector_green_revenue.parquet",
    )

    # --- Per-date data files ---
    entity_carbon_rows = []
    entity_green_rows = []
    pub_pos_rows = []
    priv_pos_rows = []
    pub_md_rows = []
    priv_val_rows = []

    for d in dates:
        date_cfg = config["dates"][d]

        # Entity carbon
        ec = date_cfg["entity_carbon"]
        n = len(ec["entity_id"])
        entity_carbon_rows.append({**ec, "as_of_date": [d] * n})

        # Entity green revenue
        egr = date_cfg["entity_green_revenue"]
        n = len(egr["entity_id"])
        entity_green_rows.append({**egr, "as_of_date": [d] * n})

        # Public positions
        pp = date_cfg["public_positions"]
        n = len(pp["portfolio_id"])
        pub_pos_rows.append({**pp, "as_of_date": [d] * n})

        # Private positions
        priv = date_cfg["private_positions"]
        n = len(priv["portfolio_id"])
        priv_pos_rows.append({**priv, "as_of_date": [d] * n})

        # Public market data
        md = date_cfg["public_market_data"]
        n = len(md["isin"])
        pub_md_rows.append({**md, "as_of_date": [d] * n})

        # Private valuations
        pv = date_cfg["private_valuations"]
        n = len(pv["fund_id"])
        priv_val_rows.append({**pv, "as_of_date": [d] * n})

    pq.write_table(
        pa.table(_concat_rows(entity_carbon_rows, ["entity_id", "entity_name", "carbon_emissions_tonnes", "revenue_usd", "as_of_date"])),
        env_dir / "entity_carbon.parquet",
    )
    pq.write_table(
        pa.table(_concat_rows(entity_green_rows, ["entity_id", "entity_name", "green_revenue_usd", "total_revenue_usd", "as_of_date"])),
        env_dir / "entity_green_revenue.parquet",
    )
    pq.write_table(
        pa.table(_concat_rows(pub_pos_rows, ["portfolio_id", "isin", "quantity", "as_of_date"])),
        env_dir / "public_positions.parquet",
    )
    pq.write_table(
        pa.table(_concat_rows(priv_pos_rows, ["portfolio_id", "fund_id", "units_held", "as_of_date"])),
        env_dir / "private_positions.parquet",
    )
    pq.write_table(
        pa.table(_concat_rows(pub_md_rows, ["isin", "price", "currency", "as_of_date"])),
        env_dir / "public_market_data.parquet",
    )
    pq.write_table(
        pa.table(_concat_rows(priv_val_rows, ["fund_id", "nav_per_unit", "valuation_method", "as_of_date"])),
        env_dir / "private_valuations.parquet",
    )


if __name__ == "__main__":
    for env, config in ENVIRONMENTS.items():
        write_env_data(env, config)
        dates = sorted(config["dates"].keys())
        first_date = config["dates"][dates[0]]
        n_pub = len(first_date["public_positions"]["portfolio_id"])
        n_priv = len(first_date["private_positions"]["portfolio_id"])
        n_sec = len(first_date["public_market_data"]["isin"])
        print(f"  {env}: {n_pub} public + {n_priv} private positions, {n_sec} securities, {len(dates)} dates ({', '.join(dates)})")

    print(f"\nSeed data written to {DATA_DIR}/")
