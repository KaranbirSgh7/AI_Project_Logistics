"""Ingest the supply chain logistics CSV into the SQL Server running on the EC2 instance.

Usage:
    python scripts/ingest_to_sql.py
    python scripts/ingest_to_sql.py --csv data/raw/other_file.csv --table my_table --if-exists replace
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
import pymssql
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import os

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CSV = REPO_ROOT / "data" / "raw" / "dynamic_supply_chain_logistics_dataset.csv"
ENV_PATH = REPO_ROOT / "data" / "raw" / ".env"


def load_config() -> dict:
    load_dotenv(dotenv_path=ENV_PATH)

    required = ["SQL_SERVER_HOST", "SQL_SERVER_USER", "SQL_SERVER_PASSWORD", "SQL_SERVER_DATABASE"]
    config = {key: os.getenv(key) for key in required}
    config["SQL_SERVER_PORT"] = os.getenv("SQL_SERVER_PORT", "1433")
    config["SQL_SERVER_TABLE"] = os.getenv("SQL_SERVER_TABLE", "supply_chain_logistics")

    missing = [key for key, value in config.items() if not value and key in required]
    if missing:
        sys.exit(
            f"Missing required values in {ENV_PATH}: {', '.join(missing)}\n"
            "Fill these in before running the script."
        )
    return config


def ensure_database(config: dict) -> None:
    """Connect to the server (via the 'master' db) and create the target database if missing."""
    conn = pymssql.connect(
        server=config["SQL_SERVER_HOST"],
        port=config["SQL_SERVER_PORT"],
        user=config["SQL_SERVER_USER"],
        password=config["SQL_SERVER_PASSWORD"],
        database="master",
        autocommit=True,
    )
    try:
        cursor = conn.cursor()
        cursor.execute(
            "IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = %s) "
            "EXEC('CREATE DATABASE [' + %s + ']')",
            (config["SQL_SERVER_DATABASE"], config["SQL_SERVER_DATABASE"]),
        )
    finally:
        conn.close()


def build_engine(config: dict):
    url = (
        f"mssql+pymssql://{config['SQL_SERVER_USER']}:{config['SQL_SERVER_PASSWORD']}"
        f"@{config['SQL_SERVER_HOST']}:{config['SQL_SERVER_PORT']}/{config['SQL_SERVER_DATABASE']}"
    )
    return create_engine(url)


def ingest(csv_path: Path, table: str, if_exists: str, chunksize: int) -> None:
    config = load_config()
    table = table or config["SQL_SERVER_TABLE"]

    print(f"Ensuring database '{config['SQL_SERVER_DATABASE']}' exists ...")
    ensure_database(config)

    print(f"Connecting to {config['SQL_SERVER_HOST']}:{config['SQL_SERVER_PORT']}/{config['SQL_SERVER_DATABASE']} ...")
    engine = build_engine(config)

    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("Connection OK.")

    print(f"Reading {csv_path} ...")
    df = pd.read_csv(csv_path, parse_dates=["timestamp"])
    print(f"Loaded {len(df):,} rows, {len(df.columns)} columns.")

    print(f"Writing to table '{table}' (if_exists={if_exists}) ...")
    df.to_sql(
        table,
        engine,
        if_exists=if_exists,
        index=False,
        chunksize=chunksize,
        method="multi",
    )
    print(f"Done. Ingested {len(df):,} rows into '{table}'.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV, help="Path to the CSV file to ingest.")
    parser.add_argument("--table", type=str, default=None, help="Destination table name (overrides SQL_SERVER_TABLE).")
    parser.add_argument(
        "--if-exists",
        choices=["fail", "replace", "append"],
        default="replace",
        help="Behavior if the table already exists (default: replace).",
    )
    parser.add_argument("--chunksize", type=int, default=1000, help="Rows per batch insert (default: 1000).")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    ingest(args.csv, args.table, args.if_exists, args.chunksize)
