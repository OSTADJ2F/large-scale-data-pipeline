"""Load analytical marts from DuckDB into the PostgreSQL serving database."""

from __future__ import annotations

import duckdb
from sqlalchemy import create_engine

from pipeline.config import Settings, get_settings, postgres_dsn
from pipeline.logging_setup import get_logger

log = get_logger(__name__)

MARTS = [
    "dim_zones",
    "daily_demand",
    "hourly_demand",
    "route_performance",
    "weather_impact",
]


def _engine(settings: Settings):
    return create_engine(
        postgres_dsn(settings).replace("postgresql://", "postgresql+psycopg://", 1)
    )


def load_marts(settings: Settings | None = None) -> dict[str, int]:
    """Copy each mart table from DuckDB into PostgreSQL (replace semantics)."""
    settings = settings or get_settings()
    con = duckdb.connect(str(settings.duckdb_path))
    engine = _engine(settings)
    counts: dict[str, int] = {}
    try:
        for mart in MARTS:
            df = con.execute(f'SELECT * FROM main_marts."{mart}"').fetch_df()
            df.to_sql(mart, engine, if_exists="replace", index=False, schema="marts")
            counts[mart] = len(df)
            log.info("loaded_mart", mart=mart, rows=len(df))
    finally:
        con.close()
        engine.dispose()
    return counts


def ensure_schema(settings: Settings | None = None) -> None:
    settings = settings or get_settings()
    engine = _engine(settings)
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS marts")
    finally:
        engine.dispose()
