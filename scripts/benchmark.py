"""Benchmark the pipeline stages against the local dataset.

Run from the repo root:
    .venv/Scripts/python -m scripts.benchmark
"""

from __future__ import annotations

import time

import duckdb
import polars as pl
import psutil
from pipeline.config import get_settings
from pipeline.enrich.enrich import enrich_trips
from pipeline.enrich.weather import parse_weather
from pipeline.normalize.trips import normalize_trips
from pipeline.validate.validator import validate_trips

PARTITION = "2025-01"


def _mb(n: int) -> str:
    return f"{n / (1024 * 1024):.1f} MB"


def _timed(label: str, fn):
    proc = psutil.Process()
    proc.cpu_percent()  # prime
    t0 = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - t0
    rss = proc.memory_info().rss
    print(f"{label:<22} {elapsed:>8.2f}s   peak_rss={_mb(rss)}")
    return result


def main() -> None:
    settings = get_settings()
    raw = (
        settings.raw_dir
        / "taxi_trips"
        / "year=2025"
        / "month=01"
        / "yellow_tripdata_2025-01.parquet"
    )
    weather = settings.raw_dir / "weather" / "year=2025" / "month=01" / "weather_2025-01.json"
    print("=== Input ===")
    print(f"raw parquet size      {_mb(raw.stat().st_size)}")
    raw_df = _timed("read raw parquet", lambda: pl.read_parquet(raw))
    print(f"raw row count         {raw_df.height:,}")

    print("\n=== Validate ===")
    valid = _timed("validate_trips", lambda: validate_trips(raw_df))
    print(
        f"valid={valid.report['valid_rows']:,} invalid={valid.report['invalid_rows']:,} "
        f"quarantined={valid.report['quarantined_rows']:,}"
    )

    print("\n=== Normalize ===")
    normalized = _timed("normalize_trips", lambda: normalize_trips(valid.valid))

    print("\n=== Enrich ===")
    weather_df = parse_weather(weather)

    def _enrich():
        return enrich_trips(normalized, weather_df)

    enriched = _timed("enrich_trips", _enrich)
    print(f"weather coverage       {enriched.coverage:.2%}")

    print("\n=== Output sizes ===")
    staged = settings.staging_dir / "trips"
    curated = settings.curated_dir / "trips"
    staged_bytes = sum(f.stat().st_size for f in staged.rglob("*.parquet"))
    curated_bytes = sum(f.stat().st_size for f in curated.rglob("*.parquet"))
    print(f"staging parquet       {_mb(staged_bytes)}")
    print(f"curated parquet       {_mb(curated_bytes)}")

    print("\n=== Query (DuckDB on marts) ===")
    con = duckdb.connect(str(settings.duckdb_path))

    def _query():
        return con.execute(
            "select pickup_location_id, sum(trip_count) c "
            "from main_marts.daily_demand group by 1 order by c desc limit 10"
        ).fetchall()

    t0 = time.perf_counter()
    rows = _query()
    print(f"top-zones query        {time.perf_counter() - t0:.4f}s   ({len(rows)} rows)")
    con.close()


if __name__ == "__main__":
    main()
