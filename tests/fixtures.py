"""Small on-disk fixtures used by tests (no external downloads)."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import polars as pl


def write_raw_trips(data_dir: Path, partition: str = "2025-01") -> Path:
    """Write a small raw TLC parquet with valid, invalid, and null rows."""
    y, m = partition.split("-")
    path = (
        data_dir
        / "raw"
        / "taxi_trips"
        / f"year={y}"
        / f"month={m}"
        / f"yellow_tripdata_{partition}.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)

    rows = {
        "tpep_pickup_datetime": [
            datetime(2025, 1, 1, 8, 0),
            datetime(2025, 1, 1, 9, 0),
            datetime(2025, 1, 1, 10, 0),
            datetime(2025, 1, 1, 11, 0),
            datetime(2025, 1, 2, 8, 0),
        ],
        "tpep_dropoff_datetime": [
            datetime(2025, 1, 1, 8, 30),
            datetime(2025, 1, 1, 9, 15),
            datetime(2025, 1, 1, 10, 30),
            datetime(2025, 1, 1, 11, 45),
            datetime(2025, 1, 2, 8, 20),
        ],
        "passenger_count": [1, 1, None, 2, 1],
        "trip_distance": [2.5, 3.0, 4.0, -1.0, 6.0],
        "PULocationID": [132, 100, 100, 100, 100],
        "DOLocationID": [200, 200, 138, 200, 200],
        "payment_type": [1, 2, 1, 1, 1],
        "fare_amount": [12.0, 15.0, 20.0, 10.0, 25.0],
        "tip_amount": [2.0, 3.0, 4.0, 1.0, 5.0],
    }
    pl.DataFrame(rows).write_parquet(path)
    return path


def write_weather(data_dir: Path, partition: str = "2025-01") -> Path:
    """Write a small Open-Meteo-style weather JSON covering Jan 1-2."""
    y, m = partition.split("-")
    path = data_dir / "raw" / "weather" / f"year={y}" / f"month={m}" / f"weather_{partition}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "timezone": "America/New_York",
        "hourly": {
            "time": [f"2025-01-{d:02d}T{h:02d}:00" for d in (1, 2) for h in range(24)],
            "temperature_2m": [float(h) for _ in (1, 2) for h in range(24)],
            "precipitation": [0.0 for _ in range(48)],
            "wind_speed_10m": [5.0 for _ in range(48)],
            "weather_code": [0 for _ in range(48)],
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def write_zones(data_dir: Path) -> Path:
    """Write a small taxi-zone lookup CSV covering referenced zone ids."""
    path = data_dir / "raw" / "taxi_zones" / "taxi_zone_lookup.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["LocationID,Borough,Zone,service_zone"]
    lines += ["100,Manhattan,Upper East Side North,Yellow Zone"]
    lines += ["132,Queens,JFK Airport,Airport"]
    lines += ["138,Queens,LaGuardia Airport,Airport"]
    lines += ["200,Manhattan,Lower Manhattan,Yellow Zone"]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path
