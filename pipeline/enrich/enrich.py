"""Join staging trips with hourly weather.

Join strategy: nearest hour (floor-to-hour). A trip's pickup_datetime is
truncated to its local hour and joined to the matching weather hour. Because
both timestamps are naive America/New_York local time, daylight-saving
transitions (23/25-hour days) are handled by local-hour alignment.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

WEATHER_COLUMNS = ["temperature", "precipitation", "wind_speed", "weather_condition"]


@dataclass
class EnrichResult:
    enriched: pl.DataFrame
    coverage: float
    unmatched_rows: int
    total_rows: int


def enrich_trips(trips: pl.DataFrame, weather: pl.DataFrame) -> EnrichResult:
    trips = trips.with_columns(pl.col("pickup_datetime").dt.truncate("1h").alias("__pickup_hour"))
    joined = trips.join(
        weather,
        left_on="__pickup_hour",
        right_on="weather_hour",
        how="left",
    ).drop("__pickup_hour")

    joined = joined.with_columns(pl.col("temperature").is_not_null().alias("weather_joined"))

    total = joined.height
    unmatched = joined.filter(~pl.col("weather_joined")).height
    coverage = (total - unmatched) / total if total else 0.0
    return EnrichResult(
        enriched=joined,
        coverage=coverage,
        unmatched_rows=unmatched,
        total_rows=total,
    )
