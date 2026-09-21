"""Weather parsing and normalization for Open-Meteo archive payloads."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

# WMO weather interpretation codes -> human-readable condition.
WMO_WEATHER_CONDITIONS: dict[int, str] = {
    0: "clear",
    1: "mainly_clear",
    2: "partly_cloudy",
    3: "overcast",
    45: "fog",
    48: "rime_fog",
    51: "light_drizzle",
    53: "drizzle",
    55: "heavy_drizzle",
    56: "freezing_drizzle",
    57: "heavy_freezing_drizzle",
    61: "light_rain",
    63: "rain",
    65: "heavy_rain",
    66: "freezing_rain",
    67: "heavy_freezing_rain",
    71: "light_snow",
    73: "snow",
    75: "heavy_snow",
    77: "snow_grains",
    80: "light_rain_showers",
    81: "rain_showers",
    82: "heavy_rain_showers",
    85: "snow_showers",
    86: "heavy_snow_showers",
    95: "thunderstorm",
    96: "thunderstorm_hail",
    99: "thunderstorm_heavy_hail",
}


def weather_condition(code: int) -> str:
    return WMO_WEATHER_CONDITIONS.get(code, "unknown")


def parse_weather(path: str | Path) -> pl.DataFrame:
    """Parse an Open-Meteo archive JSON file into an hourly weather frame."""
    with Path(path).open("r", encoding="utf-8") as fh:
        data = json.load(fh)

    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    df = pl.DataFrame(
        {
            "timestamp": times,
            "temperature": hourly.get("temperature_2m"),
            "precipitation": hourly.get("precipitation"),
            "wind_speed": hourly.get("wind_speed_10m"),
            "weather_code": hourly.get("weather_code"),
        }
    ).with_columns(pl.col("timestamp").str.to_datetime().cast(pl.Datetime("us")))

    df = df.with_columns(
        pl.col("temperature").cast(pl.Float64),
        pl.col("precipitation").cast(pl.Float64),
        pl.col("wind_speed").cast(pl.Float64),
        pl.col("weather_code").cast(pl.Int64),
    ).with_columns(
        pl.col("weather_code")
        .replace(WMO_WEATHER_CONDITIONS, default="unknown")
        .alias("weather_condition")
    )

    return df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("weather_hour")).select(
        ["weather_hour", "temperature", "precipitation", "wind_speed", "weather_condition"]
    )
