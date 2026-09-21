import json
from datetime import datetime

import polars as pl
from pipeline.enrich.enrich import enrich_trips
from pipeline.enrich.weather import parse_weather, weather_condition


def make_weather_json(path):
    data = {
        "timezone": "America/New_York",
        "hourly": {
            "time": ["2025-01-01T00:00", "2025-01-01T01:00", "2025-01-01T02:00"],
            "temperature_2m": [0.0, 1.0, 2.0],
            "precipitation": [0.0, 0.2, 0.0],
            "wind_speed_10m": [5.0, 6.0, 7.0],
            "weather_code": [0, 61, 2],
        },
    }
    path.write_text(json.dumps(data), encoding="utf-8")


def test_parse_weather(tmp_path):
    p = tmp_path / "weather.json"
    make_weather_json(p)
    df = parse_weather(p)
    assert df.columns == [
        "weather_hour",
        "temperature",
        "precipitation",
        "wind_speed",
        "weather_condition",
    ]
    assert df.height == 3
    assert df["weather_condition"].to_list() == ["clear", "light_rain", "partly_cloudy"]
    assert df["weather_hour"].to_list()[0] == datetime(2025, 1, 1, 0, 0)


def test_weather_condition_mapping():
    assert weather_condition(0) == "clear"
    assert weather_condition(63) == "rain"
    assert weather_condition(999) == "unknown"


def make_trips():
    return pl.DataFrame(
        {
            "pickup_datetime": [
                datetime(2025, 1, 1, 0, 15),
                datetime(2025, 1, 1, 1, 45),
                datetime(2025, 1, 1, 3, 0),
            ],
            "trip_distance": [1.0, 2.0, 3.0],
        }
    )


def make_weather():
    return pl.DataFrame(
        {
            "weather_hour": [datetime(2025, 1, 1, 0, 0), datetime(2025, 1, 1, 1, 0)],
            "temperature": [0.0, 1.0],
            "precipitation": [0.0, 0.2],
            "wind_speed": [5.0, 6.0],
            "weather_condition": ["clear", "light_rain"],
        }
    )


def test_enrich_join_and_coverage():
    result = enrich_trips(make_trips(), make_weather())
    assert result.total_rows == 3
    assert result.unmatched_rows == 1  # 3:00 has no weather
    assert result.coverage == 2 / 3
    joined = result.enriched.sort("pickup_datetime")
    assert joined["weather_joined"].to_list() == [True, True, False]
    assert joined["weather_condition"].to_list()[0] == "clear"


def test_enrich_midnight_boundary():
    trips = pl.DataFrame(
        {"pickup_datetime": [datetime(2025, 1, 1, 0, 5), datetime(2025, 1, 1, 0, 55)]}
    )
    weather = pl.DataFrame(
        {
            "weather_hour": [datetime(2025, 1, 1, 0, 0)],
            "temperature": [0.0],
            "precipitation": [0.0],
            "wind_speed": [5.0],
            "weather_condition": ["clear"],
        }
    )
    result = enrich_trips(trips, weather)
    assert result.unmatched_rows == 0
