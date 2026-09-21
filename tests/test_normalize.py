from datetime import datetime

import polars as pl
import pytest
from pipeline.normalize.runner import write_partitioned
from pipeline.normalize.trips import normalize_trips


def make_valid_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "pickup_datetime": [
                datetime(2025, 1, 1, 8, 0),
                datetime(2025, 1, 1, 9, 0),
                datetime(2025, 1, 2, 8, 0),
            ],
            "dropoff_datetime": [
                datetime(2025, 1, 1, 8, 30),
                datetime(2025, 1, 1, 9, 15),
                datetime(2025, 1, 2, 9, 0),
            ],
            "pickup_location_id": [132, 100, 100],
            "dropoff_location_id": [200, 200, 138],
            "passenger_count": [1, None, 2],
            "trip_distance": [2.5, 3.0, 10.0],
            "fare_amount": [12.0, 15.0, 20.0],
            "tip_amount": [2.0, 3.0, 4.0],
            "payment_type": [1, 2, 1],
        }
    )


def test_derived_fields():
    out = normalize_trips(make_valid_df())
    row = out.row(0, named=True)
    assert row["pickup_date"] == datetime(2025, 1, 1, 8, 0).date()
    assert row["pickup_hour"] == 8
    assert row["pickup_day_of_week"] == 2  # Wednesday
    assert row["trip_duration_seconds"] == 1800
    assert row["average_speed"] == pytest.approx(5.0)
    assert row["payment_type"] == "credit_card"
    assert row["is_airport_trip"] is True


def test_passenger_count_null_defaulted_to_zero():
    out = normalize_trips(make_valid_df())
    assert out["passenger_count"].to_list() == [1, 0, 2]


def test_total_charge_sum():
    out = normalize_trips(make_valid_df())
    assert out["total_charge"].to_list()[0] == pytest.approx(14.0)


def test_airport_dropoff_flagged():
    out = normalize_trips(make_valid_df())
    assert out["is_airport_trip"].to_list() == [True, False, True]


def test_write_partitioned(tmp_path):
    out = normalize_trips(make_valid_df())
    dates = write_partitioned(out, tmp_path / "trips")
    assert dates == ["2025-01-01", "2025-01-02"]
    d1 = tmp_path / "trips" / "pickup_date=2025-01-01" / "part-00000.parquet"
    assert d1.exists()
    assert pl.read_parquet(d1).height == 2
