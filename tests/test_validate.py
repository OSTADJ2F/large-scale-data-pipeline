from datetime import datetime

import polars as pl
import pytest
from pipeline.validate.validator import ValidationError, validate_trips


def make_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "pickup_datetime": [
                datetime(2025, 1, 1, 8, 0),
                datetime(2025, 1, 1, 9, 0),
                datetime(2025, 1, 1, 10, 0),
                datetime(2025, 1, 1, 11, 0),
                datetime(2025, 1, 1, 12, 0),
                datetime(2025, 1, 1, 13, 0),
                datetime(2025, 1, 1, 8, 0),
                datetime(2025, 1, 1, 14, 0),
            ],
            "dropoff_datetime": [
                datetime(2025, 1, 1, 8, 30),
                datetime(2025, 1, 1, 9, 30),
                datetime(2025, 1, 1, 9, 0),
                datetime(2025, 1, 1, 11, 30),
                datetime(2025, 1, 1, 12, 30),
                datetime(2025, 1, 1, 13, 30),
                datetime(2025, 1, 1, 8, 30),
                datetime(2025, 1, 1, 14, 30),
            ],
            "pickup_location_id": [100, 100, 100, 100, 999, 100, 100, 100],
            "dropoff_location_id": [200, 200, 200, 200, 200, 200, 200, 200],
            "passenger_count": [1, 1, 1, 15, 1, 1, 1, 1],
            "trip_distance": [2.5, -1.0, 2.0, 2.0, 2.0, None, 2.5, 500.0],
            "fare_amount": [12.0] * 8,
            "tip_amount": [2.0] * 8,
            "payment_type": [1] * 8,
        }
    )


def test_validate_classification():
    result = validate_trips(make_df())
    assert result.report["total_rows"] == 8
    assert result.report["valid_rows"] == 1
    assert result.report["invalid_rows"] == 4
    assert result.report["quarantined_rows"] == 3


def test_invalid_reasons_attached():
    result = validate_trips(make_df())
    reasons = set(result.invalid["violation_reasons"].to_list())
    assert reasons == {
        "negative_distance",
        "dropoff_before_pickup",
        "invalid_passenger_count",
        "invalid_pickup_location",
    }


def test_quarantine_reasons_attached():
    result = validate_trips(make_df())
    reasons = set(result.quarantined["quarantine_reason"].to_list())
    assert reasons == {"null", "duplicate", "outlier"}


def test_missing_required_column_raises():
    df = make_df().drop("fare_amount")
    with pytest.raises(ValidationError):
        validate_trips(df)


def test_report_contains_nulls_and_categories():
    report = validate_trips(make_df()).report
    assert report["null_counts"]["trip_distance"] == 1
    assert report["duplicate_count"] == 1
    assert report["error_categories"]["negative_distance"] == 1
    assert report["numeric_minmax"]["trip_distance"]["min"] == -1.0
    assert report["numeric_minmax"]["trip_distance"]["max"] == 500.0


def test_passenger_count_null_is_soft_null_not_quarantined():
    df = pl.DataFrame(
        {
            "pickup_datetime": [datetime(2025, 1, 1, 8, 0)],
            "dropoff_datetime": [datetime(2025, 1, 1, 8, 30)],
            "pickup_location_id": [1],
            "dropoff_location_id": [2],
            "passenger_count": [None],
            "trip_distance": [2.0],
            "fare_amount": [10.0],
            "tip_amount": [1.0],
            "payment_type": [1],
        }
    )
    result = validate_trips(df)
    assert result.report["valid_rows"] == 1
    assert result.report["quarantined_rows"] == 0
    assert result.report["null_counts"]["passenger_count"] == 1


def test_all_valid_rows_pass():
    df = pl.DataFrame(
        {
            "pickup_datetime": [datetime(2025, 1, 1, 8, 0)],
            "dropoff_datetime": [datetime(2025, 1, 1, 8, 30)],
            "pickup_location_id": [1],
            "dropoff_location_id": [2],
            "passenger_count": [1],
            "trip_distance": [2.0],
            "fare_amount": [10.0],
            "tip_amount": [1.0],
            "payment_type": [1],
        }
    )
    result = validate_trips(df)
    assert result.report["valid_rows"] == 1
    assert result.report["invalid_rows"] == 0
    assert result.report["quarantined_rows"] == 0


def test_runner_validates_partition_and_writes_outputs(tmp_path):
    from pipeline.config import Settings
    from pipeline.validate.runner import Validator

    settings = Settings(
        data_dir=tmp_path / "data",
        storage_backend="local",
        quality_max_invalid_ratio=1.0,
        quality_max_null_ratio=1.0,
    )
    settings.ensure_dirs()
    raw_dir = settings.raw_dir / "taxi_trips" / "year=2025" / "month=01"
    raw_dir.mkdir(parents=True)

    raw = pl.DataFrame(
        {
            "tpep_pickup_datetime": [datetime(2025, 1, 1, 8, 0), datetime(2025, 1, 1, 9, 0)],
            "tpep_dropoff_datetime": [datetime(2025, 1, 1, 8, 30), datetime(2025, 1, 1, 9, 30)],
            "passenger_count": [1, 1],
            "trip_distance": [2.0, -1.0],
            "PULocationID": [100, 100],
            "DOLocationID": [200, 200],
            "payment_type": [1, 1],
            "fare_amount": [10.0, 10.0],
            "tip_amount": [1.0, 1.0],
        }
    )
    raw.write_parquet(raw_dir / "yellow_tripdata_2025-01.parquet")

    summary = Validator(settings).validate_partition("2025-01")
    assert summary.report["total_rows"] == 2
    assert summary.report["valid_rows"] == 1
    assert summary.report["invalid_rows"] == 1
    assert summary.valid_path.exists()
    assert summary.invalid_path.exists()
    assert summary.quarantined_path.exists()
    report = settings.validated_dir / "trips" / "reports" / "2025-01.json"
    assert report.exists()
