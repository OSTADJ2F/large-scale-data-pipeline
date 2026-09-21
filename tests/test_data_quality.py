"""Data-quality tests asserting invariants across the pipeline output."""

from __future__ import annotations

from datetime import date

import polars as pl
from pipeline.config import Settings
from pipeline.enrich.runner import Enricher
from pipeline.normalize.runner import Normalizer
from pipeline.normalize.trips import STAGING_COLUMNS
from pipeline.validate.runner import Validator
from pipeline.validate.schemas import CRITICAL_TRIP_COLUMNS

from tests import fixtures


def _run(tmp_path):
    data_dir = tmp_path / "data"
    fixtures.write_raw_trips(data_dir)
    fixtures.write_weather(data_dir)
    fixtures.write_zones(data_dir)
    settings = Settings(
        data_dir=data_dir,
        storage_backend="local",
        quality_max_invalid_ratio=1.0,
        quality_max_null_ratio=1.0,
    )
    Validator(settings).validate_partition("2025-01")
    Normalizer(settings).normalize_partition("2025-01")
    Enricher(settings).enrich_range(date(2025, 1, 1), date(2025, 1, 2))
    return settings


def _curated(settings) -> pl.DataFrame:
    return pl.read_parquet(
        settings.curated_dir / "trips" / "pickup_date=2025-01-01" / "part-00000.parquet"
    )


def test_curated_critical_fields_non_null(tmp_path):
    settings = _run(tmp_path)
    df = _curated(settings)
    for col in CRITICAL_TRIP_COLUMNS:
        assert df[col].null_count() == 0, f"{col} has nulls in curated output"


def test_curated_value_ranges(tmp_path):
    settings = _run(tmp_path)
    df = _curated(settings)
    assert df["trip_distance"].min() >= 0
    assert df["passenger_count"].min() >= 0
    assert df["passenger_count"].max() <= 9
    assert (df["trip_duration_seconds"] >= 0).all()


def test_curated_referential_integrity(tmp_path):
    settings = _run(tmp_path)
    df = _curated(settings)
    zones = pl.read_csv(settings.raw_dir / "taxi_zones" / "taxi_zone_lookup.csv")
    valid_ids = set(zones["LocationID"].to_list())
    assert set(df["pickup_location_id"].to_list()).issubset(valid_ids)
    assert set(df["dropoff_location_id"].to_list()).issubset(valid_ids)


def test_row_count_reconciliation(tmp_path):
    data_dir = tmp_path / "data"
    fixtures.write_raw_trips(data_dir)
    fixtures.write_weather(data_dir)
    settings = Settings(
        data_dir=data_dir,
        storage_backend="local",
        quality_max_invalid_ratio=1.0,
        quality_max_null_ratio=1.0,
    )
    v = Validator(settings).validate_partition("2025-01")
    n = Normalizer(settings).normalize_partition("2025-01")
    assert n.input_rows == v.report["valid_rows"]
    assert n.output_rows == n.input_rows


def test_staging_schema_has_expected_columns(tmp_path):
    settings = _run(tmp_path)
    df = _curated(settings)
    for col in STAGING_COLUMNS:
        assert col in df.columns, f"missing staging column {col}"
