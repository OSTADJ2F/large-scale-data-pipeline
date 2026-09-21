"""Schemas and column mappings for pipeline inputs.

Raw NYC TLC column names are mapped to canonical snake_case names on read.
"""

from __future__ import annotations

import pandera.polars as pa
import polars as pl

# Raw TLC column -> canonical column name.
TRIP_COLUMN_MAP: dict[str, str] = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "Airport_fee": "airport_fee",
}

# Columns that must be present and non-null for a trip to be considered valid.
REQUIRED_TRIP_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
]

# Subset that must be non-null for a trip to avoid quarantine. passenger_count
# is a "soft" field: nulls are defaulted to 0 during normalization rather than
# quarantined (a known TLC data characteristic).
CRITICAL_TRIP_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
]

TRIP_SCHEMA = pa.DataFrameSchema(
    {
        "pickup_datetime": pa.Column(pl.Datetime("us"), nullable=True),
        "dropoff_datetime": pa.Column(pl.Datetime("us"), nullable=True),
        "pickup_location_id": pa.Column(pl.Int64, nullable=True),
        "dropoff_location_id": pa.Column(pl.Int64, nullable=True),
        "passenger_count": pa.Column(pl.Int64, nullable=True),
        "trip_distance": pa.Column(pl.Float64, nullable=True),
        "fare_amount": pa.Column(pl.Float64, nullable=True),
        "tip_amount": pa.Column(pl.Float64, nullable=True),
        "payment_type": pa.Column(pl.Int64, nullable=True),
    },
    coerce=False,
)

WEATHER_SCHEMA = pa.DataFrameSchema(
    {
        "timestamp": pa.Column(pl.Datetime("us"), nullable=False),
        "temperature": pa.Column(pl.Float64, nullable=True),
        "precipitation": pa.Column(pl.Float64, nullable=True),
        "wind_speed": pa.Column(pl.Float64, nullable=True),
        "weather_code": pa.Column(pl.Int64, nullable=True),
    },
    coerce=False,
)
