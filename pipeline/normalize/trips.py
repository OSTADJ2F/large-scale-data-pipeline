"""Staging normalization for validated trip data.

Timestamps from the NYC TLC feed are naive local (America/New_York) timestamps.
We keep them naive to avoid daylight-saving ambiguity and derive date/hour fields
directly from local time. This assumption is documented here and must be
consistent with the weather join in the enrichment layer.
"""

from __future__ import annotations

import polars as pl

TIMEZONE = "America/New_York"

PAYMENT_TYPE_LABELS: dict[int, str] = {
    0: "not_recorded",
    1: "credit_card",
    2: "cash",
    3: "no_charge",
    4: "dispute",
    5: "unknown",
    6: "voided_trip",
}

AIRPORT_ZONE_IDS: set[int] = {1, 132, 138}  # EWR, JFK, LGA

MONEY_COLUMNS = [
    "fare_amount",
    "tip_amount",
    "extra",
    "mta_tax",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
]


def normalize_trips(valid_df: pl.DataFrame) -> pl.DataFrame:
    """Normalize validated trips into the canonical staging schema."""
    df = valid_df

    # Soft-null passenger_count defaulted to 0.
    if "passenger_count" in df.columns:
        df = df.with_columns(pl.col("passenger_count").fill_null(0).cast(pl.Int64))

    # Normalize payment_type into a readable label.
    if "payment_type" in df.columns:
        df = df.with_columns(
            pl.col("payment_type")
            .cast(pl.Int64)
            .replace(PAYMENT_TYPE_LABELS, default="unknown")
            .alias("payment_type")
        )

    duration = (pl.col("dropoff_datetime") - pl.col("pickup_datetime")).dt.total_seconds()
    distance = pl.col("trip_distance").cast(pl.Float64)

    money_present = [c for c in MONEY_COLUMNS if c in df.columns]
    total_charge = None
    for c in money_present:
        term = pl.col(c).fill_null(0).cast(pl.Float64)
        total_charge = term if total_charge is None else (total_charge + term)

    airport_expr = pl.col("pickup_location_id").is_in(list(AIRPORT_ZONE_IDS)) | pl.col(
        "dropoff_location_id"
    ).is_in(list(AIRPORT_ZONE_IDS))

    exprs = [
        pl.col("pickup_datetime").dt.date().alias("pickup_date"),
        pl.col("pickup_datetime").dt.hour().alias("pickup_hour"),
        # 0 = Monday ... 6 = Sunday (Python weekday convention).
        (pl.col("pickup_datetime").dt.weekday() - 1).alias("pickup_day_of_week"),
        duration.alias("trip_duration_seconds"),
        pl.when(duration > 0)
        .then(distance / (duration / 3600.0))
        .otherwise(None)
        .alias("average_speed"),
        airport_expr.alias("is_airport_trip"),
    ]
    if total_charge is not None:
        exprs.append(total_charge.alias("total_charge"))

    return df.with_columns(exprs)


STAGING_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_date",
    "pickup_hour",
    "pickup_day_of_week",
    "pickup_location_id",
    "dropoff_location_id",
    "passenger_count",
    "trip_distance",
    "trip_duration_seconds",
    "average_speed",
    "fare_amount",
    "tip_amount",
    "total_charge",
    "payment_type",
    "is_airport_trip",
]
