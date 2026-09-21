"""Validation rule definitions.

Each correctness rule maps to a boolean Polars expression that is True where a
row violates the rule. Nulls are handled separately (quarantined), so rule
expressions should not treat null as a violation.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

MIN_LOCATION_ID = 1
MAX_LOCATION_ID = 265
MAX_PASSENGER_COUNT = 9
MAX_TRIP_DISTANCE_MILES = 200.0
MAX_TRIP_DURATION_SECONDS = 24 * 60 * 60
VALID_PAYMENT_TYPES = {1, 2, 3, 4, 5, 6}


@dataclass(frozen=True)
class Rule:
    name: str
    description: str
    expr: pl.Expr


CORRECTNESS_RULES: list[Rule] = [
    Rule(
        "dropoff_before_pickup",
        "dropoff_datetime must be on or after pickup_datetime",
        pl.col("dropoff_datetime") < pl.col("pickup_datetime"),
    ),
    Rule(
        "negative_distance",
        "trip_distance must be non-negative",
        pl.col("trip_distance") < 0,
    ),
    Rule(
        "negative_fare",
        "fare_amount must be non-negative",
        pl.col("fare_amount") < 0,
    ),
    Rule(
        "negative_tip",
        "tip_amount must be non-negative",
        pl.col("tip_amount") < 0,
    ),
    Rule(
        "invalid_passenger_count",
        f"passenger_count must be between 0 and {MAX_PASSENGER_COUNT}",
        (pl.col("passenger_count") < 0) | (pl.col("passenger_count") > MAX_PASSENGER_COUNT),
    ),
    Rule(
        "invalid_pickup_location",
        f"pickup_location_id must be between {MIN_LOCATION_ID} and {MAX_LOCATION_ID}",
        (pl.col("pickup_location_id") < MIN_LOCATION_ID)
        | (pl.col("pickup_location_id") > MAX_LOCATION_ID),
    ),
    Rule(
        "invalid_dropoff_location",
        f"dropoff_location_id must be between {MIN_LOCATION_ID} and {MAX_LOCATION_ID}",
        (pl.col("dropoff_location_id") < MIN_LOCATION_ID)
        | (pl.col("dropoff_location_id") > MAX_LOCATION_ID),
    ),
    Rule(
        "invalid_payment_type",
        "payment_type must be a known TLC payment code",
        ~pl.col("payment_type").is_in(list(VALID_PAYMENT_TYPES)),
    ),
]

RULE_BY_NAME = {r.name: r for r in CORRECTNESS_RULES}


def violation_masks(df: pl.DataFrame) -> dict[str, pl.Series]:
    """Return a boolean Series per rule, True where the row violates."""
    return {
        r.name: df.select(r.expr.fill_null(False).alias(r.name)).to_series()
        for r in CORRECTNESS_RULES
    }


def null_mask(df: pl.DataFrame, columns: list[str]) -> pl.Series:
    """True where any required column is null."""
    return df.select(
        pl.any_horizontal([pl.col(c).is_null() for c in columns]).fill_null(False).alias("__null")
    ).to_series()


def duplicate_mask(df: pl.DataFrame) -> pl.Series:
    """True for every row that is a duplicate of a row seen earlier (keep-first)."""
    keys = df.columns
    with_idx = df.with_row_index("__ridx")
    first = with_idx.group_by(keys, maintain_order=True).agg(
        pl.col("__ridx").min().alias("__first")
    )
    joined = with_idx.join(first, on=keys, how="left")
    return (joined["__ridx"] > joined["__first"]).fill_null(False)


def outlier_mask(df: pl.DataFrame) -> pl.Series:
    """True where the trip looks physically implausible (suspicious, not invalid)."""
    duration = (pl.col("dropoff_datetime") - pl.col("pickup_datetime")).dt.total_seconds()
    return df.select(
        (
            (pl.col("trip_distance") > MAX_TRIP_DISTANCE_MILES)
            | (duration > MAX_TRIP_DURATION_SECONDS)
        )
        .fill_null(False)
        .alias("__out")
    ).to_series()
