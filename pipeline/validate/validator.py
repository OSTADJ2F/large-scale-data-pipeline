"""Core validation logic: classify trips into valid / invalid / quarantined."""

from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from typing import Any

import polars as pl

from pipeline.validate.rules import (
    duplicate_mask,
    null_mask,
    outlier_mask,
    violation_masks,
)
from pipeline.validate.schemas import (
    CRITICAL_TRIP_COLUMNS,
    REQUIRED_TRIP_COLUMNS,
    TRIP_COLUMN_MAP,
    TRIP_SCHEMA,
)


class ValidationError(RuntimeError):
    pass


@dataclass
class ValidationResult:
    valid: pl.DataFrame
    invalid: pl.DataFrame
    quarantined: pl.DataFrame
    report: dict[str, Any]


def rename_to_canonical(df: pl.DataFrame) -> pl.DataFrame:
    mapping = {k: v for k, v in TRIP_COLUMN_MAP.items() if k in df.columns}
    return df.rename(mapping)


def ensure_required_columns(df: pl.DataFrame) -> None:
    missing = [c for c in REQUIRED_TRIP_COLUMNS if c not in df.columns]
    if missing:
        raise ValidationError(f"Missing required columns: {missing}")


def _cast_types(df: pl.DataFrame) -> pl.DataFrame:
    exprs = []
    for c in df.columns:
        if c in {"pickup_datetime", "dropoff_datetime"}:
            exprs.append(pl.col(c).cast(pl.Datetime("us")))
        elif c in {
            "pickup_location_id",
            "dropoff_location_id",
            "passenger_count",
            "payment_type",
        }:
            exprs.append(pl.col(c).cast(pl.Int64))
        elif c in {
            "trip_distance",
            "fare_amount",
            "tip_amount",
            "extra",
            "mta_tax",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "congestion_surcharge",
            "airport_fee",
        }:
            exprs.append(pl.col(c).cast(pl.Float64))
        else:
            exprs.append(pl.col(c))
    return df.select(exprs)


def validate_structure(df: pl.DataFrame) -> None:
    """Run Pandera structural validation; raise if the schema is violated."""
    TRIP_SCHEMA.validate(df, lazy=False)


def validate_trips(df: pl.DataFrame) -> ValidationResult:
    df = rename_to_canonical(df)
    ensure_required_columns(df)
    df = _cast_types(df)
    validate_structure(df)

    null_s = null_mask(df, CRITICAL_TRIP_COLUMNS)
    masks = violation_masks(df)
    invalid_s = reduce(lambda a, b: a | b, masks.values())
    clean_s = (~null_s) & (~invalid_s)

    dup_s = duplicate_mask(df) & clean_s
    out_s = outlier_mask(df) & clean_s & (~dup_s)
    quarantined_s = null_s | dup_s | out_s
    valid_s = clean_s & (~quarantined_s)

    flags = pl.DataFrame(
        {
            "__null": null_s,
            **{f"__r_{n}": s for n, s in masks.items()},
            "__dup": dup_s,
            "__out": out_s,
        }
    )

    invalid_reason = (
        pl.concat_list([pl.when(pl.col(f"__r_{n}")).then(pl.lit(n)) for n in masks])
        .list.drop_nulls()
        .list.join(", ")
    )

    quarantine_reason = (
        pl.when(pl.col("__null"))
        .then(pl.lit("null"))
        .otherwise(pl.when(pl.col("__dup")).then(pl.lit("duplicate")).otherwise(pl.lit("outlier")))
    )

    invalid_reason_s = flags.select(invalid_reason.alias("violation_reasons")).to_series()
    quarantine_reason_s = flags.select(quarantine_reason.alias("quarantine_reason")).to_series()

    valid = df.filter(valid_s)
    invalid = df.filter(invalid_s).with_columns(
        invalid_reason_s.filter(invalid_s).alias("violation_reasons")
    )
    quarantined = df.filter(quarantined_s).with_columns(
        quarantine_reason_s.filter(quarantined_s).alias("quarantine_reason")
    )

    report = _build_report(df, valid, invalid, quarantined, masks, null_s, dup_s, out_s)
    return ValidationResult(valid=valid, invalid=invalid, quarantined=quarantined, report=report)


def _build_report(
    df: pl.DataFrame,
    valid: pl.DataFrame,
    invalid: pl.DataFrame,
    quarantined: pl.DataFrame,
    masks: dict[str, pl.Series],
    null_s: pl.Series,
    dup_s: pl.Series,
    out_s: pl.Series,
) -> dict[str, Any]:
    error_categories = {name: int(s.sum()) for name, s in masks.items()}
    quarantine_categories = {
        "null": int(null_s.sum()),
        "duplicate": int(dup_s.sum()),
        "outlier": int(out_s.sum()),
    }
    null_counts = {c: int(df.select(pl.col(c).null_count()).item()) for c in REQUIRED_TRIP_COLUMNS}

    numeric_minmax = {}
    for c in ("trip_distance", "fare_amount", "tip_amount", "passenger_count"):
        s = df.select(pl.col(c).cast(pl.Float64)).to_series()
        numeric_minmax[c] = {"min": s.min(), "max": s.max()}

    return {
        "total_rows": df.height,
        "valid_rows": valid.height,
        "invalid_rows": invalid.height,
        "quarantined_rows": quarantined.height,
        "null_counts": null_counts,
        "duplicate_count": quarantine_categories["duplicate"],
        "error_categories": error_categories,
        "quarantine_categories": quarantine_categories,
        "numeric_minmax": numeric_minmax,
    }
