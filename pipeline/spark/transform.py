"""Distributed (PySpark) execution path.

Mirrors the Polars/DuckDB transformation logic so the same curated Parquet can
be processed at scale. Requires the ``spark`` extra: ``pip install -e .[spark]``
and a Java runtime. Run via ``scripts/spark_job.py``.

The derived-field definitions here are kept consistent with
``pipeline.normalize.trips`` so local and distributed outputs agree.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812

AIRPORT_ZONE_IDS = {1, 132, 138}

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

PAYMENT_TYPE_LABELS = {
    0: "not_recorded",
    1: "credit_card",
    2: "cash",
    3: "no_charge",
    4: "dispute",
    5: "unknown",
    6: "voided_trip",
}


def normalize_trips_spark(spark: SparkSession, path: str) -> DataFrame:
    """Read curated Parquet and compute derived fields (same rules as Polars)."""
    df = spark.read.parquet(path)

    if "passenger_count" in df.columns:
        df = df.withColumn("passenger_count", F.coalesce(F.col("passenger_count"), F.lit(0)))

    if "payment_type" in df.columns:
        payment_map = F.create_map(
            [F.lit(x) for k, v in PAYMENT_TYPE_LABELS.items() for x in (k, v)]
        )
        df = df.withColumn(
            "payment_type",
            F.coalesce(payment_map.getItem(F.col("payment_type").cast("long")), F.lit("unknown")),
        )

    duration = F.unix_timestamp("dropoff_datetime").cast("double") - F.unix_timestamp(
        "pickup_datetime"
    ).cast("double")
    distance = F.col("trip_distance").cast("double")

    money = [
        F.coalesce(F.col(c), F.lit(0)).cast("double") for c in MONEY_COLUMNS if c in df.columns
    ]
    total_charge = None
    for term in money:
        total_charge = term if total_charge is None else (total_charge + term)

    is_airport = F.col("pickup_location_id").isin(list(AIRPORT_ZONE_IDS)) | F.col(
        "dropoff_location_id"
    ).isin(list(AIRPORT_ZONE_IDS))

    df = df.withColumn("pickup_date", F.to_date("pickup_datetime"))
    df = df.withColumn("pickup_hour", F.hour("pickup_datetime"))
    # 0 = Monday ... 6 = Sunday (Python weekday convention).
    df = df.withColumn("pickup_day_of_week", (F.dayofweek("pickup_datetime") + 5) % 7)
    df = df.withColumn("trip_duration_seconds", duration)
    df = df.withColumn(
        "average_speed",
        F.when(duration > 0, distance / (duration / 3600.0)).otherwise(None),
    )
    df = df.withColumn("is_airport_trip", is_airport)
    if total_charge is not None:
        df = df.withColumn("total_charge", total_charge)
    return df


def build_marts_spark(df: DataFrame) -> dict[str, DataFrame]:
    daily = (
        df.groupBy("pickup_date", "pickup_location_id")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_duration_seconds").alias("average_trip_duration"),
            F.sum("trip_distance").alias("total_distance"),
            F.sum("total_charge").alias("total_revenue"),
            F.avg("fare_amount").alias("average_fare"),
        )
        .withColumnRenamed("pickup_date", "date")
    )

    hourly = (
        df.groupBy("pickup_date", "pickup_hour")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_duration_seconds").alias("average_trip_duration"),
            F.avg("fare_amount").alias("average_fare"),
        )
        .withColumnRenamed("pickup_date", "date")
        .withColumnRenamed("pickup_hour", "hour")
    )

    route = df.groupBy("pickup_location_id", "dropoff_location_id").agg(
        F.count("*").alias("trip_count"),
        F.avg("trip_distance").alias("average_distance"),
        F.avg("trip_duration_seconds").alias("average_duration"),
        F.sum("total_charge").alias("total_revenue"),
        F.avg("tip_amount").alias("average_tip"),
    )

    weather = (
        df.groupBy("pickup_date", "weather_condition", "precipitation")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_duration_seconds").alias("average_duration"),
            F.avg("fare_amount").alias("average_fare"),
        )
        .withColumnRenamed("pickup_date", "date")
    )

    return {
        "daily_demand": daily,
        "hourly_demand": hourly,
        "route_performance": route,
        "weather_impact": weather,
    }
