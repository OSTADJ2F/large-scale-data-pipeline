"""Runnable PySpark job: read curated Parquet and write analytical marts.

Run in a Spark environment (e.g. Docker Spark, EMR, Databricks):

    spark-submit scripts/spark_job.py \
        --input s3://bucket/curated/trips \
        --output s3://bucket/marts
"""

from __future__ import annotations

import argparse

from pipeline.spark.transform import build_marts_spark, normalize_trips_spark
from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Glob/path to curated trip Parquet")
    parser.add_argument("--output", required=True, help="Output base directory for marts")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("taxi-analytics-marts").getOrCreate()

    trips = normalize_trips_spark(spark, args.input)
    marts = build_marts_spark(trips)

    for name, df in marts.items():
        df.write.mode("overwrite").parquet(f"{args.output.rstrip('/')}/{name}")
        print(f"wrote {name}: {df.count()} rows")

    spark.stop()


if __name__ == "__main__":
    main()
