from pathlib import Path

from pipeline.normalize.trips import STAGING_COLUMNS

ROOT = Path(__file__).resolve().parent.parent
SPARK_TRANSFORM = ROOT / "pipeline" / "spark" / "transform.py"
SPARK_JOB = ROOT / "scripts" / "spark_job.py"

DERIVED_FIELDS = [
    "pickup_date",
    "pickup_hour",
    "pickup_day_of_week",
    "trip_duration_seconds",
    "average_speed",
    "total_charge",
    "is_airport_trip",
]


def test_spark_files_are_syntactically_valid():
    for f in (SPARK_TRANSFORM, SPARK_JOB):
        compile(f.read_text(encoding="utf-8"), str(f), "exec")


def test_spark_mirrors_polars_derived_fields():
    source = SPARK_TRANSFORM.read_text(encoding="utf-8")
    for field in DERIVED_FIELDS:
        assert field in source, f"spark transform missing derived field {field}"


def test_staging_columns_contain_derived_fields():
    for field in DERIVED_FIELDS:
        assert field in STAGING_COLUMNS, f"staging schema missing {field}"
