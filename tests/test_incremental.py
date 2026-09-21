from datetime import date

from pipeline.incremental.runner import (
    _month_bounds,
    _resolve_range,
    is_partition_complete,
)
from pipeline.metadata import MetadataStore, utcnow


def test_resolve_range_single_date():
    start, end = _resolve_range("2025-01-15", None, None)
    assert start == date(2025, 1, 15)
    assert end == date(2025, 1, 15)


def test_resolve_range_between():
    start, end = _resolve_range(None, "2025-01-01", "2025-01-31")
    assert start == date(2025, 1, 1)
    assert end == date(2025, 1, 31)


def test_month_bounds():
    start, end = _month_bounds(2025, 2)
    assert start == date(2025, 2, 1)
    assert end == date(2025, 2, 28)
    _, end_leap = _month_bounds(2024, 2)
    assert end_leap == date(2024, 2, 29)


def test_is_partition_complete(tmp_path):
    meta = MetadataStore(tmp_path / "meta.duckdb")
    assert not is_partition_complete(meta, "2025-01")
    meta.record_run(
        {
            "run_id": "r1",
            "pipeline_name": "taxi_pipeline",
            "partition_date": "2025-01",
            "status": "success",
            "started_at": utcnow(),
            "completed_at": utcnow(),
            "input_rows": 100,
            "output_rows": 90,
            "error_message": None,
        }
    )
    assert is_partition_complete(meta, "2025-01")
    assert not is_partition_complete(meta, "2025-02")
    meta.close()


def test_failed_partition_not_complete(tmp_path):
    meta = MetadataStore(tmp_path / "meta.duckdb")
    meta.record_run(
        {
            "run_id": "r1",
            "pipeline_name": "taxi_pipeline",
            "partition_date": "2025-01",
            "status": "failed",
            "started_at": utcnow(),
            "completed_at": utcnow(),
            "input_rows": None,
            "output_rows": None,
            "error_message": "boom",
        }
    )
    assert not is_partition_complete(meta, "2025-01")
    meta.close()
