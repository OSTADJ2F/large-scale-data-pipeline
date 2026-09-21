"""End-to-end pipeline test on a small fixture dataset (no external downloads).

Covers: validate -> normalize -> enrich -> aggregate (marts-equivalent query),
using the same code paths as the real pipeline.
"""

from __future__ import annotations

from datetime import date

import polars as pl
from pipeline.config import Settings
from pipeline.enrich.runner import Enricher
from pipeline.normalize.runner import Normalizer
from pipeline.validate.runner import Validator

from tests import fixtures


def _settings(tmp_path) -> Settings:
    return Settings(
        data_dir=tmp_path / "data",
        storage_backend="local",
        quality_max_invalid_ratio=1.0,
        quality_max_null_ratio=1.0,
    )


def test_end_to_end_pipeline(tmp_path):
    data_dir = tmp_path / "data"
    fixtures.write_raw_trips(data_dir)
    fixtures.write_weather(data_dir)
    fixtures.write_zones(data_dir)
    settings = _settings(tmp_path)

    # validate
    summary = Validator(settings).validate_partition("2025-01")
    assert summary.report["total_rows"] == 5
    assert summary.report["valid_rows"] == 4
    assert summary.report["invalid_rows"] == 1  # negative distance
    assert summary.report["quarantined_rows"] == 0

    # normalize
    n = Normalizer(settings).normalize_partition("2025-01")
    assert n.input_rows == n.output_rows == 4

    # enrich
    e = Enricher(settings).enrich_range(date(2025, 1, 1), date(2025, 1, 2))
    assert e.coverage == 1.0
    assert e.unmatched_rows == 0

    # aggregate (marts-equivalent) from curated parquet
    curated = pl.read_parquet(
        settings.curated_dir / "trips" / "pickup_date=2025-01-01" / "part-00000.parquet"
    )
    assert {"temperature", "precipitation", "wind_speed", "weather_condition"}.issubset(
        curated.columns
    )
    assert curated.height >= 1
