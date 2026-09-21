"""Normalization orchestration: write validated trips into partitioned staging."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl

from pipeline.config import Settings, get_settings
from pipeline.ingest.runner import iter_months, parse_date
from pipeline.logging_setup import get_logger
from pipeline.normalize.trips import normalize_trips

log = get_logger(__name__)


@dataclass
class NormalizeSummary:
    partition: str
    input_rows: int
    output_rows: int
    dates_written: list[str]


def write_partitioned(df: pl.DataFrame, base_dir: Path) -> list[str]:
    """Write ``df`` as Hive-partitioned Parquet keyed by pickup_date."""
    base_dir.mkdir(parents=True, exist_ok=True)
    dated = df.with_columns(pl.col("pickup_date").cast(pl.Utf8).alias("__pd")).sort(
        "pickup_datetime"
    )
    written: list[str] = []
    for key, group in dated.group_by("__pd", maintain_order=True):
        date_str = key[0]
        out_dir = base_dir / f"pickup_date={date_str}"
        out_dir.mkdir(parents=True, exist_ok=True)
        group.drop("__pd").write_parquet(out_dir / "part-00000.parquet")
        written.append(date_str)
    return sorted(written)


class Normalizer:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.settings.ensure_dirs()

    def _valid_file(self, partition: str) -> Path:
        return self.settings.validated_dir / "trips" / f"partition={partition}" / "valid.parquet"

    def normalize_partition(self, partition: str) -> NormalizeSummary:
        valid_file = self._valid_file(partition)
        if not valid_file.exists():
            raise FileNotFoundError(f"No validated data for partition {partition}")
        valid = pl.read_parquet(valid_file)
        normalized = normalize_trips(valid)
        dates = write_partitioned(normalized, self.settings.staging_dir / "trips")
        summary = NormalizeSummary(
            partition=partition,
            input_rows=valid.height,
            output_rows=normalized.height,
            dates_written=dates,
        )
        log.info(
            "normalize_done",
            partition=partition,
            input_rows=summary.input_rows,
            output_rows=summary.output_rows,
            dates=len(dates),
        )
        return summary

    def run(self, start: date, end: date) -> list[NormalizeSummary]:
        summaries = []
        for y, m in iter_months(start, end):
            partition = f"{y}-{m:02d}"
            try:
                summaries.append(self.normalize_partition(partition))
            except FileNotFoundError as exc:
                log.warning("missing_validated_partition", partition=partition, error=str(exc))
        return summaries


def run_normalize(start_date: str | None, end_date: str | None) -> None:
    settings = get_settings()
    start = parse_date(start_date, settings.default_start_date)
    end = parse_date(end_date, settings.default_end_date)
    normalizer = Normalizer(settings)
    summaries = normalizer.run(start, end)
    for s in summaries:
        print(
            f"  {s.partition}: input={s.input_rows} output={s.output_rows} "
            f"dates={len(s.dates_written)}"
        )
