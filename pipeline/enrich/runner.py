"""Enrichment orchestration: join staging trips with weather and write curated."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from pipeline.config import Settings, get_settings
from pipeline.enrich.enrich import enrich_trips
from pipeline.enrich.weather import parse_weather
from pipeline.ingest.runner import iter_months, parse_date
from pipeline.logging_setup import get_logger
from pipeline.normalize.runner import write_partitioned

log = get_logger(__name__)


@dataclass
class EnrichSummary:
    start: date
    end: date
    total_rows: int
    unmatched_rows: int
    coverage: float


def iter_dates(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


class Enricher:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.settings.ensure_dirs()

    def _staging_files(self, start: date, end: date) -> list[Path]:
        base = self.settings.staging_dir / "trips"
        files = []
        for d in iter_dates(start, end):
            p = base / f"pickup_date={d.isoformat()}" / "part-00000.parquet"
            if p.exists():
                files.append(p)
        return files

    def _weather_files(self, start: date, end: date) -> list[Path]:
        base = self.settings.raw_dir / "weather"
        files = []
        for y, m in iter_months(start, end):
            p = base / f"year={y}" / f"month={m:02d}" / f"weather_{y}-{m:02d}.json"
            if p.exists():
                files.append(p)
        return files

    def enrich_range(self, start: date, end: date) -> EnrichSummary:
        staging = self._staging_files(start, end)
        if not staging:
            raise FileNotFoundError(f"No staging data between {start} and {end}")

        trips = pl.concat([pl.read_parquet(f) for f in staging], how="vertical_relaxed")

        weather_files = self._weather_files(start, end)
        weather = pl.concat([parse_weather(f) for f in weather_files], how="vertical_relaxed")

        result = enrich_trips(trips, weather)

        out = result.enriched
        dates = write_partitioned(out, self.settings.curated_dir / "trips")

        summary = EnrichSummary(
            start=start,
            end=end,
            total_rows=result.total_rows,
            unmatched_rows=result.unmatched_rows,
            coverage=result.coverage,
        )
        log.info(
            "enrich_done",
            start=start.isoformat(),
            end=end.isoformat(),
            total=summary.total_rows,
            unmatched=summary.unmatched_rows,
            coverage=f"{summary.coverage:.2%}",
            dates=len(dates),
        )
        return summary

    def run(self, start: date, end: date) -> list[EnrichSummary]:
        return [self.enrich_range(start, end)]


def run_enrich(start_date: str | None, end_date: str | None) -> None:
    settings = get_settings()
    start = parse_date(start_date, settings.default_start_date)
    end = parse_date(end_date, settings.default_end_date)
    enricher = Enricher(settings)
    for summary in enricher.run(start, end):
        print(
            f"  {summary.start}..{summary.end}: total={summary.total_rows} "
            f"unmatched={summary.unmatched_rows} coverage={summary.coverage:.2%}"
        )
