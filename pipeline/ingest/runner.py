"""Ingestion orchestration.

Downloads raw source files into the immutable raw layer, records metadata in the
ingestion manifest, and makes re-runs idempotent.
"""

from __future__ import annotations

import tempfile
from calendar import monthrange
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from urllib.parse import urlencode

from pipeline.config import Settings, get_settings, load_sources
from pipeline.ingest.downloader import DownloadError, checksum_file, download_file, row_count
from pipeline.logging_setup import get_logger
from pipeline.metadata import MetadataStore, utcnow
from pipeline.storage import get_storage

log = get_logger(__name__)

WEATHER_HOURLY = "temperature_2m,precipitation,wind_speed_10m,weather_code"


@dataclass
class IngestionResult:
    source_name: str
    partition_date: str | None
    status: str
    file_path: str | None = None
    file_size: int | None = None
    row_count: int | None = None
    error: str | None = None


def parse_date(value: str | None, fallback: str) -> date:
    return date.fromisoformat(value) if value else date.fromisoformat(fallback)


def iter_months(start: date, end: date):
    """Yield (year, month) tuples covering [start, end] inclusive."""
    if end < start:
        raise ValueError("end_date precedes start_date")
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield y, m
        m += 1
        if m > 12:
            m = 1
            y += 1


class Ingestor:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.settings.ensure_dirs()
        self.storage = get_storage(self.settings)
        self.meta = MetadataStore(self.settings.data_dir / "pipeline_meta.duckdb")
        self.sources = load_sources()

    def _key(self, source: str, partition: str | None) -> str:
        if source == "taxi_trips":
            y, m = partition.split("-")
            return f"raw/taxi_trips/year={y}/month={m}/yellow_tripdata_{y}-{m}.parquet"
        if source == "taxi_zones":
            return "raw/taxi_zones/taxi_zone_lookup.csv"
        if source == "weather":
            y, m = partition.split("-")
            return f"raw/weather/year={y}/month={m}/weather_{y}-{m}.json"
        raise ValueError(f"Unknown source: {source}")

    def _already_ingested(self, source: str, partition: str | None, key: str) -> bool:
        rec = self.meta.get_ingestion(source, partition)
        return bool(rec and rec["status"] == "success" and self.storage.exists(key))

    def _record(
        self,
        source: str,
        partition: str | None,
        url: str,
        key: str,
        path: Path,
        status: str,
        error: str | None = None,
    ) -> dict:
        record = {
            "source_name": source,
            "source_url": url,
            "partition_date": partition,
            "downloaded_at": utcnow(),
            "file_path": key,
            "file_size": path.stat().st_size if path and path.exists() else None,
            "checksum": checksum_file(path) if path and path.exists() else None,
            "row_count": None,
            "status": status,
        }
        if status == "success":
            record["row_count"] = row_count(path, self.sources[source]["format"])
        self.meta.record_ingestion(record)
        if error:
            log.error("ingestion_failed", source=source, partition=partition, error=error)
        return record

    def _store(self, key: str, tmp: Path) -> None:
        self.storage.write_file(key, tmp)

    # --- source handlers ----------------------------------------------------

    def _ingest_taxi_trips(self, start: date, end: date) -> list[IngestionResult]:
        template = self.sources["taxi_trips"]["url"]
        results: list[IngestionResult] = []
        for y, m in iter_months(start, end):
            partition = f"{y}-{m:02d}"
            url = template.format(year=y, month=m)
            key = self._key("taxi_trips", partition)
            results.append(self._download_one("taxi_trips", partition, url, key))
        return results

    def _ingest_taxi_zones(self) -> list[IngestionResult]:
        url = self.sources["taxi_zones"]["url"]
        key = self._key("taxi_zones", None)
        return [self._download_one("taxi_zones", None, url, key)]

    def _ingest_weather(self, start: date, end: date) -> list[IngestionResult]:
        base = self.sources["weather"]["url"]
        results: list[IngestionResult] = []
        for y, m in iter_months(start, end):
            partition = f"{y}-{m:02d}"
            first = date(y, m, 1)
            last = date(y, m, monthrange(y, m)[1])
            params = {
                "latitude": self.settings.weather_latitude,
                "longitude": self.settings.weather_longitude,
                "start_date": first.isoformat(),
                "end_date": last.isoformat(),
                "hourly": WEATHER_HOURLY,
                "timezone": "America/New_York",
            }
            url = f"{base}?{urlencode(params)}"
            key = self._key("weather", partition)
            results.append(self._download_one("weather", partition, url, key))
        return results

    # --- generic single-file download --------------------------------------

    def _download_one(
        self, source: str, partition: str | None, url: str, key: str
    ) -> IngestionResult:
        if self._already_ingested(source, partition, key):
            log.info("already_ingested", source=source, partition=partition)
            rec = self.meta.get_ingestion(source, partition)
            return IngestionResult(
                source_name=source,
                partition_date=partition,
                status="success",
                file_path=key,
                file_size=rec["file_size"],
                row_count=rec["row_count"],
            )
        try:
            with tempfile.TemporaryDirectory() as td:
                tmp = Path(td) / Path(key).name
                download_file(
                    url, tmp, self.settings.max_retries, self.settings.retry_delay_seconds
                )
                rec = self._record(source, partition, url, key, tmp, "success")
                self._store(key, tmp)
                log.info(
                    "ingested",
                    source=source,
                    partition=partition,
                    bytes=rec["file_size"],
                    rows=rec["row_count"],
                )
            return IngestionResult(
                source_name=source,
                partition_date=partition,
                status="success",
                file_path=key,
                file_size=rec["file_size"],
                row_count=rec["row_count"],
            )
        except Exception as exc:  # noqa: BLE001
            self._record_failed(source, partition, url, key, str(exc))
            return IngestionResult(
                source_name=source,
                partition_date=partition,
                status="failed",
                error=str(exc),
            )

    def _record_failed(
        self, source: str, partition: str | None, url: str, key: str, error: str
    ) -> dict:
        record = {
            "source_name": source,
            "source_url": url,
            "partition_date": partition,
            "downloaded_at": utcnow(),
            "file_path": key,
            "file_size": None,
            "checksum": None,
            "row_count": None,
            "status": "failed",
        }
        self.meta.record_ingestion(record)
        log.error("ingestion_failed", source=source, partition=partition, error=error)
        return record

    def run(self, start: date, end: date) -> list[IngestionResult]:
        results: list[IngestionResult] = []
        results += self._ingest_taxi_trips(start, end)
        results += self._ingest_taxi_zones()
        results += self._ingest_weather(start, end)
        self.meta.close()
        return results


def run_ingest(start_date: str | None, end_date: str | None) -> None:
    settings = get_settings()
    start = parse_date(start_date, settings.default_start_date)
    end = parse_date(end_date, settings.default_end_date)
    ingestor = Ingestor(settings)
    results = ingestor.run(start, end)

    failed = [r for r in results if r.status != "success"]
    summary = {
        "total": len(results),
        "success": len(results) - len(failed),
        "failed": len(failed),
    }
    log.info("ingest_summary", **summary)
    print(f"Ingestion complete: {summary['success']} succeeded, {summary['failed']} failed.")
    if failed:
        for r in failed:
            print(f"  FAILED {r.source_name} {r.partition_date}: {r.error}")
        raise DownloadError(f"{len(failed)} ingestion(s) failed")
