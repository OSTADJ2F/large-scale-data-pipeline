"""Incremental pipeline orchestration and backfills.

The pipeline is partitioned by month (matching the granularity of the source
taxi files). Each partition runs ingest -> validate -> normalize -> enrich and
records its status in the ``pipeline_runs`` metadata table. Re-running a
completed partition is a no-op unless ``force`` is set (backfill).
"""

from __future__ import annotations

from calendar import monthrange
from datetime import date
from uuid import uuid4

from pipeline.config import Settings, get_settings
from pipeline.enrich.runner import Enricher
from pipeline.ingest.runner import Ingestor, iter_months, parse_date
from pipeline.logging_setup import get_logger
from pipeline.metadata import MetadataStore, utcnow
from pipeline.normalize.runner import Normalizer
from pipeline.validate.runner import Validator

log = get_logger(__name__)

PIPELINE_NAME = "taxi_pipeline"


def _resolve_range(
    date_str: str | None, start_date: str | None, end_date: str | None
) -> tuple[date, date]:
    settings = get_settings()
    if date_str:
        d = parse_date(date_str, settings.default_start_date)
        return d, d
    start = parse_date(start_date, settings.default_start_date)
    end = parse_date(end_date, settings.default_end_date)
    return start, end


def _month_bounds(y: int, m: int) -> tuple[date, date]:
    return date(y, m, 1), date(y, m, monthrange(y, m)[1])


def is_partition_complete(meta: MetadataStore, partition: str) -> bool:
    run = meta.get_latest_run(PIPELINE_NAME, partition)
    return bool(run and run["status"] == "success")


def run_partition(settings: Settings, meta: MetadataStore, partition: str) -> dict:
    """Execute the full pipeline for a single month partition."""
    run_id = uuid4().hex
    started = utcnow()
    meta.record_run(
        {
            "run_id": run_id,
            "pipeline_name": PIPELINE_NAME,
            "partition_date": partition,
            "status": "running",
            "started_at": started,
            "completed_at": None,
            "input_rows": None,
            "output_rows": None,
            "error_message": None,
        }
    )

    y, m = (int(x) for x in partition.split("-"))
    start, end = _month_bounds(y, m)

    try:
        Ingestor(settings).run(start, end)
        v = Validator(settings).validate_partition(partition)
        n = Normalizer(settings).normalize_partition(partition)
        Enricher(settings).enrich_range(start, end)
        meta.record_run(
            {
                "run_id": run_id,
                "pipeline_name": PIPELINE_NAME,
                "partition_date": partition,
                "status": "success",
                "started_at": started,
                "completed_at": utcnow(),
                "input_rows": v.report["total_rows"],
                "output_rows": n.output_rows,
                "error_message": None,
            }
        )
        log.info(
            "partition_done",
            partition=partition,
            input_rows=v.report["total_rows"],
            output_rows=n.output_rows,
        )
        return {"partition": partition, "status": "success"}
    except Exception as exc:  # noqa: BLE001
        meta.record_run(
            {
                "run_id": run_id,
                "pipeline_name": PIPELINE_NAME,
                "partition_date": partition,
                "status": "failed",
                "started_at": started,
                "completed_at": utcnow(),
                "input_rows": None,
                "output_rows": None,
                "error_message": str(exc),
            }
        )
        log.error("partition_failed", partition=partition, error=str(exc))
        raise


def _run_global_steps() -> None:
    from pipeline.quality.runner import run_quality
    from pipeline.serving.loader import ensure_schema, load_marts

    run_quality()
    ensure_schema()
    load_marts()


def run_pipeline(
    date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    force: bool = False,
) -> list[dict]:
    settings = get_settings()
    start, end = _resolve_range(date, start_date, end_date)
    meta = MetadataStore(settings.data_dir / "pipeline_meta.duckdb")
    results: list[dict] = []
    try:
        for y, m in iter_months(start, end):
            partition = f"{y}-{m:02d}"
            if not force and is_partition_complete(meta, partition):
                log.info("partition_skipped", partition=partition, reason="already_complete")
                results.append({"partition": partition, "status": "skipped"})
                continue
            results.append(run_partition(settings, meta, partition))
        _run_global_steps()
    finally:
        meta.close()
    return results


def run_backfill(start_date: str, end_date: str) -> list[dict]:
    return run_pipeline(start_date=start_date, end_date=end_date, force=True)
