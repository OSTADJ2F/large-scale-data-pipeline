"""Pipeline metrics exposed in Prometheus text format.

Metrics are computed from the pipeline metadata store on every scrape and set
as gauges (absolute values), so repeated scrapes are idempotent.
"""

from __future__ import annotations

from pathlib import Path

from prometheus_client import CollectorRegistry, Gauge, Histogram, generate_latest

from pipeline.config import get_settings
from pipeline.metadata import MetadataStore

registry = CollectorRegistry()

ROWS_INGESTED = Gauge(
    "pipeline_rows_ingested_total",
    "Rows ingested per source",
    ["source"],
    registry=registry,
)
ROWS_REJECTED = Gauge(
    "pipeline_rows_rejected_total",
    "Rows rejected during validation (latest successful run)",
    registry=registry,
)
ROWS_TRANSFORMED = Gauge(
    "pipeline_rows_transformed_total",
    "Rows normalized into staging (latest successful run)",
    registry=registry,
)
PARTITION_STATUS = Gauge(
    "pipeline_last_partition_status",
    "Status of the most recent partition run (1=success, 0=failed)",
    registry=registry,
)
PIPELINE_DURATION = Gauge(
    "pipeline_run_duration_seconds",
    "Duration of the most recent pipeline run in seconds",
    registry=registry,
)
JOB_FAILURES = Gauge(
    "pipeline_job_failures_total",
    "Number of failed pipeline runs recorded",
    registry=registry,
)
LAST_SUCCESS_TIMESTAMP = Gauge(
    "pipeline_last_success_timestamp_seconds",
    "Unix timestamp of the last successful run",
    registry=registry,
)
CURRENT_PARTITION = Gauge(
    "pipeline_current_partition",
    "The partition of the most recent run (1 if present)",
    ["partition"],
    registry=registry,
)
STORAGE_BYTES = Gauge(
    "pipeline_storage_bytes",
    "Bytes stored in the data directory",
    registry=registry,
)
REQUEST_LATENCY = Histogram(
    "pipeline_http_request_duration_seconds",
    "API request latency in seconds",
    ["endpoint"],
    registry=registry,
)


def _data_dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def collect_metrics() -> None:
    settings = get_settings()
    meta = MetadataStore(settings.data_dir / "pipeline_meta.duckdb")
    try:
        per_source: dict[str, int] = {}
        for rec in meta.list_ingestions():
            if rec["status"] == "success":
                per_source[rec["source_name"]] = per_source.get(rec["source_name"], 0) + (
                    rec["row_count"] or 0
                )
        for source, n in per_source.items():
            ROWS_INGESTED.labels(source).set(n)

        runs = meta.list_runs()
        JOB_FAILURES.set(sum(1 for r in runs if r["status"] == "failed"))

        if runs:
            latest = runs[0]
            PARTITION_STATUS.set(1 if latest["status"] == "success" else 0)
            CURRENT_PARTITION.labels(latest["partition_date"]).set(1)
            started = latest.get("started_at")
            completed = latest.get("completed_at")
            if started and completed:
                PIPELINE_DURATION.set((completed - started).total_seconds())
            if latest["status"] == "success":
                ROWS_TRANSFORMED.set(latest["output_rows"] or 0)
                ROWS_REJECTED.set((latest["input_rows"] or 0) - (latest["output_rows"] or 0))
                if completed:
                    LAST_SUCCESS_TIMESTAMP.set(completed.timestamp())
    finally:
        meta.close()

    STORAGE_BYTES.set(_data_dir_size(settings.data_dir))


def render_metrics() -> bytes:
    collect_metrics()
    return generate_latest(registry)


def run_metrics() -> None:
    collect_metrics()
    latest = None
    settings = get_settings()
    meta = MetadataStore(settings.data_dir / "pipeline_meta.duckdb")
    try:
        runs = meta.list_runs()
        if runs:
            latest = runs[0]
    finally:
        meta.close()
    print(render_metrics().decode())
    if latest:
        print(
            f"Latest run: partition={latest['partition_date']} status={latest['status']} "
            f"input={latest['input_rows']} output={latest['output_rows']}"
        )
