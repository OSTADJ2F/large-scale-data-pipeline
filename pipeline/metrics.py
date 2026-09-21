"""Pipeline metrics reporting.

Populates a Prometheus-compatible metrics registry and renders it for the
``pipeline metrics`` command. Metrics are expanded in the observability layer.
"""

from __future__ import annotations

from pathlib import Path

from prometheus_client import CollectorRegistry, Counter, Gauge, generate_latest

from pipeline.config import get_settings
from pipeline.metadata import MetadataStore

registry = CollectorRegistry()

ROWS_INGESTED = Counter(
    "pipeline_rows_ingested_total", "Rows ingested", ["source"], registry=registry
)
ROWS_REJECTED = Counter(
    "pipeline_rows_rejected_total", "Rows rejected during validation", registry=registry
)
ROWS_TRANSFORMED = Counter(
    "pipeline_rows_transformed_total", "Rows normalized into staging", registry=registry
)
PARTITION_STATUS = Gauge(
    "pipeline_last_partition_status",
    "Status of the most recent partition run (1=success, 0=failed)",
    registry=registry,
)
STORAGE_BYTES = Gauge(
    "pipeline_storage_bytes", "Bytes stored in the data directory", registry=registry
)


def _data_dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def collect_metrics() -> None:
    settings = get_settings()
    meta = MetadataStore(settings.data_dir / "pipeline_meta.duckdb")
    try:
        ingestions = meta.list_ingestions()
        for rec in ingestions:
            if rec["status"] == "success":
                ROWS_INGESTED.labels(rec["source_name"]).inc(rec["row_count"] or 0)

        latest = meta.list_runs()
        if latest:
            PARTITION_STATUS.set(1 if latest[0]["status"] == "success" else 0)
            if latest[0]["status"] == "success":
                ROWS_TRANSFORMED.inc(latest[0]["output_rows"] or 0)
                ROWS_REJECTED.inc((latest[0]["input_rows"] or 0) - (latest[0]["output_rows"] or 0))
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
