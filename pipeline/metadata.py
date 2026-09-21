"""DuckDB-backed metadata store.

Persists the ingestion manifest and pipeline-run history. DuckDB is used for
metadata because it is a single-file, zero-config store that keeps the pipeline
self-contained (no external database required until the serving layer).
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

INGESTION_SCHEMA = """
CREATE TABLE IF NOT EXISTS ingestion_manifest (
    source_name    VARCHAR NOT NULL,
    source_url     VARCHAR,
    partition_date VARCHAR,
    downloaded_at  TIMESTAMP,
    file_path      VARCHAR,
    file_size      BIGINT,
    checksum       VARCHAR,
    row_count      BIGINT,
    status         VARCHAR,
    PRIMARY KEY (source_name, partition_date)
);
"""

RUNS_SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id         VARCHAR PRIMARY KEY,
    pipeline_name  VARCHAR,
    partition_date VARCHAR,
    status         VARCHAR,
    started_at     TIMESTAMP,
    completed_at   TIMESTAMP,
    input_rows     BIGINT,
    output_rows    BIGINT,
    error_message  VARCHAR
);
"""


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class MetadataStore:
    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(self.path)
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(INGESTION_SCHEMA)
        self._conn.execute(RUNS_SCHEMA)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> MetadataStore:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # --- ingestion manifest -------------------------------------------------

    def record_ingestion(self, record: dict[str, Any]) -> None:
        self._conn.execute(
            """
            INSERT OR REPLACE INTO ingestion_manifest
                (source_name, source_url, partition_date, downloaded_at,
                 file_path, file_size, checksum, row_count, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                record["source_name"],
                record.get("source_url"),
                record.get("partition_date") or "",
                record.get("downloaded_at"),
                record.get("file_path"),
                record.get("file_size"),
                record.get("checksum"),
                record.get("row_count"),
                record.get("status"),
            ],
        )

    def get_ingestion(self, source_name: str, partition_date: str | None) -> dict[str, Any] | None:
        row = self._conn.execute(
            """
            SELECT source_name, source_url, partition_date, downloaded_at,
                   file_path, file_size, checksum, row_count, status
            FROM ingestion_manifest
            WHERE source_name = ? AND partition_date IS NOT DISTINCT FROM ?
            """,
            [source_name, partition_date or ""],
        ).fetchone()
        if row is None:
            return None
        cols = [
            "source_name",
            "source_url",
            "partition_date",
            "downloaded_at",
            "file_path",
            "file_size",
            "checksum",
            "row_count",
            "status",
        ]
        return dict(zip(cols, row, strict=True))

    def list_ingestions(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT source_name, partition_date, downloaded_at, file_path,
                   file_size, checksum, row_count, status
            FROM ingestion_manifest ORDER BY source_name, partition_date
            """
        ).fetchall()
        cols = [
            "source_name",
            "partition_date",
            "downloaded_at",
            "file_path",
            "file_size",
            "checksum",
            "row_count",
            "status",
        ]
        return [dict(zip(cols, r, strict=True)) for r in rows]

    # --- pipeline runs ------------------------------------------------------

    def record_run(self, record: dict[str, Any]) -> None:
        self._conn.execute(
            """
            INSERT OR REPLACE INTO pipeline_runs
                (run_id, pipeline_name, partition_date, status, started_at,
                 completed_at, input_rows, output_rows, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                record["run_id"],
                record.get("pipeline_name"),
                record.get("partition_date"),
                record.get("status"),
                record.get("started_at"),
                record.get("completed_at"),
                record.get("input_rows"),
                record.get("output_rows"),
                record.get("error_message"),
            ],
        )

    def get_latest_run(self, pipeline_name: str, partition_date: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            """
            SELECT * FROM pipeline_runs
            WHERE pipeline_name = ? AND partition_date = ?
            ORDER BY started_at DESC LIMIT 1
            """,
            [pipeline_name, partition_date],
        ).fetchone()
        if row is None:
            return None
        cols = [
            "run_id",
            "pipeline_name",
            "partition_date",
            "status",
            "started_at",
            "completed_at",
            "input_rows",
            "output_rows",
            "error_message",
        ]
        return dict(zip(cols, row, strict=True))

    def list_runs(self, pipeline_name: str | None = None) -> list[dict[str, Any]]:
        if pipeline_name:
            rows = self._conn.execute(
                "SELECT * FROM pipeline_runs WHERE pipeline_name = ? ORDER BY started_at DESC",
                [pipeline_name],
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM pipeline_runs ORDER BY started_at DESC"
            ).fetchall()
        cols = [
            "run_id",
            "pipeline_name",
            "partition_date",
            "status",
            "started_at",
            "completed_at",
            "input_rows",
            "output_rows",
            "error_message",
        ]
        return [dict(zip(cols, r, strict=True)) for r in rows]
