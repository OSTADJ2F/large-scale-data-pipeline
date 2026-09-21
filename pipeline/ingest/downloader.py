"""HTTP download helpers with retries, checksums, and row counting."""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

import requests

from pipeline.logging_setup import get_logger

log = get_logger(__name__)

_CHUNK = 1024 * 1024


class DownloadError(RuntimeError):
    pass


def download_file(url: str, dest: Path, retries: int = 3, delay: float = 5.0) -> int:
    """Download ``url`` to ``dest`` (atomically) and return the byte size.

    Retries transient failures (non-200 responses and network errors) with
    exponential backoff before giving up.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    last_error: Exception | None = None

    for attempt in range(retries + 1):
        try:
            with requests.get(url, stream=True, timeout=(10, 300)) as resp:
                if resp.status_code != 200:
                    raise DownloadError(f"HTTP {resp.status_code} for {url}")
                size = 0
                with tmp.open("wb") as fh:
                    for chunk in resp.iter_content(chunk_size=_CHUNK):
                        fh.write(chunk)
                        size += len(chunk)
            tmp.replace(dest)
            log.info("downloaded", url=url, dest=str(dest), bytes=size)
            return size
        except (DownloadError, requests.RequestException) as exc:
            last_error = exc
            tmp.unlink(missing_ok=True)
            if attempt < retries:
                backoff = delay * (2**attempt)
                log.warning(
                    "download_retry",
                    url=url,
                    attempt=attempt + 1,
                    error=str(exc),
                    backoff=backoff,
                )
                time.sleep(backoff)
    raise DownloadError(str(last_error)) from last_error


def checksum_file(path: Path, algorithm: str = "sha256") -> str:
    h = hashlib.new(algorithm)
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def row_count(path: Path, fmt: str) -> int:
    """Count rows for a downloaded artifact by format."""
    if fmt == "parquet":
        import pyarrow.parquet as pq

        return pq.ParquetFile(path).metadata.num_rows
    if fmt == "csv":
        import polars as pl

        return pl.scan_csv(path).select(pl.len()).collect().item()
    if fmt == "json":
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        hourly = data.get("hourly", {})
        times = hourly.get("time", []) if isinstance(hourly, dict) else []
        return len(times) if times else len(data) if isinstance(data, list) else 1
    raise ValueError(f"Unsupported row-count format: {fmt}")
