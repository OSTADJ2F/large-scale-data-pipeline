from datetime import date
from unittest.mock import patch

import pytest
from pipeline.config import Settings
from pipeline.ingest.downloader import DownloadError
from pipeline.ingest.runner import Ingestor, iter_months


@pytest.fixture()
def ingestor(tmp_path):
    s = Settings(data_dir=tmp_path / "data", storage_backend="local", max_retries=0)
    return Ingestor(s)


def test_iter_months():
    months = list(iter_months(date(2025, 1, 15), date(2025, 3, 1)))
    assert months == [(2025, 1), (2025, 2), (2025, 3)]


def _fake_download(url, dest, retries=0, delay=0):
    dest.write_bytes(b"a,b,c\n1,2,3\n")


def test_download_one_records_manifest(ingestor):
    with patch("pipeline.ingest.runner.download_file", side_effect=_fake_download):
        result = ingestor._download_one(
            "taxi_zones", None, "http://example/zones.csv", "raw/taxi_zones/taxi_zone_lookup.csv"
        )
    assert result.status == "success"
    assert ingestor.storage.exists("raw/taxi_zones/taxi_zone_lookup.csv")
    rec = ingestor.meta.get_ingestion("taxi_zones", None)
    assert rec["status"] == "success"
    assert rec["row_count"] == 1
    ingestor.meta.close()


def test_download_one_idempotent(ingestor):
    calls = []

    def fake(url, dest, retries=0, delay=0):
        calls.append(url)
        dest.write_bytes(b"a,b\n1,2\n")

    with patch("pipeline.ingest.runner.download_file", side_effect=fake):
        ingestor._download_one(
            "taxi_zones", None, "http://example/z.csv", "raw/taxi_zones/taxi_zone_lookup.csv"
        )
        ingestor._download_one(
            "taxi_zones", None, "http://example/z.csv", "raw/taxi_zones/taxi_zone_lookup.csv"
        )
    assert len(calls) == 1
    ingestor.meta.close()


def test_download_one_failure_recorded(ingestor):
    with patch(
        "pipeline.ingest.runner.download_file",
        side_effect=DownloadError("boom"),
    ):
        result = ingestor._download_one(
            "taxi_zones", None, "http://example/z.csv", "raw/taxi_zones/taxi_zone_lookup.csv"
        )
    assert result.status == "failed"
    assert "boom" in result.error
    rec = ingestor.meta.get_ingestion("taxi_zones", None)
    assert rec["status"] == "failed"
    ingestor.meta.close()
