from unittest.mock import patch

import pytest
from pipeline.ingest.downloader import DownloadError, checksum_file, download_file


class FakeResponse:
    def __init__(self, status_code=200, chunks=None):
        self.status_code = status_code
        self._chunks = chunks if chunks is not None else [b"hello world"]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def iter_content(self, chunk_size=None):
        yield from self._chunks


def test_download_success(tmp_path):
    dest = tmp_path / "f.txt"
    with patch("pipeline.ingest.downloader.requests.get", return_value=FakeResponse()):
        size = download_file("http://example/f.txt", dest, retries=0)
    assert size == len(b"hello world")
    assert dest.read_bytes() == b"hello world"


def test_download_http_error_raises(tmp_path):
    with (
        patch(
            "pipeline.ingest.downloader.requests.get", return_value=FakeResponse(status_code=404)
        ),
        pytest.raises(DownloadError),
    ):
        download_file("http://example/f.txt", tmp_path / "f.txt", retries=0)


def test_download_retries_then_succeeds(tmp_path):
    responses = iter(
        [
            FakeResponse(status_code=500),
            FakeResponse(chunks=[b"ok"]),
        ]
    )
    with patch(
        "pipeline.ingest.downloader.requests.get", side_effect=lambda *a, **k: next(responses)
    ):
        dest = tmp_path / "f.txt"
        size = download_file("http://example/f.txt", dest, retries=2, delay=0)
    assert dest.read_bytes() == b"ok"
    assert size == 2


def test_download_exhausts_retries(tmp_path):
    with (
        patch(
            "pipeline.ingest.downloader.requests.get", return_value=FakeResponse(status_code=500)
        ),
        pytest.raises(DownloadError),
    ):
        download_file("http://example/f.txt", tmp_path / "f.txt", retries=2, delay=0)


def test_checksum(tmp_path):
    p = tmp_path / "c.txt"
    p.write_bytes(b"abc")
    assert checksum_file(p) == ("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
