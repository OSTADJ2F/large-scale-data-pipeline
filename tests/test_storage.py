import pytest
from pipeline.storage import Storage, StorageError


@pytest.fixture()
def storage(tmp_path):
    from pipeline.config import Settings

    s = Settings(data_dir=tmp_path / "data", storage_backend="local")
    return Storage(s)


def test_write_and_read_bytes(storage):
    storage.write_bytes("raw/foo.txt", b"hello")
    assert storage.exists("raw/foo.txt")
    assert storage.read_bytes("raw/foo.txt") == b"hello"


def test_no_overwrite_by_default(storage):
    storage.write_bytes("raw/foo.txt", b"first")
    with pytest.raises(StorageError):
        storage.write_bytes("raw/foo.txt", b"second")


def test_allow_overwrite(storage):
    storage.write_bytes("raw/foo.txt", b"first")
    storage.write_bytes("raw/foo.txt", b"second", overwrite=True)
    assert storage.read_bytes("raw/foo.txt") == b"second"


def test_checksum(storage):
    storage.write_bytes("raw/foo.txt", b"abc")
    assert storage.checksum("raw/foo.txt") == (
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )


def test_list_keys(storage):
    storage.write_bytes("raw/a.txt", b"1")
    storage.write_bytes("raw/b/c.txt", b"2")
    keys = storage.list_keys("raw/")
    assert set(keys) == {"raw/a.txt", "raw/b/c.txt"}
