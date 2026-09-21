"""Storage abstraction over local filesystem and S3-compatible object storage.

The raw layer is treated as immutable: callers should write to a unique key and
never overwrite existing objects. The backend is selected via
`Settings.storage_backend`.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import IO

from pipeline.config import Settings, get_settings


class StorageError(RuntimeError):
    pass


@dataclass
class Storage:
    """Provides read/write/exists/checksum across backends."""

    settings: Settings

    def __post_init__(self) -> None:
        if self.settings.storage_backend == "local":
            self.settings.ensure_dirs()
            self._s3 = None
        elif self.settings.storage_backend in {"minio", "s3"}:
            self._s3 = self._build_s3_client()
        else:
            raise StorageError(f"Unknown storage backend: {self.settings.storage_backend}")

    def _build_s3_client(self):
        import boto3

        kwargs = {}
        if self.settings.storage_backend == "minio" or self.settings.aws_endpoint_url:
            endpoint = self.settings.aws_endpoint_url or (
                f"{'https' if self.settings.minio_secure else 'http'}://"
                f"{self.settings.minio_endpoint}"
            )
            kwargs["endpoint_url"] = endpoint
            kwargs["aws_access_key_id"] = (
                self.settings.aws_access_key_id or self.settings.minio_access_key
            )
            kwargs["aws_secret_access_key"] = (
                self.settings.aws_secret_access_key or self.settings.minio_secret_key
            )
        return boto3.client("s3", region_name=self.settings.aws_region, **kwargs)

    def bucket(self) -> str:
        return self.settings.minio_bucket

    def _local_path(self, key: str) -> Path:
        # Keys are relative to data_dir for the local backend.
        return self.settings.data_dir / key

    def write_bytes(self, key: str, data: bytes, overwrite: bool = False) -> str:
        if not overwrite and self.exists(key):
            raise StorageError(f"Refusing to overwrite existing object: {key}")
        if self._s3 is not None:
            self._s3.put_object(Bucket=self.bucket(), Key=key, Body=data)
        else:
            path = self._local_path(key)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        return key

    def write_file(self, key: str, src: Path, overwrite: bool = False) -> str:
        return self.write_bytes(key, src.read_bytes(), overwrite=overwrite)

    def read_bytes(self, key: str) -> bytes:
        if self._s3 is not None:
            obj = self._s3.get_object(Bucket=self.bucket(), Key=key)
            return obj["Body"].read()
        return self._local_path(key).read_bytes()

    def open(self, key: str, mode: str = "rb") -> IO:
        if self._s3 is not None:
            raise StorageError("Streaming not supported on S3 backend; use read_bytes.")
        return self._local_path(key).open(mode)

    def exists(self, key: str) -> bool:
        if self._s3 is not None:
            try:
                self._s3.head_object(Bucket=self.bucket(), Key=key)
                return True
            except Exception:
                return False
        return self._local_path(key).exists()

    def list_keys(self, prefix: str = "") -> list[str]:
        if self._s3 is not None:
            paginator = self._s3.get_paginator("list_objects_v2")
            keys: list[str] = []
            for page in paginator.paginate(Bucket=self.bucket(), Prefix=prefix):
                keys.extend(obj["Key"] for obj in page.get("Contents", []))
            return keys
        base = self._local_path(prefix)
        if not base.exists():
            return []
        return [p.relative_to(self.settings.data_dir).as_posix() for p in base.rglob("*") if p.is_file()]

    def checksum(self, key: str, algorithm: str = "sha256") -> str:
        if self._s3 is not None:
            data = self.read_bytes(key)
            return hashlib.new(algorithm, data).hexdigest()
        h = hashlib.new(algorithm)
        with self._local_path(key).open("rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()


def get_storage(settings: Settings | None = None) -> Storage:
    return Storage(settings or get_settings())
