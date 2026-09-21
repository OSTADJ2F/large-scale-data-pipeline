"""Application configuration.

Configuration is assembled from (in priority order):
  1. Environment variables
  2. config/settings.yaml
  3. Built-in defaults

`config/sources.yaml` declares the data-source manifest (URLs, formats,
partition columns) and is loaded separately via `load_sources`.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    """Runtime settings backed by environment variables."""

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    environment: str = "local"
    log_level: str = "INFO"

    default_start_date: str = "2025-01-01"
    default_end_date: str = "2025-01-31"
    workers: int = 4
    max_retries: int = 3
    retry_delay_seconds: float = 5.0

    storage_backend: str = "local"
    data_dir: Path = Path("./data")

    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "raw"
    minio_secure: bool = False

    aws_endpoint_url: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_region: str = "us-east-1"

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "taxi"
    postgres_user: str = "taxi"
    postgres_password: str = "taxi"

    duckdb_path: Path = Path("./data/analytics.duckdb")

    dbt_profiles_dir: Path = Path("./dbt")
    dbt_project_dir: Path = Path("./dbt")
    dbt_target: str = "dev"

    api_host: str = "0.0.0.0"
    api_port: int = 8000
    dashboard_port: int = 8501

    prometheus_enabled: bool = False

    taxi_trips_url: str = (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    )
    taxi_zones_url: str = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
    weather_url: str = "https://archive-api.open-meteo.com/v1/archive"
    weather_latitude: float = 40.7143
    weather_longitude: float = -74.0060

    # Overrides merged on top of config/settings.yaml below.
    settings_file: Path = Field(default=PROJECT_ROOT / "config" / "settings.yaml", exclude=True)

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def staging_dir(self) -> Path:
        return self.data_dir / "staging"

    @property
    def curated_dir(self) -> Path:
        return self.data_dir / "curated"

    @property
    def marts_dir(self) -> Path:
        return self.data_dir / "marts"

    def ensure_dirs(self) -> None:
        for d in (self.raw_dir, self.staging_dir, self.curated_dir, self.marts_dir):
            d.mkdir(parents=True, exist_ok=True)


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load settings, letting config/settings.yaml provide defaults below env vars."""
    yaml_values = _load_yaml(PROJECT_ROOT / "config" / "settings.yaml")
    return Settings(**yaml_values)


@lru_cache(maxsize=1)
def load_sources() -> dict[str, Any]:
    """Load the data-source manifest from config/sources.yaml."""
    manifest = _load_yaml(PROJECT_ROOT / "config" / "sources.yaml")
    return manifest.get("sources", {})


def postgres_dsn(settings: Settings | None = None) -> str:
    s = settings or get_settings()
    return (
        f"postgresql://{s.postgres_user}:{s.postgres_password}"
        f"@{s.postgres_host}:{s.postgres_port}/{s.postgres_db}"
    )
