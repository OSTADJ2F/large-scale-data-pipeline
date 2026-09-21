from pipeline.config import get_settings, load_sources, postgres_dsn


def test_get_settings_defaults():
    s = get_settings()
    assert s.environment == "local"
    assert s.storage_backend == "local"
    assert s.workers == 4


def test_settings_ensure_dirs(tmp_path):
    s = get_settings().model_copy(deep=True)
    s.data_dir = tmp_path / "data"
    s.ensure_dirs()
    assert (tmp_path / "data" / "raw").exists()
    assert (tmp_path / "data" / "marts").exists()


def test_load_sources_manifest():
    sources = load_sources()
    assert "taxi_trips" in sources
    assert "weather" in sources
    assert sources["taxi_trips"]["format"] == "parquet"


def test_postgres_dsn():
    s = get_settings().model_copy(deep=True)
    s.postgres_user = "u"
    s.postgres_password = "p"
    s.postgres_host = "h"
    s.postgres_port = 5433
    s.postgres_db = "d"
    assert postgres_dsn(s) == "postgresql://u:p@h:5433/d"
