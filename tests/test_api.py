import pytest
from api.app import app
from fastapi.testclient import TestClient
from pipeline.config import postgres_dsn
from sqlalchemy import create_engine

client = TestClient(app)


def _db_available() -> bool:
    try:
        engine = create_engine(postgres_dsn().replace("postgresql://", "postgresql+psycopg://", 1))
        with engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1")
        return True
    except Exception:
        return False


requires_postgres = pytest.mark.skipif(not _db_available(), reason="PostgreSQL not available")


def test_healthz():
    assert client.get("/healthz").json() == {"status": "ok"}


@requires_postgres
def test_routes_endpoint():
    resp = client.get("/metrics/routes?limit=5")
    assert resp.status_code == 200
    assert len(resp.json()) == 5


@requires_postgres
def test_demand_filter_by_location():
    resp = client.get("/metrics/demand?start_date=2025-01-01&end_date=2025-01-31&location_id=132")
    assert resp.status_code == 200
    rows = resp.json()
    assert rows, "expected some rows for JFK (location 132)"
    assert all(r["pickup_location_id"] == 132 for r in rows)


@requires_postgres
def test_pipeline_runs_endpoint():
    resp = client.get("/pipeline/runs?limit=3")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_prometheus_metrics_endpoint():
    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert b"pipeline_" in resp.content
