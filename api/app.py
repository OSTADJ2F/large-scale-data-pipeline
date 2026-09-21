"""FastAPI query API serving analytical marts from PostgreSQL."""

from __future__ import annotations

import time
from datetime import date
from typing import Any

from fastapi import FastAPI, Query, Request
from pipeline.config import get_settings, postgres_dsn
from pipeline.metadata import MetadataStore
from pipeline.metrics import REQUEST_LATENCY, render_metrics
from prometheus_client import CONTENT_TYPE_LATEST
from sqlalchemy import create_engine, text

app = FastAPI(title="Taxi Analytics API", version="0.1.0")

_engine = None


@app.middleware("http")
async def latency_middleware(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    REQUEST_LATENCY.labels(request.url.path).observe(time.perf_counter() - start)
    return response


def engine():
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_engine(
            postgres_dsn(settings).replace("postgresql://", "postgresql+psycopg://", 1)
        )
    return _engine


def _query(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    with engine().connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return [dict(r) for r in rows]


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics/demand")
def demand(
    start_date: date | None = None,
    end_date: date | None = None,
    location_id: int | None = None,
) -> list[dict[str, Any]]:
    sql = """
        SELECT date, pickup_location_id, trip_count, average_trip_duration,
               total_distance, total_revenue, average_fare
        FROM marts.daily_demand
        WHERE (:start_date IS NULL OR date >= :start_date)
          AND (:end_date IS NULL OR date <= :end_date)
          AND (:location_id IS NULL OR pickup_location_id = :location_id)
        ORDER BY date, pickup_location_id
    """
    return _query(sql, {"start_date": start_date, "end_date": end_date, "location_id": location_id})


@app.get("/metrics/revenue")
def revenue(
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[dict[str, Any]]:
    sql = """
        SELECT date, SUM(total_revenue) AS total_revenue, SUM(trip_count) AS trip_count
        FROM marts.daily_demand
        WHERE (:start_date IS NULL OR date >= :start_date)
          AND (:end_date IS NULL OR date <= :end_date)
        GROUP BY date ORDER BY date
    """
    return _query(sql, {"start_date": start_date, "end_date": end_date})


@app.get("/metrics/routes")
def routes(limit: int = Query(10, ge=1, le=1000)) -> list[dict[str, Any]]:
    sql = """
        SELECT pickup_location_id, dropoff_location_id, trip_count, average_distance,
               average_duration, total_revenue, average_tip
        FROM marts.route_performance
        ORDER BY total_revenue DESC
        LIMIT :limit
    """
    return _query(sql, {"limit": limit})


@app.get("/metrics/weather-impact")
def weather_impact(
    start_date: date | None = None,
    end_date: date | None = None,
    weather_condition: str | None = None,
) -> list[dict[str, Any]]:
    sql = """
        SELECT date, weather_condition, precipitation, trip_count,
               average_duration, average_fare
        FROM marts.weather_impact
        WHERE (:start_date IS NULL OR date >= :start_date)
          AND (:end_date IS NULL OR date <= :end_date)
          AND (:weather_condition IS NULL OR weather_condition = :weather_condition)
        ORDER BY date, weather_condition, precipitation
    """
    return _query(
        sql,
        {"start_date": start_date, "end_date": end_date, "weather_condition": weather_condition},
    )


@app.get("/pipeline/runs")
def pipeline_runs(limit: int = Query(50, ge=1, le=1000)) -> list[dict[str, Any]]:
    settings = get_settings()
    meta = MetadataStore(settings.data_dir / "pipeline_meta.duckdb")
    try:
        runs = meta.list_runs()
    finally:
        meta.close()
    return runs[:limit]


@app.get("/metrics")
def prometheus_metrics():
    from fastapi.responses import Response

    return Response(content=render_metrics(), media_type=CONTENT_TYPE_LATEST)
