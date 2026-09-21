"""Streamlit dashboard for the taxi analytics platform.

Queries analytical marts in PostgreSQL (plus the local DuckDB metadata for
pipeline run status). Run with `streamlit run dashboard/app.py`.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st
from pipeline.config import get_settings, postgres_dsn
from pipeline.metadata import MetadataStore
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Taxi Analytics", layout="wide")


@st.cache_resource
def pg_engine():
    settings = get_settings()
    return create_engine(
        postgres_dsn(settings).replace("postgresql://", "postgresql+psycopg://", 1)
    )


def query(sql: str, params: dict | None = None) -> pd.DataFrame:
    with pg_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def latest_run() -> dict | None:
    settings = get_settings()
    meta = MetadataStore(settings.data_dir / "pipeline_meta.duckdb")
    try:
        runs = meta.list_runs()
    finally:
        meta.close()
    successful = [r for r in runs if r["status"] == "success"]
    return successful[0] if successful else (runs[0] if runs else None)


def payment_breakdown() -> pd.DataFrame:
    settings = get_settings()
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        return con.execute(
            "select payment_type, count(*) as trip_count from main_marts.fct_trips "
            "group by 1 order by 2 desc"
        ).fetch_df()
    finally:
        con.close()


def _date_filter(start: pd.Timestamp, end: pd.Timestamp) -> tuple[str, dict]:
    return (
        "date >= :start_date AND date <= :end_date",
        {"start_date": start.date(), "end_date": end.date()},
    )


st.title("NYC Taxi + Weather Analytics")

run = latest_run()
if run:
    st.caption(
        f"Latest pipeline run: partition={run['partition_date']} status={run['status']} "
        f"input={run['input_rows']:,} output={run['output_rows']:,}"
    )

# --- Filters -------------------------------------------------------------
col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start date", pd.to_datetime("2025-01-01"))
with col2:
    end_date = st.date_input("End date", pd.to_datetime("2025-04-30"))

date_clause, date_params = _date_filter(start_date, end_date)

# --- KPIs ----------------------------------------------------------------
kpi = query(
    "SELECT SUM(trip_count) AS trips, SUM(total_revenue) AS revenue, "
    "AVG(average_trip_duration) AS avg_duration "
    f"FROM marts.daily_demand WHERE {date_clause}",
    date_params,
)
c1, c2, c3 = st.columns(3)
if not kpi.empty and pd.notna(kpi.iloc[0]["trips"]):
    c1.metric("Trips", f"{int(kpi.iloc[0]['trips']):,}")
    c2.metric("Total revenue", f"${kpi.iloc[0]['revenue']:,.0f}")
    c3.metric("Avg trip duration (s)", f"{kpi.iloc[0]['avg_duration']:.0f}")
else:
    st.info("No data for the selected range.")

# --- Charts --------------------------------------------------------------
st.subheader("Trips by day")
daily = query(
    "SELECT date, SUM(trip_count) AS trips FROM marts.daily_demand "
    f"WHERE {date_clause} GROUP BY date ORDER BY date",
    date_params,
)
st.line_chart(daily.set_index("date"), use_container_width=True)

left, right = st.columns(2)
with left:
    st.subheader("Trips by hour")
    hourly = query(
        "SELECT hour, SUM(trip_count) AS trips FROM marts.hourly_demand GROUP BY hour ORDER BY hour"
    )
    st.bar_chart(hourly.set_index("hour"), use_container_width=True)

with right:
    st.subheader("Top pickup zones")
    zones = query(
        "SELECT z.zone, SUM(d.trip_count) AS trips "
        "FROM marts.daily_demand d JOIN marts.dim_zones z ON d.pickup_location_id = z.location_id "
        f"WHERE {date_clause} GROUP BY z.zone ORDER BY trips DESC LIMIT 10",
        date_params,
    )
    st.bar_chart(zones.set_index("zone"), use_container_width=True)

st.subheader("Revenue trend")
revenue = query(
    "SELECT date, SUM(total_revenue) AS revenue FROM marts.daily_demand "
    f"WHERE {date_clause} GROUP BY date ORDER BY date",
    date_params,
)
st.line_chart(revenue.set_index("date"), use_container_width=True)

left, right = st.columns(2)
with left:
    st.subheader("Top routes by revenue")
    routes = query(
        "SELECT pu.zone AS pickup, doz.zone AS dropoff, r.trip_count, r.total_revenue "
        "FROM marts.route_performance r "
        "JOIN marts.dim_zones pu ON r.pickup_location_id = pu.location_id "
        "JOIN marts.dim_zones doz ON r.dropoff_location_id = doz.location_id "
        "ORDER BY r.total_revenue DESC LIMIT 10"
    )
    st.dataframe(routes, use_container_width=True)

with right:
    st.subheader("Weather impact")
    weather = query(
        "SELECT weather_condition, SUM(trip_count) AS trips, AVG(average_duration) AS avg_duration "
        "FROM marts.weather_impact GROUP BY weather_condition ORDER BY trips DESC"
    )
    st.dataframe(weather, use_container_width=True)

st.subheader("Payment type breakdown")
st.bar_chart(payment_breakdown().set_index("payment_type"), use_container_width=True)
