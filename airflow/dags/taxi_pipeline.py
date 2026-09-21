"""Apache Airflow DAG for the taxi analytics pipeline.

Task graph:

    check_source
        -> ingest_raw
        -> validate_raw
        -> normalize_staging
        -> enrich_weather
        -> build_marts
        -> run_quality_checks
        -> publish_metrics
        -> load_postgres

Each partition task shells out to the ``pipeline`` CLI. The DAG is scheduled
daily; each run processes its logical date (``{{ ds }}``) or an explicit range
supplied via ``--conf '{"start_date": "...", "end_date": "..."}'`` (backfill).
"""

from __future__ import annotations

from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PIPELINE_DIR = "/opt/pipeline"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
    "start_date": datetime(2025, 1, 1),
}


def _bash(task_id: str, command: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=command,
        cwd=PIPELINE_DIR,
        dag=dag,
    )


def _check_sources(**context) -> None:
    """Verify source URLs are reachable before ingestion begins."""
    from pipeline.config import load_sources

    sources = load_sources()
    for name, meta in sources.items():
        url = meta.get("url")
        if not url:
            continue
        # Only probe static URLs (weather is a base URL with query params).
        if "{" in url or name == "weather":
            continue
        resp = requests.head(url, timeout=30)
        resp.raise_for_status()
        context["task_instance"].log.info("source_ok source=%s status=%s", name, resp.status_code)


with DAG(
    dag_id="taxi_analytics_pipeline",
    default_args=default_args,
    description="Ingest, validate, normalize, enrich, and serve NYC taxi + weather data.",
    schedule_interval="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["taxi", "analytics"],
    params={
        "start_date": "{{ ds }}",
        "end_date": "{{ ds }}",
    },
) as dag:

    check_source = PythonOperator(
        task_id="check_source",
        python_callable=_check_sources,
        provide_context=True,
    )

    ingest_raw = _bash(
        "ingest_raw",
        "python -m pipeline ingest --start-date '{{ params.start_date }}' --end-date '{{ params.end_date }}'",
    )
    validate_raw = _bash(
        "validate_raw",
        "python -m pipeline validate --start-date '{{ params.start_date }}' --end-date '{{ params.end_date }}'",
    )
    normalize_staging = _bash(
        "normalize_staging",
        "python -m pipeline normalize --start-date '{{ params.start_date }}' --end-date '{{ params.end_date }}'",
    )
    enrich_weather = _bash(
        "enrich_weather",
        "python -m pipeline enrich --start-date '{{ params.start_date }}' --end-date '{{ params.end_date }}'",
    )
    build_marts = _bash(
        "build_marts",
        "dbt run --project-dir dbt --profiles-dir dbt",
    )
    run_quality_checks = _bash(
        "run_quality_checks",
        "dbt test --project-dir dbt --profiles-dir dbt",
    )
    publish_metrics = _bash(
        "publish_metrics",
        "python -m pipeline metrics",
    )
    load_postgres = _bash(
        "load_postgres",
        "python -m pipeline load",
    )

    (
        check_source
        >> ingest_raw
        >> validate_raw
        >> normalize_staging
        >> enrich_weather
        >> build_marts
        >> run_quality_checks
        >> publish_metrics
        >> load_postgres
    )
