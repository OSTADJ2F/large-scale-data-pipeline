# Build from the repository root:
#   docker build -f airflow/Dockerfile -t taxi-analytics-airflow .
FROM apache/airflow:2.10.5-python3.10

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

USER airflow
WORKDIR /opt/pipeline

COPY --chown=airflow:root . /opt/pipeline

RUN pip install --no-cache-dir -e "/opt/pipeline[dev]" \
    && pip install --no-cache-dir -r /opt/pipeline/requirements-dbt.txt
