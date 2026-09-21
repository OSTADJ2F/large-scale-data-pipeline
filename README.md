# Large-Scale Data Pipeline

A production-style **transportation analytics platform** that ingests public
NYC taxi-trip data, enriches it with historical weather data, and exposes
analytical dashboards and query APIs.

## What it does

The pipeline ingests **NYC Yellow Taxi** trips (Parquet) and **Open-Meteo**
historical weather, validates and normalizes the records, enriches them with
weather context, builds analytical data marts, and serves them through a
Streamlit dashboard and a FastAPI query API.

Pipeline questions answered:

- Which zones have the highest demand?
- How does weather affect trip duration?
- What are the busiest hours?
- Which routes generate the most revenue?
- How has demand changed over time?
- How many invalid or suspicious records were detected?

## Stack

| Layer            | Technology                                  |
| ---------------- | ------------------------------------------- |
| Processing       | Python 3.10, Polars, PySpark (later)        |
| Orchestration    | Apache Airflow (Docker)                     |
| Storage          | MinIO (S3-compatible), local filesystem     |
| Warehousing      | DuckDB (analytics), PostgreSQL (serving)    |
| Transformations  | dbt-duckdb                                  |
| Data quality     | Pandera + custom validation rules           |
| Dashboard / API  | Streamlit / FastAPI                         |
| Observability    | structlog, Prometheus (optional)            |
| Deployment       | Docker Compose, GitHub Actions              |

## Repository layout

```
pipeline/      Python package (ingest, validate, normalize, enrich, incremental)
dbt/           dbt-duckdb transformation models and tests
airflow/       Airflow DAGs and image
api/           FastAPI query API
dashboard/     Streamlit dashboard
config/        data-source manifest and settings
tests/         unit, integration, and end-to-end tests
data/          raw / staging / curated / marts (gitignored)
```

## Local setup

Prerequisites: Python 3.10, Docker Desktop (running), `git`.

```powershell
# 1. Create a virtualenv and install dependencies
.\make.ps1 setup
#   (or on Linux/macOS: make setup)

# 2. Copy environment configuration
Copy-Item .env.example .env   # fill in any overrides

# 3. Start infrastructure (PostgreSQL + MinIO)
docker compose up -d

# 4. Verify the environment
.\make.ps1 doctor
```

## Commands

```powershell
.\make.ps1 test        # run tests
.\make.ps1 lint        # lint
.\make.ps1 format      # format
.\make.ps1 ingest      # ingest raw data (-Start / -End)
.\make.ps1 pipeline    # run the full pipeline (-Start / -End)
.\make.ps1 dashboard   # launch Streamlit dashboard
.\make.ps1 api         # launch FastAPI query API
```

On Linux/macOS (or Git Bash), the same targets exist as `make <target>`.

## Configuration

Runtime configuration comes from environment variables (`.env`), with defaults
in `config/settings.yaml`. The data-source manifest lives in
`config/sources.yaml` (URL templates, formats, partition columns).

## License

[MIT](./LICENSE)
