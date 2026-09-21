# Large-Scale Data Pipeline

A production-style **transportation analytics platform** that ingests public NYC
taxi-trip data, enriches it with historical weather, builds analytical data
marts, and serves them through a dashboard and query API.

It answers questions such as:

- Which zones have the highest demand?
- How does weather affect trip duration?
- What are the busiest hours?
- Which routes generate the most revenue?
- How has demand changed over time?
- How many invalid or suspicious records were detected?

---

## Architecture

```text
                    NYC TLC (Parquet)        Open-Meteo (JSON)
                          │                        │
                          ▼                        ▼
                ┌─────────────────────────────────────┐
                │        Raw ingestion layer          │   idempotent, checksummed
                └─────────────────────────────────────┘
                          │
                          ▼
                ┌─────────────────────────────────────┐
                │   Immutable object storage (MinIO)  │   data/raw (append-only)
                └─────────────────────────────────────┘
                          │
                          ▼
                ┌─────────────────────────────────────┐
                │  Validation & normalization         │   Pandera + custom rules
                │  valid / invalid / quarantined      │
                └─────────────────────────────────────┘
                          │
                          ▼
                ┌─────────────────────────────────────┐
                │   Cleaned Parquet (staging)         │   partitioned by pickup_date
                └─────────────────────────────────────┘
                          │
                          ▼
                ┌─────────────────────────────────────┐
                │   Weather enrichment                │   nearest-hour join
                └─────────────────────────────────────┘
                          │
                          ▼
                ┌─────────────────────────────────────┐
                │   Analytical marts (dbt-duckdb)     │   daily/hourly/route/weather
                └─────────────────────────────────────┘
                          │
                          ▼
                ┌─────────────────────────────────────┐
                │   Query DB (PostgreSQL)             │   serving marts
                └─────────────────────────────────────┘
                          │
                 ┌────────┴────────┐
                 ▼                 ▼
        Dashboard (Streamlit)   API (FastAPI)
```

**Storage layout** (gitignored):

```text
data/
├── raw/          # immutable source files (taxi, weather, zones)
├── validated/    # valid/ invalid/ quarantined outputs + reports
├── staging/      # normalized, date-partitioned Parquet
├── curated/      # weather-enriched Parquet
└── marts/        # analytical aggregates (also in DuckDB + PostgreSQL)
```

## Technology choices

| Layer             | Technology                          | Why |
| ----------------- | ----------------------------------- | --- |
| Processing        | Python 3.10, Polars                 | Fast columnar, in-memory, lazy |
| Distributed       | PySpark (optional)                  | Same transform logic at scale |
| Orchestration     | Apache Airflow (Docker)             | DAGs, retries, backfills, history |
| Storage           | MinIO (S3-compatible) / local FS    | Immutable object store |
| Warehousing       | DuckDB (analytics)                  | Zero-config columnar analytics |
| Serving           | PostgreSQL                          | Reliable analytical serving |
| Transformations   | dbt-duckdb                          | Versioned, tested, documented SQL |
| Data quality      | Pandera + custom rules + dbt tests  | Schema + rule + referential checks |
| Dashboard / API   | Streamlit / FastAPI                 | Interactive + programmatic access |
| Observability     | structlog + Prometheus + Grafana    | Logs, metrics, alerts |

## Quick start

Prerequisites: Python 3.10, Docker Desktop (running), `git`.

```powershell
# 1. Create a virtualenv and install dependencies
.\make.ps1 setup            # Linux/macOS: make setup

# 2. Configure environment
Copy-Item .env.example .env

# 3. Start infrastructure (PostgreSQL + MinIO)
docker compose up -d

# 4. Verify configuration
.\make.ps1 doctor
```

## Run the app

After the quick start, run the pipeline for a small date range to populate the
serving database (skip this if data is already loaded), then start everything:

```powershell
# Populate data (one month is enough to explore the dashboard)
python -m pipeline run --start-date 2025-01-01 --end-date 2025-01-31
```

Then, with a single command:

```powershell
.\make.ps1 serve
```

`serve` starts PostgreSQL/MinIO (if needed), the FastAPI query API, and the
Streamlit dashboard, waits until both are ready, and runs smoke checks.

Open in a browser:

| Service        | URL                             |
| -------------- | ------------------------------- |
| Dashboard      | http://localhost:8501           |
| API docs       | http://localhost:8000/docs      |
| API metrics    | http://localhost:8000/metrics   |

Manage the running services:

```powershell
.\make.ps1 status    # show what is running
.\make.ps1 smoke     # verify API, dashboard, and database
.\make.ps1 stop      # stop the API and dashboard
```

Equivalent cross-platform commands (any OS):

```bash
python -m scripts.dev start     # start + smoke-test
python -m scripts.dev status
python -m scripts.dev smoke
python -m scripts.dev stop
```

Service logs are written to `logs/api.log` and `logs/dashboard.log`.

### Manual start (two terminals)

```powershell
# terminal 1
.venv\Scripts\python -m uvicorn api.app:app --reload
# terminal 2
.venv\Scripts\python -m streamlit run dashboard/app.py
```

## Pipeline commands

```powershell
# Ingest raw data for a date range (idempotent)
python -m pipeline ingest --start-date 2025-01-01 --end-date 2025-01-31

# Validate and split valid / invalid / quarantined
python -m pipeline validate --start-date 2025-01-01 --end-date 2025-01-31

# Normalize into partitioned staging Parquet
python -m pipeline normalize --start-date 2025-01-01 --end-date 2025-01-31

# Enrich with weather
python -m pipeline enrich --start-date 2025-01-01 --end-date 2025-01-31

# Build marts (dbt) + run data tests
python -m pipeline quality

# Load marts into PostgreSQL
python -m pipeline load

# Run the full pipeline for a single date or range (incremental, idempotent)
python -m pipeline run --date 2025-01-15
python -m pipeline run --start-date 2025-01-01 --end-date 2025-04-30

# Backfill (force reprocess) a historical range
python -m pipeline backfill --start-date 2024-01-01 --end-date 2024-12-31

# Operational checks
python -m pipeline metrics     # Prometheus text metrics
python -m pipeline alerts      # run alert rules
```

Equivalent `make` / `make.ps1` targets: `setup`, `test`, `lint`, `format`,
`doctor`, `ingest`, `transform`, `pipeline`, `dashboard`, `api`, `up`, `down`.

## Data sources

| Source   | Format | Description |
| -------- | ------ | ----------- |
| `taxi_trips` | Parquet | NYC TLC Yellow Taxi trips (monthly files) |
| `taxi_zones` | CSV    | Taxi-zone id -> borough/zone lookup |
| `weather`    | JSON   | Open-Meteo historical hourly weather for NYC |

Sources are configured in [`config/sources.yaml`](config/sources.yaml) and can be
overridden via environment variables.

## Schema

The canonical trip schema (staging/curated) includes:

| Column | Type | Description |
| ------ | ---- | ----------- |
| `pickup_datetime` / `dropoff_datetime` | timestamp | naive America/New_York |
| `pickup_date` / `pickup_hour` / `pickup_day_of_week` | date/int/int | derived |
| `pickup_location_id` / `dropoff_location_id` | int | TLC taxi-zone id |
| `passenger_count` | int | nulls defaulted to 0 |
| `trip_distance` | float | miles |
| `trip_duration_seconds` / `average_speed` | float | derived |
| `fare_amount` / `tip_amount` / `total_charge` | float | currency (USD) |
| `payment_type` | string | `credit_card`, `cash`, ... |
| `is_airport_trip` | bool | derived |
| `temperature` / `precipitation` / `wind_speed` / `weather_condition` | various | weather |

Time zone: TLC and Open-Meteo timestamps are naive **America/New_York** local
time. Weather is joined by **nearest hour** (floor-to-hour on pickup time).

## Data quality

- **Pandera schema** validates column presence and types.
- **Correctness rules** flag: dropoff before pickup, negative distance/fare/tip,
  invalid passenger counts, invalid location IDs, invalid payment types.
- **Quarantine** captures critical nulls, exact duplicates, and outliers.
- **Thresholds** (`QUALITY_MAX_INVALID_RATIO`, `QUALITY_MAX_NULL_RATIO`) fail the
  pipeline when exceeded.
- **dbt tests** (21) enforce uniqueness, referential integrity, accepted values,
  and not-null constraints on every mart.

Validation reports are saved as JSON artifacts under `data/validated/trips/reports/`.

### Findings on real data (Jan–Apr 2025)

The pipeline surfaced real data-quality issues in the TLC feed:

- **`payment_type=0`** (up to 806k rows/month) — a non-standard code correlated
  with null `passenger_count`; treated as a valid `not_recorded` value rather
  than a correctness failure.
- **Negative fares** (~5% of rows) — genuine TLC adjustment/refund records,
  flagged as invalid and excluded from revenue marts.
- **Soft-null `passenger_count`** — defaulted to 0 during normalization.
- **Late-arriving trips** — monthly files contain a few trips from adjacent
  dates (e.g. `pickup_date=2024-12-31` inside the January file).

## Performance

Benchmarked on a single month (3.48M rows, 56 MB Parquet) on a local machine:

| Stage            | Time  |
| ---------------- | ----- |
| Read raw Parquet | 0.06s |
| Validate         | 2.59s |
| Normalize        | 0.03s |
| Enrich (join)    | 0.05s |
| Top-zones query  | 0.001s |

- Peak memory ~3.3 GB (in-memory columnar processing).
- Ingestion downloads multiple files in parallel (`WORKERS`).
- End-to-end run processed **15.2M raw rows** across 4 months (Jan–Apr 2025).

**Scaling**: for datasets larger than available memory, the same transformation
logic is available as a PySpark job (`scripts/spark_job.py`, `pipeline/spark/`)
that can run on any Spark cluster; results are consistent with the local path.

## Observability

- Structured JSON logs via `structlog`.
- Prometheus metrics at `GET /metrics` (rows ingested/rejected/transformed, run
  duration, job failures, storage, query latency, last-success timestamp).
- Alert rules for pipeline failure, no data, high failure rate, and slow runs
  (`monitoring/alert_rules.yml`).
- Optional Grafana dashboard via `docker compose --profile monitoring up -d`.

## Deployment

### Local services

```powershell
docker compose up -d                              # Postgres + MinIO
docker compose --profile monitoring up -d         # + Prometheus + Grafana
```

### Airflow

```powershell
docker compose -f airflow/docker-compose.yaml up -d
# UI at http://localhost:8080 (admin / admin)
```

The DAG (`airflow/dags/taxi_pipeline.py`) schedules daily and supports backfills
via `--conf '{"start_date":"...","end_date":"..."}'`.

### Production containers

```powershell
docker build -t taxi-analytics .                  # API + dashboard + CLI image
docker build -f airflow/Dockerfile -t taxi-analytics-airflow .
```

### Backups

```powershell
# PostgreSQL
docker exec pipeline-postgres pg_dump -U taxi taxi > backup.sql

# Data (raw/staging/curated/marts + metadata)
tar -czf data-backup.tar.gz data/
```

## Testing

```powershell
.\make.ps1 test        # 65 unit + integration + end-to-end tests
.\make.ps1 lint
.\make.ps1 format
```

Coverage includes unit tests (parsers, normalizers, rules, config), integration
tests (object storage, validation, weather joins), data-quality tests, and an
end-to-end test over a small fixture dataset (no external downloads). CI runs
lint, tests, and `dbt parse` on every push.

## Known limitations

- Naive America/New_York timestamps (no UTC conversion) — documented and kept
  consistent across trips and weather.
- Raw taxi files are monthly; a month's file may contain a few trips from
  adjacent days (late-arriving data) — captured when that month is processed.
- PySpark path requires a Java runtime and is not exercised in the default CI.
- In-memory Polars processing is bounded by available RAM (~3 GB per ~3.5M rows);
  use the Spark path for larger scales.

## License

[MIT](./LICENSE)
