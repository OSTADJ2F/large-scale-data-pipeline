# Production service image (API + dashboard + CLI).
# Build from the repository root:
#   docker build -t taxi-analytics .
FROM python:3.10-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY pipeline ./pipeline
COPY api ./api
COPY dashboard ./dashboard
COPY config ./config
COPY dbt ./dbt

RUN pip install -e ".[dev]" \
    && pip install -r requirements-dbt.txt

EXPOSE 8000 8501

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8000/healthz || exit 1

CMD ["uvicorn", "api.app:app", "--host", "0.0.0.0", "--port", "8000"]
