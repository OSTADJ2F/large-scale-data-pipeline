"""Command-line interface for the pipeline.

Usage:
    python -m pipeline <command> [options]
    pipeline <command> [options]
"""

from __future__ import annotations

from datetime import date

import typer

from pipeline.config import get_settings, load_sources, postgres_dsn
from pipeline.logging_setup import configure_logging, get_logger

app = typer.Typer(
    name="pipeline",
    help="Transportation analytics pipeline (taxi trips + weather).",
    no_args_is_help=True,
)


def _date(value: str) -> date:
    return date.fromisoformat(value)


@app.callback()
def _main(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
) -> None:
    settings = get_settings()
    configure_logging("DEBUG" if verbose else settings.log_level)


@app.command()
def doctor() -> None:
    """Check configuration, storage, and database connectivity."""
    log = get_logger(__name__)
    settings = get_settings()
    sources = load_sources()

    log.info("environment", environment=settings.environment)
    log.info("storage", backend=settings.storage_backend, data_dir=str(settings.data_dir))
    log.info("sources", names=list(sources.keys()))
    log.info("postgres", dsn=postgres_dsn(settings))
    typer.echo(f"Environment: {settings.environment}")
    typer.echo(f"Storage backend: {settings.storage_backend}")
    typer.echo(f"Data directory: {settings.data_dir}")
    typer.echo(f"Registered sources: {', '.join(sources.keys())}")


@app.command()
def ingest(
    start_date: str = typer.Option(None, "--start-date", help="YYYY-MM-DD"),
    end_date: str = typer.Option(None, "--end-date", help="YYYY-MM-DD"),
) -> None:
    """Download and store raw source files."""
    from pipeline.ingest.runner import run_ingest

    run_ingest(start_date, end_date)


@app.command()
def validate(
    start_date: str = typer.Option(None, "--start-date", help="YYYY-MM-DD"),
    end_date: str = typer.Option(None, "--end-date", help="YYYY-MM-DD"),
) -> None:
    """Validate raw data and split into valid/invalid/quarantined."""
    from pipeline.validate.runner import run_validate

    run_validate(start_date, end_date)


@app.command()
def normalize(
    start_date: str = typer.Option(None, "--start-date", help="YYYY-MM-DD"),
    end_date: str = typer.Option(None, "--end-date", help="YYYY-MM-DD"),
) -> None:
    """Normalize validated data into partitioned staging Parquet."""
    from pipeline.normalize.runner import run_normalize

    run_normalize(start_date, end_date)


@app.command()
def enrich(
    start_date: str = typer.Option(None, "--start-date", help="YYYY-MM-DD"),
    end_date: str = typer.Option(None, "--end-date", help="YYYY-MM-DD"),
) -> None:
    """Enrich staging data with historical weather."""
    from pipeline.enrich.runner import run_enrich

    run_enrich(start_date, end_date)


@app.command()
def run(
    date: str = typer.Option(None, "--date", help="Single partition date YYYY-MM-DD"),
    start_date: str = typer.Option(None, "--start-date", help="YYYY-MM-DD"),
    end_date: str = typer.Option(None, "--end-date", help="YYYY-MM-DD"),
) -> None:
    """Run the full pipeline for a date or date range."""
    from pipeline.incremental.runner import run_pipeline

    run_pipeline(date=date, start_date=start_date, end_date=end_date)


@app.command()
def backfill(
    start_date: str = typer.Option(..., "--start-date", help="YYYY-MM-DD"),
    end_date: str = typer.Option(..., "--end-date", help="YYYY-MM-DD"),
) -> None:
    """Reprocess a historical date range."""
    from pipeline.incremental.runner import run_backfill

    run_backfill(start_date, end_date)


@app.command()
def quality() -> None:
    """Run data-quality checks."""
    from pipeline.quality.runner import run_quality

    run_quality()


@app.command()
def load() -> None:
    """Load analytical marts into the PostgreSQL serving database."""
    from pipeline.serving.loader import ensure_schema, load_marts

    ensure_schema()
    counts = load_marts()
    typer.echo("Loaded marts into PostgreSQL:")
    for mart, rows in counts.items():
        typer.echo(f"  {mart}: {rows} rows")


@app.command()
def metrics() -> None:
    """Render pipeline metrics in Prometheus text format."""
    from pipeline.metrics import run_metrics

    run_metrics()


if __name__ == "__main__":
    app()
