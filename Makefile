# Makefile for the transportation analytics pipeline.
# On Windows PowerShell, use `.\make.ps1 <target>` instead.

PYTHON ?= python
PIP ?= pip

.PHONY: setup test lint format typecheck ingest transform pipeline dashboard api \
        up down clean doctor

setup:            ## Create virtualenv and install dependencies
	$(PYTHON) -m venv .venv
	.venv/Scripts/pip install --upgrade pip
	.venv/Scripts/pip install -e ".[dev]"

test:             ## Run the test suite
	.venv/Scripts/python -m pytest

lint:             ## Run the linter
	.venv/Scripts/python -m ruff check pipeline tests api dashboard

format:           ## Format the code
	.venv/Scripts/python -m ruff format pipeline tests api dashboard
	.venv/Scripts/python -m ruff check --fix pipeline tests api dashboard

typecheck:        ## Run mypy
	.venv/Scripts/python -m mypy pipeline

doctor:           ## Check configuration
	.venv/Scripts/python -m pipeline doctor

ingest:           ## Ingest raw data
	.venv/Scripts/python -m pipeline ingest --start-date $(START) --end-date $(END)

transform:        ## Run normalize + enrich
	.venv/Scripts/python -m pipeline run --start-date $(START) --end-date $(END)

pipeline:         ## Run the full pipeline
	.venv/Scripts/python -m pipeline run --start-date $(START) --end-date $(END)

dashboard:        ## Launch the Streamlit dashboard
	.venv/Scripts/python -m streamlit run dashboard/app.py

api:              ## Launch the FastAPI query API
	.venv/Scripts/python -m uvicorn api.app:app --reload

up:               ## Start infrastructure services
	docker compose up -d

down:             ## Stop infrastructure services
	docker compose down

clean:            ## Remove data and caches
	rm -rf data .pytest_cache .mypy_cache .ruff_cache
