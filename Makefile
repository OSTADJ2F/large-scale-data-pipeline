# Makefile for the transportation analytics pipeline.
# On Windows PowerShell, use `.\make.ps1 <target>` instead.

PYTHON ?= python

# Use the correct venv interpreter path on Windows vs POSIX.
ifeq ($(OS),Windows_NT)
  VENV_PY := .venv/Scripts/python.exe
  VENV_PIP := .venv/Scripts/pip.exe
else
  VENV_PY := .venv/bin/python
  VENV_PIP := .venv/bin/pip
endif

.PHONY: setup test lint format typecheck ingest transform pipeline dashboard api \
        serve stop status smoke up down clean doctor

setup:            ## Create virtualenv and install dependencies
	$(PYTHON) -m venv .venv
	$(VENV_PIP) install --upgrade pip
	$(VENV_PIP) install -e ".[dev]"

test:             ## Run the test suite
	$(VENV_PY) -m pytest

lint:             ## Run the linter
	$(VENV_PY) -m ruff check pipeline tests api dashboard scripts

format:           ## Format the code
	$(VENV_PY) -m ruff format pipeline tests api dashboard scripts
	$(VENV_PY) -m ruff check --fix pipeline tests api dashboard scripts

typecheck:        ## Run mypy
	$(VENV_PY) -m mypy pipeline

doctor:           ## Check configuration
	$(VENV_PY) -m pipeline doctor

ingest:           ## Ingest raw data
	$(VENV_PY) -m pipeline ingest --start-date $(START) --end-date $(END)

transform:        ## Run normalize + enrich
	$(VENV_PY) -m pipeline run --start-date $(START) --end-date $(END)

pipeline:         ## Run the full pipeline
	$(VENV_PY) -m pipeline run --start-date $(START) --end-date $(END)

dashboard:        ## Launch the Streamlit dashboard
	$(VENV_PY) -m streamlit run dashboard/app.py

api:              ## Launch the FastAPI query API
	$(VENV_PY) -m uvicorn api.app:app --reload

serve:            ## Start infra + API + dashboard and smoke-test
	$(VENV_PY) -m scripts.dev start

stop:             ## Stop the API and dashboard
	$(VENV_PY) -m scripts.dev stop

status:           ## Show running services
	$(VENV_PY) -m scripts.dev status

smoke:            ## Verify the API, dashboard, and database
	$(VENV_PY) -m scripts.dev smoke

up:               ## Start infrastructure services
	docker compose up -d

down:             ## Stop infrastructure services
	docker compose down

clean:            ## Remove data and caches
	rm -rf data .pytest_cache .mypy_cache .ruff_cache
