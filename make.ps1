# make.ps1 — Windows PowerShell equivalent of the Makefile.
# Usage: .\make.ps1 <target> [-Start 2025-01-01] [-End 2025-01-31]

param(
    [Parameter(Position = 0)][string]$Target,
    [string]$Start,
    [string]$End
)

$ErrorActionPreference = "Stop"
$Py = ".venv\Scripts\python.exe"

function Invoke-Target {
    switch ($Target) {
        "setup" {
            py -3.10 -m venv .venv
            & $Py -m pip install --upgrade pip
            & $Py -m pip install -e ".[dev]"
        }
        "test" { & $Py -m pytest @args }
        "lint" { & $Py -m ruff check pipeline tests api dashboard scripts }
        "format" {
            & $Py -m ruff format pipeline tests api dashboard scripts
            & $Py -m ruff check --fix pipeline tests api dashboard scripts
        }
        "typecheck" { & $Py -m mypy pipeline }
        "doctor" { & $Py -m pipeline doctor }
        "ingest" { & $Py -m pipeline ingest --start-date $Start --end-date $End }
        "transform" { & $Py -m pipeline run --start-date $Start --end-date $End }
        "pipeline" { & $Py -m pipeline run --start-date $Start --end-date $End }
        "dashboard" { & $Py -m streamlit run dashboard/app.py }
        "api" { & $Py -m uvicorn api.app:app --reload }
        "serve" { & $Py -m scripts.dev start }
        "stop" { & $Py -m scripts.dev stop }
        "status" { & $Py -m scripts.dev status }
        "smoke" { & $Py -m scripts.dev smoke }
        "up" { docker compose up -d }
        "down" { docker compose down }
        "clean" { Remove-Item -Recurse -Force data,.pytest_cache,.mypy_cache,.ruff_cache -ErrorAction SilentlyContinue }
        default {
            Write-Host "Available targets: setup, test, lint, format, typecheck, doctor,"
            Write-Host "  ingest, transform, pipeline, dashboard, api, serve, stop, status, smoke,"
            Write-Host "  up, down, clean"
            Write-Host "Pass dates via -Start and -End (YYYY-MM-DD)."
        }
    }
}

Invoke-Target
