"""Run dbt transformations and data-quality tests."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

from pipeline.logging_setup import get_logger

log = get_logger(__name__)


def _dbt_executable() -> str:
    exe = shutil.which("dbt")
    if exe:
        return exe
    script_dir = Path(sys.executable).parent
    for name in ("dbt.exe", "dbt"):
        candidate = script_dir / name
        if candidate.exists():
            return str(candidate)
    raise RuntimeError("dbt executable not found; install dbt-duckdb first")


def _run_dbt(*args: str) -> None:
    cmd = [_dbt_executable(), *args, "--project-dir", "dbt", "--profiles-dir", "dbt"]
    log.info("dbt", command=" ".join(cmd))
    subprocess.run(cmd, check=True)


def run_quality() -> None:
    _run_dbt("run")
    _run_dbt("test")
