"""Validation report persistence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def save_report(report: dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)
    return path
