"""Validation orchestration: validate raw trips and split outputs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl

from pipeline.config import Settings, get_settings
from pipeline.ingest.runner import iter_months, parse_date
from pipeline.logging_setup import get_logger
from pipeline.validate.report import save_report
from pipeline.validate.validator import ValidationError, ValidationResult, validate_trips

log = get_logger(__name__)


@dataclass
class PartitionValidationSummary:
    partition: str
    report: dict
    valid_path: Path | None = None
    invalid_path: Path | None = None
    quarantined_path: Path | None = None


class Validator:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.settings.ensure_dirs()

    def _raw_trip_files(self, y: int, m: int) -> list[Path]:
        pattern = (
            self.settings.raw_dir / "taxi_trips" / f"year={y}" / f"month={m:02d}" / "*.parquet"
        )
        return sorted(pattern.parent.glob(pattern.name)) if pattern.parent.exists() else []

    def _read_partition(self, y: int, m: int) -> pl.DataFrame:
        files = self._raw_trip_files(y, m)
        if not files:
            raise FileNotFoundError(f"No raw trip data for {y}-{m:02d}")
        return pl.concat([pl.read_parquet(f) for f in files], how="vertical_relaxed")

    def validate_partition(self, partition: str) -> PartitionValidationSummary:
        y, m = (int(x) for x in partition.split("-"))
        df = self._read_partition(y, m)
        result = validate_trips(df)
        self._check_thresholds(result)
        paths = self._write_outputs(partition, result)
        report_path = save_report(
            result.report,
            self.settings.validated_dir / "trips" / "reports" / f"{partition}.json",
        )
        log.info(
            "validation_done",
            partition=partition,
            total=result.report["total_rows"],
            valid=result.report["valid_rows"],
            invalid=result.report["invalid_rows"],
            quarantined=result.report["quarantined_rows"],
            report=str(report_path),
        )
        return PartitionValidationSummary(partition=partition, report=result.report, **paths)

    def _check_thresholds(self, result: ValidationResult) -> None:
        r = result.report
        total = r["total_rows"] or 1
        invalid_ratio = r["invalid_rows"] / total
        null_ratio = r["quarantine_categories"]["null"] / total
        if invalid_ratio > self.settings.quality_max_invalid_ratio:
            raise ValidationError(
                f"Invalid row ratio {invalid_ratio:.2%} exceeds "
                f"{self.settings.quality_max_invalid_ratio:.0%}"
            )
        if null_ratio > self.settings.quality_max_null_ratio:
            raise ValidationError(
                f"Null row ratio {null_ratio:.2%} exceeds "
                f"{self.settings.quality_max_null_ratio:.0%}"
            )

    def _write_outputs(self, partition: str, result: ValidationResult) -> dict[str, Path]:
        base = self.settings.validated_dir / "trips" / f"partition={partition}"
        outputs = {}
        for label, df in (
            ("valid", result.valid),
            ("invalid", result.invalid),
            ("quarantined", result.quarantined),
        ):
            path = base / f"{label}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            df.write_parquet(path)
            outputs[f"{label}_path"] = path
        return outputs

    def run(self, start: date, end: date) -> list[PartitionValidationSummary]:
        summaries = []
        for y, m in iter_months(start, end):
            partition = f"{y}-{m:02d}"
            try:
                summaries.append(self.validate_partition(partition))
            except FileNotFoundError as exc:
                log.warning("missing_raw_partition", partition=partition, error=str(exc))
        return summaries


def run_validate(start_date: str | None, end_date: str | None) -> None:
    settings = get_settings()
    start = parse_date(start_date, settings.default_start_date)
    end = parse_date(end_date, settings.default_end_date)
    validator = Validator(settings)
    summaries = validator.run(start, end)
    print(f"Validated {len(summaries)} partition(s).")
    for s in summaries:
        print(
            f"  {s.partition}: total={s.report['total_rows']} valid={s.report['valid_rows']} "
            f"invalid={s.report['invalid_rows']} quarantined={s.report['quarantined_rows']}"
        )
