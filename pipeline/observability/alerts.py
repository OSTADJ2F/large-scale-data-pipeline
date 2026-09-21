"""Operational alerts derived from pipeline metadata."""

from __future__ import annotations

from pipeline.config import Settings, get_settings
from pipeline.logging_setup import get_logger
from pipeline.metadata import MetadataStore

log = get_logger(__name__)


def _month_key(partition: str) -> tuple[int, int]:
    y, m = partition.split("-")
    return int(y), int(m)


def check_alerts(settings: Settings | None = None) -> list[dict]:
    settings = settings or get_settings()
    meta = MetadataStore(settings.data_dir / "pipeline_meta.duckdb")
    alerts: list[dict] = []
    try:
        runs = meta.list_runs()
        if not runs:
            return [
                {"name": "no_data", "severity": "critical", "message": "No pipeline runs recorded."}
            ]

        latest = runs[0]
        if latest["status"] == "failed":
            alerts.append(
                {
                    "name": "pipeline_failure",
                    "severity": "critical",
                    "message": f"Partition {latest['partition_date']} failed: {latest['error_message']}",
                }
            )

        successes = [r for r in runs if r["status"] == "success"]
        if not successes:
            alerts.append(
                {
                    "name": "no_data",
                    "severity": "critical",
                    "message": "No successful runs recorded.",
                }
            )
        else:
            last = successes[0]
            if (last["output_rows"] or 0) == 0:
                alerts.append(
                    {
                        "name": "no_data",
                        "severity": "critical",
                        "message": "Latest run produced 0 rows.",
                    }
                )

            if len(successes) >= 2:
                prev = successes[1]
                if prev["input_rows"]:
                    ratio = (last["input_rows"] or 0) / prev["input_rows"]
                    if ratio > settings.alert_row_change_ratio or ratio < (
                        1 / settings.alert_row_change_ratio
                    ):
                        alerts.append(
                            {
                                "name": "row_count_change",
                                "severity": "warning",
                                "message": (
                                    f"Input rows changed {ratio:.1f}x between partitions "
                                    f"{prev['partition_date']} and {last['partition_date']}."
                                ),
                            }
                        )

            started = last.get("started_at")
            completed = last.get("completed_at")
            if started and completed:
                duration = (completed - started).total_seconds()
                if duration > settings.alert_max_duration_seconds:
                    alerts.append(
                        {
                            "name": "excessive_duration",
                            "severity": "warning",
                            "message": f"Latest run took {duration:.0f}s (>{settings.alert_max_duration_seconds:.0f}s).",
                        }
                    )

            partitions = sorted({r["partition_date"] for r in successes})
            for a, b in zip(partitions, partitions[1:], strict=False):
                ay, am = _month_key(a)
                by, bm = _month_key(b)
                next_month = (ay, am + 1) if am < 12 else (ay + 1, 1)
                if (by, bm) != next_month:
                    alerts.append(
                        {
                            "name": "missing_partition",
                            "severity": "warning",
                            "message": f"Gap in processed partitions between {a} and {b}.",
                        }
                    )
                    break
    finally:
        meta.close()

    for alert in alerts:
        log.warning(
            "alert", name=alert["name"], severity=alert["severity"], message=alert["message"]
        )
    return alerts


def run_alerts() -> None:
    alerts = check_alerts()
    if not alerts:
        print("No alerts.")
        return
    for a in alerts:
        print(f"[{a['severity'].upper()}] {a['name']}: {a['message']}")
