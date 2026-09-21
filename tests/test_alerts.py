from datetime import timedelta

from pipeline.config import Settings
from pipeline.metadata import MetadataStore, utcnow
from pipeline.observability.alerts import check_alerts


def _settings(tmp_path):
    return Settings(data_dir=tmp_path)


def _record(meta, partition, status, input_rows=None, output_rows=None, error=None, duration=10.0):
    started = utcnow()
    meta.record_run(
        {
            "run_id": f"{partition}-{status}",
            "pipeline_name": "taxi_pipeline",
            "partition_date": partition,
            "status": status,
            "started_at": started,
            "completed_at": started + timedelta(seconds=duration),
            "input_rows": input_rows,
            "output_rows": output_rows,
            "error_message": error,
        }
    )


def _names(alerts):
    return {a["name"] for a in alerts}


def test_no_runs_triggers_no_data(tmp_path):
    s = _settings(tmp_path)
    alerts = check_alerts(s)
    assert "no_data" in _names(alerts)


def test_failed_latest_run(tmp_path):
    s = _settings(tmp_path)
    meta = MetadataStore(s.data_dir / "pipeline_meta.duckdb")
    _record(meta, "2025-01", "failed", error="boom")
    meta.close()
    alerts = check_alerts(s)
    names = _names(alerts)
    assert "pipeline_failure" in names
    assert "no_data" in names


def test_zero_output_triggers_no_data(tmp_path):
    s = _settings(tmp_path)
    meta = MetadataStore(s.data_dir / "pipeline_meta.duckdb")
    _record(meta, "2025-01", "success", input_rows=100, output_rows=0)
    meta.close()
    alerts = check_alerts(s)
    assert "no_data" in _names(alerts)


def test_row_count_change(tmp_path):
    s = _settings(tmp_path)
    meta = MetadataStore(s.data_dir / "pipeline_meta.duckdb")
    _record(meta, "2025-02", "success", input_rows=1_000_000, output_rows=900_000)
    _record(meta, "2025-01", "success", input_rows=100_000, output_rows=90_000)
    meta.close()
    alerts = check_alerts(s)
    assert "row_count_change" in _names(alerts)


def test_missing_partition(tmp_path):
    s = _settings(tmp_path)
    meta = MetadataStore(s.data_dir / "pipeline_meta.duckdb")
    _record(meta, "2025-03", "success", input_rows=100, output_rows=90)
    _record(meta, "2025-01", "success", input_rows=100, output_rows=90)
    meta.close()
    alerts = check_alerts(s)
    assert "missing_partition" in _names(alerts)


def test_healthy_state_has_no_alerts(tmp_path):
    s = _settings(tmp_path)
    meta = MetadataStore(s.data_dir / "pipeline_meta.duckdb")
    _record(meta, "2025-02", "success", input_rows=100_000, output_rows=90_000)
    _record(meta, "2025-01", "success", input_rows=110_000, output_rows=99_000)
    meta.close()
    assert check_alerts(s) == []
