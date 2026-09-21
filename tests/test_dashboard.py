from pathlib import Path

DASHBOARD = Path(__file__).resolve().parent.parent / "dashboard" / "app.py"

REQUIRED_MARTS = [
    "marts.daily_demand",
    "marts.hourly_demand",
    "marts.route_performance",
    "marts.weather_impact",
    "marts.dim_zones",
]


def test_dashboard_compiles():
    compile(DASHBOARD.read_text(encoding="utf-8"), str(DASHBOARD), "exec")


def test_dashboard_reads_marts_not_raw_data():
    source = DASHBOARD.read_text(encoding="utf-8")
    for mart in REQUIRED_MARTS:
        assert mart in source, f"dashboard missing {mart}"
    assert "data/raw" not in source
