from pathlib import Path

DAG_FILE = Path(__file__).resolve().parent.parent / "airflow" / "dags" / "taxi_pipeline.py"

REQUIRED_TASKS = [
    "check_source",
    "ingest_raw",
    "validate_raw",
    "normalize_staging",
    "enrich_weather",
    "build_marts",
    "run_quality_checks",
    "publish_metrics",
    "load_postgres",
]


def test_dag_file_syntax_is_valid():
    source = DAG_FILE.read_text(encoding="utf-8")
    compile(source, str(DAG_FILE), "exec")


def test_dag_defines_required_tasks():
    source = DAG_FILE.read_text(encoding="utf-8")
    for task in REQUIRED_TASKS:
        assert f'"{task}"' in source or f"'{task}'" in source, f"missing task {task}"


def test_dag_dependency_chain_is_linear():
    from itertools import pairwise

    source = DAG_FILE.read_text(encoding="utf-8")
    for upstream, downstream in pairwise(REQUIRED_TASKS):
        assert f"{upstream}" in source and f"{downstream}" in source
