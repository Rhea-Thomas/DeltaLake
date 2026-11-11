"""Airflow orchestration for the DeltaLake lakehouse.

The streaming pipeline itself is a long-running Spark application (or a
continuous Databricks job), not an Airflow task: Airflow schedules the batch
work *around* the stream and watches its health.

lakehouse_hourly
    check_services -> ensure_connector -> check_pipeline_lag -> dbt_build
    * check_services: Postgres, Kafka, Connect, Trino reachable; replication slot active
    * ensure_connector: repair Debezium if FAILED/UNASSIGNED (REST restart) or missing
    * check_pipeline_lag: fail if bronze trails Kafka or silver trails bronze too far
    * dbt_build: rebuild the gold star schema and run every dbt test (a failing
      test fails the task, so bad gold data is never silently published)
    * dbt_source_freshness runs alongside as a warning signal

lakehouse_maintenance (daily)
    optimize (compact + Z-order) -> vacuum -> dbt_build
    * Rebuilding gold afterwards keeps marts in step with any layout change.

All tasks shell out to project commands (Makefile / python -m ...), so the same
steps run identically by hand, in CI, or from any other scheduler. Each tool
keeps its own virtualenv (.venv for Spark, .venv-dbt for dbt); this Airflow
environment needs no Spark or dbt installed.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO = os.environ.get("LAKEHOUSE_REPO", str(Path(__file__).resolve().parents[2]))
ENV = {
    "PATH": os.environ.get("PATH", "/usr/bin:/bin") + ":/opt/homebrew/bin:/usr/local/bin",
    "HOME": os.environ.get("HOME", "/tmp"),
    "PYTHONPATH": f"{REPO}/src",
    "JAVA_HOME": os.environ.get("JAVA_HOME", "/opt/homebrew/opt/openjdk@17"),
}
PY = f"{REPO}/.venv/bin/python"

default_args = {
    "owner": "lakehouse",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


def bash(task_id: str, command: str, **kw) -> BashOperator:
    # The trailing space matters: Airflow treats a bash_command ending in ".sh"
    # as the path of a Jinja template file instead of a command to run.
    return BashOperator(task_id=task_id, bash_command=f"cd {REPO} && {command} ", env=ENV,
                        append_env=False, **kw)


with DAG(
    dag_id="lakehouse_hourly",
    description="Health-check the CDC stream, then rebuild and test the gold star schema",
    schedule="@hourly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["lakehouse", "dbt"],
) as hourly:
    check_services = bash("check_services", f"{PY} -m lakehouse.health services")
    ensure_connector = bash("ensure_connector", "./scripts/ensure_connector.sh")
    check_lag = bash("check_pipeline_lag",
                     f"{PY} -m lakehouse.health lag --max-kafka-lag 100000 --max-silver-lag 100")
    freshness = bash("dbt_source_freshness",
                     "cd dbt && ../.venv-dbt/bin/dbt source freshness --profiles-dir .",
                     retries=0)
    dbt_build = bash("dbt_build", "make dbt")

    check_services >> ensure_connector >> check_lag >> dbt_build
    ensure_connector >> freshness


with DAG(
    dag_id="lakehouse_maintenance",
    description="Compact and Z-order Delta tables, vacuum old files, rebuild gold",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["lakehouse", "maintenance"],
) as maintenance:
    optimize = bash("optimize", f"{PY} -m lakehouse.maintenance optimize")
    vacuum = bash("vacuum", f"{PY} -m lakehouse.maintenance vacuum --retain-hours 168")
    rebuild_gold = bash("dbt_build", "make dbt")

    optimize >> vacuum >> rebuild_gold
