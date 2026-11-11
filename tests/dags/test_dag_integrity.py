"""DAG integrity: both DAGs import cleanly and have the expected task graph.
Runs in the Airflow venv / CI job (needs apache-airflow, not Spark)."""
from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[2] / "airflow" / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_hourly_graph(dagbag):
    dag = dagbag.get_dag("lakehouse_hourly")
    assert dag.schedule_interval == "@hourly" and not dag.catchup
    order = ["check_services", "ensure_connector", "check_pipeline_lag", "dbt_build"]
    for up, down in zip(order, order[1:]):
        assert down in dag.get_task(up).downstream_task_ids
    assert "dbt_source_freshness" in dag.get_task("ensure_connector").downstream_task_ids


def test_maintenance_graph(dagbag):
    dag = dagbag.get_dag("lakehouse_maintenance")
    assert dag.schedule_interval == "@daily"
    assert "vacuum" in dag.get_task("optimize").downstream_task_ids
    assert "dbt_build" in dag.get_task("vacuum").downstream_task_ids


def test_sh_commands_are_not_treated_as_templates(dagbag):
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            assert not task.bash_command.rstrip().endswith(".sh") or task.bash_command.endswith(" ")
