"""
tests/dags/test_dag_integrity.py
──────────────────────────────────
Basic DAG integrity checks: import, task count, no cycles.
Run with:  pytest tests/dags/
"""

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="airflow/dags", include_examples=False)


def test_no_import_errors(dagbag):
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"


def test_etl_pipeline_dag_exists(dagbag):
    assert "inventory_etl_pipeline" in dagbag.dags


def test_weekly_analytics_dag_exists(dagbag):
    assert "inventory_weekly_analytics" in dagbag.dags


def test_etl_pipeline_task_count(dagbag):
    dag = dagbag.dags["inventory_etl_pipeline"]
    # start + extract + transform + 5 analytics + end = 9 tasks (grouped)
    assert len(dag.tasks) >= 8


def test_weekly_analytics_task_count(dagbag):
    dag = dagbag.dags["inventory_weekly_analytics"]
    assert len(dag.tasks) >= 4


def test_etl_pipeline_schedule(dagbag):
    dag = dagbag.dags["inventory_etl_pipeline"]
    assert dag.schedule_interval == "0 2 * * *"


def test_weekly_analytics_schedule(dagbag):
    dag = dagbag.dags["inventory_weekly_analytics"]
    assert dag.schedule_interval == "0 3 * * 1"


def test_no_cycles(dagbag):
    for dag_id, dag in dagbag.dags.items():
        assert not dag.test_cycle(), f"Cycle detected in DAG: {dag_id}"
