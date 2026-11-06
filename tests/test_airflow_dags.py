"""Automated tests for Airflow DAG integrity and configuration."""

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dagbag():
    """Load all DAGs from the airflow/dags directory without writing to DB."""
    return DagBag(dag_folder="airflow/dags", include_examples=False)


def test_no_import_errors(dagbag):
    """Ensure no syntax errors, missing dependencies, or broken imports."""
    assert len(dagbag.import_errors) == 0, (
        f"DAG import failures detected: {dagbag.import_errors}"
    )


def test_dags_loaded(dagbag):
    """Ensure at least one DAG is discovered."""
    assert len(dagbag.dags) > 0, "No DAGs found in airflow/dags folder."


def test_dag_cycle_check(dagbag):
    """Ensure DAGs do not contain cyclic dependencies (deadlocks)."""
    for dag_id, dag in dagbag.dags.items():
        # test_cycle() will raise an AirflowDagCycleException if a loop is detected.
        dag.test_cycle()


def test_dag_standards(dagbag):
    """Verify production standards on all discovered DAGs."""
    for dag_id, dag in dagbag.dags.items():
        # 1. Must define at least 1 task
        assert len(dag.tasks) > 0, f"DAG '{dag_id}' has no tasks defined."

        # 2. Prevent accidental backfills on new deployments
        assert dag.catchup is False, (
            f"DAG '{dag_id}' must explicitly set catchup=False."
        )

        # 3. Ensure retries are configured for production resilience
        retries = dag.default_args.get("retries", 0)
        assert retries >= 1, (
            f"DAG '{dag_id}' default_args must configure at least 1 retry."
        )
