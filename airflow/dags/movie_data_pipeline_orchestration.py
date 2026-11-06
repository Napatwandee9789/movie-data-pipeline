"""Orchestration Pipeline - Daily & Monthly Transformation DAG.

Orchestrates the asynchronous ingest loop across AWS Lambda and SQS, enforces
safe worker completion buffer, and triggers dbt Core transformations on AWS Athena.
"""

from datetime import datetime, timedelta
import logging
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
import time
from airflow.operators.python import PythonOperator

# ===========================================================================
# STEP 0: DAG RUNTIME CONFIGURATION & SENSITIVE PATH DEFINITIONS
# ===========================================================================

logger = logging.getLogger(__name__)

# Execution paths on Airflow host / mounted container volume
DBT_PROJECT_DIR = "/opt/airflow/data_transformation_dbt"
DBT_PROFILES_DIR = "/opt/airflow/data_transformation_dbt"

# AWS resource identifiers managed via AWS IAM / Secrets Manager
AWS_CONN_ID = "aws_default"
PRODUCER_LAMBDA_NAME = "movie-pipeline-lambda-producer"
SNAPSHOT_LAMBDA_NAME = "movie-pipeline-lambda-snapshot"

# Default task parameters enforcing production fault-tolerance
DEFAULT_ARGS = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


# ===========================================================================
# (HELPER FUNCTION FOR STEP 5): MONTHLY CALENDAR ARBITRATION LOGIC
# ===========================================================================


def evaluate_monthly_branch(**context: Any) -> str:
    """Evaluate whether the given DAG run corresponds to the first calendar day of the month.

    Inspects if the current logical date falls on day 1 of the month.

    Args:
        logical_date: Airflow execution date (pendulum.DateTime) representing the data interval.
        **context: Airflow task instance context dictionary passed by the execution runtime.

    Returns:
        str: Task ID of the subsequent operator to route execution to
            ('trigger_dbt_monthly_models' or 'skip_monthly_transformation').

    Raises:
        AirflowException: If logical_date is null or cannot be evaluated against calendar logic.
    """
    logical_date = context.get("logical_date")

    if not logical_date:
        raise AirflowException(
            "Execution failed: Missing logical_date in task runtime context."
        )

    # -----------------------------------------------------------------------
    # SCHEMA: Upstream Context Inspection (Sample Context Payload Subset)
    # {
    #   "dag": "<DAG: movie_data_pipeline_orchestration>",
    #   "ds": "2026-11-01",
    #   "logical_date": "2026-11-01T03:00:00+00:00",
    #   "task_instance": "<TaskInstance: ... evaluate_calendar_boundary ...>"
    # }
    # -----------------------------------------------------------------------

    logger.info(
        "Evaluating calendar boundary: Current Date=%s",
        logical_date.strftime("%Y-%m-%d"),
    )

    if logical_date.day == 1:
        logger.info(
            "Calendar evaluation positive: Today is the 1st. Triggering monthly reconciliation models."
        )
        return "trigger_dbt_monthly_models"

    logger.info(
        "Calendar evaluation negative: Today is not the 1st. Bypassing monthly reconciliation models."
    )
    return "skip_monthly_transformation"


# ===========================================================================
# STEP 1: DAG DEFINITION & TIMING SPECIFICATION
# ===========================================================================

with DAG(
    dag_id="movie_data_pipeline_orchestration",
    default_args=DEFAULT_ARGS,
    description="End-to-end ingestion and dbt-athena transformation orchestrator for movie data warehouse.",
    # Trigger at 03:00 UTC (10:00 AM Bangkok GMT+7) allowing upstream TMDB exports to settle
    schedule="0 3 * * *",
    start_date=datetime(2026, 1, 7),
    # Critical: Enforce catchup=False to prevent backfill storms on distributed serverless targets
    catchup=False,
    # Restrict concurrent runs to avoid Athena Iceberg table lock contention during MERGE statements
    max_active_runs=1,
    tags=["movie_data_pipeline", "dbt", "athena", "lambda", "sqs", "orchestration"],
) as dag:
    # =======================================================================
    # STEP 2: INGESTION STAGE (PARALLEL PRODUCER & SNAPSHOT LAMBDAS)
    # =======================================================================

    # -----------------------------------------------------------------------
    # SCHEMA: Synchronous Lambda Invocation Response Envelope
    # {
    #   "StatusCode": 200,
    #   "ExecutedVersion": "$LATEST",
    #   "Payload": "{\"statusCode\": 200, \"messages_dispatched\": 450, \"batch_date\": \"2026-11-04\"}"
    # }
    # -----------------------------------------------------------------------
    invoke_producer_lambda = LambdaInvokeFunctionOperator(
        task_id="invoke_producer_lambda",
        function_name=PRODUCER_LAMBDA_NAME,
        # Synchronous invocation guarantees Airflow waits until all IDs are successfully queued to SQS
        invocation_type="RequestResponse",
        aws_conn_id=AWS_CONN_ID,
    )

    invoke_snapshot_lambda = LambdaInvokeFunctionOperator(
        task_id="invoke_snapshot_lambda",
        function_name=SNAPSHOT_LAMBDA_NAME,
        # Snapshots execute atomically via single S3 put_object; safe to run concurrently with changes
        invocation_type="RequestResponse",
        aws_conn_id=AWS_CONN_ID,
    )

    # =======================================================================
    # STEP 3: ASYNCHRONOUS CONSUMPTION BUFFER (WORKER DRAIN DELAY)
    # =======================================================================

    # Sleep strictly for 300 seconds (5 minutes) right after upstream completes
    wait_for_workers_completion = PythonOperator(
        task_id="wait_for_workers_completion",
        python_callable=lambda: time.sleep(300),
    )

    # =======================================================================
    # STEP 4: DAILY DATA TRANSFORMATION LAYER (DBT CORE ON ATHENA)
    # =======================================================================

    # Compiles Presto/Trino SQL and submits execution asynchronously to AWS Athena Workgroup.
    # Runs staging, intermediate, dimensional SCD2 merges, and incremental fact partitions.
    trigger_dbt_daily_models = BashOperator(
        task_id="trigger_dbt_daily_models",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"/opt/airflow/dbt_venv/bin/dbt build --select tag:daily_run --profiles-dir {DBT_PROFILES_DIR}"
        ),
        # Treat non-zero exit code as fatal error to halt downstream conditional logic
        append_env=True,
    )

    # =======================================================================
    # STEP 5: MONTHLY RECONCILIATION ROUTING & CONDITIONAL EXECUTION
    # =======================================================================

    evaluate_calendar_boundary = BranchPythonOperator(
        task_id="evaluate_calendar_boundary",
        python_callable=evaluate_monthly_branch,
    )

    # Executes heavy monthly aggregates, cumulative cohort calculations, and historical fact rebuilds
    trigger_dbt_monthly_models = BashOperator(
        task_id="trigger_dbt_monthly_models",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"/opt/airflow/dbt_venv/bin/dbt build --select tag:monthly_run --profiles-dir {DBT_PROFILES_DIR}"
        ),
        append_env=True,
    )

    # Safe terminal state for standard calendar days where monthly reconciliation is omitted
    skip_monthly_transformation = EmptyOperator(task_id="skip_monthly_transformation")

    # =======================================================================
    # STEP 6: DEPENDENCY GRAPH DEFINITION
    # =======================================================================

    # 1. Ingestion Branch: Producer feeds SQS -> Sleep sensor absorbs consumer runtime
    invoke_producer_lambda >> wait_for_workers_completion

    # 2. Join Barrier: Daily dbt models require BOTH Worker batch ingestion AND Daily Snapshot parity
    [wait_for_workers_completion, invoke_snapshot_lambda] >> trigger_dbt_daily_models

    # 3. Monthly Branching: Evaluate calendar boundary immediately following daily success
    trigger_dbt_daily_models >> evaluate_calendar_boundary
    evaluate_calendar_boundary >> [
        trigger_dbt_monthly_models,
        skip_monthly_transformation,
    ]
