"""
Silver DAG: Applies dbt transformations on the Silver layer.

Description:
    - Executes `dbt run` on models tagged as `silver`.
    - Executes `dbt test` to ensure model quality.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

# -----------------------------------------------------------------------------
# Project parameters and constants
# -----------------------------------------------------------------------------
DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_PROFILES_DIR = Variable.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)
SILVER_MODELS_SELECTOR = "tag:silver"

DEFAULT_ARGS = {
    "owner": "qversity",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# -----------------------------------------------------------------------------
# DAG definition
# -----------------------------------------------------------------------------
with DAG(
    dag_id="silver_pipeline_dag",
    description="Applies dbt transformations and tests for the Silver layer",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["silver", "qversity", "dbt"],
) as dag:
    # ---------------------------------------------------------------------
    # Tasks
    # ---------------------------------------------------------------------
    run_dbt_silver = BashOperator(
        task_id="run_dbt_silver_models",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt deps && "
            f"dbt run --models {SILVER_MODELS_SELECTOR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    test_dbt_silver = BashOperator(
        task_id="test_dbt_silver_models",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt deps && "
            f"dbt test --models {SILVER_MODELS_SELECTOR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    run_dbt_silver >> test_dbt_silver 