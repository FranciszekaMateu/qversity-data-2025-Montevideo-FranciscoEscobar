"""
Gold DAG: Executes dbt transformations and tests on the Gold layer.

Description:
    - Executes `dbt run` on models tagged as `gold`.
    - Executes `dbt test` to validate metrics and views.
    - Logically depends on the Silver layer being updated.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
# -----------------------------------------------------------------------------
# Parameters and constants
# -----------------------------------------------------------------------------
DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_PROFILES_DIR = Variable.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)
GOLD_MODELS_SELECTOR = "tag:gold"

DEFAULT_ARGS = {
    "owner": "qversity",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="gold_pipeline_dag",
    description="Applies dbt transformations and tests for the Gold layer",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gold", "qversity", "dbt"],
) as dag:

    run_dbt_gold = BashOperator(
        task_id="run_dbt_gold_models",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --models {GOLD_MODELS_SELECTOR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    test_dbt_gold = BashOperator(
        task_id="test_dbt_gold_models",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --models {GOLD_MODELS_SELECTOR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    run_dbt_gold >> test_dbt_gold 