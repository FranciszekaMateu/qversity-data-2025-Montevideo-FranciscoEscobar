"""
Master DAG: Executes DAGs in sequential order

1. Bronze → 2. Silver → 3. Gold

"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Existing DAG IDs with consistent naming
BRONZE_DAG_ID = "bronze_pipeline_dag"
SILVER_DAG_ID = "silver_pipeline_dag"
GOLD_DAG_ID = "gold_pipeline_dag"

DEFAULT_ARGS = {
    "owner": "qversity",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Create the DAG
dag = DAG(
    dag_id="master_pipeline_dag",
    description="Executes Bronze → Silver → Gold in sequence",
    default_args=DEFAULT_ARGS,
    schedule=None,  
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["qversity", "master", "pipeline"],
)

# Task 1: Execute Bronze
trigger_bronze = TriggerDagRunOperator(
    task_id="run_bronze_pipeline",
    trigger_dag_id=BRONZE_DAG_ID,
    wait_for_completion=True,
    dag=dag,
)

# Task 2: Execute Silver
trigger_silver = TriggerDagRunOperator(
    task_id="run_silver_pipeline", 
    trigger_dag_id=SILVER_DAG_ID,
    wait_for_completion=True,
    dag=dag,
)

# Task 3: Execute Gold
trigger_gold = TriggerDagRunOperator(
    task_id="run_gold_pipeline",
    trigger_dag_id=GOLD_DAG_ID,
    wait_for_completion=True,
    dag=dag,
)

# ORDER: Bronze → Silver → Gold
trigger_bronze >> trigger_silver >> trigger_gold 