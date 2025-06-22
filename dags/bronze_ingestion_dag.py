"""
Bronze Layer Ingestion Pipeline.

Responsibility:
    * Downloads raw data from S3.
    * Inserts records as JSONB into the `public_bronze.raw_mobile_customers` table.

"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List

import boto3
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from botocore import UNSIGNED
from botocore.config import Config
from psycopg2.extras import execute_values

# -------------------------------------------------------------------------------------
# Configuration derived from Airflow Variables
# -------------------------------------------------------------------------------------
POSTGRES_CONN_ID: str = Variable.get("POSTGRES_CONN_ID")
S3_BUCKET_NAME: str = Variable.get("S3_BUCKET_NAME")
S3_OBJECT_KEY: str = Variable.get("S3_OBJECT_KEY")

# -------------------------------------------------------------------------------------
# Target schema and table
# -------------------------------------------------------------------------------------
BRONZE_SCHEMA: str = "public_bronze"
BRONZE_TABLE: str = "raw_mobile_customers"
BRONZE_TABLE_FQN: str = f"{BRONZE_SCHEMA}.{BRONZE_TABLE}"


@dag(
    dag_id="bronze_pipeline_dag",
    description="Raw data ingestion to Bronze layer",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["qversity", "bronze"],
)
def bronze_pipeline_dag():  
    """Orchestrates the ingestion of raw mobile customer data."""

    create_bronze_schema = PostgresOperator(
        task_id="create_bronze_schema",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA};",
    )

    create_bronze_table = PostgresOperator(
        task_id="create_bronze_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {BRONZE_TABLE_FQN} (
                data JSONB,
                _loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )

    # -------------------------------------------------------------------------
    # Ingestion task
    # -------------------------------------------------------------------------
    @task(task_id="ingest_data_from_s3", retries=3, retry_delay=timedelta(minutes=2))
    def ingest_data_from_s3() -> None:
        """Downloads the JSON file from S3 and inserts records into the Bronze table."""

        logger = logging.getLogger("bronze_ingestion")
        s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

        logger.info("Downloading %s from %s", S3_OBJECT_KEY, S3_BUCKET_NAME)
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_OBJECT_KEY)
        json_data: List[Dict] = json.loads(response["Body"].read())

        if not json_data:
            logger.warning("JSON object is empty. No records were inserted.")
            return

        records_to_insert = [(json.dumps(record),) for record in json_data]

        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        truncate_sql = f"TRUNCATE TABLE {BRONZE_TABLE_FQN};"
        insert_sql = f"INSERT INTO {BRONZE_TABLE_FQN} (data) VALUES %s"

        logger.info(
            "Inserting %s records into %s", len(records_to_insert), BRONZE_TABLE_FQN
        )

        with postgres_hook.get_conn() as conn, conn.cursor() as cursor:
            cursor.execute(truncate_sql)
            execute_values(cursor, insert_sql, records_to_insert)
            conn.commit()

        logger.info("Ingestion completed successfully.")

    # -------------------------------------------------------------------------
    # Task flow definition
    # -------------------------------------------------------------------------
    create_bronze_schema >> create_bronze_table >> ingest_data_from_s3()


bronze_pipeline_dag() 