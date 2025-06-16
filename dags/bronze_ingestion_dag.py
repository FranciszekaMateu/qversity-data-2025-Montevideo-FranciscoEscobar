import json
from datetime import datetime

import boto3
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from botocore import UNSIGNED
from botocore.config import Config
from psycopg2.extras import execute_values

# Configuración de conexiones y fuentes de datos
POSTGRES_CONN_ID = Variable.get("POSTGRES_CONN_ID")
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
S3_OBJECT_KEY = Variable.get("S3_OBJECT_KEY")

# Configuración de la estructura de datos
BRONZE_SCHEMA = "bronze"
BRONZE_TABLE = "raw_mobile_customers"
BRONZE_TABLE_FQN = f"{BRONZE_SCHEMA}.{BRONZE_TABLE}"

# Definición del DAG y sus metadatos
DAG_CONFIG = {
    "dag_id": "bronze_ingestion_pipeline",
    "start_date": datetime(2023, 1, 1),
    "schedule_interval": None,
    "catchup": False,
    "tags": ["qversity", "bronze"],
    "doc_md": """
    ### Pipeline de Ingesta a la Capa de Bronce

    Este proceso es el punto de entrada para los datos de clientes móviles en nuestro sistema.

    Su responsabilidad es tomar los datos crudos desde su origen y almacenarlos en nuestra
    área inicial de preparación (la capa de Bronce).

    Cada ejecución asegura que los datos en esta capa representen una copia fiel y actualizada
    de la fuente, preparando el terreno para las siguientes fases de transformación y análisis.
    """
}

@dag(**DAG_CONFIG)
def bronze_ingestion_dag():
    """Define el pipeline que trae los datos crudos de clientes al sistema."""

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
                _loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )

    @task
    def ingest_data_from_s3():
        """Extrae los datos de clientes desde la fuente externa y los carga en la capa inicial."""
        s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
        
        try:
            response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_OBJECT_KEY)
            json_data = json.loads(response["Body"].read())
            records_to_insert = [(json.dumps(record),) for record in json_data]

            if not records_to_insert:
                print("No se encontraron registros para insertar.")
                return

            postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            
            with postgres_hook.get_conn() as conn, conn.cursor() as cursor:
                print(f"Limpiando la tabla {BRONZE_TABLE_FQN}...")
                cursor.execute(f"TRUNCATE TABLE {BRONZE_TABLE_FQN};")

                print(f"Insertando {len(records_to_insert)} registros...")
                execute_values(
                    cursor,
                    f"INSERT INTO {BRONZE_TABLE_FQN} (data) VALUES %s",
                    records_to_insert,
                )
                conn.commit()
                print("Inserción completada exitosamente.")

        except Exception as e:
            print(f"Error durante la ingesta de datos: {e}")
            raise

    # Definición del flujo de tareas
    create_bronze_schema >> create_bronze_table >> ingest_data_from_s3()


# Instanciación del DAG
bronze_ingestion_dag() 