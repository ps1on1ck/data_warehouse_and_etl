from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "start_date": datetime(2021, 6, 3),
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": True,
}

with DAG(
        dag_id="core_create_tables",
        default_args=DEFAULT_ARGS,
        schedule_interval="@once",
        tags=['core-data-flow'],
        catchup=False
) as dag:
    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="target_db",
        sql="./core_schemas.sql"
    )

    create_tables
