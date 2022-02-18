from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import os

migration_tables = ['NATION', 'REGION', 'PART', 'SUPPLIER', 'PARTSUPP', 'CUSTOMER', 'ORDERS', 'LINEITEM']


def dump_data(conn_string_id, tables, result_folder="results", **kwargs):
    phook = PostgresHook(postgres_conn_id=conn_string_id)
    conn = phook.get_conn()
    with conn.cursor() as cursor:
        for table in tables:
            q = f"COPY {table} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
            filename = f"./{result_folder}/{table}.csv"
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'w', encoding='utf-8') as f:
                cursor.copy_expert(q, f)


def load_data(conn_string_id, tables, result_folder="results", **kwargs):
    phook = PostgresHook(postgres_conn_id=conn_string_id)
    conn = phook.get_conn()
    with conn.cursor() as cursor:
        for table in tables:
            q = f"COPY {table} from STDIN WITH DELIMITER ',' CSV HEADER;"
            filename = f"./{result_folder}/{table}.csv"
            with open(filename, 'r', encoding='utf-8') as f:
                cursor.copy_expert(q, f)
        conn.commit()


DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "start_date": datetime(2021, 6, 3),
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}

with DAG(
        dag_id="simple_migration",
        default_args=DEFAULT_ARGS,
        schedule_interval="@monthly",
        tags=['simple-data-flow'],
        catchup=False
) as dag:
    dump_data = PythonOperator(
        task_id='dump_my_data',
        python_callable=dump_data,
        op_args=["postgres_source", migration_tables]
    )

    load_data = PythonOperator(
        task_id='load_my_data',
        python_callable=load_data,
        op_args=["postgres_target", migration_tables]
    )

    dump_data >> load_data
