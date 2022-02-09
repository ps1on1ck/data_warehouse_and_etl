from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
# from migration import MigrationAirflow # ModuleNotFoundError: No module named 'migration'
import os

migration_tables = ['NATION', 'REGION', 'PART', 'SUPPLIER', 'PARTSUPP', 'CUSTOMER', 'ORDERS', 'LINEITEM']

def dump_data(**kwargs):
    conn_string_id = "postgres_source"
    # db_migration = MigrationAirflow(conn_string_id)
    # db_migration.download_data_by_tables(migration_tables)
    result_folder="results"
    phook = PostgresHook(postgres_conn_id=conn_string_id)
    conn = phook.get_conn()
    with conn.cursor() as cursor:
        for table in migration_tables:
            q = f"COPY {table} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
            filename = f"./{result_folder}/{table}.csv"
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'w', encoding='utf-8') as f:
                cursor.copy_expert(q, f)


def load_data(**kwargs):
    conn_string_id = "postgres_target"
    # db_migration = MigrationAirflow(conn_string_id)
    # db_migration.load_data_by_tables(migration_tables)
    # db_migration.check_loaded_data_by_tables(migration_tables)
    result_folder="results"
    phook = PostgresHook(postgres_conn_id=conn_string_id)
    conn = phook.get_conn()
    with conn.cursor() as cursor:
        for table in migration_tables:
            q = f"COPY {table} from STDIN WITH DELIMITER ',' CSV HEADER;"
            filename = f"./{result_folder}/{table}.csv"
            with open(filename, 'r', encoding='utf-8') as f:
                cursor.copy_expert(q, f)


DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "start_date": datetime(2021, 6, 3),
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}

with DAG(
        dag_id="test_migration",
        default_args=DEFAULT_ARGS,
        schedule_interval="@monthly",
        tags=['data-flow'],
        catchup=False
) as dag:
    dump_data = PythonOperator(
        task_id='dump_my_data',
        python_callable=dump_data
    )

    load_data = PythonOperator(
        task_id='load_my_data',
        python_callable=load_data
    )

#    insert_my_data = PostgresOperator(
#            task_id='insert_my_data',
#            postgres_conn_id='postgres_target',
#            sql="INSERT INTO nation VALUES (17, 'name', 7, 'comment')"
#        )

    dump_data >> load_data
