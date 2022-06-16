from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import json

from operators.migration import MigrationAirflow

migration_store_name = "migrations"


def init_storage(**context):
    context['ti'].xcom_push(key=migration_store_name, value={})


def dump_data(conn_string_id, migration_name, **kwargs):
    migration_tables = ['orders', 'products', 'suppliers', 'orderdetails', 'productsuppl']
    db_migration = MigrationAirflow(conn_string_id)
    data = db_migration.download_data_by_tables(migration_tables)
    migrations = kwargs['ti'].xcom_pull(key=migration_store_name)
    migrations[migration_name] = {'source': conn_string_id, 'timestamp': str(datetime.now()), 'data': []}
    migrations[migration_name]['data'] = data
    return kwargs['ti'].xcom_push(key=migration_store_name, value=migrations)


def load_data(conn_string_id, schema_name, migration_name, **kwargs):
    migrations = kwargs['ti'].xcom_pull(key=migration_store_name)
    migration = migrations[migration_name]

    db_migration = MigrationAirflow(conn_string_id)
    db_migration.load_data_by_tables_with_schemas(migration['data'], schema_name)


def print_all_data(**kwargs):
    print(json.dumps(kwargs['ti'].xcom_pull(key=migration_store_name), indent=4, sort_keys=True))


def remove_files(migration_name, **kwargs):
    migrations = kwargs['ti'].xcom_pull(key=migration_store_name)
    migration = migrations[migration_name]
    file_list = [k['filename'] for k in migration['data']]
    print(file_list)
    MigrationAirflow.remove_files(file_list)


DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "start_date": datetime(2021, 6, 3),
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": True,
}

with DAG(
        dag_id="stage_copy_tables",
        default_args=DEFAULT_ARGS,
        schedule_interval="@once",
        tags=['stage-data-flow'],
        catchup=False
) as dag:
    init_storage = PythonOperator(
        task_id="init_storage",
        python_callable=init_storage,
        provide_context=True
    )

    dump_data = PythonOperator(
        task_id='dump_my_data',
        python_callable=dump_data,
        op_args=["source_db", "migration_v1"],
        provide_context=True
    )

    load_data = PythonOperator(
        task_id='load_my_data',
        python_callable=load_data,
        op_args=["target_db", "stage", "migration_v1"],
        provide_context=True
    )

    print_result = PythonOperator(
        task_id="print_result",
        python_callable=print_all_data,
        provide_context=True
    )

    remove_files = PythonOperator(
        task_id="remove_files",
        python_callable=remove_files,
        op_args=["migration_v1"],
        provide_context=True
    )

    init_storage >> dump_data >> load_data >> print_result >> remove_files
