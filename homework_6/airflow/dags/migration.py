import os
from airflow.hooks.postgres_hook import PostgresHook


class MigrationAirflow:
    def __init__(self, conn_id, result_folder='results'):
        self.postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        self.conn = postgres_hook.get_conn()
        self.result_folder = result_folder

    def download_data_by_tables(self, tables):
        with self.conn.cursor() as cursor:
            for table in tables:
                q = f"COPY {table} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
                filename = f"./{self.result_folder}/{table}.csv"
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                with open(filename, 'w', encoding='utf-8') as f:
                    cursor.copy_expert(q, f)

    def load_data_by_tables(self, tables):
        with self.conn.cursor() as cursor:
            for table in tables:
                q = f"COPY {table} from STDIN WITH DELIMITER ',' CSV HEADER;"
                filename = f"./{self.result_folder}/{table}.csv"
                with open(filename, 'r', encoding='utf-8') as f:
                    cursor.copy_expert(q, f)

    def check_loaded_data_by_tables(self, tables):
        with self.conn.cursor() as cursor:
            print("-----Source Database-----")
            for table in tables:
                cursor.execute(f'select count(*) from {table} limit 1')
                count = cursor.fetchall()
                print(f"{table} count: {count}")

        with self.conn.cursor() as cursor:
            print("-----Target Database-----")
            for table in tables:
                cursor.execute(f'select count(*) from {table} limit 1')
                count = cursor.fetchall()
                print(f"{table} count: {count}")
