import os
from airflow.hooks.postgres_hook import PostgresHook
import uuid


class MigrationAirflow:
    def __init__(self, conn_id, result_folder='results'):
        self.postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        self.conn = self.postgres_hook.get_conn()
        self.result_folder = result_folder

    def download_data_by_tables(self, tables):
        data = []
        with self.conn.cursor() as cursor:
            for table in tables:
                q = f"COPY {table} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
                filename = f"{table}-{uuid.uuid4()}.csv"
                pathToFile = f"./{self.result_folder}/{filename}"
                os.makedirs(os.path.dirname(pathToFile), exist_ok=True)
                print(pathToFile)
                with open(pathToFile, 'w', encoding='utf-8') as f:
                    cursor.copy_expert(q, f)
                data.append({'table_name': table, 'filename': filename})
        return data

    def load_data_by_tables_with_schemas(self, data, schema_name):
        with self.conn.cursor() as cursor:
            for d in data:
                q = f"COPY {schema_name}.{d['table_name']} from STDIN WITH DELIMITER ',' CSV HEADER;"
                pathToFile = f"./{self.result_folder}/{d['filename']}"
                with open(pathToFile, 'r', encoding='utf-8') as f:
                    cursor.copy_expert(q, f)
            self.conn.commit()

    def load_data_by_tables(self, data):
        with self.conn.cursor() as cursor:
            for d in data:
                q = f"COPY {d['table_name']} from STDIN WITH DELIMITER ',' CSV HEADER;"
                pathToFile = f"./{self.result_folder}/{d['filename']}"
                with open(pathToFile, 'r', encoding='utf-8') as f:
                    cursor.copy_expert(q, f)
            self.conn.commit()

    @staticmethod
    def remove_files(files, folder='results'):
        for file in files:
            pathToFile = f"./{folder}/{file}"
            if os.path.exists(pathToFile):
                os.remove(pathToFile)

    def check_loaded_data_by_tables(self, data):
        with self.conn.cursor() as cursor:
            print("-----Source Database-----")
            for d in data:
                table_name = d['table_name']
                cursor.execute(f'select count(*) from {table_name}')
                count = cursor.fetchall()
                print(f"{table_name} count: {count}")

        with self.conn.cursor() as cursor:
            print("-----Target Database-----")
            for d in data:
                table_name = d['table_name']
                cursor.execute(f'select count(*) from {table_name}')
                count = cursor.fetchall()
                print(f"{table_name} count: {count}")
