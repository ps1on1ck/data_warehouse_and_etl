import psycopg2
import os


class Migration:
    def __init__(self, conn_string_source, conn_string_target, result_folder='results'):
        self.conn_string_source = conn_string_source
        self.conn_string_target = conn_string_target
        self.result_folder = result_folder

    def download_data_by_tables(self, tables):
        with psycopg2.connect(self.conn_string_source) as conn, conn.cursor() as cursor:
            for table in tables:
                q = f"COPY {table} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
                filename = f"./{self.result_folder}/{table}.csv"
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                with open(filename, 'w', encoding='utf-8') as f:
                    cursor.copy_expert(q, f)


    def load_data_by_tables(self, tables):
        with psycopg2.connect(self.conn_string_target) as conn, conn.cursor() as cursor:
            for table in tables:
                q = f"COPY {table} from STDIN WITH DELIMITER ',' CSV HEADER;"
                filename = f"./{self.result_folder}/{table}.csv"
                with open(filename, 'r', encoding='utf-8') as f:
                    cursor.copy_expert(q, f)


    def check_loaded_data_by_tables(self, tables):
        with psycopg2.connect(self.conn_string_source) as conn, conn.cursor() as cursor:
                print("-----Source Database-----")
                for table in tables:
                    cursor.execute(f'select count(*) from {table} limit 1')
                    count = cursor.fetchall()
                    print(f"{table} count: {count}")

        with psycopg2.connect(self.conn_string_target) as conn, conn.cursor() as cursor:
            print("-----Target Database-----")
            for table in tables:
                cursor.execute(f'select count(*) from {table} limit 1')
                count = cursor.fetchall()
                print(f"{table} count: {count}")
